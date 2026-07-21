//! #037 integration + security: the FIX 4.4 TCP acceptor over IronFix's
//! `FixCodec`.
//!
//! These tests drive the acceptor over a **real** ephemeral TCP socket with a
//! `tokio::net::TcpStream` client, asserting the accept → frame → decode →
//! dispatch pipe and every DoS control as a **security** control
//! ([08 §4 / §5](../docs/08-threat-model.md), [037 spec](../milestones/v0.4-fix-gateway/037-fix-tcp-acceptor-codec.md)):
//!
//! - the acceptor binds an ephemeral port, accepts a connection, and round-trips a
//!   `FixCodec`-framed message through the dispatch seam;
//! - the connection cap refuses the N+1th connection (not queued unbounded);
//! - an oversize frame is rejected at the framing boundary with no panic;
//! - clean shutdown drains in-flight connections, and connect/disconnect churn
//!   leaves the live-session task count flat (no leak).
//!
//! The bounded-mailbox DoS control and the no-credential-in-logs guarantee are
//! covered deterministically by the co-located unit tests in
//! `src/gateway/fix/acceptor.rs` (`test_outbound_mailbox_full_surfaces_typed_busy_and_latches`
//! and `test_stub_dispatch_logs_no_credential`, which greps a captured subscriber
//! over the real decode + stub-dispatch path).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fauxchange::gateway::fix::{
    DecodedMessage, FixAcceptor, FixAcceptorConfig, FixDecodeError, FixSession, FixSessionFactory,
    SessionControl, SessionOutbound,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;

// ============================================================================
// Wire-frame helpers (a valid FIX 4.4 frame with a computed BodyLength/CheckSum)
// ============================================================================

/// Builds a complete valid frame from a body (the fields after `9=<len>SOH`),
/// computing a real `BodyLength (9)` and `CheckSum (10)`.
fn frame_with_body(body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
    msg.extend_from_slice(body);
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
    msg
}

/// A minimal valid `Heartbeat (0)` frame.
fn heartbeat_frame() -> Vec<u8> {
    frame_with_body(b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01")
}

/// A frame with an ARBITRARY declared `BodyLength (9)` and a valid CheckSum over
/// the real bytes — the shape a BodyLength / oversize attack takes.
fn frame_with_declared_body_length(declared: &str, body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={declared}\x01").as_bytes());
    msg.extend_from_slice(body);
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
    msg
}

// ============================================================================
// Test sessions (the dispatch-seam impls the tests plug in)
// ============================================================================

/// A session that keeps the connection open and never replies — for the cap /
/// churn / shutdown / oversize lifecycle tests. It holds each accepted connection
/// alive so its slot stays reserved.
struct SilentFactory;

impl FixSessionFactory for SilentFactory {
    type Session = SilentSession;
    fn create(&self, _peer: SocketAddr) -> SilentSession {
        SilentSession
    }
}

struct SilentSession;

impl FixSession for SilentSession {
    async fn on_message(
        &mut self,
        _message: DecodedMessage,
        _out: &SessionOutbound,
    ) -> SessionControl {
        SessionControl::Continue
    }
    async fn on_decode_error(
        &mut self,
        _error: &FixDecodeError,
        _out: &SessionOutbound,
    ) -> SessionControl {
        SessionControl::Continue
    }
}

/// A session that replies to each decoded message with a canned Heartbeat frame —
/// proving the full read → decode → dispatch → bounded-outbound → write pipe.
struct EchoFactory;

impl FixSessionFactory for EchoFactory {
    type Session = EchoSession;
    fn create(&self, _peer: SocketAddr) -> EchoSession {
        EchoSession
    }
}

struct EchoSession;

impl FixSession for EchoSession {
    async fn on_message(
        &mut self,
        _message: DecodedMessage,
        out: &SessionOutbound,
    ) -> SessionControl {
        // The reply proves the message was decoded and the outbound mailbox reaches
        // the socket writer.
        let _ = out.send(heartbeat_frame());
        SessionControl::Continue
    }
    async fn on_decode_error(
        &mut self,
        _error: &FixDecodeError,
        _out: &SessionOutbound,
    ) -> SessionControl {
        SessionControl::Continue
    }
}

// ============================================================================
// Harness
// ============================================================================

/// A bound-and-serving acceptor plus the handles the tests observe.
struct Harness {
    addr: SocketAddr,
    sessions: Arc<AtomicUsize>,
    slots: Arc<tokio::sync::Semaphore>,
    cap: usize,
    shutdown: watch::Sender<bool>,
    serve: JoinHandle<()>,
}

impl Harness {
    /// Starts an acceptor with a generous idle timeout (the lifecycle tests do not
    /// exercise the idle path).
    async fn start<F: FixSessionFactory>(factory: F, cap: usize, max_frame_bytes: usize) -> Self {
        Self::start_with_idle(factory, cap, max_frame_bytes, Duration::from_secs(30)).await
    }

    /// Starts an acceptor with an explicit idle timeout (the idle-timeout tests use
    /// a short one so they run fast; the runtime config takes a `Duration`, so the
    /// tests bypass the config layer's whole-seconds granularity).
    async fn start_with_idle<F: FixSessionFactory>(
        factory: F,
        cap: usize,
        max_frame_bytes: usize,
        idle_timeout: Duration,
    ) -> Self {
        let config = FixAcceptorConfig {
            addr: "127.0.0.1:0".parse().expect("loopback addr"),
            connection_cap: cap,
            mailbox_depth: 16,
            max_frame_bytes,
            idle_timeout,
        };
        let acceptor = FixAcceptor::bind(config)
            .await
            .expect("bind ephemeral port");
        let addr = acceptor.local_addr();
        let sessions = acceptor.active_sessions_handle();
        let slots = acceptor.connection_slots_handle();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let serve = tokio::spawn(acceptor.serve(Arc::new(factory), shutdown_rx));
        Self {
            addr,
            sessions,
            slots,
            cap,
            shutdown,
            serve,
        }
    }

    fn live_sessions(&self) -> usize {
        self.sessions.load(Ordering::Relaxed)
    }

    fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }
}

/// Polls `predicate` until it holds or the deadline elapses; returns whether it
/// held (so a test asserts rather than hangs).
async fn wait_until<F: Fn() -> bool>(predicate: F, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    predicate()
}

/// Reads until at least one byte arrives or EOF/timeout; returns the bytes read
/// (empty on EOF).
async fn read_some(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
        Ok(Ok(n)) => buf[..n].to_vec(),
        _ => Vec::new(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_acceptor_binds_and_round_trips_a_framed_message() {
    // The acceptor binds an ephemeral port, a per-connection task frames a real
    // FixCodec frame, decodes it (#036), and the echo session replies — the client
    // reads the framed reply back.
    let harness = Harness::start(EchoFactory, 8, 64 * 1024).await;
    let mut client = TcpStream::connect(harness.addr)
        .await
        .expect("connect to acceptor");
    client
        .write_all(&heartbeat_frame())
        .await
        .expect("send heartbeat");
    client.flush().await.expect("flush");

    let reply = read_some(&mut client, Duration::from_secs(2)).await;
    assert!(
        reply.starts_with(b"8=FIX.4.4\x01"),
        "the acceptor decoded the frame and the echo session replied with a FIX frame; got {} bytes",
        reply.len()
    );

    harness.shutdown.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(2), harness.serve).await;
}

#[tokio::test]
async fn test_connection_cap_refuses_the_n_plus_1th_connection() {
    // Cap = 2: two connections take both slots; the third is REFUSED (its socket is
    // dropped by the venue — the client reads EOF), never queued unbounded.
    let harness = Harness::start(SilentFactory, 2, 64 * 1024).await;

    let _a = TcpStream::connect(harness.addr).await.expect("conn a");
    let _b = TcpStream::connect(harness.addr).await.expect("conn b");
    // Wait until both sessions have reserved their slots.
    assert!(
        wait_until(|| harness.free_slots() == 0, Duration::from_secs(2)).await,
        "two live connections consume both slots"
    );
    assert_eq!(harness.live_sessions(), 2);

    // The third connects at the OS level but the venue refuses it and drops the
    // socket → the client reads EOF (0 bytes), rather than the connection being
    // queued unbounded.
    let mut third = TcpStream::connect(harness.addr)
        .await
        .expect("conn c (tcp)");
    let refused = read_some(&mut third, Duration::from_secs(2)).await;
    assert!(
        refused.is_empty(),
        "the N+1th connection is refused (EOF), not served; got {} bytes",
        refused.len()
    );
    // The two originals still hold their slots (the cap did not admit a third).
    assert_eq!(harness.free_slots(), 0);

    harness.shutdown.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(3), harness.serve).await;
}

#[tokio::test]
async fn test_oversize_frame_is_rejected_at_boundary_without_panic() {
    // A frame declaring a u64::MAX BodyLength would overflow FixCodec's unchecked
    // add (panicking in debug); the venue guard rejects it at the framing boundary
    // and closes the session — no panic, and the acceptor keeps serving (a fresh
    // connection still round-trips).
    let harness = Harness::start(EchoFactory, 8, 64 * 1024).await;

    let mut hostile = TcpStream::connect(harness.addr)
        .await
        .expect("hostile conn");
    let oversize = frame_with_declared_body_length(
        "18446744073709551615",
        b"35=0\x0149=C\x0156=V\x0134=1\x0152=20240329-12:00:00.000\x01",
    );
    hostile.write_all(&oversize).await.expect("send oversize");
    hostile.flush().await.expect("flush");
    // The venue closes the hostile session → the client reads EOF.
    let closed = read_some(&mut hostile, Duration::from_secs(2)).await;
    assert!(
        closed.is_empty(),
        "the oversize frame is rejected and the session closed (EOF); got {} bytes",
        closed.len()
    );
    // The hostile session tore down; the acceptor did not panic and still serves.
    assert!(
        wait_until(|| harness.live_sessions() == 0, Duration::from_secs(2)).await,
        "the rejected session is reaped"
    );

    let mut healthy = TcpStream::connect(harness.addr)
        .await
        .expect("healthy conn");
    healthy
        .write_all(&heartbeat_frame())
        .await
        .expect("send heartbeat");
    healthy.flush().await.expect("flush");
    let reply = read_some(&mut healthy, Duration::from_secs(2)).await;
    assert!(
        reply.starts_with(b"8=FIX.4.4\x01"),
        "the acceptor keeps serving after rejecting a hostile frame"
    );

    harness.shutdown.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(3), harness.serve).await;
}

#[tokio::test]
async fn test_churn_leaves_the_task_count_flat() {
    // Connect/disconnect churn must leave NO leaked task: after many
    // connect-then-close cycles the live-session gauge returns to zero and every
    // connection slot is reclaimed.
    let harness = Harness::start(SilentFactory, 8, 64 * 1024).await;

    for _ in 0..40 {
        let mut client = TcpStream::connect(harness.addr).await.expect("churn conn");
        client
            .write_all(&heartbeat_frame())
            .await
            .expect("send heartbeat");
        client.flush().await.expect("flush");
        // Close the client — the server session observes EOF and tears down.
        drop(client);
    }

    assert!(
        wait_until(|| harness.live_sessions() == 0, Duration::from_secs(5)).await,
        "churn leaves no leaked session task (gauge returns to 0), got {}",
        harness.live_sessions()
    );
    assert_eq!(
        harness.free_slots(),
        harness.cap,
        "every connection slot is reclaimed after churn"
    );

    harness.shutdown.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(3), harness.serve).await;
}

#[tokio::test]
async fn test_shutdown_drains_in_flight_connections() {
    // A held-open connection is drained on venue shutdown: signalling shutdown
    // stops the accept loop AND closes the in-flight session, so the serve task
    // completes and the gauge returns to zero (no task leak).
    let harness = Harness::start(SilentFactory, 8, 64 * 1024).await;
    let mut held = TcpStream::connect(harness.addr).await.expect("held conn");
    held.write_all(&heartbeat_frame()).await.expect("send");
    held.flush().await.expect("flush");
    assert!(
        wait_until(|| harness.live_sessions() == 1, Duration::from_secs(2)).await,
        "the connection is live before shutdown"
    );

    // Signal venue shutdown; the accept loop stops and drains the in-flight session.
    harness.shutdown.send(true).ok();
    let served = tokio::time::timeout(Duration::from_secs(3), harness.serve).await;
    assert!(
        served.is_ok(),
        "the serve loop returns after shutdown (drained in-flight connections)"
    );
    assert_eq!(
        harness.sessions.load(Ordering::Relaxed),
        0,
        "the in-flight session was drained on shutdown (no leak)"
    );
    // The held socket was closed by the venue on drain.
    let closed = read_some(&mut held, Duration::from_secs(2)).await;
    assert!(closed.is_empty(), "the drained connection was closed");
}

#[tokio::test]
async fn test_idle_silent_connection_is_reclaimed() {
    // Slowloris hygiene: a connection that sends NO bytes is closed after the read
    // idle timeout, releasing its cap slot — it cannot pin the connection cap
    // forever. A short idle timeout keeps the test fast.
    let idle = Duration::from_millis(300);
    let harness = Harness::start_with_idle(SilentFactory, 4, 64 * 1024, idle).await;

    let mut silent = TcpStream::connect(harness.addr).await.expect("silent conn");
    assert!(
        wait_until(|| harness.live_sessions() == 1, Duration::from_secs(2)).await,
        "the silent connection is live before the idle timeout"
    );
    // Never write a byte. After the idle timeout the venue closes it and reclaims
    // the slot/gauge.
    assert!(
        wait_until(|| harness.live_sessions() == 0, Duration::from_secs(3)).await,
        "the silent connection is reclaimed after the idle timeout (gauge→0)"
    );
    assert_eq!(
        harness.free_slots(),
        harness.cap,
        "the idle connection's slot is reclaimed"
    );
    let closed = read_some(&mut silent, Duration::from_secs(2)).await;
    assert!(
        closed.is_empty(),
        "the idle connection was closed by the venue"
    );

    harness.shutdown.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(3), harness.serve).await;
}

#[tokio::test]
async fn test_idle_partial_frame_then_silent_is_reclaimed() {
    // A connection that sends a partial `8=FIX.4.4|` then goes silent (a classic
    // slow-header Slowloris) is likewise reclaimed after the idle timeout — the
    // partial frame does not keep it alive.
    let idle = Duration::from_millis(300);
    let harness = Harness::start_with_idle(SilentFactory, 4, 64 * 1024, idle).await;

    let mut client = TcpStream::connect(harness.addr)
        .await
        .expect("partial conn");
    client
        .write_all(b"8=FIX.4.4\x01")
        .await
        .expect("send partial header");
    client.flush().await.expect("flush");
    assert!(
        wait_until(|| harness.live_sessions() == 1, Duration::from_secs(2)).await,
        "the partial connection is live"
    );
    // Then go silent — reclaimed after the idle timeout.
    assert!(
        wait_until(|| harness.live_sessions() == 0, Duration::from_secs(3)).await,
        "a partial-then-silent connection is reclaimed after the idle timeout"
    );

    harness.shutdown.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(3), harness.serve).await;
}

#[tokio::test]
async fn test_idle_connection_making_steady_progress_is_not_killed() {
    // A connection making steady progress (a frame every < idle_timeout) must NOT
    // be killed — the idle timer resets on every read.
    let idle = Duration::from_millis(400);
    let harness = Harness::start_with_idle(EchoFactory, 4, 64 * 1024, idle).await;

    let mut client = TcpStream::connect(harness.addr).await.expect("steady conn");
    // Send several frames spaced well within the idle window; the connection must
    // stay live throughout.
    for _ in 0..5 {
        client
            .write_all(&heartbeat_frame())
            .await
            .expect("send heartbeat");
        client.flush().await.expect("flush");
        // The echo reply proves the connection is still being served.
        let reply = read_some(&mut client, Duration::from_secs(1)).await;
        assert!(
            reply.starts_with(b"8=FIX.4.4\x01"),
            "the steadily-progressing connection is still served (got a reply)"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert_eq!(
        harness.live_sessions(),
        1,
        "a connection making steady progress is not killed by the idle timeout"
    );

    harness.shutdown.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(3), harness.serve).await;
}
