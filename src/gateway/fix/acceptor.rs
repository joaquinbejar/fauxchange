//! The FIX 4.4 TCP acceptor — the raw-TCP accept loop, the per-connection task
//! lifecycle, and the DoS controls, built over `ironfix-transport`'s `FixCodec`
//! (#037).
//!
//! IronFix ships **no** acceptor — `ironfix-transport` has the `FixCodec` framing
//! codec only, no TCP listener — so the accept loop, the per-connection task, and
//! the connection lifecycle are new venue work
//! ([03 §5 / §5.1](../../../docs/03-protocol-surfaces.md#5-fix-44-gateway-new),
//! [ADR-0002](../../../docs/adr/0002-fix-4-4-gateway-on-ironfix.md)). This module
//! delivers that plumbing with a clean **dispatch seam** ([`FixSession`]) where the
//! session FSM (logon / heartbeat / sequence store / resend, #038) plugs in; #037
//! ships a logging stub ([`StubSessionFactory`]).
//!
//! ## The pipe
//!
//! `TcpListener::accept` → one tokio task per connection → the connection's read
//! half is framed by a [`BoundedFrameDecoder`] (a by-policy byte cap around
//! `FixCodec`, see below) and each complete frame is decoded into a typed
//! [`DecodedMessage`](super::DecodedMessage) via the #036
//! [`decode`](super::decode) path → handed to the connection's [`FixSession`] →
//! any reply frames are enqueued on a **bounded** outbound `mpsc` a dedicated
//! writer task drains to the socket. The gateway holds an
//! [`Arc<AppState>`](crate::state::AppState) through its
//! [`FixSessionFactory`] to reach auth / rate-limit / the sequencer — the gateway
//! depends on `AppState`, never the reverse
//! ([CLAUDE.md](../../../CLAUDE.md) "Module Boundaries").
//!
//! ## DoS controls (security, not fairness — [08 §4 / §5](../../../docs/08-threat-model.md))
//!
//! Every bound below is a **security control** wired from the first commit:
//!
//! - **Connection cap** — a venue-wide [`Semaphore`]; the N+1th connection is
//!   **refused** (the socket is dropped), never queued unbounded.
//! - **Bounded outbound mailbox** — the per-session `mpsc` is bounded at
//!   `mailbox_depth`; a full mailbox marks the session overflowed and closes it
//!   (a typed busy), never an unbounded queue.
//! - **Max frame length** — [`BoundedFrameDecoder`] caps a frame at
//!   `max_frame_bytes` by **policy** (a DoS/resource ceiling), handing `FixCodec`
//!   that cap as its `max_message_size`, so a frame whose declared total length
//!   exceeds it is rejected at the framing boundary with
//!   `CodecError::MessageTooLarge` and no unbounded allocation. The
//!   hostile-arithmetic *correctness* — an overflowing `BodyLength (9)` add, an
//!   out-of-range `CheckSum (10)` — is owned by `FixCodec` itself as of
//!   `ironfix-transport` 0.4, which folds both fields with checked,
//!   non-wrapping arithmetic and returns a typed `CodecError` (never a panic), so
//!   the venue no longer pre-checks them (the framing-layer precheck was retired
//!   in #140). The accumulated read buffer is independently capped at
//!   `max_frame_bytes`, so a peer dribbling an unframed blob cannot grow it
//!   unbounded either.
//! - **Graceful shutdown** — every task observes a shared [`watch`] signal;
//!   on shutdown the accept loop stops and the in-flight sessions drain and close,
//!   so connect/disconnect churn and process shutdown both leave **no task leak**
//!   (an [`Arc<AtomicUsize>`] session gauge witnesses it).
//!
//! ## No credential ever reaches a log
//!
//! `tracing` spans/events carry the peer address and message-**type** / error-**kind**
//! only — **never** a frame payload or a decoded field (a `Logon` `Password (554)`
//! lives inside the decoded message and is never logged;
//! [08 §4](../../../docs/08-threat-model.md#4-untrusted-input-hardening)).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use ironfix_transport::{CodecError, FixCodec};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::codec::Decoder;
use tracing::Instrument;

use super::{DecodedMessage, FixBody, FixDecodeError};

/// The bytes reserved for the per-connection read buffer on each socket read — a
/// bounded chunk so the buffer grows in steps, never a single huge reservation.
/// The total accumulation is capped at `max_frame_bytes` (see [`run_session`]).
const READ_CHUNK_BYTES: usize = 8 * 1024;

/// The pause before retrying after a transient `accept()` error, so a persistent
/// accept failure (e.g. the process is out of file descriptors) does not spin the
/// accept loop hot.
const ACCEPT_RETRY_PAUSE: Duration = Duration::from_millis(5);

/// The grace window a closing session waits for its writer to drain queued
/// outbound frames before force-aborting it. A cooperative peer receives its
/// queued frames well within this on localhost; a peer that has stopped reading
/// must **not** stall teardown (that would leak the task), so past the grace the
/// writer is aborted and the socket force-closed.
const WRITER_DRAIN_GRACE: Duration = Duration::from_millis(500);

/// The cadence at which [`run_session`] delivers [`FixSession::on_tick`] — the
/// granularity the session's own heartbeat / logon-timeout / revocation checks run
/// at (the session compares venue-clock instants, so this is only the polling
/// resolution, not the protocol interval itself).
const SESSION_TICK: Duration = Duration::from_millis(500);

/// The ceiling on a single inline dispatch. #038's logon `on_message` runs an
/// Argon2id verify (on the blocking pool) and a registry lookup; this bound closes
/// the #038 obligation — a dispatch that neither the session nor `spawn_blocking`
/// completes within the bound must not pin the connection slot / live-session gauge
/// past the read-idle timeout or block graceful drain, so it is **raced against the
/// shutdown signal and force-timed-out** here. Generous relative to an Argon2id
/// verify at the pinned OWASP parameters, tight relative to a stall.
const MAX_DISPATCH: Duration = Duration::from_secs(5);

// ============================================================================
// The dispatch seam — where #038's session FSM plugs in
// ============================================================================

/// A per-connection FIX session: the seam where #038's acceptor-side session FSM
/// (logon auth, heartbeat cadence, the durable sequence store, resend / gap-fill)
/// plugs in. #037 ships a logging stub ([`StubSession`]); the acceptor owns the
/// transport (framing, the bounded mailbox, the lifecycle) and calls into the
/// session for every decoded frame.
///
/// A session is created per accepted connection by a [`FixSessionFactory`] and
/// runs inside that connection's single tokio task, so it need not be `Sync` — it
/// is only ever touched from its own task.
///
/// The callbacks are **async** (return-position `impl Future + Send`, no
/// `async-trait` box — the acceptor is generic over the session type): #038's
/// logon path runs an Argon2id verify (a deliberately slow, CPU-bound hash) and a
/// possibly DB-backed registry lookup, neither of which can run in a synchronous
/// callback without blocking a tokio worker. The `+ Send` bound keeps the
/// per-connection task spawnable.
///
/// # Bounding the dispatch — a #038 obligation (contract gap tracked here)
///
/// The acceptor `.await`s [`on_message`](Self::on_message) /
/// [`on_decode_error`](Self::on_decode_error) **inline** in the per-connection
/// frame-drain loop and does **not** currently bound or cancel an in-flight call.
/// With the #037 [`StubSession`] this returns instantly, so it is not exploitable
/// today; but #038's `on_message` will run an Argon2id verify + a possibly
/// DB-backed registry lookup, and an unbounded or stalled dispatch would hold the
/// connection slot and the live-session gauge **past the read-idle timeout** (which
/// fires only from the *outer* `select!`, never while a dispatch future is in
/// flight) and block graceful drain. **#038 MUST** therefore either (a) race the
/// dispatch against the shutdown signal and enforce a max-dispatch timeout in the
/// acceptor, or (b) require every `FixSession` impl to bound its own async work
/// (e.g. a `tokio::time::timeout` around the Argon2id / registry call). This gap is
/// documented at the seam so #038 cannot miss it.
pub trait FixSession: Send + 'static {
    /// Handles one successfully-decoded inbound message, using `out` to enqueue
    /// any reply frames onto the bounded outbound mailbox. Returns whether to keep
    /// the session open.
    fn on_message(
        &mut self,
        message: DecodedMessage,
        out: &SessionOutbound,
    ) -> impl std::future::Future<Output = SessionControl> + Send;

    /// Handles a typed post-framing decode failure (a bad enum, a missing
    /// required tag, …); [`FixDecodeError::reject_route`](super::FixDecodeError::reject_route)
    /// classifies the reject #038 will emit. #037's stub logs the kind and keeps
    /// the session open (the actual reject frame is #038/#039).
    fn on_decode_error(
        &mut self,
        error: &FixDecodeError,
        out: &SessionOutbound,
    ) -> impl std::future::Future<Output = SessionControl> + Send;

    /// A periodic wall-clock tick (every [`SESSION_TICK`]) that lets the session
    /// drive its own **negotiated** protocol cadence off the venue clock — the
    /// heartbeat interval, the `TestRequest` liveness probe, the logon timeout, and
    /// the per-tick revocation drop (#038). It is decoupled from the read-idle
    /// timeout (connection hygiene, this file's) — the tick fires whether or not
    /// bytes arrive. The default is a no-op (the #037 [`StubSession`] has no
    /// cadence).
    fn on_tick(
        &mut self,
        out: &SessionOutbound,
    ) -> impl std::future::Future<Output = SessionControl> + Send {
        let _ = out;
        std::future::ready(SessionControl::Continue)
    }
}

/// Whether a [`FixSession`] callback wants the connection to continue or close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionControl {
    /// Keep serving the connection.
    Continue,
    /// Close the connection after flushing any queued outbound frames.
    Close,
}

/// Builds one [`FixSession`] per accepted connection.
///
/// The concrete factory holds the [`Arc<AppState>`](crate::state::AppState) the
/// gateway reaches auth / rate-limit / the sequencer through — the seam is the one
/// place the FIX transport touches the application layer.
pub trait FixSessionFactory: Send + Sync + 'static {
    /// The session type this factory creates.
    type Session: FixSession;

    /// Whether the venue currently admits a new FIX session. A refused connection
    /// is dropped (not queued). The default admits unconditionally; the concrete
    /// factory gates on venue-serving state.
    fn admit(&self) -> bool {
        true
    }

    /// Creates a session for the connection from `peer`.
    fn create(&self, peer: SocketAddr) -> Self::Session;
}

// ============================================================================
// The bounded outbound mailbox handle
// ============================================================================

/// Why a [`SessionOutbound::send`] could not enqueue a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OutboundBusy {
    /// The bounded mailbox is full — the writer is not draining fast enough. This
    /// is the **DoS bound**: the session is closed rather than growing an unbounded
    /// queue.
    #[error("fix outbound mailbox is full (bounded at the configured mailbox depth)")]
    Full,
    /// The writer task has gone (the socket closed) — the session is ending.
    #[error("fix outbound mailbox is closed (the connection is shutting down)")]
    Closed,
}

/// A cloneable handle onto a session's **bounded** outbound mailbox — the only way
/// a session emits a frame.
///
/// It is a non-blocking `try_send` onto a bounded `mpsc` (`mailbox_depth`): a full
/// mailbox returns [`OutboundBusy::Full`] **and** latches an overflow flag the
/// acceptor reads to close the session (so the DoS bound is enforced even if a
/// session ignores the error). #038/#040 clone this to push venue-originated
/// frames (execution reports, market-data deltas) into the same bounded pipe.
#[derive(Clone)]
pub struct SessionOutbound {
    tx: mpsc::Sender<Vec<u8>>,
    overflowed: Arc<AtomicBool>,
}

impl SessionOutbound {
    /// Enqueues one complete FIX frame onto the bounded mailbox without blocking.
    ///
    /// # Errors
    ///
    /// [`OutboundBusy::Full`] when the mailbox is at its bound (the overflow flag
    /// is latched), or [`OutboundBusy::Closed`] when the writer has gone.
    pub fn send(&self, frame: Vec<u8>) -> Result<(), OutboundBusy> {
        match self.tx.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.overflowed.store(true, Ordering::Relaxed);
                Err(OutboundBusy::Full)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(OutboundBusy::Closed),
        }
    }

    /// Whether a send has overflowed the bounded mailbox since this handle was
    /// created — the acceptor closes the session when it has.
    #[must_use]
    fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Relaxed)
    }
}

// ============================================================================
// The bounded framing decoder — the by-policy byte-cap ceiling around FixCodec
// ============================================================================

/// A **by-policy byte cap** around [`FixCodec`] — a DoS / resource ceiling, **not**
/// a security correctness check.
///
/// It caps a frame at `max_frame_bytes` by handing that value to `FixCodec` as its
/// `max_message_size`, so a frame whose declared total length exceeds the cap is
/// rejected at the framing boundary ([`CodecError::MessageTooLarge`]) with no
/// unbounded allocation. That is the *only* thing this wrapper adds; the cap is a
/// resource policy the operator tunes (`[fix] max_frame_bytes`), not a
/// correctness guard.
///
/// The **hostile-arithmetic correctness** — an overflowing `BodyLength (9)` add
/// (`... + body_length + 7`) and an out-of-range `CheckSum (10)` triple — is owned
/// by `FixCodec` itself as of `ironfix-transport` 0.4: the frame-length add is a
/// `checked_add` chain returning [`CodecError::InvalidBodyLength`] on overflow, and
/// the checksum is folded in `u16` and range-checked to `0..=255`
/// (`parse_checksum` returns `None` → `InvalidBodyLength`), so a hostile value is a
/// typed [`CodecError`] and **never a panic**, in a debug or a release build. The
/// venue therefore does **not** pre-check either field — the framing-layer precheck
/// was retired in #140. Do **not** re-add a security rationale here: that
/// correctness now lives upstream in the checked decoder.
#[derive(Debug)]
pub struct BoundedFrameDecoder {
    inner: FixCodec,
}

impl BoundedFrameDecoder {
    /// Builds a decoder capping frames at `max_frame_bytes` bytes (the by-policy
    /// resource ceiling), clamped up to [`MIN_FRAME_BYTES`] so `FixCodec` is never
    /// handed a nonsensically tiny cap.
    #[must_use]
    pub fn new(max_frame_bytes: usize) -> Self {
        let max = max_frame_bytes.max(MIN_FRAME_BYTES);
        Self {
            // The byte cap is enforced by `FixCodec`'s own `max_message_size`
            // total-length check (`total_length > max_message_size` →
            // `MessageTooLarge`), which — as of 0.4 — runs after a *checked*
            // frame-length add, so no hostile declared length can overflow it.
            inner: FixCodec::new().with_max_message_size(max),
        }
    }

    /// Decodes one complete frame by delegating to `FixCodec`.
    ///
    /// This is an inherent method, **not** the `tokio_util::codec::Decoder` trait:
    /// the acceptor drives the socket read loop manually (split halves + a bounded
    /// writer task), so the codec is used as a plain frame extractor. All framing
    /// rejection (oversize, begin-string, body-length, checksum) is owned by
    /// `FixCodec` and surfaces as a typed [`CodecError`] — no venue pre-check runs.
    ///
    /// # Errors
    ///
    /// A [`CodecError`] for any inner `FixCodec` framing error (incomplete is
    /// `Ok(None)`, not an error).
    pub fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, CodecError> {
        self.inner.decode(src)
    }
}

/// The absolute floor [`BoundedFrameDecoder::new`] clamps `max_frame_bytes` up to,
/// so `FixCodec::with_max_message_size` is never handed a nonsensically tiny value.
/// This is the decoder's own internal floor and is **independent of** — and much
/// smaller than — the config-layer minimum [`crate::config::FIX_MIN_MAX_FRAME_BYTES`]
/// (the config never passes a value this low; this only defends a direct caller).
const MIN_FRAME_BYTES: usize = 32;

// ============================================================================
// The acceptor
// ============================================================================

/// The runtime configuration for a [`FixAcceptor`], mapped from the `[fix]`
/// [`crate::config::FixConfig`] section.
#[derive(Debug, Clone, Copy)]
pub struct FixAcceptorConfig {
    /// The TCP bind address.
    pub addr: SocketAddr,
    /// The venue connection cap — concurrent sessions past this are refused.
    pub connection_cap: usize,
    /// The per-session bounded outbound mailbox depth.
    pub mailbox_depth: usize,
    /// The maximum on-the-wire frame size, in bytes.
    pub max_frame_bytes: usize,
    /// The read-idle timeout — a connection sending no bytes for this long is
    /// closed (connection hygiene, so a Slowloris of silent sockets cannot pin the
    /// connection cap). Superseded/refined by the negotiated heartbeat in #038.
    pub idle_timeout: Duration,
}

impl FixAcceptorConfig {
    /// Maps the validated `[fix]` config section onto the acceptor runtime config
    /// (the config carries `idle_timeout_secs`; the runtime uses a [`Duration`]).
    #[must_use]
    pub fn from_config(fix: &crate::config::FixConfig) -> Self {
        Self {
            addr: fix.fix_addr,
            connection_cap: fix.connection_cap,
            mailbox_depth: fix.mailbox_depth,
            max_frame_bytes: fix.max_frame_bytes,
            idle_timeout: Duration::from_secs(fix.idle_timeout_secs),
        }
    }
}

/// A bound FIX acceptor: the listening socket plus the venue-wide DoS-control
/// state (the connection-slot semaphore and the live-session gauge).
///
/// [`bind`](Self::bind) binds eagerly (so a bind error surfaces at startup and the
/// resolved local address is known — the ephemeral-port tests read it), then
/// [`serve`](Self::serve) runs the accept loop until the shared shutdown signal
/// fires.
pub struct FixAcceptor {
    listener: TcpListener,
    config: FixAcceptorConfig,
    local_addr: SocketAddr,
    connection_slots: Arc<Semaphore>,
    active_sessions: Arc<AtomicUsize>,
}

impl FixAcceptor {
    /// Binds the acceptor to its configured address.
    ///
    /// # Errors
    ///
    /// Propagates the bind [`std::io::Error`] (e.g. the port is in use).
    pub async fn bind(config: FixAcceptorConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(config.addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            local_addr,
            connection_slots: Arc::new(Semaphore::new(config.connection_cap.max(1))),
            active_sessions: Arc::new(AtomicUsize::new(0)),
            config,
        })
    }

    /// The resolved local bind address (the concrete port when `:0` was requested).
    #[must_use]
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The number of connection slots still free (for observability / tests). Equal
    /// to the cap when no session is live.
    #[must_use]
    #[inline]
    pub fn available_slots(&self) -> usize {
        self.connection_slots.available_permits()
    }

    /// A shared handle onto the live-session gauge — the count of running
    /// per-connection tasks. Clone before [`serve`](Self::serve) (which consumes
    /// `self`) to observe it; it returns to `0` once every session has torn down
    /// (the churn / no-leak witness).
    #[must_use]
    #[inline]
    pub fn active_sessions_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active_sessions)
    }

    /// A shared handle onto the connection-slot semaphore — its available-permit
    /// count returns to the cap once every session has released its slot.
    #[must_use]
    #[inline]
    pub fn connection_slots_handle(&self) -> Arc<Semaphore> {
        Arc::clone(&self.connection_slots)
    }

    /// Runs the accept loop until `shutdown` fires, spawning one task per accepted
    /// connection and draining the in-flight sessions on shutdown.
    ///
    /// A connection past the cap is refused (its socket dropped); a connection the
    /// factory declines to [`admit`](FixSessionFactory::admit) is refused too. On
    /// shutdown — the `shutdown` value changes **or** its sender drops — the loop
    /// stops accepting and awaits the in-flight sessions (each observes the same
    /// signal and closes), so no task leaks.
    pub async fn serve<F: FixSessionFactory>(
        self,
        factory: Arc<F>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        tracing::info!(
            addr = %self.local_addr,
            connection_cap = self.config.connection_cap,
            mailbox_depth = self.config.mailbox_depth,
            max_frame_bytes = self.config.max_frame_bytes,
            idle_ms = self.config.idle_timeout.as_millis(),
            "fix acceptor listening"
        );
        let mut sessions: JoinSet<()> = JoinSet::new();

        loop {
            tokio::select! {
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok((socket, peer)) => {
                            self.spawn_session(socket, peer, &factory, &shutdown, &mut sessions);
                        }
                        Err(error) => {
                            // A transient accept failure (fd exhaustion, a reset
                            // during handshake): log and pause briefly so a
                            // persistent error does not spin the loop hot.
                            tracing::warn!(%error, "fix accept error; pausing before retry");
                            tokio::time::sleep(ACCEPT_RETRY_PAUSE).await;
                        }
                    }
                }
                // Reap finished session tasks continuously so the set stays bounded
                // at the live-connection count (only when non-empty).
                Some(_joined) = sessions.join_next(), if !sessions.is_empty() => {}
                _ = shutdown.changed() => break,
            }
        }

        // Graceful drain: the shutdown signal has fired; every in-flight session
        // holds its own receiver clone, observes the same signal, closes its
        // socket, and returns — so awaiting the set drains them with no leak.
        tracing::info!(
            in_flight = sessions.len(),
            "fix acceptor shutting down; draining in-flight sessions"
        );
        while sessions.join_next().await.is_some() {}
        tracing::info!("fix acceptor stopped");
    }

    /// Admits (or refuses) one accepted connection and, on admission, spawns its
    /// per-connection task into `sessions`.
    fn spawn_session<F: FixSessionFactory>(
        &self,
        socket: TcpStream,
        peer: SocketAddr,
        factory: &Arc<F>,
        shutdown: &watch::Receiver<bool>,
        sessions: &mut JoinSet<()>,
    ) {
        // Connection cap: reserve a slot BEFORE serving; at the cap the connection
        // is refused (the socket is dropped), never queued unbounded.
        let Ok(permit) = Arc::clone(&self.connection_slots).try_acquire_owned() else {
            tracing::warn!(%peer, "fix connection refused: venue connection cap reached");
            drop(socket);
            return;
        };
        // Venue admission gate (the AppState seam): refuse before the venue serves.
        if !factory.admit() {
            tracing::debug!(%peer, "fix connection refused: venue is not admitting sessions");
            drop(permit);
            drop(socket);
            return;
        }
        // Low-latency writes (FIX is latency-sensitive); a failure here is not fatal.
        if let Err(error) = socket.set_nodelay(true) {
            tracing::debug!(%peer, %error, "could not set TCP_NODELAY on fix connection");
        }

        let session = factory.create(peer);
        let guard = SessionGuard::new(Arc::clone(&self.active_sessions));
        let config = self.config;
        let shutdown = shutdown.clone();
        let span = tracing::debug_span!("fix_session", %peer);
        sessions.spawn(
            run_session(socket, peer, session, config, permit, guard, shutdown).instrument(span),
        );
    }
}

/// An RAII guard incrementing the live-session gauge on creation and decrementing
/// it on drop — so the gauge exactly tracks running per-connection tasks (the
/// no-leak / churn witness), whatever path a task exits by.
struct SessionGuard(Arc<AtomicUsize>);

impl SessionGuard {
    fn new(gauge: Arc<AtomicUsize>) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self(gauge)
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

// ============================================================================
// The per-connection task
// ============================================================================

/// Serves one accepted connection: frame → decode → dispatch → bounded outbound,
/// until the peer closes, an error occurs, the session asks to close, or the venue
/// shuts down. Holds the connection slot (`_permit`) and the session gauge
/// (`_guard`) for its whole lifetime, so both are reclaimed on any exit.
async fn run_session<S: FixSession>(
    socket: TcpStream,
    peer: SocketAddr,
    mut session: S,
    config: FixAcceptorConfig,
    _permit: OwnedSemaphorePermit,
    _guard: SessionGuard,
    mut shutdown: watch::Receiver<bool>,
) {
    tracing::debug!(%peer, "fix connection accepted");
    let (mut read_half, write_half) = socket.into_split();

    // The bounded outbound mailbox + its dedicated writer task. The writer ends
    // when every `SessionOutbound` sender drops (the session returns) or a write
    // fails, so awaiting it below drains queued frames before the task exits.
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>(config.mailbox_depth.max(1));
    let outbound = SessionOutbound {
        tx: out_tx,
        overflowed: Arc::new(AtomicBool::new(false)),
    };
    let mut writer = tokio::spawn(writer_loop(write_half, out_rx));

    let mut decoder = BoundedFrameDecoder::new(config.max_frame_bytes);
    let mut buf = BytesMut::with_capacity(READ_CHUNK_BYTES.min(config.max_frame_bytes));

    // The session cadence tick (#038): the session drives its own negotiated
    // heartbeat / logon-timeout / revocation checks off it. The first tick fires
    // one interval in (not immediately), so a just-accepted session is not ticked
    // before it can send its logon.
    let mut tick = tokio::time::interval(SESSION_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.reset();

    'serve: loop {
        // Drain every complete frame currently buffered before reading more.
        loop {
            match decoder.decode(&mut buf) {
                Ok(Some(frame)) => {
                    // #038 obligation, closed here: the dispatch `.await` is BOUNDED —
                    // raced against the shutdown signal and a hard `MAX_DISPATCH`
                    // timeout, so #038's Argon2id-verifying `on_message` (whose verify
                    // runs on the blocking pool) can never pin the connection slot /
                    // live-session gauge past the read-idle timeout or block graceful
                    // drain. On shutdown or timeout the in-flight dispatch future is
                    // dropped (any `spawn_blocking` it awaited finishes on the blocking
                    // pool and is reclaimed) and the session closes.
                    let control = tokio::select! {
                        biased;
                        _ = shutdown.changed() => break 'serve,
                        dispatched = tokio::time::timeout(
                            MAX_DISPATCH,
                            dispatch(&mut session, &frame, &outbound),
                        ) => match dispatched {
                            Ok(control) => control,
                            Err(_elapsed) => {
                                tracing::warn!(
                                    %peer,
                                    max_dispatch_ms = MAX_DISPATCH.as_millis(),
                                    "fix dispatch exceeded the max-dispatch bound; closing session"
                                );
                                break 'serve;
                            }
                        },
                    };
                    // Enforce the outbound-mailbox DoS bound regardless of what the
                    // session did with the send result: a latched overflow closes.
                    if outbound.overflowed() {
                        tracing::warn!(
                            %peer,
                            "fix outbound mailbox full; closing session (bounded-mailbox DoS control)"
                        );
                        break 'serve;
                    }
                    if control == SessionControl::Close {
                        break 'serve;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    // A framing / checksum / oversize / body-length error — a
                    // malformed or hostile frame at the session boundary. Close;
                    // the stream cannot be resynchronised. Only the error KIND is
                    // logged (never the bytes).
                    tracing::debug!(
                        %peer,
                        kind = codec_error_kind(&error),
                        "fix frame rejected at the framing boundary; closing"
                    );
                    break 'serve;
                }
            }
        }

        // Cap the accumulated read buffer: after draining complete frames, any
        // remainder is a partial next frame; if it already exceeds the frame cap
        // it is oversize (or an unframed dribble) — reject without growing further.
        if buf.len() > config.max_frame_bytes {
            tracing::debug!(
                %peer,
                buffered = buf.len(),
                max_frame_bytes = config.max_frame_bytes,
                "fix inbound buffer exceeded the max frame size; closing"
            );
            break 'serve;
        }

        buf.reserve(READ_CHUNK_BYTES);
        tokio::select! {
            read = read_half.read_buf(&mut buf) => {
                match read {
                    Ok(0) => break 'serve,          // peer closed (EOF)
                    Ok(_) => {}                      // loop to drain new frames
                    Err(error) => {
                        tracing::debug!(%peer, %error, "fix connection read error; closing");
                        break 'serve;
                    }
                }
            }
            _ = shutdown.changed() => {
                // Venue shutdown (value changed or sender dropped): tear down.
                break 'serve;
            }
            // The session cadence tick (#038): the session emits any due heartbeat
            // / test request / logout and may ask to close (logon timeout, missed
            // heartbeat, revocation). Bounded like the frame dispatch.
            _ = tick.tick() => {
                let control = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break 'serve,
                    ticked = tokio::time::timeout(MAX_DISPATCH, session.on_tick(&outbound)) => {
                        match ticked {
                            Ok(control) => control,
                            Err(_elapsed) => break 'serve,
                        }
                    }
                };
                if outbound.overflowed() {
                    tracing::warn!(
                        %peer,
                        "fix outbound mailbox full; closing session (bounded-mailbox DoS control)"
                    );
                    break 'serve;
                }
                if control == SessionControl::Close {
                    break 'serve;
                }
            }
            // Read-idle timeout — connection hygiene this issue owns (the socket
            // loop, NOT #038's negotiated protocol heartbeat): a connection that
            // sends no bytes for `idle_timeout` is closed, releasing its cap slot
            // (via the RAII `_permit`/`_guard`) so a Slowloris of silent sockets
            // cannot pin the connection cap ([08 §5](../../../docs/08-threat-model.md#5-denial-of-service-posture)).
            // The timer resets on every read, so a connection making steady progress
            // is never killed. A negotiated heartbeat refines this in #038.
            _ = tokio::time::sleep(config.idle_timeout) => {
                tracing::debug!(
                    %peer,
                    idle_ms = config.idle_timeout.as_millis(),
                    "fix connection idle timeout; closing (connection hygiene, pre-#038 heartbeat)"
                );
                break 'serve;
            }
        }
    }

    // Dropping `outbound` (its sender) closes the mailbox so the writer finishes
    // once it has drained the queued frames. Await it only within a bounded grace:
    // a peer that has stopped reading must not stall teardown (a task leak), so past
    // the grace the writer is aborted and the socket force-closed.
    drop(outbound);
    match tokio::time::timeout(WRITER_DRAIN_GRACE, &mut writer).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%peer, %error, "fix writer task join error"),
        Err(_elapsed) => {
            writer.abort();
            tracing::debug!(%peer, "fix writer did not drain within the grace window; aborting");
        }
    }
    tracing::debug!(%peer, "fix connection closed");
}

/// Decodes one complete frame's bytes into a typed message and hands it (or its
/// decode error) to the session, awaiting the session's (async) decision.
///
/// The seam is `async` because #038's logon path runs an Argon2id verify (a
/// deliberately slow, CPU-bound hash) and a possibly DB-backed registry lookup —
/// neither can run in a synchronous callback without blocking a tokio worker.
async fn dispatch<S: FixSession>(
    session: &mut S,
    frame: &[u8],
    outbound: &SessionOutbound,
) -> SessionControl {
    match super::decode(frame) {
        Ok(message) => session.on_message(message, outbound).await,
        Err(error) => session.on_decode_error(&error, outbound).await,
    }
}

/// Drains the bounded outbound mailbox to the socket's write half, one complete
/// pre-framed FIX frame at a time, until the mailbox closes (every sender dropped)
/// or a write fails. A polite half-close is attempted on exit.
async fn writer_loop(mut write_half: OwnedWriteHalf, mut out_rx: mpsc::Receiver<Vec<u8>>) {
    while let Some(frame) = out_rx.recv().await {
        if write_half.write_all(&frame).await.is_err() {
            break;
        }
        if write_half.flush().await.is_err() {
            break;
        }
    }
    let _ = write_half.shutdown().await;
}

/// A stable, payload-free label for a [`CodecError`] — the framing error **kind**
/// only, never the offending bytes (safe to log).
fn codec_error_kind(error: &CodecError) -> &'static str {
    match error {
        CodecError::InvalidBeginString => "invalid_begin_string",
        CodecError::MissingBodyLength => "missing_body_length",
        CodecError::InvalidBodyLength => "invalid_body_length",
        // Refined framing errors added in ironfix-transport 0.4 (a malformed/over-255
        // `CheckSum (10)`, an over-long standard header, a malformed trailer) — each
        // now a distinct variant instead of the old `InvalidBodyLength` overload.
        CodecError::InvalidChecksumFormat => "invalid_checksum_format",
        CodecError::HeaderTooLong { .. } => "header_too_long",
        CodecError::InvalidTrailer { .. } => "invalid_trailer",
        CodecError::ChecksumMismatch { .. } => "checksum_mismatch",
        CodecError::MessageTooLarge { .. } => "message_too_large",
        CodecError::Io(_) => "io",
        // `CodecError` is `#[non_exhaustive]` as of ironfix-transport 0.4, and
        // `Incomplete` is deprecated (incompleteness is signalled by `Ok(None)`, so
        // it never reaches this label). A wildcard keeps the label stable if upstream
        // adds a framing-error variant.
        _ => "unknown",
    }
}

/// The FIX `MsgType (35)` character of a decoded message — a stable identifier,
/// **never** a field value, for safe logging.
#[must_use]
pub fn message_type_str(message: &DecodedMessage) -> &'static str {
    use super::{execution, marketdata, order, session};
    match message {
        DecodedMessage::Logon(_) => session::Logon::MSG_TYPE,
        DecodedMessage::Logout(_) => session::Logout::MSG_TYPE,
        DecodedMessage::Heartbeat(_) => session::Heartbeat::MSG_TYPE,
        DecodedMessage::TestRequest(_) => session::TestRequest::MSG_TYPE,
        DecodedMessage::ResendRequest(_) => session::ResendRequest::MSG_TYPE,
        DecodedMessage::SequenceReset(_) => session::SequenceReset::MSG_TYPE,
        DecodedMessage::Reject(_) => session::Reject::MSG_TYPE,
        DecodedMessage::NewOrderSingle(_) => order::NewOrderSingle::MSG_TYPE,
        DecodedMessage::OrderCancelRequest(_) => order::OrderCancelRequest::MSG_TYPE,
        DecodedMessage::OrderCancelReplaceRequest(_) => order::OrderCancelReplaceRequest::MSG_TYPE,
        DecodedMessage::OrderMassCancelRequest(_) => order::OrderMassCancelRequest::MSG_TYPE,
        DecodedMessage::OrderStatusRequest(_) => order::OrderStatusRequest::MSG_TYPE,
        DecodedMessage::ExecutionReport(_) => execution::ExecutionReport::MSG_TYPE,
        DecodedMessage::OrderCancelReject(_) => execution::OrderCancelReject::MSG_TYPE,
        DecodedMessage::OrderMassCancelReport(_) => execution::OrderMassCancelReport::MSG_TYPE,
        DecodedMessage::BusinessMessageReject(_) => execution::BusinessMessageReject::MSG_TYPE,
        DecodedMessage::MarketDataRequest(_) => marketdata::MarketDataRequest::MSG_TYPE,
        DecodedMessage::MarketDataSnapshotFullRefresh(_) => {
            marketdata::MarketDataSnapshotFullRefresh::MSG_TYPE
        }
        DecodedMessage::MarketDataIncrementalRefresh(_) => {
            marketdata::MarketDataIncrementalRefresh::MSG_TYPE
        }
        DecodedMessage::MarketDataRequestReject(_) => marketdata::MarketDataRequestReject::MSG_TYPE,
    }
}

// ============================================================================
// The #037 stub session (the seam #038 replaces)
// ============================================================================

/// A [`FixSessionFactory`] for the #037 acceptor: a logging **stub**. It holds the
/// [`Arc<AppState>`](crate::state::AppState) the acceptor reaches the venue through
/// (the module boundary: the gateway depends on `AppState`, never the reverse) and
/// admits sessions only while the venue is serving. The real logon-authenticating,
/// order-routing session FSM is #038/#039.
#[derive(Clone)]
pub struct StubSessionFactory {
    state: Arc<crate::state::AppState>,
}

impl StubSessionFactory {
    /// Wires a stub factory over the shared venue state.
    #[must_use]
    #[inline]
    pub fn new(state: Arc<crate::state::AppState>) -> Self {
        Self { state }
    }
}

impl FixSessionFactory for StubSessionFactory {
    type Session = StubSession;

    fn admit(&self) -> bool {
        // The AppState seam: do not admit FIX sessions before the venue is serving.
        self.state.is_serving()
    }

    fn create(&self, peer: SocketAddr) -> StubSession {
        StubSession { peer }
    }
}

/// The #037 stub session: it logs the decoded message **type** (never a payload)
/// and keeps the connection open. It sends no frames — logon, heartbeat, order
/// routing, and market data are #038–#040; this proves the accept → frame →
/// decode → dispatch pipe end to end without inventing protocol behavior.
pub struct StubSession {
    peer: SocketAddr,
}

impl FixSession for StubSession {
    async fn on_message(
        &mut self,
        message: DecodedMessage,
        _out: &SessionOutbound,
    ) -> SessionControl {
        tracing::debug!(
            peer = %self.peer,
            msg_type = message_type_str(&message),
            "fix frame decoded (stub dispatch; the session FSM is #038)"
        );
        SessionControl::Continue
    }

    async fn on_decode_error(
        &mut self,
        error: &FixDecodeError,
        _out: &SessionOutbound,
    ) -> SessionControl {
        // Only the reject classification (a category, no field value) is logged.
        tracing::debug!(
            peer = %self.peer,
            reject = ?error.reject_route(),
            "fix frame failed post-framing validation (stub; the reject frame is #038/#039)"
        );
        SessionControl::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironfix_core::types::{CompId, SeqNum};

    use crate::gateway::fix::{DecodedMessage, StandardHeader, UtcTimestamp, session};

    /// Builds a complete valid frame from a body (the fields after `9=<len>SOH`),
    /// computing a real BodyLength + CheckSum — same shape the #036 codec tests use.
    fn frame_with_body(body: &[u8]) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(b"8=FIX.4.4\x01");
        msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
        msg.extend_from_slice(body);
        let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
        msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
        msg
    }

    /// A frame carrying an ARBITRARY declared BodyLength(9) with a valid CheckSum —
    /// the shape a BodyLength attack takes.
    fn frame_with_declared_body_length(declared: &str, body: &[u8]) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(b"8=FIX.4.4\x01");
        msg.extend_from_slice(format!("9={declared}\x01").as_bytes());
        msg.extend_from_slice(body);
        let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
        msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
        msg
    }

    const HEARTBEAT_BODY: &[u8] =
        b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01";

    #[test]
    fn test_bounded_decoder_frames_a_conformant_heartbeat() {
        let mut decoder = BoundedFrameDecoder::new(64 * 1024);
        let mut buf = BytesMut::from(&frame_with_body(HEARTBEAT_BODY)[..]);
        match decoder.decode(&mut buf) {
            Ok(Some(frame)) => {
                // The framed bytes decode into the typed Heartbeat via #036.
                assert!(matches!(
                    super::super::decode(&frame),
                    Ok(DecodedMessage::Heartbeat(_))
                ));
            }
            other => panic!("expected a framed heartbeat, got {other:?}"),
        }
        assert!(buf.is_empty(), "the frame was consumed");
    }

    #[test]
    fn test_bounded_decoder_rejects_oversize_declared_length_without_panic() {
        // #140: a declared BodyLength of u64::MAX would overflow FixCodec's
        // `body_len_soh + 1 + body_length + 7` add. As of ironfix-transport 0.4
        // that add is a `checked_add` chain returning `InvalidBodyLength` on
        // overflow — so the frame is rejected at the framing boundary with a typed
        // CodecError, no panic and no allocation of the payload (the decoder owns
        // this; the retired venue precheck did the same job).
        let mut decoder = BoundedFrameDecoder::new(64 * 1024);
        let hostile = frame_with_declared_body_length("18446744073709551615", HEARTBEAT_BODY);
        let mut buf = BytesMut::from(&hostile[..]);
        match decoder.decode(&mut buf) {
            Err(CodecError::InvalidBodyLength) => {}
            other => panic!("expected InvalidBodyLength, got {other:?}"),
        }
    }

    #[test]
    fn test_bounded_decoder_rejects_over_cap_but_in_range_declared_length() {
        // A declared length above the (small) by-policy cap but well within usize is
        // rejected at the boundary too — FixCodec's `total_length > max_message_size`
        // check (the byte cap `BoundedFrameDecoder` sets) fires MessageTooLarge.
        let mut decoder = BoundedFrameDecoder::new(128);
        let hostile = frame_with_declared_body_length("100000", HEARTBEAT_BODY);
        let mut buf = BytesMut::from(&hostile[..]);
        match decoder.decode(&mut buf) {
            Err(CodecError::MessageTooLarge { max_size, .. }) => {
                assert_eq!(max_size, 128);
            }
            other => panic!("expected MessageTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn test_over_cap_frame_is_rejected_before_the_full_body_is_buffered() {
        // The by-policy byte cap rejects an over-cap declared length WITHOUT waiting
        // for (or allocating) the full hostile body: FixCodec 0.4 checks
        // `total_length > max_message_size` before the completeness check, so a small
        // buffer carrying only the header + a complete over-cap `BodyLength (9)` is
        // rejected as MessageTooLarge at the framing boundary — no unbounded growth.
        let mut decoder = BoundedFrameDecoder::new(1024);
        let mut buf = BytesMut::from(&b"8=FIX.4.4\x019=5000\x0135=0\x01"[..]);
        match decoder.decode(&mut buf) {
            Err(CodecError::MessageTooLarge { max_size, .. }) => assert_eq!(max_size, 1024),
            other => panic!("expected MessageTooLarge before completeness, got {other:?}"),
        }
    }

    /// Builds a COMPLETE, BodyLength-correct frame with `body` and an ARBITRARY
    /// 3-char literal `CheckSum (10)` string — the shape a checksum-overflow attack
    /// takes. The checksum must be exactly 3 chars for `FixCodec`'s `10=XXX<SOH>`
    /// framing math.
    fn frame_with_raw_checksum(body: &[u8], checksum: &str) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(b"8=FIX.4.4\x01");
        msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
        msg.extend_from_slice(body);
        msg.extend_from_slice(format!("10={checksum}\x01").as_bytes());
        msg
    }

    #[test]
    fn test_bounded_decoder_rejects_malformed_checksum_without_panic() {
        // #140 regression (was P1): a COMPLETE, BodyLength-correct frame whose
        // CheckSum(10) parses to a value > 255 — a hundreds digit >= 3 (`999`), AND
        // `256`/`260`/`299` (d0=2, d1>=5). ironfix-tagvalue 0.4's parse_checksum
        // folds the three digits in u16 and range-checks to 0..=255, returning None
        // for every one of these; FixCodec maps that to a typed `InvalidChecksumFormat`
        // (0.4 refined this from the earlier `InvalidBodyLength` overload) — a typed
        // reject at the framing boundary, no u8-fold overflow and no panic. `35=0<SOH>`
        // is a 5-byte body, so BodyLength(9)=5 is correct — only the checksum fails.
        for hostile_checksum in ["999", "256", "260", "299"] {
            let frame = frame_with_raw_checksum(b"35=0\x01", hostile_checksum);
            let mut decoder = BoundedFrameDecoder::new(64 * 1024);
            let mut buf = BytesMut::from(&frame[..]);
            match decoder.decode(&mut buf) {
                Err(CodecError::InvalidChecksumFormat) => {}
                other => panic!(
                    "expected InvalidChecksumFormat for `10={hostile_checksum}`, got {other:?}"
                ),
            }
        }
    }

    #[test]
    fn test_bounded_decoder_checksum_boundary_is_exact_at_255() {
        // Boundary-exact through the real decoder: `256` (the first value that
        // overflows a u8) is rejected as InvalidChecksumFormat (parse_checksum → None;
        // 0.4 refined this from InvalidBodyLength), while `255` (the max valid checksum)
        // is NOT — it parses and flows to FixCodec's real sum check (a ChecksumMismatch
        // here, since 255 is unlikely to be the true sum), never a fold-failure reject.
        let mut decoder = BoundedFrameDecoder::new(64 * 1024);
        let mut over = BytesMut::from(&frame_with_raw_checksum(b"35=0\x01", "256")[..]);
        assert!(
            matches!(
                decoder.decode(&mut over),
                Err(CodecError::InvalidChecksumFormat)
            ),
            "256 overflows the u8 domain and must be rejected by the checked fold"
        );
        let mut at_max = BytesMut::from(&frame_with_raw_checksum(b"35=0\x01", "255")[..]);
        assert!(
            !matches!(
                decoder.decode(&mut at_max),
                Err(CodecError::InvalidChecksumFormat)
            ),
            "255 is in-domain and must reach FixCodec's real sum check, not the fold reject"
        );
    }

    #[test]
    fn test_decoder_checksum_fold_never_panics_across_the_input_space() {
        // Exhaustive over the FULL 3-digit checksum input space `000..=999`, driven
        // through the REAL decoder (the retired precheck's magnitude sweep, moved to
        // prove the upstream checked fold): every value `> 255` (the whole `256..=999`
        // band) must be rejected as InvalidChecksumFormat (parse_checksum's u16 fold
        // range-checks to 0..=255 → None; 0.4 refined this from InvalidBodyLength);
        // every value `<= 255` must NOT be an InvalidChecksumFormat (it reaches
        // FixCodec's real sum check — a ChecksumMismatch, or a match for the one true
        // value). The loop completing IS the no-panic assertion, in a debug OR a
        // release build.
        for value in 0u16..=999 {
            let checksum = format!("{value:03}");
            let frame = frame_with_raw_checksum(b"35=0\x01", &checksum);
            let mut decoder = BoundedFrameDecoder::new(64 * 1024);
            let mut buf = BytesMut::from(&frame[..]);
            let outcome = decoder.decode(&mut buf);
            if value > 255 {
                assert!(
                    matches!(outcome, Err(CodecError::InvalidChecksumFormat)),
                    "`10={checksum}` (> 255) must be rejected by the checked fold, got {outcome:?}"
                );
            } else {
                assert!(
                    !matches!(outcome, Err(CodecError::InvalidChecksumFormat)),
                    "`10={checksum}` (<= 255) must reach the real sum check, got {outcome:?}"
                );
            }
        }
    }

    #[test]
    fn test_bounded_decoder_defers_on_incomplete_frame() {
        // A partial frame (BodyLength field not yet SOH-terminated) is not rejected
        // early — the decoder waits for more bytes.
        let mut decoder = BoundedFrameDecoder::new(64 * 1024);
        let mut buf = BytesMut::from(&b"8=FIX.4.4\x019=999"[..]);
        assert!(matches!(decoder.decode(&mut buf), Ok(None)));
    }

    #[test]
    fn test_outbound_mailbox_full_surfaces_typed_busy_and_latches() {
        // A capacity-1 mailbox with no reader: the first send fits, the second
        // overflows with a typed Full and latches the overflow flag (the acceptor's
        // close trigger) — never an unbounded queue.
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(1);
        let outbound = SessionOutbound {
            tx,
            overflowed: Arc::new(AtomicBool::new(false)),
        };
        assert!(outbound.send(vec![1, 2, 3]).is_ok());
        assert!(!outbound.overflowed());
        assert_eq!(outbound.send(vec![4, 5, 6]), Err(OutboundBusy::Full));
        assert!(
            outbound.overflowed(),
            "an overflow latches the flag the acceptor closes on"
        );
    }

    #[test]
    fn test_outbound_mailbox_closed_surfaces_typed_closed() {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(4);
        drop(rx);
        let outbound = SessionOutbound {
            tx,
            overflowed: Arc::new(AtomicBool::new(false)),
        };
        assert_eq!(outbound.send(vec![0]), Err(OutboundBusy::Closed));
    }

    #[test]
    fn test_message_type_str_is_the_msgtype_not_a_payload() {
        let header = StandardHeader::new(
            CompId::new("CLIENT").expect("comp"),
            CompId::new("VENUE").expect("comp"),
            SeqNum::new(1),
            UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("ts"),
        );
        let logon = DecodedMessage::Logon(session::Logon {
            header,
            heart_bt_int: 30,
            username: "trader".to_string(),
            password: session::SecretField::new("s3cr3t-should-never-log"),
            reset_seq_num_flag: None,
        });
        // The logged identifier is the MsgType char, never the username/password.
        assert_eq!(message_type_str(&logon), "A");
    }

    #[test]
    fn test_stub_dispatch_logs_no_credential() {
        // Capture tracing on THIS thread while dispatching a Logon (bearing a
        // sentinel password) through the real decode + stub session, then grep the
        // captured output: the credential must never appear.
        use std::io::Write;
        use std::sync::Mutex;

        // A `Fn() -> impl Write` is itself a `MakeWriter`, so no trait impl is
        // needed — the closure hands each layer a writer appending to the buffer.
        struct SharedWriter(Arc<Mutex<Vec<u8>>>);
        impl Write for SharedWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .expect("log buffer lock")
                    .extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        const SENTINEL_PASSWORD: &str = "SENTINEL-PW-DO-NOT-LOG-a1b2c3";
        let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
        let make_writer = {
            let buffer = Arc::clone(&buffer);
            move || SharedWriter(Arc::clone(&buffer))
        };
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(make_writer)
            .with_ansi(false)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            // A Logon frame carrying the sentinel password, framed on the wire.
            let logon = DecodedMessage::Logon(session::Logon {
                header: StandardHeader::new(
                    CompId::new("CLIENT").expect("comp"),
                    CompId::new("VENUE").expect("comp"),
                    SeqNum::new(1),
                    UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("ts"),
                ),
                heart_bt_int: 30,
                username: "trader".to_string(),
                password: session::SecretField::new(SENTINEL_PASSWORD),
                reset_seq_num_flag: None,
            });
            let frame = logon.encode().expect("test encode");
            // The frame really does carry the password on the wire...
            assert!(
                String::from_utf8_lossy(&frame).contains(SENTINEL_PASSWORD),
                "the wire frame carries the credential (the thing that must not be logged)"
            );
            // ...and running the real framing + decode + stub dispatch must not log it.
            let mut decoder = BoundedFrameDecoder::new(64 * 1024);
            let mut buf = BytesMut::from(&frame[..]);
            let framed = match decoder.decode(&mut buf) {
                Ok(Some(framed)) => framed,
                other => panic!("expected a framed logon, got {other:?}"),
            };
            let (tx, _rx) = mpsc::channel::<Vec<u8>>(4);
            let outbound = SessionOutbound {
                tx,
                overflowed: Arc::new(AtomicBool::new(false)),
            };
            let mut stub = StubSession {
                peer: "127.0.0.1:5000".parse().expect("peer"),
            };
            // `dispatch` is async (the #038 seam); a current-thread runtime drives it
            // to completion ON this thread, so its tracing events are captured by the
            // thread-local subscriber above.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("current-thread runtime");
            runtime.block_on(dispatch(&mut stub, &framed, &outbound));
        });

        let captured =
            String::from_utf8_lossy(&buffer.lock().expect("log buffer lock")).to_string();
        assert!(
            captured.contains("msg_type=\"A\"") || captured.contains("msg_type=A"),
            "the stub logs the MsgType (A); captured: {captured}"
        );
        assert!(
            !captured.contains(SENTINEL_PASSWORD),
            "no credential byte may reach tracing output; captured: {captured}"
        );
    }
}
