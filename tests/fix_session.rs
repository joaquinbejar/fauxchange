//! #038 conformance + security: the acceptor-side FIX 4.4 session layer.
//!
//! These tests drive the **real** [`VenueFixSessionFactory`] over an ephemeral TCP
//! socket against a serving [`AppState`] whose registry holds provisioned FIX
//! accounts (username + Argon2id password + immutable `(SenderCompID,
//! TargetCompID)` binding), exercising the whole logon → auth → bind → active flow
//! and the security-critical rejections end to end:
//!
//! - a valid logon authenticates and reaches `Active` with a credential-free ack;
//! - a CompID tuple bound to a *different* account (or an unbound tuple) is rejected
//!   at logon (`Logout 5`), never reaching `Active` ([ADR-0010](../docs/adr/0010-fix-session-account-binding.md));
//! - a `Read` logon is refused order entry order-level ([ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md));
//! - revocation drops the live session and refuses future logons;
//! - counters + resend store are account-keyed and resume across a reconnect;
//! - a logon rate limit refuses a flooding peer;
//! - no password ever appears in a captured log.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fauxchange::auth::{AccountProvision, AccountStore, CompIdBinding, RateLimitKey};
use fauxchange::exchange::Hash32;
use fauxchange::gateway::fix::{
    FixAcceptor, FixAcceptorConfig, FixSessionStore, InMemoryFixSessionStore, SessionConfig,
    VenueFixSessionFactory,
};
use fauxchange::models::{AccountId, Permission};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

const VENUE: &str = "FAUXCHANGE";
const TRADER_USER: &str = "trader-fix";
const TRADER_PW: &str = "trader-plaintext-pw-DoNotLog-777";
const TRADER_SENDER: &str = "TRADERCLIENT";
const READER_USER: &str = "reader-fix";
const READER_PW: &str = "reader-plaintext-pw-DoNotLog-888";
const READER_SENDER: &str = "READERCLIENT";

// ============================================================================
// Harness
// ============================================================================

struct Harness {
    addr: SocketAddr,
    state: Arc<AppState>,
    store: Arc<dyn FixSessionStore>,
    shutdown: watch::Sender<bool>,
}

impl Harness {
    async fn start() -> Self {
        let accounts = vec![
            AccountProvision::new(
                AccountId::new("trader-1"),
                Hash32([2; 32]),
                vec![Permission::Trade],
            )
            .with_fix_login(TRADER_USER, TRADER_PW)
            .with_comp_ids(CompIdBinding {
                sender_comp_id: TRADER_SENDER.to_string(),
                target_comp_id: VENUE.to_string(),
            }),
            AccountProvision::new(
                AccountId::new("reader-1"),
                Hash32([4; 32]),
                vec![Permission::Read],
            )
            .with_fix_login(READER_USER, READER_PW)
            .with_comp_ids(CompIdBinding {
                sender_comp_id: READER_SENDER.to_string(),
                target_comp_id: VENUE.to_string(),
            }),
        ];
        Self::start_with(accounts, u32::MAX).await
    }

    async fn start_with(accounts: Vec<AccountProvision>, rate_limit: u32) -> Self {
        let auth = AuthConfig::dev()
            .expect("dev auth")
            .with_bootstrap_secret("boot-secret")
            .with_rate_limit(rate_limit)
            .with_accounts(accounts);
        let state = AppState::new(
            AppStateConfig::new(["BTC"])
                .with_serving(true)
                .with_auth(auth),
        )
        .expect("AppState");

        let config = FixAcceptorConfig {
            addr: "127.0.0.1:0".parse().expect("addr"),
            connection_cap: 16,
            mailbox_depth: 64,
            max_frame_bytes: 64 * 1024,
            idle_timeout: Duration::from_secs(30),
        };
        let acceptor = FixAcceptor::bind(config).await.expect("bind");
        let addr = acceptor.local_addr();
        let store: Arc<dyn FixSessionStore> = Arc::new(InMemoryFixSessionStore::new());
        let factory = Arc::new(VenueFixSessionFactory::new(
            Arc::clone(&state),
            Arc::clone(&store),
            SessionConfig {
                logon_timeout_ms: 10_000,
                max_heart_bt_int_secs: 60,
            },
        ));
        let (shutdown, shutdown_rx) = watch::channel(false);
        tokio::spawn(acceptor.serve(factory, shutdown_rx));
        Self {
            addr,
            state,
            store,
            shutdown,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

// ============================================================================
// Wire helpers
// ============================================================================

fn frame_with_body(body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
    msg.extend_from_slice(body);
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
    msg
}

#[allow(clippy::too_many_arguments)]
fn logon_frame(sender: &str, target: &str, seq: u64, user: &str, pw: &str, reset: bool) -> Vec<u8> {
    let reset_field = if reset { "141=Y\x01" } else { "" };
    let body = format!(
        "35=A\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x0198=0\x01108=30\x01553={user}\x01554={pw}\x01{reset_field}"
    );
    frame_with_body(body.as_bytes())
}

fn heartbeat_frame(sender: &str, target: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=0\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
    frame_with_body(body.as_bytes())
}

fn new_order_frame(sender: &str, target: &str, seq: u64) -> Vec<u8> {
    let body = format!(
        "35=D\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x0111=cl-1\x0155=BTC-20240329-50000-C\x0154=1\x0160=20240329-12:00:00.000\x0140=1\x0138=1\x01"
    );
    frame_with_body(body.as_bytes())
}

/// Splits a byte buffer into complete FIX frames (locating each `8=FIX.4.4`,
/// parsing its `BodyLength (9)`, and cutting `body + 7`-byte checksum trailer).
fn split_frames(buf: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let needle = b"8=FIX.4.4\x01";
    let mut pos = 0;
    while pos < buf.len() {
        let Some(rel) = buf[pos..].windows(needle.len()).position(|w| w == needle) else {
            break;
        };
        let start = pos + rel;
        // Find `9=` then its value up to SOH.
        let after_begin = start + needle.len();
        let Some(bl_prefix) = buf[after_begin..].windows(2).position(|w| w == b"9=") else {
            break;
        };
        let digits_start = after_begin + bl_prefix + 2;
        let Some(soh_rel) = buf[digits_start..].iter().position(|&b| b == 0x01) else {
            break;
        };
        let Ok(body_len) = std::str::from_utf8(&buf[digits_start..digits_start + soh_rel])
            .unwrap_or("")
            .parse::<usize>()
        else {
            break;
        };
        let body_start = digits_start + soh_rel + 1;
        let total_end = body_start + body_len + 7; // `10=NNN\x01`
        if total_end > buf.len() {
            break;
        }
        frames.push(buf[start..total_end].to_vec());
        pos = total_end;
    }
    frames
}

/// Reads from `stream` until at least one complete frame is available (or a
/// timeout), returning the raw frames. Raw (not decoded), because the venue's
/// credential-free `Logon (A)` ack deliberately omits `Username (553)` /
/// `Password (554)` — which the inbound-tuned [`fauxchange::gateway::fix::decode`]
/// treats as required — so an outbound-frame test inspects fields by tag.
async fn recv_frames(stream: &mut TcpStream, timeout: Duration) -> Vec<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut buf = Vec::new();
    let mut scratch = [0u8; 4096];
    while tokio::time::Instant::now() < deadline {
        let read =
            tokio::time::timeout(Duration::from_millis(200), stream.read(&mut scratch)).await;
        match read {
            Ok(Ok(0)) => break, // EOF
            Ok(Ok(n)) => {
                buf.extend_from_slice(&scratch[..n]);
                let frames = split_frames(&buf);
                if !frames.is_empty() {
                    return frames;
                }
            }
            Ok(Err(_)) => break,
            Err(_elapsed) => {
                let frames = split_frames(&buf);
                if !frames.is_empty() {
                    return frames;
                }
            }
        }
    }
    split_frames(&buf)
}

/// The value of scalar `tag` in a FIX frame (`SOH`-delimited `tag=value`).
fn field(frame: &[u8], tag: &str) -> Option<String> {
    let text = String::from_utf8_lossy(frame);
    text.split('\u{1}')
        .find_map(|part| part.strip_prefix(&format!("{tag}=")).map(str::to_string))
}

/// The `MsgType (35)` of a frame.
fn msg_type(frame: &[u8]) -> Option<String> {
    field(frame, "35")
}

/// Whether any frame in `frames` has `MsgType (35) == ty`.
fn any_msg_type(frames: &[Vec<u8>], ty: &str) -> bool {
    frames.iter().any(|f| msg_type(f).as_deref() == Some(ty))
}

async fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.expect("connect")
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_valid_logon_reaches_active_with_a_credential_free_ack() {
    let harness = Harness::start().await;
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("send logon");
    let replies = recv_frames(&mut client, Duration::from_secs(3)).await;
    let ack = replies.first().expect("a Logon ack");
    assert_eq!(
        msg_type(ack).as_deref(),
        Some("A"),
        "the venue acks with a Logon"
    );
    assert_eq!(
        field(ack, "108").as_deref(),
        Some("30"),
        "HeartBtInt echoed"
    );
    // The ack addresses the client and carries NO credential.
    assert_eq!(field(ack, "49").as_deref(), Some(VENUE));
    assert_eq!(field(ack, "56").as_deref(), Some(TRADER_SENDER));
    assert!(
        field(ack, "554").is_none(),
        "the ack must not carry a Password(554)"
    );
    assert!(
        field(ack, "553").is_none(),
        "the ack must not carry a Username(553)"
    );
}

#[tokio::test]
async fn test_logon_with_wrong_password_is_rejected() {
    let harness = Harness::start().await;
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            "WRONG-PW",
            false,
        ))
        .await
        .expect("send logon");
    let replies = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&replies, "5"),
        "a wrong password is a Logout(5)"
    );
}

#[tokio::test]
async fn test_logon_presenting_a_tuple_bound_to_another_account_is_rejected() {
    // trader-1's credential but reader-1's bound SenderCompID → binding violation.
    let harness = Harness::start().await;
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            READER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("send logon");
    let replies = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&replies, "5"),
        "a cross-account CompID tuple is a Logout(5)"
    );
}

#[tokio::test]
async fn test_read_logon_is_refused_order_entry_order_level() {
    let harness = Harness::start().await;
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            READER_SENDER,
            VENUE,
            1,
            READER_USER,
            READER_PW,
            false,
        ))
        .await
        .expect("send logon");
    let logon_reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&logon_reply, "A"),
        "the Read logon is admitted"
    );

    // A Read session sends a NewOrderSingle → order-level ExecutionReport Rejected.
    client
        .write_all(&new_order_frame(READER_SENDER, VENUE, 2))
        .await
        .expect("send order");
    let order_reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    let report = order_reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("8"))
        .expect("an order-level ExecutionReport");
    // ExecType(150)=8 (Rejected), OrdStatus(39)=8 (Rejected).
    assert_eq!(field(report, "150").as_deref(), Some("8"));
    assert_eq!(field(report, "39").as_deref(), Some("8"));
}

#[tokio::test]
async fn test_trade_logon_admits_order_at_the_session_boundary() {
    let harness = Harness::start().await;
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    let _ = recv_frames(&mut client, Duration::from_secs(3)).await;
    // A Trade session's order is admitted (routing lands in #039), so no reject.
    client
        .write_all(&new_order_frame(TRADER_SENDER, VENUE, 2))
        .await
        .expect("order");
    let reply = recv_frames(&mut client, Duration::from_millis(600)).await;
    assert!(
        !any_msg_type(&reply, "8") && !any_msg_type(&reply, "3"),
        "a permitted order is not rejected at the session boundary, got {reply:?}"
    );
}

#[tokio::test]
async fn test_revocation_drops_the_live_session_and_refuses_future_logons() {
    let harness = Harness::start().await;
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    let logon_reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(any_msg_type(&logon_reply, "A"), "the logon is admitted");

    // Revoke the account: the live session is dropped on its next message/tick.
    let epoch = AccountStore::revoke(harness.state.accounts(), &AccountId::new("trader-1"));
    assert_eq!(epoch, Some(1));
    client
        .write_all(&heartbeat_frame(TRADER_SENDER, VENUE, 2))
        .await
        .expect("heartbeat");
    let dropped = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&dropped, "5"),
        "a revoked live session is logged out(5)"
    );

    // A fresh logon for the revoked account is refused.
    let mut second = connect(harness.addr).await;
    second
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    let refused = recv_frames(&mut second, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&refused, "5"),
        "a revoked account cannot log in again"
    );
}

#[tokio::test]
async fn test_reconnect_resumes_the_sender_sequence_from_the_durable_store() {
    let harness = Harness::start().await;
    // First session: logon (ack seq 1) + a TestRequest reply (Heartbeat seq 2).
    let mut first = connect(harness.addr).await;
    first
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    let _ = recv_frames(&mut first, Duration::from_secs(3)).await;
    let test_req = format!(
        "35=1\x0149={TRADER_SENDER}\x0156={VENUE}\x0134=2\x0152=20240329-12:00:00.000\x01112=PING\x01"
    );
    first
        .write_all(&frame_with_body(test_req.as_bytes()))
        .await
        .expect("test request");
    let _ = recv_frames(&mut first, Duration::from_secs(3)).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reconnect (no reset): the ack's MsgSeqNum resumes past 1.
    let mut second = connect(harness.addr).await;
    second
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            3,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    let replies = recv_frames(&mut second, Duration::from_secs(3)).await;
    let ack = replies
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("A"))
        .expect("a resumed Logon ack");
    let seq: u64 = field(ack, "34").and_then(|v| v.parse().ok()).expect("seq");
    assert!(seq > 1, "reconnect resumes numbering; ack seq was {seq}");
}

#[tokio::test]
async fn test_reset_seq_num_flag_resets_and_journals_a_session_event() {
    let harness = Harness::start().await;
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            true,
        ))
        .await
        .expect("logon");
    let replies = recv_frames(&mut client, Duration::from_secs(3)).await;
    let ack = replies
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("A"))
        .expect("a reset Logon ack");
    assert_eq!(
        field(ack, "34").as_deref(),
        Some("1"),
        "reset makes the ack seq 1"
    );
    assert_eq!(
        field(ack, "141").as_deref(),
        Some("Y"),
        "the ack echoes ResetSeqNumFlag"
    );
    // The reset is durably journaled as a session event (survives reconnect).
    let key =
        fauxchange::gateway::fix::SessionKey::new(AccountId::new("trader-1"), TRADER_SENDER, VENUE);
    let events = harness.store.reset_events(&key).expect("reset events");
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn test_logon_rate_limit_refuses_a_flooding_peer() {
    // A dedicated low-limit venue so the peer budget drains quickly.
    let accounts = vec![
        AccountProvision::new(
            AccountId::new("trader-1"),
            Hash32([2; 32]),
            vec![Permission::Trade],
        )
        .with_fix_login(TRADER_USER, TRADER_PW)
        .with_comp_ids(CompIdBinding {
            sender_comp_id: TRADER_SENDER.to_string(),
            target_comp_id: VENUE.to_string(),
        }),
    ];
    let harness = Harness::start_with(accounts, 3).await;
    // Pre-drain the peer (127.0.0.1) budget directly on the shared limiter.
    let peer: IpAddr = "127.0.0.1".parse().expect("ip");
    let limiter = harness.state.auth().rate_limiter();
    for _ in 0..3 {
        let _ = limiter.check_and_record_status(&RateLimitKey::Peer(peer));
    }
    // The next logon is over budget → refused with a Logout.
    let mut client = connect(harness.addr).await;
    client
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    let replies = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&replies, "5"),
        "a rate-limited logon is refused with a Logout(5)"
    );
}

/// A `tracing` writer that captures every emitted byte into a shared buffer.
#[derive(Clone)]
struct CaptureBuffer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_no_password_appears_in_a_captured_log_over_a_logon_flow() {
    let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer({
            let buffer = Arc::clone(&buffer);
            move || CaptureBuffer(Arc::clone(&buffer))
        })
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let harness = Harness::start().await;
    // A good logon, a wrong-password logon, and a binding violation — every branch
    // that touches the credential.
    for (sender, user, pw) in [
        (TRADER_SENDER, TRADER_USER, TRADER_PW),
        (TRADER_SENDER, TRADER_USER, "another-wrong-secret-DoNotLog"),
        (READER_SENDER, TRADER_USER, TRADER_PW),
    ] {
        let mut client = connect(harness.addr).await;
        let _ = client
            .write_all(&logon_frame(sender, VENUE, 1, user, pw, false))
            .await;
        let _ = recv_frames(&mut client, Duration::from_secs(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let captured = String::from_utf8_lossy(&buffer.lock().expect("lock")).to_string();
    assert!(
        !captured.contains(TRADER_PW),
        "the plaintext password must never be logged"
    );
    assert!(
        !captured.contains("another-wrong-secret-DoNotLog"),
        "a wrong password must never be logged"
    );
}
