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
use fauxchange::exchange::{ExecutionsStore, Hash32};
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

/// A `NewOrderSingle (D)` limit order (`side` `1`=Buy/`2`=Sell, decimal `price`).
#[allow(clippy::too_many_arguments)]
fn limit_order_frame(
    sender: &str,
    target: &str,
    seq: u64,
    cl_ord_id: &str,
    side: &str,
    price: &str,
    qty: u64,
) -> Vec<u8> {
    let body = format!(
        "35=D\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0154={side}\x0160=20240329-12:00:00.000\x0140=2\x0144={price}\x0138={qty}\x0159=1\x01"
    );
    frame_with_body(body.as_bytes())
}

/// An `OrderCancelRequest (F)` referencing `orig_cl_ord_id`.
fn cancel_frame(
    sender: &str,
    target: &str,
    seq: u64,
    orig_cl_ord_id: &str,
    cl_ord_id: &str,
) -> Vec<u8> {
    let body = format!(
        "35=F\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x0141={orig_cl_ord_id}\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0154=1\x01"
    );
    frame_with_body(body.as_bytes())
}

/// A well-formed application message with an unsupported `MsgType` (`R`,
/// QuoteRequest — recognised by FIX 4.4, unhandled by the venue dialect).
fn unsupported_app_frame(sender: &str, target: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=R\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
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

/// Logs a trader in and drains the credential-free `Logon (A)` ack.
async fn logon_trader(addr: SocketAddr) -> TcpStream {
    let mut client = connect(addr).await;
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
    let ack = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(any_msg_type(&ack, "A"), "the trader logon is admitted");
    client
}

#[tokio::test]
async fn test_new_order_single_market_against_empty_book_reports_new_then_canceled() {
    // The #038 "admitted at the session boundary" no-op is now the #039 order path:
    // a permitted market order against an empty book is ACCEPTED (New) then its
    // remainder killed (Canceled) — never a bare session Reject(3), and never a
    // Rejected(8) (the order was valid, just unfillable).
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    client
        .write_all(&new_order_frame(TRADER_SENDER, VENUE, 2))
        .await
        .expect("order");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        !any_msg_type(&reply, "3"),
        "an application order is never a session Reject(3), got {reply:?}"
    );
    let reports: Vec<&Vec<u8>> = reply
        .iter()
        .filter(|f| msg_type(f).as_deref() == Some("8"))
        .collect();
    assert!(!reports.is_empty(), "the order path emits ExecutionReports");
    // New (ExecType 150=0) on accept, Canceled (150=4) for the killed remainder.
    assert_eq!(
        field(reports[0], "150").as_deref(),
        Some("0"),
        "New on accept"
    );
    assert!(
        reports
            .iter()
            .any(|f| field(f, "150").as_deref() == Some("4")),
        "the unfilled market remainder is Canceled"
    );
    assert!(
        !reports
            .iter()
            .any(|f| field(f, "150").as_deref() == Some("8")),
        "an accepted-but-unfillable order is not Rejected"
    );
}

#[tokio::test]
async fn test_resting_limit_order_reports_new() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // A non-crossing limit rests → a single New report, LeavesQty = the order qty.
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "rest-1",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("order");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    let report = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("8"))
        .expect("an ExecutionReport");
    assert_eq!(field(report, "150").as_deref(), Some("0"), "ExecType New");
    assert_eq!(field(report, "39").as_deref(), Some("0"), "OrdStatus New");
    assert_eq!(field(report, "151").as_deref(), Some("3"), "LeavesQty = 3");
    assert_eq!(field(report, "14").as_deref(), Some("0"), "CumQty = 0");
}

#[tokio::test]
async fn test_crossing_order_reports_trade_with_join_keys_and_commission() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // A resting sell, then a crossing buy (STP is None, so a same-account cross
    // fills) — the taker reports a Trade with the cross-surface join keys.
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "maker-1",
            "2",
            "500.00",
            3,
        ))
        .await
        .expect("maker");
    let _ = recv_frames(&mut client, Duration::from_secs(3)).await;
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            3,
            "taker-1",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("taker");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    let trade = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("F"))
        .expect("a Trade ExecutionReport");
    // ExecType=Trade(F), OrdStatus=Filled(2), the full fill.
    assert_eq!(field(trade, "39").as_deref(), Some("2"), "OrdStatus Filled");
    assert_eq!(field(trade, "32").as_deref(), Some("3"), "LastQty = 3");
    assert_eq!(field(trade, "31").as_deref(), Some("500.00"), "LastPx");
    assert_eq!(field(trade, "14").as_deref(), Some("3"), "CumQty = 3");
    assert_eq!(field(trade, "151").as_deref(), Some("0"), "LeavesQty = 0");
    // SecondaryExecID(527) = the underlying_sequence (the cross-surface join key);
    // it is present and numeric.
    let secondary = field(trade, "527").expect("SecondaryExecID(527)");
    assert!(
        secondary.parse::<u64>().is_ok(),
        "527 is numeric: {secondary}"
    );
    // The per-leg fee rides Commission(12) + CommType(13)=3, LastLiquidityInd(851).
    assert!(field(trade, "12").is_some(), "Commission(12) present");
    assert_eq!(
        field(trade, "13").as_deref(),
        Some("3"),
        "CommType absolute"
    );
    assert_eq!(
        field(trade, "851").as_deref(),
        Some("2"),
        "LastLiquidityInd = Taker"
    );
}

#[tokio::test]
async fn test_cancel_of_unknown_order_is_order_cancel_reject_9() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // A cancel referencing an OrigClOrdID the session never placed → 9, not a
    // bare Reject(3), with CxlRejReason(102)=1 (Unknown order) and
    // CxlRejResponseTo(434)=1 (Order Cancel Request).
    client
        .write_all(&cancel_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "never-placed",
            "cxl-1",
        ))
        .await
        .expect("cancel");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        !any_msg_type(&reply, "3"),
        "a cancel failure is never a session Reject(3), got {reply:?}"
    );
    let reject = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("9"))
        .expect("an OrderCancelReject(9)");
    assert_eq!(field(reject, "102").as_deref(), Some("1"), "Unknown order");
    assert_eq!(
        field(reject, "434").as_deref(),
        Some("1"),
        "CxlRejResponseTo = Order Cancel Request"
    );
    assert_eq!(
        field(reject, "41").as_deref(),
        Some("never-placed"),
        "the OrigClOrdID is echoed"
    );
}

#[tokio::test]
async fn test_unsupported_application_message_is_business_message_reject_j() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // A well-formed application MsgType the venue has no handler for (R,
    // QuoteRequest) → BusinessMessageReject(j), never a bare session Reject(3).
    client
        .write_all(&unsupported_app_frame(TRADER_SENDER, VENUE, 2))
        .await
        .expect("unsupported");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    let reject = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("j"))
        .unwrap_or_else(|| panic!("expected a BusinessMessageReject(j), got {reply:?}"));
    // BusinessRejectReason(380)=3 (Unsupported Message Type), RefMsgType(372)=R.
    assert_eq!(field(reject, "380").as_deref(), Some("3"));
    assert_eq!(field(reject, "372").as_deref(), Some("R"));
}

#[tokio::test]
async fn test_malformed_frame_is_a_session_reject_3() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // A structurally-invalid order (a NewOrderSingle missing the required Side(54))
    // is a session-level Reject(3) with a RefTagID — NOT an order-level 8/9 (a
    // malformed field is a session-protocol failure, fix-dialect §5).
    let body = format!(
        "35=D\x0149={TRADER_SENDER}\x0156={VENUE}\x0134=2\x0152=20240329-12:00:00.000\x0111=nodside\x0155=BTC-20240329-50000-C\x0160=20240329-12:00:00.000\x0140=2\x0144=500.00\x0138=1\x0159=1\x01"
    );
    client
        .write_all(&frame_with_body(body.as_bytes()))
        .await
        .expect("bad order");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&reply, "3"),
        "a malformed order field is a session Reject(3), got {reply:?}"
    );
}

#[tokio::test]
async fn test_idempotent_resend_of_the_same_clordid_does_not_open_a_second_order() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // A resting maker sell of 4, then a taker buy of 2 (ClOrdID "dup") that fills 2
    // — recording exactly two execution legs (maker + taker).
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "maker-4",
            "2",
            "500.00",
            4,
        ))
        .await
        .expect("maker");
    let _ = recv_frames(&mut client, Duration::from_secs(3)).await;
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            3,
            "dup",
            "1",
            "500.00",
            2,
        ))
        .await
        .expect("taker");
    let _ = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert_eq!(
        harness.state.executions().len(),
        2,
        "the first crossing records two legs (maker + taker)"
    );

    // Resend the byte-identical taker (same ClOrdID + payload): the executor
    // deduplicates on (account, ClOrdID), so it returns the stored terminal
    // outcome WITHOUT opening a second order — no second fill against the maker's
    // remaining 2, so the executions store still holds exactly two legs.
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            4,
            "dup",
            "1",
            "500.00",
            2,
        ))
        .await
        .expect("resend");
    let _ = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert_eq!(
        harness.state.executions().len(),
        2,
        "a resent D with the same ClOrdID does not open a second order (idempotent)"
    );
}

#[tokio::test]
async fn test_idempotent_resend_after_fill_re_renders_real_state_and_keeps_correlation() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // Maker sell 2 @ 500, taker buy 3 @ 500 (ClOrdID "dup") → fills 2, rests 1.
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "maker-p",
            "2",
            "500.00",
            2,
        ))
        .await
        .expect("maker");
    let _ = recv_frames(&mut client, Duration::from_secs(3)).await;
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            3,
            "dup",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("taker");
    let first = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        first
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("F")),
        "the first D partially fills (a Trade), got {first:?}"
    );
    assert_eq!(harness.state.executions().len(), 2);

    // Resend the byte-identical taker (same ClOrdID "dup", NEW MsgSeqNum 4 — the
    // standard retry after a dropped ack; the transport dup-seq guard does not
    // catch it). It must re-render the REAL order's state, not a fabricated New.
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            4,
            "dup",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("resend");
    let resend = recv_frames(&mut client, Duration::from_secs(3)).await;
    let report = resend
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("8"))
        .unwrap_or_else(|| panic!("expected a status ExecutionReport, got {resend:?}"));
    // (a) The resend re-renders the REAL partially-filled state (CumQty=2), NEVER a
    // fabricated New/CumQty=0.
    assert_eq!(
        field(report, "39").as_deref(),
        Some("1"),
        "resend shows PartiallyFilled, not a fabricated New"
    );
    assert_eq!(
        field(report, "14").as_deref(),
        Some("2"),
        "resend CumQty is the real 2, not 0"
    );
    assert_ne!(
        field(report, "150").as_deref(),
        Some("0"),
        "resend must not be a fabricated ExecType=New"
    );
    // (d) No second order/command: still exactly two execution legs, no phantom fill.
    assert_eq!(
        harness.state.executions().len(),
        2,
        "the resend opened no second order (no phantom submit)"
    );

    // (b)+(c) The correlation still points at the REAL order: a cancel of "dup"
    // targets it (8 Canceled), never a lost-correlation OrderCancelReject(9).
    client
        .write_all(&cancel_frame(TRADER_SENDER, VENUE, 5, "dup", "cxl-dup"))
        .await
        .expect("cancel");
    let cancel_reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        cancel_reply
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("4")),
        "the cancel of the resent ClOrdID targets the real order (8 Canceled), got {cancel_reply:?}"
    );
    assert!(
        !any_msg_type(&cancel_reply, "9"),
        "the resend did not lose the real correlation"
    );
}

#[tokio::test]
async fn test_conflicting_clordid_reuse_is_rejected_duplicate_order() {
    let harness = Harness::start().await;
    let mut client = logon_trader(harness.addr).await;
    // Place a resting order, then reuse its ClOrdID with DIFFERENT economics
    // (a different quantity) — a conflicting reuse, rejected 8/Duplicate Order,
    // never overwriting the real correlation.
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "reused",
            "1",
            "400.00",
            3,
        ))
        .await
        .expect("first");
    let _ = recv_frames(&mut client, Duration::from_secs(3)).await;
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            3,
            "reused",
            "1",
            "400.00",
            7, // different quantity → conflicting payload for the same ClOrdID
        ))
        .await
        .expect("conflict");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    let reject = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("8"))
        .unwrap_or_else(|| panic!("expected an 8 Rejected, got {reply:?}"));
    // OrdRejReason(103)=6 (Duplicate Order); no phantom submit occurred.
    assert_eq!(
        field(reject, "103").as_deref(),
        Some("6"),
        "Duplicate Order"
    );
    assert!(
        harness.state.executions().is_empty(),
        "a resting-order conflict records no fills and no phantom order"
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

// ============================================================================
// #96/#112 Bug 2 — per-SessionKey exclusivity lease (concurrent-logon race)
// ============================================================================

/// A `TestRequest (1)` at `seq` carrying `TestReqID (112) = id` — used to prove a
/// live session still answers with a `Heartbeat (0)`.
fn test_request_frame(sender: &str, target: &str, seq: u64, id: &str) -> Vec<u8> {
    let body = format!(
        "35=1\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x01112={id}\x01"
    );
    frame_with_body(body.as_bytes())
}

#[tokio::test]
async fn test_second_concurrent_logon_for_the_same_key_is_refused_while_first_stays_active() {
    let harness = Harness::start().await;
    // The first session logs on and is admitted; its socket stays open (live).
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
        .expect("first logon");
    assert!(
        any_msg_type(&recv_frames(&mut first, Duration::from_secs(3)).await, "A"),
        "the first session is admitted"
    );

    // A SECOND connection presenting the SAME account + CompID tuple logs on
    // concurrently — the lease is held by the first, so it is refused with a Logout
    // and NEVER admitted.
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
        .expect("second logon");
    let second_reply = recv_frames(&mut second, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&second_reply, "5"),
        "a concurrent second logon for the same key is a Logout(5), got {second_reply:?}"
    );
    assert!(
        !any_msg_type(&second_reply, "A"),
        "the second concurrent logon is never admitted"
    );

    // The FIRST session is still alive and serving: it answers a TestRequest with a
    // Heartbeat (the lease did not disturb it).
    first
        .write_all(&test_request_frame(TRADER_SENDER, VENUE, 2, "PING"))
        .await
        .expect("test request");
    let hb = recv_frames(&mut first, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&hb, "0"),
        "the first session stays active and answers the TestRequest with a Heartbeat(0), got {hb:?}"
    );
}

#[tokio::test]
async fn test_session_lease_is_released_on_disconnect_so_a_later_logon_succeeds() {
    let harness = Harness::start().await;
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
        .expect("first logon");
    assert!(
        any_msg_type(&recv_frames(&mut first, Duration::from_secs(3)).await, "A"),
        "the first session is admitted"
    );
    // Abrupt disconnect — the RAII lease guard must release the key on teardown.
    drop(first);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A later logon for the SAME key is admitted (the lease was released). Presenting
    // seq 2 (the stored inbound expectation after the first logon consumed seq 1) so
    // this exercises the lease, not the stale-seq guard.
    let mut second = connect(harness.addr).await;
    second
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            2,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("second logon");
    let reply = recv_frames(&mut second, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&reply, "A"),
        "the lease released on disconnect lets the key re-admit, got {reply:?}"
    );
}

#[tokio::test]
async fn test_two_different_keys_hold_independent_leases() {
    let harness = Harness::start().await;
    // trader-1 and reader-1 are distinct accounts with distinct CompID tuples.
    let mut trader = connect(harness.addr).await;
    trader
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("trader logon");
    assert!(
        any_msg_type(&recv_frames(&mut trader, Duration::from_secs(3)).await, "A"),
        "the trader session is admitted"
    );
    // The reader logs on CONCURRENTLY under a different key — no lease conflict.
    let mut reader = connect(harness.addr).await;
    reader
        .write_all(&logon_frame(
            READER_SENDER,
            VENUE,
            1,
            READER_USER,
            READER_PW,
            false,
        ))
        .await
        .expect("reader logon");
    assert!(
        any_msg_type(&recv_frames(&mut reader, Duration::from_secs(3)).await, "A"),
        "a different key logs on concurrently, unaffected by the trader's lease"
    );
}

// ============================================================================
// #96/#112 Bug 1 — reconnect MsgSeqNum validation (replay of consumed messages)
// ============================================================================

#[tokio::test]
async fn test_reconnect_at_a_stale_seq_without_reset_is_refused_no_replay() {
    let harness = Harness::start().await;
    // The first session advances the inbound expectation to 3 (logon@1 consumed →
    // 2, then a TestRequest@2 consumed → 3).
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
    first
        .write_all(&test_request_frame(TRADER_SENDER, VENUE, 2, "PING"))
        .await
        .expect("test request");
    let _ = recv_frames(&mut first, Duration::from_secs(3)).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reconnect presenting a STALE seq 1 (below the stored expectation of 3) without
    // ResetSeqNumFlag → refused with a Logout, never admitted: a backward jump would
    // replay already-consumed messages.
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
        .expect("stale logon");
    let reply = recv_frames(&mut second, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&reply, "5"),
        "a stale-seq reconnect is a Logout(5), got {reply:?}"
    );
    assert!(
        !any_msg_type(&reply, "A"),
        "a stale-seq reconnect is never admitted"
    );
}

#[tokio::test]
async fn test_reconnect_with_reset_seq_num_flag_at_seq_one_is_admitted() {
    let harness = Harness::start().await;
    // Advance the inbound expectation past 1 on a first session, then disconnect.
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
    first
        .write_all(&test_request_frame(TRADER_SENDER, VENUE, 2, "PING"))
        .await
        .expect("test request");
    let _ = recv_frames(&mut first, Duration::from_secs(3)).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A ResetSeqNumFlag=Y reconnect at seq 1 legitimately resets and IS admitted —
    // the only sanctioned backward path (a stale seq 1 without the flag is refused,
    // per test_reconnect_at_a_stale_seq_without_reset_is_refused_no_replay).
    let mut second = connect(harness.addr).await;
    second
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            1,
            TRADER_USER,
            TRADER_PW,
            true,
        ))
        .await
        .expect("reset logon");
    let reply = recv_frames(&mut second, Duration::from_secs(3)).await;
    let ack = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("A"))
        .expect("a reset Logon ack");
    assert_eq!(
        field(ack, "34").as_deref(),
        Some("1"),
        "ResetSeqNumFlag=Y makes the ack seq 1"
    );
    assert_eq!(
        field(ack, "141").as_deref(),
        Some("Y"),
        "the ack echoes ResetSeqNumFlag"
    );
}
