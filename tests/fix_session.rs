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
const NOPERM_USER: &str = "noperm-fix";
const NOPERM_PW: &str = "noperm-plaintext-pw-DoNotLog-999";
const NOPERM_SENDER: &str = "NOPERMCLIENT";

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
            // An authenticated account with an EMPTY permission set — it logs on but
            // holds neither Read nor Trade, so its `V` is refused in the market-data
            // context (a `Y`, MDReqRejReason = 3).
            AccountProvision::new(AccountId::new("noperm-1"), Hash32([6; 32]), Vec::new())
                .with_fix_login(NOPERM_USER, NOPERM_PW)
                .with_comp_ids(CompIdBinding {
                    sender_comp_id: NOPERM_SENDER.to_string(),
                    target_comp_id: VENUE.to_string(),
                }),
        ];
        Self::start_with(accounts, u32::MAX).await
    }

    async fn start_with(accounts: Vec<AccountProvision>, rate_limit: u32) -> Self {
        Self::start_full(accounts, rate_limit, "boot-secret").await
    }

    /// Like [`Self::start_with`], but with a caller-chosen bootstrap secret — so a
    /// captured-log test can plant a distinctive marker and assert its absence
    /// (#042).
    async fn start_full(
        accounts: Vec<AccountProvision>,
        rate_limit: u32,
        bootstrap_secret: &str,
    ) -> Self {
        let auth = AuthConfig::dev()
            .expect("dev auth")
            .with_bootstrap_secret(bootstrap_secret)
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

/// An `OrderCancelReplaceRequest (G)` re-pricing/re-sizing `orig_cl_ord_id`.
#[allow(clippy::too_many_arguments)]
fn replace_frame(
    sender: &str,
    target: &str,
    seq: u64,
    orig_cl_ord_id: &str,
    cl_ord_id: &str,
    price: &str,
    qty: u64,
) -> Vec<u8> {
    let body = format!(
        "35=G\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x0141={orig_cl_ord_id}\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0154=1\x0160=20240329-12:00:00.000\x0140=2\x0144={price}\x0138={qty}\x0159=1\x01"
    );
    frame_with_body(body.as_bytes())
}

/// A `MarketDataRequest (V)`: `sub_type` (`1`=snap+updates / `2`=unsubscribe),
/// `depth` (`0`=full book), and the `MDEntryType (269)` group values (`0`=Bid /
/// `1`=Offer / `2`=Trade) for one symbol.
#[allow(clippy::too_many_arguments)]
fn market_data_request_frame(
    sender: &str,
    target: &str,
    seq: u64,
    md_req_id: &str,
    sub_type: &str,
    depth: u32,
    entry_types: &[&str],
    symbol: &str,
) -> Vec<u8> {
    let mut group = String::new();
    for entry_type in entry_types {
        group.push_str(&format!("269={entry_type}\x01"));
    }
    let body = format!(
        "35=V\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x01262={md_req_id}\x01263={sub_type}\x01264={depth}\x01267={count}\x01{group}146=1\x0155={symbol}\x01",
        count = entry_types.len(),
    );
    frame_with_body(body.as_bytes())
}

/// A `MarketDataRequest (V)` naming an explicit list of `symbols` in the
/// `NoRelatedSym (146)` group (so a test can name the same symbol twice).
fn market_data_request_frame_symbols(
    sender: &str,
    target: &str,
    seq: u64,
    md_req_id: &str,
    entry_types: &[&str],
    symbols: &[&str],
) -> Vec<u8> {
    let mut group = String::new();
    for entry_type in entry_types {
        group.push_str(&format!("269={entry_type}\x01"));
    }
    let mut sym_group = format!("146={}\x01", symbols.len());
    for symbol in symbols {
        sym_group.push_str(&format!("55={symbol}\x01"));
    }
    let body = format!(
        "35=V\x0149={sender}\x0156={target}\x0134={seq}\x0152=20240329-12:00:00.000\x01262={md_req_id}\x01263=1\x01264=0\x01267={count}\x01{group}{sym_group}",
        count = entry_types.len(),
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

/// Like [`recv_frames`], but keeps accumulating frames across reads until one
/// satisfies `wanted` or `timeout` elapses, returning everything received.
///
/// A single [`recv_frames`] returns on the first complete frame. When a logical
/// response arrives as several frames written with a gap — e.g. a `New (150=0)`
/// ack followed by a `Trade (150=F)` fill — a lone call can return only the ack
/// and race the fill. Under a CPU-loaded test binary (the Argon2id-heavy #42
/// suite adds contention) that gap widens and the fill lands after the first
/// call returns. This waits for the awaited frame instead; only the receive
/// window is widened — the assertions on the returned frames are unchanged.
async fn recv_frames_until(
    stream: &mut TcpStream,
    timeout: Duration,
    wanted: impl Fn(&[u8]) -> bool,
) -> Vec<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut all = Vec::new();
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let batch = recv_frames(stream, deadline - now).await;
        let matched = batch.iter().any(|frame| wanted(frame));
        let empty = batch.is_empty();
        all.extend(batch);
        if matched {
            break;
        }
        // A window that elapsed with no new frame at all is a genuine timeout,
        // not a mid-write gap — stop rather than spin to the deadline.
        if empty {
            break;
        }
    }
    all
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
    // New (150=0) and the killed-remainder Canceled (150=4) are written as separate
    // frames with a gap; wait for the Canceled so a single read can't race past it
    // (a CPU-loaded binary widens the gap) — assertions on the returned set are
    // unchanged.
    let reply = recv_frames_until(&mut client, Duration::from_secs(3), |f| {
        field(f, "150").as_deref() == Some("4")
    })
    .await;
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
    let reply = recv_frames_until(&mut client, Duration::from_secs(10), |f| {
        msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("F")
    })
    .await;
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
    // TransactTime(60) = the venue event-time carrier (venue_ts), the 4th
    // observation join key (#104) — present and a well-formed FIX UTC timestamp.
    let transact_time = field(trade, "60").expect("TransactTime(60)");
    assert!(
        fauxchange::gateway::fix::header::UtcTimestamp::parse(60, &transact_time).is_ok(),
        "60 is a FIX UTC timestamp: {transact_time}"
    );
}

/// Logs a session in as `(user, pw, sender)` and drains the credential-free ack.
async fn logon_as(addr: SocketAddr, user: &str, pw: &str, sender: &str) -> TcpStream {
    let mut client = connect(addr).await;
    client
        .write_all(&logon_frame(sender, VENUE, 1, user, pw, false))
        .await
        .expect("logon");
    let ack = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(any_msg_type(&ack, "A"), "the logon is admitted");
    client
}

#[tokio::test]
async fn test_market_data_subscribe_snapshots_then_streams_a_delta_with_rpt_seq() {
    // #040: a reader `V` (Bid+Offer) receives a `W` baseline immediately, then a
    // user-driven book change on another session streams an `X` carrying the same
    // per-instrument `instrument_sequence` as `RptSeq (83)` — observation parity
    // and the FIX MD orderbook projection end to end.
    let harness = Harness::start().await;
    let mut reader = logon_as(harness.addr, READER_USER, READER_PW, READER_SENDER).await;
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            2,
            "MDR-1",
            "1",
            0,
            &["0", "1"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("subscribe");
    let snapshot = recv_frames(&mut reader, Duration::from_secs(3)).await;
    let w = snapshot
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("W"))
        .expect("a W snapshot in reply to V");
    assert_eq!(
        field(w, "262").as_deref(),
        Some("MDR-1"),
        "W echoes MDReqID"
    );
    assert_eq!(
        field(w, "55").as_deref(),
        Some("BTC-20240329-50000-C"),
        "W carries the symbol"
    );
    assert!(field(w, "83").is_some(), "W carries RptSeq(83)");

    // A trader rests a limit order — a user-driven book delta that streams as `X`.
    let mut trader = logon_trader(harness.addr).await;
    trader
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "mm-rest-1",
            "2",
            "500.00",
            3,
        ))
        .await
        .expect("resting order");
    let _ = recv_frames(&mut trader, Duration::from_secs(3)).await;

    // The reader receives the `X` on the session cadence tick.
    let incremental = recv_frames(&mut reader, Duration::from_secs(3)).await;
    let x = incremental
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("X"))
        .expect("an X incremental after the book delta");
    assert_eq!(
        field(x, "262").as_deref(),
        Some("MDR-1"),
        "X echoes MDReqID"
    );
    assert!(field(x, "83").is_some(), "X carries RptSeq(83)");
    // The resting sell at 500.00 appears as an Offer (269=1) at the resulting size.
    assert_eq!(
        field(x, "269").as_deref(),
        Some("1"),
        "the ask level is an Offer"
    );
    assert_eq!(
        field(x, "270").as_deref(),
        Some("500.00"),
        "MDEntryPx decimal"
    );
    assert_eq!(
        field(x, "271").as_deref(),
        Some("3"),
        "MDEntrySize = resting qty"
    );
    // The MD `RptSeq(83)` and the session `MsgSeqNum(34)` are distinct namespaces.
    assert_ne!(
        field(x, "83"),
        field(x, "34"),
        "RptSeq(83) is the instrument_sequence, not the session MsgSeqNum(34)"
    );
    // The baseline filter: the X's RptSeq is STRICTLY greater than the W's — no
    // redundant X at the baseline, RptSeq strictly increases across the W→X boundary.
    let w_seq: u64 = field(w, "83")
        .and_then(|v| v.parse().ok())
        .expect("W RptSeq");
    let x_seq: u64 = field(x, "83")
        .and_then(|v| v.parse().ok())
        .expect("X RptSeq");
    assert!(
        x_seq > w_seq,
        "X RptSeq({x_seq}) must be strictly after the W baseline({w_seq})"
    );
    // Exactly one X for the single book change (no redundant delta at the baseline).
    let x_count = incremental
        .iter()
        .filter(|f| msg_type(f).as_deref() == Some("X"))
        .count();
    assert_eq!(x_count, 1, "one book change → exactly one X");
}

#[tokio::test]
async fn test_market_data_trade_only_request_is_a_market_data_reject() {
    // A `V` asking only for the trade tape (MDEntryType 2) requests no book side, so
    // the FIX MD orderbook surface rejects it with `Y` (MDReqRejReason = 8), never a
    // bare session `Reject (3)`.
    let harness = Harness::start().await;
    let mut reader = logon_as(harness.addr, READER_USER, READER_PW, READER_SENDER).await;
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            2,
            "MDR-TRADE",
            "1",
            0,
            &["2"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("subscribe");
    let reply = recv_frames(&mut reader, Duration::from_secs(3)).await;
    assert!(
        !any_msg_type(&reply, "3"),
        "never a bare Reject(3): {reply:?}"
    );
    let y = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("Y"))
        .expect("a MarketDataRequestReject Y");
    assert_eq!(field(y, "262").as_deref(), Some("MDR-TRADE"));
    assert_eq!(
        field(y, "281").as_deref(),
        Some("8"),
        "Unsupported MDEntryType"
    );
}

#[tokio::test]
async fn test_market_data_request_without_read_permission_is_a_market_data_reject() {
    // A session holding no permission (neither Read nor Trade) is refused market
    // data with `Y` (MDReqRejReason = 3), never a bare session `Reject (3)`.
    let harness = Harness::start().await;
    let mut client = logon_as(harness.addr, NOPERM_USER, NOPERM_PW, NOPERM_SENDER).await;
    client
        .write_all(&market_data_request_frame(
            NOPERM_SENDER,
            VENUE,
            2,
            "MDR-NOPERM",
            "1",
            0,
            &["0", "1"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("subscribe");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        !any_msg_type(&reply, "3"),
        "never a bare Reject(3): {reply:?}"
    );
    let y = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("Y"))
        .expect("a MarketDataRequestReject Y");
    assert_eq!(field(y, "262").as_deref(), Some("MDR-NOPERM"));
    assert_eq!(
        field(y, "281").as_deref(),
        Some("3"),
        "Insufficient permissions"
    );
}

#[tokio::test]
async fn test_market_data_duplicate_md_req_id_is_a_market_data_reject() {
    // A second `V` reusing a live `MDReqID` is a duplicate → `Y` (MDReqRejReason=1).
    let harness = Harness::start().await;
    let mut reader = logon_as(harness.addr, READER_USER, READER_PW, READER_SENDER).await;
    // First `V` subscribes and returns a `W`.
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            2,
            "MDR-DUP",
            "1",
            0,
            &["0", "1"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("first subscribe");
    let first = recv_frames(&mut reader, Duration::from_secs(3)).await;
    assert!(any_msg_type(&first, "W"), "the first V is a W: {first:?}");
    // Second `V` reuses the same MDReqID → a duplicate reject.
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            3,
            "MDR-DUP",
            "1",
            0,
            &["0", "1"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("duplicate subscribe");
    let reply = recv_frames(&mut reader, Duration::from_secs(3)).await;
    let saw_dup = reply
        .iter()
        .any(|f| msg_type(f).as_deref() == Some("Y") && field(f, "281").as_deref() == Some("1"));
    assert!(saw_dup, "the duplicate MDReqID is a Y(281=1): {reply:?}");
}

#[tokio::test]
async fn test_market_data_mixed_trade_entry_is_rejected_not_silently_served() {
    // #101 (task 3): a mixed `V` (a book side AND a Trade entry type, 269=0,1,2) is
    // NOT silently served with the Trade entry dropped — the whole request is
    // rejected `Y` (MDReqRejReason=8, Unsupported MDEntryType), never a `W`.
    let harness = Harness::start().await;
    let mut reader = logon_as(harness.addr, READER_USER, READER_PW, READER_SENDER).await;
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            2,
            "MDR-MIX",
            "1",
            0,
            &["0", "1", "2"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("mixed subscribe");
    let reply = recv_frames(&mut reader, Duration::from_secs(3)).await;
    assert!(
        !any_msg_type(&reply, "W"),
        "a mixed Trade V is never silently served as a W: {reply:?}"
    );
    assert!(
        !any_msg_type(&reply, "3"),
        "never a bare Reject(3): {reply:?}"
    );
    let y = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("Y"))
        .expect("a MarketDataRequestReject Y");
    assert_eq!(field(y, "262").as_deref(), Some("MDR-MIX"));
    assert_eq!(
        field(y, "281").as_deref(),
        Some("8"),
        "Trade entry type is Unsupported MDEntryType(281=8)"
    );
}

#[tokio::test]
async fn test_market_data_resubscribe_of_a_live_symbol_is_rejected_and_keeps_the_original() {
    // #101 (task 2, security P3): a second `V` (a NEW MDReqID) naming a symbol
    // already subscribed on this session must NOT silently overwrite the prior
    // subscription — the original MDReqID must stay live. The re-subscribe is
    // rejected `Y` (MDReqRejReason=1, duplicate subscription); the original keeps
    // streaming (its `X` still echoes the original MDReqID).
    let harness = Harness::start().await;
    let mut reader = logon_as(harness.addr, READER_USER, READER_PW, READER_SENDER).await;
    // V #1 subscribes symbol S under MDR-ORIG → a W.
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            2,
            "MDR-ORIG",
            "1",
            0,
            &["0", "1"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("first subscribe");
    let first = recv_frames(&mut reader, Duration::from_secs(3)).await;
    assert!(any_msg_type(&first, "W"), "the first V is a W: {first:?}");

    // V #2 re-subscribes the SAME symbol under a NEW MDReqID → rejected, no overwrite.
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            3,
            "MDR-NEW",
            "1",
            0,
            &["0", "1"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("re-subscribe");
    let reject = recv_frames(&mut reader, Duration::from_secs(3)).await;
    let y = reject
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("Y"))
        .expect("a MarketDataRequestReject Y for the re-subscribe");
    assert_eq!(
        field(y, "262").as_deref(),
        Some("MDR-NEW"),
        "the reject echoes the NEW (rejected) MDReqID"
    );
    assert_eq!(
        field(y, "281").as_deref(),
        Some("1"),
        "a re-subscribe of a live symbol is a duplicate subscription (281=1)"
    );
    assert!(
        !any_msg_type(&reject, "W"),
        "the re-subscribe never emits a second W: {reject:?}"
    );

    // The ORIGINAL subscription is untouched: a book delta streams an `X` that still
    // echoes MDR-ORIG (the earlier MDReqID was not orphaned or overwritten).
    let mut trader = logon_trader(harness.addr).await;
    trader
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "mm-orig-1",
            "2",
            "500.00",
            3,
        ))
        .await
        .expect("resting order");
    let _ = recv_frames(&mut trader, Duration::from_secs(3)).await;

    let incremental = recv_frames_until(&mut reader, Duration::from_secs(3), |f| {
        msg_type(f).as_deref() == Some("X")
    })
    .await;
    let x = incremental
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("X"))
        .expect("an X on the still-live original subscription");
    assert_eq!(
        field(x, "262").as_deref(),
        Some("MDR-ORIG"),
        "the original MDReqID still streams — it was not orphaned by the re-subscribe"
    );
    assert!(
        !incremental
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("X")
                && field(f, "262").as_deref() == Some("MDR-NEW")),
        "the rejected MDReqID never streams an X: {incremental:?}"
    );
}

#[tokio::test]
async fn test_market_data_request_naming_the_same_symbol_twice_is_rejected() {
    // #101 review: a single `V` that names the SAME symbol twice in NoRelatedSym(146)
    // bypassed the already-live-symbol rule (which only checks the existing map),
    // so it would emit duplicate `W` snapshots, overwrite its own map entry, and
    // double-count against the ceiling. The whole request is now rejected `Y`
    // (duplicate subscription, 281=1) and NO `W` is emitted.
    let harness = Harness::start().await;
    let mut reader = logon_as(harness.addr, READER_USER, READER_PW, READER_SENDER).await;
    reader
        .write_all(&market_data_request_frame_symbols(
            READER_SENDER,
            VENUE,
            2,
            "MDR-DUP",
            &["0", "1"],
            &["BTC-20240329-50000-C", "BTC-20240329-50000-C"],
        ))
        .await
        .expect("duplicate-symbol subscribe");
    let reply = recv_frames(&mut reader, Duration::from_secs(3)).await;
    let y = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("Y"))
        .expect("a MarketDataRequestReject Y for the intra-request duplicate");
    assert_eq!(field(y, "262").as_deref(), Some("MDR-DUP"));
    assert_eq!(
        field(y, "281").as_deref(),
        Some("1"),
        "a duplicate symbol within the request is a duplicate subscription (281=1)"
    );
    assert!(
        !any_msg_type(&reply, "W"),
        "a duplicate-symbol request emits NO W snapshot: {reply:?}"
    );
}

#[tokio::test]
async fn test_market_data_top_of_book_quotes_are_the_depth_bounded_book_projection() {
    // #101 (task 1, quotes): best-bid/offer "quotes" are not a separate FIX channel
    // — they are the depth-bounded (MarketDepth=1) orderbook projection. A `V` with
    // Bid/Offer (269=0,1) and depth 1 is SERVED (a `W`), never a `Y`.
    let harness = Harness::start().await;
    let mut reader = logon_as(harness.addr, READER_USER, READER_PW, READER_SENDER).await;
    reader
        .write_all(&market_data_request_frame(
            READER_SENDER,
            VENUE,
            2,
            "MDR-QUOTE",
            "1",
            1,
            &["0", "1"],
            "BTC-20240329-50000-C",
        ))
        .await
        .expect("quotes subscribe");
    let reply = recv_frames(&mut reader, Duration::from_secs(3)).await;
    assert!(
        !any_msg_type(&reply, "Y"),
        "a depth-1 Bid/Offer quotes request is served, not rejected: {reply:?}"
    );
    let w = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("W"))
        .expect("a W snapshot for the depth-1 quotes request");
    assert_eq!(
        field(w, "262").as_deref(),
        Some("MDR-QUOTE"),
        "W echoes MDReqID"
    );
    assert!(field(w, "83").is_some(), "W carries RptSeq(83)");
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
async fn test_cross_session_cancel_of_an_already_gone_order_is_masked_9_not_a_false_canceled() {
    // #098 fix 3: a stale index entry that still RESOLVES (the AddOrder recorded it)
    // but whose order is no longer resting — cancelled on a prior connection — makes
    // the sequenced cancel capture a `VenueOutcome::Rejected`. The gateway MUST render
    // the OBSERVED outcome as an indistinguishable masked `9`, NEVER a false `8
    // Canceled`. Session 1 places then cancels `stale-1`; session 2 re-cancels it.
    let harness = Harness::start().await;

    let mut first = logon_trader(harness.addr).await;
    first
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "stale-1",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("place");
    let _ = recv_frames_until(&mut first, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("8")
    })
    .await;
    first
        .write_all(&cancel_frame(
            TRADER_SENDER,
            VENUE,
            3,
            "stale-1",
            "cxl-first",
        ))
        .await
        .expect("cancel");
    let canceled = recv_frames_until(&mut first, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("4")
    })
    .await;
    assert!(
        canceled
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("4")),
        "session 1 cancels the order (8, ExecType 4), got {canceled:?}"
    );
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Session 2 — a fresh connection. The venue index still resolves `stale-1` (the
    // AddOrder recorded it; a cancel does not retire it), but the order is gone, so
    // the sequenced cancel is `Rejected` → a masked `9`, never an `8 Canceled`.
    let mut second = connect(harness.addr).await;
    second
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            4,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    assert!(
        any_msg_type(&recv_frames(&mut second, Duration::from_secs(3)).await, "A"),
        "the reconnect is admitted"
    );
    second
        .write_all(&cancel_frame(
            TRADER_SENDER,
            VENUE,
            5,
            "stale-1",
            "cxl-second",
        ))
        .await
        .expect("re-cancel");
    let reply = recv_frames_until(&mut second, Duration::from_secs(10), |f| {
        matches!(msg_type(f).as_deref(), Some("8") | Some("9"))
    })
    .await;
    assert!(
        any_msg_type(&reply, "9")
            && !reply
                .iter()
                .any(|f| msg_type(f).as_deref() == Some("8")
                    && field(f, "150").as_deref() == Some("4")),
        "a re-cancel of an already-gone order is a masked 9, never a false 8 Canceled, \
         got {reply:?}"
    );
}

#[tokio::test]
async fn test_cross_session_cancel_of_a_prior_session_order_succeeds() {
    // #098: place a resting order on one FIX connection under a ClOrdID, drop the
    // connection, then cancel that same OrigClOrdID on a NEW connection. Before
    // #098 the new session's correlation map was empty → 9 Unknown order; now the
    // account-scoped venue index resolves it cross-session → 8 Canceled.
    let harness = Harness::start().await;

    let mut first = logon_trader(harness.addr).await;
    first
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "xsession-1",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("place");
    let placed = recv_frames_until(&mut first, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("8")
    })
    .await;
    assert!(
        placed
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("0")),
        "the order rests (New) in session 1, got {placed:?}"
    );
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Session 2 — a brand-new connection with an empty per-session map.
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
    let ack = recv_frames(&mut second, Duration::from_secs(3)).await;
    assert!(any_msg_type(&ack, "A"), "the reconnect is admitted");
    second
        .write_all(&cancel_frame(
            TRADER_SENDER,
            VENUE,
            4,
            "xsession-1",
            "cxl-xs",
        ))
        .await
        .expect("cancel");
    let reply = recv_frames_until(&mut second, Duration::from_secs(10), |f| {
        (msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("4"))
            || msg_type(f).as_deref() == Some("9")
    })
    .await;
    assert!(
        !any_msg_type(&reply, "9"),
        "a cross-session cancel is no longer 9 Unknown order, got {reply:?}"
    );
    assert!(
        reply
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("4")),
        "the prior-session order is Canceled (8, ExecType 4), got {reply:?}"
    );
}

#[tokio::test]
async fn test_cross_session_replace_of_a_prior_session_order_succeeds() {
    // #098: the replace (G) leg of the same cross-session correlation — place on
    // one connection, replace that OrigClOrdID on a new one → 8 Replaced, not 9.
    let harness = Harness::start().await;

    let mut first = logon_trader(harness.addr).await;
    first
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "xrepl-1",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("place");
    let _ = recv_frames_until(&mut first, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("8")
    })
    .await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

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
    let ack = recv_frames(&mut second, Duration::from_secs(3)).await;
    assert!(any_msg_type(&ack, "A"), "the reconnect is admitted");
    second
        .write_all(&replace_frame(
            TRADER_SENDER,
            VENUE,
            4,
            "xrepl-1",
            "xrepl-2",
            "450.00",
            2,
        ))
        .await
        .expect("replace");
    let reply = recv_frames_until(&mut second, Duration::from_secs(10), |f| {
        (msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("5"))
            || msg_type(f).as_deref() == Some("9")
    })
    .await;
    assert!(
        !any_msg_type(&reply, "9"),
        "a cross-session replace is no longer 9 Unknown order, got {reply:?}"
    );
    assert!(
        reply
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("5")),
        "the prior-session order is Replaced (8, ExecType 5), got {reply:?}"
    );
}

#[tokio::test]
async fn test_cross_session_replace_rekeys_the_shared_index_new_id_cancels_old_id_masked() {
    // #098 fix 4: a committed replace must update the JOURNAL-DERIVED shared index —
    // publish `(account, new_ClOrdID) → new_order_id` and retire the stale
    // `(account, OrigClOrdID)`. So on a THIRD, fresh connection: a cancel by the NEW
    // ClOrdID succeeds (the replacement is cross-session correlatable), and a cancel
    // by the OLD ClOrdID is an indistinguishable masked `9` (its entry was retired —
    // the original was cancelled by the replace's cancel leg).
    let harness = Harness::start().await;

    // Session 1 — place a resting sell under `rk-1`.
    let mut first = logon_trader(harness.addr).await;
    first
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "rk-1",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("place");
    let _ = recv_frames_until(&mut first, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("8")
    })
    .await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Session 2 — replace `rk-1` → `rk-2` (rests at the new price).
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
    assert!(
        any_msg_type(&recv_frames(&mut second, Duration::from_secs(3)).await, "A"),
        "the second logon is admitted"
    );
    second
        .write_all(&replace_frame(
            TRADER_SENDER,
            VENUE,
            4,
            "rk-1",
            "rk-2",
            "450.00",
            2,
        ))
        .await
        .expect("replace");
    let replaced = recv_frames_until(&mut second, Duration::from_secs(10), |f| {
        (msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("5"))
            || msg_type(f).as_deref() == Some("9")
    })
    .await;
    assert!(
        replaced
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("5")),
        "the replace is Replaced (8, ExecType 5), got {replaced:?}"
    );
    drop(second);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Session 3 — a fresh connection resolving ONLY through the shared venue index.
    let mut third = connect(harness.addr).await;
    third
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE,
            5,
            TRADER_USER,
            TRADER_PW,
            false,
        ))
        .await
        .expect("logon");
    assert!(
        any_msg_type(&recv_frames(&mut third, Duration::from_secs(3)).await, "A"),
        "the third logon is admitted"
    );

    // (a) The OLD ClOrdID was retired: a cancel by it is an indistinguishable masked
    // `9` (never an `8 Canceled`) — it no longer resolves to a live order.
    third
        .write_all(&cancel_frame(TRADER_SENDER, VENUE, 6, "rk-1", "cxl-old"))
        .await
        .expect("cancel old");
    let old = recv_frames_until(&mut third, Duration::from_secs(10), |f| {
        matches!(msg_type(f).as_deref(), Some("8") | Some("9"))
    })
    .await;
    assert!(
        any_msg_type(&old, "9")
            && !old
                .iter()
                .any(|f| msg_type(f).as_deref() == Some("8")
                    && field(f, "150").as_deref() == Some("4")),
        "a cancel by the RETIRED old ClOrdID is a masked 9, never an 8 Canceled, got {old:?}"
    );

    // (b) The NEW ClOrdID resolves cross-session to the live replacement: a cancel by
    // it is `8 Canceled` (ExecType 4), not `9`.
    third
        .write_all(&cancel_frame(TRADER_SENDER, VENUE, 7, "rk-2", "cxl-new"))
        .await
        .expect("cancel new");
    let new = recv_frames_until(&mut third, Duration::from_secs(10), |f| {
        (msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("4"))
            || msg_type(f).as_deref() == Some("9")
    })
    .await;
    assert!(
        !any_msg_type(&new, "9"),
        "a cancel by the replacement ClOrdID is not 9 Unknown order, got {new:?}"
    );
    assert!(
        new.iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("4")),
        "the replacement order is Canceled (8, ExecType 4) via its new ClOrdID, got {new:?}"
    );
}

#[tokio::test]
async fn test_account_isolation_a_colliding_clordid_cannot_cancel_another_account() {
    // #098 security invariant: a cancel resolves a ClOrdID only WITHIN the
    // authenticated account. Account A places under "shared-id"; account B, holding
    // the SAME ClOrdID string, cannot resolve or cancel A's order — the lookup is a
    // plain 9 Unknown order (masked, never a leak that another account owns it).
    const TRADER2_USER: &str = "trader2-fix";
    const TRADER2_PW: &str = "trader2-plaintext-pw-DoNotLog-654";
    const TRADER2_SENDER: &str = "TRADER2CLIENT";

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
            AccountId::new("trader-2"),
            Hash32([9; 32]),
            vec![Permission::Trade],
        )
        .with_fix_login(TRADER2_USER, TRADER2_PW)
        .with_comp_ids(CompIdBinding {
            sender_comp_id: TRADER2_SENDER.to_string(),
            target_comp_id: VENUE.to_string(),
        }),
    ];
    let harness = Harness::start_with(accounts, u32::MAX).await;

    // Account A places a resting order under the colliding id.
    let mut client_a = logon_trader(harness.addr).await;
    client_a
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "shared-id",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("A places");
    let placed = recv_frames_until(&mut client_a, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("8")
    })
    .await;
    assert!(
        placed
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("0")),
        "A's order rests (New), got {placed:?}"
    );

    // Account B logs on and cancels the SAME ClOrdID string.
    let mut client_b = connect(harness.addr).await;
    client_b
        .write_all(&logon_frame(
            TRADER2_SENDER,
            VENUE,
            1,
            TRADER2_USER,
            TRADER2_PW,
            false,
        ))
        .await
        .expect("B logon");
    let ack = recv_frames(&mut client_b, Duration::from_secs(3)).await;
    assert!(any_msg_type(&ack, "A"), "B is admitted");
    client_b
        .write_all(&cancel_frame(
            TRADER2_SENDER,
            VENUE,
            2,
            "shared-id",
            "b-cxl",
        ))
        .await
        .expect("B cancel");
    let reply = recv_frames_until(&mut client_b, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("9") || msg_type(f).as_deref() == Some("8")
    })
    .await;
    assert!(
        !reply
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("4")),
        "B must NOT be able to cancel A's order via the colliding ClOrdID, got {reply:?}"
    );
    let reject = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("9"))
        .unwrap_or_else(|| panic!("B's cross-account cancel is a masked 9, got {reply:?}"));
    assert_eq!(
        field(reject, "102").as_deref(),
        Some("1"),
        "masked as Unknown order — no leak that another account owns the id"
    );

    // A still owns and can cancel its order (B's probe changed nothing).
    client_a
        .write_all(&cancel_frame(TRADER_SENDER, VENUE, 3, "shared-id", "a-cxl"))
        .await
        .expect("A cancel");
    let a_reply = recv_frames_until(&mut client_a, Duration::from_secs(5), |f| {
        msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("4")
    })
    .await;
    assert!(
        a_reply
            .iter()
            .any(|f| msg_type(f).as_deref() == Some("8")
                && field(f, "150").as_deref() == Some("4")),
        "A cancels its own order (8 Canceled), got {a_reply:?}"
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
    let first = recv_frames_until(&mut client, Duration::from_secs(10), |f| {
        msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("F")
    })
    .await;
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
    let resend = recv_frames_until(&mut client, Duration::from_secs(10), |f| {
        msg_type(f).as_deref() == Some("8")
    })
    .await;
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
    let cancel_reply = recv_frames_until(&mut client, Duration::from_secs(10), |f| {
        msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some("4")
    })
    .await;
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

/// #042: extends the logon-only capture test above with a **full** logon +
/// order flow (resting order, crossing fill, unknown-order cancel reject,
/// unsupported-message business reject — every reply type that can carry a
/// `Text (58)`), and widens the forbidden-secret set beyond the password to
/// the bootstrap secret, the Argon2id PHC marker, and the JWT dev signing key
/// fragment — asserted absent from BOTH the captured `tracing` output AND
/// every outbound `Text (58)` field across the whole flow. A `DATABASE_URL`
/// forward guard (the FIX gateway itself never touches `DATABASE_URL` today;
/// its redaction is proven generically by `tests/security.rs`'s boot-config
/// test) is included so a future change wiring DB logging into this same
/// tracing scope would be caught here too.
#[tokio::test]
async fn test_no_credential_appears_in_a_captured_log_or_text58_over_a_full_order_flow() {
    const BOOTSTRAP_MARKER: &str = "BOOTSTRAP-SECRET-marker-DoNotLog-042";
    const DB_PASSWORD_MARKER: &str = "DBPASS-marker-DoNotLog-042";
    // A fragment unique to the embedded dev **private** key (`JwtAuth::dev`,
    // src/auth.rs) — the same marker `tests/security.rs` uses.
    const DEV_SIGNING_KEY_FRAGMENT: &str = "VNh0Vk8l7tR9inRKTQaO";

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
    let harness = Harness::start_full(accounts, u32::MAX, BOOTSTRAP_MARKER).await;

    // A forward-guard boot-config log line naming a DATABASE_URL-shaped
    // secret: proves a future change that logged it from within this same
    // tracing scope would be caught (mirrors tests/security.rs's boot-config
    // redaction test).
    let database_url = format!("postgres://venue:{DB_PASSWORD_MARKER}@db:5432/fauxchange");
    tracing::info!(
        database_url = "<redacted>",
        "fix gateway effective config at boot"
    );
    assert!(database_url.contains(DB_PASSWORD_MARKER));

    let mut client = logon_trader(harness.addr).await;
    let mut all_frames: Vec<Vec<u8>> = Vec::new();

    // A resting maker, a crossing taker (a real Trade fill).
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            2,
            "maker-042",
            "2",
            "500.00",
            3,
        ))
        .await
        .expect("maker");
    all_frames.extend(recv_frames(&mut client, Duration::from_secs(3)).await);
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE,
            3,
            "taker-042",
            "1",
            "500.00",
            3,
        ))
        .await
        .expect("taker");
    all_frames.extend(recv_frames(&mut client, Duration::from_secs(3)).await);

    // An unknown-order cancel — a Text-bearing OrderCancelReject(9).
    client
        .write_all(&cancel_frame(
            TRADER_SENDER,
            VENUE,
            4,
            "never-placed-042",
            "cxl-042",
        ))
        .await
        .expect("cancel");
    all_frames.extend(recv_frames(&mut client, Duration::from_secs(3)).await);

    // An unsupported application message — a Text-bearing BusinessMessageReject(j).
    client
        .write_all(&unsupported_app_frame(TRADER_SENDER, VENUE, 5))
        .await
        .expect("unsupported");
    all_frames.extend(recv_frames(&mut client, Duration::from_secs(3)).await);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Every Text(58) value seen across the whole flow, plus the captured log.
    let mut haystack = String::from_utf8_lossy(&buffer.lock().expect("lock")).to_string();
    for frame in &all_frames {
        if let Some(text) = field(frame, "58") {
            haystack.push('\n');
            haystack.push_str(&text);
        }
    }

    let forbidden: &[(&str, &str)] = &[
        ("FIX plaintext password", TRADER_PW),
        ("bootstrap secret", BOOTSTRAP_MARKER),
        ("Argon2id PHC marker", "$argon2id$"),
        ("JWT signing key fragment", DEV_SIGNING_KEY_FRAGMENT),
        ("DATABASE_URL password marker", DB_PASSWORD_MARKER),
    ];
    for (label, needle) in forbidden {
        assert!(
            !haystack.contains(needle),
            "SECURITY: {label} leaked into a captured log or a FIX Text(58) field \
             over a full logon+order flow"
        );
    }

    // Positive proof the flow actually reached the Text-bearing reply types this
    // test means to cover.
    assert!(
        any_msg_type(&all_frames, "9"),
        "expected an OrderCancelReject(9) in the flow, got {all_frames:?}"
    );
    assert!(
        any_msg_type(&all_frames, "j"),
        "expected a BusinessMessageReject(j) in the flow, got {all_frames:?}"
    );
}
