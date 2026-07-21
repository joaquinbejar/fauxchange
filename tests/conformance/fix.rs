//! Reusable **FIX order-entry parity substrate** (#041) — the FIX arm the
//! conformance module's doc reserves: a `drive_fix_orders` returning the same
//! `Vec<VenueEvent>` the REST driver ([`super::drive_rest_orders`]) returns, so the
//! *same* [`super::assert_streams_parity`] comparator proves order-entry parity
//! (REST ≡ FIX) with no change to the normalization rule
//! ([03 §7](../../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees),
//! [TESTING.md §6](../../docs/TESTING.md#6-conformance--parity-rest--ws--fix)).
//!
//! ## What lives here
//!
//! - **An identically-seeded serving venue with a live acceptor** ([`FixParityHarness`]):
//!   the same four-account tier set as [`super::venue`] — same account ids, **same
//!   owner hashes** (so the fills' compared-verbatim `account`/`owner` match REST),
//!   same default run lineage, same fixed venue clock — additionally provisioned
//!   with FIX logins + immutable `(SenderCompID, TargetCompID)` bindings and a bound
//!   TCP [`FixAcceptor`]. Two `parity_accounts`-seeded venues (one per surface) are
//!   the valid per-surface parity pair the topology requires.
//! - **A minimal FIX test client** ([`FixClient`]): a real `TcpStream` that logs on
//!   as one account and drives `NewOrderSingle (D)` / `OrderCancelRequest (F)` /
//!   `OrderCancelReplaceRequest (G)` / `MarketDataRequest (V)` frames, tracking its
//!   own checked `MsgSeqNum` — the wire helpers ported from `tests/fix_session.rs`.
//! - **The order-entry driver** ([`drive_fix_orders`]): plays a protocol-agnostic
//!   [`super::Step`] scenario over per-account FIX sessions (the same scenario the
//!   REST driver plays) and returns the journaled `VenueEvent` stream.
//! - **The FIX fill projection** ([`fix_report_projection`]): the join keys a FIX
//!   `ExecutionReport (8)` carries — `execution_id` (`ExecID 17`), `liquidity`
//!   (`LastLiquidityInd 851`), `underlying_sequence` (`SecondaryExecID 527`),
//!   `side`, `quantity`, `price` — for observation parity against the REST / WS
//!   projections. FIX carries **no** `venue_ts` in this dialect, so that join key is
//!   REST≡WS only (documented at the call site).

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

use fauxchange::auth::{AccountProvision, CompIdBinding};
use fauxchange::exchange::{Cents, Hash32, VenueEvent};
use fauxchange::gateway::fix::price::render_cents_to_decimal;
use fauxchange::gateway::fix::{
    FixAcceptor, FixAcceptorConfig, FixSessionStore, InMemoryFixSessionStore, SessionConfig,
    VenueFixSessionFactory,
};
use fauxchange::models::{AccountId, Permission};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};

use super::{SECRET, Step, journaled_events};

// ============================================================================
// Identities — the FIX side of the shared four-account tier set
// ============================================================================

/// The venue `TargetCompID` every parity session addresses.
pub const VENUE: &str = "FAUXCHANGE";

/// The default read/receive timeout for a reply from the live acceptor.
pub const REPLY_TIMEOUT: Duration = Duration::from_secs(3);

/// One account's FIX credentials + immutable CompID binding.
#[derive(Debug, Clone, Copy)]
pub struct FixIdentity {
    /// The venue [`AccountId`] (the same id the REST tier uses).
    pub account: &'static str,
    /// `Username (553)`.
    pub user: &'static str,
    /// `Password (554)` — a labelled test credential, never a real secret.
    pub pw: &'static str,
    /// The bound `SenderCompID (49)`.
    pub sender: &'static str,
    /// The account's owner-hash seed byte (must match [`super::venue`]).
    pub owner_byte: u8,
    /// The account's permission tier.
    pub permission: Permission,
}

/// `admin-1` — Admin (unused by FIX order entry, present so the tier set is the
/// same shape as [`super::venue`]).
pub const ADMIN: FixIdentity = FixIdentity {
    account: "admin-1",
    user: "admin-fix",
    pw: "admin-plaintext-pw-DoNotLog-111",
    sender: "ADMINCLIENT",
    owner_byte: 1,
    permission: Permission::Admin,
};

/// `trader-1` — Trade (the maker in the crossing scenarios).
pub const TRADER1: FixIdentity = FixIdentity {
    account: "trader-1",
    user: "trader1-fix",
    pw: "trader1-plaintext-pw-DoNotLog-222",
    sender: "TRADER1CLIENT",
    owner_byte: 2,
    permission: Permission::Trade,
};

/// `trader-2` — Trade (the taker in the crossing scenarios).
pub const TRADER2: FixIdentity = FixIdentity {
    account: "trader-2",
    user: "trader2-fix",
    pw: "trader2-plaintext-pw-DoNotLog-333",
    sender: "TRADER2CLIENT",
    owner_byte: 3,
    permission: Permission::Trade,
};

/// `reader-1` — Read (market data only; refused order entry).
pub const READER: FixIdentity = FixIdentity {
    account: "reader-1",
    user: "reader1-fix",
    pw: "reader1-plaintext-pw-DoNotLog-444",
    sender: "READER1CLIENT",
    owner_byte: 4,
    permission: Permission::Read,
};

/// The four tier identities, in the fixed order [`super::venue`] provisions them.
pub const IDENTITIES: [FixIdentity; 4] = [ADMIN, TRADER1, TRADER2, READER];

/// Resolves the [`FixIdentity`] for a `Step` account label (`"trader-1"`, …).
#[must_use]
pub fn identity_for(account: &str) -> FixIdentity {
    match IDENTITIES.into_iter().find(|id| id.account == account) {
        Some(id) => id,
        None => panic!("no FIX identity for account {account}"),
    }
}

/// The shared account-provision set: the **same** ids and owner hashes as
/// [`super::venue`] plus FIX logins and CompID bindings. Because the fills' `account`
/// and `owner` are compared **verbatim** by the parity normalizer, matching the
/// owner bytes is what makes the REST and FIX journaled streams comparable.
#[must_use]
pub fn parity_accounts() -> Vec<AccountProvision> {
    IDENTITIES
        .into_iter()
        .map(|id| {
            AccountProvision::new(
                AccountId::new(id.account),
                Hash32([id.owner_byte; 32]),
                vec![id.permission],
            )
            .with_fix_login(id.user, id.pw)
            .with_comp_ids(CompIdBinding {
                sender_comp_id: id.sender.to_string(),
                target_comp_id: VENUE.to_string(),
            })
        })
        .collect()
}

/// Builds an identically-seeded **serving** venue for the REST arm of a parity
/// pair — the same `parity_accounts` provisioning the FIX harness uses, so the two
/// arms differ only in the arrival surface, never in the seed.
#[must_use]
pub fn rest_parity_venue() -> Arc<AppState> {
    let auth = match AuthConfig::dev() {
        Ok(auth) => auth
            .with_bootstrap_secret(SECRET)
            .with_accounts(parity_accounts())
            .with_rate_limit(100_000),
        Err(error) => panic!("dev auth must build: {error}"),
    };
    match AppState::new(
        AppStateConfig::new(["BTC"])
            .with_serving(true)
            .with_auth(auth),
    ) {
        Ok(state) => state,
        Err(error) => panic!("REST parity venue must build: {error}"),
    }
}

// ============================================================================
// The live acceptor harness
// ============================================================================

/// A serving [`AppState`] with a bound, serving [`FixAcceptor`] — the FIX arm of a
/// parity pair. Identically seeded to [`rest_parity_venue`]. Dropping it signals
/// the serve loop to shut down.
pub struct FixParityHarness {
    addr: SocketAddr,
    state: Arc<AppState>,
    store: Arc<dyn FixSessionStore>,
    shutdown: watch::Sender<bool>,
}

impl FixParityHarness {
    /// Binds an ephemeral acceptor over a fresh `parity_accounts`-seeded serving
    /// venue and spawns its serve loop.
    pub async fn start() -> Self {
        let auth = match AuthConfig::dev() {
            Ok(auth) => auth
                .with_bootstrap_secret(SECRET)
                .with_accounts(parity_accounts())
                .with_rate_limit(100_000),
            Err(error) => panic!("dev auth must build: {error}"),
        };
        let state = match AppState::new(
            AppStateConfig::new(["BTC"])
                .with_serving(true)
                .with_auth(auth),
        ) {
            Ok(state) => state,
            Err(error) => panic!("FIX parity venue must build: {error}"),
        };

        let config = FixAcceptorConfig {
            addr: match "127.0.0.1:0".parse() {
                Ok(addr) => addr,
                Err(e) => panic!("loopback addr must parse: {e}"),
            },
            connection_cap: 16,
            mailbox_depth: 64,
            max_frame_bytes: 64 * 1024,
            idle_timeout: Duration::from_secs(30),
        };
        let acceptor = match FixAcceptor::bind(config).await {
            Ok(acceptor) => acceptor,
            Err(e) => panic!("acceptor must bind: {e}"),
        };
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

    /// The serving venue behind the acceptor (for a REST/WS observation read of the
    /// same committed event).
    #[must_use]
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// The bound acceptor address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The durable session store (for asserting reset/session events).
    #[must_use]
    pub fn store(&self) -> &Arc<dyn FixSessionStore> {
        &self.store
    }
}

impl Drop for FixParityHarness {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

// ============================================================================
// Wire helpers (ported from tests/fix_session.rs so the parity + conformance
// tests share one FIX test-client shape)
// ============================================================================

/// Wraps a body in a `BeginString`/`BodyLength` header and a checksum trailer,
/// with `SOH` = `\x01`.
#[must_use]
pub fn frame_with_body(body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
    msg.extend_from_slice(body);
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
    msg
}

/// A `Logon (A)` with `Username`/`Password` and an optional `ResetSeqNumFlag`.
#[must_use]
pub fn logon_frame(sender: &str, seq: u64, user: &str, pw: &str, reset: bool) -> Vec<u8> {
    let reset_field = if reset { "141=Y\x01" } else { "" };
    let body = format!(
        "35=A\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0198=0\x01108=30\x01553={user}\x01554={pw}\x01{reset_field}"
    );
    frame_with_body(body.as_bytes())
}

/// A `NewOrderSingle (D)` limit order (`side` `1`=Buy / `2`=Sell, decimal `price`).
#[must_use]
pub fn limit_order_frame(
    sender: &str,
    seq: u64,
    cl_ord_id: &str,
    side: &str,
    price: &str,
    qty: u64,
    tif: &str,
) -> Vec<u8> {
    let body = format!(
        "35=D\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0154={side}\x0160=20240329-12:00:00.000\x0140=2\x0144={price}\x0138={qty}\x0159={tif}\x01"
    );
    frame_with_body(body.as_bytes())
}

/// An `OrderCancelRequest (F)` referencing `orig_cl_ord_id`.
#[must_use]
pub fn cancel_frame(
    sender: &str,
    seq: u64,
    orig_cl_ord_id: &str,
    cl_ord_id: &str,
    side: &str,
) -> Vec<u8> {
    let body = format!(
        "35=F\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0141={orig_cl_ord_id}\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0154={side}\x01"
    );
    frame_with_body(body.as_bytes())
}

/// An `OrderMassCancelRequest (q)` — `All (530=7)` cancels the account's whole
/// resting set; a `Symbol (55)` would make it a per-security (`530=1`) sweep.
#[must_use]
pub fn mass_cancel_frame(sender: &str, seq: u64, cl_ord_id: &str) -> Vec<u8> {
    let body = format!(
        "35=q\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0111={cl_ord_id}\x01530=7\x01"
    );
    frame_with_body(body.as_bytes())
}

/// An `OrderCancelReplaceRequest (G)` referencing `orig_cl_ord_id`.
#[must_use]
pub fn replace_frame(
    sender: &str,
    seq: u64,
    orig_cl_ord_id: &str,
    cl_ord_id: &str,
    side: &str,
    price: &str,
    qty: u64,
) -> Vec<u8> {
    let body = format!(
        "35=G\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0141={orig_cl_ord_id}\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0154={side}\x0140=2\x0144={price}\x0138={qty}\x01"
    );
    frame_with_body(body.as_bytes())
}

/// A **market** `OrderCancelReplaceRequest (G)` (`OrdType=1`, no `Price (44)`).
/// The executor's add leg rejects it — a market order does not rest — so the
/// captured outcome is the partial-replace `Replace { cancelled: true, add: Rejected }`.
#[must_use]
pub fn market_replace_frame(
    sender: &str,
    seq: u64,
    orig_cl_ord_id: &str,
    cl_ord_id: &str,
    side: &str,
    qty: u64,
) -> Vec<u8> {
    let body = format!(
        "35=G\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0141={orig_cl_ord_id}\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0154={side}\x0140=1\x0138={qty}\x01"
    );
    frame_with_body(body.as_bytes())
}

/// A `TestRequest (1)`.
#[must_use]
pub fn test_request_frame(sender: &str, seq: u64, test_req_id: &str) -> Vec<u8> {
    let body = format!(
        "35=1\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01112={test_req_id}\x01"
    );
    frame_with_body(body.as_bytes())
}

/// A `Heartbeat (0)`.
#[must_use]
pub fn heartbeat_frame(sender: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=0\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
    frame_with_body(body.as_bytes())
}

/// A `ResendRequest (2)` for `[begin, end]` (`end` `0` = infinity).
#[must_use]
pub fn resend_request_frame(sender: &str, seq: u64, begin: u64, end: u64) -> Vec<u8> {
    let body = format!(
        "35=2\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x017={begin}\x0116={end}\x01"
    );
    frame_with_body(body.as_bytes())
}

/// A `Logout (5)`.
#[must_use]
pub fn logout_frame(sender: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=5\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
    frame_with_body(body.as_bytes())
}

/// A `MarketDataRequest (V)` for `entry_types` (`0`=Bid / `1`=Offer / `2`=Trade).
#[must_use]
pub fn market_data_request_frame(
    sender: &str,
    seq: u64,
    md_req_id: &str,
    entry_types: &[&str],
) -> Vec<u8> {
    let mut group = String::new();
    for entry_type in entry_types {
        group.push_str(&format!("269={entry_type}\x01"));
    }
    let body = format!(
        "35=V\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01262={md_req_id}\x01263=1\x01264=0\x01267={count}\x01{group}146=1\x0155=BTC-20240329-50000-C\x01",
        count = entry_types.len(),
    );
    frame_with_body(body.as_bytes())
}

/// A well-formed application message with an unsupported `MsgType` (`R`,
/// QuoteRequest — recognised by FIX 4.4, unhandled by the venue dialect).
#[must_use]
pub fn unsupported_app_frame(sender: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=R\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
    frame_with_body(body.as_bytes())
}

/// A `NewOrderSingle (D)` missing the required `Side (54)` — a session-level
/// malformed frame (`Reject (3)`, not an order-level `8`).
#[must_use]
pub fn order_missing_side_frame(sender: &str, seq: u64, cl_ord_id: &str) -> Vec<u8> {
    let body = format!(
        "35=D\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0111={cl_ord_id}\x0155=BTC-20240329-50000-C\x0160=20240329-12:00:00.000\x0140=2\x0144=500.00\x0138=1\x0159=1\x01"
    );
    frame_with_body(body.as_bytes())
}

/// Splits a byte buffer into complete FIX frames.
#[must_use]
pub fn split_frames(buf: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let needle = b"8=FIX.4.4\x01";
    let mut pos = 0;
    while pos < buf.len() {
        let Some(rel) = buf[pos..].windows(needle.len()).position(|w| w == needle) else {
            break;
        };
        let start = pos + rel;
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
/// timeout), returning the raw frames.
pub async fn recv_frames(stream: &mut TcpStream, timeout: Duration) -> Vec<Vec<u8>> {
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
#[must_use]
pub fn field(frame: &[u8], tag: &str) -> Option<String> {
    let text = String::from_utf8_lossy(frame);
    text.split('\u{1}')
        .find_map(|part| part.strip_prefix(&format!("{tag}=")).map(str::to_string))
}

/// The `MsgType (35)` of a frame.
#[must_use]
pub fn msg_type(frame: &[u8]) -> Option<String> {
    field(frame, "35")
}

/// Whether any frame in `frames` has `MsgType (35) == ty`.
#[must_use]
pub fn any_msg_type(frames: &[Vec<u8>], ty: &str) -> bool {
    frames.iter().any(|f| msg_type(f).as_deref() == Some(ty))
}

/// The first frame in `frames` with `MsgType (35) == ty`.
#[must_use]
pub fn find_msg<'a>(frames: &'a [Vec<u8>], ty: &str) -> Option<&'a Vec<u8>> {
    frames.iter().find(|f| msg_type(f).as_deref() == Some(ty))
}

// ============================================================================
// The FIX test client
// ============================================================================

/// A live FIX session bound to one account: a `TcpStream` plus the account's
/// `SenderCompID` and its next checked `MsgSeqNum`.
pub struct FixClient {
    stream: TcpStream,
    sender: String,
    seq: u64,
}

impl FixClient {
    /// Connects and logs on as `identity`, draining the credential-free `Logon (A)`
    /// ack, leaving the session `Active` at `MsgSeqNum = 2`.
    pub async fn logon(addr: SocketAddr, identity: FixIdentity) -> Self {
        let mut stream = match TcpStream::connect(addr).await {
            Ok(stream) => stream,
            Err(e) => panic!("connect must succeed: {e}"),
        };
        let frame = logon_frame(identity.sender, 1, identity.user, identity.pw, false);
        if let Err(e) = stream.write_all(&frame).await {
            panic!("logon write must succeed: {e}");
        }
        let ack = recv_frames(&mut stream, REPLY_TIMEOUT).await;
        assert!(
            any_msg_type(&ack, "A"),
            "the {} logon must be admitted, got {ack:?}",
            identity.account
        );
        Self {
            stream,
            sender: identity.sender.to_string(),
            seq: 2,
        }
    }

    /// Sends `frame` and reads the reply frames.
    async fn round_trip(&mut self, frame: Vec<u8>) -> Vec<Vec<u8>> {
        if let Err(e) = self.stream.write_all(&frame).await {
            panic!("write must succeed: {e}");
        }
        recv_frames(&mut self.stream, REPLY_TIMEOUT).await
    }

    /// Places a resting/crossing limit order (`price` in cents), returning the reply
    /// reports.
    pub async fn place_limit(
        &mut self,
        cl_ord_id: &str,
        side: &str,
        price_cents: u64,
        qty: u64,
        tif: &str,
    ) -> Vec<Vec<u8>> {
        let price = render_cents_to_decimal(Cents::new(price_cents));
        let frame = limit_order_frame(&self.sender, self.seq, cl_ord_id, side, &price, qty, tif);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Cancels a resting order by its original `ClOrdID`.
    pub async fn cancel(
        &mut self,
        orig_cl_ord_id: &str,
        cl_ord_id: &str,
        side: &str,
    ) -> Vec<Vec<u8>> {
        let frame = cancel_frame(&self.sender, self.seq, orig_cl_ord_id, cl_ord_id, side);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Sends an `OrderMassCancelRequest (q)` (`All` scope) and reads the reply —
    /// the accepted `OrderMassCancelReport (r)` plus one `ExecutionReport (8)`
    /// `Canceled` per swept order, or a single `r Rejected`.
    pub async fn mass_cancel(&mut self, cl_ord_id: &str) -> Vec<Vec<u8>> {
        let frame = mass_cancel_frame(&self.sender, self.seq, cl_ord_id);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Replaces a resting order (`G`).
    pub async fn replace(
        &mut self,
        orig_cl_ord_id: &str,
        cl_ord_id: &str,
        side: &str,
        price_cents: u64,
        qty: u64,
    ) -> Vec<Vec<u8>> {
        let price = render_cents_to_decimal(Cents::new(price_cents));
        let frame = replace_frame(
            &self.sender,
            self.seq,
            orig_cl_ord_id,
            cl_ord_id,
            side,
            &price,
            qty,
        );
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Sends a **market** replace (`G`, `OrdType=1`, no price) — the cancel leg
    /// commits but the replacement add is rejected (a market order does not rest).
    pub async fn market_replace(
        &mut self,
        orig_cl_ord_id: &str,
        cl_ord_id: &str,
        side: &str,
        qty: u64,
    ) -> Vec<Vec<u8>> {
        let frame =
            market_replace_frame(&self.sender, self.seq, orig_cl_ord_id, cl_ord_id, side, qty);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Subscribes to market data for the fixture contract.
    pub async fn market_data(&mut self, md_req_id: &str, entry_types: &[&str]) -> Vec<Vec<u8>> {
        let frame = market_data_request_frame(&self.sender, self.seq, md_req_id, entry_types);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Sends a `TestRequest (1)` and reads the `Heartbeat (0)` reply.
    pub async fn test_request(&mut self, test_req_id: &str) -> Vec<Vec<u8>> {
        let frame = test_request_frame(&self.sender, self.seq, test_req_id);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Sends an unsupported application `MsgType` (`R`).
    pub async fn unsupported(&mut self) -> Vec<Vec<u8>> {
        let frame = unsupported_app_frame(&self.sender, self.seq);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Sends a `NewOrderSingle (D)` missing `Side (54)` — a session-level malformed
    /// frame.
    pub async fn order_missing_side(&mut self, cl_ord_id: &str) -> Vec<Vec<u8>> {
        let frame = order_missing_side_frame(&self.sender, self.seq, cl_ord_id);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Sends a frame that skips ahead of the expected `MsgSeqNum` (a deliberate
    /// inbound gap), returning the reply (a `ResendRequest (2)`).
    pub async fn send_out_of_order(&mut self) -> Vec<Vec<u8>> {
        // Jump the sender sequence forward, leaving a hole the acceptor must detect.
        let gapped = self.seq + 5;
        let frame = heartbeat_frame(&self.sender, gapped);
        self.round_trip(frame).await
    }

    /// Sends a `Logout (5)` and reads the reply.
    pub async fn logout(&mut self) -> Vec<Vec<u8>> {
        let frame = logout_frame(&self.sender, self.seq);
        self.seq += 1;
        self.round_trip(frame).await
    }

    /// Drains any further buffered frames (best effort, short timeout).
    pub async fn drain(&mut self) -> Vec<Vec<u8>> {
        recv_frames(&mut self.stream, Duration::from_millis(300)).await
    }
}

/// Connects and sends a single `Logon (A)` with the given credentials **without
/// asserting admission**, returning the reply frames — a `Logout (5)` for a bad
/// credential (the conformance script's logon-failure reject row).
pub async fn attempt_logon(addr: SocketAddr, sender: &str, user: &str, pw: &str) -> Vec<Vec<u8>> {
    let mut stream = match TcpStream::connect(addr).await {
        Ok(stream) => stream,
        Err(e) => panic!("connect must succeed: {e}"),
    };
    let frame = logon_frame(sender, 1, user, pw, false);
    if let Err(e) = stream.write_all(&frame).await {
        panic!("logon write must succeed: {e}");
    }
    recv_frames(&mut stream, REPLY_TIMEOUT).await
}

// ============================================================================
// The order-entry driver (the FIX twin of super::drive_rest_orders)
// ============================================================================

/// The deterministic `ClOrdID` for the `Place` at step `index`. Shared by the
/// driver so a later `Cancel` can reference it as an `OrigClOrdID`.
#[must_use]
fn cl_ord_id_for(index: usize) -> String {
    format!("parity-cl-{index}")
}

/// Drives a protocol-agnostic [`Step`] scenario over per-account **live FIX
/// sessions** against `harness`, then returns the journaled `VenueEvent` stream —
/// the exact artifact [`super::journaled_events`] returns for the REST arm, so the
/// same [`super::assert_streams_parity`] comparator proves order-entry parity.
///
/// Draining each command's reply reports before sending the next guarantees the
/// command committed and journaled (the report is emitted post-commit), so the
/// returned stream is complete.
pub async fn drive_fix_orders(harness: &FixParityHarness, steps: &[Step]) -> Vec<VenueEvent> {
    let mut clients: HashMap<&'static str, FixClient> = HashMap::new();
    // The side each placed order rests on (for a later cancel's Side (54)).
    let mut placed_side: Vec<Option<&'static str>> = Vec::with_capacity(steps.len());

    for (index, step) in steps.iter().cloned().enumerate() {
        match step {
            Step::Place {
                account,
                side,
                price,
                qty,
                tif,
            } => {
                if !clients.contains_key(account) {
                    let client = FixClient::logon(harness.addr(), identity_for(account)).await;
                    clients.insert(account, client);
                }
                let client = match clients.get_mut(account) {
                    Some(client) => client,
                    None => panic!("client for {account} must be present"),
                };
                let fix_side = match side {
                    "buy" => "1",
                    "sell" => "2",
                    other => panic!("unknown side {other}"),
                };
                let fix_tif = match tif {
                    None | Some("gtc") => "1",
                    Some("ioc") => "3",
                    Some("fok") => "4",
                    Some(other) => panic!("unsupported TIF {other} for the FIX driver"),
                };
                let reply = client
                    .place_limit(&cl_ord_id_for(index), fix_side, price, qty, fix_tif)
                    .await;
                assert!(
                    any_msg_type(&reply, "8"),
                    "a FIX place must emit an ExecutionReport, got {reply:?}"
                );
                assert!(
                    !any_msg_type(&reply, "3") && !any_msg_type(&reply, "j"),
                    "a valid FIX place must not be a session/business reject, got {reply:?}"
                );
                placed_side.push(Some(fix_side));
            }
            Step::Cancel { account, target } => {
                let orig = cl_ord_id_for(target);
                let side = match placed_side.get(target).and_then(|s| *s) {
                    Some(side) => side,
                    None => panic!("Cancel target #{target} did not place an order"),
                };
                let client = match clients.get_mut(account) {
                    Some(client) => client,
                    None => panic!("Cancel account {account} never opened a session"),
                };
                let reply = client.cancel(&orig, &cl_ord_id_for(index), side).await;
                assert!(
                    any_msg_type(&reply, "8") || any_msg_type(&reply, "9"),
                    "a FIX cancel must emit an 8 (Canceled) or 9 (reject), got {reply:?}"
                );
                assert!(
                    !any_msg_type(&reply, "3"),
                    "a cancel is never a session Reject(3), got {reply:?}"
                );
                placed_side.push(None);
            }
        }
    }

    journaled_events(harness.state(), "BTC").await
}

// ============================================================================
// The FIX fill projection (observation parity)
// ============================================================================

/// The join keys a FIX `ExecutionReport (8)` `Trade` carries — the FIX projection
/// of one fill leg. FIX does **not** carry `venue_ts` in this dialect, so it is
/// absent here (that join key is REST≡WS only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixReportProjection {
    /// `ExecID (17)` — the composite execution id.
    pub execution_id: String,
    /// `LastLiquidityInd (851)` mapped to the wire word (`maker` / `taker`).
    pub liquidity: String,
    /// `SecondaryExecID (527)` — the `underlying_sequence` join key.
    pub underlying_sequence: u64,
    /// `Side (54)` mapped to the wire word (`buy` / `sell`).
    pub side: String,
    /// `LastQty (32)`.
    pub quantity: u64,
    /// `LastPx (31)` parsed back to integer cents through the price seam.
    pub price: u64,
}

/// Extracts the [`FixReportProjection`] from a `Trade` `ExecutionReport (8)` frame,
/// or `None` if `frame` is not a `Trade` report.
#[must_use]
pub fn fix_report_projection(frame: &[u8]) -> Option<FixReportProjection> {
    if msg_type(frame).as_deref() != Some("8") {
        return None;
    }
    if field(frame, "150").as_deref() != Some("F") {
        return None; // only a Trade leg carries the fill join keys
    }
    let liquidity = match field(frame, "851")?.as_str() {
        "1" => "maker",
        "2" => "taker",
        _ => return None,
    }
    .to_string();
    let side = match field(frame, "54")?.as_str() {
        "1" => "buy",
        "2" => "sell",
        _ => return None,
    }
    .to_string();
    let price = fauxchange::gateway::fix::price::parse_decimal_to_cents(&field(frame, "31")?)
        .ok()?
        .get();
    Some(FixReportProjection {
        execution_id: field(frame, "17")?,
        liquidity,
        underlying_sequence: field(frame, "527")?.parse().ok()?,
        side,
        quantity: field(frame, "32")?.parse().ok()?,
        price,
    })
}
