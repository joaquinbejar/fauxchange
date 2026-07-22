//! The v0.5 **DoS-control security-gate** suite (#48) — proves the five bounds
//! wired by earlier issues are **security controls** (a bounded resource
//! ceiling under adversarial flood), not merely fairness knobs
//! ([08 §5](../docs/08-threat-model.md#5-denial-of-service-posture),
//! [TESTING.md §14](../docs/TESTING.md#14-security-testing)).
//!
//! `tests/security.rs` §4 already covers each control at a correctness level —
//! a handful of requests past the bound get a typed reject. This suite goes
//! one step further for every bound: it floods far past the bound (hundreds to
//! thousands of attempts / distinct keys) and asserts the control's **own
//! bookkeeping** — a tracked-key count, a mailbox queue depth, a broadcast
//! backlog, a connection/subscription count — never grows past its configured
//! ceiling, no matter how large the flood grows. Every control is driven
//! through its real production seam (the real `RateLimiter`, the real spawned
//! actor + bounded `mpsc`, the real `OrderbookSubscriptionManager` broadcast,
//! the real `ws_handler` / FIX acceptor, the real checked sequence counter) —
//! nothing here is mocked.
//!
//! Kept in the **fast** suite: no `#[ignore]`, no `SOAK=1` gate, no long
//! sleeps. Sustained-duration DoS testing is a separate v0.6 concern (#54).
//!
//! 1. **Rate limiter** — the tracked key-space never grows past `max_keys`
//!    under a flood of thousands of distinct callers
//!    ([`test_dos_rate_limiter_key_space_bounded_under_flood_of_distinct_keys`]),
//!    and one `AccountId`'s budget is the **same** shared [`RateLimiter`]
//!    instance REST and FIX both consult, so exhausting it on one surface
//!    throttles the other
//!    ([`test_dos_rate_limiter_one_budget_shared_across_rest_and_fix`]).
//! 2. **Bounded actor mailbox** — a concurrent flood of submissions against a
//!    single-slot mailbox never queues past its capacity; every over-capacity
//!    attempt gets an immediate typed [`fauxchange::VenueError::RateLimited`],
//!    never an unbounded queue
//!    ([`test_dos_bounded_actor_mailbox_flood_never_queues_past_capacity`]).
//! 3. **Bounded broadcast** — a flood of committed events against a
//!    small-capacity broadcast ring never grows a non-draining receiver's
//!    backlog past that capacity; the laggard is dropped and a fresh snapshot
//!    still reflects every folded mutation
//!    ([`test_dos_bounded_broadcast_drops_a_laggard_and_never_grows_past_capacity_under_flood`]).
//! 4. **Connection + per-connection subscription caps** — a flood of connection
//!    attempts past the venue cap
//!    ([`test_dos_connection_cap_bounds_concurrent_sockets_under_flood`]), and a
//!    flood of subscribe requests over one real WebSocket past the
//!    per-connection cap
//!    ([`test_dos_ws_subscription_cap_bounds_per_connection_topics_under_flood`]),
//!    are each refused without growing tracked resource use past the
//!    configured ceiling.
//! 5. **Sequence-exhaustion sealing** — a counter seeded at `u64::MAX` via the
//!    actor's own test seam (`ActorConfig::start_sequence`) returns a typed
//!    [`fauxchange::VenueError::SequenceExhausted`] and stays sealed under a
//!    flood of further attempts, rather than wrapping
//!    ([`test_dos_sequence_exhaustion_seals_permanently_under_flood`]).

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, watch};
use tower::ServiceExt;

use fauxchange::VenueError;
use fauxchange::auth::{AccountProvision, CompIdBinding, RateLimitKey, RateLimiter};
use fauxchange::exchange::{
    ActorConfig, Cents, EventTimestamp, FixedClock, Hash32, InMemoryVenueJournal, JournalHeader,
    LineageId, NoopFanOut, PlaceholderExecutor, STPMode, SequenceNumber, Side as SeamSide, Symbol,
    TimeInForce as SeamTif, UnderlyingActor, VenueCommand, VenueEvent, VenueOutcome,
    spawn_underlying_actor,
};
use fauxchange::gateway::fix::{
    FixAcceptor, FixAcceptorConfig, FixSessionStore, InMemoryFixSessionStore, SessionConfig,
    VenueFixSessionFactory,
};
use fauxchange::gateway::rest::create_router;
use fauxchange::gateway::ws::MAX_SUBSCRIPTIONS_PER_CONNECTION;
use fauxchange::models::{AccountId, OrderType, Permission, VenueOrderId, WsMessage};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};
use fauxchange::subscription::{OrderbookSubscriptionManager, WS_BROADCAST_CAPACITY};

/// The concrete per-contract order-entry path for `BTC` call `50000 / 20240329`
/// (the same fixture contract every other integration suite uses).
const ORDER_PATH: &str =
    "/api/v1/underlyings/BTC/expirations/20240329/strikes/50000/options/call/orders";

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// Bound 1a — the rate limiter's tracked key-space is bounded under a flood of
// thousands of distinct callers (never one bucket per attacker-controlled key).
// ============================================================================

#[test]
fn test_dos_rate_limiter_key_space_bounded_under_flood_of_distinct_keys() {
    // A key-space ceiling far below the flood size: thousands of distinct
    // callers (the classic spoofed-source-IP flood) must never grow the
    // limiter's tracked bucket count past `max_keys` — the DoS bound named at
    // `src/auth.rs` `RateLimiter::max_keys` / `DEFAULT_MAX_RATE_LIMIT_KEYS`. The
    // clock never advances, so no window ever expires mid-flood: every denial
    // is attributable to the key-space ceiling alone, not to a lucky sweep.
    let clock = FixedClock::new(EventTimestamp::new(0));
    let max_keys: usize = 64;
    let limiter = RateLimiter::with_capacity(clock, 100, 60_000, max_keys);

    let flood_size: u32 = 5_000;
    let mut denied: u32 = 0;
    for i in 0..flood_size {
        let key = RateLimitKey::Peer(IpAddr::V4(Ipv4Addr::new(
            10,
            (i >> 16) as u8,
            (i >> 8) as u8,
            i as u8,
        )));
        let decision = limiter.check_and_record_status(&key);
        if !decision.allowed {
            denied += 1;
        }
        // The core DoS bound: the tracked key-space NEVER exceeds the
        // configured ceiling, no matter how large the flood grows.
        assert!(
            limiter.tracked_keys() <= max_keys,
            "tracked key-space {} exceeded max_keys ({max_keys}) at flood index {i}",
            limiter.tracked_keys(),
        );
    }
    assert_eq!(
        denied,
        flood_size - u32::try_from(max_keys).unwrap_or(0),
        "exactly the flood attempts past the {max_keys}-key ceiling fail closed"
    );
    assert_eq!(
        limiter.tracked_keys(),
        max_keys,
        "post-flood the tracked key-space sits exactly at (never past) the ceiling"
    );
}

// ============================================================================
// Bound 1b — one `AccountId`'s budget is the SAME shared limiter across REST
// and FIX: exhausting it on one surface throttles the other.
// ============================================================================

#[tokio::test]
async fn test_dos_rate_limiter_one_budget_shared_across_rest_and_fix() {
    // A uniform 1-request/window budget: a single REST order spends the whole
    // budget, then a FIX order for the SAME account is refused by the SAME
    // shared `RateLimiter` (`src/gateway/fix/fsm.rs` `rate_limited()` builds
    // the byte-identical `RateLimitKey::Account`) — proving one budget spans
    // both surfaces, which is what stops a flood from evading the limiter by
    // round-robining protocols.
    const SECRET: &str = "dos-cross-surface-secret";
    const TRADER_SENDER: &str = "DOSTRADER";
    const VENUE_COMP: &str = "FAUXCHANGE";
    const FIX_USER: &str = "dos-trader-fix";
    const FIX_PW: &str = "dos-trader-plaintext-pw-DoNotLog";

    let accounts = vec![
        AccountProvision::new(
            AccountId::new("dos-trader-1"),
            Hash32([9; 32]),
            vec![Permission::Trade],
        )
        .with_fix_login(FIX_USER, FIX_PW)
        .with_comp_ids(CompIdBinding {
            sender_comp_id: TRADER_SENDER.to_string(),
            target_comp_id: VENUE_COMP.to_string(),
        }),
    ];
    let auth = AuthConfig::dev()
        .expect("dev auth must build")
        .with_bootstrap_secret(SECRET)
        .with_accounts(accounts)
        .with_rate_limit(1);
    let state =
        AppState::new(AppStateConfig::new(["BTC"]).with_auth(auth)).expect("AppState must build");

    // Consume the account's whole budget over REST.
    let bearer = state
        .mint_token(&AccountId::new("dos-trader-1"), SECRET, now_secs(), 3_600)
        .expect("minting must succeed");
    let request = Request::builder()
        .method("POST")
        .uri(ORDER_PATH)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"side": "buy", "price": 50_000, "quantity": 1}))
                .expect("serialise body"),
        ))
        .expect("build request");
    let response = create_router(Arc::clone(&state))
        .oneshot(request)
        .await
        .expect("router is infallible");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the first REST order is within the account's whole (1-request) budget"
    );

    // Start a real FIX acceptor wired to the SAME `AppState` (the SAME shared
    // `RateLimiter` behind `state.auth().rate_limiter()`).
    let config = FixAcceptorConfig {
        addr: "127.0.0.1:0".parse().expect("addr"),
        connection_cap: 4,
        mailbox_depth: 16,
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
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(acceptor.serve(factory, shutdown_rx));

    let mut client = TcpStream::connect(addr).await.expect("connect");
    client
        .write_all(&logon_frame(
            TRADER_SENDER,
            VENUE_COMP,
            1,
            FIX_USER,
            FIX_PW,
            false,
        ))
        .await
        .expect("logon write");
    let logon_reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    assert!(
        any_msg_type(&logon_reply, "A"),
        "the FIX logon succeeds independently of the (unrelated, peer-keyed) logon rate limit, \
         got {logon_reply:?}"
    );

    // The account's REST-exhausted budget throttles the FIX order too.
    client
        .write_all(&limit_order_frame(
            TRADER_SENDER,
            VENUE_COMP,
            2,
            "dos-cross-1",
            "1",
            "500.00",
            1,
        ))
        .await
        .expect("order write");
    let reply = recv_frames(&mut client, Duration::from_secs(3)).await;
    let report = reply
        .iter()
        .find(|f| msg_type(f).as_deref() == Some("8"))
        .expect("an ExecutionReport for the throttled FIX order");
    assert_eq!(
        field(report, "39").as_deref(),
        Some("8"),
        "OrdStatus Rejected"
    );
    assert_eq!(
        field(report, "103").as_deref(),
        Some("99"),
        "OrdRejReason 99 (Other) — FIX 4.4 has no dedicated throttle code"
    );
    assert_eq!(
        field(report, "58").as_deref(),
        Some("rate limited"),
        "Text (58) names the throttle explicitly, ruling out a coincidental reject \
         (an unauthorized / not-found / invalid-order reject renders different text)"
    );

    let _ = shutdown_tx.send(true);
}

// ============================================================================
// Bound 2 — a concurrent flood against a single-slot actor mailbox never
// queues past its configured capacity.
// ============================================================================

/// A `CommandExecutor` that blocks in `execute` until a gate is released, so
/// the actor is stuck on one command while the flood hammers the mailbox.
struct GatedExecutor {
    release: Arc<AtomicBool>,
}

impl fauxchange::exchange::CommandExecutor for GatedExecutor {
    fn execute(&mut self, _context: fauxchange::exchange::ExecutionContext<'_>) -> VenueOutcome {
        while !self.release.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
        VenueOutcome::ControlApplied { swept: vec![] }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dos_bounded_actor_mailbox_flood_never_queues_past_capacity() {
    let release = Arc::new(AtomicBool::new(false));
    let config = ActorConfig {
        underlying: Arc::from("BTC"),
        lineage_id: LineageId::new("dos-mailbox-flood"),
        mailbox_capacity: 1,
        start_sequence: SequenceNumber::START,
    };
    let journal =
        InMemoryVenueJournal::new(JournalHeader::new(LineageId::new("dos-mailbox-flood")));
    let (handle, join) = spawn_underlying_actor(
        config,
        journal,
        GatedExecutor {
            release: Arc::clone(&release),
        },
        NoopFanOut,
        FixedClock::new(EventTimestamp::new(0)),
    );

    let clock_cmd = || VenueCommand::Clock {
        now_ms: EventTimestamp::new(1),
    };

    // Command A: dequeued immediately, blocks in `execute` (gate closed).
    let handle_a = handle.clone();
    let a = tokio::spawn(async move { handle_a.submit(clock_cmd()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Command B: fills the single mailbox slot behind A.
    let handle_b = handle.clone();
    let b = tokio::spawn(async move { handle_b.submit(clock_cmd()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The flood: hundreds of CONCURRENT submissions while the mailbox is full
    // and the actor is busy. Every single one must be rejected immediately —
    // none may queue, no matter how large the flood.
    let flood_size: usize = 300;
    let mut flood_handles = Vec::with_capacity(flood_size);
    for _ in 0..flood_size {
        let handle_c = handle.clone();
        flood_handles.push(tokio::spawn(
            async move { handle_c.submit(clock_cmd()).await },
        ));
    }
    let mut rejected: usize = 0;
    for task in flood_handles {
        match task.await.expect("a flood task must not panic") {
            Err(VenueError::RateLimited) => rejected += 1,
            other => panic!("a full mailbox under flood must reject, got {other:?}"),
        }
    }
    assert_eq!(
        rejected, flood_size,
        "every one of {flood_size} concurrent over-capacity submissions is rejected — the \
         mailbox queue depth never grows past its configured capacity, regardless of flood size"
    );

    // Release: A and B — the only two ever admitted — drain and complete.
    release.store(true, Ordering::SeqCst);
    assert!(a.await.expect("join A").is_ok());
    assert!(b.await.expect("join B").is_ok());

    drop(handle);
    join.await.expect("the actor shuts down cleanly");
}

// ============================================================================
// Bound 3 — a flood of committed events against a small broadcast ring never
// grows a non-draining receiver's backlog past the ring's own capacity.
// ============================================================================

fn dos_symbol() -> Symbol {
    Symbol::parse("BTC-20240329-50000-C").expect("a valid fixture symbol")
}

/// A resting add whose committed outcome opens a fresh book level (and thus
/// emits one `orderbook_delta`) — the minimal event to drive the fan-out.
fn dos_resting_add(seq: u64, order_id: &str, price: u64) -> VenueEvent {
    VenueEvent::new(
        SequenceNumber::new(seq),
        EventTimestamp::new(1),
        VenueCommand::AddOrder {
            symbol: dos_symbol(),
            order_id: VenueOrderId::new(order_id),
            account: AccountId::new("acct"),
            owner: Hash32([0x11; 32]),
            client_order_id: None,
            side: SeamSide::Sell,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity: 1,
            time_in_force: SeamTif::Gtc,
            stp_mode: STPMode::None,
        },
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: 1,
            stp_cancelled: vec![],
        },
    )
}

#[test]
fn test_dos_bounded_broadcast_drops_a_laggard_and_never_grows_past_capacity_under_flood() {
    let capacity: usize = 8;
    let manager = OrderbookSubscriptionManager::with_capacity(capacity);
    let mut receiver = manager.subscribe();

    // A flood two orders of magnitude past the ring's capacity, with the
    // receiver never draining. The ring's own STORAGE is a fixed-size buffer
    // (`capacity` slots, allocated once, each overwritten in place) — sending
    // far past capacity never grows that allocation; `Receiver::len()` is just
    // a send/receive distance counter (two integers), not a materialized
    // backlog, so it is deliberately NOT asserted bounded here. What must stay
    // bounded is what the receiver has to actually **process** to catch up,
    // asserted below.
    let flood_size: u64 = 2_000;
    for i in 0..flood_size {
        manager.on_committed_event(&dos_resting_add(i, &format!("m{i}"), 50_000 + i));
    }

    // The core DoS bound: catching up costs the receiver ONE `Lagged`
    // notification (not a replay of `flood_size` stale messages), and after it
    // at most `capacity` buffered messages are left to drain — never
    // `flood_size` of them. A non-draining consumer can never force the venue
    // to hold (or the consumer to process) an OOM-scale backlog.
    match receiver.try_recv() {
        Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
            assert!(
                skipped >= flood_size - u64::try_from(capacity).unwrap_or(0),
                "the reported skip ({skipped}) should account for nearly the whole flood \
                 ({flood_size}), proving the backlog was never replayed message-by-message"
            );
        }
        other => panic!(
            "a non-draining receiver behind a {flood_size}-event flood must lag, got {other:?}"
        ),
    }
    let mut drained_after_lag = 0usize;
    while receiver.try_recv().is_ok() {
        drained_after_lag += 1;
    }
    assert!(
        drained_after_lag <= capacity,
        "only {drained_after_lag} messages remained to drain after the lag skip — must never \
         exceed the ring capacity ({capacity}), regardless of the {flood_size}-event flood"
    );

    // Recovery: a fresh snapshot still reflects every folded mutation, even
    // though the broadcast ring itself never held more than `capacity` at once.
    match manager.orderbook_snapshot(&dos_symbol(), None) {
        WsMessage::OrderbookSnapshot { asks, .. } => {
            assert_eq!(
                asks.len(),
                usize::try_from(flood_size).unwrap_or(usize::MAX),
                "the re-snapshot reflects every resting level from the whole flood"
            );
        }
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

// ============================================================================
// Bound 4a — a flood of connection attempts past the venue cap is refused
// without growing the concurrent-connection count past the cap.
// ============================================================================

#[test]
fn test_dos_connection_cap_bounds_concurrent_sockets_under_flood() {
    let cap: usize = 4;
    let manager = OrderbookSubscriptionManager::with_limits(WS_BROADCAST_CAPACITY, cap);
    let mut held = Vec::with_capacity(cap);
    for _ in 0..cap {
        held.push(
            manager
                .try_acquire_connection()
                .expect("the first `cap` connections must be admitted"),
        );
    }

    let flood_size: usize = 2_000;
    let mut refused: usize = 0;
    for _ in 0..flood_size {
        if manager.try_acquire_connection().is_none() {
            refused += 1;
        }
    }
    assert_eq!(
        refused, flood_size,
        "every one of {flood_size} over-cap connection attempts is refused — the concurrent \
         connection count never exceeds {cap}, no matter how large the flood"
    );
    assert_eq!(manager.available_connection_slots(), 0);

    // Releasing one slot reopens exactly one — the cap is not a permanent
    // lockout, and reclaiming never over-admits.
    held.truncate(cap - 1);
    assert_eq!(manager.available_connection_slots(), 1);
    let reopened = manager.try_acquire_connection();
    assert!(reopened.is_some(), "a released slot is reclaimed");
    assert_eq!(manager.available_connection_slots(), 0);
    drop(reopened);
}

// ============================================================================
// Bound 4b — a flood of subscribe requests over ONE real WebSocket past the
// per-connection cap is refused without growing tracked subscriptions past it.
// ============================================================================

/// Writes one masked WS **text** frame (client → server framing requires
/// masking; every payload here is a short JSON subscribe request).
fn ws_write_text(stream: &mut std::net::TcpStream, text: &str) {
    let payload = text.as_bytes();
    let len = payload.len();
    let mut frame = Vec::with_capacity(len + 14);
    frame.push(0x81); // FIN + text opcode
    if len < 126 {
        frame.push(0x80 | (len as u8));
    } else {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(len as u16).to_be_bytes());
    }
    let mask = [0x3d, 0x17, 0x9a, 0x5c];
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    stream.write_all(&frame).expect("write ws frame");
}

/// Parses ONE complete WS frame at the start of `buf` (server → client,
/// unmasked): returns `(opcode, payload_bytes, bytes_consumed)`, or `None`
/// when `buf` holds fewer bytes than one full frame (a normal "read more"
/// signal) OR the leading byte's low nibble is not a WS opcode this venue's
/// server ever sends — a desync guard: never reinterpret misaligned bytes as
/// a frame header (validated BEFORE the next byte is read as a length field).
fn parse_one_ws_frame(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buf.len() < 2 {
        return None;
    }
    let opcode = buf[0] & 0x0f;
    // Continuation (0x0), text (0x1), binary (0x2), close (0x8), ping (0x9),
    // pong (0xA) — the full legal WS opcode set; anything else means `buf`
    // is not aligned on a frame boundary and must not be read as one.
    if !matches!(opcode, 0x0 | 0x1 | 0x2 | 0x8 | 0x9 | 0xa) {
        return None;
    }

    let b1 = buf[1];
    let masked = b1 & 0x80 != 0;
    let mut len = usize::from(b1 & 0x7f);
    let mut header_len: usize = 2;
    if len == 126 {
        if buf.len() < 4 {
            return None;
        }
        len = usize::from(u16::from_be_bytes([buf[2], buf[3]]));
        header_len = 4;
    } else if len == 127 {
        if buf.len() < 10 {
            return None;
        }
        let mut extended = [0u8; 8];
        extended.copy_from_slice(&buf[2..10]);
        len = usize::try_from(u64::from_be_bytes(extended)).unwrap_or(usize::MAX);
        header_len = 10;
    }
    if masked {
        header_len += 4;
    }
    let total = header_len.checked_add(len)?;
    if total > buf.len() {
        return None;
    }
    Some((opcode, buf[header_len..total].to_vec(), total))
}

/// Parses every complete frame at the FRONT of `buf`, returning the **text**
/// payload of each `Text (0x1)` frame — a non-text opcode (ping/pong/close/
/// continuation) is skipped, never misread as a message — plus how many
/// bytes were consumed in total, so the caller can carry a trailing
/// partial-frame remainder forward instead of discarding it.
fn parse_ws_text_frames(buf: &[u8]) -> (Vec<String>, usize) {
    let mut out = Vec::new();
    let mut pos: usize = 0;
    while let Some((opcode, payload, consumed)) = parse_one_ws_frame(&buf[pos..]) {
        if opcode == 0x1
            && let Ok(text) = std::str::from_utf8(&payload)
        {
            out.push(text.to_string());
        }
        pos += consumed;
    }
    (out, pos)
}

/// Sends the raw HTTP `GET /ws` upgrade request and returns the now-upgraded
/// blocking stream plus any bytes read past the HTTP header terminator.
fn ws_upgrade(addr: SocketAddr, bearer: &str) -> (std::net::TcpStream, Vec<u8>) {
    let mut stream = std::net::TcpStream::connect(addr).expect("connect must succeed");
    let request = format!(
        "GET /ws HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Authorization: Bearer {bearer}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write handshake");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    let mut buf = Vec::new();
    let mut scratch = [0u8; 1024];
    loop {
        let n = stream.read(&mut scratch).expect("read handshake response");
        assert!(n > 0, "server closed before completing the handshake");
        buf.extend_from_slice(&scratch[..n]);
        let text = String::from_utf8_lossy(&buf);
        if let Some(header_end) = text.find("\r\n\r\n") {
            let status: u16 = text
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|code| code.parse().ok())
                .unwrap_or(0);
            assert_eq!(status, 101, "the WS handshake must upgrade");
            let leftover = buf[header_end + 4..].to_vec();
            return (stream, leftover);
        }
    }
}

/// Binds an ephemeral port and serves the router; the spawned task lives until
/// the test's own runtime tears down.
async fn spawn_ws_server(state: Arc<AppState>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind must succeed");
    let addr = listener.local_addr().expect("local_addr must succeed");
    let router = create_router(state);
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    addr
}

/// Drives the real `GET /ws` handshake, drains the `Connected` welcome, floods
/// `flood_size` distinct-symbol `subscribe` requests over the ONE connection,
/// and returns `(subscribed_count, cap_rejected_count)` from the replies.
fn subscription_cap_flood(addr: SocketAddr, bearer: &str, flood_size: usize) -> (usize, usize) {
    let (mut stream, leftover) = ws_upgrade(addr, bearer);

    // Drain the `Connected` welcome before flooding, so it is never miscounted
    // as a `subscribed` / `error` reply below. `HEARTBEAT_INTERVAL_SECS` fires
    // an IMMEDIATE first tick, so a heartbeat frame can race the welcome and
    // land in the same read as (or split across a read boundary from) it —
    // whatever bytes `buf` holds beyond the frames actually consumed here are
    // carried forward into `reply_buf` below, never discarded, so a frame
    // split mid-byte-stream can never desync the flood-reply parse that
    // follows.
    let mut buf = leftover;
    let (welcome, reply_seed) = loop {
        let (frames, consumed) = parse_ws_text_frames(&buf);
        if let Some(first) = frames.into_iter().next() {
            break (first, buf[consumed..].to_vec());
        }
        let mut scratch = [0u8; 1024];
        let n = stream.read(&mut scratch).expect("read welcome");
        assert!(n > 0, "server closed before the welcome frame");
        buf.extend_from_slice(&scratch[..n]);
    };
    assert!(
        welcome.contains("\"connected\""),
        "the first frame is the connection welcome, got {welcome:?}"
    );

    for i in 0..flood_size {
        let symbol = format!("BTC-20240329-{}-C", 10_000 + i);
        let request = format!(r#"{{"action":"subscribe","channel":"trades","symbol":"{symbol}"}}"#);
        ws_write_text(&mut stream, &request);
    }

    // Every 30 s heartbeat tick also fires an IMMEDIATE first tick
    // (`tokio::time::interval`'s documented behaviour), so a stray
    // `{"type":"heartbeat",...}` frame can interleave anywhere in the flood's
    // replies — filtered out below rather than counted as a subscribe/cap
    // reply. Read until the two reply categories together reach the flood
    // size (not until the raw frame count does), so an interleaved heartbeat
    // never causes an early / hung exit.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    // Seeded with whatever bytes were read (but not consumed as the welcome
    // frame) above — never start this buffer empty when `buf` still held a
    // partial/next frame, or the parse below would desync.
    let mut reply_buf = reply_seed;
    let mut messages: Vec<String> = Vec::new();
    let relevant_count = |messages: &[String]| -> usize {
        messages
            .iter()
            .filter(|m| {
                m.contains("\"subscribed\"") || (m.contains("\"error\"") && m.contains("cap"))
            })
            .count()
    };
    while relevant_count(&messages) < flood_size && std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let poll = remaining
            .min(Duration::from_millis(200))
            .max(Duration::from_millis(1));
        let _ = stream.set_read_timeout(Some(poll));
        let mut scratch = [0u8; 16 * 1024];
        match stream.read(&mut scratch) {
            Ok(0) => break,
            Ok(n) => {
                reply_buf.extend_from_slice(&scratch[..n]);
                let (parsed, _consumed) = parse_ws_text_frames(&reply_buf);
                messages = parsed;
            }
            // A per-poll read timeout is expected while the server is still
            // mid-flight on the flood — keep polling until every reply has
            // arrived or the overall deadline elapses; only a genuine I/O
            // error ends the read early.
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(_) => break,
        }
    }

    let subscribed = messages
        .iter()
        .filter(|m| m.contains("\"subscribed\""))
        .count();
    let capped = messages
        .iter()
        .filter(|m| m.contains("\"error\"") && m.contains("cap"))
        .count();
    (subscribed, capped)
}

#[tokio::test]
async fn test_dos_ws_subscription_cap_bounds_per_connection_topics_under_flood() {
    const SECRET: &str = "dos-ws-subscription-secret";
    let accounts = vec![AccountProvision::new(
        AccountId::new("dos-reader-1"),
        Hash32([7; 32]),
        vec![Permission::Read],
    )];
    let auth = AuthConfig::dev()
        .expect("dev auth must build")
        .with_bootstrap_secret(SECRET)
        .with_accounts(accounts)
        .with_rate_limit(10_000);
    let state =
        AppState::new(AppStateConfig::new(["BTC"]).with_auth(auth)).expect("AppState must build");
    let bearer = state
        .mint_token(&AccountId::new("dos-reader-1"), SECRET, now_secs(), 3_600)
        .expect("minting must succeed");
    let addr = spawn_ws_server(Arc::clone(&state)).await;

    let flood_size = MAX_SUBSCRIPTIONS_PER_CONNECTION + 100;
    let (subscribed, capped) =
        tokio::task::spawn_blocking(move || subscription_cap_flood(addr, &bearer, flood_size))
            .await
            .expect("the blocking flood task must not panic");

    assert_eq!(
        subscribed, MAX_SUBSCRIPTIONS_PER_CONNECTION,
        "exactly the per-connection cap's worth of subscriptions is accepted — never more, no \
         matter how many the flood attempts"
    );
    assert_eq!(
        capped,
        flood_size - MAX_SUBSCRIPTIONS_PER_CONNECTION,
        "every over-cap subscribe attempt in the flood gets the typed cap-error reject"
    );
}

// ============================================================================
// Bound 5 — a sequence counter driven to `u64::MAX` seals with a typed error,
// and STAYS sealed under a flood of further attempts (never wraps).
// ============================================================================

#[test]
fn test_dos_sequence_exhaustion_seals_permanently_under_flood() {
    // The seam: `ActorConfig::start_sequence` seeds the checked `u64` sequence
    // counter directly at its ceiling, rather than actually driving billions of
    // commands to reach `u64::MAX` — the same test-only seam
    // `src/exchange/actor.rs`'s own unit tests and `tests/security.rs` use.
    let config = ActorConfig {
        underlying: Arc::from("BTC"),
        lineage_id: LineageId::new("dos-exhaust-flood"),
        mailbox_capacity: 8,
        start_sequence: SequenceNumber::new(u64::MAX),
    };
    let journal =
        InMemoryVenueJournal::new(JournalHeader::new(LineageId::new("dos-exhaust-flood")));
    let mut actor = UnderlyingActor::new(
        config,
        journal,
        PlaceholderExecutor,
        NoopFanOut,
        FixedClock::new(EventTimestamp::new(0)),
    );

    let command = VenueCommand::Clock {
        now_ms: EventTimestamp::new(1),
    };
    assert!(
        actor.handle(command.clone()).is_ok(),
        "the final available sequence (u64::MAX) still commits"
    );
    match actor.handle(command.clone()) {
        Err(VenueError::SequenceExhausted) => {}
        other => panic!("an exhausted sequence must seal, got {other:?}"),
    }

    // The seal is PERMANENT: a flood of further attempts after the seal is
    // never re-admitted, and the checked counter never silently wraps back to
    // resume — the underlying stays sealed for good, regardless of how many
    // more commands hammer it.
    for _ in 0..500 {
        match actor.handle(command.clone()) {
            Err(VenueError::SequenceExhausted) => {}
            other => panic!("a sealed underlying must stay sealed under flood, got {other:?}"),
        }
    }
}

// ============================================================================
// Shared FIX wire helpers (mirroring `tests/fix_session.rs`'s conventions —
// each integration-test file keeps its own minimal copy, no shared support
// module).
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

async fn recv_frames(stream: &mut TcpStream, timeout: Duration) -> Vec<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut buf = Vec::new();
    let mut scratch = [0u8; 4096];
    while tokio::time::Instant::now() < deadline {
        let read =
            tokio::time::timeout(Duration::from_millis(200), stream.read(&mut scratch)).await;
        match read {
            Ok(Ok(0)) => break,
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
