//! Docker end-to-end smoke test (#27) — the "one command" proof.
//!
//! ## Gating (the main suite stays green WITHOUT Docker)
//!
//! Gated on `DOCKER=1`: [`test_docker_e2e_smoke`] checks the env var FIRST and, if
//! unset, prints a skip line and returns `Ok(())` immediately — no container
//! is ever started by a plain `cargo test`. It runs the real flow only under
//! `DOCKER=1 cargo test --test docker_smoke -- --nocapture` (the dedicated CI
//! `docker-smoke` stage, `.github/workflows/ci.yml`, or `make docker-smoke`
//! locally).
//!
//! ## What it proves
//!
//! `docker compose -f docker/docker-compose.yml up -d` (the image built ONCE,
//! untimed, before the measured window — [`docs/07-performance-budgets.md`
//! §7](../docs/07-performance-budgets.md#7-what-is-explicitly-out-of-budget)
//! is explicit that cold start is container-start + self-seed → serving, NOT
//! `cargo build --release`, which runs in minutes) reaches a **serving,
//! seeded** venue in **< 30 s cold** ([PRD NFR-3](../docs/PRD.md#4-non-functional-requirements)),
//! a market order placed over REST against a **seeded** contract fills
//! against the seeded market-maker's resting quote, and that fill is observed
//! over the SAME `fills` WS channel — proving REST order entry and WS
//! observation share one real order path end to end, against the actual
//! shipped container, not an in-process test harness.
//!
//! **Why polling `GET /health` alone measures "serving AND seeded."**
//! `src/main.rs` runs the bounded seeding phase
//! ([`fauxchange::seed::apply_seed_phase`]) and calls `AppState::begin_serving()`
//! **before** it ever calls `rest::serve` — the REST listener does not exist
//! until seeding has already completed. So the first successful `GET /health`
//! response IS the serving-and-seeded signal; there is no separate "is the
//! chain queryable yet" race to account for.
//!
//! ## Design constraints honoured
//!
//! - **Wall-clock NFR, not a `bench-hdr` quantile**
//!   ([TESTING.md §13](../docs/TESTING.md#13-benchmarks)): the cold-bring-up
//!   time is one real measured duration, printed and asserted against the
//!   budget — never a fabricated number, never a synthesized percentile.
//! - **Hard bound on the whole test.** [`OVERALL_TEST_TIMEOUT`] wraps the
//!   entire flow; a hang anywhere (a stuck `docker` subprocess, a WS frame
//!   that never arrives) fails the test instead of hanging CI forever.
//! - **Guaranteed teardown.** [`ComposeGuard`] runs `docker compose down -v`
//!   in its `Drop` impl, which Rust runs whether the test returns `Ok`,
//!   returns `Err`, panics (stack unwinding drops live locals), or the
//!   overall timeout fires (`tokio::time::timeout` drops the inner future,
//!   which drops ITS live locals the same way) — a failed run never leaks
//!   containers or volumes. The CI job additionally runs an unconditional
//!   `docker compose down -v` step as a belt-and-braces safety net in case
//!   the test process itself is killed before `Drop` can run (e.g. a CI job
//!   timeout).
//!
//! ## Fixture grounding
//!
//! - Account: `market-taker` (`seeds/default.toml`, permissions
//!   `["read", "trade"]`) — the only seeded account with `Trade`.
//! - Bootstrap secret: `docker/docker-compose.yml`'s DEV DEFAULT
//!   (`AUTH_BOOTSTRAP_SECRET: "${AUTH_BOOTSTRAP_SECRET:-dev-bootstrap-secret-change-me}"`) —
//!   this test does not override the env var, so the compose default applies.
//! - Contract: `BTC-20261231-50000-C` (`seeds/default.toml`'s `[instruments.BTC]`:
//!   expiration `20261231`, strike `50000` — at-the-money against the
//!   $50,000.00 opening price). The default `balanced` persona's `Quoter`
//!   always produces a valid two-sided quote once a contract is registered
//!   and priced (`ask_price > bid_price`, both sizes `>= 1`,
//!   `src/market_maker/quoter.rs`), so a market BUY for quantity `1` is
//!   guaranteed to cross the seeded resting ask.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

/// The REST/WS host:port `docker/docker-compose.yml` publishes (`"8080:8080"`).
const REST_ADDR: &str = "127.0.0.1:8080";

/// The seeded `Trade`-permissioned account (`seeds/default.toml`).
const TAKER_ACCOUNT: &str = "market-taker";

/// `docker/docker-compose.yml`'s DEV DEFAULT `AUTH_BOOTSTRAP_SECRET` — this
/// test never overrides the env var, so the compose default is what the
/// running container actually gates token issuance with.
const BOOTSTRAP_SECRET: &str = "dev-bootstrap-secret-change-me";

/// The seeded at-the-money BTC call (`seeds/default.toml`: expiration
/// `20261231`, strike `50000`).
const UNDERLYING: &str = "BTC";
const EXPIRATION: &str = "20261231";
const STRIKE: u64 = 50000;
const STYLE: &str = "call";
/// The canonical contract symbol (`UNDERLYING-YYYYMMDD-STRIKE-STYLE`,
/// `src/exchange/symbol.rs`) — the `fills` WS channel subscribes and
/// classifies by this exact string.
const INSTRUMENT: &str = "BTC-20261231-50000-C";

/// [PRD NFR-3](../docs/PRD.md#4-non-functional-requirements) / [docs/07 §7](../docs/07-performance-budgets.md#7-what-is-explicitly-out-of-budget):
/// `docker compose up` → serving, seeded venue, cold, as a DESIGN TARGET.
const COLD_BRINGUP_BUDGET: Duration = Duration::from_secs(30);

/// The `/health` poll deadline — deliberately a little past
/// [`COLD_BRINGUP_BUDGET`] so a near-miss run still reports a real measured
/// number (and a clear "exceeded the budget" assertion) instead of an
/// ambiguous poll-loop timeout.
const HEALTH_POLL_DEADLINE: Duration = Duration::from_secs(45);

/// The hard wall-clock bound on the ENTIRE test, including the untimed image
/// build (which can run minutes on a cold cache) — bounds a stuck subprocess
/// or a WS frame that never arrives so a broken run fails instead of hanging
/// CI indefinitely.
const OVERALL_TEST_TIMEOUT: Duration = Duration::from_secs(600);

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The Docker e2e smoke test (#27) — self-skips cleanly without `DOCKER=1`.
#[tokio::test]
async fn test_docker_e2e_smoke() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("DOCKER").as_deref() != Ok("1") {
        eprintln!(
            "docker_smoke: skipping (DOCKER=1 not set) — set DOCKER=1 to run the real \
             `docker compose up` -> order -> WS fill flow"
        );
        return Ok(());
    }

    match tokio::time::timeout(OVERALL_TEST_TIMEOUT, run_smoke()).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "docker_smoke: exceeded the {OVERALL_TEST_TIMEOUT:?} hard wall-clock bound \
             (compose stack torn down by the Drop guard regardless)"
        )
        .into()),
    }
}

/// The compose file path, resolved from `CARGO_MANIFEST_DIR` (never the
/// process's current directory) so the test is correct regardless of where
/// `cargo test` was invoked from.
fn compose_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/docker-compose.yml")
}

/// Runs `docker compose -f <compose_file> down -v --remove-orphans` in its
/// `Drop` impl — the guaranteed-teardown mechanism described in the module
/// docs. Constructed BEFORE anything that can fail, so every early-return
/// error path (build/up/health/token/order/WS) and the overall timeout still
/// tear the stack down.
struct ComposeGuard {
    compose_file: PathBuf,
}

impl ComposeGuard {
    fn new(compose_file: PathBuf) -> Self {
        Self { compose_file }
    }
}

impl Drop for ComposeGuard {
    fn drop(&mut self) {
        eprintln!("docker_smoke: [Drop] tearing down the compose stack (guaranteed cleanup)");
        match Command::new("docker")
            .args([
                "compose",
                "-f",
                &self.compose_file.to_string_lossy(),
                "down",
                "-v",
                "--remove-orphans",
            ])
            .status()
        {
            Ok(status) if status.success() => {
                eprintln!("docker_smoke: [Drop] compose stack torn down cleanly");
            }
            Ok(status) => {
                eprintln!("docker_smoke: [Drop] `docker compose down` exited non-zero: {status}");
            }
            Err(error) => {
                eprintln!("docker_smoke: [Drop] failed to run `docker compose down`: {error}");
            }
        }
    }
}

/// The real flow, run only under `DOCKER=1` and wrapped by
/// [`OVERALL_TEST_TIMEOUT`] in [`test_docker_e2e_smoke`].
async fn run_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let compose_file = compose_file();
    // Constructed FIRST: every path out of this function from here on — early
    // `?`, an `assert!` panic, or the outer timeout dropping this future —
    // runs `ComposeGuard::drop`.
    let _guard = ComposeGuard::new(compose_file.clone());
    let compose_file_str = compose_file.to_string_lossy().into_owned();

    eprintln!(
        "docker_smoke: building the compose image once (untimed — excluded from the \
         cold-bring-up budget by design)"
    );
    run_compose(&compose_file_str, &["build"])?;

    eprintln!("docker_smoke: `docker compose up -d`");
    let bringup_start = Instant::now();
    run_compose(&compose_file_str, &["up", "-d"])?;

    wait_for_health(REST_ADDR, HEALTH_POLL_DEADLINE).await?;
    let cold_bringup = bringup_start.elapsed();
    eprintln!(
        "docker_smoke: measured cold bring-up = {:.3} s (DESIGN TARGET budget: {:.0} s)",
        cold_bringup.as_secs_f64(),
        COLD_BRINGUP_BUDGET.as_secs_f64()
    );

    let token = mint_token(REST_ADDR, BOOTSTRAP_SECRET, TAKER_ACCOUNT)?;
    eprintln!("docker_smoke: minted a bootstrap token for account '{TAKER_ACCOUNT}'");

    // Subscribe to the `fills` channel over WS BEFORE placing the order — a
    // fill broadcast is not replayed on subscribe (unlike an orderbook
    // snapshot), so subscribing after placing the order would race it.
    let ws_url = format!("ws://{REST_ADDR}/ws?token={token}");
    let (mut ws_stream, _response) = tokio_tungstenite::connect_async(&ws_url).await?;
    subscribe_fills(&mut ws_stream, INSTRUMENT).await?;
    eprintln!("docker_smoke: WS connected and subscribed to fills/{INSTRUMENT}");

    let (status, order_response) =
        place_market_order(REST_ADDR, &token, UNDERLYING, EXPIRATION, STRIKE, STYLE, 1)?;
    if status != 200 {
        return Err(
            format!("market order placement failed: HTTP {status}: {order_response}").into(),
        );
    }
    let filled_quantity = order_response
        .get("filled_quantity")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if filled_quantity == 0 {
        return Err(format!(
            "market order did not fill against the seeded market-maker quote \
             (expected the resting ask on {INSTRUMENT} to be crossed): {order_response}"
        )
        .into());
    }
    eprintln!(
        "docker_smoke: order placed and filled {filled_quantity} contract(s): {order_response}"
    );

    let fill = wait_for_fill(&mut ws_stream, INSTRUMENT, Duration::from_secs(15)).await?;
    eprintln!("docker_smoke: observed the fill over WS: {fill}");
    if fill.get("instrument").and_then(Value::as_str) != Some(INSTRUMENT) {
        return Err(format!("WS fill instrument mismatch: {fill}").into());
    }

    // No panics in the container logs.
    let logs = compose_logs(&compose_file_str)?;
    if logs.contains("panicked at") {
        return Err(format!("container logs show a panic:\n{logs}").into());
    }
    eprintln!("docker_smoke: container logs contain no panic");

    // Clean shutdown — asserted explicitly here (not left only to the Drop
    // guard) so a non-zero `docker compose down` is a reported test failure,
    // not a silent Drop-time eprintln.
    eprintln!("docker_smoke: `docker compose down -v` (clean-shutdown assertion)");
    run_compose(&compose_file_str, &["down", "-v", "--remove-orphans"])?;
    eprintln!("docker_smoke: clean shutdown confirmed");

    if cold_bringup >= COLD_BRINGUP_BUDGET {
        return Err(format!(
            "cold bring-up took {:.3} s, exceeding the {:.0} s DESIGN TARGET budget (NFR-3, \
             docs/07-performance-budgets.md §7)",
            cold_bringup.as_secs_f64(),
            COLD_BRINGUP_BUDGET.as_secs_f64()
        )
        .into());
    }

    Ok(())
}

/// Runs `docker compose -f <compose_file> <args...>`, returning an error
/// (with stderr attached) on a non-zero exit.
fn run_compose(compose_file: &str, args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let mut full_args = vec!["compose", "-f", compose_file];
    full_args.extend_from_slice(args);
    let output = Command::new("docker").args(&full_args).output()?;
    if !output.status.success() {
        return Err(format!(
            "docker {} exited {}: {}",
            full_args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

/// `docker compose -f <compose_file> logs` (all services), for the
/// no-panic-in-logs assertion.
fn compose_logs(compose_file: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("docker")
        .args(["compose", "-f", compose_file, "logs", "--no-color"])
        .output()?;
    // `logs` itself is best-effort observability, not a correctness gate —
    // report a nonzero exit as an empty log rather than failing the whole
    // test on a logging-plumbing hiccup.
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

/// Polls `GET /health` until it returns `200`, or `deadline` elapses.
async fn wait_for_health(addr: &str, deadline: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        if let Ok((200, _)) = http_json(addr, "GET", "/health", None, None) {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "docker_smoke: /health did not return 200 within {:.0} s",
                deadline.as_secs_f64()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// `POST /api/v1/auth/token` for a seeded account, returning the minted JWT.
fn mint_token(
    addr: &str,
    secret: &str,
    account: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let body = json!({
        "secret": secret,
        "account": account,
        // Advisory only — the venue resolves the account's REAL permissions
        // from the registry and ignores this field (src/gateway/rest/meta.rs
        // `issue_token` doc comment); still required by `TokenRequest`'s
        // shape (no `#[serde(default)]` on `permissions`).
        "permissions": ["read", "trade"],
    });
    let (status, response) = http_json(addr, "POST", "/api/v1/auth/token", None, Some(&body))?;
    if status != 200 {
        return Err(format!("token mint failed: HTTP {status}: {response}").into());
    }
    response
        .get("token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("token response missing `token`: {response}").into())
}

/// `POST .../orders/market` for one contract.
#[allow(clippy::too_many_arguments)]
fn place_market_order(
    addr: &str,
    token: &str,
    underlying: &str,
    expiration: &str,
    strike: u64,
    style: &str,
    quantity: u64,
) -> Result<(u16, Value), Box<dyn std::error::Error>> {
    let path = format!(
        "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/orders/market"
    );
    let body = json!({ "side": "buy", "quantity": quantity });
    Ok(http_json(addr, "POST", &path, Some(token), Some(&body))?)
}

/// Sends a `subscribe` action for `(fills, instrument)` and waits for its
/// `subscribed` ack, ignoring any earlier frame (the `connected` welcome,
/// heartbeats).
async fn subscribe_fills(
    ws: &mut WsStream,
    instrument: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let subscribe = json!({
        "action": "subscribe",
        "channel": "fills",
        "symbol": instrument,
    });
    ws.send(Message::Text(subscribe.to_string().into())).await?;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("timed out waiting for the fills-channel `subscribed` ack".into());
        }
        let frame = tokio::time::timeout(remaining, ws.next())
            .await
            .map_err(|_| "timed out waiting for the fills-channel `subscribed` ack")?
            .ok_or("WS closed before the fills-channel `subscribed` ack arrived")??;
        let Message::Text(text) = frame else { continue };
        let value: Value = serde_json::from_str(&text)?;
        if value.get("type").and_then(Value::as_str) == Some("subscribed")
            && value["data"]["channel"] == "fills"
            && value["data"]["symbol"] == instrument
        {
            return Ok(());
        }
    }
}

/// Waits for a `fill` broadcast matching `instrument`, returning its `data`
/// object.
async fn wait_for_fill(
    ws: &mut WsStream,
    instrument: &str,
    timeout: Duration,
) -> Result<Value, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("timed out waiting for a fill broadcast over WS".into());
        }
        let frame = tokio::time::timeout(remaining, ws.next())
            .await
            .map_err(|_| "timed out waiting for a fill broadcast over WS")?
            .ok_or("WS closed before a fill broadcast arrived")??;
        let Message::Text(text) = frame else { continue };
        let value: Value = serde_json::from_str(&text)?;
        if value.get("type").and_then(Value::as_str) == Some("fill")
            && value["data"]["instrument"] == instrument
        {
            return Ok(value["data"].clone());
        }
    }
}

// ============================================================================
// A minimal, hand-rolled HTTP/1.1 JSON client over a raw `TcpStream`.
// ============================================================================
//
// Deliberately NOT a new dependency: the REST leg only ever makes three
// simple JSON request/response calls (health poll, mint token, place one
// order), so a small, auditable client over the ALREADY-present
// `std::net::TcpStream` is simpler and adds less surface than wiring
// `hyper-util`'s client machinery (also already resolved in the tree via
// axum, but only for its own SERVER-side usage today). Every request sends
// `Connection: close`, so the response is read to EOF rather than needing a
// `Content-Length` / chunked-transfer parser.

/// Sends one JSON request and returns `(status, response_json)`.
///
/// `response_json` is [`Value::Null`] for an empty body. Blocking (`std::net`)
/// — called only from short, sequential points in the async flow above (never
/// concurrently with other work), so blocking the current task briefly is an
/// acceptable, deliberate simplification for a test client, not production
/// code.
fn http_json(
    addr: &str,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&Value>,
) -> std::io::Result<(u16, Value)> {
    let socket_addr: SocketAddr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::other(format!("could not resolve {addr}")))?;
    let mut stream = TcpStream::connect_timeout(&socket_addr, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    let body_bytes = body
        .map(|value| serde_json::to_vec(value).unwrap_or_default())
        .unwrap_or_default();

    let mut request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Connection: close\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n",
        body_bytes.len()
    );
    if let Some(token) = bearer {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");

    stream.write_all(request.as_bytes())?;
    if !body_bytes.is_empty() {
        stream.write_all(&body_bytes)?;
    }
    stream.flush()?;

    // Read to EOF (the server closes the connection per `Connection: close`),
    // tolerating a read timeout once SOME data has already arrived (a
    // defensive fallback, not the expected path).
    let mut raw = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if raw.is_empty() {
                    return Err(error);
                }
                break;
            }
            Err(error) => return Err(error),
        }
    }

    let text = String::from_utf8_lossy(&raw);
    let mut parts = text.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body_text = parts.next().unwrap_or_default();

    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);

    let json = if body_text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(body_text).unwrap_or(Value::Null)
    };

    Ok((status, json))
}
