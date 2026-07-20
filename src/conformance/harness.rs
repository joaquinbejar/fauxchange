//! The **conformance harness** — the ephemeral, in-process venue the packaged
//! `fauxchange conformance` run drives, plus the REST/FIX clients and the
//! order-entry drivers the cases share
//! ([051](../../milestones/v1.0-stability/051-conformance-harness.md)).
//!
//! A [`VenueServer`] binds one identically-seeded parity venue behind a real
//! ephemeral REST server *and* a real ephemeral FIX 4.4 acceptor on loopback, so
//! the cases exercise the true gateways over real sockets — never a private
//! matching shortcut. It is the library-side, production-grade sibling of the
//! `tests/conformance/` fixtures (#018/#041): the same four-account tier set, the
//! same owner hashes, the same default lineage and fixed clock, so two servers
//! are a valid per-surface parity pair, but with **no panics on inbound wire
//! data** — a malformed reply is a redacted [`CaseOutcome`](super::report::CaseOutcome)
//! failure, not a crash.
//!
//! Order-entry parity spins **one fresh [`VenueServer`] per surface** (the §7
//! topology: submitting the same order twice to one live actor cannot show
//! parity), while observation / control / conformance cases each take a single
//! fresh server. The report-producing case runners live in
//! [`super::cases`]; this module owns the plumbing they call.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::auth::{AccountProvision, CompIdBinding};
use crate::exchange::{Cents, Hash32, JournalRecord, VenueEvent};
use crate::gateway::fix::price::render_cents_to_decimal;
use crate::gateway::fix::{
    FixAcceptor, FixAcceptorConfig, FixSessionStore, InMemoryFixSessionStore, SessionConfig,
    VenueFixSessionFactory,
};
use crate::gateway::rest::create_router;
use crate::models::{AccountId, Permission};
use crate::state::{AppState, AppStateConfig, AuthConfig};

// ============================================================================
// Errors
// ============================================================================

/// A fatal harness-setup failure — the venue could not be assembled or a
/// loopback socket could not be bound. A *case* assertion failure is never one
/// of these; it is a redacted [`CaseOutcome`] recorded into the report.
#[derive(Debug, thiserror::Error)]
pub enum ConformanceError {
    /// A loopback listener/acceptor could not be bound.
    #[error("conformance harness could not bind a loopback socket: {0}")]
    Bind(#[from] std::io::Error),
    /// The parity venue (state, auth, or FIX config) could not be assembled.
    #[error("conformance harness setup failed: {0}")]
    Setup(String),
}

// ============================================================================
// The shared four-account tier set
// ============================================================================

/// The bootstrap secret that gates token issuance on the parity venues.
pub const SECRET: &str = "conformance-op-secret";

/// The single underlying every conformance venue hosts.
pub const UNDERLYING: &str = "BTC";

/// The canonical fixture contract symbol.
pub const CALL: &str = "BTC-20240329-50000-C";

/// The per-contract REST path prefix for the fixture contract.
pub const CONTRACT: &str =
    "/api/v1/underlyings/BTC/expirations/20240329/strikes/50000/options/call";

/// A generous rate-limit budget so throttling never masks a conformance assertion.
pub const AMPLE_RATE_LIMIT: u32 = 100_000;

/// The venue `TargetCompID` every FIX session addresses.
pub const VENUE: &str = "FAUXCHANGE";

/// The default read timeout for a reply from the live FIX acceptor.
pub const REPLY_TIMEOUT: Duration = Duration::from_secs(3);

/// The maximum on-the-wire FIX frame the harness accepts — the same ceiling the
/// ephemeral acceptor is configured with. A peer-declared `BodyLength (9)` above
/// this is refused before any arithmetic, so a hostile length can neither
/// overflow nor drive an out-of-bounds slice in [`split_frames`].
pub const MAX_FIX_FRAME_BYTES: usize = 64 * 1024;

/// One account's identity across every surface: the venue [`AccountId`], its
/// permission tier, its owner-hash seed byte, and its FIX login + CompID binding.
#[derive(Debug, Clone, Copy)]
pub struct Identity {
    /// The venue account id (shared by the REST tier and the FIX login).
    pub account: &'static str,
    /// FIX `Username (553)`.
    pub user: &'static str,
    /// FIX `Password (554)` — a labelled test credential, never a real secret.
    pub pw: &'static str,
    /// The bound FIX `SenderCompID (49)`.
    pub sender: &'static str,
    /// The owner-hash seed byte (identical across surfaces so fills compare verbatim).
    pub owner_byte: u8,
    /// The permission tier.
    pub permission: Permission,
}

/// `admin-1` — Admin (control-plane cases).
pub const ADMIN: Identity = Identity {
    account: "admin-1",
    user: "admin-fix",
    pw: "admin-plaintext-pw-DoNotLog-111",
    sender: "ADMINCLIENT",
    owner_byte: 1,
    permission: Permission::Admin,
};

/// `trader-1` — Trade (the maker in the crossing scenarios).
pub const TRADER1: Identity = Identity {
    account: "trader-1",
    user: "trader1-fix",
    pw: "trader1-plaintext-pw-DoNotLog-222",
    sender: "TRADER1CLIENT",
    owner_byte: 2,
    permission: Permission::Trade,
};

/// `trader-2` — Trade (the taker in the crossing scenarios).
pub const TRADER2: Identity = Identity {
    account: "trader-2",
    user: "trader2-fix",
    pw: "trader2-plaintext-pw-DoNotLog-333",
    sender: "TRADER2CLIENT",
    owner_byte: 3,
    permission: Permission::Trade,
};

/// `reader-1` — Read (market data only; refused order entry).
pub const READER: Identity = Identity {
    account: "reader-1",
    user: "reader1-fix",
    pw: "reader1-plaintext-pw-DoNotLog-444",
    sender: "READER1CLIENT",
    owner_byte: 4,
    permission: Permission::Read,
};

/// The four tier identities, in the fixed provisioning order.
pub const IDENTITIES: [Identity; 4] = [ADMIN, TRADER1, TRADER2, READER];

/// Resolves the [`Identity`] for an account label.
fn identity_for(account: &str) -> Result<Identity, String> {
    IDENTITIES
        .into_iter()
        .find(|id| id.account == account)
        .ok_or_else(|| format!("no conformance identity for account {account}"))
}

/// The shared account-provision set: the same ids and owner hashes across
/// surfaces, plus the FIX logins and immutable CompID bindings.
fn parity_accounts() -> Vec<AccountProvision> {
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

/// Builds one identically-seeded, **serving** parity venue.
fn build_parity_state() -> Result<Arc<AppState>, ConformanceError> {
    let auth = AuthConfig::dev()
        .map_err(|e| ConformanceError::Setup(format!("dev auth: {e}")))?
        .with_bootstrap_secret(SECRET)
        .with_accounts(parity_accounts())
        .with_rate_limit(AMPLE_RATE_LIMIT);
    AppState::new(
        AppStateConfig::new([UNDERLYING])
            .with_serving(true)
            .with_auth(auth),
    )
    .map_err(|e| ConformanceError::Setup(format!("app state: {e}")))
}

/// Wall-clock seconds for token minting (the credential plane is wall-clock).
fn now_secs() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| format!("system clock before epoch: {e}"))
}

// ============================================================================
// The ephemeral venue server (REST + FIX over one parity venue)
// ============================================================================

/// A serving parity [`AppState`] behind a real ephemeral REST server and a real
/// ephemeral FIX 4.4 acceptor on loopback. Dropping it stops both.
pub struct VenueServer {
    state: Arc<AppState>,
    rest_addr: SocketAddr,
    fix_addr: SocketAddr,
    rest_task: JoinHandle<()>,
    fix_shutdown: watch::Sender<bool>,
    _store: Arc<dyn FixSessionStore>,
}

impl VenueServer {
    /// Assembles a fresh parity venue and binds its REST + FIX gateways on
    /// ephemeral loopback ports.
    pub async fn start() -> Result<Self, ConformanceError> {
        let state = build_parity_state()?;

        // The REST gateway: a real ephemeral server over the true router.
        let rest_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let rest_addr = rest_listener.local_addr()?;
        let router = create_router(Arc::clone(&state));
        let rest_task = tokio::spawn(async move {
            let service = router.into_make_service_with_connect_info::<SocketAddr>();
            if let Err(error) = axum::serve(rest_listener, service).await {
                tracing::debug!(%error, "conformance REST server stopped");
            }
        });

        // The FIX gateway: a real ephemeral acceptor over the same venue.
        let loopback: SocketAddr = "127.0.0.1:0"
            .parse()
            .map_err(|e| ConformanceError::Setup(format!("loopback addr: {e}")))?;
        let acceptor = FixAcceptor::bind(FixAcceptorConfig {
            addr: loopback,
            connection_cap: 16,
            mailbox_depth: 64,
            max_frame_bytes: MAX_FIX_FRAME_BYTES,
            idle_timeout: Duration::from_secs(30),
        })
        .await?;
        let fix_addr = acceptor.local_addr();
        let store: Arc<dyn FixSessionStore> = Arc::new(InMemoryFixSessionStore::new());
        let factory = Arc::new(VenueFixSessionFactory::new(
            Arc::clone(&state),
            Arc::clone(&store),
            SessionConfig {
                logon_timeout_ms: 10_000,
                max_heart_bt_int_secs: 60,
            },
        ));
        let (fix_shutdown, fix_rx) = watch::channel(false);
        tokio::spawn(acceptor.serve(factory, fix_rx));

        Ok(Self {
            state,
            rest_addr,
            fix_addr,
            rest_task,
            fix_shutdown,
            _store: store,
        })
    }

    /// The serving venue (for a WS observation read or a journal snapshot).
    #[must_use]
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// The bound REST server address.
    #[must_use]
    pub fn rest_addr(&self) -> SocketAddr {
        self.rest_addr
    }

    /// The bound FIX acceptor address.
    #[must_use]
    pub fn fix_addr(&self) -> SocketAddr {
        self.fix_addr
    }

    /// Mints a JWT for `account` through the bootstrap-gated path.
    pub fn token(&self, account: &str) -> Result<String, String> {
        let issued = now_secs()?;
        self.state
            .mint_token(&AccountId::new(account), SECRET, issued, 3_600)
            .map_err(|e| format!("mint token for {account}: {e}"))
    }
}

impl Drop for VenueServer {
    fn drop(&mut self) {
        let _ = self.fix_shutdown.send(true);
        self.rest_task.abort();
    }
}

// ============================================================================
// A minimal REST HTTP/1.1 client (no client dependency)
// ============================================================================

/// A parsed REST reply.
pub struct RestReply {
    /// The HTTP status code.
    pub status: u16,
    /// The JSON body (`Value::Null` for an empty body).
    pub body: Value,
}

/// Sends one HTTP/1.1 request to the ephemeral REST server and reads the reply.
/// Uses `Connection: close`, so the server closes after responding and the whole
/// reply is read to EOF — no keep-alive/chunk parsing. Never panics on wire data.
pub async fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> Result<RestReply, String> {
    let body_bytes = match &body {
        Some(value) => {
            serde_json::to_vec(value).map_err(|e| format!("serialise request body: {e}"))?
        }
        None => Vec::new(),
    };

    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAccept: application/json\r\n"
    );
    if let Some(token) = bearer {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    if body.is_some() {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body_bytes.len()));
    }
    request.push_str("\r\n");

    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect to REST server: {e}"))?;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write REST request: {e}"))?;
    if !body_bytes.is_empty() {
        stream
            .write_all(&body_bytes)
            .await
            .map_err(|e| format!("write REST body: {e}"))?;
    }

    let mut raw = Vec::new();
    let read = tokio::time::timeout(REPLY_TIMEOUT, stream.read_to_end(&mut raw)).await;
    match read {
        Ok(Ok(_)) => {}
        // A mutating route commonly rejects (401 / 403) BEFORE draining the
        // request body, then closes with `Connection: close`; the server's
        // still-buffered unread body makes the OS surface a connection reset
        // instead of a clean EOF. `read_to_end` appends everything read before
        // the error, so the full reply has already landed in `raw` — tolerate
        // the reset when what we read parses as a complete HTTP reply, and only
        // propagate a genuine short read where nothing usable arrived.
        Ok(Err(e)) => {
            if parse_http_reply(&raw).is_err() {
                return Err(format!("read REST reply: {e}"));
            }
        }
        Err(_) => return Err("timed out reading REST reply".to_string()),
    }
    parse_http_reply(&raw)
}

/// A redacted one-line summary of a REST error reply — the status plus the
/// venue's **typed** error fields only (`code` / `message`), never the whole
/// body. This keeps the "report detail is hand-authored, never a raw wire value"
/// invariant literally true: only the venue's own typed error envelope
/// (`src/error.rs`) reaches a failure string, not an arbitrary response payload.
fn rest_error_summary(reply: &RestReply) -> String {
    let code = reply
        .body
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("?");
    let message = reply
        .body
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");
    format!("status {} code {code} message {message}", reply.status)
}

/// Parses `HTTP/1.1 <status> …\r\n…\r\n\r\n<body>` into a [`RestReply`], validating
/// the framing before accepting it.
///
/// A header-terminated reply is NOT accepted on the strength of its status line
/// alone: a `Content-Length` header, when present, must be fully received (a
/// connection reset after only the `401`/`403` headers leaves a short body — that
/// is a truncated reply, not a valid status-only pass), and a reply that carries a
/// body must parse as JSON (the venue answers every API route with a JSON
/// envelope). A truncated or unframed reply is the harness's typed `Err`, never a
/// silently-accepted `Value::Null`.
fn parse_http_reply(raw: &[u8]) -> Result<RestReply, String> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "malformed REST reply: no header terminator".to_string())?;
    let header = &raw[..split];
    // The header terminator is 4 bytes (`\r\n\r\n`); `split` is a valid `windows(4)`
    // offset, so `split + 4 <= raw.len()`.
    let body = &raw[split + 4..];

    let header_text = String::from_utf8_lossy(header);
    let status_line = header_text
        .lines()
        .next()
        .ok_or_else(|| "malformed REST reply: empty status line".to_string())?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("malformed REST status line: {status_line}"))?;

    // A declared `Content-Length` (case-insensitive header name) must be fully
    // received — a short body is a truncated, reset-mid-reply frame.
    let content_length: Option<usize> = header_text
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok());
    if let Some(declared) = content_length
        && body.len() < declared
    {
        return Err(format!(
            "truncated REST reply: body {} bytes, Content-Length {declared}",
            body.len()
        ));
    }

    // A reply that carries a body must parse as JSON — a garbled / partial body is
    // rejected, never accepted as `Value::Null`.
    let expects_body = content_length.is_some_and(|len| len > 0) || !body.is_empty();
    let value = if expects_body {
        serde_json::from_slice(body).map_err(|e| format!("malformed REST reply body: {e}"))?
    } else {
        Value::Null
    };
    Ok(RestReply {
        status,
        body: value,
    })
}

// ============================================================================
// A minimal live-WebSocket client (masked client frames; no client dependency)
// ============================================================================

/// A live `GET /ws` connection to the ephemeral REST/WS server: the upgraded
/// stream plus a carry-forward buffer of bytes read past a frame boundary. It
/// drives REAL masked client frames through the true `/ws` gateway, so the WS
/// surface is exercised end-to-end (its own message parse + permission gate), not
/// bypassed via `AppState`.
pub struct WsClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl WsClient {
    /// Performs the `GET /ws` upgrade with a bearer token, asserts the `101`
    /// switch, and drains the initial `connected` welcome.
    pub async fn connect(addr: SocketAddr, bearer: &str) -> Result<Self, String> {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("WS connect: {e}"))?;
        let request = format!(
            "GET /ws HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer {bearer}\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| format!("WS handshake write: {e}"))?;

        let mut raw = Vec::new();
        let mut scratch = [0u8; 1024];
        let leftover = loop {
            let read = tokio::time::timeout(REPLY_TIMEOUT, stream.read(&mut scratch)).await;
            let n = match read {
                Ok(Ok(0)) => return Err("WS server closed before the handshake".to_string()),
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(format!("WS handshake read: {e}")),
                Err(_) => return Err("WS handshake timed out".to_string()),
            };
            raw.extend_from_slice(&scratch[..n]);
            if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                let header = String::from_utf8_lossy(&raw[..end]);
                let status: u16 = header
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|code| code.parse().ok())
                    .unwrap_or(0);
                if status != 101 {
                    return Err(format!("WS handshake did not upgrade (status {status})"));
                }
                break raw[end + 4..].to_vec();
            }
        };

        let mut client = Self {
            stream,
            buf: leftover,
        };
        // Best-effort drain of the `connected` welcome so it does not shadow the
        // first control reply.
        let _ = client
            .read_frames_until(Duration::from_millis(500), |_| false)
            .await;
        Ok(client)
    }

    /// Sends one masked text control frame and reads until a `config` (applied) or
    /// `error` (rejected) reply lands or the window elapses.
    pub async fn send_control(&mut self, text: &str) -> Result<Vec<String>, String> {
        self.write_text(text).await?;
        self.read_frames_until(REPLY_TIMEOUT, |frames| {
            frames
                .iter()
                .any(|f| ws_type_of(f).is_some_and(|t| t == "config" || t == "error"))
        })
        .await
    }

    async fn write_text(&mut self, text: &str) -> Result<(), String> {
        let payload = text.as_bytes();
        let len = payload.len();
        let mut frame = Vec::with_capacity(len + 14);
        frame.push(0x81); // FIN + text opcode
        if len < 126 {
            frame.push(0x80 | (len as u8));
        } else if len <= usize::from(u16::MAX) {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            return Err("WS control frame too large for the harness".to_string());
        }
        let mask = [0x3d, 0x17, 0x9a, 0x5c];
        frame.extend_from_slice(&mask);
        frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        self.stream
            .write_all(&frame)
            .await
            .map_err(|e| format!("WS write: {e}"))
    }

    /// Reads server text frames until `done` is satisfied by the accumulated set
    /// or the window elapses, returning every text frame seen.
    async fn read_frames_until(
        &mut self,
        window: Duration,
        done: impl Fn(&[String]) -> bool,
    ) -> Result<Vec<String>, String> {
        let deadline = tokio::time::Instant::now() + window;
        let mut out = Vec::new();
        let mut scratch = [0u8; 4096];
        loop {
            let (frames, consumed) = parse_ws_text_frames(&self.buf);
            if consumed > 0 {
                self.buf.drain(..consumed);
            }
            out.extend(frames);
            if done(&out) || tokio::time::Instant::now() >= deadline {
                return Ok(out);
            }
            let read =
                tokio::time::timeout(Duration::from_millis(200), self.stream.read(&mut scratch))
                    .await;
            match read {
                Ok(Ok(0)) => return Ok(out),
                Ok(Ok(n)) => self.buf.extend_from_slice(&scratch[..n]),
                Ok(Err(e)) => return Err(format!("WS read: {e}")),
                Err(_) => {}
            }
        }
    }
}

/// Parses ONE complete WS frame at the front of `buf` (server → client), returning
/// `(opcode, payload, bytes_consumed)`, or `None` when fewer than one full frame is
/// buffered or the leading byte is not a legal WS opcode (a desync guard).
fn parse_one_ws_frame(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buf.len() < 2 {
        return None;
    }
    let opcode = buf[0] & 0x0f;
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
        header_len = header_len.checked_add(4)?;
    }
    let total = header_len.checked_add(len)?;
    if total > buf.len() {
        return None;
    }
    Some((opcode, buf[header_len..total].to_vec(), total))
}

/// Every complete text-frame payload at the front of `buf`, plus the bytes
/// consumed (a non-text opcode is skipped, never misread as a message).
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

/// The `type` tag of a WS text frame (`{ "type": …, "data": … }`).
fn ws_type_of(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    value.get("type")?.as_str().map(str::to_string)
}

/// The `data` object of the first WS text frame whose `type` equals `ty`.
#[must_use]
pub fn ws_find_type(frames: &[String], ty: &str) -> Option<Value> {
    frames.iter().find_map(|text| {
        let value: Value = serde_json::from_str(text).ok()?;
        if value.get("type")?.as_str()? == ty {
            value.get("data").cloned()
        } else {
            None
        }
    })
}

// ============================================================================
// FIX wire helpers
// ============================================================================

/// Wraps a body in a `BeginString`/`BodyLength` header and a checksum trailer
/// (`SOH` = `\x01`).
fn frame_with_body(body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
    msg.extend_from_slice(body);
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
    msg
}

fn logon_frame(sender: &str, seq: u64, user: &str, pw: &str) -> Vec<u8> {
    let body = format!(
        "35=A\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0198=0\x01108=30\x01553={user}\x01554={pw}\x01"
    );
    frame_with_body(body.as_bytes())
}

fn limit_order_frame(
    sender: &str,
    seq: u64,
    cl_ord_id: &str,
    side: &str,
    price: &str,
    qty: u64,
    tif: &str,
) -> Vec<u8> {
    let body = format!(
        "35=D\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0111={cl_ord_id}\x0155={CALL}\x0154={side}\x0160=20240329-12:00:00.000\x0140=2\x0144={price}\x0138={qty}\x0159={tif}\x01"
    );
    frame_with_body(body.as_bytes())
}

fn cancel_frame(sender: &str, seq: u64, orig: &str, cl_ord_id: &str, side: &str) -> Vec<u8> {
    let body = format!(
        "35=F\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0141={orig}\x0111={cl_ord_id}\x0155={CALL}\x0154={side}\x01"
    );
    frame_with_body(body.as_bytes())
}

fn replace_frame(
    sender: &str,
    seq: u64,
    orig: &str,
    cl_ord_id: &str,
    side: &str,
    price: &str,
    qty: u64,
) -> Vec<u8> {
    let body = format!(
        "35=G\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0141={orig}\x0111={cl_ord_id}\x0155={CALL}\x0154={side}\x0140=2\x0144={price}\x0138={qty}\x01"
    );
    frame_with_body(body.as_bytes())
}

fn test_request_frame(sender: &str, seq: u64, test_req_id: &str) -> Vec<u8> {
    let body = format!(
        "35=1\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01112={test_req_id}\x01"
    );
    frame_with_body(body.as_bytes())
}

fn heartbeat_frame(sender: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=0\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
    frame_with_body(body.as_bytes())
}

fn logout_frame(sender: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=5\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
    frame_with_body(body.as_bytes())
}

/// A `SequenceReset (4)` with `NewSeqNo (36)` and `GapFillFlag (123) = Y`.
fn sequence_reset_frame(sender: &str, seq: u64, new_seq_no: u64) -> Vec<u8> {
    let body = format!(
        "35=4\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0136={new_seq_no}\x01123=Y\x01"
    );
    frame_with_body(body.as_bytes())
}

fn market_data_request_frame(
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
        "35=V\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01262={md_req_id}\x01263=1\x01264=0\x01267={count}\x01{group}146=1\x0155={CALL}\x01",
        count = entry_types.len(),
    );
    frame_with_body(body.as_bytes())
}

fn unsupported_app_frame(sender: &str, seq: u64) -> Vec<u8> {
    let body =
        format!("35=R\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x01");
    frame_with_body(body.as_bytes())
}

fn order_missing_side_frame(sender: &str, seq: u64, cl_ord_id: &str) -> Vec<u8> {
    let body = format!(
        "35=D\x0149={sender}\x0156={VENUE}\x0134={seq}\x0152=20240329-12:00:00.000\x0111={cl_ord_id}\x0155={CALL}\x0160=20240329-12:00:00.000\x0140=2\x0144=500.00\x0138=1\x0159=1\x01"
    );
    frame_with_body(body.as_bytes())
}

/// Splits a byte buffer into complete FIX frames.
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
        // Bound the PEER-declared BodyLength before any arithmetic: a hostile `9=`
        // must never overflow the offset math or drive an out-of-bounds slice.
        if body_len > MAX_FIX_FRAME_BYTES {
            break;
        }
        let body_start = digits_start + soh_rel + 1;
        // `10=NNN\x01` trailer is 7 bytes; every add is checked (a debug build must
        // not panic and a release build must not wrap on adversarial input).
        let Some(total_end) = body_start
            .checked_add(body_len)
            .and_then(|n| n.checked_add(7))
        else {
            break;
        };
        if total_end > buf.len() {
            break;
        }
        frames.push(buf[start..total_end].to_vec());
        pos = total_end;
    }
    frames
}

/// Reads from `stream` until at least one complete frame is available or the
/// timeout elapses.
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

/// The value of scalar `tag` in a FIX frame.
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

/// Whether any frame has `MsgType (35) == ty`.
#[must_use]
pub fn any_msg_type(frames: &[Vec<u8>], ty: &str) -> bool {
    frames.iter().any(|f| msg_type(f).as_deref() == Some(ty))
}

/// The first frame with `MsgType (35) == ty`.
#[must_use]
pub fn find_msg<'a>(frames: &'a [Vec<u8>], ty: &str) -> Option<&'a Vec<u8>> {
    frames.iter().find(|f| msg_type(f).as_deref() == Some(ty))
}

/// The first `ExecutionReport (8)` frame with `ExecType (150) == exec_type`.
#[must_use]
pub fn find_report<'a>(frames: &'a [Vec<u8>], exec_type: &str) -> Option<&'a Vec<u8>> {
    frames.iter().find(|f| {
        msg_type(f).as_deref() == Some("8") && field(f, "150").as_deref() == Some(exec_type)
    })
}

// ============================================================================
// The FIX test client
// ============================================================================

/// A live FIX session bound to one account: a `TcpStream`, its `SenderCompID`,
/// and its next **checked** `MsgSeqNum`.
pub struct FixClient {
    stream: TcpStream,
    sender: String,
    seq: u64,
}

impl FixClient {
    /// Connects and logs on as `identity`, draining the credential-free `Logon (A)`
    /// ack, leaving the session `Active` at `MsgSeqNum = 2`.
    pub async fn logon(addr: SocketAddr, identity: Identity) -> Result<Self, String> {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("FIX connect for {}: {e}", identity.account))?;
        let frame = logon_frame(identity.sender, 1, identity.user, identity.pw);
        stream
            .write_all(&frame)
            .await
            .map_err(|e| format!("FIX logon write for {}: {e}", identity.account))?;
        let ack = recv_frames(&mut stream, REPLY_TIMEOUT).await;
        if !any_msg_type(&ack, "A") {
            return Err(format!(
                "the {} logon was not admitted (no Logon(A) ack)",
                identity.account
            ));
        }
        Ok(Self {
            stream,
            sender: identity.sender.to_string(),
            seq: 2,
        })
    }

    /// Advances the checked outbound `MsgSeqNum`, refusing to wrap.
    fn advance_seq(&mut self) -> Result<u64, String> {
        let used = self.seq;
        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| "FIX MsgSeqNum exhausted".to_string())?;
        Ok(used)
    }

    async fn round_trip(&mut self, frame: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        self.stream
            .write_all(&frame)
            .await
            .map_err(|e| format!("FIX write: {e}"))?;
        Ok(recv_frames(&mut self.stream, REPLY_TIMEOUT).await)
    }

    /// Places a limit order (`price` in cents).
    pub async fn place_limit(
        &mut self,
        cl_ord_id: &str,
        side: &str,
        price_cents: u64,
        qty: u64,
        tif: &str,
    ) -> Result<Vec<Vec<u8>>, String> {
        let price = render_cents_to_decimal(Cents::new(price_cents));
        let seq = self.advance_seq()?;
        let frame = limit_order_frame(&self.sender, seq, cl_ord_id, side, &price, qty, tif);
        self.round_trip(frame).await
    }

    /// Cancels a resting order by its original `ClOrdID`.
    pub async fn cancel(
        &mut self,
        orig: &str,
        cl_ord_id: &str,
        side: &str,
    ) -> Result<Vec<Vec<u8>>, String> {
        let seq = self.advance_seq()?;
        let frame = cancel_frame(&self.sender, seq, orig, cl_ord_id, side);
        self.round_trip(frame).await
    }

    /// Replaces a resting order (`G`).
    pub async fn replace(
        &mut self,
        orig: &str,
        cl_ord_id: &str,
        side: &str,
        price_cents: u64,
        qty: u64,
    ) -> Result<Vec<Vec<u8>>, String> {
        let price = render_cents_to_decimal(Cents::new(price_cents));
        let seq = self.advance_seq()?;
        let frame = replace_frame(&self.sender, seq, orig, cl_ord_id, side, &price, qty);
        self.round_trip(frame).await
    }

    /// Subscribes to market data for the fixture contract.
    pub async fn market_data(
        &mut self,
        md_req_id: &str,
        entry_types: &[&str],
    ) -> Result<Vec<Vec<u8>>, String> {
        let seq = self.advance_seq()?;
        let frame = market_data_request_frame(&self.sender, seq, md_req_id, entry_types);
        self.round_trip(frame).await
    }

    /// Sends a `TestRequest (1)` and reads the `Heartbeat (0)` reply.
    pub async fn test_request(&mut self, test_req_id: &str) -> Result<Vec<Vec<u8>>, String> {
        let seq = self.advance_seq()?;
        let frame = test_request_frame(&self.sender, seq, test_req_id);
        self.round_trip(frame).await
    }

    /// Sends an unsupported application `MsgType` (`R`, QuoteRequest).
    pub async fn unsupported(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let seq = self.advance_seq()?;
        let frame = unsupported_app_frame(&self.sender, seq);
        self.round_trip(frame).await
    }

    /// Sends a `NewOrderSingle (D)` missing the required `Side (54)`.
    pub async fn order_missing_side(&mut self, cl_ord_id: &str) -> Result<Vec<Vec<u8>>, String> {
        let seq = self.advance_seq()?;
        let frame = order_missing_side_frame(&self.sender, seq, cl_ord_id);
        self.round_trip(frame).await
    }

    /// Sends a frame that skips ahead of the expected `MsgSeqNum` (a deliberate
    /// inbound gap), returning the `ResendRequest (2)` reply.
    pub async fn send_out_of_order(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let gapped = self
            .seq
            .checked_add(5)
            .ok_or_else(|| "FIX MsgSeqNum exhausted".to_string())?;
        let frame = heartbeat_frame(&self.sender, gapped);
        self.round_trip(frame).await
    }

    /// Sends a `SequenceReset (4)` gap-fill announcing the next real message will
    /// be at `new_seq_no`, advancing the client's own outbound counter to match.
    /// A gap-fill reset is processed regardless of its own `MsgSeqNum`, so it is
    /// sent at the current sequence without advancing it first.
    pub async fn sequence_reset_gap_fill(
        &mut self,
        new_seq_no: u64,
    ) -> Result<Vec<Vec<u8>>, String> {
        let frame = sequence_reset_frame(&self.sender, self.seq, new_seq_no);
        self.seq = new_seq_no;
        self.round_trip(frame).await
    }

    /// Sends a `Logout (5)` and reads the reply.
    pub async fn logout(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let seq = self.advance_seq()?;
        let frame = logout_frame(&self.sender, seq);
        self.round_trip(frame).await
    }

    /// Drains any further buffered frames (best effort, short timeout).
    pub async fn drain(&mut self) -> Vec<Vec<u8>> {
        recv_frames(&mut self.stream, Duration::from_millis(300)).await
    }
}

/// Connects and sends a single `Logon (A)` **without** asserting admission,
/// returning the reply frames (a `Logout (5)` for a bad credential).
pub async fn attempt_logon(
    addr: SocketAddr,
    sender: &str,
    user: &str,
    pw: &str,
) -> Result<Vec<Vec<u8>>, String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("FIX connect: {e}"))?;
    let frame = logon_frame(sender, 1, user, pw);
    stream
        .write_all(&frame)
        .await
        .map_err(|e| format!("FIX logon write: {e}"))?;
    Ok(recv_frames(&mut stream, REPLY_TIMEOUT).await)
}

// ============================================================================
// Order-entry scenario driver + journal reader (shared by the parity cases)
// ============================================================================

/// One logical order-entry step of a parity scenario (protocol-agnostic so the
/// same scenario drives over REST and FIX). WS is never an order-entry surface.
#[derive(Debug, Clone)]
pub enum Step {
    /// Place a resting / crossing limit order on the fixture contract.
    Place {
        /// The placing account label.
        account: &'static str,
        /// `buy` / `sell`.
        side: &'static str,
        /// The limit price in cents.
        price: u64,
        /// The order quantity.
        qty: u64,
        /// The optional time-in-force (`gtc` / `ioc` / `fok`).
        tif: Option<&'static str>,
    },
    /// Cancel the order placed by a prior [`Step::Place`] at index `target`.
    Cancel {
        /// The cancelling account label.
        account: &'static str,
        /// The index of the prior `Place` to cancel.
        target: usize,
    },
}

/// The deterministic `ClOrdID` for the `Place` at step `index`.
fn cl_ord_id_for(index: usize) -> String {
    format!("conf-cl-{index}")
}

/// Reads the ordered committed `VenueEvent` stream for `underlying` from its
/// actor journal — the order-entry parity oracle.
pub async fn journaled_events(
    state: &Arc<AppState>,
    underlying: &str,
) -> Result<Vec<VenueEvent>, String> {
    let snapshot = state
        .journal_snapshot(underlying)
        .await
        .map_err(|e| format!("journal snapshot for {underlying}: {e}"))?;
    Ok(snapshot
        .records
        .into_iter()
        .filter_map(|record| match record {
            JournalRecord::Event(event) => Some(event),
            _ => None,
        })
        .collect())
}

/// Drives a scenario over the **live REST surface**, returning the committed
/// `VenueEvent` stream. Every place/cancel goes through the real router.
pub async fn drive_rest_orders(
    server: &VenueServer,
    steps: &[Step],
) -> Result<Vec<VenueEvent>, String> {
    let addr = server.rest_addr();
    let mut tokens: HashMap<&'static str, String> = HashMap::new();
    let mut order_ids: Vec<Option<String>> = Vec::with_capacity(steps.len());

    for (index, step) in steps.iter().cloned().enumerate() {
        match step {
            Step::Place {
                account,
                side,
                price,
                qty,
                tif,
            } => {
                let token = resolve_token(server, &mut tokens, account)?;
                let mut body = json!({ "side": side, "price": price, "quantity": qty });
                if let Some(tif) = tif {
                    body["time_in_force"] = json!(tif);
                }
                let uri = format!("{CONTRACT}/orders");
                let reply = http(addr, "POST", &uri, Some(&token), Some(body)).await?;
                if reply.status != 200 {
                    return Err(format!(
                        "REST place #{index} rejected: {}",
                        rest_error_summary(&reply)
                    ));
                }
                order_ids.push(reply.body["order_id"].as_str().map(str::to_string));
            }
            Step::Cancel { account, target } => {
                let token = resolve_token(server, &mut tokens, account)?;
                let order_id = order_ids
                    .get(target)
                    .and_then(Clone::clone)
                    .ok_or_else(|| format!("cancel target #{target} did not place an order"))?;
                let uri = format!("{CONTRACT}/orders/{order_id}");
                let reply = http(addr, "DELETE", &uri, Some(&token), None).await?;
                if reply.status != 200 {
                    return Err(format!(
                        "REST cancel #{index} rejected: {}",
                        rest_error_summary(&reply)
                    ));
                }
                order_ids.push(None);
            }
        }
    }

    journaled_events(server.state(), UNDERLYING).await
}

fn resolve_token(
    server: &VenueServer,
    tokens: &mut HashMap<&'static str, String>,
    account: &'static str,
) -> Result<String, String> {
    if let Some(token) = tokens.get(account) {
        return Ok(token.clone());
    }
    let token = server.token(account)?;
    tokens.insert(account, token.clone());
    Ok(token)
}

/// Drives the same scenario over **live FIX sessions**, returning the committed
/// `VenueEvent` stream — the FIX twin of [`drive_rest_orders`].
pub async fn drive_fix_orders(
    server: &VenueServer,
    steps: &[Step],
) -> Result<Vec<VenueEvent>, String> {
    let addr = server.fix_addr();
    let mut clients: HashMap<&'static str, FixClient> = HashMap::new();
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
                    let client = FixClient::logon(addr, identity_for(account)?).await?;
                    clients.insert(account, client);
                }
                let client = clients
                    .get_mut(account)
                    .ok_or_else(|| format!("FIX client for {account} missing"))?;
                let fix_side = fix_side(side)?;
                let fix_tif = fix_tif(tif)?;
                let reply = client
                    .place_limit(&cl_ord_id_for(index), fix_side, price, qty, fix_tif)
                    .await?;
                if !any_msg_type(&reply, "8") {
                    return Err(format!("FIX place #{index} emitted no ExecutionReport(8)"));
                }
                if any_msg_type(&reply, "3") || any_msg_type(&reply, "j") {
                    return Err(format!("FIX place #{index} was a session/business reject"));
                }
                placed_side.push(Some(fix_side));
            }
            Step::Cancel { account, target } => {
                let orig = cl_ord_id_for(target);
                let side = placed_side
                    .get(target)
                    .and_then(|s| *s)
                    .ok_or_else(|| format!("cancel target #{target} did not place an order"))?;
                let client = clients
                    .get_mut(account)
                    .ok_or_else(|| format!("cancel account {account} never opened a session"))?;
                let reply = client.cancel(&orig, &cl_ord_id_for(index), side).await?;
                if !any_msg_type(&reply, "8") && !any_msg_type(&reply, "9") {
                    return Err(format!("FIX cancel #{index} emitted no 8/9"));
                }
                if any_msg_type(&reply, "3") {
                    return Err(format!("FIX cancel #{index} was a session Reject(3)"));
                }
                placed_side.push(None);
            }
        }
    }

    journaled_events(server.state(), UNDERLYING).await
}

fn fix_side(side: &str) -> Result<&'static str, String> {
    match side {
        "buy" => Ok("1"),
        "sell" => Ok("2"),
        other => Err(format!("unknown side {other}")),
    }
}

fn fix_tif(tif: Option<&str>) -> Result<&'static str, String> {
    match tif {
        None | Some("gtc") => Ok("1"),
        Some("ioc") => Ok("3"),
        Some("fok") => Ok("4"),
        Some(other) => Err(format!("unsupported TIF {other}")),
    }
}
