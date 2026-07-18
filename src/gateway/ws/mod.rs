//! Transport layer: WebSocket gateway — the `WsMessage` protocol, the
//! subscription manager, and the `GET /ws` handshake. Observation and control
//! surface, tier T1 (v0.1); WS carries **no** order-entry message
//! ([03 §4](../../../docs/03-protocol-surfaces.md)).
//!
//! ## Shape
//!
//! - [`crate::subscription`] — the shared market-data **service**
//!   ([`OrderbookSubscriptionManager`], re-exported here) plus its `WsFanOut`.
//!   It is a service module (a sibling of [`crate::auth`]), **not** part of this
//!   gateway, so [`AppState`] can own it without importing a gateway; it depends
//!   only on the DTOs + the exchange core.
//! - [`protocol`] — the client → server [`ClientAction`] set and its frame parser
//!   ([`parse_frame`]), which rejects any order-entry-shaped frame.
//! - This root — [`ws_handler`] (the authenticated `GET /ws` upgrade) and the
//!   per-connection socket loop, which reaches the service through [`AppState`].
//!
//! ## Handshake authentication
//!
//! `GET /ws` authenticates through the venue's **one**
//! [`AuthService::admit`](crate::auth::AuthService::admit) with a baseline
//! [`Permission::Read`], reading the bearer JWT from the `Authorization` header
//! **or** a `?token=` / `?access_token=` query parameter (a browser WebSocket
//! cannot set headers). A missing / invalid token or an exhausted rate-limit
//! budget **refuses the upgrade** (`401` / `429`) — the socket never opens. Once
//! open, a per-action check gates the market-maker control actions behind
//! [`Permission::Admin`] and passes them through the rate limiter, mirroring the
//! REST control plane (control parity, REST ≡ WS).
//!
//! ## Close vs continue
//!
//! An authentication / terminal error closes the socket; a command error (a bad
//! frame, a forbidden control, a not-yet-routable control) emits the typed
//! [`WsError`] envelope and **keeps the connection open**
//! ([03 §4.2](../../../docs/03-protocol-surfaces.md)).

pub mod protocol;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, broadcast};
use tokio::time::MissedTickBehavior;

use crate::auth::{Admission, Claims, PeerAddr, RateLimitKey, RateLimitTier};
use crate::error::{VenueError, WsError};
use crate::exchange::{EventTimestamp, Symbol, VenueCommand};
use crate::models::{
    ActiveSubscription, Permission, SubscriptionChannel, SubscriptionResult, WsMessage,
};
use crate::simulation::ScenarioBundle;
use crate::state::AppState;

// The subscription manager is a `crate::subscription` SERVICE (not a gateway); it
// is re-exported here for the WS handler and for test-path stability, but
// `AppState` imports the canonical `crate::subscription`, never this re-export.
pub use self::protocol::{ClientAction, FrameOutcome, parse_frame};
pub use crate::subscription::OrderbookSubscriptionManager;

/// The maximum number of active subscriptions one connection may hold — a
/// **DoS control** bounding per-connection state
/// ([08 §5](../../../docs/08-threat-model.md)). The live value is venue config
/// (#022); this fixes a bounded default.
pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 256;

/// The maximum number of items one `batch_subscribe` / `batch_unsubscribe` frame
/// may carry — a **DoS control** ([08 §5](../../../docs/08-threat-model.md)): an
/// over-size batch is rejected up-front (before the per-item [`MAX_SUBSCRIPTIONS_PER_CONNECTION`]
/// cap applies), so a single frame cannot force an unbounded loop.
pub const MAX_BATCH_SIZE: usize = 64;

/// The maximum inbound WebSocket frame / message size, in **bytes** — a **DoS
/// control** ([08 §5](../../../docs/08-threat-model.md)). Client frames are small
/// JSON action objects (the largest, a full `batch_subscribe`, is a few KiB), so
/// a 64 KiB ceiling is generous while replacing axum's 16 MiB / 64 MiB defaults.
pub const MAX_WS_FRAME_BYTES: usize = 64 * 1024;

/// The liveness heartbeat interval, in **seconds**. Each tick re-validates the
/// session (revocation + expiry) and sends a protocol ping to elicit a pong.
const HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// The number of consecutive heartbeat ticks with **no** inbound traffic (not
/// even a pong) after which an idle / dead connection is closed — a liveness /
/// resource-reclaim bound. At [`HEARTBEAT_INTERVAL_SECS`] = 30 s this is a
/// ~2-minute idle window.
const MAX_IDLE_TICKS: u32 = 4;

/// Mounts the WebSocket route onto the shared [`AppState`] router. Mount this
/// alongside the REST routes (below the peer-injection layer, so the real socket
/// peer reaches the handshake rate-limit key).
pub fn ws_routes() -> Router<Arc<AppState>> {
    Router::new().route("/ws", get(ws_handler))
}

/// The handshake token carried on the `GET /ws` query string, for clients that
/// cannot set an `Authorization` header (browsers). Either `token` or
/// `access_token` is accepted; the header takes precedence when both are present.
#[derive(Debug, Default, Deserialize)]
pub struct WsAuthQuery {
    /// The bearer JWT (`?token=…`).
    #[serde(default)]
    pub token: Option<String>,
    /// The bearer JWT (`?access_token=…`).
    #[serde(default)]
    pub access_token: Option<String>,
}

/// `GET /ws` — authenticates the handshake, reserves a connection slot, then
/// upgrades to the `WsMessage` protocol.
///
/// The bearer is read from the `Authorization` header or a `?token=` /
/// `?access_token=` query parameter and admitted through the venue's one
/// [`AuthService`](crate::auth::AuthService) with a baseline [`Permission::Read`];
/// a rejection **refuses the upgrade** (the typed `401`/`429` response, with the
/// rate-limit headers), so an unauthenticated socket never opens. After admission
/// a venue-wide connection slot ([`MAX_WS_CONNECTIONS`](crate::subscription::MAX_WS_CONNECTIONS))
/// is reserved — at the cap the upgrade is refused (`503`) — and the inbound
/// frame / message size is capped ([`MAX_WS_FRAME_BYTES`]).
pub async fn ws_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(auth_query): Query<WsAuthQuery>,
    peer: Option<Extension<PeerAddr>>,
    ws: WebSocketUpgrade,
) -> Response {
    let token = extract_token(&headers, &auth_query);
    let peer_ip = peer
        .map(|Extension(peer)| peer.0)
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    match state
        .auth()
        .admit("/ws", token.as_deref(), peer_ip, Permission::Read)
    {
        Admission::Admitted {
            identity,
            rate_limit,
        } => {
            // Reserve a venue-wide connection slot BEFORE upgrading; at the cap the
            // upgrade is refused rather than admitting an unbounded socket count.
            let Some(permit) = state.subscriptions().try_acquire_connection() else {
                let mut response = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "websocket connection limit reached",
                )
                    .into_response();
                rate_limit.apply_headers(response.headers_mut());
                return response;
            };
            let claims = identity.claims;
            let manager = Arc::clone(state.subscriptions());
            // Bound the inbound frame / message size (a small JSON ceiling), then
            // upgrade — the permit is moved into the socket task and released on
            // close.
            let mut response = ws
                .max_frame_size(MAX_WS_FRAME_BYTES)
                .max_message_size(MAX_WS_FRAME_BYTES)
                .on_upgrade(move |socket| handle_socket(socket, state, manager, claims, permit));
            rate_limit.apply_headers(response.headers_mut());
            response
        }
        Admission::Rejected { error, rate_limit } => {
            let mut response = error.into_response();
            if let Some(decision) = rate_limit {
                decision.apply_headers(response.headers_mut());
            }
            response
        }
        // `/ws` is not on the exempt list, so this arm is defensively unauthorized.
        Admission::Exempt => VenueError::Unauthorized.into_response(),
    }
}

/// Extracts the bearer token from the `Authorization` header (preferred) or the
/// handshake query string.
fn extract_token(headers: &HeaderMap, query: &WsAuthQuery) -> Option<String> {
    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        && let Some(token) = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
    {
        let token = token.trim();
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    query
        .token
        .clone()
        .or_else(|| query.access_token.clone())
        .filter(|token| !token.is_empty())
}

/// The per-connection socket loop: send the welcome, subscribe to the bounded
/// market-data broadcast, then multiplex inbound client actions, outbound
/// market-data forwarding (with per-instrument laggard re-snapshot), and
/// heartbeats until the socket closes.
///
/// `_permit` is the venue-wide connection slot, held for the socket's lifetime and
/// released (reclaiming the slot) when this function returns. Each heartbeat tick
/// re-validates the session (revocation + expiry, closing on failure) and, if the
/// peer has produced no traffic for [`MAX_IDLE_TICKS`], closes the idle socket.
async fn handle_socket(
    mut socket: WebSocket,
    state: Arc<AppState>,
    manager: Arc<OrderbookSubscriptionManager>,
    claims: Claims,
    _permit: OwnedSemaphorePermit,
) {
    if !send_message(
        &mut socket,
        &WsMessage::Connected {
            message: "connected to fauxchange".to_string(),
        },
    )
    .await
    {
        return;
    }

    let mut broadcast_rx = manager.subscribe();
    let mut connection = Connection::new(claims);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Consecutive heartbeat ticks with no inbound traffic; reset on any frame.
    let mut idle_ticks: u32 = 0;

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                // Any inbound frame (including a pong) is liveness — reset the idle
                // counter before handling it.
                idle_ticks = 0;
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let messages = match parse_frame(text.as_str()) {
                            FrameOutcome::Action(action, request_id) => {
                                connection.on_action(action, request_id, &state, &manager).await
                            }
                            FrameOutcome::Reject(error) => vec![WsMessage::Error(*error)],
                        };
                        if send_all(&mut socket, messages).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        let error = protocol::decode_error(None, "binary frames are not supported");
                        if send_all(&mut socket, vec![WsMessage::Error(error)]).await {
                            break;
                        }
                    }
                    // A ping is answered by the codec; a pong is liveness only.
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    // Close, an inbound error, or the stream end all end the loop.
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                }
            }
            broadcast = broadcast_rx.recv() => {
                match broadcast {
                    Ok(message) => {
                        if let Some(forward) = connection.filter(message)
                            && !send_message(&mut socket, &forward).await
                        {
                            break;
                        }
                    }
                    // A laggard drops its backlog and re-snapshots every orderbook
                    // subscription — a gap is repaired only by a fresh snapshot.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let messages = connection.resnapshot(&manager);
                        if send_all(&mut socket, messages).await {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = heartbeat.tick() => {
                // Re-validate the long-lived session: a token that has been revoked
                // or has expired since the handshake closes the socket with a
                // terminal error (the handshake admits only once).
                if let Err(error) = state
                    .auth()
                    .revalidate_session(&connection.claims, wall_clock_secs())
                {
                    let _ = send_message(&mut socket, &WsMessage::Error(error.ws_error(None))).await;
                    break;
                }
                // Idle / liveness reaping: close a connection that has produced no
                // inbound traffic (not even a pong) for MAX_IDLE_TICKS ticks. The
                // counter is compared to the small cap immediately, so the checked
                // clamp (never `saturating_*`, per the arithmetic rule) is exact.
                idle_ticks = idle_ticks.checked_add(1).unwrap_or(MAX_IDLE_TICKS);
                if idle_ticks >= MAX_IDLE_TICKS {
                    tracing::debug!(
                        account = connection.claims.account().as_str(),
                        "closing idle websocket connection (no traffic within the liveness window)"
                    );
                    break;
                }
                // Send the app heartbeat AND a protocol ping to elicit a pong from a
                // live peer (a pure subscriber sends no frames otherwise).
                if !send_message(&mut socket, &WsMessage::Heartbeat {
                    timestamp: heartbeat_timestamp(),
                })
                .await
                {
                    break;
                }
                if socket
                    .send(Message::Ping(Vec::<u8>::new().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    // Dropping `_permit`, `broadcast_rx`, and `connection` here reclaims the
    // connection slot and tears down all subscription state.
    tracing::debug!(
        account = connection.claims.account().as_str(),
        "websocket connection closed; subscription state torn down, connection slot released"
    );
}

/// Serialises and sends one message; returns `true` on success, `false` when the
/// socket errored (the caller should stop).
async fn send_message(socket: &mut WebSocket, message: &WsMessage) -> bool {
    match serde_json::to_string(message) {
        Ok(json) => socket.send(Message::text(json)).await.is_ok(),
        // A DTO that cannot serialise is a bug, not a client error; skip it
        // rather than close the socket.
        Err(error) => {
            tracing::error!(%error, "failed to serialise a ws message; skipping");
            true
        }
    }
}

/// Sends a batch of messages, returning `true` (stop the loop) when a send fails
/// **or** a terminal error was sent (an auth/terminal error closes the socket; a
/// command error leaves it open).
async fn send_all(socket: &mut WebSocket, messages: Vec<WsMessage>) -> bool {
    for message in messages {
        let terminal = matches!(&message, WsMessage::Error(error) if error.terminal);
        if !send_message(socket, &message).await {
            return true;
        }
        if terminal {
            return true;
        }
    }
    false
}

/// A liveness-only timestamp for the heartbeat frame. This is **transport
/// liveness**, not the venue clock and not journaled — the WS fan-out is
/// best-effort and outside the determinism oracle ([07 §4](../../../docs/07-performance-budgets.md)),
/// so a wall-clock read here never touches the sequenced path.
fn heartbeat_timestamp() -> EventTimestamp {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    EventTimestamp::new(millis)
}

/// Wall-clock **seconds** for the periodic session re-validation (token expiry is
/// a wall-clock credential-plane concern, an explicit replay exclusion — never the
/// venue clock, and this is transport-side, outside the sequenced path).
fn wall_clock_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// Per-connection state
// ============================================================================

/// The market-maker control knobs one control action sets.
#[derive(Debug, Clone, Copy, Default)]
struct ControlKnobs {
    spread_multiplier: Option<f64>,
    size_scalar: Option<f64>,
    directional_skew: Option<f64>,
    enabled: Option<bool>,
}

/// One authenticated connection's subscription state — the per-`(channel,
/// symbol)` set (with its orderbook `depth`) and the per-orderbook-symbol
/// delivered `instrument_sequence` baseline (for the snapshot→delta gap filter).
///
/// Bounded at [`MAX_SUBSCRIPTIONS_PER_CONNECTION`]; torn down on unsubscribe,
/// close, and laggard drop (dropping the struct frees it).
struct Connection {
    claims: Claims,
    /// `(channel, symbol-key)` → orderbook depth (`None` for non-orderbook).
    subscriptions: HashMap<(SubscriptionChannel, String), Option<usize>>,
    /// Per-orderbook-symbol last-delivered `instrument_sequence` — a delta at or
    /// below it is dropped (it is already in the client's snapshot).
    baselines: HashMap<String, u64>,
}

impl Connection {
    fn new(claims: Claims) -> Self {
        Self {
            claims,
            subscriptions: HashMap::new(),
            baselines: HashMap::new(),
        }
    }

    /// Dispatches one client action to its handler, returning the messages to
    /// send back (acks, snapshots, and/or a typed error).
    async fn on_action(
        &mut self,
        action: ClientAction,
        request_id: Option<String>,
        state: &Arc<AppState>,
        manager: &OrderbookSubscriptionManager,
    ) -> Vec<WsMessage> {
        match action {
            ClientAction::Subscribe(params) => self.subscribe(params, manager),
            ClientAction::Unsubscribe(params) => self.unsubscribe(&params.channel, &params.symbol),
            ClientAction::BatchSubscribe(batch) => self.batch_subscribe(batch, manager),
            ClientAction::BatchUnsubscribe(batch) => self.batch_unsubscribe(batch),
            ClientAction::ListSubscriptions => vec![self.list_subscriptions()],
            ClientAction::SetSpread(value) => vec![
                self.control(
                    state,
                    ControlKnobs {
                        spread_multiplier: Some(value.value),
                        ..ControlKnobs::default()
                    },
                    request_id,
                )
                .await,
            ],
            ClientAction::SetSize(value) => vec![
                self.control(
                    state,
                    ControlKnobs {
                        size_scalar: Some(value.value),
                        ..ControlKnobs::default()
                    },
                    request_id,
                )
                .await,
            ],
            ClientAction::SetSkew(value) => vec![
                self.control(
                    state,
                    ControlKnobs {
                        directional_skew: Some(value.value),
                        ..ControlKnobs::default()
                    },
                    request_id,
                )
                .await,
            ],
            ClientAction::Kill => vec![
                self.control(
                    state,
                    ControlKnobs {
                        enabled: Some(false),
                        ..ControlKnobs::default()
                    },
                    request_id,
                )
                .await,
            ],
            ClientAction::Enable => vec![
                self.control(
                    state,
                    ControlKnobs {
                        enabled: Some(true),
                        ..ControlKnobs::default()
                    },
                    request_id,
                )
                .await,
            ],
            ClientAction::Record(param) => {
                vec![self.record_control(state, param.enabled, request_id).await]
            }
            ClientAction::ReplayBundle(param) => {
                vec![self.replay_control(state, param.bundle, request_id).await]
            }
        }
    }

    /// Handles the `record` control action (#030), **admission-first** like
    /// [`control`](Self::control): pass the rate limiter, gate [`Permission::Admin`],
    /// then flip the venue's scenario-capture window via the **same**
    /// [`AppState::set_recording`] the REST record route calls (control parity).
    async fn record_control(
        &self,
        state: &Arc<AppState>,
        enabled: bool,
        request_id: Option<String>,
    ) -> WsMessage {
        if let Some(error) = self.admit_control(state, &request_id) {
            return error;
        }
        state.set_recording(enabled);
        WsMessage::RecordingState {
            recording: state.is_recording(),
        }
    }

    /// Handles the `replay_bundle` control action (#030): admission-first, then
    /// replay the submitted bundle **offline** via the **same**
    /// [`AppState::replay_bundle`] the REST replay route calls (control parity). A
    /// corrupt / version-mismatched / malformed bundle surfaces as a non-terminal
    /// typed WS error (the connection stays open); a clean replay returns the
    /// reconstructed summary.
    async fn replay_control(
        &self,
        state: &Arc<AppState>,
        bundle: ScenarioBundle,
        request_id: Option<String>,
    ) -> WsMessage {
        if let Some(error) = self.admit_control(state, &request_id) {
            return error;
        }
        match state.replay_bundle(&bundle).await {
            Ok(report) => WsMessage::ReplayComplete {
                report: report.to_response(),
            },
            Err(error) => WsMessage::Error(VenueError::from(error).ws_error(request_id)),
        }
    }

    /// The shared control-admission gate: rate-limit **before** the permission
    /// check (so a forbidden control frame still counts against the budget), then
    /// require [`Permission::Admin`]. Returns `Some(error)` to reject, `None` to
    /// proceed.
    fn admit_control(
        &self,
        state: &Arc<AppState>,
        request_id: &Option<String>,
    ) -> Option<WsMessage> {
        let key = RateLimitKey::Account {
            account: self.claims.account().clone(),
            revocation_epoch: self.claims.revocation_epoch,
            tier: RateLimitTier::from_permissions(&self.claims.permissions),
        };
        let decision = state.auth().rate_limiter().check_and_record_status(&key);
        if !decision.allowed {
            return Some(WsMessage::Error(
                VenueError::RateLimited.ws_error(request_id.clone()),
            ));
        }
        if !self.claims.has_permission(Permission::Admin) {
            return Some(WsMessage::Error(
                VenueError::Forbidden(Permission::Admin).ws_error(request_id.clone()),
            ));
        }
        None
    }

    /// Resolves the subscription key for `(channel, symbol)`: the canonical
    /// [`Symbol`] string for the instrument channels, or the raw underlying for
    /// `prices`. Returns the parsed [`Symbol`] for the instrument channels so an
    /// orderbook subscribe can serve a snapshot without re-parsing.
    fn resolve_key(
        &self,
        channel: SubscriptionChannel,
        symbol: &str,
    ) -> Result<(String, Option<Symbol>), WsError> {
        match channel {
            SubscriptionChannel::Prices => Ok((symbol.to_string(), None)),
            _ => match Symbol::parse(symbol) {
                Ok(parsed) => Ok((parsed.as_str().to_string(), Some(parsed))),
                Err(error) => Err(VenueError::from(error).ws_error(None)),
            },
        }
    }

    fn subscribe(
        &mut self,
        params: protocol::SubscribeParams,
        manager: &OrderbookSubscriptionManager,
    ) -> Vec<WsMessage> {
        let (key, parsed) = match self.resolve_key(params.channel, &params.symbol) {
            Ok(resolved) => resolved,
            Err(error) => return vec![WsMessage::Error(error)],
        };

        let entry = (params.channel, key.clone());
        if !self.subscriptions.contains_key(&entry)
            && self.subscriptions.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION
        {
            return vec![WsMessage::Error(cap_error(None))];
        }
        self.subscriptions.insert(entry, params.depth);

        let mut out = Vec::new();
        if params.channel == SubscriptionChannel::Orderbook
            && let Some(symbol) = &parsed
        {
            let snapshot = manager.orderbook_snapshot(symbol, params.depth);
            if let WsMessage::OrderbookSnapshot { sequence, .. } = &snapshot {
                self.baselines.insert(key.clone(), *sequence);
            }
            out.push(snapshot);
        }
        out.push(WsMessage::Subscribed {
            channel: params.channel,
            symbol: key,
        });
        out
    }

    fn unsubscribe(&mut self, channel: &SubscriptionChannel, symbol: &str) -> Vec<WsMessage> {
        // Canonicalise so an unsubscribe matches the stored key; fall back to the
        // raw string when it does not parse (a best-effort teardown).
        let key = match channel {
            SubscriptionChannel::Prices => symbol.to_string(),
            _ => Symbol::parse(symbol)
                .map(|parsed| parsed.as_str().to_string())
                .unwrap_or_else(|_| symbol.to_string()),
        };
        self.subscriptions.remove(&(*channel, key.clone()));
        if *channel == SubscriptionChannel::Orderbook {
            self.baselines.remove(&key);
        }
        vec![WsMessage::Unsubscribed {
            channel: *channel,
            symbol: key,
        }]
    }

    fn batch_subscribe(
        &mut self,
        batch: protocol::BatchParams,
        manager: &OrderbookSubscriptionManager,
    ) -> Vec<WsMessage> {
        // Reject an over-size batch up-front (a DoS control) before iterating the
        // client array — the per-item cap alone would still admit an unbounded loop.
        if batch.subscriptions.len() > MAX_BATCH_SIZE {
            return vec![WsMessage::Error(batch_too_large(batch.request_id))];
        }
        let mut out = Vec::new();
        let mut results = Vec::new();
        for item in batch.subscriptions {
            let params = protocol::SubscribeParams {
                channel: item.channel,
                symbol: item.symbol.clone(),
                depth: item.depth,
            };
            let messages = self.subscribe(params, manager);
            let status = match messages.iter().find_map(|m| match m {
                WsMessage::Error(error) => Some(error.message.clone()),
                _ => None,
            }) {
                Some(message) => message,
                None => "ok".to_string(),
            };
            // Forward any snapshot produced by the item, but not its per-item
            // `subscribed` ack (the batch ack subsumes it).
            out.extend(messages.into_iter().filter(|m| {
                matches!(m, WsMessage::OrderbookSnapshot { .. } | WsMessage::Error(_))
            }));
            results.push(SubscriptionResult {
                channel: item.channel,
                symbol: Some(item.symbol),
                underlying: None,
                status,
            });
        }
        out.push(WsMessage::BatchSubscribed {
            request_id: batch.request_id,
            subscriptions: results,
        });
        out
    }

    fn batch_unsubscribe(&mut self, batch: protocol::BatchParams) -> Vec<WsMessage> {
        if batch.subscriptions.len() > MAX_BATCH_SIZE {
            return vec![WsMessage::Error(batch_too_large(batch.request_id))];
        }
        let mut results = Vec::new();
        for item in batch.subscriptions {
            self.unsubscribe(&item.channel, &item.symbol);
            results.push(SubscriptionResult {
                channel: item.channel,
                symbol: Some(item.symbol),
                underlying: None,
                status: "ok".to_string(),
            });
        }
        vec![WsMessage::BatchUnsubscribed {
            request_id: batch.request_id,
            subscriptions: results,
        }]
    }

    fn list_subscriptions(&self) -> WsMessage {
        let mut active: Vec<ActiveSubscription> = self
            .subscriptions
            .iter()
            .map(|((channel, key), depth)| ActiveSubscription {
                channel: *channel,
                symbol: Some(key.clone()),
                underlying: None,
                depth: *depth,
            })
            .collect();
        // Deterministic order regardless of map iteration order.
        active.sort_by(|a, b| {
            a.symbol
                .cmp(&b.symbol)
                .then_with(|| format!("{:?}", a.channel).cmp(&format!("{:?}", b.channel)))
        });
        WsMessage::SubscriptionList { active }
    }

    /// Handles a market-maker control action, **admission-first** (mirroring
    /// [`AuthService::admit`](crate::auth::AuthService::admit)): pass through the
    /// rate limiter, then gate [`Permission::Admin`], then route it as a sequenced
    /// [`VenueCommand::MarketMakerControl`]. Rate-limiting **before** the
    /// permission check means a forbidden control frame still counts against the
    /// budget, so it cannot be used to probe/flood for free.
    async fn control(
        &self,
        state: &Arc<AppState>,
        knobs: ControlKnobs,
        request_id: Option<String>,
    ) -> WsMessage {
        let key = RateLimitKey::Account {
            account: self.claims.account().clone(),
            revocation_epoch: self.claims.revocation_epoch,
            tier: RateLimitTier::from_permissions(&self.claims.permissions),
        };
        let decision = state.auth().rate_limiter().check_and_record_status(&key);
        if !decision.allowed {
            return WsMessage::Error(VenueError::RateLimited.ws_error(request_id));
        }
        if !self.claims.has_permission(Permission::Admin) {
            return WsMessage::Error(VenueError::Forbidden(Permission::Admin).ws_error(request_id));
        }
        let command = VenueCommand::MarketMakerControl {
            spread_multiplier: knobs.spread_multiplier,
            size_scalar: knobs.size_scalar,
            directional_skew: knobs.directional_skew,
            enabled: knobs.enabled,
        };
        match state.submit(command).await {
            // `MarketMakerControl` is venue-global and not routable on the
            // per-underlying submit path yet (#010 deviation); the error is
            // surfaced honestly rather than fabricating a success.
            Ok(_receipt) => WsMessage::Config {
                enabled: knobs.enabled.unwrap_or(true),
                spread_multiplier: knobs.spread_multiplier.unwrap_or(1.0),
                size_scalar: knobs.size_scalar.unwrap_or(1.0),
                directional_skew: knobs.directional_skew.unwrap_or(0.0),
            },
            Err(error) => WsMessage::Error(error.ws_error(request_id)),
        }
    }

    /// Decides whether a broadcast message is forwarded to this connection: it
    /// must match a subscribed `(channel, symbol)`, and an orderbook delta at or
    /// below the delivered baseline is dropped (already in the client's snapshot).
    fn filter(&mut self, message: WsMessage) -> Option<WsMessage> {
        let (channel, key, sequence) = classify(&message)?;
        if !self.subscriptions.contains_key(&(channel, key.clone())) {
            return None;
        }
        if channel == SubscriptionChannel::Orderbook
            && let Some(sequence) = sequence
        {
            let baseline = self.baselines.get(&key).copied().unwrap_or(0);
            if sequence <= baseline {
                return None;
            }
            self.baselines.insert(key, sequence);
        }
        Some(message)
    }

    /// Re-snapshots every orderbook subscription after a laggard drop, resetting
    /// each baseline to its fresh snapshot's sequence.
    fn resnapshot(&mut self, manager: &OrderbookSubscriptionManager) -> Vec<WsMessage> {
        let orderbook: Vec<(String, Option<usize>)> = self
            .subscriptions
            .iter()
            .filter(|((channel, _), _)| *channel == SubscriptionChannel::Orderbook)
            .map(|((_, key), depth)| (key.clone(), *depth))
            .collect();
        let mut out = Vec::new();
        for (key, depth) in orderbook {
            if let Ok(symbol) = Symbol::parse(&key) {
                let snapshot = manager.orderbook_snapshot(&symbol, depth);
                if let WsMessage::OrderbookSnapshot { sequence, .. } = &snapshot {
                    self.baselines.insert(key, *sequence);
                }
                out.push(snapshot);
            }
        }
        out
    }
}

/// A non-terminal WS error for a connection at its subscription cap.
fn cap_error(request_id: Option<String>) -> WsError {
    let mut error = protocol::decode_error(
        request_id,
        "subscription cap reached (max 256 per connection)",
    );
    error.category = crate::error::WsErrorCategory::Validation;
    error
}

/// A non-terminal WS error rejecting an over-size batch before it is iterated (a
/// DoS control).
fn batch_too_large(request_id: Option<String>) -> WsError {
    let mut error = protocol::decode_error(
        request_id,
        "batch too large (max 64 items per batch action)",
    );
    error.category = crate::error::WsErrorCategory::Validation;
    error
}

/// Classifies a broadcast market-data message into `(channel, symbol-key,
/// orderbook-sequence)`; returns `None` for a non-forwarded (directly-sent)
/// message.
fn classify(message: &WsMessage) -> Option<(SubscriptionChannel, String, Option<u64>)> {
    match message {
        WsMessage::OrderbookDelta {
            symbol, sequence, ..
        } => Some((
            SubscriptionChannel::Orderbook,
            symbol.as_str().to_string(),
            Some(*sequence),
        )),
        WsMessage::Trade { symbol, .. } => Some((
            SubscriptionChannel::Trades,
            symbol.as_str().to_string(),
            None,
        )),
        WsMessage::Fill { instrument, .. } => Some((
            SubscriptionChannel::Fills,
            instrument.as_str().to_string(),
            None,
        )),
        WsMessage::Price { symbol, .. } => {
            Some((SubscriptionChannel::Prices, symbol.clone(), None))
        }
        WsMessage::Quote { symbol, .. } => Some((
            SubscriptionChannel::Quotes,
            symbol.as_str().to_string(),
            None,
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::AccountId;

    fn claims(account: &str, permissions: Vec<Permission>) -> Claims {
        Claims::new(AccountId::new(account), permissions, 0, u64::MAX, 0)
    }

    fn sym() -> Symbol {
        match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        }
    }

    #[test]
    fn test_subscribe_orderbook_yields_snapshot_then_subscribed() {
        let manager = OrderbookSubscriptionManager::new();
        let mut connection = Connection::new(claims("reader", vec![Permission::Read]));
        let out = connection.subscribe(
            protocol::SubscribeParams {
                channel: SubscriptionChannel::Orderbook,
                symbol: "BTC-20240329-50000-C".to_string(),
                depth: Some(10),
            },
            &manager,
        );
        assert!(matches!(out[0], WsMessage::OrderbookSnapshot { .. }));
        assert!(matches!(out[1], WsMessage::Subscribed { .. }));
        // The baseline is recorded from the snapshot's sequence.
        assert_eq!(
            connection.baselines.get("BTC-20240329-50000-C").copied(),
            Some(0)
        );
    }

    #[test]
    fn test_subscription_cap_is_enforced() {
        let manager = OrderbookSubscriptionManager::new();
        let mut connection = Connection::new(claims("reader", vec![Permission::Read]));
        // Fill the cap with distinct trade subscriptions (unique symbols).
        for i in 0..MAX_SUBSCRIPTIONS_PER_CONNECTION {
            let symbol = format!("BTC-20240329-{}-C", 10_000 + i);
            let out = connection.subscribe(
                protocol::SubscribeParams {
                    channel: SubscriptionChannel::Trades,
                    symbol,
                    depth: None,
                },
                &manager,
            );
            assert!(matches!(out.last(), Some(WsMessage::Subscribed { .. })));
        }
        assert_eq!(
            connection.subscriptions.len(),
            MAX_SUBSCRIPTIONS_PER_CONNECTION
        );
        // One more distinct subscription is rejected.
        let out = connection.subscribe(
            protocol::SubscribeParams {
                channel: SubscriptionChannel::Trades,
                symbol: "BTC-20240329-99999-C".to_string(),
                depth: None,
            },
            &manager,
        );
        match &out[0] {
            WsMessage::Error(error) => assert!(error.message.contains("cap")),
            other => panic!("expected a cap error, got {other:?}"),
        }
        assert_eq!(
            connection.subscriptions.len(),
            MAX_SUBSCRIPTIONS_PER_CONNECTION
        );
    }

    #[test]
    fn test_batch_over_max_size_is_rejected_before_iterating() {
        let manager = OrderbookSubscriptionManager::new();
        let mut connection = Connection::new(claims("reader", vec![Permission::Read]));
        let items: Vec<protocol::BatchItem> = (0..(MAX_BATCH_SIZE + 1))
            .map(|i| protocol::BatchItem {
                channel: SubscriptionChannel::Trades,
                symbol: format!("BTC-20240329-{}-C", 10_000 + i),
                depth: None,
            })
            .collect();
        let out = connection.batch_subscribe(
            protocol::BatchParams {
                request_id: Some("req-1".to_string()),
                subscriptions: items,
            },
            &manager,
        );
        match &out[0] {
            WsMessage::Error(error) => {
                assert!(error.message.contains("batch too large"));
                assert!(!error.terminal);
                assert_eq!(error.request_id.as_deref(), Some("req-1"));
            }
            other => panic!("expected a batch-too-large error, got {other:?}"),
        }
        // Nothing was inserted (the over-size batch never iterated).
        assert!(connection.subscriptions.is_empty());
    }

    #[tokio::test]
    async fn test_control_rate_limit_precedes_permission_check() {
        // Admission-first: a Read (non-Admin) caller whose budget is exhausted gets
        // a throttle, NOT a forbidden — the rate limiter runs before the permission
        // check (so a forbidden control frame still counts against budget).
        let auth = match crate::state::AuthConfig::dev() {
            Ok(auth) => auth.with_rate_limit(0),
            Err(e) => panic!("dev auth builds: {e}"),
        };
        let state =
            crate::state::AppState::new(crate::state::AppStateConfig::new(["BTC"]).with_auth(auth))
                .expect("state builds");
        let connection = Connection::new(claims("reader", vec![Permission::Read]));
        let message = connection
            .control(
                &state,
                ControlKnobs {
                    enabled: Some(false),
                    ..ControlKnobs::default()
                },
                None,
            )
            .await;
        match message {
            WsMessage::Error(error) => assert_eq!(error.code, crate::error::WsErrorCode::Throttled),
            other => panic!("expected a throttle (rate limit before permission), got {other:?}"),
        }
    }

    #[test]
    fn test_filter_only_forwards_subscribed_symbols_and_dedups_by_baseline() {
        let manager = OrderbookSubscriptionManager::new();
        let mut connection = Connection::new(claims("reader", vec![Permission::Read]));
        connection.subscribe(
            protocol::SubscribeParams {
                channel: SubscriptionChannel::Orderbook,
                symbol: "BTC-20240329-50000-C".to_string(),
                depth: None,
            },
            &manager,
        );
        // A delta at seq 1 (> baseline 0) is forwarded and advances the baseline.
        let delta = WsMessage::OrderbookDelta {
            symbol: sym(),
            sequence: 1,
            changes: vec![],
        };
        assert!(connection.filter(delta).is_some());
        // A stale delta (seq 1 again) is dropped.
        let stale = WsMessage::OrderbookDelta {
            symbol: sym(),
            sequence: 1,
            changes: vec![],
        };
        assert!(connection.filter(stale).is_none());
        // A trade for an unsubscribed channel is dropped.
        let trade = WsMessage::Trade {
            trade_id: "x".to_string(),
            symbol: sym(),
            price: crate::exchange::Cents::new(1),
            quantity: 1,
            timestamp: EventTimestamp::new(1),
            maker_order_id: crate::models::VenueOrderId::new("m"),
            taker_order_id: crate::models::VenueOrderId::new("t"),
        };
        assert!(connection.filter(trade).is_none());
    }

    #[tokio::test]
    async fn test_control_without_admin_is_forbidden_and_non_terminal() {
        let state = crate::state::AppState::new(crate::state::AppStateConfig::new(["BTC"]))
            .expect("state builds");
        let connection = Connection::new(claims("reader", vec![Permission::Read]));
        let message = connection
            .control(
                &state,
                ControlKnobs {
                    enabled: Some(false),
                    ..ControlKnobs::default()
                },
                Some("req-1".to_string()),
            )
            .await;
        match message {
            WsMessage::Error(error) => {
                assert_eq!(error.code, crate::error::WsErrorCode::Forbidden);
                assert!(!error.terminal, "a forbidden control is non-terminal");
                assert_eq!(error.request_id.as_deref(), Some("req-1"));
            }
            other => panic!("expected a forbidden error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_admin_control_surfaces_not_routable_error_non_terminal() {
        // An Admin caller: the control is permission-admitted but
        // `MarketMakerControl` is not routable on the per-underlying path yet, so
        // the honest not-routable error surfaces (never a fabricated success).
        let state = crate::state::AppState::new(crate::state::AppStateConfig::new(["BTC"]))
            .expect("state builds");
        let connection = Connection::new(claims("admin", vec![Permission::Admin]));
        let message = connection
            .control(
                &state,
                ControlKnobs {
                    enabled: Some(false),
                    ..ControlKnobs::default()
                },
                None,
            )
            .await;
        match message {
            WsMessage::Error(error) => {
                assert_eq!(error.code, crate::error::WsErrorCode::InvalidOrder);
                assert!(!error.terminal);
            }
            other => panic!("expected a not-routable error, got {other:?}"),
        }
    }

    // ---- record / replay control actions (#030) --------------------------

    /// A crossing add pair seeded onto the sequenced path so the venue journal +
    /// executions carry one fill (for the replay-control test).
    async fn seed_crossing_ws(state: &Arc<AppState>) {
        use crate::exchange::{Cents, Hash32, LineageId, STPMode, TimeInForce};
        let lineage = LineageId::new("fauxchange");
        for (seq, account, owner, side) in [
            (0u64, "maker", 0x11u8, crate::exchange::Side::Sell),
            (1, "taker", 0x22, crate::exchange::Side::Buy),
        ] {
            let command = VenueCommand::AddOrder {
                symbol: sym(),
                order_id: lineage.venue_order_id(
                    "BTC",
                    crate::exchange::SequenceNumber::new(seq),
                    0,
                ),
                account: AccountId::new(account),
                owner: Hash32([owner; 32]),
                client_order_id: None,
                side,
                order_type: crate::models::OrderType::Limit,
                limit_price: Some(Cents::new(50_000)),
                quantity: 2,
                time_in_force: TimeInForce::Gtc,
                stp_mode: STPMode::None,
            };
            state.submit(command).await.expect("seed submit commits");
        }
    }

    #[tokio::test]
    async fn test_ws_record_control_admin_flips_the_shared_flag() {
        let state = crate::state::AppState::new(crate::state::AppStateConfig::new(["BTC"]))
            .expect("state builds");
        assert!(state.is_recording(), "records by default");
        let connection = Connection::new(claims("admin", vec![Permission::Admin]));
        let message = connection.record_control(&state, false, None).await;
        match message {
            WsMessage::RecordingState { recording } => assert!(!recording),
            other => panic!("expected a RecordingState ack, got {other:?}"),
        }
        assert!(
            !state.is_recording(),
            "the WS record action flips the SAME AppState flag the REST route does"
        );
    }

    #[tokio::test]
    async fn test_ws_record_control_without_admin_is_forbidden_and_leaves_flag() {
        let state = crate::state::AppState::new(crate::state::AppStateConfig::new(["BTC"]))
            .expect("state builds");
        let connection = Connection::new(claims("reader", vec![Permission::Read]));
        let message = connection
            .record_control(&state, false, Some("req-1".to_string()))
            .await;
        match message {
            WsMessage::Error(error) => {
                assert_eq!(error.code, crate::error::WsErrorCode::Forbidden);
                assert!(!error.terminal, "a forbidden control is non-terminal");
            }
            other => panic!("expected a forbidden error, got {other:?}"),
        }
        assert!(
            state.is_recording(),
            "a forbidden record action did not flip the flag"
        );
    }

    #[tokio::test]
    async fn test_ws_replay_control_admin_returns_reconstructed_report() {
        let state = crate::state::AppState::new(crate::state::AppStateConfig::new(["BTC"]))
            .expect("state builds");
        seed_crossing_ws(&state).await;
        let bundle = state.export_bundle().await.expect("export bundle");
        let connection = Connection::new(claims("admin", vec![Permission::Admin]));
        let message = connection.replay_control(&state, bundle, None).await;
        match message {
            WsMessage::ReplayComplete { report } => {
                assert_eq!(
                    report.executions, 2,
                    "the crossing's two legs are reconstructed"
                );
            }
            other => panic!("expected a ReplayComplete ack, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_ws_replay_control_version_mismatch_is_non_terminal_error() {
        let state = crate::state::AppState::new(crate::state::AppStateConfig::new(["BTC"]))
            .expect("state builds");
        seed_crossing_ws(&state).await;
        let mut bundle = state.export_bundle().await.expect("export bundle");
        bundle.manifest.versions.fauxchange = "0.0.0-mismatch".to_string();
        let connection = Connection::new(claims("admin", vec![Permission::Admin]));
        let message = connection.replay_control(&state, bundle, None).await;
        match message {
            WsMessage::Error(error) => {
                assert_eq!(error.code, crate::error::WsErrorCode::InvalidOrder);
                assert!(!error.terminal, "a bad-bundle reject keeps the socket open");
            }
            other => panic!("expected a non-terminal version-mismatch error, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_token_prefers_header_then_query() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer header-token".parse().unwrap(),
        );
        let query = WsAuthQuery {
            token: Some("query-token".to_string()),
            access_token: None,
        };
        assert_eq!(
            extract_token(&headers, &query).as_deref(),
            Some("header-token")
        );
        // Query fallback when no header is present.
        assert_eq!(
            extract_token(&HeaderMap::new(), &query).as_deref(),
            Some("query-token")
        );
        // `access_token` is accepted too.
        let query = WsAuthQuery {
            token: None,
            access_token: Some("access-token".to_string()),
        };
        assert_eq!(
            extract_token(&HeaderMap::new(), &query).as_deref(),
            Some("access-token")
        );
    }

    #[test]
    fn test_classify_maps_messages_to_channels() {
        let delta = WsMessage::OrderbookDelta {
            symbol: sym(),
            sequence: 7,
            changes: vec![],
        };
        assert_eq!(
            classify(&delta),
            Some((
                SubscriptionChannel::Orderbook,
                "BTC-20240329-50000-C".to_string(),
                Some(7)
            ))
        );
        let price = WsMessage::Price {
            symbol: "BTC".to_string(),
            price_cents: crate::exchange::Cents::new(1),
        };
        assert_eq!(
            classify(&price),
            Some((SubscriptionChannel::Prices, "BTC".to_string(), None))
        );
        // A directly-sent message classifies as None (never broadcast-forwarded).
        assert_eq!(
            classify(&WsMessage::Connected {
                message: "x".to_string()
            }),
            None
        );
    }
}
