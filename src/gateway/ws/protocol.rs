//! The client → server WebSocket action protocol and its frame parser
//! ([03 §4](../../../docs/03-protocol-surfaces.md)).
//!
//! Client frames are JSON objects tagged by an `action` field. WS is **not** an
//! order-entry surface — its only client actions are subscription management
//! (`subscribe` / `unsubscribe` / `batch_subscribe` / `batch_unsubscribe` /
//! `list_subscriptions`) and the permission-gated market-maker control actions
//! (`set_spread` / `set_size` / `set_skew` / `kill` / `enable`). Any
//! order-entry-shaped frame is **rejected** with a typed WS error (order entry
//! is REST/FIX only).

use serde::Deserialize;

use crate::error::{WS_ERROR_SCHEMA, WsError, WsErrorCategory, WsErrorCode};
use crate::models::SubscriptionChannel;
use crate::simulation::ScenarioBundle;

/// A `subscribe` / `unsubscribe` action payload: a channel, a symbol, and an
/// optional orderbook `depth`.
#[derive(Debug, Clone, Deserialize)]
pub struct SubscribeParams {
    /// The market-data channel.
    pub channel: SubscriptionChannel,
    /// The symbol (canonical `UNDERLYING-YYYYMMDD-STRIKE-STYLE` for the
    /// instrument channels; the underlying ticker for `prices`).
    pub symbol: String,
    /// The orderbook depth to serve on the snapshot (best-N levels per side).
    #[serde(default)]
    pub depth: Option<usize>,
}

/// One item of a batch subscribe / unsubscribe.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchItem {
    /// The market-data channel.
    pub channel: SubscriptionChannel,
    /// The symbol.
    pub symbol: String,
    /// The orderbook depth to serve on the snapshot.
    #[serde(default)]
    pub depth: Option<usize>,
}

/// A `batch_subscribe` / `batch_unsubscribe` action payload.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchParams {
    /// A correlation id echoed on the batch ack, when present.
    #[serde(default)]
    pub request_id: Option<String>,
    /// The per-item subscription actions.
    pub subscriptions: Vec<BatchItem>,
}

/// A single-`value` control action payload (`set_spread` / `set_size` /
/// `set_skew`).
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct ValueParam {
    /// The new dimensionless multiplier (not money).
    pub value: f64,
}

/// A `record` control action payload (#030): flip the scenario-capture window.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct RecordParam {
    /// `true` opens the capture window, `false` closes it.
    pub enabled: bool,
}

/// A `replay_bundle` control action payload (#030): the self-describing scenario
/// bundle to replay **offline** into a fresh registry. Bounded by the inbound WS
/// frame-size cap ([`MAX_WS_FRAME_BYTES`](super::MAX_WS_FRAME_BYTES)); an oversize
/// bundle is rejected by the transport before it reaches this parser.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplayBundleParam {
    /// The scenario bundle (journal streams + run manifest).
    pub bundle: ScenarioBundle,
}

/// A client → server action, internally tagged by `action`
/// (`#[serde(tag = "action", rename_all = "snake_case")]`).
///
/// There is **no order-entry variant**: WS carries subscriptions and
/// market-maker control only.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ClientAction {
    /// Subscribe to one `(channel, symbol)`.
    Subscribe(SubscribeParams),
    /// Unsubscribe from one `(channel, symbol)`.
    Unsubscribe(SubscribeParams),
    /// Subscribe to several `(channel, symbol)` at once.
    BatchSubscribe(BatchParams),
    /// Unsubscribe from several `(channel, symbol)` at once.
    BatchUnsubscribe(BatchParams),
    /// List this connection's active subscriptions.
    ListSubscriptions,
    /// Set the global market-maker spread multiplier (control; `Admin`).
    SetSpread(ValueParam),
    /// Set the global market-maker size scalar (control; `Admin`).
    SetSize(ValueParam),
    /// Set the global market-maker directional skew (control; `Admin`).
    SetSkew(ValueParam),
    /// Disable all market-maker quoting — the kill switch (control; `Admin`).
    Kill,
    /// Enable market-maker quoting (control; `Admin`).
    Enable,
    /// Flip the scenario-capture window on or off (control; `Admin`, #030).
    Record(RecordParam),
    /// Replay a recorded scenario bundle offline into a fresh registry (control;
    /// `Admin`, #030).
    ReplayBundle(ReplayBundleParam),
}

/// The outcome of parsing one client text frame.
#[derive(Debug)]
pub enum FrameOutcome {
    /// A well-formed action plus its correlation id (when present).
    Action(ClientAction, Option<String>),
    /// A rejected frame (order-entry-shaped or undecodable) — a **non-terminal**
    /// command error: the connection stays open.
    Reject(Box<WsError>),
}

/// The `action` verbs that name order entry — rejected because WS has no
/// place / cancel / replace.
const ORDER_ENTRY_ACTIONS: &[&str] = &[
    "place_order",
    "place_limit_order",
    "place_market_order",
    "new_order",
    "new_order_single",
    "order",
    "cancel_order",
    "cancel",
    "replace_order",
    "replace",
    "modify_order",
];

/// Parses one client text frame into a [`FrameOutcome`], rejecting any
/// order-entry-shaped frame and any undecodable frame with a typed
/// (non-terminal) WS error.
#[must_use]
pub fn parse_frame(text: &str) -> FrameOutcome {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => {
            return FrameOutcome::Reject(Box::new(decode_error(None, "malformed JSON frame")));
        }
    };

    let request_id = value
        .get("request_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);

    // `serde_json::Value` silently keeps the last value for a repeated key, so a
    // duplicate top-level field (e.g. two `action`s) would slip through the parse
    // above. A duplicate key is a malformed frame — detect it on the raw text and
    // reject with the same non-terminal decode error shape as malformed JSON.
    if value.is_object() && has_duplicate_top_level_key(text) {
        return FrameOutcome::Reject(Box::new(decode_error(
            request_id,
            "duplicate top-level field",
        )));
    }

    let action = value
        .get("action")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);

    match action {
        Some(action) if is_order_entry_action(&action) => {
            FrameOutcome::Reject(Box::new(order_entry_rejected(request_id)))
        }
        Some(_) => match serde_json::from_value::<ClientAction>(value) {
            Ok(action) => FrameOutcome::Action(action, request_id),
            Err(_) => FrameOutcome::Reject(Box::new(decode_error(
                request_id,
                "unknown or malformed action",
            ))),
        },
        None if looks_like_order_entry(&value) => {
            FrameOutcome::Reject(Box::new(order_entry_rejected(request_id)))
        }
        None => FrameOutcome::Reject(Box::new(decode_error(request_id, "missing `action` field"))),
    }
}

/// Whether `action` names an order-entry operation (which WS does not carry).
#[must_use]
pub fn is_order_entry_action(action: &str) -> bool {
    ORDER_ENTRY_ACTIONS.contains(&action)
}

/// Whether an actionless frame is order-entry-shaped (carries a `side` plus a
/// `price` or `quantity`).
fn looks_like_order_entry(value: &serde_json::Value) -> bool {
    value.get("side").is_some() && (value.get("price").is_some() || value.get("quantity").is_some())
}

/// Whether `text` — already known to parse as a JSON object — carries a
/// duplicate top-level key, which [`serde_json::Value`] would silently collapse
/// to the last value. Only the top-level object keys are inspected.
#[must_use]
fn has_duplicate_top_level_key(text: &str) -> bool {
    // Given a valid JSON object, [`UniqueTopLevelKeys`] only ever errors on the
    // first repeated key (nested values are ignored), so `is_err()` means the
    // frame has a duplicate top-level field.
    serde_json::from_str::<UniqueTopLevelKeys>(text).is_err()
}

/// A structural guard whose [`Deserialize`] visits a JSON object's top-level
/// keys and fails on the first repeat. Values are ignored via
/// [`serde::de::IgnoredAny`] — nested objects are not inspected — so the check
/// stays pure (no wall-clock, no RNG) and touches only top-level keys.
struct UniqueTopLevelKeys;

impl<'de> Deserialize<'de> for UniqueTopLevelKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct KeyVisitor;

        impl<'de> serde::de::Visitor<'de> for KeyVisitor {
            type Value = UniqueTopLevelKeys;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object with unique top-level keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut seen: Vec<String> = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    if seen.contains(&key) {
                        return Err(serde::de::Error::custom("duplicate top-level key"));
                    }
                    map.next_value::<serde::de::IgnoredAny>()?;
                    seen.push(key);
                }
                Ok(UniqueTopLevelKeys)
            }
        }

        deserializer.deserialize_map(KeyVisitor)
    }
}

/// A non-terminal decode/validation WS error for a malformed frame.
#[must_use]
pub fn decode_error(request_id: Option<String>, message: &str) -> WsError {
    WsError {
        schema: WS_ERROR_SCHEMA.to_string(),
        code: WsErrorCode::BadRequest,
        category: WsErrorCategory::Decode,
        message: message.to_string(),
        request_id,
        retryable: false,
        retry_after_ms: None,
        terminal: false,
    }
}

/// A non-terminal WS error rejecting an order-entry-shaped frame — WS is not an
/// order-entry surface.
#[must_use]
pub fn order_entry_rejected(request_id: Option<String>) -> WsError {
    WsError {
        schema: WS_ERROR_SCHEMA.to_string(),
        code: WsErrorCode::BadRequest,
        category: WsErrorCategory::Validation,
        message: "WebSocket is not an order-entry surface; place orders over REST or FIX"
            .to_string(),
        request_id,
        retryable: false,
        retry_after_ms: None,
        terminal: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_subscribe_action() {
        let frame = r#"{"action":"subscribe","channel":"orderbook","symbol":"BTC-20240329-50000-C","depth":5}"#;
        match parse_frame(frame) {
            FrameOutcome::Action(ClientAction::Subscribe(p), _) => {
                assert_eq!(p.channel, SubscriptionChannel::Orderbook);
                assert_eq!(p.symbol, "BTC-20240329-50000-C");
                assert_eq!(p.depth, Some(5));
            }
            other => panic!("expected a Subscribe action, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unit_control_actions() {
        for (frame, ok) in [
            (
                r#"{"action":"kill"}"#,
                matches!(
                    parse_frame(r#"{"action":"kill"}"#),
                    FrameOutcome::Action(ClientAction::Kill, _)
                ),
            ),
            (
                r#"{"action":"enable"}"#,
                matches!(
                    parse_frame(r#"{"action":"enable"}"#),
                    FrameOutcome::Action(ClientAction::Enable, _)
                ),
            ),
            (
                r#"{"action":"list_subscriptions"}"#,
                matches!(
                    parse_frame(r#"{"action":"list_subscriptions"}"#),
                    FrameOutcome::Action(ClientAction::ListSubscriptions, _)
                ),
            ),
        ] {
            assert!(ok, "frame {frame} must parse to its unit action");
        }
    }

    #[test]
    fn test_parse_set_spread_carries_value() {
        match parse_frame(r#"{"action":"set_spread","value":1.5}"#) {
            FrameOutcome::Action(ClientAction::SetSpread(v), _) => {
                assert!((v.value - 1.5).abs() < f64::EPSILON);
            }
            other => panic!("expected SetSpread, got {other:?}"),
        }
    }

    #[test]
    fn test_order_entry_action_is_rejected() {
        for action in ORDER_ENTRY_ACTIONS {
            let frame =
                format!(r#"{{"action":"{action}","side":"buy","price":50000,"quantity":1}}"#);
            match parse_frame(&frame) {
                FrameOutcome::Reject(err) => {
                    assert_eq!(err.category, WsErrorCategory::Validation);
                    assert!(err.message.contains("not an order-entry surface"));
                    assert!(!err.terminal, "an order-entry rejection is non-terminal");
                }
                other => panic!("order-entry action {action} must be rejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_actionless_order_shaped_frame_is_rejected() {
        // No `action`, but a side + price/quantity → order-entry-shaped → rejected.
        match parse_frame(r#"{"side":"buy","price":50000,"quantity":10}"#) {
            FrameOutcome::Reject(err) => {
                assert!(err.message.contains("not an order-entry surface"))
            }
            other => panic!("expected an order-entry rejection, got {other:?}"),
        }
    }

    #[test]
    fn test_malformed_frame_is_a_decode_error() {
        match parse_frame("not json") {
            FrameOutcome::Reject(err) => {
                assert_eq!(err.code, WsErrorCode::BadRequest);
                assert_eq!(err.category, WsErrorCategory::Decode);
                assert!(!err.terminal);
            }
            other => panic!("expected a decode error, got {other:?}"),
        }
    }

    #[test]
    fn test_unknown_action_is_a_decode_error() {
        match parse_frame(r#"{"action":"frobnicate"}"#) {
            FrameOutcome::Reject(err) => assert_eq!(err.code, WsErrorCode::BadRequest),
            other => panic!("expected a decode error, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_frame_rejects_duplicate_action_field() {
        // Two `action` keys: `serde_json::Value` would keep only the last; the
        // frame is malformed and must be rejected as a decode error.
        let frame = r#"{"action":"subscribe","action":"set_spread","channel":"orderbook","symbol":"BTC-20240329-50000-C","value":1.5}"#;
        match parse_frame(frame) {
            FrameOutcome::Reject(err) => {
                assert_eq!(err.code, WsErrorCode::BadRequest);
                assert_eq!(err.category, WsErrorCategory::Decode);
                assert!(!err.terminal, "a duplicate-key rejection is non-terminal");
            }
            other => panic!("a duplicate `action` frame must be rejected, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_frame_rejects_duplicate_request_id_field() {
        let frame = r#"{"action":"list_subscriptions","request_id":"a","request_id":"b"}"#;
        match parse_frame(frame) {
            FrameOutcome::Reject(err) => {
                assert_eq!(err.code, WsErrorCode::BadRequest);
                assert_eq!(err.category, WsErrorCategory::Decode);
                assert!(!err.terminal);
            }
            other => panic!("a duplicate `request_id` frame must be rejected, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_frame_accepts_unique_keys() {
        // A well-formed frame with all-unique top-level keys still decodes.
        let frame = r#"{"action":"subscribe","channel":"orderbook","symbol":"BTC-20240329-50000-C","depth":5}"#;
        assert!(matches!(
            parse_frame(frame),
            FrameOutcome::Action(ClientAction::Subscribe(_), _)
        ));
    }

    #[test]
    fn test_parse_record_action_carries_enabled() {
        match parse_frame(r#"{"action":"record","enabled":false}"#) {
            FrameOutcome::Action(ClientAction::Record(p), _) => assert!(!p.enabled),
            other => panic!("expected a Record action, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_replay_bundle_action_carries_bundle() {
        let frame = r#"{"action":"replay_bundle","request_id":"r-1","bundle":{"schema":"scenario-bundle.v1","manifest":{"seed":0,"clock_mode":"realtime"},"streams":[]}}"#;
        match parse_frame(frame) {
            FrameOutcome::Action(ClientAction::ReplayBundle(p), rid) => {
                assert!(p.bundle.is_current_schema());
                assert!(p.bundle.streams.is_empty());
                assert_eq!(rid.as_deref(), Some("r-1"));
            }
            other => panic!("expected a ReplayBundle action, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_replay_bundle_without_manifest_is_a_decode_error() {
        // The bundle's `manifest` is required; a bundle-shaped frame missing it is a
        // non-terminal decode error, never a panic.
        let frame =
            r#"{"action":"replay_bundle","bundle":{"schema":"scenario-bundle.v1","streams":[]}}"#;
        match parse_frame(frame) {
            FrameOutcome::Reject(error) => {
                assert_eq!(error.code, WsErrorCode::BadRequest);
                assert!(!error.terminal);
            }
            other => panic!("a manifest-less replay bundle must be rejected, got {other:?}"),
        }
    }

    #[test]
    fn test_batch_request_id_is_captured() {
        let frame = r#"{"action":"batch_subscribe","request_id":"req-1","subscriptions":[{"channel":"trades","symbol":"BTC-20240329-50000-C"}]}"#;
        match parse_frame(frame) {
            FrameOutcome::Action(ClientAction::BatchSubscribe(b), rid) => {
                assert_eq!(rid.as_deref(), Some("req-1"));
                assert_eq!(b.subscriptions.len(), 1);
            }
            other => panic!("expected BatchSubscribe, got {other:?}"),
        }
    }
}
