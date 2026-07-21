//! The **parity comparison primitives** — the normalization rule and the
//! cross-surface join-key projections the conformance cases compare
//! ([03 §7](../../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees)).
//!
//! These are the library-side, `Result`-returning equivalents of the
//! `tests/conformance/` comparators (#018/#041): the **normalization rule**
//! ([`normalize_event`] / [`streams_parity`]) strips protocol-only fields (the
//! transport `venue_ts`, the per-surface `order_id` / `new_order_id` /
//! `client_order_id` mapping placeholder) and compares the venue identifiers
//! (`underlying_sequence`, `execution_id`, fills, resting-book state) verbatim;
//! the **join-key projection** ([`FillJoinKeys`]) extracts the shared observation
//! keys from a REST `ExecutionRecord`, a WS `fill`, and a FIX
//! `ExecutionReport (8)`.

use serde_json::Value;
use tokio::sync::broadcast;

use crate::exchange::VenueEvent;
use crate::gateway::fix::price::parse_decimal_to_cents;
use crate::models::WsMessage;

use super::harness::{field, msg_type};

/// The object keys carrying a per-surface order-id / `ClOrdID` mapping
/// placeholder, normalized away before a cross-surface stream comparison.
pub const STRIPPED_KEYS: &[&str] = &["order_id", "new_order_id", "client_order_id"];

/// The transport-timestamp key normalized to a canonical value.
pub const TRANSPORT_TS_KEY: &str = "venue_ts";

/// The canonical value every stripped key is rewritten to.
pub const NORMALIZED_PLACEHOLDER: &str = "<normalized>";

/// The canonical value the transport timestamp is rewritten to.
pub const NORMALIZED_TS: u64 = 0;

fn canonicalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if STRIPPED_KEYS.contains(&key.as_str()) {
                    *child = Value::String(NORMALIZED_PLACEHOLDER.to_string());
                } else if key == TRANSPORT_TS_KEY {
                    *child = Value::Number(NORMALIZED_TS.into());
                } else {
                    canonicalize(child);
                }
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                canonicalize(item);
            }
        }
        _ => {}
    }
}

/// Normalizes one committed `VenueEvent` to its comparable JSON projection.
pub fn normalize_event(event: &VenueEvent) -> Result<Value, String> {
    let mut value =
        serde_json::to_value(event).map_err(|e| format!("serialise VenueEvent: {e}"))?;
    canonicalize(&mut value);
    Ok(value)
}

/// Asserts two surfaces' committed `VenueEvent` streams are equal after
/// normalization (order-entry parity). Returns a redacted failure detail on any
/// divergence.
pub fn streams_parity(
    label_a: &str,
    events_a: &[VenueEvent],
    label_b: &str,
    events_b: &[VenueEvent],
) -> Result<(), String> {
    if events_a.len() != events_b.len() {
        return Err(format!(
            "{label_a} journaled {} events but {label_b} journaled {}",
            events_a.len(),
            events_b.len()
        ));
    }
    for (index, (ea, eb)) in events_a.iter().zip(events_b.iter()).enumerate() {
        let na = normalize_event(ea)?;
        let nb = normalize_event(eb)?;
        if na != nb {
            return Err(format!(
                "normalized event #{index} differs across {label_a} and {label_b}"
            ));
        }
    }
    Ok(())
}

// ============================================================================
// Cross-surface join keys (observation parity)
// ============================================================================

/// The surface-independent projection of one fill leg — the venue join keys
/// (`execution_id`, `liquidity`, `underlying_sequence`) plus `price` /
/// `quantity` / `side`. `venue_ts` is REST≡WS only (the FIX dialect carries no
/// venue-timestamp tag), so it is compared separately where present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillJoinKeys {
    /// The composite execution id (`ExecID (17)` on FIX).
    pub execution_id: String,
    /// `maker` / `taker`.
    pub liquidity: String,
    /// The total-order position (`SecondaryExecID (527)` on FIX).
    pub underlying_sequence: u64,
    /// `buy` / `sell`.
    pub side: String,
    /// The fill quantity.
    pub quantity: u64,
    /// The fill price in cents.
    pub price: u64,
}

fn json_str(value: &Value, key: &str) -> Option<String> {
    value.get(key)?.as_str().map(str::to_string)
}

fn json_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key)?.as_u64()
}

/// The `data` object of a WS `fill` message, or `None` for any other message.
#[must_use]
pub fn ws_fill_data(message: &WsMessage) -> Option<Value> {
    let value = serde_json::to_value(message).ok()?;
    if value.get("type")?.as_str()? != "fill" {
        return None;
    }
    value.get("data").cloned()
}

/// The taker-leg WS `fill` (side `buy`, liquidity `taker`) among drained messages.
#[must_use]
pub fn find_taker_fill(messages: &[WsMessage]) -> Option<WsMessage> {
    messages
        .iter()
        .find(|message| {
            matches!(ws_fill_data(message), Some(data)
                if data.get("liquidity").and_then(Value::as_str) == Some("taker")
                    && data.get("side").and_then(Value::as_str) == Some("buy"))
        })
        .cloned()
}

/// The join keys from a WS `fill` (the public anonymised projection).
#[must_use]
pub fn ws_fill_join_keys(message: &WsMessage) -> Option<(FillJoinKeys, u64)> {
    let data = ws_fill_data(message)?;
    let keys = FillJoinKeys {
        execution_id: json_str(&data, "execution_id")?,
        liquidity: json_str(&data, "liquidity")?,
        underlying_sequence: json_u64(&data, "underlying_sequence")?,
        side: json_str(&data, "side")?,
        quantity: json_u64(&data, "quantity")?,
        price: json_u64(&data, "price")?,
    };
    let venue_ts = json_u64(&data, "venue_ts")?;
    Some((keys, venue_ts))
}

/// The join keys from a REST `ExecutionRecord` body (the account-scoped
/// projection).
#[must_use]
pub fn execution_record_join_keys(record: &Value) -> Option<(FillJoinKeys, u64)> {
    let keys = FillJoinKeys {
        execution_id: json_str(record, "execution_id")?,
        liquidity: json_str(record, "liquidity")?,
        underlying_sequence: json_u64(record, "underlying_sequence")?,
        side: json_str(record, "side")?,
        quantity: json_u64(record, "quantity")?,
        price: json_u64(record, "price_cents")?,
    };
    let venue_ts = json_u64(record, "executed_at")?;
    Some((keys, venue_ts))
}

/// The join keys a `Trade` `ExecutionReport (8)` carries (the FIX projection; no
/// `venue_ts` in this dialect).
#[must_use]
pub fn fix_report_projection(frame: &[u8]) -> Option<FillJoinKeys> {
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
    let price = parse_decimal_to_cents(&field(frame, "31")?).ok()?.get();
    Some(FillJoinKeys {
        execution_id: field(frame, "17")?,
        liquidity,
        underlying_sequence: field(frame, "527")?.parse().ok()?,
        side,
        quantity: field(frame, "32")?.parse().ok()?,
        price,
    })
}

/// Drains every currently-buffered message from a WS broadcast receiver.
#[must_use]
pub fn drain(rx: &mut broadcast::Receiver<WsMessage>) -> Vec<WsMessage> {
    let mut out = Vec::new();
    while let Ok(message) = rx.try_recv() {
        out.push(message);
    }
    out
}
