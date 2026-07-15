//! Golden wire-format tests for the error envelopes (TESTING.md §4).
//!
//! The redacted, versioned error-envelope shape is part of the wire contract,
//! so both the REST `ErrorEnvelope` and the WebSocket `ws-error.v1` envelope are
//! pinned against a committed golden. A change to either shape must update the
//! matching golden in the same commit ([docs/03 §4.2](../docs/03-protocol-surfaces.md),
//! [docs/01 §11](../docs/01-domain-model.md)).
//!
//! Fixtures are compared as parsed JSON values so key order and whitespace do
//! not make the assertion brittle.

use fauxchange::{Permission, VenueError};

/// Loads and parses a golden fixture under `tests/golden/`.
fn load_golden(relative: &str) -> serde_json::Value {
    let path = format!("{}/tests/golden/{}", env!("CARGO_MANIFEST_DIR"), relative);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) => panic!("failed to read golden {path}: {e}"),
    };
    match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(e) => panic!("failed to parse golden {path}: {e}"),
    }
}

#[test]
fn test_golden_rest_error_envelope_matches_forbidden_shape() {
    let envelope = VenueError::Forbidden(Permission::Trade).error_envelope();
    let produced = match serde_json::to_value(&envelope) {
        Ok(value) => value,
        Err(e) => panic!("failed to serialise the REST error envelope: {e}"),
    };
    assert_eq!(produced, load_golden("rest/error_envelope.json"));
}

#[test]
fn test_golden_ws_error_envelope_matches_forbidden_shape() {
    let envelope = VenueError::Forbidden(Permission::Trade).ws_error(Some("req-1".to_string()));
    let produced = match serde_json::to_value(&envelope) {
        Ok(value) => value,
        Err(e) => panic!("failed to serialise the WS error envelope: {e}"),
    };
    assert_eq!(produced, load_golden("ws/error.json"));
}
