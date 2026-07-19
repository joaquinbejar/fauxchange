//! The REST JSON request-body decoder fuzz target (#052) — a **secondary**
//! fuzz target ([docs/08 §6](../../docs/08-threat-model.md#6-fuzzing-and-adversarial-testing));
//! the FIX tag-value parser (`fix_decode.rs`, #042) is the primary target.
//!
//! Drives arbitrary bytes through the **exact same two-stage decode path**
//! a REST order-entry handler drives on every inbound request body
//! (`src/gateway/rest/orders.rs`, e.g. `place_limit_order`):
//!
//! 1. [`axum::Json::<T>::from_bytes`] — the SAME function axum's own
//!    `Json<T>: FromRequest` impl calls internally after buffering a request
//!    body (`axum-0.8.9/src/json.rs`), turning bytes into a typed DTO or a
//!    typed [`axum::extract::rejection::JsonRejection`]. Bounded first by the
//!    router's own explicit [`MAX_REQUEST_BODY_BYTES`] ceiling
//!    (`DefaultBodyLimit::max`, `src/gateway/rest/mod.rs`) — a body over this
//!    ceiling never reaches an extractor in production (the tower layer
//!    rejects it with a `413` first), so the harness mirrors that gate by
//!    skipping any larger input rather than reimplementing the tower
//!    middleware here.
//! 2. `.validate()` — the SAME economic-shape check the handler calls next
//!    (`request.validate()?`, routing through
//!    [`fauxchange::validate_order_shape`]), turning an in-range-syntax DTO
//!    into `Ok(())` or a typed [`fauxchange::VenueError`].
//!
//! Four representative request DTOs are driven per input (not a
//! reimplementation — the type parameter alone selects which real `Deserialize`
//! impl runs): [`PlaceLimitOrderRequest`] (the priced order-entry shape, the
//! `MAX_PRICE_CENTS` / `MAX_ORDER_QUANTITY` economic ceiling), coverage
//! chosen matches the [08 §4](../../docs/08-threat-model.md#4-untrusted-input-hardening)
//! REST-JSON-body row: typed DTO + `#[serde(deny_unknown_fields)]` under the
//! body-size ceiling, a `PlaceMarketOrderRequest` (no price field, a distinct
//! shape), a `BulkOrderRequest` (a nested `Vec` whose `symbol` field is the
//! canonical [`fauxchange::exchange::Symbol`] newtype — its `#[serde(try_from
//! = "String")]` re-validates through `SymbolParser` on every array element,
//! covering "malformed symbols" at scale), and an `InsertPriceRequest` (a bare
//! `String` symbol field, no round-trip validator, for contrast).
//!
//! Neither stage may ever panic or allocate unboundedly; a malformed input
//! must always reject cleanly. This target does not assert WHICH typed
//! reject a given input produces (that is `tests/security.rs`'s
//! adversarial-fixture matrix, which shares this exact corpus); it only
//! proves the decode path itself never crashes or hangs on adversarial
//! input. `#![no_main]` + the raw-pointer libFuzzer FFI entrypoint
//! `fuzz_target!` expands to is the standard, documented `unsafe` exception
//! for a libFuzzer harness — isolated to this fuzz-only crate, never the
//! venue's `#![forbid(unsafe_code)]` source (`src/lib.rs`).

#![no_main]

use axum::Json;
use fauxchange::gateway::rest::MAX_REQUEST_BODY_BYTES;
use fauxchange::{
    BulkOrderRequest, InsertPriceRequest, PlaceLimitOrderRequest, PlaceMarketOrderRequest,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The real router rejects an over-ceiling body with a `413` BEFORE any
    // extractor runs (`DefaultBodyLimit::max`, `src/gateway/rest/mod.rs`) — a
    // body this large never reaches `Json::from_bytes` in production.
    if data.len() > MAX_REQUEST_BODY_BYTES {
        return;
    }

    // A priced limit order — the economic-field ceiling (`MAX_PRICE_CENTS` /
    // `MAX_ORDER_QUANTITY`, `validate_order_shape`).
    if let Ok(Json(request)) = Json::<PlaceLimitOrderRequest>::from_bytes(data) {
        let _ = request.validate();
    }
    // A market order — no price field, the zero-quantity / no-price shape.
    if let Ok(Json(request)) = Json::<PlaceMarketOrderRequest>::from_bytes(data) {
        let _ = request.validate();
    }
    // A bulk batch — nested array + the `Symbol` round-trip validator on every
    // item (the "malformed symbols" class, at array scale).
    if let Ok(Json(request)) = Json::<BulkOrderRequest>::from_bytes(data) {
        for item in &request.orders {
            let _ = item.validate();
        }
    }
    // A bare-`String`-symbol DTO, for contrast with the newtype-validated
    // shape above.
    let _ = Json::<InsertPriceRequest>::from_bytes(data);
});
