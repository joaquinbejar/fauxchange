//! Shared boundary: the typed [`VenueError`] every gateway translates through,
//! with three renderings of one failure.
//!
//! - **HTTP (REST/WS)** — [`VenueError::http_status`] maps each variant to
//!   exactly one status (`404`/`400`/`401`/`403`/`409`/`429`/`500`), and
//!   [`IntoResponse`] emits a typed [`ErrorEnvelope`] JSON body (never a
//!   `serde_json::Value`) plus `X-RateLimit-*` context on `429`.
//! - **FIX** — [`VenueError::fix_reject`] resolves, **by the inbound message
//!   context** ([`FixRejectContext`]) rather than by the error alone, to the
//!   right reject message ([`FixReject`]): `ExecutionReport (8) Rejected` /
//!   `OrderCancelReject (9)` / `MarketDataRequestReject (Y)` /
//!   `BusinessMessageReject (j)` / `Reject (3)` with the reason field and a
//!   redacted `Text (58)`. This is a **seam** — types plus a pure mapping the
//!   v0.4 acceptor resolves against — not a wire encoder (that is #039).
//! - **WebSocket** — [`VenueError::ws_error`] maps each variant to the
//!   versioned [`WsError`] envelope (`ws-error.v1`) with a stable `(code,
//!   category)` pair, `terminal`/`retryable` flags, and `retry_after_ms`.
//!
//! Internal failures ([`VenueError::Overflow`], [`VenueError::Upstream`]) render
//! with their cause **redacted** on every surface — the generic
//! [`REDACTED_INTERNAL_MESSAGE`] out to the client, the detail left in
//! `Display`/`source` for the gateway handler to log with request context.
//!
//! Governed by [`docs/01-domain-model.md §11`](../docs/01-domain-model.md) and
//! [`docs/03-protocol-surfaces.md §8`](../docs/03-protocol-surfaces.md) (the
//! authoritative, context-sensitive FIX reject matrix) and §4.2 (the WS error
//! envelope).

use axum::Json;
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::exchange::{MoneyError, RejectKind, SymbolError};
use crate::models::Permission;
use crate::simulation::ReplayError;

/// Schema tag identifying the versioned REST error-envelope wire contract.
///
/// Versioned analogously to the WebSocket [`WS_ERROR_SCHEMA`]; a breaking change
/// to [`ErrorEnvelope`] bumps this tag and its golden together.
pub const REST_ERROR_SCHEMA: &str = "rest-error.v1";

/// Schema tag identifying the versioned WebSocket error-envelope wire contract
/// ([03 §4.2](../docs/03-protocol-surfaces.md)).
pub const WS_ERROR_SCHEMA: &str = "ws-error.v1";

/// The redacted client-facing message for an internal failure. Never carries
/// internal state, a cause chain, or a secret.
pub const REDACTED_INTERNAL_MESSAGE: &str = "internal error";

/// The uniform, client-safe message a cancel/replace reject surfaces on REST when
/// the referenced order cannot be cancelled — collapsing not-found / not-owner /
/// already-gone into ONE indistinguishable reply so the reject is never a
/// cross-account existence/ownership enumeration oracle (BOLA/IDOR, #132/#118).
///
/// `VenueOrderId`s are minted deterministically, so a distinct not-owner vs
/// not-found reply would let an authenticated caller enumerate which ids hold a
/// live resting order owned by another account. The true [`RejectKind`] (especially
/// [`RejectKind::NotOwner`]) stays internal — the gateway journals + traces it as a
/// detective control, never on the wire. The FIX surface uses its own `Text (58)`
/// idiom for the same mask.
pub const CANCEL_REJECT_MASKED_REASON: &str = "order not found or not cancellable";

/// Retry-after hint for a throttled request, in milliseconds — the venue's
/// sliding rate-limit window ([03 §6.1](../docs/03-protocol-surfaces.md)).
///
/// [`VenueError::RateLimited`] carries no per-request budget (it is the
/// documented unit variant), so the seam surfaces this window-derived default;
/// the live `RateLimiter` (#011) attaches the precise `X-RateLimit-Limit` /
/// `X-RateLimit-Reset` values from its own state when it constructs a throttle
/// response.
pub const RATE_LIMIT_RETRY_AFTER_MS: u64 = 60_000;

/// The single typed boundary error every gateway translates through
/// ([01 §11](../docs/01-domain-model.md)).
///
/// A **closed set** so gateways match it exhaustively (no `_` arm) and every
/// surface reports the same failure consistently. Lower-level errors are folded
/// in only where the mapping is unambiguous: the upstream matching error via
/// [`VenueError::Upstream`] (`#[from]`), and the #002 domain errors
/// ([`MoneyError`] / [`SymbolError`]) via `From` impls below. No module returns
/// an opaque `serde_json::Value` error on a public surface.
#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    /// A referenced resource (order, instrument, snapshot, …) does not exist.
    /// HTTP `404`. The carried string is the client's own reference and is safe
    /// to echo.
    #[error("not found: {0}")]
    NotFound(String),
    /// An order (or other client input) failed validation at the boundary
    /// before reaching the sequencer. HTTP `400`. The carried string describes
    /// the validation failure of the client's own input and is safe to echo.
    #[error("invalid order: {0}")]
    InvalidOrder(String),
    /// The request carried no valid credential (missing/expired/invalid token,
    /// or a failed logon). HTTP `401`. On WS this is **terminal** (the socket
    /// closes); on FIX a logon-credential failure is handled by the acceptor's
    /// logon path as `Logout (5)`.
    #[error("unauthorized")]
    Unauthorized,
    /// The authenticated session lacks the permission the operation requires.
    /// HTTP `403`. Carries the **missing** [`Permission`].
    #[error("forbidden: missing permission {0:?}")]
    Forbidden(Permission),
    /// An order targeted an instrument that is not `Active` (halted, settling, or
    /// expired) and so is not accepting orders — the venue's sequenced
    /// instrument-status gate (#47). HTTP `409` (the request conflicts with the
    /// instrument's current lifecycle state). The carried string names the client's
    /// own symbol + the refusing status and is safe to echo. This is the boundary
    /// rendering of the sequenced [`crate::exchange::VenueOutcome::Rejected`] a
    /// halted-instrument order is captured as on the order path; a gateway maps a
    /// halt-reject outcome onto it once the `Receipt`→`VenueOutcome` surfacing seam
    /// lands (until then the sequenced rejection is journaled and replays, and the
    /// order-mutating route reports accepted-and-sequenced).
    #[error("instrument not accepting orders: {0}")]
    InstrumentHalted(String),
    /// The caller exceeded its rate-limit budget. HTTP `429` with
    /// `X-RateLimit-*` / `Retry-After` context.
    #[error("rate limited")]
    RateLimited,
    /// A checked arithmetic operation (cents, notional, a counter) overflowed.
    /// HTTP `500`, cause redacted — never a wrap or a saturate.
    #[error("arithmetic overflow")]
    Overflow,
    /// The per-underlying `underlying_sequence` reached `u64::MAX` and cannot
    /// advance without wrapping, so the underlying was **sealed** — a wrapped
    /// sequence would corrupt gap detection and replay. HTTP `500`, cause
    /// **redacted** on every client surface. This is the actor's checked
    /// sequence-exhaustion seal ([ADR-0006 §2](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
    /// [08 §5](../docs/08-threat-model.md)); it never surfaces internal state.
    #[error("sequence exhausted")]
    SequenceExhausted,
    /// The command journal could not durably record a write, so the command was
    /// rejected — either a confirmed pre-execution write-ahead append failure
    /// (the sequence is reused, the book untouched) or a post-mutation event
    /// append failure that **sealed** the underlying fail-stop. HTTP `500`, cause
    /// **redacted** ([ADR-0006 §3](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
    #[error("journal unavailable")]
    JournalUnavailable,
    /// The venue is **shutting down**: an explicit mid-flight actor shutdown was
    /// triggered (a `CancellationToken`) and this command was **error-drained**
    /// from the actor's mailbox *before* it was journaled or matched. Like a
    /// full-mailbox [`RateLimited`](VenueError::RateLimited), the command was
    /// never accepted onto the sequenced path — it changed **no** book state, was
    /// **never journaled**, and left **no** write-ahead mid-turn, so it is
    /// **replay-neutral**. HTTP `503` Service Unavailable; the command was never
    /// applied, so a retry (against a restarted venue) can succeed. The message
    /// carries no internal state and is safe to echo (#139,
    /// [ADR-0006 §3](../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
    #[error("shutting down")]
    ShuttingDown,
    /// A failure propagated from the upstream matching stack. HTTP `500`, cause
    /// **redacted** on every client surface. `#[error(transparent)]` keeps the
    /// upstream `Display`/`source` chain intact for server-side logging, but
    /// the client renderings never surface it.
    #[error(transparent)]
    Upstream(#[from] option_chain_orderbook::Error),
}

impl From<MoneyError> for VenueError {
    /// Folds the #002 money error into the boundary per its documented mapping
    /// ([`MoneyError`]): an arithmetic overflow is an internal
    /// [`VenueError::Overflow`] (`500`); a negative-cents violation is a client
    /// input failure, [`VenueError::InvalidOrder`] (`400`).
    #[cold]
    #[inline]
    fn from(err: MoneyError) -> Self {
        match err {
            MoneyError::Overflow => VenueError::Overflow,
            MoneyError::NegativeCents(cents) => {
                VenueError::InvalidOrder(format!("negative money value {cents} cents"))
            }
        }
    }
}

impl From<SymbolError> for VenueError {
    /// Folds the #002 symbol error into the boundary. Only
    /// [`SymbolError::InvalidSymbol`] reaches the runtime order boundary (a
    /// client supplied an unparseable `Symbol (55)` / REST symbol), and it maps
    /// to [`VenueError::InvalidOrder`] (`400`) per its documented mapping.
    ///
    /// The expiry variants ([`SymbolError::RelativeExpiryRefused`],
    /// [`SymbolError::NonCanonicalExpiryInstant`],
    /// [`SymbolError::UnresolvableExpiry`]) are **startup config errors** that
    /// are refused before any order is admitted, so they do not reach here at
    /// runtime; this conversion renders them defensively as an invalid-input
    /// `400` rather than crashing the boundary. All variants carry only the
    /// client's own symbol/expiry detail and are safe to echo.
    #[cold]
    #[inline]
    fn from(err: SymbolError) -> Self {
        VenueError::InvalidOrder(err.to_string())
    }
}

impl From<ReplayError> for VenueError {
    /// Folds the #030 replay error into the boundary. A submitted scenario bundle
    /// that is corrupt, schema-refused, version-mismatched, or malformed is a
    /// **client-input** validation failure — [`VenueError::InvalidOrder`] (`400`),
    /// carrying only the client's own bundle detail (underlying + sequence, a
    /// version tag, a decode message) which is safe to echo. A durable-store read
    /// failure while building a replay input is an internal
    /// [`VenueError::JournalUnavailable`] (`500`, redacted).
    #[cold]
    #[inline]
    fn from(err: ReplayError) -> Self {
        match err {
            ReplayError::JournalCorruption { .. }
            | ReplayError::SchemaRefused { .. }
            | ReplayError::VersionMismatch { .. }
            | ReplayError::BundleDecode(_)
            // An oversized / over-ceiling bundle is a client-input validation failure;
            // the message carries only non-secret size/count detail, safe to echo.
            | ReplayError::ResourceLimit { .. }
            // A rejected microstructure config (unprovable fee / out-of-range specs)
            // or an out-of-band journaled order price is a client-input validation
            // failure on the submitted bundle; the detail is non-secret, safe to echo.
            | ReplayError::ConfigRejected { .. }
            | ReplayError::PriceOutOfBand { .. } => VenueError::InvalidOrder(err.to_string()),
            ReplayError::Backend { .. } => VenueError::JournalUnavailable,
        }
    }
}

impl VenueError {
    /// The HTTP status for this error on the REST/WS surface
    /// ([01 §11](../docs/01-domain-model.md)). The match is **exhaustive** — a
    /// new variant will not compile until it is mapped here.
    #[must_use]
    pub fn http_status(&self) -> StatusCode {
        match self {
            VenueError::NotFound(_) => StatusCode::NOT_FOUND,
            VenueError::InvalidOrder(_) => StatusCode::BAD_REQUEST,
            VenueError::Unauthorized => StatusCode::UNAUTHORIZED,
            VenueError::Forbidden(_) => StatusCode::FORBIDDEN,
            VenueError::InstrumentHalted(_) => StatusCode::CONFLICT,
            VenueError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            VenueError::Overflow => StatusCode::INTERNAL_SERVER_ERROR,
            VenueError::SequenceExhausted => StatusCode::INTERNAL_SERVER_ERROR,
            VenueError::JournalUnavailable => StatusCode::INTERNAL_SERVER_ERROR,
            VenueError::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
            VenueError::Upstream(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The stable, machine-readable error code — one shared vocabulary across
    /// the REST envelope and the WS [`WsErrorCode`]. `Overflow` / `Upstream`
    /// both render as the generic `"internal"` so no internal taxonomy leaks.
    #[must_use]
    pub fn machine_code(&self) -> &'static str {
        match self {
            VenueError::NotFound(_) => "not_found",
            VenueError::InvalidOrder(_) => "invalid_order",
            VenueError::Unauthorized => "unauthorized",
            VenueError::Forbidden(_) => "forbidden",
            VenueError::InstrumentHalted(_) => "instrument_halted",
            VenueError::RateLimited => "throttled",
            VenueError::Overflow => "internal",
            VenueError::SequenceExhausted => "internal",
            VenueError::JournalUnavailable => "internal",
            VenueError::ShuttingDown => "unavailable",
            VenueError::Upstream(_) => "internal",
        }
    }

    /// The client-facing, **redacted** message. Safe variants echo their own
    /// (client-supplied) detail; `Overflow` / `Upstream` collapse to the
    /// generic [`REDACTED_INTERNAL_MESSAGE`] so no internal state or cause chain
    /// reaches the client. This is distinct from [`std::fmt::Display`], which is
    /// the server-side/log form and may carry the internal cause.
    #[must_use]
    pub fn redacted_message(&self) -> String {
        match self {
            VenueError::NotFound(detail) => format!("not found: {detail}"),
            VenueError::InvalidOrder(detail) => format!("invalid order: {detail}"),
            VenueError::Unauthorized => "unauthorized".to_string(),
            VenueError::Forbidden(permission) => format!("missing permission {permission:?}"),
            VenueError::InstrumentHalted(detail) => {
                format!("instrument not accepting orders: {detail}")
            }
            VenueError::RateLimited => "rate limited".to_string(),
            VenueError::Overflow => REDACTED_INTERNAL_MESSAGE.to_string(),
            VenueError::SequenceExhausted => REDACTED_INTERNAL_MESSAGE.to_string(),
            VenueError::JournalUnavailable => REDACTED_INTERNAL_MESSAGE.to_string(),
            // Not an internal-cause failure: the venue is shutting down and the
            // command was never accepted. The message carries no internal state,
            // so it is echoed rather than collapsed to the generic redaction.
            VenueError::ShuttingDown => "shutting down".to_string(),
            VenueError::Upstream(_) => REDACTED_INTERNAL_MESSAGE.to_string(),
        }
    }

    /// The typed REST error-envelope body for this error (never a
    /// `serde_json::Value`).
    #[must_use]
    pub fn error_envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            schema: REST_ERROR_SCHEMA.to_string(),
            code: self.machine_code().to_string(),
            message: self.redacted_message(),
        }
    }

    /// The CLIENT-FACING error for a cancel/replace whose sequenced outcome was a
    /// captured [`VenueOutcome::Rejected`](crate::exchange::VenueOutcome::Rejected),
    /// keyed on the TYPED [`RejectKind`] — never a string-match of the human reason
    /// (#132).
    ///
    /// The authorization-sensitive existence kinds ([`RejectKind::NotOwner`] /
    /// [`RejectKind::NotFound`] / [`RejectKind::NotResting`]) collapse to ONE
    /// indistinguishable not-found reject carrying `masked_reason`, so an
    /// authenticated caller cannot tell a live order owned by another account from a
    /// nonexistent id (the BOLA/IDOR mask). `masked_reason` is the surface's own
    /// client-safe text (REST uses [`CANCEL_REJECT_MASKED_REASON`]; FIX uses its
    /// `Text (58)` idiom) — never the internal `reason`.
    ///
    /// The remaining kinds are not reachable as a *top-level* cancel/replace reject
    /// from a well-formed gateway request (the gateway validates the symbol before
    /// submit and never gates a cancel on instrument status), so they render
    /// defensively as their natural error and [`RejectKind::Internal`]'s cause is
    /// redacted.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::VenueError;
    /// use fauxchange::exchange::RejectKind;
    /// // not-owner and not-found are byte-identical at the client boundary.
    /// let owner = VenueError::masked_cancel_reject(RejectKind::NotOwner, "unknown order");
    /// let found = VenueError::masked_cancel_reject(RejectKind::NotFound, "unknown order");
    /// assert_eq!(owner.http_status(), found.http_status());
    /// assert_eq!(owner.redacted_message(), found.redacted_message());
    /// ```
    #[must_use]
    pub fn masked_cancel_reject(kind: RejectKind, masked_reason: &str) -> VenueError {
        match kind {
            RejectKind::NotFound | RejectKind::NotOwner | RejectKind::NotResting => {
                VenueError::NotFound(masked_reason.to_string())
            }
            RejectKind::InstrumentNotActive => {
                VenueError::InstrumentHalted(masked_reason.to_string())
            }
            RejectKind::InvalidOrder | RejectKind::NotFillable => {
                VenueError::InvalidOrder(masked_reason.to_string())
            }
            RejectKind::Internal => VenueError::JournalUnavailable,
        }
    }

    /// The FIX reject for this error **in the given inbound message context**
    /// ([03 §8](../docs/03-protocol-surfaces.md)).
    ///
    /// The [`FixRejectContext`] selects the reject **message** (which is why a
    /// `Forbidden` on a `NewOrderSingle` becomes `ExecutionReport (8) Rejected`
    /// but on a cancel becomes `OrderCancelReject (9)`); the error selects the
    /// reason **category** placed in that message's reason field and the
    /// redacted `Text (58)`. The message-derived tags the reject also needs —
    /// `RefTagID (371)` on a session reject, `CxlRejResponseTo (434)` on a
    /// cancel reject, `RefMsgType (372)` on a business reject — come from the
    /// inbound message and are filled by the v0.4 acceptor, not this seam.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::{FixRejectContext, FixRejectReason, VenueError};
    /// let reject = VenueError::RateLimited.fix_reject(FixRejectContext::NewOrder);
    /// assert_eq!(reject.kind.msg_type(), "8"); // ExecutionReport, ExecType=Rejected
    /// assert_eq!(reject.kind.reason_field_tag(), 103); // OrdRejReason
    /// assert_eq!(reject.reason, FixRejectReason::Throttle);
    /// ```
    #[must_use]
    pub fn fix_reject(&self, context: FixRejectContext) -> FixReject {
        FixReject {
            kind: FixRejectKind::for_context(context),
            reason: self.fix_reason(),
            text: Some(self.redacted_message()),
        }
    }

    /// The semantic FIX reason category for this error, placed in the reason
    /// field of whichever reject message the context selects.
    #[must_use]
    fn fix_reason(&self) -> FixRejectReason {
        match self {
            VenueError::NotFound(_) => FixRejectReason::NotFound,
            VenueError::InvalidOrder(_) => FixRejectReason::Invalid,
            VenueError::Unauthorized => FixRejectReason::Authorization,
            VenueError::Forbidden(_) => FixRejectReason::Authorization,
            VenueError::InstrumentHalted(_) => FixRejectReason::Invalid,
            VenueError::RateLimited => FixRejectReason::Throttle,
            VenueError::Overflow => FixRejectReason::Internal,
            VenueError::SequenceExhausted => FixRejectReason::Internal,
            VenueError::JournalUnavailable => FixRejectReason::Internal,
            // A shutting-down venue is a transient "resend later" condition, not an
            // internal failure to redact — the closest existing FIX category is the
            // retryable throttle bucket (no new FIX reason vocabulary added).
            VenueError::ShuttingDown => FixRejectReason::Throttle,
            VenueError::Upstream(_) => FixRejectReason::Internal,
        }
    }

    /// The versioned WebSocket error envelope for this error
    /// ([03 §4.2](../docs/03-protocol-surfaces.md)). `request_id` correlates the
    /// envelope to the client action that caused it, when one is present.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::{VenueError, WsErrorCategory, WsErrorCode};
    /// let env = VenueError::Unauthorized.ws_error(None);
    /// assert_eq!(env.code, WsErrorCode::Unauthorized);
    /// assert!(env.terminal); // an auth failure closes the socket
    /// ```
    #[must_use]
    pub fn ws_error(&self, request_id: Option<String>) -> WsError {
        let (code, category) = self.ws_code_category();
        // `terminal`: only an authentication failure closes the socket; every
        // other error is a command error and leaves the connection open.
        // `retryable` / `retry_after_ms`: only a throttle can succeed on retry.
        let (retryable, retry_after_ms, terminal) = match self {
            VenueError::Unauthorized => (false, None, true),
            VenueError::RateLimited => (true, Some(RATE_LIMIT_RETRY_AFTER_MS), false),
            // A mid-flight shutdown never accepted the command: the same action can
            // succeed on retry (against a restarted venue), so it is retryable but
            // carries no fixed backoff window; it is a command error, not terminal.
            VenueError::ShuttingDown => (true, None, false),
            VenueError::NotFound(_)
            | VenueError::InvalidOrder(_)
            | VenueError::Forbidden(_)
            | VenueError::InstrumentHalted(_)
            | VenueError::Overflow
            | VenueError::SequenceExhausted
            | VenueError::JournalUnavailable
            | VenueError::Upstream(_) => (false, None, false),
        };
        WsError {
            schema: WS_ERROR_SCHEMA.to_string(),
            code,
            category,
            message: self.redacted_message(),
            request_id,
            retryable,
            retry_after_ms,
            terminal,
        }
    }

    /// The stable `(code, category)` pair for the WS envelope. Total over every
    /// variant.
    #[must_use]
    pub fn ws_code_category(&self) -> (WsErrorCode, WsErrorCategory) {
        match self {
            VenueError::NotFound(_) => (WsErrorCode::NotFound, WsErrorCategory::NotFound),
            VenueError::InvalidOrder(_) => (WsErrorCode::InvalidOrder, WsErrorCategory::Validation),
            VenueError::Unauthorized => (WsErrorCode::Unauthorized, WsErrorCategory::Authorization),
            VenueError::Forbidden(_) => (WsErrorCode::Forbidden, WsErrorCategory::Authorization),
            VenueError::InstrumentHalted(_) => {
                (WsErrorCode::InstrumentHalted, WsErrorCategory::Validation)
            }
            VenueError::RateLimited => (WsErrorCode::Throttled, WsErrorCategory::Throttle),
            VenueError::Overflow => (WsErrorCode::Internal, WsErrorCategory::Internal),
            VenueError::SequenceExhausted => (WsErrorCode::Internal, WsErrorCategory::Internal),
            VenueError::JournalUnavailable => (WsErrorCode::Internal, WsErrorCategory::Internal),
            VenueError::ShuttingDown => (WsErrorCode::Unavailable, WsErrorCategory::Unavailable),
            VenueError::Upstream(_) => (WsErrorCode::Internal, WsErrorCategory::Internal),
        }
    }
}

impl IntoResponse for VenueError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        let body = Json(self.error_envelope());
        match self {
            // A throttle carries rate-limit context headers. `Retry-After` is
            // the window-derived default; `X-RateLimit-Remaining: 0` is exact
            // (a throttled request has no budget left). The precise
            // `X-RateLimit-Limit` / `X-RateLimit-Reset` are attached by the
            // #011 RateLimiter from its live window state.
            VenueError::RateLimited => {
                let mut response = (status, body).into_response();
                let headers = response.headers_mut();
                headers.insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
                headers.insert(
                    HeaderName::from_static("x-ratelimit-remaining"),
                    HeaderValue::from_static("0"),
                );
                response
            }
            VenueError::NotFound(_)
            | VenueError::InvalidOrder(_)
            | VenueError::Unauthorized
            | VenueError::Forbidden(_)
            | VenueError::InstrumentHalted(_)
            | VenueError::Overflow
            | VenueError::SequenceExhausted
            | VenueError::JournalUnavailable
            | VenueError::ShuttingDown
            | VenueError::Upstream(_) => (status, body).into_response(),
        }
    }
}

/// The typed REST error-envelope body ([01 §11](../docs/01-domain-model.md)).
///
/// Serialised as the JSON response body by [`VenueError::into_response`]; a
/// concrete struct, never a `serde_json::Value`. `code` shares its vocabulary
/// with the WS [`WsErrorCode`]; `message` is the redacted, client-safe form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ErrorEnvelope {
    /// Schema tag pinning this wire shape — always [`REST_ERROR_SCHEMA`].
    pub schema: String,
    /// Stable machine-readable error code ([`VenueError::machine_code`]).
    pub code: String,
    /// Human-readable, redacted message — never internal state or a secret.
    pub message: String,
}

/// The class of the inbound FIX message a failure occurred while processing.
///
/// This is the **primary axis** of the FIX reject mapping
/// ([03 §8](../docs/03-protocol-surfaces.md)): the context — not the
/// [`VenueError`] — selects which reject message the venue emits. The v0.4
/// acceptor supplies the context from the message it is handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FixRejectContext {
    /// A `NewOrderSingle (35=D)` — rejects render as `ExecutionReport (8)`
    /// with `ExecType=Rejected` and `OrdRejReason (103)`.
    NewOrder,
    /// An `OrderCancelRequest (35=F)` or `OrderCancelReplaceRequest (35=G)` —
    /// rejects render as `OrderCancelReject (9)` with `CxlRejReason (102)`.
    CancelReplace,
    /// A `MarketDataRequest (35=V)` — rejects render as
    /// `MarketDataRequestReject (Y)` with `MDReqRejReason (281)`.
    MarketData,
    /// Any other well-formed application message the venue understands but
    /// cannot business-process — renders as `BusinessMessageReject (j)` with
    /// `BusinessRejectReason (380)`.
    OtherApplication,
    /// A session-protocol failure (bad checksum, unknown `MsgType`, session
    /// sequence) — renders as a session `Reject (3)` with
    /// `SessionRejectReason (373)`. Genuine malformed-frame rejects are produced
    /// by the codec/session layer directly; this context exists so a
    /// [`VenueError`] that must surface at session level maps correctly.
    Session,
}

/// The FIX reject **message** the venue emits, selected purely by
/// [`FixRejectContext`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FixRejectKind {
    /// `ExecutionReport (35=8)` with `ExecType (150)=8` (Rejected); reason in
    /// `OrdRejReason (103)`.
    ExecutionReportRejected,
    /// `OrderCancelReject (35=9)`; reason in `CxlRejReason (102)`.
    OrderCancelReject,
    /// `MarketDataRequestReject (35=Y)`; reason in `MDReqRejReason (281)`.
    MarketDataRequestReject,
    /// `BusinessMessageReject (35=j)`; reason in `BusinessRejectReason (380)`.
    BusinessMessageReject,
    /// Session-level `Reject (35=3)`; reason in `SessionRejectReason (373)`.
    SessionReject,
}

impl FixRejectKind {
    /// Selects the reject message for an inbound message context.
    #[must_use]
    fn for_context(context: FixRejectContext) -> Self {
        match context {
            FixRejectContext::NewOrder => FixRejectKind::ExecutionReportRejected,
            FixRejectContext::CancelReplace => FixRejectKind::OrderCancelReject,
            FixRejectContext::MarketData => FixRejectKind::MarketDataRequestReject,
            FixRejectContext::OtherApplication => FixRejectKind::BusinessMessageReject,
            FixRejectContext::Session => FixRejectKind::SessionReject,
        }
    }

    /// The FIX `MsgType (35)` value this reject is carried on
    /// (`8`/`9`/`Y`/`j`/`3`).
    #[must_use]
    pub fn msg_type(self) -> &'static str {
        match self {
            FixRejectKind::ExecutionReportRejected => "8",
            FixRejectKind::OrderCancelReject => "9",
            FixRejectKind::MarketDataRequestReject => "Y",
            FixRejectKind::BusinessMessageReject => "j",
            FixRejectKind::SessionReject => "3",
        }
    }

    /// The FIX tag number of the reason field this reject uses
    /// (`103`/`102`/`281`/`380`/`373`).
    #[must_use]
    pub fn reason_field_tag(self) -> u16 {
        match self {
            FixRejectKind::ExecutionReportRejected => 103,
            FixRejectKind::OrderCancelReject => 102,
            FixRejectKind::MarketDataRequestReject => 281,
            FixRejectKind::BusinessMessageReject => 380,
            FixRejectKind::SessionReject => 373,
        }
    }
}

/// The semantic reason category placed in the chosen reject's reason field.
///
/// The seam carries the **category** ([03 §8](../docs/03-protocol-surfaces.md):
/// "a `Forbidden` … resolves to `ExecutionReport (8) Rejected` with an
/// authorization `OrdRejReason`"); the concrete numeric enum value for the
/// reason field is FIX **wire encoding** rendered by the v0.4 encoder against
/// [`specs/fix-dialect.md`](../docs/specs/fix-dialect.md) (#039), which is out
/// of scope here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FixRejectReason {
    /// A business-validation failure (from [`VenueError::InvalidOrder`]).
    Invalid,
    /// A referenced resource does not exist (from [`VenueError::NotFound`]).
    NotFound,
    /// An authorization failure (from [`VenueError::Forbidden`] /
    /// [`VenueError::Unauthorized`]).
    Authorization,
    /// A rate-limit throttle (from [`VenueError::RateLimited`]).
    Throttle,
    /// An internal failure, cause redacted (from [`VenueError::Overflow`] /
    /// [`VenueError::Upstream`]).
    Internal,
}

/// A resolved FIX reject: which message ([`FixRejectKind`]), the reason category
/// in its reason field ([`FixRejectReason`]), and a **redacted** `Text (58)`.
///
/// This is the seam output the v0.4 acceptor turns into a wire frame; it carries
/// no wire bytes and no message-derived tags (those come from the inbound
/// message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixReject {
    /// The reject message, selected by the inbound message context.
    pub kind: FixRejectKind,
    /// The reason category placed in the message's reason field.
    pub reason: FixRejectReason,
    /// The redacted `Text (58)` — safe to echo; `Internal` carries only the
    /// generic [`REDACTED_INTERNAL_MESSAGE`], never a cause or secret.
    pub text: Option<String>,
}

/// The stable machine code on the WS error envelope
/// ([03 §4.2](../docs/03-protocol-surfaces.md)). Serialised `snake_case`.
///
/// The full wire vocabulary — including `BadRequest` (a decode failure) and
/// `Busy` (a full mailbox), which are produced by the WS transport (#014)
/// rather than a [`VenueError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum WsErrorCode {
    /// A malformed client frame or action (WS transport).
    BadRequest,
    /// Order/input validation failed ([`VenueError::InvalidOrder`]).
    InvalidOrder,
    /// No valid credential ([`VenueError::Unauthorized`]) — terminal.
    Unauthorized,
    /// The session lacks the required permission ([`VenueError::Forbidden`]).
    Forbidden,
    /// An order targeted an instrument not accepting orders
    /// ([`VenueError::InstrumentHalted`]).
    InstrumentHalted,
    /// The caller is rate-limited ([`VenueError::RateLimited`]).
    Throttled,
    /// A referenced resource does not exist ([`VenueError::NotFound`]).
    NotFound,
    /// The server's mailbox is full (WS transport).
    Busy,
    /// The venue is shutting down and never accepted the command
    /// ([`VenueError::ShuttingDown`]) — a transient, retryable unavailability
    /// distinct from a full-mailbox `Busy` and from an internal failure.
    Unavailable,
    /// An internal failure, cause redacted ([`VenueError::Overflow`] /
    /// [`VenueError::Upstream`]).
    Internal,
}

/// The category on the WS error envelope
/// ([03 §4.2](../docs/03-protocol-surfaces.md)). Serialised `snake_case`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum WsErrorCategory {
    /// A frame/action could not be decoded (WS transport).
    Decode,
    /// Input validation failed.
    Validation,
    /// An authentication/authorization failure.
    Authorization,
    /// A rate-limit throttle.
    Throttle,
    /// A referenced resource does not exist.
    NotFound,
    /// The server is momentarily too busy (full mailbox).
    Busy,
    /// The venue is shutting down and is temporarily unavailable (retryable).
    Unavailable,
    /// An internal failure, cause redacted.
    Internal,
}

/// The versioned typed WebSocket error envelope — the `data` payload of a
/// `{ "type": "error", "data": … }` `WsMessage`
/// ([03 §4.2](../docs/03-protocol-surfaces.md)).
///
/// The outer `{ type, data }` framing is the WS transport's `WsMessage::Error`
/// variant (#014); this struct is the versioned envelope it wraps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WsError {
    /// Schema tag pinning this wire shape — always [`WS_ERROR_SCHEMA`].
    pub schema: String,
    /// Stable machine-readable error code.
    pub code: WsErrorCode,
    /// The error category.
    pub category: WsErrorCategory,
    /// Human-readable, redacted message — never internal state or a secret.
    pub message: String,
    /// Correlates to the client action that caused the error, when present.
    pub request_id: Option<String>,
    /// Whether a retry of the same action can succeed.
    pub retryable: bool,
    /// Backoff hint in milliseconds; set only on a throttle.
    pub retry_after_ms: Option<u64>,
    /// Whether the connection will close (`true` only for an auth failure); a
    /// command error is non-terminal and leaves the connection open.
    pub terminal: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a representative `Upstream` error whose cause carries a
    /// distinctive marker, so redaction can be asserted against it.
    fn upstream_with_marker(marker: &str) -> VenueError {
        VenueError::from(option_chain_orderbook::Error::UnderlyingNotFound {
            underlying: marker.to_string(),
        })
    }

    /// Every `VenueError` variant, for exhaustive table tests. Upstream is
    /// constructed from a real upstream variant.
    fn all_variants() -> Vec<VenueError> {
        vec![
            VenueError::NotFound("order 42".to_string()),
            VenueError::InvalidOrder("quantity must be positive".to_string()),
            VenueError::Unauthorized,
            VenueError::Forbidden(Permission::Trade),
            VenueError::InstrumentHalted("BTC-20240329-50000-C is Halted".to_string()),
            VenueError::RateLimited,
            VenueError::Overflow,
            VenueError::SequenceExhausted,
            VenueError::JournalUnavailable,
            VenueError::ShuttingDown,
            upstream_with_marker("marker"),
        ]
    }

    // ---- HTTP status table -------------------------------------------------

    #[test]
    fn test_venue_error_not_found_maps_to_404() {
        assert_eq!(
            VenueError::NotFound("x".to_string()).http_status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn test_venue_error_invalid_order_maps_to_400() {
        assert_eq!(
            VenueError::InvalidOrder("x".to_string()).http_status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn test_venue_error_unauthorized_maps_to_401() {
        assert_eq!(
            VenueError::Unauthorized.http_status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn test_venue_error_forbidden_maps_to_403() {
        assert_eq!(
            VenueError::Forbidden(Permission::Trade).http_status(),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn test_venue_error_rate_limited_maps_to_429() {
        assert_eq!(
            VenueError::RateLimited.http_status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn test_venue_error_overflow_maps_to_500() {
        assert_eq!(
            VenueError::Overflow.http_status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn test_venue_error_upstream_maps_to_500() {
        assert_eq!(
            upstream_with_marker("x").http_status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn test_venue_error_sequence_exhausted_is_redacted_internal_500() {
        // The actor's checked sequence-exhaustion seal surfaces as a redacted
        // internal failure — never leaking the operational cause to a client.
        let err = VenueError::SequenceExhausted;
        assert_eq!(err.http_status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.machine_code(), "internal");
        assert_eq!(err.redacted_message(), REDACTED_INTERNAL_MESSAGE);
        // Display carries the distinct cause for server-side logs.
        assert_eq!(err.to_string(), "sequence exhausted");
    }

    #[test]
    fn test_venue_error_instrument_halted_maps_to_409_and_echoes_symbol() {
        // The sequenced instrument-status gate's boundary rendering (#47): a halt
        // reject is a 409 (a conflict with the instrument's lifecycle state), with a
        // stable machine code and a client-safe message that echoes the symbol/status.
        let err = VenueError::InstrumentHalted("BTC-20240329-50000-C is Halted".to_string());
        assert_eq!(err.http_status(), StatusCode::CONFLICT);
        assert_eq!(err.machine_code(), "instrument_halted");
        assert_eq!(
            err.redacted_message(),
            "instrument not accepting orders: BTC-20240329-50000-C is Halted"
        );
        // FIX: a business-validation reject on whichever context is processing it.
        assert_eq!(
            err.fix_reject(FixRejectContext::NewOrder).reason,
            FixRejectReason::Invalid
        );
        // WS: a non-terminal, non-retryable validation error.
        let env = err.ws_error(None);
        assert_eq!(env.code, WsErrorCode::InstrumentHalted);
        assert_eq!(env.category, WsErrorCategory::Validation);
        assert!(!env.terminal);
        assert!(!env.retryable);
    }

    #[test]
    fn test_venue_error_journal_unavailable_is_redacted_internal_500() {
        let err = VenueError::JournalUnavailable;
        assert_eq!(err.http_status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.machine_code(), "internal");
        assert_eq!(err.redacted_message(), REDACTED_INTERNAL_MESSAGE);
        assert_eq!(err.to_string(), "journal unavailable");
    }

    #[test]
    fn test_venue_error_shutting_down_maps_to_503_and_is_retryable() {
        // The #139 explicit mid-flight actor shutdown: a command error-drained from
        // the mailbox before it was journaled/matched surfaces as a 503 Service
        // Unavailable, a distinct machine code (`unavailable`, NOT the internal
        // taxonomy), and a client-safe echoed message — never a redacted internal.
        let err = VenueError::ShuttingDown;
        assert_eq!(err.http_status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(err.machine_code(), "unavailable");
        assert_eq!(err.redacted_message(), "shutting down");
        assert_eq!(err.to_string(), "shutting down");
        // FIX: the retryable "resend later" throttle bucket (no new FIX vocabulary).
        assert_eq!(
            err.fix_reject(FixRejectContext::NewOrder).reason,
            FixRejectReason::Throttle
        );
        // WS: a retryable, non-terminal unavailability with no fixed backoff window.
        let env = err.ws_error(Some("req-9".to_string()));
        assert_eq!(env.code, WsErrorCode::Unavailable);
        assert_eq!(env.category, WsErrorCategory::Unavailable);
        assert!(env.retryable);
        assert!(!env.terminal);
        assert_eq!(env.retry_after_ms, None);
        assert_eq!(env.request_id, Some("req-9".to_string()));
        // IntoResponse carries the 503 without rate-limit headers.
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().get(header::RETRY_AFTER).is_none());
    }

    #[test]
    fn test_venue_error_sequencing_failures_are_non_retryable_non_terminal_on_ws() {
        for err in [
            VenueError::SequenceExhausted,
            VenueError::JournalUnavailable,
        ] {
            let env = err.ws_error(None);
            assert_eq!(env.code, WsErrorCode::Internal);
            assert!(!env.retryable);
            assert!(!env.terminal);
            assert_eq!(env.retry_after_ms, None);
        }
    }

    // ---- machine code ------------------------------------------------------

    #[test]
    fn test_venue_error_machine_code_table_is_stable() {
        assert_eq!(
            VenueError::NotFound("x".to_string()).machine_code(),
            "not_found"
        );
        assert_eq!(
            VenueError::InvalidOrder("x".to_string()).machine_code(),
            "invalid_order"
        );
        assert_eq!(VenueError::Unauthorized.machine_code(), "unauthorized");
        assert_eq!(
            VenueError::Forbidden(Permission::Trade).machine_code(),
            "forbidden"
        );
        assert_eq!(VenueError::RateLimited.machine_code(), "throttled");
        assert_eq!(VenueError::Overflow.machine_code(), "internal");
        assert_eq!(upstream_with_marker("x").machine_code(), "internal");
    }

    // ---- redaction ---------------------------------------------------------

    #[test]
    fn test_venue_error_overflow_message_is_redacted() {
        assert_eq!(
            VenueError::Overflow.redacted_message(),
            REDACTED_INTERNAL_MESSAGE
        );
    }

    #[test]
    fn test_venue_error_upstream_redacted_message_hides_cause() {
        let err = upstream_with_marker("SECRET-INTERNAL-DETAIL");
        let message = err.redacted_message();
        assert_eq!(message, REDACTED_INTERNAL_MESSAGE);
        assert!(!message.contains("SECRET-INTERNAL-DETAIL"));
    }

    #[test]
    fn test_venue_error_upstream_envelope_hides_cause() {
        let err = upstream_with_marker("SECRET-INTERNAL-DETAIL");
        let envelope = err.error_envelope();
        assert_eq!(envelope.code, "internal");
        assert_eq!(envelope.message, REDACTED_INTERNAL_MESSAGE);
        assert!(!envelope.message.contains("SECRET-INTERNAL-DETAIL"));
    }

    #[test]
    fn test_venue_error_upstream_fix_text_hides_cause() {
        let err = upstream_with_marker("SECRET-INTERNAL-DETAIL");
        let reject = err.fix_reject(FixRejectContext::NewOrder);
        match reject.text {
            Some(text) => {
                assert_eq!(text, REDACTED_INTERNAL_MESSAGE);
                assert!(!text.contains("SECRET-INTERNAL-DETAIL"));
            }
            None => panic!("expected a redacted Text(58), got none"),
        }
    }

    #[test]
    fn test_venue_error_display_keeps_cause_for_logging() {
        // Display is the server-side/log form and DOES carry the cause — the
        // redaction lives in `redacted_message`, not `Display`.
        let err = upstream_with_marker("SECRET-INTERNAL-DETAIL");
        assert!(err.to_string().contains("SECRET-INTERNAL-DETAIL"));
    }

    #[test]
    fn test_venue_error_forbidden_message_names_missing_permission() {
        assert_eq!(
            VenueError::Forbidden(Permission::Trade).redacted_message(),
            "missing permission Trade"
        );
    }

    // ---- IntoResponse ------------------------------------------------------

    #[test]
    fn test_into_response_forbidden_has_403_status() {
        let response = VenueError::Forbidden(Permission::Trade).into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_into_response_rate_limited_sets_context_headers() {
        let response = VenueError::RateLimited.into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let headers = response.headers();
        match headers.get(header::RETRY_AFTER) {
            Some(value) => assert_eq!(value, "60"),
            None => panic!("expected a Retry-After header"),
        }
        match headers.get("x-ratelimit-remaining") {
            Some(value) => assert_eq!(value, "0"),
            None => panic!("expected an X-RateLimit-Remaining header"),
        }
    }

    #[test]
    fn test_into_response_not_found_has_no_rate_limit_headers() {
        let response = VenueError::NotFound("x".to_string()).into_response();
        assert!(response.headers().get(header::RETRY_AFTER).is_none());
        assert!(response.headers().get("x-ratelimit-remaining").is_none());
    }

    // ---- FIX reject matrix -------------------------------------------------

    /// The reject **message** is a pure function of the context, independent of
    /// the error — the core acceptance criterion of the seam.
    #[test]
    fn test_fix_reject_kind_is_determined_by_context_not_error() {
        let expected = [
            (
                FixRejectContext::NewOrder,
                FixRejectKind::ExecutionReportRejected,
            ),
            (
                FixRejectContext::CancelReplace,
                FixRejectKind::OrderCancelReject,
            ),
            (
                FixRejectContext::MarketData,
                FixRejectKind::MarketDataRequestReject,
            ),
            (
                FixRejectContext::OtherApplication,
                FixRejectKind::BusinessMessageReject,
            ),
            (FixRejectContext::Session, FixRejectKind::SessionReject),
        ];
        for (context, kind) in expected {
            for err in all_variants() {
                assert_eq!(
                    err.fix_reject(context).kind,
                    kind,
                    "context {context:?} must always select {kind:?}"
                );
            }
        }
    }

    /// The reason **category** is a pure function of the error, independent of
    /// the context.
    #[test]
    fn test_fix_reject_reason_is_determined_by_error_not_context() {
        let expected = [
            (
                VenueError::NotFound("x".to_string()),
                FixRejectReason::NotFound,
            ),
            (
                VenueError::InvalidOrder("x".to_string()),
                FixRejectReason::Invalid,
            ),
            (VenueError::Unauthorized, FixRejectReason::Authorization),
            (
                VenueError::Forbidden(Permission::Trade),
                FixRejectReason::Authorization,
            ),
            (VenueError::RateLimited, FixRejectReason::Throttle),
            (VenueError::Overflow, FixRejectReason::Internal),
            (upstream_with_marker("x"), FixRejectReason::Internal),
        ];
        let contexts = [
            FixRejectContext::NewOrder,
            FixRejectContext::CancelReplace,
            FixRejectContext::MarketData,
            FixRejectContext::OtherApplication,
            FixRejectContext::Session,
        ];
        for (err, reason) in expected {
            for context in contexts {
                assert_eq!(err.fix_reject(context).reason, reason);
            }
        }
    }

    #[test]
    fn test_fix_reject_forbidden_new_order_is_execution_report_authorization() {
        let reject =
            VenueError::Forbidden(Permission::Trade).fix_reject(FixRejectContext::NewOrder);
        assert_eq!(reject.kind, FixRejectKind::ExecutionReportRejected);
        assert_eq!(reject.kind.msg_type(), "8");
        assert_eq!(reject.kind.reason_field_tag(), 103);
        assert_eq!(reject.reason, FixRejectReason::Authorization);
    }

    #[test]
    fn test_fix_reject_forbidden_cancel_is_order_cancel_reject() {
        let reject =
            VenueError::Forbidden(Permission::Trade).fix_reject(FixRejectContext::CancelReplace);
        assert_eq!(reject.kind, FixRejectKind::OrderCancelReject);
        assert_eq!(reject.kind.msg_type(), "9");
        assert_eq!(reject.kind.reason_field_tag(), 102);
        assert_eq!(reject.reason, FixRejectReason::Authorization);
    }

    // ---- masked cancel/replace reject (BOLA/IDOR mask, #132) ---------------

    /// The three authorization-sensitive existence kinds render **byte-identically**
    /// to the client across HTTP, the REST envelope, the FIX reject, and the WS
    /// envelope — so a not-owner reject is indistinguishable from a not-found one.
    #[test]
    fn test_masked_cancel_reject_existence_kinds_are_byte_identical() {
        let masked_reason = CANCEL_REJECT_MASKED_REASON;
        let rejects: Vec<VenueError> = [
            RejectKind::NotOwner,
            RejectKind::NotFound,
            RejectKind::NotResting,
        ]
        .into_iter()
        .map(|kind| VenueError::masked_cancel_reject(kind, masked_reason))
        .collect();
        // Every rendering is identical to the not-found baseline.
        let baseline = &rejects[0];
        for reject in &rejects {
            assert_eq!(reject.http_status(), baseline.http_status());
            assert_eq!(reject.machine_code(), baseline.machine_code());
            assert_eq!(reject.redacted_message(), baseline.redacted_message());
            assert_eq!(reject.error_envelope(), baseline.error_envelope());
            assert_eq!(
                reject.fix_reject(FixRejectContext::CancelReplace),
                baseline.fix_reject(FixRejectContext::CancelReplace)
            );
            assert_eq!(reject.ws_error(None), baseline.ws_error(None));
        }
        // The baseline is a not-found: FIX CxlRejReason category is NotFound (→ 102=1).
        assert_eq!(baseline.http_status(), StatusCode::NOT_FOUND);
        assert_eq!(
            baseline.fix_reject(FixRejectContext::CancelReplace).reason,
            FixRejectReason::NotFound
        );
    }

    /// The mask keys on the TYPED [`RejectKind`], not the human reason string — the
    /// masked wire text is exactly the supplied masked reason, never the executor's
    /// internal reason, so refactoring that string cannot change the mask.
    #[test]
    fn test_masked_cancel_reject_uses_masked_reason_not_internal_reason() {
        let masked = VenueError::masked_cancel_reject(RejectKind::NotOwner, "unknown order");
        match masked {
            VenueError::NotFound(text) => {
                assert_eq!(text, "unknown order");
                assert!(!text.contains("does not own"));
            }
            other => panic!("a masked not-owner cancel reject must be a NotFound, got {other:?}"),
        }
    }

    #[test]
    fn test_fix_reject_rate_limited_market_data_is_md_request_reject_throttle() {
        let reject = VenueError::RateLimited.fix_reject(FixRejectContext::MarketData);
        assert_eq!(reject.kind, FixRejectKind::MarketDataRequestReject);
        assert_eq!(reject.kind.msg_type(), "Y");
        assert_eq!(reject.kind.reason_field_tag(), 281);
        assert_eq!(reject.reason, FixRejectReason::Throttle);
    }

    #[test]
    fn test_fix_reject_other_application_is_business_message_reject() {
        let reject = VenueError::InvalidOrder("x".to_string())
            .fix_reject(FixRejectContext::OtherApplication);
        assert_eq!(reject.kind, FixRejectKind::BusinessMessageReject);
        assert_eq!(reject.kind.msg_type(), "j");
        assert_eq!(reject.kind.reason_field_tag(), 380);
    }

    #[test]
    fn test_fix_reject_session_context_is_session_reject() {
        let reject = VenueError::NotFound("x".to_string()).fix_reject(FixRejectContext::Session);
        assert_eq!(reject.kind, FixRejectKind::SessionReject);
        assert_eq!(reject.kind.msg_type(), "3");
        assert_eq!(reject.kind.reason_field_tag(), 373);
    }

    #[test]
    fn test_fix_reject_safe_variant_text_echoes_detail() {
        let reject = VenueError::InvalidOrder("quantity must be positive".to_string())
            .fix_reject(FixRejectContext::NewOrder);
        assert_eq!(
            reject.text,
            Some("invalid order: quantity must be positive".to_string())
        );
    }

    // ---- WS envelope mapping ----------------------------------------------

    #[test]
    fn test_ws_error_code_category_table_is_stable() {
        let expected = [
            (
                VenueError::NotFound("x".to_string()),
                WsErrorCode::NotFound,
                WsErrorCategory::NotFound,
            ),
            (
                VenueError::InvalidOrder("x".to_string()),
                WsErrorCode::InvalidOrder,
                WsErrorCategory::Validation,
            ),
            (
                VenueError::Unauthorized,
                WsErrorCode::Unauthorized,
                WsErrorCategory::Authorization,
            ),
            (
                VenueError::Forbidden(Permission::Trade),
                WsErrorCode::Forbidden,
                WsErrorCategory::Authorization,
            ),
            (
                VenueError::RateLimited,
                WsErrorCode::Throttled,
                WsErrorCategory::Throttle,
            ),
            (
                VenueError::Overflow,
                WsErrorCode::Internal,
                WsErrorCategory::Internal,
            ),
            (
                upstream_with_marker("x"),
                WsErrorCode::Internal,
                WsErrorCategory::Internal,
            ),
        ];
        for (err, code, category) in expected {
            assert_eq!(err.ws_code_category(), (code, category));
        }
    }

    #[test]
    fn test_ws_error_unauthorized_is_terminal() {
        let env = VenueError::Unauthorized.ws_error(None);
        assert!(env.terminal);
        assert!(!env.retryable);
        assert_eq!(env.retry_after_ms, None);
    }

    #[test]
    fn test_ws_error_command_error_is_non_terminal() {
        // A Forbidden command error leaves the connection open.
        let env = VenueError::Forbidden(Permission::Trade).ws_error(Some("req-1".to_string()));
        assert!(!env.terminal);
        assert_eq!(env.request_id, Some("req-1".to_string()));
    }

    #[test]
    fn test_ws_error_rate_limited_sets_retry_after_and_retryable() {
        let env = VenueError::RateLimited.ws_error(None);
        assert!(env.retryable);
        assert!(!env.terminal);
        assert_eq!(env.retry_after_ms, Some(RATE_LIMIT_RETRY_AFTER_MS));
    }

    #[test]
    fn test_ws_error_upstream_message_is_redacted() {
        let env = upstream_with_marker("SECRET-INTERNAL-DETAIL").ws_error(None);
        assert_eq!(env.code, WsErrorCode::Internal);
        assert_eq!(env.message, REDACTED_INTERNAL_MESSAGE);
        assert!(!env.message.contains("SECRET-INTERNAL-DETAIL"));
    }

    #[test]
    fn test_ws_error_schema_is_versioned() {
        let env = VenueError::RateLimited.ws_error(None);
        assert_eq!(env.schema, WS_ERROR_SCHEMA);
    }

    /// The WS `code` serialises to the same string the REST envelope's
    /// `machine_code` returns — one shared vocabulary across surfaces.
    #[test]
    fn test_ws_error_code_serialization_matches_rest_machine_code() {
        for err in all_variants() {
            let (code, _) = err.ws_code_category();
            let serialised = match serde_json::to_value(code) {
                Ok(serde_json::Value::String(s)) => s,
                other => panic!("expected a JSON string code, got {other:?}"),
            };
            assert_eq!(serialised, err.machine_code());
        }
    }

    // ---- From conversions (#002 domain errors) -----------------------------

    #[test]
    fn test_from_money_error_overflow_maps_to_overflow() {
        let err = VenueError::from(MoneyError::Overflow);
        assert!(matches!(err, VenueError::Overflow));
    }

    #[test]
    fn test_from_money_error_negative_cents_maps_to_invalid_order() {
        let err = VenueError::from(MoneyError::NegativeCents(-5));
        match err {
            VenueError::InvalidOrder(message) => assert!(message.contains("-5")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
    }

    #[test]
    fn test_from_symbol_error_invalid_symbol_maps_to_invalid_order() {
        let symbol_err = match crate::exchange::Symbol::parse("not-a-symbol") {
            Err(e) => e,
            Ok(s) => panic!("expected a parse error, got {s:?}"),
        };
        let err = VenueError::from(symbol_err);
        assert!(matches!(err, VenueError::InvalidOrder(_)));
        assert_eq!(err.http_status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_from_symbol_error_expiry_variant_maps_to_invalid_order() {
        // #032 DTO-boundary guard: a relative `ExpirationDate::Days` reaching the
        // runtime order boundary is a typed client rejection — `VenueError::InvalidOrder`
        // (HTTP 400 / FIX Reject), never a silent wall-clock re-resolution.
        let err = VenueError::from(SymbolError::RelativeExpiryRefused);
        assert!(matches!(err, VenueError::InvalidOrder(_)));
        assert_eq!(err.http_status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_from_upstream_error_maps_to_upstream_variant() {
        let err: VenueError = option_chain_orderbook::Error::UnderlyingNotFound {
            underlying: "BTC".to_string(),
        }
        .into();
        assert!(matches!(err, VenueError::Upstream(_)));
        assert_eq!(err.http_status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
