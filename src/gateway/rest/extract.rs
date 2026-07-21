//! REST request-body extraction that maps an Axum `Json` rejection onto the
//! shared typed [`VenueError`] boundary.
//!
//! Axum's built-in [`axum::Json`] extractor rejects a malformed / mistyped /
//! wrong-content-type body with its OWN plaintext `4xx` response, bypassing the
//! venue's [`ErrorEnvelope`](crate::error::ErrorEnvelope) contract every other
//! error uses. This [`Json`] wrapper delegates parsing to [`axum::Json`] but
//! converts the [`JsonRejection`] into a typed [`VenueError::InvalidOrder`] — so a
//! bad body renders the SAME typed envelope (HTTP `400` / FIX Reject) as a
//! `deny_unknown_fields` typo or a failed shape validation, never Axum's default
//! plaintext ([03 §8](../../../docs/03-protocol-surfaces.md)).
//!
//! The wrapper is a **drop-in** for [`axum::Json`]: it is both a [`FromRequest`]
//! extractor (request bodies) and an [`IntoResponse`] responder (JSON responses),
//! so a handler file swaps `use axum::Json;` for this one type and keeps its
//! `Json(request): Json<T>` destructuring and `Ok(Json(response))` construction
//! unchanged.

use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::VenueError;

/// A request-body / response JSON wrapper whose parse rejection renders the shared
/// typed [`ErrorEnvelope`](crate::error::ErrorEnvelope) (see the module docs).
/// Drop-in for [`axum::Json`].
pub struct Json<T>(pub T);

impl<T, S> FromRequest<S> for Json<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    // A `Response` rejection (not a bare `VenueError`) so the length-limit case can
    // keep its own semantically-correct `413` status while a parse failure renders
    // the typed `400` envelope.
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(axum::Json(value)) => Ok(Json(value)),
            Err(rejection) => Err(json_rejection_response(rejection)),
        }
    }
}

impl<T: Serialize> IntoResponse for Json<T> {
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

/// Renders an Axum [`JsonRejection`] as a typed [`VenueError`] envelope response,
/// **preserving** the oversized-body `413` (a DoS body-limit concern).
///
/// A malformed body (bad JSON syntax), a shape / `deny_unknown_fields` mismatch, or
/// a wrong / missing `application/json` content type are all **client-input**
/// failures → [`VenueError::InvalidOrder`] (HTTP `400` / FIX Reject), rendered as
/// the shared typed [`ErrorEnvelope`](crate::error::ErrorEnvelope) rather than
/// Axum's default plaintext. A request-body **length-limit** rejection keeps its
/// original `413 Payload Too Large` (the explicit
/// [`MAX_REQUEST_BODY_BYTES`](crate::gateway::rest::MAX_REQUEST_BODY_BYTES) DoS
/// ceiling, [08 §5](../../../docs/08-threat-model.md#5-denial-of-service-posture)).
///
/// The rejection text describes the client's OWN body (a syntax position or a field
/// name), so it is safe to echo — it never carries venue state, a cause chain, or a
/// secret — and is logged at `debug` for correlation.
fn json_rejection_response(rejection: JsonRejection) -> Response {
    tracing::debug!(detail = %rejection.body_text(), "rejected a malformed JSON request body");
    // Preserve the oversized-body 413 (the DoS body-limit ceiling), including Axum's
    // original response; every parse / shape / content-type failure funnels to the
    // typed 400 envelope.
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return rejection.into_response();
    }
    VenueError::InvalidOrder(format!(
        "malformed JSON request body: {}",
        rejection.body_text()
    ))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{StatusCode, header};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Sample {
        #[allow(dead_code)]
        value: u64,
    }

    /// Extracts a `Response`'s status and its JSON body (as a `serde_json::Value`).
    async fn read_response(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    fn post_json(body: &'static str) -> Request {
        Request::builder()
            .method("POST")
            .uri("/")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .expect("request builds")
    }

    /// A malformed JSON body is rejected as the shared typed `ErrorEnvelope`
    /// (HTTP `400`, code `invalid_order`), never Axum's default plaintext `4xx`.
    #[tokio::test]
    async fn test_malformed_body_renders_typed_error_envelope() {
        let rejection =
            match Json::<Sample>::from_request(post_json("{ not valid json "), &()).await {
                Err(response) => response,
                Ok(_) => panic!("expected a rejection for a malformed body"),
            };
        let (status, body) = read_response(rejection).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // The typed envelope shape — schema + code + message — not plaintext.
        assert_eq!(body["schema"], crate::error::REST_ERROR_SCHEMA);
        assert_eq!(body["code"], "invalid_order");
        assert!(
            body["message"]
                .as_str()
                .unwrap_or_default()
                .contains("malformed JSON request body")
        );
    }

    /// A shape / `deny_unknown_fields` mismatch (Axum's default `422`) also funnels
    /// to the typed `400` envelope, not plaintext.
    #[tokio::test]
    async fn test_shape_mismatch_is_typed_400_envelope() {
        // `value` should be a u64; a string is a data (shape) error.
        let rejection = match Json::<Sample>::from_request(post_json(r#"{"value":"x"}"#), &()).await
        {
            Err(response) => response,
            Ok(_) => panic!("expected a rejection for a shape mismatch"),
        };
        let (status, body) = read_response(rejection).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_order");
    }

    /// A well-formed body extracts cleanly through the wrapper.
    #[tokio::test]
    async fn test_valid_body_extracts() {
        match Json::<Sample>::from_request(post_json(r#"{"value":7}"#), &()).await {
            Ok(Json(sample)) => assert_eq!(sample.value, 7),
            Err(_) => panic!("expected a clean extraction"),
        }
    }
}
