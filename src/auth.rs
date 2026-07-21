//! Services layer: JWT authentication (RS256 / x509 key pair), the
//! [`Permission`] model, and the sliding-window [`RateLimiter`] shared by every
//! gateway.
//!
//! This is the **one** authorization model across REST, WebSocket, and FIX
//! ([ADR-0005](../docs/adr/0005-jwt-auth-for-rest-ws.md),
//! [03 §6](../docs/03-protocol-surfaces.md#6-authentication),
//! [01 §8](../docs/01-domain-model.md#8-accounts-and-sessions)). The legacy
//! Backend `ApiKeyStore` (SHA-256 `sk_live_`) is **not** carried over — there is
//! no `ApiKeyStore` / `sk_live_` path anywhere; JWT is the only credential
//! mechanism.
//!
//! ## What lands here (#011)
//!
//! - [`JwtAuth`] — RS256 signing with an x509 key pair. [`JwtAuth::from_paths`]
//!   loads the PEM pair (the public key is extracted **from the certificate**);
//!   [`JwtAuth::mint_token`] / [`JwtAuth::verify_token`]; a clearly-labelled
//!   [`JwtAuth::dev`] fixture built from an embedded, **non-secret** dev keypair.
//! - [`Claims`] carrying the account [`AccountId`], its permission set, `iat` /
//!   `exp`, and the account [revocation epoch](Claims::revocation_epoch), plus
//!   [`Claims::has_permission`] with the `Admin ⇒ Read + Trade` implication (the
//!   implication is enforced **here**, in the auth layer, not structurally on the
//!   [`Permission`] enum — [`Permission::grants`]).
//! - [`RateLimiter`] — a sliding 60 s window on the **injected venue clock**
//!   (never `SystemTime`), keyed on the resolved [`AccountId`] with a peer-IP
//!   fallback, so a fixed-seed run rate-limits **deterministically**
//!   ([03 §6.1](../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)).
//! - [`auth_middleware`] — the Axum layer that resolves identity, enforces the
//!   admission rate limit, checks the revocation epoch, and gates the required
//!   [`Permission`]; `GET /health` is fully exempt from **both** auth and rate
//!   limiting.
//! - The [`BootstrapGate`] on token issuance (`AUTH_BOOTSTRAP_SECRET`) and the
//!   [`DevMode`] release gate that refuses [`JwtAuth::dev`] keys in a published
//!   image unless `--dev` is set.
//!
//! ## The venue-clock seam (rate limiting must be replay-deterministic)
//!
//! The rate limiter reads time from an injected [`RateLimitClock`] — the **same**
//! venue clock the sequenced order path stamps events from
//! ([`crate::exchange::VenueClock`]) — never `SystemTime`. Because the
//! `auth_middleware` runs **outside** the per-underlying actor, the venue injects
//! a shared clock handle it owns; [`FixedClock`]
//! bridges [`RateLimitClock`] today (it is what the actor uses), and the
//! advanceable seeded/stepped clock arrives with the simulation clock (#016)
//! while the rate-limit configuration (the per-window budget) is wired by #046.
//! When two admissions carry the same venue-clock deadline the deterministic
//! tie-break is `(session_id, arrival_sequence)`; the counting here is
//! order-independent, and that final ingress ordering into the single writer is
//! applied by the gateway layer (#012/#013) that serialises competing requests.
//!
//! ## Secrets
//!
//! The signing key, the embedded dev keypair, and `AUTH_BOOTSTRAP_SECRET` are
//! **never** logged or echoed in an error body / FIX `Text (58)`
//! ([08 §7](../docs/08-threat-model.md#7-secrets-handling)): [`JwtAuth`] and
//! [`BootstrapGate`] have redacting [`std::fmt::Debug`] impls,
//! [`JwtAuth::verify_token`] collapses every failure to [`VenueError::Unauthorized`]
//! without leaking the cause, and no token is ever logged.

use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

use crate::error::VenueError;
use crate::exchange::{FixedClock, VenueClock};
use crate::models::{AccountId, Permission};

// ============================================================================
// Constants
// ============================================================================

/// The rate-limiter's sliding window, in **milliseconds** — the venue's 60 s
/// window ([03 §6.1](../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)).
/// Equal to [`crate::error::RATE_LIMIT_RETRY_AFTER_MS`].
pub const RATE_LIMIT_WINDOW_MS: u64 = 60_000;

/// The default per-window request budget. The live per-account/per-endpoint
/// value is venue config (#046); this is the bounded default until then.
pub const DEFAULT_RATE_LIMIT_PER_WINDOW: u32 = 100;

/// The default **key-space** ceiling on the [`RateLimiter`] — the maximum number
/// of distinct rate-limit buckets tracked at once. A DoS control: an attacker
/// cycling source IPs (trivial over IPv6) cannot grow the map without bound, so
/// the limiter is bounded **by construction**, not by a future periodic sweep
/// being wired ([08 §5](../docs/08-threat-model.md#5-resource-exhaustion)). The
/// live value is venue config (#046); this is the bounded default until then.
pub const DEFAULT_MAX_RATE_LIMIT_KEYS: usize = 100_000;

/// The request paths fully exempt from **both** auth and rate limiting. Only the
/// container health check qualifies — it must answer unconditionally
/// ([03 §6](../docs/03-protocol-surfaces.md#6-authentication)).
pub const EXEMPT_PATHS: &[&str] = &["/health"];

/// The `X-RateLimit-Limit` response header (the window budget).
pub const HEADER_RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
/// The `X-RateLimit-Remaining` response header (budget left in the window).
pub const HEADER_RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
/// The `X-RateLimit-Reset` response header — venue-clock **milliseconds** at
/// which the window frees up.
pub const HEADER_RATELIMIT_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");

// ============================================================================
// Permission implication (Admin ⇒ Read + Trade), enforced in the auth layer
// ============================================================================

impl Permission {
    /// Whether holding `self` grants the `required` permission — the venue's
    /// authorization implication, enforced **here** rather than structurally on
    /// the [`Permission`] enum ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions)).
    ///
    /// The relation is the total order `Read ⊂ Trade ⊂ Admin`: `Admin` grants
    /// everything, `Trade` grants `Trade` and `Read` (a trading client also reads
    /// market data — `V` needs `Read`, [03 §6](../docs/03-protocol-surfaces.md#6-authentication)),
    /// and `Read` grants only `Read`. Matched **exhaustively** on `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::models::Permission;
    /// assert!(Permission::Admin.grants(Permission::Trade));
    /// assert!(Permission::Trade.grants(Permission::Read));
    /// assert!(!Permission::Read.grants(Permission::Trade));
    /// ```
    #[must_use]
    #[inline]
    pub fn grants(self, required: Permission) -> bool {
        match self {
            Permission::Admin => true,
            Permission::Trade => matches!(required, Permission::Read | Permission::Trade),
            Permission::Read => matches!(required, Permission::Read),
        }
    }
}

// ============================================================================
// Claims
// ============================================================================

/// The JWT claim set carried by an authenticated REST/WS session — resolved
/// **identically** for a FIX logon (#037), one permission model across every
/// surface ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions),
/// [ADR-0005](../docs/adr/0005-jwt-auth-for-rest-ws.md)).
///
/// `sub` **is** the [`AccountId`] (no separate subject field); `exp` / `iat` are
/// standard NumericDate **seconds** since the Unix epoch (validated by
/// `jsonwebtoken` against the wall clock — token expiry is a credential-plane
/// concern and an explicit replay exclusion, [03 §10](../docs/03-protocol-surfaces.md#10-state-changing-operation-classification),
/// so it does **not** use the venue clock). `revocation_epoch` is the account's
/// epoch **at mint time**: a token below the account's current epoch is refused
/// per-request (the middleware) so a revoke drops outstanding tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// The account identity — the JWT `sub`.
    pub sub: AccountId,
    /// The permission set (`Admin` implies `Read` + `Trade`, [`Permission::grants`]).
    pub permissions: Vec<Permission>,
    /// Issued-at, seconds since the Unix epoch.
    pub iat: u64,
    /// Expiry, seconds since the Unix epoch — validated on every verify.
    pub exp: u64,
    /// The account revocation epoch at mint time; a token below the account's
    /// current epoch is refused ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions)).
    pub revocation_epoch: u64,
}

impl Claims {
    /// Builds a claim set for `account`.
    ///
    /// `iat` / `exp` are seconds since the Unix epoch (the caller computes them
    /// from the wall clock at issuance); `revocation_epoch` is the account's epoch
    /// at mint time.
    #[must_use]
    pub fn new(
        account: AccountId,
        permissions: Vec<Permission>,
        iat: u64,
        exp: u64,
        revocation_epoch: u64,
    ) -> Self {
        Self {
            sub: account,
            permissions,
            iat,
            exp,
            revocation_epoch,
        }
    }

    /// The account this token authenticates.
    #[must_use]
    #[inline]
    pub fn account(&self) -> &AccountId {
        &self.sub
    }

    /// Whether this session holds the `required` permission, applying the
    /// `Admin ⇒ Read + Trade` implication ([`Permission::grants`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::auth::Claims;
    /// use fauxchange::models::{AccountId, Permission};
    /// let claims = Claims::new(AccountId::new("acct-1"), vec![Permission::Admin], 0, 1, 0);
    /// assert!(claims.has_permission(Permission::Trade));
    /// ```
    #[must_use]
    #[inline]
    pub fn has_permission(&self, required: Permission) -> bool {
        self.permissions.iter().any(|held| held.grants(required))
    }
}

// ============================================================================
// AuthError — construction / minting / gate failures
// ============================================================================

/// A failure constructing [`JwtAuth`], minting a token, or clearing a gate.
///
/// Distinct from the request-boundary [`VenueError`]: these are startup / issuance
/// failures. No variant carries key material, a token, or the bootstrap secret —
/// [`AuthError::KeyLoad`] carries only a **non-secret** step label
/// ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// PEM key material could not be loaded or parsed. Carries the failing step
    /// (`"certificate"` / `"private key"`) — never the bytes.
    #[error("failed to load auth key material: {0}")]
    KeyLoad(&'static str),
    /// Signing the token failed (cause redacted; never carries key material).
    #[error("failed to sign token")]
    Signing,
    /// Token issuance is disabled — `AUTH_BOOTSTRAP_SECRET` is unset.
    #[error("token issuance disabled: AUTH_BOOTSTRAP_SECRET is not set")]
    BootstrapDisabled,
    /// The presented bootstrap secret did not match.
    #[error("invalid bootstrap secret")]
    BootstrapMismatch,
    /// Dev auth keys are refused in a published image unless `--dev` is set.
    #[error("dev auth keys are refused without --dev (dev mode disabled)")]
    DevKeyRefused,
}

// ============================================================================
// Embedded dev keypair — a clearly-labelled NON-SECRET test fixture
// ============================================================================
//
// This is a published, well-known dev keypair — NOT a real credential and NOT a
// secret. It exists so `JwtAuth::dev()` gives a runnable local venue without key
// provisioning. It is refused in a published image unless `--dev` is set (the
// `DevMode` release gate); the image-scan test that asserts the release image
// ships without it is #026. Do NOT use these keys anywhere real.

/// The embedded dev **certificate** (public-key carrier). NON-SECRET dev fixture.
const DEV_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDJDCCAgwCCQD9liRgGlkcGzANBgkqhkiG9w0BAQsFADBTMTAwLgYDVQQDDCdm
YXV4Y2hhbmdlLWRldi1ETy1OT1QtVVNFLUlOLVBST0RVQ1RJT04xHzAdBgNVBAoM
FmZhdXhjaGFuZ2UgZGV2IGZpeHR1cmUwIBcNMjYwNzE1MjIwMTE2WhgPMjEyNjA2
MjEyMjAxMTZaMFMxMDAuBgNVBAMMJ2ZhdXhjaGFuZ2UtZGV2LURPLU5PVC1VU0Ut
SU4tUFJPRFVDVElPTjEfMB0GA1UECgwWZmF1eGNoYW5nZSBkZXYgZml4dHVyZTCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALrWMD+IXSMlKo3/UMYb+w+l
6iyQIR89yoNiMlczcZL2Gxgpa8U4+smJ1qwtnI/ZNyzho7SZZWEGoChjG7xrhSAt
UiPjAVMz3Yo19Jxeos9sTTBWq9pOlwGh378qjy/laJgCJXFiMnU/Ld/RKnw5lQjA
gCWQOER0yODGCpYgwmJKWfCiynROz1/CJbvl8rsxRTnLIGKySl3yFGrRPa2F48NV
xBDIOVa7b1yjUaBXV72+eHhwRootS49iTQGiJ19ShOmrHLfJuDAVbXRfA6ku4wQH
f0qWbzDxIdb7E2u/K9PqrwaVlADWhocedY6wxQ74t3KcxrK/SaSIDKJk4zwuInEC
AwEAATANBgkqhkiG9w0BAQsFAAOCAQEAqWIIPQ7EDU5N6C3FUUCBc6+RPI/h7J3g
Yjkhkj7qzAmEKruwQYi77guE9GK2rBjtgMaBaFr17fJ+hWbf7XIOBC0mzqJ14Azv
D363pfgv5+e43W4FJSa0B5JwDAhXQ5MaHZdfl3f8JDSAGpn0ezcGLFCVLIHoZxEC
9bDQWQvV0fPsmOS0SUsX84KxJ3UyRLPxKMSwGiCpVkvJmUrJXGhH9VN16JPibYYt
25XGioVJ289WnDoaMA+i7mgaijQJdgP7djERq2zCo9vI+QXshTpmBOSCnAkCOC1m
JvqSVeJPqQsFXAgF4sQVoh5wdbj5lxCjC6d5cH1JALMYlxRLMBzHuA==
-----END CERTIFICATE-----
";

/// The embedded dev **private key** (PKCS#8). NON-SECRET dev fixture — never a
/// real credential.
const DEV_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC61jA/iF0jJSqN
/1DGG/sPpeoskCEfPcqDYjJXM3GS9hsYKWvFOPrJidasLZyP2Tcs4aO0mWVhBqAo
Yxu8a4UgLVIj4wFTM92KNfScXqLPbE0wVqvaTpcBod+/Ko8v5WiYAiVxYjJ1Py3f
0Sp8OZUIwIAlkDhEdMjgxgqWIMJiSlnwosp0Ts9fwiW75fK7MUU5yyBiskpd8hRq
0T2thePDVcQQyDlWu29co1GgV1e9vnh4cEaKLUuPYk0BoidfUoTpqxy3ybgwFW10
XwOpLuMEB39Klm8w8SHW+xNrvyvT6q8GlZQA1oaHHnWOsMUO+LdynMayv0mkiAyi
ZOM8LiJxAgMBAAECggEBAKCrLJaWB7H/dhbiZm3XBhGw1i44S6N4GbzeJvhCLvr4
VNh0Vk8l7tR9inRKTQaO/xnDeGoIN9w2PGg+wk1IERVYo/hkcHFCetMuDwqhf1Ts
h3x4LBTx3H303FqimLvRhh6iSdy1Wzrkd+ivEN//DKCYGhszaI/F9jEFXXk49rBa
Uk8QXpC4VIBeY0FDBta0kd097p78KLQB10P72R4vFwxOZyAc8bbsjeENnPnnOnAQ
WJl56d7NQFEVVpe4O/M1oCGY0vW333ixwBfZISNFUPvlqbWdtZ1NBl1qY//PevqW
egmoGmzf/djTEtyp8Mrr/OKT0rftkNz8GQf1B/gOVdkCgYEA70WWBemeAbnvuZVS
L40Ia0Vz2n7Jb+9ebpNQR9e9qJvV7OoCT8mUwirfO9JVBN/wKkuRMd9roA6ZCkhB
5zTe1VGI7OCZsbcxWCi/z8otpa/bSZbFInnrBgD5MjuotkoTbHqsz1fJgYfYgSH9
7qG6JYj3M+r9QrruOGAn8QGlPB8CgYEAx+YiWtgwVseBGOPpbB9BD0rD7XbFRQOJ
k3t19nPkiJ0mVvslzY18akAc9iDX2c5Nvl+ESAUgdus5msA2iz62f5WxyZbDfjv7
JVdH+/h8T1ahuSKb7hn47aD3ize4p7tGC8YWaOZOwPLOBNngNZ9J9pHI3vUSTjwP
H2YRctiUz28CgYBvCT7WnZRKvsultsq98Ffg2AksczvtqwqKi+hsfoywCylaWTob
ZrOW66hOrYvwyC8+oXTOzRy32S5iHCghMGLcYYsGSjBozVejzr08o1lNk29TFhmD
p0pOrfL2wcLIXVXoOIGrctS7PJxXSLv7mqe0tXvqZvmClxbnqI/Agv/4BwKBgQCv
fQtf8TbOmCpvbXX4Y5+8CwjiKUiZg7d9b/9pMujIPh3wcl8Hi1RT+qDyOncEUSbT
IAuDJm0PuQVDI8c+ivmwG/yOWvqYkZOzfmJFhCmthQJJA2ccqlRsWMm4wFwtdCzU
HTyDLtyoawAOJi+9I2/NNMLBaSh+4h7sk7BxwE0zpQKBgQC2HfY0sZK/+A8yECc4
xo4atZWfl6OpX7vj5wr84Hv3ou5eXGuS7W3xJjl0tadVHhbuXm9GlBnBF/l/aOlU
PmHtjB7W7eHtdJ9yNFYAAKL71yw2xIkgl2msa6xZBVvgqRB53jkWFughWvOV6CRt
65jfGip84diNIbtDf9vDtAq5JQ==
-----END PRIVATE KEY-----
";

// ============================================================================
// JwtAuth
// ============================================================================

/// The RS256 / x509 JWT service: signs tokens with the private key and verifies
/// them with the public key extracted from the certificate
/// ([ADR-0005](../docs/adr/0005-jwt-auth-for-rest-ws.md)). Asymmetric on purpose
/// — a consumer verifies with only the public key, a realistic auth shape to test
/// a client against.
pub struct JwtAuth {
    encoding: EncodingKey,
    decoding: DecodingKey,
    validation: Validation,
    /// `true` when built from the embedded dev fixtures — the release gate refuses
    /// these unless `--dev` is set ([`JwtAuth::release_gated`]).
    is_dev: bool,
}

impl std::fmt::Debug for JwtAuth {
    /// Redacts all key material — never prints the signing/verifying keys
    /// ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtAuth")
            .field("algorithm", &"RS256")
            .field("is_dev", &self.is_dev)
            .field("keys", &"<redacted>")
            .finish()
    }
}

impl JwtAuth {
    /// Builds RS256 auth from a **certificate** PEM (public-key carrier) and a
    /// **private key** PEM (PKCS#1 or PKCS#8). The public key is extracted from
    /// the certificate — a consumer needs only the cert to verify.
    ///
    /// # Errors
    ///
    /// [`AuthError::KeyLoad`] if either PEM fails to parse. The error carries only
    /// a non-secret step label, never the key bytes.
    #[cold]
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, AuthError> {
        Self::from_pem_inner(cert_pem, key_pem, false)
    }

    /// Loads the RS256 key pair from PEM files: `cert_pem_path` is the x509
    /// certificate (public key extracted from it), `key_pem_path` the private key.
    ///
    /// Reads the files synchronously — intended for **bootstrap** (`main.rs`), not
    /// an async handler.
    ///
    /// # Errors
    ///
    /// [`AuthError::KeyLoad`] if a file cannot be read or a PEM cannot be parsed.
    /// The error names the failing step (`"certificate"` / `"private key"`) but
    /// never the path contents or key bytes.
    #[cold]
    pub fn from_paths(
        cert_pem_path: impl AsRef<Path>,
        key_pem_path: impl AsRef<Path>,
    ) -> Result<Self, AuthError> {
        let cert = std::fs::read(cert_pem_path).map_err(|_| AuthError::KeyLoad("certificate"))?;
        let key = std::fs::read(key_pem_path).map_err(|_| AuthError::KeyLoad("private key"))?;
        Self::from_pem_inner(&cert, &key, false)
    }

    /// Builds auth from the **embedded, non-secret dev fixtures** — a clearly
    /// labelled local-only keypair, never a real credential. Combine with
    /// [`JwtAuth::release_gated`] so a published image refuses it without `--dev`.
    ///
    /// # Errors
    ///
    /// [`AuthError::KeyLoad`] only if the embedded fixtures fail to parse (a build
    /// invariant covered by the unit tests); never in practice.
    pub fn dev() -> Result<Self, AuthError> {
        Self::from_pem_inner(DEV_CERT_PEM.as_bytes(), DEV_KEY_PEM.as_bytes(), true)
    }

    /// The release gate: refuses dev keys unless dev mode is explicitly enabled.
    ///
    /// A production key pair (`is_dev == false`) always passes; the embedded dev
    /// fixture is admitted **only** when `dev_mode` is enabled (the `--dev` flag /
    /// `FAUXCHANGE_DEV`), matching the threat-model release gate
    /// ([08 §7](../docs/08-threat-model.md#7-secrets-handling),
    /// [06 §8](../docs/06-deployment.md#8-auth-bootstrap)). The image-scan test is
    /// #026.
    ///
    /// # Errors
    ///
    /// [`AuthError::DevKeyRefused`] if these are dev keys and dev mode is disabled.
    pub fn release_gated(self, dev_mode: DevMode) -> Result<Self, AuthError> {
        if self.is_dev && !dev_mode.is_enabled() {
            tracing::error!(
                "refusing to start auth on embedded dev keys in release mode; set --dev for local use"
            );
            return Err(AuthError::DevKeyRefused);
        }
        Ok(self)
    }

    /// Whether this service was built from the embedded dev fixtures.
    #[must_use]
    #[inline]
    pub fn is_dev(&self) -> bool {
        self.is_dev
    }

    /// Signs an RS256 token for `claims` — **gated** on the bootstrap secret so
    /// minting requires the operator credential (`AUTH_BOOTSTRAP_SECRET`,
    /// [03 §6](../docs/03-protocol-surfaces.md#6-authentication)). Account
    /// resolution (which account / permissions a caller may mint for) is the
    /// registry's job (#012); here the gate is the only issuance control.
    ///
    /// # Errors
    ///
    /// - [`AuthError::BootstrapDisabled`] / [`AuthError::BootstrapMismatch`] if the
    ///   gate rejects the presented secret;
    /// - [`AuthError::Signing`] if signing fails (cause redacted).
    pub fn mint_token(
        &self,
        gate: &BootstrapGate,
        presented_secret: &str,
        claims: &Claims,
    ) -> Result<String, AuthError> {
        gate.authorize(presented_secret)?;
        let header = Header::new(Algorithm::RS256);
        encode(&header, claims, &self.encoding).map_err(|_| AuthError::Signing)
    }

    /// Verifies an RS256 bearer token and returns its [`Claims`]. Validation pins
    /// **RS256** (rejecting `alg:none` and HS256 algorithm-confusion) and enforces
    /// `exp`.
    ///
    /// # Errors
    ///
    /// [`VenueError::Unauthorized`] on **any** failure — a bad signature, a
    /// tampered token, an expired token, a wrong algorithm, or a malformed claim
    /// set. The underlying cause is **redacted**: it is never returned to the
    /// client and the token is never logged.
    #[must_use = "an unhandled verification result silently admits the request"]
    pub fn verify_token(&self, token: &str) -> Result<Claims, VenueError> {
        decode::<Claims>(token, &self.decoding, &self.validation)
            .map(|data| data.claims)
            .map_err(|_| VenueError::Unauthorized)
    }

    #[cold]
    fn from_pem_inner(cert_pem: &[u8], key_pem: &[u8], is_dev: bool) -> Result<Self, AuthError> {
        let encoding =
            EncodingKey::from_rsa_pem(key_pem).map_err(|_| AuthError::KeyLoad("private key"))?;
        // `from_rsa_pem` accepts a `CERTIFICATE` PEM and extracts the RSA public
        // key from its SubjectPublicKeyInfo — no separate x509 parser needed.
        let decoding =
            DecodingKey::from_rsa_pem(cert_pem).map_err(|_| AuthError::KeyLoad("certificate"))?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;
        // No audience is used by this venue; disable the check so a token without
        // `aud` is not spuriously rejected.
        validation.validate_aud = false;
        Ok(Self {
            encoding,
            decoding,
            validation,
            is_dev,
        })
    }
}

// ============================================================================
// DevMode — the dev-key release gate switch
// ============================================================================

/// Whether the process runs in local **dev mode** (`--dev` / `FAUXCHANGE_DEV`),
/// which is the only condition under which [`JwtAuth::dev`] keys are admitted
/// ([`JwtAuth::release_gated`]). A published image runs without it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DevMode {
    enabled: bool,
}

impl DevMode {
    /// Constructs a dev-mode switch from the resolved `--dev` flag.
    #[must_use]
    #[inline]
    pub const fn from_flag(dev_flag: bool) -> Self {
        Self { enabled: dev_flag }
    }

    /// Resolves dev mode from the environment: `FAUXCHANGE_DEV` set to `1` /
    /// `true` enables it; unset or any other value keeps it **disabled** (the
    /// release-image default).
    #[must_use]
    pub fn from_env() -> Self {
        let enabled = std::env::var("FAUXCHANGE_DEV")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
            .unwrap_or(false);
        Self { enabled }
    }

    /// Whether dev mode is enabled.
    #[must_use]
    #[inline]
    pub const fn is_enabled(self) -> bool {
        self.enabled
    }
}

// ============================================================================
// BootstrapGate — token issuance gate (AUTH_BOOTSTRAP_SECRET)
// ============================================================================

/// The operator gate on token issuance: minting requires the
/// `AUTH_BOOTSTRAP_SECRET` ([03 §6](../docs/03-protocol-surfaces.md#6-authentication),
/// [06 §8](../docs/06-deployment.md#8-auth-bootstrap)). When the secret is unset,
/// **no** token can be minted.
pub struct BootstrapGate {
    secret: Option<String>,
}

impl std::fmt::Debug for BootstrapGate {
    /// Redacts the secret — never prints or hints at its value
    /// ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BootstrapGate")
            .field("enabled", &self.secret.is_some())
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl BootstrapGate {
    /// Builds a gate around an explicit secret (`None` disables issuance).
    #[must_use]
    pub fn new(secret: Option<String>) -> Self {
        Self { secret }
    }

    /// Resolves the gate from `AUTH_BOOTSTRAP_SECRET`; an unset or empty value
    /// **disables** token issuance.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            secret: std::env::var("AUTH_BOOTSTRAP_SECRET")
                .ok()
                .filter(|value| !value.is_empty()),
        }
    }

    /// Whether token issuance is enabled (the secret is set).
    #[must_use]
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.secret.is_some()
    }

    /// Clears the gate for a presented secret, using a constant-time comparison so
    /// a match cannot be discovered by timing.
    ///
    /// # Errors
    ///
    /// - [`AuthError::BootstrapDisabled`] if issuance is disabled;
    /// - [`AuthError::BootstrapMismatch`] if the presented secret is wrong.
    pub fn authorize(&self, presented: &str) -> Result<(), AuthError> {
        match &self.secret {
            None => Err(AuthError::BootstrapDisabled),
            Some(secret) if constant_time_eq(secret.as_bytes(), presented.as_bytes()) => Ok(()),
            Some(_) => Err(AuthError::BootstrapMismatch),
        }
    }
}

// ============================================================================
// The venue-clock seam for rate limiting
// ============================================================================

/// The venue time source the [`RateLimiter`] reads — the **same** injected clock
/// the sequenced path stamps events from, so rate-limit decisions replay
/// deterministically ([03 §6.1](../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)).
/// Milliseconds on the venue clock, never `SystemTime`.
///
/// [`FixedClock`] bridges it today; the advanceable
/// seeded/stepped venue clock arrives with #016.
pub trait RateLimitClock: Send + Sync {
    /// The current venue-clock instant, in **milliseconds**.
    #[must_use]
    fn now_ms(&self) -> u64;
}

impl RateLimitClock for FixedClock {
    /// Bridges the venue [`VenueClock`] onto the rate-limiter clock seam — the
    /// rate limiter and the sequenced path read the **same** venue clock.
    #[inline]
    fn now_ms(&self) -> u64 {
        VenueClock::now_ms(self).get()
    }
}

// ============================================================================
// RateLimiter
// ============================================================================

/// The rate-limit bucket key: the resolved account for an authenticated request,
/// or the peer IP for an unauthenticated (pre-token) request — one budget per
/// account across surfaces ([03 §6](../docs/03-protocol-surfaces.md#6-authentication)).
///
/// The authenticated bucket is keyed on **both** the [`AccountId`] and the
/// token's `revocation_epoch`: a revoked-but-still-signed token (a stale epoch)
/// buckets **separately** from a freshly re-authenticated session, so a
/// post-compromise holder of a stale token cannot drain the budget the owner's
/// current session needs ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions)).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RateLimitKey {
    /// Keyed on the authenticated account **and** its token's revocation epoch.
    Account {
        /// The authenticated account.
        account: AccountId,
        /// The token's revocation epoch (stale-epoch tokens bucket apart).
        revocation_epoch: u64,
    },
    /// Keyed on the peer IP (unauthenticated fallback, never `/health`).
    Peer(IpAddr),
}

/// The outcome of a rate-limit check, carrying the `X-RateLimit-*` context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitDecision {
    /// Whether the request is admitted.
    pub allowed: bool,
    /// The window budget (`X-RateLimit-Limit`).
    pub limit: u32,
    /// Budget remaining in the window after this request (`X-RateLimit-Remaining`).
    pub remaining: u32,
    /// Venue-clock **milliseconds** at which the window frees up
    /// (`X-RateLimit-Reset`).
    pub reset_ms: u64,
    /// When denied, the backoff hint in **milliseconds** (`Retry-After`, rounded
    /// up to seconds on the wire).
    pub retry_after_ms: Option<u64>,
}

impl RateLimitDecision {
    /// Writes the `X-RateLimit-*` (and, when denied, `Retry-After`) headers onto a
    /// response, overwriting any placeholder values.
    pub fn apply_headers(&self, headers: &mut HeaderMap) {
        headers.insert(HEADER_RATELIMIT_LIMIT, HeaderValue::from(self.limit));
        headers.insert(
            HEADER_RATELIMIT_REMAINING,
            HeaderValue::from(self.remaining),
        );
        headers.insert(HEADER_RATELIMIT_RESET, HeaderValue::from(self.reset_ms));
        if let Some(retry_after_ms) = self.retry_after_ms {
            let retry_after_secs = retry_after_ms.div_ceil(1_000);
            headers.insert(header::RETRY_AFTER, HeaderValue::from(retry_after_secs));
        }
    }
}

/// A sliding-window rate limiter on the **injected venue clock**, keyed on
/// [`RateLimitKey`] ([03 §6.1](../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)).
///
/// It keeps, per key, the admission timestamps within the current window (a
/// sliding **log**); a request is admitted while fewer than `limit` timestamps
/// remain in the window. Because every timestamp is a venue-clock read, a
/// fixed-seed run produces identical decisions on replay. The per-key log is
/// bounded by `limit` (over-limit requests are not recorded), and the **number
/// of keys** is bounded by `max_keys` (a DoS control, [08 §5](../docs/08-threat-model.md#5-resource-exhaustion)):
/// a would-be new key at capacity triggers an opportunistic inline sweep and, if
/// the map is still full, **fails closed** (denies) rather than grow — an
/// existing key always proceeds. [`sweep_expired`] additionally reclaims idle
/// keys on a periodic path.
///
/// [`sweep_expired`]: RateLimiter::sweep_expired
pub struct RateLimiter<C> {
    clock: C,
    limit: u32,
    window_ms: u64,
    max_keys: usize,
    windows: DashMap<RateLimitKey, Vec<u64>>,
}

impl<C> std::fmt::Debug for RateLimiter<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("limit", &self.limit)
            .field("window_ms", &self.window_ms)
            .field("max_keys", &self.max_keys)
            .field("tracked_keys", &self.windows.len())
            .finish_non_exhaustive()
    }
}

impl<C: RateLimitClock> RateLimiter<C> {
    /// Builds a limiter with the venue's 60 s window ([`RATE_LIMIT_WINDOW_MS`]),
    /// the given per-window budget, and the default key-space ceiling
    /// ([`DEFAULT_MAX_RATE_LIMIT_KEYS`]).
    #[must_use]
    pub fn new(clock: C, limit: u32) -> Self {
        Self::with_window(clock, limit, RATE_LIMIT_WINDOW_MS)
    }

    /// Builds a limiter with an explicit window and the default key-space ceiling
    /// (used by tests; production uses [`RATE_LIMIT_WINDOW_MS`]).
    #[must_use]
    pub fn with_window(clock: C, limit: u32, window_ms: u64) -> Self {
        Self::with_capacity(clock, limit, window_ms, DEFAULT_MAX_RATE_LIMIT_KEYS)
    }

    /// Builds a limiter with an explicit window **and** key-space ceiling. The
    /// ceiling is clamped to at least `1`.
    #[must_use]
    pub fn with_capacity(clock: C, limit: u32, window_ms: u64, max_keys: usize) -> Self {
        Self {
            clock,
            limit,
            window_ms,
            max_keys: max_keys.max(1),
            windows: DashMap::new(),
        }
    }

    /// The per-window budget.
    #[must_use]
    #[inline]
    pub fn limit(&self) -> u32 {
        self.limit
    }

    /// The key-space ceiling (the DoS bound on the number of tracked buckets).
    #[must_use]
    #[inline]
    pub fn max_keys(&self) -> usize {
        self.max_keys
    }

    /// Checks and, when admitted, **records** a request for `key` at the current
    /// venue-clock instant, returning the [`RateLimitDecision`] with the
    /// `X-RateLimit-*` context. An over-limit request is **not** recorded (the log
    /// stays bounded by `limit`).
    ///
    /// **Key-space DoS bound.** An **existing** key always proceeds. A would-be
    /// **new** key at the `max_keys` ceiling first triggers an opportunistic inline
    /// [`sweep_expired`](Self::sweep_expired) of expired buckets; if the map is
    /// still full, the request **fails closed** (a throttle) rather than insert a
    /// new key — the tracked key-space never grows past `max_keys`
    /// ([08 §5](../docs/08-threat-model.md#5-resource-exhaustion)).
    #[must_use]
    pub fn check_and_record_status(&self, key: &RateLimitKey) -> RateLimitDecision {
        let now = self.clock.now_ms();

        // Fast path: an existing key always proceeds (no capacity concern). The
        // shard guard is held only across the pure `decide` computation — no map
        // access inside it, so no lock is held across another map operation.
        if let Some(mut timestamps) = self.windows.get_mut(key) {
            return self.decide(&mut timestamps, now);
        }

        // Would-be NEW key: enforce the key-space ceiling (DoS control). No shard
        // guard is held here, so the sweep/len calls cannot deadlock.
        if self.windows.len() >= self.max_keys {
            self.sweep_expired();
            if self.windows.len() >= self.max_keys {
                return self.denied_capacity(now);
            }
        }

        let mut timestamps = self.windows.entry(key.clone()).or_default();
        self.decide(&mut timestamps, now)
    }

    /// Prunes the expired timestamps for one bucket, then admits (recording the
    /// current instant) or throttles per the per-window budget.
    ///
    /// The `checked_*(...).unwrap_or(bound)` fallbacks handle the (unreachable for
    /// real venue-clock ms and in-branch-exact) overflow arms **explicitly**; the
    /// repo rules forbid `saturating_*` / `wrapping_*` (they silently hide
    /// overflow), so clippy's `manual_saturating_arithmetic` suggestion — which
    /// would reintroduce them — is allowed here.
    #[allow(clippy::manual_saturating_arithmetic)]
    fn decide(&self, timestamps: &mut Vec<u64>, now: u64) -> RateLimitDecision {
        let window_ms = self.window_ms;
        let limit = u64::from(self.limit);

        // Drop timestamps older than the window (age >= window_ms). A timestamp in
        // the future (clock skew) is kept — it is trivially within the window.
        timestamps.retain(|&recorded| match now.checked_sub(recorded) {
            Some(age) => age < window_ms,
            None => true,
        });

        let used = timestamps.len() as u64;
        let oldest = timestamps.iter().copied().min().unwrap_or(now);
        // reset = when the window's oldest live admission ages out (unreachable
        // overflow arm pinned at the clock ceiling, explicitly).
        let reset_ms = oldest.checked_add(window_ms).unwrap_or(u64::MAX);

        if used < limit {
            timestamps.push(now);
            // `used < limit`, so `limit - used - 1` is exact; the `0` arm is
            // unreachable.
            let remaining = limit.checked_sub(used + 1).unwrap_or(0);
            RateLimitDecision {
                allowed: true,
                limit: self.limit,
                remaining: u32::try_from(remaining).unwrap_or(u32::MAX),
                reset_ms,
                retry_after_ms: None,
            }
        } else {
            // Denied: `reset_ms > now` (the oldest admission is in-window), so this
            // is exact; the `0` arm is unreachable.
            let retry_after_ms = reset_ms.checked_sub(now).unwrap_or(0);
            RateLimitDecision {
                allowed: false,
                limit: self.limit,
                remaining: 0,
                reset_ms,
                retry_after_ms: Some(retry_after_ms),
            }
        }
    }

    /// The fail-closed throttle for a new key refused at the `max_keys` ceiling —
    /// the request is denied without being tracked (the key-space cannot grow).
    #[allow(clippy::manual_saturating_arithmetic)]
    fn denied_capacity(&self, now: u64) -> RateLimitDecision {
        let reset_ms = now.checked_add(self.window_ms).unwrap_or(u64::MAX);
        RateLimitDecision {
            allowed: false,
            limit: self.limit,
            remaining: 0,
            reset_ms,
            retry_after_ms: Some(self.window_ms),
        }
    }

    /// Reclaims keys whose windows have fully expired — called periodically by the
    /// gateway (#012/#013), never on the request path.
    pub fn sweep_expired(&self) {
        let now = self.clock.now_ms();
        let window_ms = self.window_ms;
        self.windows.retain(|_key, timestamps| {
            timestamps.retain(
                |&recorded| matches!(now.checked_sub(recorded), Some(age) if age < window_ms),
            );
            !timestamps.is_empty()
        });
    }

    /// The number of keys currently tracked (for observability / tests).
    #[must_use]
    #[inline]
    pub fn tracked_keys(&self) -> usize {
        self.windows.len()
    }
}

// ============================================================================
// Revocation oracle
// ============================================================================

/// Resolves an account's **current** revocation epoch so the middleware can
/// refuse a token minted below it ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions)).
/// Implemented by the venue account registry (#012); `None` means the account is
/// unknown (the token is refused).
pub trait RevocationOracle: Send + Sync {
    /// The account's current revocation epoch, or `None` if unknown.
    #[must_use]
    fn current_revocation_epoch(&self, account: &AccountId) -> Option<u64>;
}

// ============================================================================
// AuthService + admission
// ============================================================================

/// The authenticated identity attached to an admitted request — inserted into the
/// request extensions for downstream handlers (#013/#014).
#[derive(Debug, Clone)]
pub struct Authorized {
    /// The verified claim set.
    pub claims: Claims,
}

impl Authorized {
    /// The admitted account.
    #[must_use]
    #[inline]
    pub fn account(&self) -> &AccountId {
        self.claims.account()
    }
}

/// The outcome of an admission decision for one request.
#[derive(Debug)]
pub enum Admission {
    /// The path is exempt (`/health`): pass through with no auth and no rate
    /// limit.
    Exempt,
    /// Admitted — proceed with this identity and attach these `X-RateLimit-*`
    /// headers to the response.
    Admitted {
        /// The verified identity.
        identity: Box<Authorized>,
        /// The rate-limit context for the response headers.
        rate_limit: RateLimitDecision,
    },
    /// Rejected with this typed error; attach the rate-limit headers when present.
    Rejected {
        /// The boundary error (`401` / `403` / `429`).
        error: VenueError,
        /// The rate-limit context, when a decision was reached before rejection.
        rate_limit: Option<RateLimitDecision>,
    },
}

/// The shared auth service: JWT verification, the rate limiter, and the account
/// revocation oracle behind one handle every gateway consults
/// ([03 §6](../docs/03-protocol-surfaces.md#6-authentication)). This is the type
/// #012 slots into `AppState`'s auth field.
pub struct AuthService<C> {
    jwt: JwtAuth,
    rate_limiter: RateLimiter<C>,
    revocation: Arc<dyn RevocationOracle>,
}

impl<C> std::fmt::Debug for AuthService<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthService")
            .field("jwt", &self.jwt)
            .field("rate_limiter", &self.rate_limiter)
            .finish_non_exhaustive()
    }
}

impl<C: RateLimitClock> AuthService<C> {
    /// Assembles the auth service from its collaborators.
    #[must_use]
    pub fn new(
        jwt: JwtAuth,
        rate_limiter: RateLimiter<C>,
        revocation: Arc<dyn RevocationOracle>,
    ) -> Self {
        Self {
            jwt,
            rate_limiter,
            revocation,
        }
    }

    /// The JWT service (for the token-issuance route, #013).
    #[must_use]
    #[inline]
    pub fn jwt(&self) -> &JwtAuth {
        &self.jwt
    }

    /// The rate limiter (for periodic [`RateLimiter::sweep_expired`]).
    #[must_use]
    #[inline]
    pub fn rate_limiter(&self) -> &RateLimiter<C> {
        &self.rate_limiter
    }

    /// Whether `path` is fully exempt from auth **and** rate limiting.
    #[must_use]
    #[inline]
    pub fn is_exempt(&self, path: &str) -> bool {
        EXEMPT_PATHS.contains(&path)
    }

    /// The full admission + authorization decision for one request.
    ///
    /// Order (an **admission-first** flow, [03 §6.1](../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)):
    ///
    /// 1. `/health` and other [`EXEMPT_PATHS`] pass through untouched.
    /// 2. The bearer is verified to resolve the rate-limit key (the account, or
    ///    the peer IP when there is no valid token).
    /// 3. The **admission rate limit** is applied on that key — so every request,
    ///    authenticated or not, forbidden or not, counts against one budget
    ///    (`429` when over).
    /// 4. A missing/invalid token is `401`.
    /// 5. The **revocation epoch** is checked (`401` when the token is below the
    ///    account's current epoch, or the account is unknown).
    /// 6. The **required permission** is checked (`403` when lacking).
    #[must_use]
    pub fn admit(
        &self,
        path: &str,
        bearer: Option<&str>,
        peer: IpAddr,
        required: Permission,
    ) -> Admission {
        if self.is_exempt(path) {
            return Admission::Exempt;
        }

        // Resolve identity to pick the rate-limit key. A failed verify falls back
        // to the peer IP (unauthenticated), never leaking the cause. The
        // authenticated bucket includes the token's revocation epoch so a
        // stale-epoch (revoked) token cannot drain a fresh session's budget.
        let verified = bearer.and_then(|token| self.jwt.verify_token(token).ok());
        let key = match &verified {
            Some(claims) => RateLimitKey::Account {
                account: claims.sub.clone(),
                revocation_epoch: claims.revocation_epoch,
            },
            None => RateLimitKey::Peer(peer),
        };

        // Admission gate: rate limit BEFORE authorization, so a forbidden or
        // revoked request still counts against the budget.
        let decision = self.rate_limiter.check_and_record_status(&key);
        if !decision.allowed {
            return Admission::Rejected {
                error: VenueError::RateLimited,
                rate_limit: Some(decision),
            };
        }

        let claims = match verified {
            Some(claims) => claims,
            None => {
                return Admission::Rejected {
                    error: VenueError::Unauthorized,
                    rate_limit: Some(decision),
                };
            }
        };

        // Revocation: refuse a token minted below the account's current epoch, or
        // for an account the registry does not know.
        match self.revocation.current_revocation_epoch(&claims.sub) {
            Some(current) if claims.revocation_epoch >= current => {}
            _ => {
                return Admission::Rejected {
                    error: VenueError::Unauthorized,
                    rate_limit: Some(decision),
                };
            }
        }

        if !claims.has_permission(required) {
            return Admission::Rejected {
                error: VenueError::Forbidden(required),
                rate_limit: Some(decision),
            };
        }

        Admission::Admitted {
            identity: Box::new(Authorized { claims }),
            rate_limit: decision,
        }
    }
}

// ============================================================================
// Axum middleware
// ============================================================================

/// The peer IP for the rate-limit fallback, attached as a request extension by
/// the gateway (#013/#014) from the connection info. Absent in unit tests, where
/// the middleware falls back to `0.0.0.0`.
#[derive(Debug, Clone, Copy)]
pub struct PeerAddr(pub IpAddr);

/// The per-route middleware state: the shared [`AuthService`] plus the
/// [`Permission`] this route group requires. Cloneable (an `Arc` + a `Copy`
/// permission) so it can be an Axum layer state.
#[derive(Debug)]
pub struct AuthGuard<C> {
    service: Arc<AuthService<C>>,
    required: Permission,
}

impl<C> Clone for AuthGuard<C> {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            required: self.required,
        }
    }
}

impl<C> AuthGuard<C> {
    /// Builds a guard requiring `required` for the routes it protects.
    #[must_use]
    pub fn new(service: Arc<AuthService<C>>, required: Permission) -> Self {
        Self { service, required }
    }
}

/// The Axum auth layer: resolves identity, enforces the admission rate limit,
/// checks the revocation epoch, and gates the required [`Permission`]; `/health`
/// passes through untouched. Mount with
/// `axum::middleware::from_fn_with_state(guard, auth_middleware::<C>)`.
///
/// On admission it attaches the [`Authorized`] identity to the request extensions
/// and the `X-RateLimit-*` headers to the response; on rejection it renders the
/// typed [`VenueError`] (`401` / `403` / `429`) with the rate-limit context.
pub async fn auth_middleware<C: RateLimitClock + 'static>(
    State(guard): State<AuthGuard<C>>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_owned();
    let bearer = extract_bearer(request.headers());
    let peer = extract_peer(&request);

    match guard
        .service
        .admit(&path, bearer.as_deref(), peer, guard.required)
    {
        Admission::Exempt => next.run(request).await,
        Admission::Admitted {
            identity,
            rate_limit,
        } => {
            request.extensions_mut().insert(*identity);
            let mut response = next.run(request).await;
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
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Extracts a bearer token from the `Authorization` header, if present and
/// well-formed.
fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?
        .trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

/// Resolves the peer IP from the [`PeerAddr`] extension, falling back to the
/// unspecified address when none is attached.
fn extract_peer(request: &Request) -> IpAddr {
    request
        .extensions()
        .get::<PeerAddr>()
        .map(|peer| peer.0)
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

/// A constant-time byte comparison so a secret match cannot be discovered by
/// timing. Length inequality short-circuits (an acceptable leak for a shared
/// secret); equal-length inputs are compared in full.
#[must_use]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    // ---- fixtures --------------------------------------------------------

    /// The embedded dev auth service — the fixtures MUST parse.
    fn dev_auth() -> JwtAuth {
        match JwtAuth::dev() {
            Ok(auth) => auth,
            Err(error) => panic!("embedded dev fixtures must parse: {error}"),
        }
    }

    fn now_secs() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(error) => panic!("system clock before the Unix epoch: {error}"),
        }
    }

    /// A claim set for `account` with the given permissions, valid for one hour.
    fn valid_claims(account: &str, permissions: Vec<Permission>) -> Claims {
        let now = now_secs();
        Claims::new(AccountId::new(account), permissions, now, now + 3_600, 0)
    }

    fn bootstrap() -> BootstrapGate {
        BootstrapGate::new(Some("operator-secret".to_string()))
    }

    /// Mints a token for `claims`, expecting the gate to pass.
    fn mint(auth: &JwtAuth, claims: &Claims) -> String {
        match auth.mint_token(&bootstrap(), "operator-secret", claims) {
            Ok(token) => token,
            Err(error) => panic!("minting must succeed with the right secret: {error}"),
        }
    }

    /// A controllable venue clock for the rate-limiter tests — advanceable, so the
    /// sliding window can be exercised deterministically.
    #[derive(Clone)]
    struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        fn new(start_ms: u64) -> Self {
            Self(Arc::new(AtomicU64::new(start_ms)))
        }
        fn set(&self, ms: u64) {
            self.0.store(ms, Ordering::SeqCst);
        }
    }

    impl RateLimitClock for TestClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// A map-backed revocation oracle for tests.
    struct MapRevocation(std::collections::HashMap<AccountId, u64>);

    impl RevocationOracle for MapRevocation {
        fn current_revocation_epoch(&self, account: &AccountId) -> Option<u64> {
            self.0.get(account).copied()
        }
    }

    fn service_for(
        account: &str,
        epoch: u64,
        clock: TestClock,
        limit: u32,
    ) -> AuthService<TestClock> {
        let mut epochs = std::collections::HashMap::new();
        epochs.insert(AccountId::new(account), epoch);
        AuthService::new(
            dev_auth(),
            RateLimiter::new(clock, limit),
            Arc::new(MapRevocation(epochs)),
        )
    }

    // ---- mint / verify ---------------------------------------------------

    #[test]
    fn test_mint_token_verify_token_roundtrips_claims() {
        let auth = dev_auth();
        let claims = valid_claims("acct-1", vec![Permission::Trade]);
        let token = mint(&auth, &claims);
        match auth.verify_token(&token) {
            Ok(decoded) => {
                assert_eq!(decoded.sub, AccountId::new("acct-1"));
                assert_eq!(decoded.permissions, vec![Permission::Trade]);
            }
            Err(error) => panic!("a freshly-minted token must verify: {error}"),
        }
    }

    #[test]
    fn test_verify_token_tampered_signature_is_unauthorized() {
        let auth = dev_auth();
        let token = mint(&auth, &valid_claims("acct-1", vec![Permission::Read]));
        // Flip the final signature character.
        let mut tampered = token.clone();
        let last = match tampered.pop() {
            Some(ch) => ch,
            None => panic!("token is non-empty"),
        };
        tampered.push(if last == 'A' { 'B' } else { 'A' });
        assert!(matches!(
            auth.verify_token(&tampered),
            Err(VenueError::Unauthorized)
        ));
    }

    #[test]
    fn test_verify_token_expired_is_unauthorized() {
        let auth = dev_auth();
        let now = now_secs();
        // Expired well beyond the default 60 s leeway.
        let claims = Claims::new(
            AccountId::new("acct-1"),
            vec![Permission::Read],
            now - 7_200,
            now - 3_600,
            0,
        );
        let token = mint(&auth, &claims);
        assert!(matches!(
            auth.verify_token(&token),
            Err(VenueError::Unauthorized)
        ));
    }

    #[test]
    fn test_verify_token_alg_none_is_unauthorized() {
        // A crafted `alg:none` token (header for {"alg":"none","typ":"JWT"}, an
        // empty `{}` payload, empty signature) must be rejected — the verifier
        // pins RS256 and never accepts an unsigned token.
        let auth = dev_auth();
        let alg_none = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.e30.";
        assert!(matches!(
            auth.verify_token(alg_none),
            Err(VenueError::Unauthorized)
        ));
    }

    #[test]
    fn test_verify_token_hs256_confusion_is_unauthorized() {
        // An HS256 token signed with an attacker secret must be rejected by the
        // RS256-pinned verifier (algorithm-confusion defense).
        let auth = dev_auth();
        let claims = valid_claims("acct-1", vec![Permission::Admin]);
        let forged = match encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"attacker-secret"),
        ) {
            Ok(token) => token,
            Err(error) => panic!("HS256 encode should succeed: {error}"),
        };
        assert!(matches!(
            auth.verify_token(&forged),
            Err(VenueError::Unauthorized)
        ));
    }

    // ---- permission implication ------------------------------------------

    #[test]
    fn test_has_permission_admin_grants_read_and_trade() {
        let claims = valid_claims("acct-1", vec![Permission::Admin]);
        assert!(claims.has_permission(Permission::Read));
        assert!(claims.has_permission(Permission::Trade));
        assert!(claims.has_permission(Permission::Admin));
    }

    #[test]
    fn test_has_permission_read_refused_trade_and_admin() {
        let claims = valid_claims("acct-1", vec![Permission::Read]);
        assert!(claims.has_permission(Permission::Read));
        assert!(!claims.has_permission(Permission::Trade));
        assert!(!claims.has_permission(Permission::Admin));
    }

    #[test]
    fn test_has_permission_trade_grants_read_not_admin() {
        let claims = valid_claims("acct-1", vec![Permission::Trade]);
        assert!(claims.has_permission(Permission::Read));
        assert!(claims.has_permission(Permission::Trade));
        assert!(!claims.has_permission(Permission::Admin));
    }

    #[test]
    fn test_permission_grants_full_matrix() {
        use Permission::{Admin, Read, Trade};
        // (holder, required, expected)
        let cases = [
            (Read, Read, true),
            (Read, Trade, false),
            (Read, Admin, false),
            (Trade, Read, true),
            (Trade, Trade, true),
            (Trade, Admin, false),
            (Admin, Read, true),
            (Admin, Trade, true),
            (Admin, Admin, true),
        ];
        for (holder, required, expected) in cases {
            assert_eq!(
                holder.grants(required),
                expected,
                "{holder:?}.grants({required:?})"
            );
        }
    }

    // ---- bootstrap-secret gate -------------------------------------------

    #[test]
    fn test_mint_token_requires_bootstrap_secret() {
        let auth = dev_auth();
        let claims = valid_claims("acct-1", vec![Permission::Read]);

        // Disabled gate: no issuance.
        let disabled = BootstrapGate::new(None);
        assert_eq!(
            auth.mint_token(&disabled, "anything", &claims),
            Err(AuthError::BootstrapDisabled)
        );

        // Wrong secret.
        assert_eq!(
            auth.mint_token(&bootstrap(), "wrong-secret", &claims),
            Err(AuthError::BootstrapMismatch)
        );

        // Right secret mints.
        assert!(
            auth.mint_token(&bootstrap(), "operator-secret", &claims)
                .is_ok()
        );
    }

    // ---- dev-key release gate --------------------------------------------

    #[test]
    fn test_dev_key_release_gate_refuses_without_dev_mode() {
        let dev = dev_auth();
        assert!(dev.is_dev());
        // Dev mode disabled: refused.
        assert_eq!(
            dev.release_gated(DevMode::from_flag(false)).map(|_| ()),
            Err(AuthError::DevKeyRefused)
        );
    }

    #[test]
    fn test_dev_key_release_gate_admits_with_dev_mode() {
        let dev = dev_auth();
        match dev.release_gated(DevMode::from_flag(true)) {
            Ok(auth) => assert!(auth.is_dev()),
            Err(error) => panic!("dev mode should admit dev keys: {error}"),
        }
    }

    #[test]
    fn test_release_gate_passes_a_production_keypair_when_disabled() {
        // A non-dev keypair always passes the gate, even with dev mode disabled.
        let prod = match JwtAuth::from_pem(DEV_CERT_PEM.as_bytes(), DEV_KEY_PEM.as_bytes()) {
            Ok(auth) => auth,
            Err(error) => panic!("from_pem should build: {error}"),
        };
        assert!(!prod.is_dev());
        assert!(prod.release_gated(DevMode::from_flag(false)).is_ok());
    }

    // ---- admission: /health exemption ------------------------------------

    #[test]
    fn test_admit_health_path_is_exempt_from_auth_and_rate_limit() {
        let clock = TestClock::new(1_000);
        // Limit of 0 would reject any counted request; /health must still pass.
        let service = service_for("acct-1", 0, clock, 0);
        match service.admit(
            "/health",
            None,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Permission::Admin,
        ) {
            Admission::Exempt => {}
            other => panic!("/health must be exempt, got {other:?}"),
        }
    }

    // ---- admission: 401 / 403 / 429 --------------------------------------

    #[test]
    fn test_admit_missing_token_is_unauthorized() {
        let clock = TestClock::new(1_000);
        let service = service_for("acct-1", 0, clock, 100);
        match service.admit(
            "/api/v1/orders",
            None,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Permission::Read,
        ) {
            Admission::Rejected { error, .. } => {
                assert!(matches!(error, VenueError::Unauthorized));
            }
            other => panic!("a missing token must be unauthorized, got {other:?}"),
        }
    }

    #[test]
    fn test_admit_insufficient_permission_is_forbidden() {
        let clock = TestClock::new(1_000);
        let service = service_for("acct-1", 0, clock, 100);
        let token = mint(
            service.jwt(),
            &valid_claims("acct-1", vec![Permission::Read]),
        );
        match service.admit(
            "/api/v1/orders",
            Some(&token),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Permission::Trade,
        ) {
            Admission::Rejected { error, .. } => {
                assert!(matches!(error, VenueError::Forbidden(Permission::Trade)));
            }
            other => panic!("a Read token must be forbidden Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_admit_over_limit_is_rate_limited_with_headers() {
        let clock = TestClock::new(1_000);
        let service = service_for("acct-1", 0, clock, 1);
        let token = mint(
            service.jwt(),
            &valid_claims("acct-1", vec![Permission::Trade]),
        );
        let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);

        // First request (budget 1) is admitted.
        match service.admit("/api/v1/orders", Some(&token), peer, Permission::Trade) {
            Admission::Admitted { rate_limit, .. } => {
                assert_eq!(rate_limit.limit, 1);
                assert_eq!(rate_limit.remaining, 0);
            }
            other => panic!("first request must be admitted, got {other:?}"),
        }
        // Second request within the window is throttled.
        match service.admit("/api/v1/orders", Some(&token), peer, Permission::Trade) {
            Admission::Rejected {
                error: VenueError::RateLimited,
                rate_limit: Some(decision),
            } => {
                assert!(!decision.allowed);
                assert_eq!(decision.remaining, 0);
                assert!(decision.retry_after_ms.is_some());
            }
            other => panic!("second request must be rate limited, got {other:?}"),
        }
    }

    #[test]
    fn test_admit_revoked_token_is_unauthorized() {
        let clock = TestClock::new(1_000);
        // The account's current epoch is 5; a token minted at epoch 0 is stale.
        let service = service_for("acct-1", 5, clock, 100);
        let stale = Claims::new(
            AccountId::new("acct-1"),
            vec![Permission::Trade],
            now_secs(),
            now_secs() + 3_600,
            0,
        );
        let token = mint(service.jwt(), &stale);
        match service.admit(
            "/api/v1/orders",
            Some(&token),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Permission::Trade,
        ) {
            Admission::Rejected { error, .. } => {
                assert!(matches!(error, VenueError::Unauthorized));
            }
            other => panic!("a revoked token must be unauthorized, got {other:?}"),
        }
    }

    #[test]
    fn test_admit_unknown_account_is_unauthorized() {
        let clock = TestClock::new(1_000);
        // The registry knows only "acct-1"; a token for "ghost" is refused.
        let service = service_for("acct-1", 0, clock, 100);
        let token = mint(
            service.jwt(),
            &valid_claims("ghost", vec![Permission::Admin]),
        );
        match service.admit(
            "/api/v1/orders",
            Some(&token),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Permission::Read,
        ) {
            Admission::Rejected { error, .. } => {
                assert!(matches!(error, VenueError::Unauthorized));
            }
            other => panic!("an unknown account must be unauthorized, got {other:?}"),
        }
    }

    #[test]
    fn test_admit_stale_epoch_token_buckets_separately_from_fresh() {
        // A revoked-but-signed token (stale epoch) must NOT drain the budget the
        // owner's freshly re-authenticated session needs: the two bucket apart on
        // (account, revocation_epoch).
        let clock = TestClock::new(1_000);
        // Account current epoch = 5, per-window budget = 1.
        let service = service_for("acct-1", 5, clock, 1);
        let jwt = service.jwt();
        let stale = mint(
            jwt,
            &Claims::new(
                AccountId::new("acct-1"),
                vec![Permission::Trade],
                now_secs(),
                now_secs() + 3_600,
                0,
            ),
        );
        let fresh = mint(
            jwt,
            &Claims::new(
                AccountId::new("acct-1"),
                vec![Permission::Trade],
                now_secs(),
                now_secs() + 3_600,
                5,
            ),
        );
        let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);

        // First stale request: the (account, 0) slot is consumed, then revocation
        // rejects it (401).
        match service.admit("/api/v1/orders", Some(&stale), peer, Permission::Trade) {
            Admission::Rejected {
                error: VenueError::Unauthorized,
                ..
            } => {}
            other => panic!("a stale-epoch token must be 401, got {other:?}"),
        }
        // Second stale request: now throttled on its own (account, 0) bucket.
        match service.admit("/api/v1/orders", Some(&stale), peer, Permission::Trade) {
            Admission::Rejected {
                error: VenueError::RateLimited,
                ..
            } => {}
            other => panic!("the second stale request must be 429, got {other:?}"),
        }
        // The FRESH token still has its full budget on (account, 5) — the stale
        // flood did not starve it.
        match service.admit("/api/v1/orders", Some(&fresh), peer, Permission::Trade) {
            Admission::Admitted { .. } => {}
            other => panic!("the fresh token must be admitted despite the flood, got {other:?}"),
        }
    }

    // ---- rate limiter units ----------------------------------------------

    /// An authenticated bucket key for `account` at revocation epoch `0`.
    fn acct_key(account: &str) -> RateLimitKey {
        RateLimitKey::Account {
            account: AccountId::new(account),
            revocation_epoch: 0,
        }
    }

    /// A peer bucket key for `10.0.0.n`.
    fn peer_key(n: u8) -> RateLimitKey {
        RateLimitKey::Peer(IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, n)))
    }

    #[test]
    fn test_rate_limiter_allows_up_to_limit_then_throttles() {
        let clock = TestClock::new(0);
        let limiter = RateLimiter::new(clock, 3);
        let key = acct_key("acct-1");
        for expected_remaining in [2u32, 1, 0] {
            let decision = limiter.check_and_record_status(&key);
            assert!(decision.allowed);
            assert_eq!(decision.remaining, expected_remaining);
        }
        let denied = limiter.check_and_record_status(&key);
        assert!(!denied.allowed);
        assert_eq!(denied.remaining, 0);
    }

    #[test]
    fn test_rate_limiter_sliding_window_frees_after_window() {
        let clock = TestClock::new(0);
        let limiter = RateLimiter::new(clock.clone(), 1);
        let key = acct_key("acct-1");

        assert!(limiter.check_and_record_status(&key).allowed);
        assert!(!limiter.check_and_record_status(&key).allowed);

        // Advance past the 60 s window: the old timestamp expires, budget frees.
        clock.set(RATE_LIMIT_WINDOW_MS);
        assert!(limiter.check_and_record_status(&key).allowed);
    }

    #[test]
    fn test_rate_limiter_keys_are_independent() {
        let clock = TestClock::new(0);
        let limiter = RateLimiter::new(clock, 1);
        let a = acct_key("acct-a");
        let b = acct_key("acct-b");
        let ip = RateLimitKey::Peer(IpAddr::V4(Ipv4Addr::LOCALHOST));
        // Each key gets its own budget of 1.
        assert!(limiter.check_and_record_status(&a).allowed);
        assert!(limiter.check_and_record_status(&b).allowed);
        assert!(limiter.check_and_record_status(&ip).allowed);
        // And each is independently exhausted.
        assert!(!limiter.check_and_record_status(&a).allowed);
    }

    #[test]
    fn test_rate_limiter_sweep_reclaims_expired_keys() {
        let clock = TestClock::new(0);
        let limiter = RateLimiter::new(clock.clone(), 5);
        let key = acct_key("acct-1");
        let _ = limiter.check_and_record_status(&key);
        assert_eq!(limiter.tracked_keys(), 1);
        clock.set(RATE_LIMIT_WINDOW_MS + 1);
        limiter.sweep_expired();
        assert_eq!(limiter.tracked_keys(), 0);
    }

    #[test]
    fn test_rate_limiter_key_space_is_bounded_and_fails_closed() {
        // A key-space ceiling of 2: a third distinct key must FAIL CLOSED (deny),
        // never grow the tracked key-space past the cap (a DoS control).
        let clock = TestClock::new(0);
        let limiter = RateLimiter::with_capacity(clock.clone(), 10, RATE_LIMIT_WINDOW_MS, 2);

        assert!(limiter.check_and_record_status(&peer_key(1)).allowed);
        assert!(limiter.check_and_record_status(&peer_key(2)).allowed);
        assert_eq!(limiter.tracked_keys(), 2);

        // The third distinct key is refused without being tracked.
        let denied = limiter.check_and_record_status(&peer_key(3));
        assert!(!denied.allowed);
        assert!(denied.retry_after_ms.is_some());
        assert_eq!(
            limiter.tracked_keys(),
            2,
            "key-space must not grow past the cap"
        );

        // An EXISTING key still proceeds within its own budget.
        assert!(limiter.check_and_record_status(&peer_key(1)).allowed);
        assert_eq!(limiter.tracked_keys(), 2);

        // Once the window passes, the opportunistic inline sweep reclaims the
        // expired buckets, so a new key is admitted again — the cap is not a
        // permanent lockout.
        clock.set(RATE_LIMIT_WINDOW_MS + 1);
        assert!(limiter.check_and_record_status(&peer_key(3)).allowed);
        assert!(limiter.tracked_keys() <= 2);
    }

    #[test]
    fn test_rate_limit_decision_writes_headers() {
        let decision = RateLimitDecision {
            allowed: false,
            limit: 100,
            remaining: 0,
            reset_ms: 60_000,
            retry_after_ms: Some(60_000),
        };
        let mut headers = HeaderMap::new();
        decision.apply_headers(&mut headers);
        assert_eq!(
            headers
                .get(&HEADER_RATELIMIT_LIMIT)
                .map(|v| v.to_str().ok()),
            Some(Some("100"))
        );
        assert_eq!(
            headers
                .get(&HEADER_RATELIMIT_REMAINING)
                .map(|v| v.to_str().ok()),
            Some(Some("0"))
        );
        assert_eq!(
            headers
                .get(&HEADER_RATELIMIT_RESET)
                .map(|v| v.to_str().ok()),
            Some(Some("60000"))
        );
        assert_eq!(
            headers.get(header::RETRY_AFTER).map(|v| v.to_str().ok()),
            Some(Some("60"))
        );
    }

    // ---- determinism: venue-clock decisions are replay-stable ------------

    #[test]
    fn test_rate_limiter_decisions_are_replay_stable_on_venue_clock() {
        // Two independent limiters driven by the SAME scripted venue-clock timeline
        // must produce byte-identical decision sequences — the substrate of
        // deterministic ingress ordering (03 §6.1).
        let timeline = [0u64, 0, 0, 10_000, 10_000, 61_000, 61_000, 61_000];
        let run = || {
            let clock = TestClock::new(0);
            let limiter = RateLimiter::with_window(clock.clone(), 2, RATE_LIMIT_WINDOW_MS);
            let key = acct_key("acct-1");
            let mut outcomes = Vec::new();
            for &tick in &timeline {
                clock.set(tick);
                outcomes.push(limiter.check_and_record_status(&key).allowed);
            }
            outcomes
        };
        assert_eq!(run(), run());
    }

    // ---- secret redaction -------------------------------------------------

    #[test]
    fn test_jwt_auth_debug_redacts_key_material() {
        let rendered = format!("{:?}", dev_auth());
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("PRIVATE KEY"));
        assert!(!rendered.contains("MIIEvwIB"));
    }

    #[test]
    fn test_bootstrap_gate_debug_redacts_secret() {
        let gate = BootstrapGate::new(Some("operator-secret".to_string()));
        let rendered = format!("{gate:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("operator-secret"));
    }

    #[test]
    fn test_constant_time_eq_matches_only_equal_bytes() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secre"));
    }
}
