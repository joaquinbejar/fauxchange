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
use std::sync::{Arc, OnceLock};

use argon2::password_hash::SaltString;
use argon2::{
    Algorithm as Argon2Algorithm, Argon2, Params as Argon2Params, PasswordHash,
    PasswordHasher as _, PasswordVerifier as _, Version as Argon2Version,
};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};

use crate::error::VenueError;
use crate::exchange::{FixedClock, Hash32, VenueClock};
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
    /// Argon2id hashing or verification failed for a non-credential reason (a
    /// malformed stored hash, an invalid parameter set). The cause is **redacted**
    /// — this variant never carries the plaintext, the hash, or the pepper
    /// ([06 §8](../docs/06-deployment.md#8-auth-bootstrap)).
    #[error("password hashing failed")]
    PasswordHash,
    /// Account provisioning was rejected. Carries a **non-secret** label
    /// (`"duplicate account id"` / `"duplicate FIX username"`) — never a
    /// credential.
    #[error("account provisioning failed: {0}")]
    Provisioning(&'static str),
    /// Bootstrap minting selected an account the registry does not know. Returned
    /// **only after** the bootstrap secret has cleared, so it cannot be used to
    /// enumerate accounts pre-authentication.
    #[error("no such account")]
    UnknownAccount,
    /// The requested token lifetime (`issued_at + ttl`) overflowed `u64` seconds —
    /// an invalid, unmintable lifetime (never reached for real clocks / TTLs).
    #[error("invalid token lifetime")]
    TokenLifetime,
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
// The venue account model (registry-internal)
// ============================================================================
//
// This is the registry-internal account entity — NOT the public wire projection
// (that is `models::Account`, which never carries credentials). The credentials
// here (the Argon2id password hash, the pepper) are secrets: the `password_hash`
// is `#[serde(skip_serializing)]` so it never reaches the wire, and the redacting
// `Debug` impls keep it out of any log or error
// ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions),
// [ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md)).

/// The pinned Argon2id **memory** cost in KiB — the OWASP baseline
/// ([06 §8](../docs/06-deployment.md#8-auth-bootstrap)). Equal to
/// [`argon2::Params::DEFAULT_M_COST`].
pub const ARGON2_M_COST_KIB: u32 = 19_456;
/// The pinned Argon2id **iteration** count (time cost) — the OWASP baseline.
pub const ARGON2_T_COST: u32 = 2;
/// The pinned Argon2id **parallelism** (lanes) — the OWASP baseline.
pub const ARGON2_P_COST: u32 = 1;

/// The default bootstrap-token lifetime, in **seconds** (one hour). The live
/// per-issuance value is venue config (#046); this is the bounded default.
pub const DEFAULT_TOKEN_TTL_SECS: u64 = 3_600;

/// A non-secret, fixed plaintext hashed once to give the unknown-username FIX
/// login path a real Argon2 verification to run against, so a wrong username and
/// a wrong password cost the **same** time (no user-enumeration timing oracle).
/// It is not a credential and matches no account.
const FIX_LOGIN_TIMING_DUMMY: &str = "fauxchange-fix-login-timing-equalisation-dummy";

/// The immutable FIX session identity bound to an account at provisioning — the
/// `(SenderCompID, TargetCompID)` tuple ([ADR-0010](../docs/adr/0010-fix-session-account-binding.md)).
///
/// **Declared now, enforced from v0.4.** The type is carried on [`Credentials`]
/// so the account model is schema-complete, but the acceptor that pins a session
/// to this binding (and rejects a logon whose comp-ids do not match) lands with
/// the FIX gateway (#038). It changes no REST/WS behaviour today.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompIdBinding {
    /// The counterparty's `SenderCompID (49)` on an inbound message (the client).
    pub sender_comp_id: String,
    /// The venue's `TargetCompID (56)` on an inbound message (this acceptor).
    pub target_comp_id: String,
}

/// The credentials that resolve to an account across surfaces
/// ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions)).
///
/// There is **no JWT secret here**: a JWT is verified by the RS256 public key and
/// its `sub` **is** [`Account::id`], so the JWT path is a direct [`AccountId`]
/// lookup. The **only** stored credential is the FIX password, kept solely as an
/// Argon2id PHC string ([`Credentials::password_hash`]); the plaintext is never
/// persisted. `password_hash` is `#[serde(skip_serializing)]` — it can never
/// leave the venue on the wire — and the [`std::fmt::Debug`] impl redacts it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    /// The FIX `Logon` `Username (553)` that indexes this account, when the
    /// account may log in over FIX. `None` for a REST/WS-only account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fix_username: Option<String>,
    /// The FIX password as an **Argon2id PHC string** (`$argon2id$...`), never the
    /// plaintext. Skipped on serialize so it never reaches the wire; defaulted on
    /// deserialize so a wire-projected [`Account`] round-trips.
    #[serde(default, skip_serializing)]
    pub password_hash: Option<String>,
    /// The immutable FIX `(SenderCompID, TargetCompID)` binding (ADR-0010) —
    /// declared now, enforced from v0.4 by the acceptor (#038).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fix_comp_ids: Option<CompIdBinding>,
}

impl std::fmt::Debug for Credentials {
    /// Redacts the password hash — it never appears in a log or error
    /// ([06 §8](../docs/06-deployment.md#8-auth-bootstrap),
    /// [08 §7](../docs/08-threat-model.md#7-secrets-handling)).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("fix_username", &self.fix_username)
            .field(
                "password_hash",
                &self.password_hash.as_ref().map(|_| "<redacted>"),
            )
            .field("fix_comp_ids", &self.fix_comp_ids)
            .finish()
    }
}

/// A venue account — the registry-internal entity keyed by [`AccountId`]
/// ([01 §8](../docs/01-domain-model.md#8-accounts-and-sessions),
/// [ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md)).
///
/// [`Account::id`] **is** the JWT `sub`; [`Account::owner`] is the [`Hash32`] the
/// matching engine keys on for self-trade prevention and per-user mass cancel;
/// [`Account::revocation_epoch`] is bumped by [`AccountRegistry::revoke`] to drop
/// outstanding tokens. Its `Debug` is safe to log — the credential hash is
/// redacted by [`Credentials`]'s `Debug`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    /// The account identity — the JWT `sub`.
    pub id: AccountId,
    /// The STP / mass-cancel owner hash the matching engine keys on.
    pub owner: Hash32,
    /// The registered permission set (`Admin` implies `Read` + `Trade`).
    pub permissions: Vec<Permission>,
    /// The credentials resolving to this account (FIX password hash only).
    pub credentials: Credentials,
    /// The revocation epoch; a token/logon minted below it is refused.
    pub revocation_epoch: u64,
}

/// The explicit input to provision one account into the [`AccountRegistry`].
///
/// The seed-manifest **format** (parsing config into these) is #024; here the
/// registry takes an explicit list. The FIX password is supplied as **plaintext**
/// and hashed with Argon2id at provisioning — it is dropped immediately after and
/// never stored. The plaintext is redacted in [`std::fmt::Debug`].
#[derive(Clone, PartialEq, Eq)]
pub struct AccountProvision {
    /// The account identity (the JWT `sub`).
    pub id: AccountId,
    /// The STP / mass-cancel owner hash.
    pub owner: Hash32,
    /// The registered permission set.
    pub permissions: Vec<Permission>,
    /// The FIX `Username (553)`, when the account may log in over FIX.
    pub fix_username: Option<String>,
    /// The FIX password in **plaintext** — hashed at provisioning, then dropped.
    /// `None` for a REST/WS-only account (which has no stored credential).
    pub fix_password: Option<String>,
    /// The immutable FIX comp-id binding (ADR-0010; enforced from v0.4).
    pub fix_comp_ids: Option<CompIdBinding>,
}

impl AccountProvision {
    /// A minimal REST/WS-only provision: an account with permissions and an owner
    /// hash, no FIX credential.
    #[must_use]
    pub fn new(id: AccountId, owner: Hash32, permissions: Vec<Permission>) -> Self {
        Self {
            id,
            owner,
            permissions,
            fix_username: None,
            fix_password: None,
            fix_comp_ids: None,
        }
    }

    /// Adds a FIX `Username (553)` + plaintext password to this provision (hashed
    /// at provisioning).
    #[must_use]
    pub fn with_fix_login(
        mut self,
        fix_username: impl Into<String>,
        fix_password: impl Into<String>,
    ) -> Self {
        self.fix_username = Some(fix_username.into());
        self.fix_password = Some(fix_password.into());
        self
    }

    /// Adds the immutable FIX comp-id binding (ADR-0010).
    #[must_use]
    pub fn with_comp_ids(mut self, binding: CompIdBinding) -> Self {
        self.fix_comp_ids = Some(binding);
        self
    }
}

impl std::fmt::Debug for AccountProvision {
    /// Redacts the plaintext FIX password — a provisioning input is never logged
    /// with its secret ([06 §8](../docs/06-deployment.md#8-auth-bootstrap)).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountProvision")
            .field("id", &self.id)
            .field("owner", &self.owner)
            .field("permissions", &self.permissions)
            .field("fix_username", &self.fix_username)
            .field(
                "fix_password",
                &self.fix_password.as_ref().map(|_| "<redacted>"),
            )
            .field("fix_comp_ids", &self.fix_comp_ids)
            .finish()
    }
}

// ============================================================================
// Argon2id password hashing (pinned OWASP parameters + optional pepper)
// ============================================================================

/// The outcome of verifying a FIX password against a stored Argon2id hash.
///
/// [`PasswordVerification::VerifiedRehash`] carries a **fresh** hash at the
/// current pinned parameters when the stored hash used weaker ones (the
/// rehash-on-verify policy, [06 §8](../docs/06-deployment.md#8-auth-bootstrap));
/// the caller persists it. Its `Debug` **redacts** the fresh hash.
#[derive(Clone, PartialEq, Eq)]
pub enum PasswordVerification {
    /// The password matched and the stored hash already meets the pinned
    /// parameters — nothing to persist.
    Verified,
    /// The password matched, but the stored hash used **weaker** parameters; here
    /// is a fresh hash at the pinned parameters to store in its place.
    VerifiedRehash(String),
    /// The password did not match.
    Rejected,
}

impl std::fmt::Debug for PasswordVerification {
    /// Redacts the rehash PHC string — a hash never appears in a log or error.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Verified => f.write_str("Verified"),
            Self::VerifiedRehash(_) => f.write_str("VerifiedRehash(<redacted>)"),
            Self::Rejected => f.write_str("Rejected"),
        }
    }
}

/// Argon2**id** password hashing pinned to the OWASP baseline
/// ([`ARGON2_M_COST_KIB`] / [`ARGON2_T_COST`] / [`ARGON2_P_COST`]) with an
/// optional server-side **pepper** ([06 §8](../docs/06-deployment.md#8-auth-bootstrap)).
///
/// The variant is always Argon2id (never Argon2i/Argon2d) at version `0x13`. The
/// pepper is an Argon2 **secret** (keyed hash) never written to the PHC string, so
/// a leaked hash cannot be attacked offline without it; it is redacted in `Debug`
/// and never logged. Verification is constant-time (Argon2's own comparison).
pub struct Argon2Hasher {
    /// The optional pepper (Argon2 secret input). Empty is treated as absent.
    pepper: Option<Vec<u8>>,
    /// Memory cost (KiB).
    m_cost: u32,
    /// Time cost (iterations).
    t_cost: u32,
    /// Parallelism (lanes).
    p_cost: u32,
}

impl std::fmt::Debug for Argon2Hasher {
    /// Redacts the pepper — never prints or hints at its value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Argon2Hasher")
            .field("algorithm", &"Argon2id")
            .field("m_cost", &self.m_cost)
            .field("t_cost", &self.t_cost)
            .field("p_cost", &self.p_cost)
            .field("pepper", &self.pepper.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Argon2Hasher {
    /// Builds a hasher at the pinned OWASP parameters with an optional pepper (an
    /// empty pepper is treated as absent).
    #[must_use]
    pub fn new(pepper: Option<Vec<u8>>) -> Self {
        Self {
            pepper: pepper.filter(|bytes| !bytes.is_empty()),
            m_cost: ARGON2_M_COST_KIB,
            t_cost: ARGON2_T_COST,
            p_cost: ARGON2_P_COST,
        }
    }

    /// Builds a hasher, reading the optional pepper from `AUTH_PASSWORD_PEPPER`
    /// (an unset or empty value means no pepper). The pepper is never persisted
    /// with the hash ([06 §8](../docs/06-deployment.md#8-auth-bootstrap)).
    #[must_use]
    pub fn from_env() -> Self {
        let pepper = std::env::var("AUTH_PASSWORD_PEPPER")
            .ok()
            .filter(|value| !value.is_empty())
            .map(String::into_bytes);
        Self::new(pepper)
    }

    /// The pinned parameter set.
    fn params(&self) -> Result<Argon2Params, AuthError> {
        Argon2Params::new(self.m_cost, self.t_cost, self.p_cost, None)
            .map_err(|_| AuthError::PasswordHash)
    }

    /// An Argon2id engine at the pinned parameters, keyed with the pepper when set.
    fn engine(&self) -> Result<Argon2<'_>, AuthError> {
        let params = self.params()?;
        match &self.pepper {
            Some(secret) => Argon2::new_with_secret(
                secret,
                Argon2Algorithm::Argon2id,
                Argon2Version::V0x13,
                params,
            )
            .map_err(|_| AuthError::PasswordHash),
            None => Ok(Argon2::new(
                Argon2Algorithm::Argon2id,
                Argon2Version::V0x13,
                params,
            )),
        }
    }

    /// Hashes `plaintext` into an Argon2id PHC string with a fresh random salt.
    ///
    /// # Errors
    ///
    /// [`AuthError::PasswordHash`] if hashing fails — the cause is redacted and
    /// never carries the plaintext.
    pub fn hash(&self, plaintext: &str) -> Result<String, AuthError> {
        let salt = SaltString::generate(&mut OsRng);
        let engine = self.engine()?;
        engine
            .hash_password(plaintext.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|_| AuthError::PasswordHash)
    }

    /// Verifies `plaintext` against a stored Argon2id PHC string in constant time,
    /// returning whether it matched and — on a match against a **weaker** stored
    /// parameter set — a fresh hash to persist (rehash-on-verify).
    ///
    /// # Errors
    ///
    /// [`AuthError::PasswordHash`] only when the **stored** hash is malformed (an
    /// operator/data error, not a wrong password); a wrong password is the
    /// non-error [`PasswordVerification::Rejected`]. The cause is redacted.
    pub fn verify(
        &self,
        plaintext: &str,
        stored_phc: &str,
    ) -> Result<PasswordVerification, AuthError> {
        let parsed = PasswordHash::new(stored_phc).map_err(|_| AuthError::PasswordHash)?;
        let engine = self.engine()?;
        match engine.verify_password(plaintext.as_bytes(), &parsed) {
            Ok(()) if self.needs_rehash(&parsed) => {
                Ok(PasswordVerification::VerifiedRehash(self.hash(plaintext)?))
            }
            Ok(()) => Ok(PasswordVerification::Verified),
            Err(argon2::password_hash::Error::Password) => Ok(PasswordVerification::Rejected),
            Err(_) => Err(AuthError::PasswordHash),
        }
    }

    /// Whether a stored hash's parameters are **weaker** than the pinned baseline
    /// (so a successful verify should trigger a rehash). Unparseable parameters
    /// are treated as weaker.
    fn needs_rehash(&self, parsed: &PasswordHash<'_>) -> bool {
        match Argon2Params::try_from(parsed) {
            Ok(stored) => {
                stored.m_cost() < self.m_cost
                    || stored.t_cost() < self.t_cost
                    || stored.p_cost() < self.p_cost
            }
            Err(_) => true,
        }
    }
}

// ============================================================================
// The venue account registry
// ============================================================================

/// A successful/failed FIX logon resolution against the registry (schema-ready;
/// the acceptor that consumes it is v0.4, #038).
///
/// An unknown username, an account with no FIX credential, and a wrong password
/// are **all** [`FixLoginOutcome::Rejected`] — deliberately indistinguishable, so
/// the outcome cannot be used to enumerate accounts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixLoginOutcome {
    /// The password matched: bind the session to this account with these
    /// permissions.
    Authenticated {
        /// The resolved account (the JWT `sub` — one identity behind both paths).
        account: AccountId,
        /// The account's registered permission set.
        permissions: Vec<Permission>,
    },
    /// The logon is refused (unknown user, no credential, or wrong password).
    Rejected,
}

/// The storage contract the account registry satisfies — the **drop-in seam** for
/// the PostgreSQL-backed `accounts` store (v0.2, #023/#024). The in-memory
/// [`AccountRegistry`] is the default backend; a future PG backend implements the
/// same trait and slots in behind the same `AppState` accessor without changing
/// any gateway ([ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md)).
///
/// It extends [`RevocationOracle`] (the middleware seam), so one handle answers
/// both "what is this account's epoch?" and the resolution/verification/revocation
/// the token- and (future) logon-paths need.
pub trait AccountStore: RevocationOracle {
    /// Resolves the account behind a JWT `sub` (a direct [`AccountId`] lookup).
    #[must_use]
    fn account(&self, id: &AccountId) -> Option<Account>;
    /// Resolves the account behind a FIX `Username (553)` — the **same**
    /// [`AccountId`] the JWT path resolves.
    #[must_use]
    fn account_by_fix_username(&self, fix_username: &str) -> Option<Account>;
    /// Verifies a FIX password for `fix_username` (constant-time), persisting a
    /// rehash-on-verify when the stored parameters were weaker.
    #[must_use]
    fn verify_fix_password(&self, fix_username: &str, password: &str) -> FixLoginOutcome;
    /// Bumps the account's revocation epoch (refusing its outstanding tokens on
    /// the next request), returning the new epoch, or `None` if unknown.
    fn revoke(&self, id: &AccountId) -> Option<u64>;
    /// The number of provisioned accounts.
    #[must_use]
    fn account_count(&self) -> usize;

    /// Whether the store holds no accounts.
    #[must_use]
    fn is_empty(&self) -> bool {
        self.account_count() == 0
    }
}

/// The venue account registry: the single source of truth both credential paths
/// resolve to ([ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md)).
///
/// It indexes accounts by [`AccountId`] (the JWT `sub` — a direct lookup) and by
/// FIX `Username (553)`, both resolving to **one** account row and permission set.
/// It is **in-memory** (the default backend; the PostgreSQL path is the same
/// [`AccountStore`] contract, v0.2) and owned by
/// [`AppState`](crate::state::AppState); every gateway reaches it through that.
///
/// # Determinism / concurrency
///
/// Backed by [`DashMap`] for sharded lock-free point access; a password
/// verification clones the stored hash out and **drops the shard guard before**
/// the (CPU-bound) Argon2 computation, so a verify never holds a lock across the
/// hash. Provisioning is a seed-time single-writer step.
pub struct AccountRegistry {
    /// The Argon2id hasher (pinned parameters + optional pepper).
    hasher: Argon2Hasher,
    /// Accounts keyed by [`AccountId`] (the JWT `sub`).
    by_id: DashMap<AccountId, Account>,
    /// FIX `Username (553)` → [`AccountId`] index (both paths → one account).
    by_fix_username: DashMap<String, AccountId>,
    /// A lazily-computed dummy hash for the unknown-user FIX login path, so a
    /// wrong username costs the same time as a wrong password (no enumeration
    /// oracle). `None` only if the one-off dummy hash could not be produced.
    login_timing_dummy: OnceLock<Option<String>>,
}

impl std::fmt::Debug for AccountRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountRegistry")
            .field("hasher", &self.hasher)
            .field("accounts", &self.by_id.len())
            .field("fix_usernames", &self.by_fix_username.len())
            .finish_non_exhaustive()
    }
}

impl AccountRegistry {
    /// Builds an empty registry with the given Argon2id hasher.
    #[must_use]
    pub fn new(hasher: Argon2Hasher) -> Self {
        Self {
            hasher,
            by_id: DashMap::new(),
            by_fix_username: DashMap::new(),
            login_timing_dummy: OnceLock::new(),
        }
    }

    /// Provisions a registry from an explicit list of provisions, hashing each
    /// plaintext FIX password at the pinned parameters (the plaintext is dropped).
    ///
    /// The seed-manifest **format** is #024; this takes the parsed input.
    ///
    /// # Errors
    ///
    /// - [`AuthError::PasswordHash`] if hashing a provisioned password fails;
    /// - [`AuthError::Provisioning`] on a duplicate account id or FIX username.
    pub fn provision(
        hasher: Argon2Hasher,
        provisions: impl IntoIterator<Item = AccountProvision>,
    ) -> Result<Self, AuthError> {
        let registry = Self::new(hasher);
        for provision in provisions {
            registry.provision_account(provision)?;
        }
        Ok(registry)
    }

    /// Provisions one account, hashing its plaintext FIX password (if any) at the
    /// pinned parameters. The plaintext is not retained.
    ///
    /// # Errors
    ///
    /// - [`AuthError::PasswordHash`] if hashing fails;
    /// - [`AuthError::Provisioning`] on a duplicate account id or FIX username.
    pub fn provision_account(&self, provision: AccountProvision) -> Result<(), AuthError> {
        let AccountProvision {
            id,
            owner,
            permissions,
            fix_username,
            fix_password,
            fix_comp_ids,
        } = provision;

        // Hash the plaintext immediately; it is dropped when this scope ends.
        let password_hash = match fix_password {
            Some(plaintext) => Some(self.hasher.hash(&plaintext)?),
            None => None,
        };

        let account = Account {
            id,
            owner,
            permissions,
            credentials: Credentials {
                fix_username,
                password_hash,
                fix_comp_ids,
            },
            revocation_epoch: 0,
        };
        self.insert_account(account)
    }

    /// Inserts a fully-formed account (its `password_hash` already an Argon2id PHC
    /// string) — the **DB-restore / drop-in** path (v0.2). Rejects a duplicate
    /// account id or FIX username.
    ///
    /// # Errors
    ///
    /// [`AuthError::Provisioning`] on a duplicate account id or FIX username.
    pub fn insert_account(&self, account: Account) -> Result<(), AuthError> {
        // Check both indices before mutating either (provisioning is a seed-time
        // single-writer step, so this check-then-insert is not racing writers).
        if self.by_id.contains_key(&account.id) {
            return Err(AuthError::Provisioning("duplicate account id"));
        }
        if let Some(username) = &account.credentials.fix_username
            && self.by_fix_username.contains_key(username)
        {
            return Err(AuthError::Provisioning("duplicate FIX username"));
        }

        if let Some(username) = account.credentials.fix_username.clone() {
            self.by_fix_username.insert(username, account.id.clone());
        }
        self.by_id.insert(account.id.clone(), account);
        Ok(())
    }

    /// Registry-resolved bootstrap mint: authorises the bootstrap secret, resolves
    /// `account` to its **registered** permissions and **current** revocation
    /// epoch, and mints via #011's [`JwtAuth::mint_token`]. It never fabricates a
    /// subject or arbitrary permissions
    /// ([ADR-0007](../docs/adr/0007-fix-credentials-and-account-model.md)).
    ///
    /// The bootstrap secret is checked **before** the account is resolved, so an
    /// unauthenticated caller cannot use the `UnknownAccount` outcome to enumerate
    /// accounts. `issued_at_secs` / `ttl_secs` are wall-clock **seconds** (token
    /// expiry is a credential-plane concern, not the venue clock); the caller
    /// supplies `issued_at_secs` (e.g. from the wall clock at the request).
    ///
    /// # Errors
    ///
    /// - [`AuthError::BootstrapDisabled`] / [`AuthError::BootstrapMismatch`] if the
    ///   secret gate rejects (checked first, before any account lookup);
    /// - [`AuthError::UnknownAccount`] if the (authorised) request names an account
    ///   the registry does not hold;
    /// - [`AuthError::TokenLifetime`] if `issued_at_secs + ttl_secs` overflows;
    /// - [`AuthError::Signing`] if signing fails.
    pub fn mint_for_account(
        &self,
        jwt: &JwtAuth,
        gate: &BootstrapGate,
        account: &AccountId,
        presented_secret: &str,
        issued_at_secs: u64,
        ttl_secs: u64,
    ) -> Result<String, AuthError> {
        // Gate first: no account resolution (hence no enumeration) before the
        // operator secret has cleared.
        gate.authorize(presented_secret)?;

        let resolved = self.account(account).ok_or(AuthError::UnknownAccount)?;
        let exp = issued_at_secs
            .checked_add(ttl_secs)
            .ok_or(AuthError::TokenLifetime)?;
        let claims = Claims::new(
            resolved.id,
            resolved.permissions,
            issued_at_secs,
            exp,
            resolved.revocation_epoch,
        );
        // `mint_token` re-checks the (already-cleared) gate — harmless and cheap.
        jwt.mint_token(gate, presented_secret, &claims)
    }

    /// Runs the one-off timing-equalisation dummy verify so an unknown FIX
    /// username costs the same as a wrong password (no enumeration oracle). The
    /// dummy hash is produced lazily and cached; the result is discarded.
    fn run_login_timing_dummy(&self, password: &str) {
        let dummy = self
            .login_timing_dummy
            .get_or_init(|| self.hasher.hash(FIX_LOGIN_TIMING_DUMMY).ok());
        if let Some(dummy) = dummy {
            let _ = self.hasher.verify(password, dummy);
        }
    }
}

impl RevocationOracle for AccountRegistry {
    /// The account's current revocation epoch (`None` if unknown) — read by the
    /// `auth_middleware` on every request.
    fn current_revocation_epoch(&self, account: &AccountId) -> Option<u64> {
        self.by_id.get(account).map(|entry| entry.revocation_epoch)
    }
}

impl AccountStore for AccountRegistry {
    fn account(&self, id: &AccountId) -> Option<Account> {
        self.by_id.get(id).map(|entry| entry.clone())
    }

    fn account_by_fix_username(&self, fix_username: &str) -> Option<Account> {
        let id = self.by_fix_username.get(fix_username)?.clone();
        self.account(&id)
    }

    fn verify_fix_password(&self, fix_username: &str, password: &str) -> FixLoginOutcome {
        // Resolve the username; an unknown user still runs a dummy verify so the
        // timing does not reveal whether the username exists.
        let Some(account_id) = self.by_fix_username.get(fix_username).map(|id| id.clone()) else {
            self.run_login_timing_dummy(password);
            return FixLoginOutcome::Rejected;
        };

        // Clone the stored hash + permissions out and DROP the shard guard before
        // the CPU-bound Argon2 verification (never hold a lock across a hash).
        let resolved = {
            let Some(entry) = self.by_id.get(&account_id) else {
                self.run_login_timing_dummy(password);
                return FixLoginOutcome::Rejected;
            };
            match &entry.credentials.password_hash {
                Some(hash) => Some((hash.clone(), entry.permissions.clone())),
                None => None,
            }
        };
        let Some((stored_hash, permissions)) = resolved else {
            // The account exists but has no FIX credential.
            self.run_login_timing_dummy(password);
            return FixLoginOutcome::Rejected;
        };

        match self.hasher.verify(password, &stored_hash) {
            Ok(PasswordVerification::Verified) => FixLoginOutcome::Authenticated {
                account: account_id,
                permissions,
            },
            Ok(PasswordVerification::VerifiedRehash(fresh)) => {
                // Persist the rehash-on-verify in place.
                if let Some(mut entry) = self.by_id.get_mut(&account_id) {
                    entry.credentials.password_hash = Some(fresh);
                }
                FixLoginOutcome::Authenticated {
                    account: account_id,
                    permissions,
                }
            }
            // A wrong password or a malformed stored hash both refuse the logon;
            // neither leaks the cause.
            Ok(PasswordVerification::Rejected) | Err(_) => FixLoginOutcome::Rejected,
        }
    }

    #[allow(clippy::manual_saturating_arithmetic)]
    fn revoke(&self, id: &AccountId) -> Option<u64> {
        let mut entry = self.by_id.get_mut(id)?;
        // Checked bump; the overflow arm is unreachable for a real venue and is
        // pinned at the ceiling explicitly. The repo rules forbid `saturating_*` /
        // `wrapping_*` (they silently hide overflow), so clippy's
        // `manual_saturating_arithmetic` suggestion — which would reintroduce
        // `saturating_add` — is allowed here (matching `RateLimiter::decide`).
        let next = entry.revocation_epoch.checked_add(1).unwrap_or(u64::MAX);
        entry.revocation_epoch = next;
        Some(next)
    }

    fn account_count(&self) -> usize {
        self.by_id.len()
    }
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

    // ======================================================================
    // #012 — account registry, Argon2id hashing, revocation, resolved minting
    // ======================================================================

    /// A fast Argon2id hasher for tests (below the pinned parameters) so the
    /// suite does not pay the full OWASP memory cost on every hash. Constructed
    /// directly (private fields are in-module) — production always uses
    /// [`Argon2Hasher::new`], which pins the OWASP baseline.
    fn fast_hasher() -> Argon2Hasher {
        Argon2Hasher {
            pepper: None,
            m_cost: 16,
            t_cost: 1,
            p_cost: 1,
        }
    }

    fn owner(byte: u8) -> Hash32 {
        Hash32([byte; 32])
    }

    /// A registry provisioned with a `Trade` account that can log in over FIX and
    /// a `Read`-only account with no FIX credential, using the fast test hasher.
    fn provisioned_registry() -> AccountRegistry {
        let provisions = vec![
            AccountProvision::new(
                AccountId::new("trader"),
                owner(0x11),
                vec![Permission::Trade],
            )
            .with_fix_login("trader-fix", "sw0rdf1sh"),
            AccountProvision::new(
                AccountId::new("viewer"),
                owner(0x22),
                vec![Permission::Read],
            ),
        ];
        match AccountRegistry::provision(fast_hasher(), provisions) {
            Ok(registry) => registry,
            Err(error) => panic!("provisioning must succeed: {error}"),
        }
    }

    // ---- provisioning + lookup -------------------------------------------

    #[test]
    fn test_provision_account_resolves_by_account_id() {
        let registry = provisioned_registry();
        assert_eq!(registry.account_count(), 2);
        let trader = match registry.account(&AccountId::new("trader")) {
            Some(account) => account,
            None => panic!("the trader account must resolve by AccountId"),
        };
        assert_eq!(trader.id, AccountId::new("trader"));
        assert_eq!(trader.permissions, vec![Permission::Trade]);
        assert_eq!(trader.owner, owner(0x11));
        assert_eq!(trader.revocation_epoch, 0);
        // The password was hashed at provisioning (Argon2id PHC), not stored raw.
        let hash = match &trader.credentials.password_hash {
            Some(hash) => hash,
            None => panic!("the FIX account must carry an Argon2id hash"),
        };
        assert!(hash.starts_with("$argon2id$"), "must be an Argon2id PHC");
        assert!(!hash.contains("sw0rdf1sh"), "the plaintext is never stored");
    }

    #[test]
    fn test_lookup_by_fix_username_resolves_the_same_account_id() {
        let registry = provisioned_registry();
        let by_username = match registry.account_by_fix_username("trader-fix") {
            Some(account) => account,
            None => panic!("the FIX username must resolve an account"),
        };
        // Both paths (JWT sub and FIX username) resolve ONE AccountId + perms.
        assert_eq!(by_username.id, AccountId::new("trader"));
        assert_eq!(by_username.permissions, vec![Permission::Trade]);
        // An unknown username resolves nothing.
        assert!(registry.account_by_fix_username("ghost-fix").is_none());
    }

    #[test]
    fn test_provision_duplicate_account_id_is_rejected() {
        let provisions = vec![
            AccountProvision::new(AccountId::new("dup"), owner(1), vec![Permission::Read]),
            AccountProvision::new(AccountId::new("dup"), owner(2), vec![Permission::Trade]),
        ];
        match AccountRegistry::provision(fast_hasher(), provisions) {
            Err(AuthError::Provisioning(label)) => assert_eq!(label, "duplicate account id"),
            other => panic!("a duplicate id must be rejected, got {other:?}"),
        }
    }

    #[test]
    fn test_provision_duplicate_fix_username_is_rejected() {
        let provisions = vec![
            AccountProvision::new(AccountId::new("a"), owner(1), vec![Permission::Trade])
                .with_fix_login("shared", "pw-a"),
            AccountProvision::new(AccountId::new("b"), owner(2), vec![Permission::Trade])
                .with_fix_login("shared", "pw-b"),
        ];
        match AccountRegistry::provision(fast_hasher(), provisions) {
            Err(AuthError::Provisioning(label)) => assert_eq!(label, "duplicate FIX username"),
            other => panic!("a duplicate FIX username must be rejected, got {other:?}"),
        }
    }

    // ---- Argon2id verify happy + wrong-password --------------------------

    #[test]
    fn test_verify_fix_password_happy_path_authenticates() {
        let registry = provisioned_registry();
        match registry.verify_fix_password("trader-fix", "sw0rdf1sh") {
            FixLoginOutcome::Authenticated {
                account,
                permissions,
            } => {
                assert_eq!(account, AccountId::new("trader"));
                assert_eq!(permissions, vec![Permission::Trade]);
            }
            FixLoginOutcome::Rejected => panic!("the correct FIX password must authenticate"),
        }
    }

    #[test]
    fn test_verify_fix_password_wrong_password_is_rejected() {
        let registry = provisioned_registry();
        assert_eq!(
            registry.verify_fix_password("trader-fix", "wrong-password"),
            FixLoginOutcome::Rejected
        );
    }

    #[test]
    fn test_verify_fix_password_unknown_user_is_rejected() {
        let registry = provisioned_registry();
        assert_eq!(
            registry.verify_fix_password("ghost-fix", "anything"),
            FixLoginOutcome::Rejected
        );
        // An account without a FIX credential also refuses (viewer has none).
        assert_eq!(
            registry.verify_fix_password("viewer", "anything"),
            FixLoginOutcome::Rejected
        );
    }

    // ---- Argon2id hashing details ----------------------------------------

    #[test]
    fn test_argon2_hasher_pins_the_owasp_baseline_parameters() {
        assert_eq!(ARGON2_M_COST_KIB, 19_456);
        assert_eq!(ARGON2_T_COST, 2);
        assert_eq!(ARGON2_P_COST, 1);
        // The default production hasher stamps those parameters into the PHC.
        let hasher = Argon2Hasher::new(None);
        let phc = match hasher.hash("pw") {
            Ok(phc) => phc,
            Err(error) => panic!("hashing must succeed: {error}"),
        };
        assert!(phc.contains("m=19456"));
        assert!(phc.contains("t=2"));
        assert!(phc.contains("p=1"));
        assert!(phc.starts_with("$argon2id$"));
    }

    #[test]
    fn test_argon2_verify_roundtrips_and_rejects_wrong_password() {
        let hasher = fast_hasher();
        let phc = match hasher.hash("correct horse") {
            Ok(phc) => phc,
            Err(error) => panic!("hashing must succeed: {error}"),
        };
        assert_eq!(
            hasher.verify("correct horse", &phc),
            Ok(PasswordVerification::Verified)
        );
        assert_eq!(
            hasher.verify("battery staple", &phc),
            Ok(PasswordVerification::Rejected)
        );
    }

    #[test]
    fn test_argon2_rehash_on_verify_when_stored_params_are_weaker() {
        // Hash with the weak test hasher, then verify with the pinned one: the
        // password matches AND a fresh, stronger hash is returned to persist.
        let weak = fast_hasher();
        let phc = match weak.hash("secret") {
            Ok(phc) => phc,
            Err(error) => panic!("weak hash must succeed: {error}"),
        };
        let pinned = Argon2Hasher::new(None);
        match pinned.verify("secret", &phc) {
            Ok(PasswordVerification::VerifiedRehash(fresh)) => {
                assert!(fresh.contains("m=19456"), "rehash uses the pinned m_cost");
                assert!(fresh.starts_with("$argon2id$"));
                // The fresh hash verifies at the pinned parameters (no further rehash).
                assert_eq!(
                    pinned.verify("secret", &fresh),
                    Ok(PasswordVerification::Verified)
                );
            }
            other => panic!("a weaker stored hash must trigger a rehash, got {other:?}"),
        }
    }

    #[test]
    fn test_argon2_pepper_changes_the_verification() {
        // A hash made WITH a pepper must not verify without it (and vice versa).
        let peppered = Argon2Hasher {
            pepper: Some(b"server-pepper".to_vec()),
            m_cost: 16,
            t_cost: 1,
            p_cost: 1,
        };
        let phc = match peppered.hash("pw") {
            Ok(phc) => phc,
            Err(error) => panic!("peppered hash must succeed: {error}"),
        };
        // The pepper is NOT written into the PHC string.
        assert!(!phc.contains("server-pepper"));
        assert_eq!(
            peppered.verify("pw", &phc),
            Ok(PasswordVerification::Verified)
        );
        let no_pepper = fast_hasher();
        assert_eq!(
            no_pepper.verify("pw", &phc),
            Ok(PasswordVerification::Rejected)
        );
    }

    // ---- revocation epoch -------------------------------------------------

    #[test]
    fn test_revoke_bumps_epoch_and_oracle_reports_it() {
        let registry = provisioned_registry();
        let id = AccountId::new("trader");
        assert_eq!(registry.current_revocation_epoch(&id), Some(0));
        assert_eq!(registry.revoke(&id), Some(1));
        assert_eq!(registry.current_revocation_epoch(&id), Some(1));
        assert_eq!(registry.revoke(&id), Some(2));
        // An unknown account cannot be revoked and has no epoch.
        assert_eq!(registry.revoke(&AccountId::new("ghost")), None);
        assert_eq!(
            registry.current_revocation_epoch(&AccountId::new("ghost")),
            None
        );
    }

    #[test]
    fn test_revocation_refuses_a_stale_token_via_admit() {
        // A registry-backed AuthService refuses a token minted before a revoke.
        let registry = Arc::new(provisioned_registry());
        let clock = TestClock::new(1_000);
        let jwt = dev_auth();
        // Mint a Trade token for the trader at the current epoch (0).
        let token = match registry.mint_for_account(
            &jwt,
            &bootstrap(),
            &AccountId::new("trader"),
            "operator-secret",
            now_secs(),
            3_600,
        ) {
            Ok(token) => token,
            Err(error) => panic!("resolved mint must succeed: {error}"),
        };
        let service = AuthService::new(
            jwt,
            RateLimiter::new(clock, 100),
            Arc::clone(&registry) as Arc<dyn RevocationOracle>,
        );
        let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);

        // Before revocation the token is admitted.
        match service.admit("/api/v1/orders", Some(&token), peer, Permission::Trade) {
            Admission::Admitted { .. } => {}
            other => panic!("the token must be admitted before revocation, got {other:?}"),
        }
        // Revoke bumps the account epoch; the same (now stale) token is refused.
        assert_eq!(registry.revoke(&AccountId::new("trader")), Some(1));
        match service.admit("/api/v1/orders", Some(&token), peer, Permission::Trade) {
            Admission::Rejected { error, .. } => {
                assert!(matches!(error, VenueError::Unauthorized));
            }
            other => panic!("a revoked token must be unauthorized, got {other:?}"),
        }
    }

    // ---- account-resolved minting ----------------------------------------

    #[test]
    fn test_mint_for_account_uses_registered_permissions_not_requested() {
        let registry = provisioned_registry();
        let jwt = dev_auth();
        let iat = now_secs();
        // Mint for the named `viewer` account — a registry AccountId, NOT a fresh
        // Uuid — with the account's REGISTERED permissions (Read only).
        let token = match registry.mint_for_account(
            &jwt,
            &bootstrap(),
            &AccountId::new("viewer"),
            "operator-secret",
            iat,
            3_600,
        ) {
            Ok(token) => token,
            Err(error) => panic!("resolved mint must succeed: {error}"),
        };
        let claims = match jwt.verify_token(&token) {
            Ok(claims) => claims,
            Err(error) => panic!("the minted token must verify: {error}"),
        };
        assert_eq!(claims.sub, AccountId::new("viewer"));
        assert_eq!(claims.permissions, vec![Permission::Read]);
        assert_eq!(claims.revocation_epoch, 0);
        assert_eq!(claims.iat, iat);
        assert_eq!(claims.exp, iat + 3_600);
    }

    #[test]
    fn test_mint_for_account_carries_current_revocation_epoch() {
        let registry = provisioned_registry();
        let jwt = dev_auth();
        assert_eq!(registry.revoke(&AccountId::new("trader")), Some(1));
        let token = match registry.mint_for_account(
            &jwt,
            &bootstrap(),
            &AccountId::new("trader"),
            "operator-secret",
            now_secs(),
            3_600,
        ) {
            Ok(token) => token,
            Err(error) => panic!("resolved mint must succeed: {error}"),
        };
        let claims = match jwt.verify_token(&token) {
            Ok(claims) => claims,
            Err(error) => panic!("the minted token must verify: {error}"),
        };
        // The token carries the account's CURRENT epoch, so it is not stale.
        assert_eq!(claims.revocation_epoch, 1);
    }

    #[test]
    fn test_mint_for_account_unknown_account_after_gate_is_unknown_account() {
        let registry = provisioned_registry();
        let jwt = dev_auth();
        // The bootstrap secret is correct, but the account does not exist.
        match registry.mint_for_account(
            &jwt,
            &bootstrap(),
            &AccountId::new("ghost"),
            "operator-secret",
            1_000,
            3_600,
        ) {
            Err(AuthError::UnknownAccount) => {}
            other => panic!("an unknown account must be UnknownAccount, got {other:?}"),
        }
    }

    #[test]
    fn test_mint_for_account_wrong_secret_does_not_leak_account_existence() {
        let registry = provisioned_registry();
        let jwt = dev_auth();
        // A wrong secret fails with BootstrapMismatch BEFORE any account lookup —
        // an existing and a non-existing account are indistinguishable pre-auth.
        let existing = registry.mint_for_account(
            &jwt,
            &bootstrap(),
            &AccountId::new("trader"),
            "wrong",
            1_000,
            3_600,
        );
        let missing = registry.mint_for_account(
            &jwt,
            &bootstrap(),
            &AccountId::new("ghost"),
            "wrong",
            1_000,
            3_600,
        );
        assert_eq!(existing, Err(AuthError::BootstrapMismatch));
        assert_eq!(missing, Err(AuthError::BootstrapMismatch));
    }

    #[test]
    fn test_mint_for_account_disabled_gate_refuses() {
        let registry = provisioned_registry();
        let jwt = dev_auth();
        let disabled = BootstrapGate::new(None);
        assert_eq!(
            registry.mint_for_account(
                &jwt,
                &disabled,
                &AccountId::new("trader"),
                "anything",
                1_000,
                3_600,
            ),
            Err(AuthError::BootstrapDisabled)
        );
    }

    // ---- credentials never serialise / log the hash (security) -----------

    #[test]
    fn test_account_serialise_omits_the_password_hash() {
        let registry = provisioned_registry();
        let account = match registry.account(&AccountId::new("trader")) {
            Some(account) => account,
            None => panic!("the trader account must resolve"),
        };
        // Sanity: the account DOES hold a hash internally.
        let hash = account
            .credentials
            .password_hash
            .clone()
            .expect("a FIX account has a stored hash");
        let json = match serde_json::to_string(&account) {
            Ok(json) => json,
            Err(error) => panic!("serialising an Account must succeed: {error}"),
        };
        // The wire projection must NOT carry the field or the hash value.
        assert!(
            !json.contains("password_hash"),
            "no password_hash field on the wire"
        );
        assert!(
            !json.contains(&hash),
            "the hash value never reaches the wire"
        );
        assert!(!json.contains("$argon2id$"), "no PHC string on the wire");
        // The public fields ARE present.
        assert!(json.contains("\"id\":\"trader\""));
        assert!(json.contains("permissions"));
    }

    #[test]
    fn test_credentials_debug_redacts_the_password_hash() {
        let registry = provisioned_registry();
        let account = match registry.account(&AccountId::new("trader")) {
            Some(account) => account,
            None => panic!("the trader account must resolve"),
        };
        let rendered = format!("{:?}", account.credentials);
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("$argon2id$"));
        // A full Account debug (which includes the credentials) is also safe.
        let account_debug = format!("{account:?}");
        assert!(!account_debug.contains("$argon2id$"));
    }

    #[test]
    fn test_account_provision_debug_redacts_the_plaintext_password() {
        let provision =
            AccountProvision::new(AccountId::new("x"), owner(1), vec![Permission::Trade])
                .with_fix_login("x-fix", "top-secret-plaintext");
        let rendered = format!("{provision:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("top-secret-plaintext"));
    }

    #[test]
    fn test_password_verification_debug_redacts_the_rehash() {
        // The VerifiedRehash variant must never print the PHC it carries.
        let rendered = format!(
            "{:?}",
            PasswordVerification::VerifiedRehash("$argon2id$v=19$m=19456$abc".to_string())
        );
        assert_eq!(rendered, "VerifiedRehash(<redacted>)");
        assert!(!rendered.contains("$argon2id$"));
    }

    #[test]
    fn test_argon2_hasher_debug_redacts_the_pepper() {
        let hasher = Argon2Hasher::new(Some(b"a-real-pepper".to_vec()));
        let rendered = format!("{hasher:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("a-real-pepper"));
    }

    #[test]
    fn test_auth_error_display_never_leaks_a_credential() {
        // Every issuance/hashing error renders only a static, non-secret label.
        for error in [
            AuthError::PasswordHash,
            AuthError::Provisioning("duplicate account id"),
            AuthError::UnknownAccount,
            AuthError::TokenLifetime,
        ] {
            let rendered = error.to_string();
            assert!(!rendered.contains("$argon2id$"));
            assert!(!rendered.is_empty());
        }
    }
}
