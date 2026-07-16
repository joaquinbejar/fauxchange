//! Cross-cutting: venue configuration — the layered file + environment + CLI
//! surface loaded first in the bootstrap sequence (`fauxchange::main`) and
//! threaded through every layer.
//!
//! ## The layered model (defaults → file → environment → CLI, later wins)
//!
//! A run is configured from four layers merged in a **fixed precedence**, later
//! layers overriding earlier ones
//! ([06 §4](../docs/06-deployment.md#4-configuration)):
//!
//! 1. **defaults** — constructed in code ([`DEFAULT_HTTP_ADDR`] etc.);
//! 2. **file** — a TOML document selected by `--config <path>`, each section a
//!    typed struct carrying `#[serde(deny_unknown_fields)]` so a typo aborts
//!    startup with a [`ConfigError::UnknownKey`] **naming the offending key**
//!    rather than silently defaulting;
//! 3. **environment** — the per-section env vars (`FAUXCHANGE_HTTP_ADDR`,
//!    `FAUXCHANGE_FIX_ADDR`, `DATABASE_URL`, `FAUXCHANGE_CLOCK`,
//!    `FAUXCHANGE_SEED`, `AUTH_BOOTSTRAP_SECRET`, `FAUXCHANGE_LOG_FORMAT`);
//! 4. **CLI** — the matching `--http-addr` / `--fix-addr` / `--database-url` /
//!    `--clock` / `--seed` / `--log-format` flags (plus `--config`).
//!
//! Every value is validated **at boot, before a single request is served**:
//! bind addresses parse to [`std::net::SocketAddr`], the clock/log-format enums
//! are checked against their closed vocabularies, and the seed parses as `u64` —
//! a failure is a typed [`ConfigError`] that fails the process fast.
//!
//! ## Secrets never reach a log
//!
//! `AUTH_BOOTSTRAP_SECRET` and `DATABASE_URL` are wrapped in [`Secret`], whose
//! [`std::fmt::Debug`] / [`std::fmt::Display`] impls render `<redacted>`. The
//! effective-config-at-boot renderer ([`Config::render_effective`]) therefore
//! never emits either value — redaction lives in the [`Secret`] type, not at
//! each call site ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
//!
//! ## Extension points (v0.2 seed / v0.5 microstructure)
//!
//! This is the config **foundation** v0.5 microstructure (#44–#47) and the seed
//! (#24) **extend, never replace**
//! ([05 §2](../docs/05-microstructure-config.md#2-config-model)). The
//! `[accounts.*]`, `[instruments.*]`, `[microstructure.*]`,
//! `[market_maker.*]`, and `[rate_limits]` sections are **accepted** by the file
//! loader today (typed [`serde::de::IgnoredAny`], so a forward-looking config
//! file is not rejected) but **not validated here**; a later issue swaps each
//! placeholder for a real `#[serde(deny_unknown_fields)]` struct without
//! reshaping the loader.
//!
//! Governed by [`docs/06-deployment.md §4`](../docs/06-deployment.md#4-configuration)
//! and [`docs/05-microstructure-config.md §2`](../docs/05-microstructure-config.md#2-config-model).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::exchange::LineageId;

// ============================================================================
// Defaults
// ============================================================================

/// Default REST/WS bind address (`FAUXCHANGE_HTTP_ADDR`).
pub const DEFAULT_HTTP_ADDR: &str = "0.0.0.0:8080";
/// Default FIX 4.4 bind address (`FAUXCHANGE_FIX_ADDR`).
pub const DEFAULT_FIX_ADDR: &str = "0.0.0.0:9878";
/// Default run-level seed (`FAUXCHANGE_SEED`) — a deterministic `0`.
pub const DEFAULT_SEED: u64 = 0;
/// Default clock mode (`FAUXCHANGE_CLOCK`).
pub const DEFAULT_CLOCK: ClockMode = ClockMode::Realtime;
/// Default log format (`FAUXCHANGE_LOG_FORMAT`) — human-readable locally; the
/// production image sets `json` ([06 §9](../docs/06-deployment.md#9-observability)).
pub const DEFAULT_LOG_FORMAT: LogFormat = LogFormat::Pretty;

/// The lineage-token prefix the run seed is namespaced under
/// ([01 §6.1](../docs/01-domain-model.md#61-order-identity-and-cross-protocol-idempotency)).
/// Colon-free, matching the id-grammar invariant.
pub const LINEAGE_PREFIX: &str = "fauxchange";

/// The default maximum size of the durable `PgPool`
/// (`FAUXCHANGE_DB_MAX_CONNECTIONS`) — the connection-pool ceiling the DB layer
/// (#23) opens with when `DATABASE_URL` is set. A bounded default so the pool
/// size is **config, not hard-coded** ([06 §6](../docs/06-deployment.md#6-persistence)).
pub const DEFAULT_DB_POOL_MAX_CONNECTIONS: u32 = 10;

/// The default slow-acquire warning threshold, in **milliseconds**
/// (`FAUXCHANGE_DB_SLOW_ACQUIRE_MS`) — a pool `acquire` slower than this is logged
/// at `WARN` (`rules/global_rules.md` *Logging*: "slow pool acquires"). Config, not
/// hard-coded.
pub const DEFAULT_DB_SLOW_ACQUIRE_MS: u64 = 500;

/// The rendered placeholder for a redacted secret in any log / effective-config
/// output.
pub const REDACTED: &str = "<redacted>";

// ============================================================================
// Secret — a redacting wrapper for AUTH_BOOTSTRAP_SECRET / DATABASE_URL
// ============================================================================

/// A configuration value that must never appear in a log, error, or the
/// effective-config output (`AUTH_BOOTSTRAP_SECRET`, `DATABASE_URL`).
///
/// Both [`std::fmt::Debug`] and [`std::fmt::Display`] render [`REDACTED`] — so
/// any structured or human log of a [`Config`] is redacted by construction. The
/// plaintext is reachable **only** through the explicitly-named
/// [`Secret::expose`], called at the (few) legitimate consumers (the DB pool,
/// the bootstrap gate) ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a plaintext secret value.
    #[must_use]
    #[inline]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the underlying plaintext. Named explicitly so a reviewer can grep
    /// every site that reads a secret; the value is never printed.
    #[must_use]
    #[inline]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(REDACTED)
    }
}

impl std::fmt::Display for Secret {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(REDACTED)
    }
}

// ============================================================================
// Small stable enums (clock mode, log format, persistence backend)
// ============================================================================

/// The venue clock mode ([06 §4](../docs/06-deployment.md#4-configuration)).
///
/// The three modes are **carried through** by #022; the advanceable/stepped
/// clock services that consume them land with the clock work (#28). Until then
/// the venue runs on the deterministic [`crate::exchange::FixedClock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ClockMode {
    /// Wall-time-paced ticks (the local-dev default).
    Realtime,
    /// Faster-than-real accelerated ticks.
    Accelerated,
    /// Ticks driven explicitly, one step at a time.
    Stepped,
}

impl ClockMode {
    /// The canonical token this mode serialises to.
    #[must_use]
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            ClockMode::Realtime => "realtime",
            ClockMode::Accelerated => "accelerated",
            ClockMode::Stepped => "stepped",
        }
    }

    /// Parses a clock token, or `None` for an unknown value.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::config::ClockMode;
    /// assert_eq!(ClockMode::from_token("stepped"), Some(ClockMode::Stepped));
    /// assert_eq!(ClockMode::from_token("warp"), None);
    /// ```
    #[must_use]
    #[inline]
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "realtime" => Some(ClockMode::Realtime),
            "accelerated" => Some(ClockMode::Accelerated),
            "stepped" => Some(ClockMode::Stepped),
            _ => None,
        }
    }
}

/// The log output format ([06 §9](../docs/06-deployment.md#9-observability)).
///
/// #022 owns the config knob; the subscriber that emits structured **JSON** is
/// the observability milestone's (#06 §9), which enables the `tracing-subscriber`
/// `json` feature at that point. The value is validated and logged here so a run
/// is self-describing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogFormat {
    /// Machine-readable JSON lines (production image).
    Json,
    /// Human-readable formatted output (local dev).
    Pretty,
}

impl LogFormat {
    /// The canonical token this format serialises to.
    #[must_use]
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            LogFormat::Json => "json",
            LogFormat::Pretty => "pretty",
        }
    }

    /// Parses a log-format token, or `None` for an unknown value.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::config::LogFormat;
    /// assert_eq!(LogFormat::from_token("json"), Some(LogFormat::Json));
    /// assert_eq!(LogFormat::from_token("xml"), None);
    /// ```
    #[must_use]
    #[inline]
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "json" => Some(LogFormat::Json),
            "pretty" => Some(LogFormat::Pretty),
            _ => None,
        }
    }
}

/// Which persistence backend the config selects — decided **here**, not by the
/// DB module ([06 §6](../docs/06-deployment.md#6-persistence)). An unset
/// `DATABASE_URL` is fully in-memory; a set one records the URL for the `PgPool`
/// layer (#23) to consume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PersistenceBackend {
    /// No `DATABASE_URL`: journal + stores live in RAM.
    InMemory,
    /// `DATABASE_URL` set: the durable PostgreSQL backend (#23).
    Postgres,
}

impl PersistenceBackend {
    /// A human-readable label for the effective-config output.
    #[must_use]
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            PersistenceBackend::InMemory => "in-memory",
            PersistenceBackend::Postgres => "postgres",
        }
    }
}

// ============================================================================
// ConfigError — startup configuration failures (never `anyhow`)
// ============================================================================

/// A failure loading or validating the venue configuration at boot.
///
/// Distinct from the request-boundary [`crate::error::VenueError`]: these are
/// **startup** failures that fail the process fast before it serves a request
/// (`rules/global_rules.md` *Error Handling*, *Configuration*). Every message is
/// lowercase and, where possible, names the offending value.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The `--config` file could not be read (missing, unreadable). Carries the
    /// path (a caller-supplied value, safe to echo) — never file contents.
    #[error("failed to read config file '{path}': {source}")]
    FileRead {
        /// The config file path that could not be read.
        path: String,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// A config key was not recognised — a file typo. Names the offending key so
    /// a run aborts with an actionable error rather than silently defaulting
    /// (the v0.2 acceptance item).
    #[error("unknown config key '{key}': remove it or correct the typo")]
    UnknownKey {
        /// The unrecognised key.
        key: String,
    },
    /// The config file was not valid TOML (or a value had the wrong type). The
    /// message is the parser's own, safe-to-echo diagnostic.
    #[error("failed to parse config file: {message}")]
    TomlParse {
        /// The parser's diagnostic message.
        message: String,
    },
    /// A bind address did not parse as `host:port`. Names the field and value.
    #[error("invalid bind address '{value}' for {field}: {reason}")]
    BadAddress {
        /// The config field (`http_addr` / `fix_addr`).
        field: &'static str,
        /// The offending value.
        value: String,
        /// The parse failure reason.
        reason: String,
    },
    /// The clock mode was not one of `realtime` / `accelerated` / `stepped`.
    #[error("invalid clock mode '{value}': expected one of realtime, accelerated, stepped")]
    InvalidClock {
        /// The offending value.
        value: String,
    },
    /// The log format was not one of `json` / `pretty`.
    #[error("invalid log format '{value}': expected one of json, pretty")]
    InvalidLogFormat {
        /// The offending value.
        value: String,
    },
    /// The run seed did not parse as a `u64`.
    #[error("invalid seed '{value}': expected a non-negative u64 integer")]
    BadSeed {
        /// The offending value.
        value: String,
    },
    /// A persistence pool knob (`pool_max_connections` / `slow_acquire_ms`) did
    /// not parse as its expected integer. Names the field and value.
    #[error("invalid persistence value '{value}' for {field}: expected a positive integer")]
    BadPersistenceValue {
        /// The config field (`pool_max_connections` / `slow_acquire_ms`).
        field: &'static str,
        /// The offending value.
        value: String,
    },
    /// A CLI flag that takes a value was given none.
    #[error("missing value for CLI flag '{flag}'")]
    MissingCliValue {
        /// The flag that expected a value.
        flag: String,
    },
    /// An unrecognised CLI flag was passed.
    #[error("unknown CLI flag '{flag}'")]
    UnknownCliFlag {
        /// The unrecognised flag.
        flag: String,
    },
}

// ============================================================================
// The effective, validated config (what main.rs and every layer consume)
// ============================================================================

/// The `[server]` section: the REST/WS bind address.
///
/// `#[non_exhaustive]` so a later milestone can add a field without a breaking
/// semver change for downstream crates (the "extend, never replace" contract at
/// the API level); within-crate construction is unaffected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerConfig {
    /// The REST/WS bind address (`FAUXCHANGE_HTTP_ADDR`).
    pub http_addr: SocketAddr,
}

/// The `[fix]` section: the FIX 4.4 bind address.
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FixConfig {
    /// The FIX 4.4 bind address (`FAUXCHANGE_FIX_ADDR`).
    pub fix_addr: SocketAddr,
}

/// The `[persistence]` section: the optional durable backend toggle.
///
/// The config — not the DB module — decides the backend: `database_url` unset is
/// [`PersistenceBackend::InMemory`], set is [`PersistenceBackend::Postgres`]
/// (the URL is recorded for the `PgPool` layer #23 to consume).
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PersistenceConfig {
    /// The `DATABASE_URL`, redacted in every log; `None` ⇒ in-memory.
    pub database_url: Option<Secret>,
    /// The maximum durable `PgPool` size the DB layer (#23) opens with — carried
    /// even in-memory (unused until a URL is set). Config, not hard-coded.
    pub pool_max_connections: u32,
    /// The slow-`acquire` warning threshold, in **milliseconds** — a pool acquire
    /// slower than this is logged at `WARN`. Config, not hard-coded.
    pub slow_acquire_ms: u64,
}

impl PersistenceConfig {
    /// Whether a durable backend was selected (`DATABASE_URL` is set).
    #[must_use]
    #[inline]
    pub fn is_persistent(&self) -> bool {
        self.database_url.is_some()
    }

    /// The maximum durable pool size the DB layer opens with.
    #[must_use]
    #[inline]
    pub fn pool_max_connections(&self) -> u32 {
        self.pool_max_connections
    }

    /// The slow-`acquire` warning threshold, in **milliseconds**.
    #[must_use]
    #[inline]
    pub fn slow_acquire_ms(&self) -> u64 {
        self.slow_acquire_ms
    }

    /// The backend the config selects.
    #[must_use]
    #[inline]
    pub fn backend(&self) -> PersistenceBackend {
        if self.is_persistent() {
            PersistenceBackend::Postgres
        } else {
            PersistenceBackend::InMemory
        }
    }

    /// The connection URL for the DB pool (#23), or `None` in-memory. Exposes the
    /// secret at the one legitimate consumer; never logged.
    #[must_use]
    #[inline]
    pub fn connection_url(&self) -> Option<&str> {
        self.database_url.as_ref().map(Secret::expose)
    }
}

/// The `[clock]` section: the venue clock mode.
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClockConfig {
    /// The clock mode (`FAUXCHANGE_CLOCK`), carried through for #28.
    pub mode: ClockMode,
}

/// The `[determinism]` section: the one run-level seed.
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeterminismConfig {
    /// The run seed (`FAUXCHANGE_SEED`) — the single seed every stochastic
    /// sub-stream derives from ([04 §6](../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
    pub seed: u64,
}

impl DeterminismConfig {
    /// The run lineage id derived from the seed, namespacing every venue-minted
    /// id ([01 §6.1](../docs/01-domain-model.md#61-order-identity-and-cross-protocol-idempotency)).
    /// A pure, colon-free function of the seed, so two runs with the same seed
    /// mint ids in the same namespace.
    #[must_use]
    pub fn lineage_id(&self) -> LineageId {
        LineageId::new(format!("{LINEAGE_PREFIX}-seed-{seed}", seed = self.seed))
    }
}

/// The `[auth]` section: the token-issuance bootstrap secret.
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AuthConfig {
    /// The `AUTH_BOOTSTRAP_SECRET` gating token issuance, redacted in every log;
    /// `None` ⇒ token issuance disabled ([06 §8](../docs/06-deployment.md#8-auth-bootstrap)).
    pub bootstrap_secret: Option<Secret>,
}

impl AuthConfig {
    /// The bootstrap secret plaintext for the issuance gate, or `None` when
    /// issuance is disabled. Exposes the secret at the one legitimate consumer;
    /// never logged.
    #[must_use]
    #[inline]
    pub fn bootstrap_secret_value(&self) -> Option<&str> {
        self.bootstrap_secret.as_ref().map(Secret::expose)
    }
}

/// The `[logging]` section: the log output format.
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct LoggingConfig {
    /// The log format (`FAUXCHANGE_LOG_FORMAT`).
    pub format: LogFormat,
}

/// The fully-resolved, validated venue configuration — the effective merge of
/// defaults → file → environment → CLI ([06 §4](../docs/06-deployment.md#4-configuration)).
///
/// Every field is already typed and validated: addresses parsed, enums checked,
/// the seed a `u64`. `main.rs` builds the [`crate::state::AppStateConfig`] from
/// it. `Debug` is safe to log — the secret-bearing fields redact via [`Secret`].
///
/// This is the config foundation v0.5 (#44–#47) **extends, not replaces**: a new
/// section is a new field here and in the file loader, on the same
/// `deny_unknown_fields` contract. `#[non_exhaustive]` makes that contract real
/// at the API level — adding a section field is a non-breaking semver change for
/// downstream crates; within-crate construction ([`Config::assemble`], tests) is
/// unaffected.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// The REST/WS bind address.
    pub server: ServerConfig,
    /// The FIX bind address.
    pub fix: FixConfig,
    /// The optional durable backend toggle.
    pub persistence: PersistenceConfig,
    /// The venue clock mode.
    pub clock: ClockConfig,
    /// The one run-level seed.
    pub determinism: DeterminismConfig,
    /// The token-issuance bootstrap secret.
    pub auth: AuthConfig,
    /// The log output format.
    pub logging: LoggingConfig,
}

impl Config {
    /// Loads the effective config from the process CLI args and environment,
    /// applying the fixed precedence defaults → file → environment → CLI.
    ///
    /// This is the `main.rs` entry point; unit and property tests drive the pure
    /// [`Config::load_from`] with explicit args + an injected env lookup.
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] on an unreadable/unparsable file, an unknown key, an
    /// unknown CLI flag, or an out-of-range value (bad address / clock / seed /
    /// log format).
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from(std::env::args().skip(1), |key| std::env::var(key).ok())
    }

    /// Loads the effective config from explicit CLI `args` and an injected `env`
    /// lookup — the deterministic, side-effect-free seam unit and property tests
    /// drive (the process env is never mutated; edition-2024 `set_var` is
    /// `unsafe` and forbidden here).
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] as for [`Config::load`].
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), fauxchange::config::ConfigError> {
    /// use fauxchange::config::Config;
    /// // No file, no env, no flags: pure defaults.
    /// let config = Config::load_from(std::iter::empty::<String>(), |_| None)?;
    /// assert_eq!(config.server.http_addr.port(), 8080);
    /// assert_eq!(config.determinism.seed, 0);
    /// assert!(!config.persistence.is_persistent());
    /// # Ok(())
    /// # }
    /// ```
    pub fn load_from<A, F>(args: A, env: F) -> Result<Self, ConfigError>
    where
        A: IntoIterator<Item = String>,
        F: Fn(&str) -> Option<String>,
    {
        let cli = parse_cli(args)?;
        let file = match &cli.config_path {
            Some(path) => raw_from_file(path)?,
            None => RawConfig::default(),
        };
        let env = raw_from_env(env);
        Self::assemble(file, env, cli.raw)
    }

    /// Merges the three explicit layers over the defaults, then validates —
    /// the pure core of [`Config::load_from`].
    fn assemble(file: RawConfig, env: RawConfig, cli: RawConfig) -> Result<Self, ConfigError> {
        let merged = RawConfig::default_layer()
            .overlay(file)
            .overlay(env)
            .overlay(cli);
        merged.validate()
    }

    /// Renders the full effective config on **one line** with secrets redacted —
    /// the self-describing boot log ([06 §4](../docs/06-deployment.md#4-configuration)).
    ///
    /// Redaction lives in [`Secret`]'s [`std::fmt::Display`], so neither
    /// `AUTH_BOOTSTRAP_SECRET` nor `DATABASE_URL` can appear here.
    #[must_use]
    pub fn render_effective(&self) -> String {
        let database_url = match &self.persistence.database_url {
            Some(secret) => secret.to_string(),
            None => "<unset>".to_string(),
        };
        let bootstrap_secret = match &self.auth.bootstrap_secret {
            Some(secret) => secret.to_string(),
            None => "<unset>".to_string(),
        };
        format!(
            "server.http_addr={http} fix.fix_addr={fix} \
             persistence.backend={backend} persistence.database_url={database_url} \
             persistence.pool_max_connections={pool} persistence.slow_acquire_ms={slow} \
             clock.mode={clock} determinism.seed={seed} \
             auth.bootstrap_secret={bootstrap_secret} logging.format={log}",
            http = self.server.http_addr,
            fix = self.fix.fix_addr,
            backend = self.persistence.backend().as_str(),
            pool = self.persistence.pool_max_connections,
            slow = self.persistence.slow_acquire_ms,
            clock = self.clock.mode.as_str(),
            seed = self.determinism.seed,
            log = self.logging.format.as_str(),
        )
    }
}

// ============================================================================
// RawConfig — the untyped, per-layer merge target
// ============================================================================

/// The per-layer, still-untyped representation the four layers merge over before
/// validation. Every field is an `Option<String>` so "later wins" is a simple
/// field-wise overlay; validation into the typed [`Config`] happens once, at the
/// end, giving a single `BadAddress` / `InvalidClock` / `BadSeed` path
/// regardless of which layer supplied the value.
///
/// Deliberately **not** `Debug` — it briefly holds plaintext secrets and must
/// never be logged.
#[derive(Default, Clone)]
struct RawConfig {
    http_addr: Option<String>,
    fix_addr: Option<String>,
    database_url: Option<String>,
    db_pool_max_connections: Option<String>,
    db_slow_acquire_ms: Option<String>,
    clock: Option<String>,
    seed: Option<String>,
    bootstrap_secret: Option<String>,
    log_format: Option<String>,
}

impl RawConfig {
    /// The defaults layer — the base every other layer overlays onto. The two
    /// secrets default to **unset** (in-memory persistence; issuance disabled).
    fn default_layer() -> Self {
        Self {
            http_addr: Some(DEFAULT_HTTP_ADDR.to_string()),
            fix_addr: Some(DEFAULT_FIX_ADDR.to_string()),
            database_url: None,
            db_pool_max_connections: Some(DEFAULT_DB_POOL_MAX_CONNECTIONS.to_string()),
            db_slow_acquire_ms: Some(DEFAULT_DB_SLOW_ACQUIRE_MS.to_string()),
            clock: Some(DEFAULT_CLOCK.as_str().to_string()),
            seed: Some(DEFAULT_SEED.to_string()),
            bootstrap_secret: None,
            log_format: Some(DEFAULT_LOG_FORMAT.as_str().to_string()),
        }
    }

    /// Overlays `other` onto `self` — every `Some` field in `other` wins ("later
    /// layers win"), every `None` leaves `self` untouched.
    fn overlay(mut self, other: RawConfig) -> RawConfig {
        if other.http_addr.is_some() {
            self.http_addr = other.http_addr;
        }
        if other.fix_addr.is_some() {
            self.fix_addr = other.fix_addr;
        }
        if other.database_url.is_some() {
            self.database_url = other.database_url;
        }
        if other.db_pool_max_connections.is_some() {
            self.db_pool_max_connections = other.db_pool_max_connections;
        }
        if other.db_slow_acquire_ms.is_some() {
            self.db_slow_acquire_ms = other.db_slow_acquire_ms;
        }
        if other.clock.is_some() {
            self.clock = other.clock;
        }
        if other.seed.is_some() {
            self.seed = other.seed;
        }
        if other.bootstrap_secret.is_some() {
            self.bootstrap_secret = other.bootstrap_secret;
        }
        if other.log_format.is_some() {
            self.log_format = other.log_format;
        }
        self
    }

    /// Validates the merged raw config into the typed [`Config`], failing fast on
    /// the first out-of-range value.
    fn validate(self) -> Result<Config, ConfigError> {
        let http_addr = parse_addr("http_addr", self.http_addr)?;
        let fix_addr = parse_addr("fix_addr", self.fix_addr)?;
        let mode = parse_clock(self.clock)?;
        let format = parse_log_format(self.log_format)?;
        let seed = parse_seed(self.seed)?;
        let pool_max_connections = parse_pool_u32(
            "pool_max_connections",
            self.db_pool_max_connections,
            DEFAULT_DB_POOL_MAX_CONNECTIONS,
        )?;
        let slow_acquire_ms = parse_pool_u64(
            "slow_acquire_ms",
            self.db_slow_acquire_ms,
            DEFAULT_DB_SLOW_ACQUIRE_MS,
        )?;
        Ok(Config {
            server: ServerConfig { http_addr },
            fix: FixConfig { fix_addr },
            persistence: PersistenceConfig {
                database_url: self.database_url.map(Secret::new),
                pool_max_connections,
                slow_acquire_ms,
            },
            clock: ClockConfig { mode },
            determinism: DeterminismConfig { seed },
            auth: AuthConfig {
                bootstrap_secret: self.bootstrap_secret.map(Secret::new),
            },
            logging: LoggingConfig { format },
        })
    }
}

// ============================================================================
// Layer sources — file (TOML), environment, CLI
// ============================================================================

/// Reads and parses a TOML config file into a [`RawConfig`] layer.
fn raw_from_file(path: &Path) -> Result<RawConfig, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::FileRead {
        path: path.display().to_string(),
        source,
    })?;
    raw_from_toml_str(&contents)
}

/// Parses a TOML config document into a [`RawConfig`] layer, enforcing
/// `deny_unknown_fields` so a typo becomes a [`ConfigError::UnknownKey`] naming
/// the key. Pure — the seam unit tests exercise the file layer with.
fn raw_from_toml_str(contents: &str) -> Result<RawConfig, ConfigError> {
    let file: FileConfig = toml::from_str(contents).map_err(map_toml_error)?;
    Ok(file.into_raw())
}

/// Maps a TOML deserialize failure to a typed [`ConfigError`], extracting the
/// offending key from a `deny_unknown_fields` rejection so it is named.
fn map_toml_error(error: toml::de::Error) -> ConfigError {
    let text = error.to_string();
    match extract_unknown_field(&text) {
        Some(key) => ConfigError::UnknownKey { key },
        None => ConfigError::TomlParse { message: text },
    }
}

/// Extracts the field name from serde's `` unknown field `x` `` diagnostic.
fn extract_unknown_field(text: &str) -> Option<String> {
    const MARKER: &str = "unknown field `";
    let start = text.find(MARKER)? + MARKER.len();
    let rest = text.get(start..)?;
    let end = rest.find('`')?;
    rest.get(..end).map(str::to_string)
}

/// Reads the environment layer via an injected lookup. An empty value is treated
/// as **unset** (matching the venue's `AUTH_BOOTSTRAP_SECRET` convention), so it
/// does not override an earlier layer.
fn raw_from_env<F: Fn(&str) -> Option<String>>(get: F) -> RawConfig {
    let pick = |key: &str| get(key).filter(|value| !value.is_empty());
    RawConfig {
        http_addr: pick("FAUXCHANGE_HTTP_ADDR"),
        fix_addr: pick("FAUXCHANGE_FIX_ADDR"),
        database_url: pick("DATABASE_URL"),
        db_pool_max_connections: pick("FAUXCHANGE_DB_MAX_CONNECTIONS"),
        db_slow_acquire_ms: pick("FAUXCHANGE_DB_SLOW_ACQUIRE_MS"),
        clock: pick("FAUXCHANGE_CLOCK"),
        seed: pick("FAUXCHANGE_SEED"),
        bootstrap_secret: pick("AUTH_BOOTSTRAP_SECRET"),
        log_format: pick("FAUXCHANGE_LOG_FORMAT"),
    }
}

/// The parsed CLI layer: the optional `--config` file selector plus the scalar
/// overrides.
struct CliLayer {
    config_path: Option<PathBuf>,
    raw: RawConfig,
}

/// Parses the CLI arguments (program name already stripped) into a [`CliLayer`].
///
/// Supports both `--flag value` and `--flag=value` forms. An unknown flag or a
/// value-taking flag with no value is a typed [`ConfigError`] (the same
/// deny-unknown discipline the file layer applies).
fn parse_cli<I: IntoIterator<Item = String>>(args: I) -> Result<CliLayer, ConfigError> {
    let mut layer = CliLayer {
        config_path: None,
        raw: RawConfig::default(),
    };
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
            None => (arg, None),
        };
        match flag.as_str() {
            "--config" => {
                layer.config_path = Some(PathBuf::from(take_cli_value(
                    "--config", inline, &mut iter,
                )?));
            }
            "--http-addr" => {
                layer.raw.http_addr = Some(take_cli_value("--http-addr", inline, &mut iter)?)
            }
            "--fix-addr" => {
                layer.raw.fix_addr = Some(take_cli_value("--fix-addr", inline, &mut iter)?)
            }
            "--database-url" => {
                layer.raw.database_url = Some(take_cli_value("--database-url", inline, &mut iter)?);
            }
            "--db-max-connections" => {
                layer.raw.db_pool_max_connections =
                    Some(take_cli_value("--db-max-connections", inline, &mut iter)?);
            }
            "--db-slow-acquire-ms" => {
                layer.raw.db_slow_acquire_ms =
                    Some(take_cli_value("--db-slow-acquire-ms", inline, &mut iter)?);
            }
            "--clock" => layer.raw.clock = Some(take_cli_value("--clock", inline, &mut iter)?),
            "--seed" => layer.raw.seed = Some(take_cli_value("--seed", inline, &mut iter)?),
            "--log-format" => {
                layer.raw.log_format = Some(take_cli_value("--log-format", inline, &mut iter)?)
            }
            other => {
                return Err(ConfigError::UnknownCliFlag {
                    flag: other.to_string(),
                });
            }
        }
    }
    Ok(layer)
}

/// Resolves a flag's value: the inline `--flag=value`, else the next argument.
fn take_cli_value(
    flag: &str,
    inline: Option<String>,
    rest: &mut impl Iterator<Item = String>,
) -> Result<String, ConfigError> {
    match inline {
        Some(value) => Ok(value),
        None => rest.next().ok_or_else(|| ConfigError::MissingCliValue {
            flag: flag.to_string(),
        }),
    }
}

// ============================================================================
// Value validators
// ============================================================================

/// Parses a bind address into a [`SocketAddr`], failing with [`ConfigError::BadAddress`].
fn parse_addr(field: &'static str, value: Option<String>) -> Result<SocketAddr, ConfigError> {
    let value = value.unwrap_or_default();
    value
        .parse::<SocketAddr>()
        .map_err(|error| ConfigError::BadAddress {
            field,
            value,
            reason: error.to_string(),
        })
}

/// Parses a clock token, failing with [`ConfigError::InvalidClock`].
fn parse_clock(value: Option<String>) -> Result<ClockMode, ConfigError> {
    let value = value.unwrap_or_else(|| DEFAULT_CLOCK.as_str().to_string());
    ClockMode::from_token(&value).ok_or(ConfigError::InvalidClock { value })
}

/// Parses a log-format token, failing with [`ConfigError::InvalidLogFormat`].
fn parse_log_format(value: Option<String>) -> Result<LogFormat, ConfigError> {
    let value = value.unwrap_or_else(|| DEFAULT_LOG_FORMAT.as_str().to_string());
    LogFormat::from_token(&value).ok_or(ConfigError::InvalidLogFormat { value })
}

/// Parses the run seed as a `u64`, failing with [`ConfigError::BadSeed`].
fn parse_seed(value: Option<String>) -> Result<u64, ConfigError> {
    let value = value.unwrap_or_else(|| DEFAULT_SEED.to_string());
    match value.trim().parse::<u64>() {
        Ok(seed) => Ok(seed),
        Err(_) => Err(ConfigError::BadSeed { value }),
    }
}

/// Parses a persistence pool `u32` knob (clamped to at least `1`), failing with
/// [`ConfigError::BadPersistenceValue`].
fn parse_pool_u32(
    field: &'static str,
    value: Option<String>,
    default: u32,
) -> Result<u32, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    match value.trim().parse::<u32>() {
        // A zero-size pool can never serve a query; clamp up to a usable minimum.
        Ok(parsed) => Ok(parsed.max(1)),
        Err(_) => Err(ConfigError::BadPersistenceValue { field, value }),
    }
}

/// Parses a persistence pool `u64` knob (milliseconds), failing with
/// [`ConfigError::BadPersistenceValue`].
fn parse_pool_u64(
    field: &'static str,
    value: Option<String>,
    default: u64,
) -> Result<u64, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    match value.trim().parse::<u64>() {
        Ok(parsed) => Ok(parsed),
        Err(_) => Err(ConfigError::BadPersistenceValue { field, value }),
    }
}

// ============================================================================
// File deserialization structs — deny_unknown_fields on every section
// ============================================================================

/// The TOML file document. Every named section is optional (a partial file is
/// valid); an unrecognised top-level key is a startup [`ConfigError::UnknownKey`].
///
/// The extension-point sections (`accounts` / `instruments` / `microstructure` /
/// `market_maker` / `rate_limits`) are **accepted but ignored** here
/// ([`IgnoredAny`]) so a forward-looking config file is not rejected; a later
/// issue (#24 seed, #44–#47 microstructure) swaps each for a real
/// `deny_unknown_fields` struct without reshaping this loader.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    server: Option<FileServer>,
    #[serde(default)]
    fix: Option<FileFix>,
    #[serde(default)]
    persistence: Option<FilePersistence>,
    #[serde(default)]
    clock: Option<FileClock>,
    #[serde(default)]
    determinism: Option<FileDeterminism>,
    #[serde(default)]
    auth: Option<FileAuth>,
    #[serde(default)]
    logging: Option<FileLogging>,
    // ---- documented extension points (accepted, not validated in v0.2) ----
    // These exist so serde ACCEPTS a forward-looking `[accounts.*]` /
    // `[instruments.*]` / `[microstructure.*]` / `[market_maker.*]` /
    // `[rate_limits]` section rather than rejecting it as an unknown top-level
    // key; the content is deliberately ignored here and validated by the seed
    // (#024) / microstructure (#44–#47) issues. They are read only by serde
    // during deserialization, so Rust's dead-code analysis (which does not count
    // that) is scoped-silenced.
    #[serde(default)]
    #[allow(dead_code)]
    accounts: Option<IgnoredAny>,
    #[serde(default)]
    #[allow(dead_code)]
    instruments: Option<IgnoredAny>,
    #[serde(default)]
    #[allow(dead_code)]
    microstructure: Option<IgnoredAny>,
    #[serde(default)]
    #[allow(dead_code)]
    market_maker: Option<IgnoredAny>,
    #[serde(default)]
    #[allow(dead_code)]
    rate_limits: Option<IgnoredAny>,
}

impl FileConfig {
    /// Flattens the structured file document into the untyped [`RawConfig`] layer.
    fn into_raw(self) -> RawConfig {
        // Bind the persistence section once so all three of its knobs read the
        // same optional table (a moved-out field cannot be read twice).
        let persistence = self.persistence;
        let database_url = persistence
            .as_ref()
            .and_then(|section| section.database_url.clone());
        let db_pool_max_connections = persistence
            .as_ref()
            .and_then(|section| section.pool_max_connections)
            .map(|value| value.to_string());
        let db_slow_acquire_ms = persistence
            .as_ref()
            .and_then(|section| section.slow_acquire_ms)
            .map(|value| value.to_string());
        RawConfig {
            http_addr: self.server.and_then(|section| section.http_addr),
            fix_addr: self.fix.and_then(|section| section.fix_addr),
            database_url,
            db_pool_max_connections,
            db_slow_acquire_ms,
            clock: self.clock.and_then(|section| section.mode),
            seed: self
                .determinism
                .and_then(|section| section.seed)
                .map(|seed| seed.to_string()),
            bootstrap_secret: self.auth.and_then(|section| section.bootstrap_secret),
            log_format: self.logging.and_then(|section| section.format),
        }
    }
}

/// `[server]` — an unrecognised inner key aborts startup.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileServer {
    #[serde(default)]
    http_addr: Option<String>,
}

/// `[fix]` — an unrecognised inner key aborts startup.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileFix {
    #[serde(default)]
    fix_addr: Option<String>,
}

/// `[persistence]` — an unrecognised inner key aborts startup.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePersistence {
    #[serde(default)]
    database_url: Option<String>,
    #[serde(default)]
    pool_max_connections: Option<u32>,
    #[serde(default)]
    slow_acquire_ms: Option<u64>,
}

/// `[clock]` — an unrecognised inner key aborts startup.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileClock {
    #[serde(default)]
    mode: Option<String>,
}

/// `[determinism]` — an unrecognised inner key aborts startup. The seed is a
/// TOML integer (0..=`i64::MAX`); env/CLI carry the full `u64` range as a string.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDeterminism {
    #[serde(default)]
    seed: Option<u64>,
}

/// `[auth]` — an unrecognised inner key aborts startup.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAuth {
    #[serde(default)]
    bootstrap_secret: Option<String>,
}

/// `[logging]` — an unrecognised inner key aborts startup.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileLogging {
    #[serde(default)]
    format: Option<String>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Builds an injectable env lookup over a fixed set of pairs.
    fn env_map(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    /// No file, no env, no flags: every default applies.
    #[test]
    fn test_config_defaults_apply_when_no_layers() {
        let config = match Config::assemble(
            RawConfig::default(),
            raw_from_env(|_| None),
            RawConfig::default(),
        ) {
            Ok(config) => config,
            Err(error) => panic!("defaults must validate: {error}"),
        };
        assert_eq!(config.server.http_addr.to_string(), DEFAULT_HTTP_ADDR);
        assert_eq!(config.fix.fix_addr.to_string(), DEFAULT_FIX_ADDR);
        assert_eq!(config.clock.mode, ClockMode::Realtime);
        assert_eq!(config.logging.format, LogFormat::Pretty);
        assert_eq!(config.determinism.seed, 0);
        assert!(!config.persistence.is_persistent());
        assert_eq!(config.persistence.backend(), PersistenceBackend::InMemory);
        assert!(config.auth.bootstrap_secret.is_none());
    }

    /// The file layer overrides a default.
    #[test]
    fn test_config_file_overrides_default() -> Result<(), ConfigError> {
        let file = raw_from_toml_str("[server]\nhttp_addr = \"1.2.3.4:1111\"\n")?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        assert_eq!(config.server.http_addr.to_string(), "1.2.3.4:1111");
        // Untouched fields keep their defaults.
        assert_eq!(config.fix.fix_addr.to_string(), DEFAULT_FIX_ADDR);
        Ok(())
    }

    /// The env layer overrides the file layer.
    #[test]
    fn test_config_env_overrides_file() -> Result<(), ConfigError> {
        let file = raw_from_toml_str("[server]\nhttp_addr = \"1.1.1.1:1111\"\n")?;
        let env = raw_from_env(env_map(&[("FAUXCHANGE_HTTP_ADDR", "2.2.2.2:2222")]));
        let config = Config::assemble(file, env, RawConfig::default())?;
        assert_eq!(config.server.http_addr.to_string(), "2.2.2.2:2222");
        Ok(())
    }

    /// The CLI layer overrides the env layer.
    #[test]
    fn test_config_cli_overrides_env() -> Result<(), ConfigError> {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_HTTP_ADDR", "2.2.2.2:2222")]));
        let cli = parse_cli(vec!["--http-addr".to_string(), "3.3.3.3:3333".to_string()])?;
        let config = Config::assemble(RawConfig::default(), env, cli.raw)?;
        assert_eq!(config.server.http_addr.to_string(), "3.3.3.3:3333");
        Ok(())
    }

    /// The full precedence chain: default < file < env < CLI, each winning at
    /// its own level for the same knob (the seed).
    #[test]
    fn test_config_full_precedence_chain_cli_wins() -> Result<(), ConfigError> {
        // Default only.
        let defaults = Config::assemble(
            RawConfig::default(),
            raw_from_env(|_| None),
            RawConfig::default(),
        )?;
        assert_eq!(defaults.determinism.seed, 0);
        // File over default.
        let file = raw_from_toml_str("[determinism]\nseed = 10\n")?;
        let file_only =
            Config::assemble(file.clone(), raw_from_env(|_| None), RawConfig::default())?;
        assert_eq!(file_only.determinism.seed, 10);
        // Env over file.
        let env = raw_from_env(env_map(&[("FAUXCHANGE_SEED", "20")]));
        let env_over_file = Config::assemble(file.clone(), env.clone(), RawConfig::default())?;
        assert_eq!(env_over_file.determinism.seed, 20);
        // CLI over env over file.
        let cli = parse_cli(vec!["--seed".to_string(), "30".to_string()])?;
        let cli_wins = Config::assemble(file, env, cli.raw)?;
        assert_eq!(cli_wins.determinism.seed, 30);
        Ok(())
    }

    /// An unknown key inside a section aborts startup naming the key.
    #[test]
    fn test_config_unknown_section_key_names_the_key() {
        match raw_from_toml_str("[server]\nbogus_knob = 1\n") {
            Err(ConfigError::UnknownKey { key }) => assert_eq!(key, "bogus_knob"),
            Err(other) => panic!("expected UnknownKey(bogus_knob), got {other:?}"),
            Ok(_) => panic!("expected UnknownKey(bogus_knob), got Ok"),
        }
    }

    /// An unknown top-level key aborts startup naming the key.
    #[test]
    fn test_config_unknown_top_level_key_names_the_key() {
        match raw_from_toml_str("frobnicate = true\n") {
            Err(ConfigError::UnknownKey { key }) => assert_eq!(key, "frobnicate"),
            Err(other) => panic!("expected UnknownKey(frobnicate), got {other:?}"),
            Ok(_) => panic!("expected UnknownKey(frobnicate), got Ok"),
        }
    }

    /// An unknown top-level **section** (the TOML table form) aborts startup
    /// naming the section — the same `deny_unknown_fields` path, hardening the
    /// v0.2 acceptance item for a mistyped `[section]` header.
    #[test]
    fn test_config_unknown_top_level_section_names_the_key() {
        match raw_from_toml_str("[frobnicate]\nknob = 1\n") {
            Err(ConfigError::UnknownKey { key }) => assert_eq!(key, "frobnicate"),
            Err(other) => panic!("expected UnknownKey(frobnicate), got {other:?}"),
            Ok(_) => panic!("expected UnknownKey(frobnicate), got Ok"),
        }
    }

    /// The documented extension-point sections are accepted, not rejected.
    #[test]
    fn test_config_extension_point_sections_are_accepted() -> Result<(), ConfigError> {
        let document = "\
[microstructure.fees]
maker_bps = -10
taker_bps = 35

[market_maker.personas.tight]
base_spread_bps = 20

[instruments.\"BTC\".specs]
tick_size_cents = 5

[rate_limits]
read_per_window = 6000
";
        // Parses cleanly (accepted + ignored), and the v0.2 knobs keep defaults.
        let raw = raw_from_toml_str(document)?;
        let config = Config::assemble(raw, raw_from_env(|_| None), RawConfig::default())?;
        assert_eq!(config.determinism.seed, 0);
        Ok(())
    }

    /// An invalid clock value aborts startup naming the value.
    #[test]
    fn test_config_invalid_clock_is_rejected() {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_CLOCK", "warp")]));
        match Config::assemble(RawConfig::default(), env, RawConfig::default()) {
            Err(ConfigError::InvalidClock { value }) => assert_eq!(value, "warp"),
            other => panic!("expected InvalidClock(warp), got {other:?}"),
        }
    }

    /// An invalid log format aborts startup naming the value.
    #[test]
    fn test_config_invalid_log_format_is_rejected() {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_LOG_FORMAT", "xml")]));
        match Config::assemble(RawConfig::default(), env, RawConfig::default()) {
            Err(ConfigError::InvalidLogFormat { value }) => assert_eq!(value, "xml"),
            other => panic!("expected InvalidLogFormat(xml), got {other:?}"),
        }
    }

    /// A malformed bind address aborts startup naming the field and value.
    #[test]
    fn test_config_bad_bind_address_is_rejected() {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_HTTP_ADDR", "not-an-address")]));
        match Config::assemble(RawConfig::default(), env, RawConfig::default()) {
            Err(ConfigError::BadAddress { field, value, .. }) => {
                assert_eq!(field, "http_addr");
                assert_eq!(value, "not-an-address");
            }
            other => panic!("expected BadAddress(http_addr), got {other:?}"),
        }
    }

    /// A non-integer seed aborts startup naming the value.
    #[test]
    fn test_config_bad_seed_is_rejected() {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_SEED", "not-a-number")]));
        match Config::assemble(RawConfig::default(), env, RawConfig::default()) {
            Err(ConfigError::BadSeed { value }) => assert_eq!(value, "not-a-number"),
            other => panic!("expected BadSeed(not-a-number), got {other:?}"),
        }
    }

    /// An unset DATABASE_URL selects the in-memory backend.
    #[test]
    fn test_config_database_url_unset_selects_in_memory() -> Result<(), ConfigError> {
        let config = Config::assemble(
            RawConfig::default(),
            raw_from_env(|_| None),
            RawConfig::default(),
        )?;
        assert!(!config.persistence.is_persistent());
        assert_eq!(config.persistence.backend(), PersistenceBackend::InMemory);
        assert_eq!(config.persistence.connection_url(), None);
        Ok(())
    }

    /// A set DATABASE_URL selects the Postgres backend and records the URL for
    /// the DB layer (#23) to consume.
    #[test]
    fn test_config_database_url_set_selects_postgres() -> Result<(), ConfigError> {
        let url = "postgres://user:pw@db:5432/fauxchange";
        let env = raw_from_env(env_map(&[("DATABASE_URL", url)]));
        let config = Config::assemble(RawConfig::default(), env, RawConfig::default())?;
        assert!(config.persistence.is_persistent());
        assert_eq!(config.persistence.backend(), PersistenceBackend::Postgres);
        assert_eq!(config.persistence.connection_url(), Some(url));
        Ok(())
    }

    /// The pool knobs default when unset and are overridden from the env layer.
    #[test]
    fn test_config_persistence_pool_knobs_default_and_override() -> Result<(), ConfigError> {
        let defaults = Config::assemble(
            RawConfig::default(),
            raw_from_env(|_| None),
            RawConfig::default(),
        )?;
        assert_eq!(
            defaults.persistence.pool_max_connections(),
            DEFAULT_DB_POOL_MAX_CONNECTIONS
        );
        assert_eq!(
            defaults.persistence.slow_acquire_ms(),
            DEFAULT_DB_SLOW_ACQUIRE_MS
        );

        let env = raw_from_env(env_map(&[
            ("FAUXCHANGE_DB_MAX_CONNECTIONS", "25"),
            ("FAUXCHANGE_DB_SLOW_ACQUIRE_MS", "1500"),
        ]));
        let config = Config::assemble(RawConfig::default(), env, RawConfig::default())?;
        assert_eq!(config.persistence.pool_max_connections(), 25);
        assert_eq!(config.persistence.slow_acquire_ms(), 1_500);
        Ok(())
    }

    /// A zero pool size is clamped up to a usable minimum (a zero-size pool can
    /// never serve a query).
    #[test]
    fn test_config_persistence_pool_zero_is_clamped() -> Result<(), ConfigError> {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_DB_MAX_CONNECTIONS", "0")]));
        let config = Config::assemble(RawConfig::default(), env, RawConfig::default())?;
        assert_eq!(config.persistence.pool_max_connections(), 1);
        Ok(())
    }

    /// A non-integer pool knob aborts startup naming the field and value.
    #[test]
    fn test_config_persistence_bad_pool_value_is_rejected() {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_DB_MAX_CONNECTIONS", "lots")]));
        match Config::assemble(RawConfig::default(), env, RawConfig::default()) {
            Err(ConfigError::BadPersistenceValue { field, value }) => {
                assert_eq!(field, "pool_max_connections");
                assert_eq!(value, "lots");
            }
            other => panic!("expected BadPersistenceValue, got {other:?}"),
        }
    }

    /// The pool knobs are read from the `[persistence]` file section.
    #[test]
    fn test_config_persistence_pool_from_file_section() -> Result<(), ConfigError> {
        let file = raw_from_toml_str(
            "[persistence]\ndatabase_url = \"postgres://db/x\"\npool_max_connections = 7\nslow_acquire_ms = 250\n",
        )?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        assert!(config.persistence.is_persistent());
        assert_eq!(config.persistence.pool_max_connections(), 7);
        assert_eq!(config.persistence.slow_acquire_ms(), 250);
        Ok(())
    }

    /// The effective-config render (and the derived Debug) redact both secrets —
    /// neither the DATABASE_URL nor the AUTH_BOOTSTRAP_SECRET plaintext appears.
    #[test]
    fn test_config_effective_render_redacts_secrets() -> Result<(), ConfigError> {
        const DB_MARKER: &str = "postgres://admin:HUNTER2-DB-PASSWORD@db/venue";
        const BOOTSTRAP_MARKER: &str = "TOPSECRET-BOOTSTRAP-VALUE";
        let env = raw_from_env(env_map(&[
            ("DATABASE_URL", DB_MARKER),
            ("AUTH_BOOTSTRAP_SECRET", BOOTSTRAP_MARKER),
        ]));
        let config = Config::assemble(RawConfig::default(), env, RawConfig::default())?;

        let rendered = config.render_effective();
        assert!(
            !rendered.contains("HUNTER2-DB-PASSWORD"),
            "DATABASE_URL leaked into the effective-config render: {rendered}"
        );
        assert!(
            !rendered.contains(BOOTSTRAP_MARKER),
            "AUTH_BOOTSTRAP_SECRET leaked into the effective-config render: {rendered}"
        );
        assert!(
            rendered.contains(REDACTED),
            "secrets must render as {REDACTED}"
        );

        // The derived Debug path is equally safe.
        let debug = format!("{config:?}");
        assert!(!debug.contains("HUNTER2-DB-PASSWORD"));
        assert!(!debug.contains(BOOTSTRAP_MARKER));
        // The exposed accessor still returns the plaintext for its consumers.
        assert_eq!(config.persistence.connection_url(), Some(DB_MARKER));
        assert_eq!(config.auth.bootstrap_secret_value(), Some(BOOTSTRAP_MARKER));
        Ok(())
    }

    /// `--config <path>` selects the file layer, whose values win over defaults.
    #[test]
    fn test_config_cli_config_flag_selects_file() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!(
            "fauxchange-cfg-{pid}-{nanos}.toml",
            pid = std::process::id(),
            nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        ));
        std::fs::write(&path, "[server]\nhttp_addr = \"9.9.9.9:9999\"\n")?;
        let args = vec!["--config".to_string(), path.display().to_string()];
        let config = Config::load_from(args, |_| None)?;
        let _ = std::fs::remove_file(&path);
        assert_eq!(config.server.http_addr.to_string(), "9.9.9.9:9999");
        Ok(())
    }

    /// The `--config=<path>` inline form is supported, and CLI wins over env.
    #[test]
    fn test_config_cli_inline_value_form() -> Result<(), ConfigError> {
        let cli = parse_cli(vec!["--seed=7".to_string()])?;
        let config = Config::assemble(RawConfig::default(), raw_from_env(|_| None), cli.raw)?;
        assert_eq!(config.determinism.seed, 7);
        Ok(())
    }

    /// A missing `--config` file is a typed FileRead error, not a panic.
    #[test]
    fn test_config_missing_file_is_file_read_error() {
        let args = vec![
            "--config".to_string(),
            "/nonexistent/fauxchange/does-not-exist.toml".to_string(),
        ];
        match Config::load_from(args, |_| None) {
            Err(ConfigError::FileRead { path, .. }) => {
                assert!(path.contains("does-not-exist.toml"));
            }
            other => panic!("expected FileRead, got {other:?}"),
        }
    }

    /// An unknown CLI flag is rejected naming the flag.
    #[test]
    fn test_config_cli_unknown_flag_is_rejected() {
        match parse_cli(vec!["--bogus".to_string()]) {
            Err(ConfigError::UnknownCliFlag { flag }) => assert_eq!(flag, "--bogus"),
            Err(other) => panic!("expected UnknownCliFlag(--bogus), got {other:?}"),
            Ok(_) => panic!("expected UnknownCliFlag(--bogus), got Ok"),
        }
    }

    /// A value-taking CLI flag with no value is rejected naming the flag.
    #[test]
    fn test_config_cli_missing_value_is_rejected() {
        match parse_cli(vec!["--seed".to_string()]) {
            Err(ConfigError::MissingCliValue { flag }) => assert_eq!(flag, "--seed"),
            Err(other) => panic!("expected MissingCliValue(--seed), got {other:?}"),
            Ok(_) => panic!("expected MissingCliValue(--seed), got Ok"),
        }
    }

    /// An empty env var is treated as unset (does not override an earlier layer).
    #[test]
    fn test_config_empty_env_var_is_treated_as_unset() -> Result<(), ConfigError> {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_HTTP_ADDR", "")]));
        let config = Config::assemble(RawConfig::default(), env, RawConfig::default())?;
        assert_eq!(config.server.http_addr.to_string(), DEFAULT_HTTP_ADDR);
        Ok(())
    }

    /// The seed feeds the run lineage id namespace (seed → lineage).
    #[test]
    fn test_config_seed_feeds_lineage() -> Result<(), ConfigError> {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_SEED", "42")]));
        let config = Config::assemble(RawConfig::default(), env, RawConfig::default())?;
        assert_eq!(config.determinism.seed, 42);
        assert_eq!(
            config.determinism.lineage_id().as_str(),
            "fauxchange-seed-42"
        );
        Ok(())
    }

    /// The clock mode is carried through unchanged (clock services are #28).
    #[test]
    fn test_config_clock_mode_carried_through() -> Result<(), ConfigError> {
        let env = raw_from_env(env_map(&[("FAUXCHANGE_CLOCK", "stepped")]));
        let config = Config::assemble(RawConfig::default(), env, RawConfig::default())?;
        assert_eq!(config.clock.mode, ClockMode::Stepped);
        Ok(())
    }

    /// A malformed config file (not a UnknownKey) surfaces as a TomlParse error.
    #[test]
    fn test_config_malformed_toml_is_parse_error() {
        match raw_from_toml_str("this is not = valid = toml\n") {
            Err(ConfigError::TomlParse { .. }) => {}
            Err(other) => panic!("expected TomlParse, got {other:?}"),
            Ok(_) => panic!("expected TomlParse, got Ok"),
        }
    }
}
