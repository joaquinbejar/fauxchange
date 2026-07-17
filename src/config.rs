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
//! ([05 §2](../docs/05-microstructure-config.md#2-config-model)). As of #24 the
//! `[accounts.*]`, `[instruments.*]`, and `[market_maker.*]` sections are **real,
//! validated** [`SeedManifest`] structs carrying `#[serde(deny_unknown_fields)]`,
//! so a typo *inside* a seeded account or instrument now aborts startup naming the
//! key (the [`IgnoredAny`] placeholder used to swallow it). The remaining
//! `[microstructure.*]` and `[rate_limits]` sections are still **accepted but not
//! validated** ([`IgnoredAny`]) — #44–#47 swap each for a real struct without
//! reshaping the loader.
//!
//! The seed sections resolve into a [`SeedManifest`] on [`Config::seed`]: the
//! account registry provisions, the instrument set + opening prices, and the
//! default market-maker personas the bounded seeding phase applies **before** the
//! venue flips to serving ([06 §7](../docs/06-deployment.md#7-seed-data-and-scenarios)).
//! Every seeded expiry is validated to an absolute canonical
//! `ExpirationDate::DateTime` at **load** — a relative `Days` expiry is
//! wall-clock-relative and breaks replay, so it is refused with a
//! [`ConfigError::SeedDaysExpiry`] ([CLAUDE.md](../CLAUDE.md) Key Decisions).
//!
//! Governed by [`docs/06-deployment.md §4`](../docs/06-deployment.md#4-configuration)
//! and [`docs/05-microstructure-config.md §2`](../docs/05-microstructure-config.md#2-config-model).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::auth::{AccountProvision, CompIdBinding};
use crate::exchange::{
    Cents, Hash32, Instrument, InstrumentStatus, LineageId, OptionStyle, Symbol, SymbolError,
    SymbolParser, validate_venue_expiry,
};
use crate::market_maker::{
    DIRECTIONAL_SKEW_MAX, DIRECTIONAL_SKEW_MIN, SIZE_SCALAR_MAX, SIZE_SCALAR_MIN,
    SPREAD_MULTIPLIER_MAX, SPREAD_MULTIPLIER_MIN,
};
use crate::models::{AccountId, Permission};
use option_chain_orderbook::utils::format_expiration_yyyymmdd;
use optionstratlib::ExpirationDate;

// ============================================================================
// Defaults
// ============================================================================

/// Default REST/WS bind address (`FAUXCHANGE_HTTP_ADDR`).
pub const DEFAULT_HTTP_ADDR: &str = "0.0.0.0:8080";
/// Default FIX 4.4 bind address (`FAUXCHANGE_FIX_ADDR`).
pub const DEFAULT_FIX_ADDR: &str = "0.0.0.0:9878";
/// Default FIX gateway enablement (`[fix] enabled`) — **disabled** until the
/// session FSM (#038), order routing (#039), and market data (#040) land, so a
/// released image does not open a raw-TCP port answering only the #037 stub. The
/// operator opts in explicitly; the acceptor spawns only when this is `true`
/// ([06 §5](../docs/06-deployment.md#5-ports-and-endpoints)).
pub const DEFAULT_FIX_ENABLED: bool = false;
/// Default FIX venue connection cap (`[fix] connection_cap`) — the maximum number
/// of concurrent FIX sessions; the N+1th connection is refused, not queued. A
/// **DoS control** ([08 §5](../docs/08-threat-model.md#5-denial-of-service-posture)),
/// not a fairness knob.
pub const DEFAULT_FIX_CONNECTION_CAP: usize = 256;
/// Default FIX per-session outbound mailbox depth (`[fix] mailbox_depth`) — the
/// bounded `mpsc` between the session and its socket writer; past the bound a full
/// mailbox surfaces a typed busy and closes the session, never an unbounded queue
/// (a **DoS control**, [08 §5](../docs/08-threat-model.md#5-denial-of-service-posture)).
pub const DEFAULT_FIX_MAILBOX_DEPTH: usize = 256;
/// Default FIX maximum on-the-wire frame size in **bytes** (`[fix]
/// max_frame_bytes`) — the byte cap enforced at the framing boundary; an oversize
/// frame is rejected with no unbounded allocation and no panic (a **DoS control**,
/// [08 §4](../docs/08-threat-model.md#4-untrusted-input-hardening)). `256` KiB is
/// generous for a conformant order / market-data message while replacing the
/// codec's 1 MiB default.
pub const DEFAULT_FIX_MAX_FRAME_BYTES: usize = 256 * 1024;

/// The inclusive maximum for `[fix] connection_cap` / `[fix] mailbox_depth` — a
/// coarse sanity ceiling so a config typo (a nonsensical `10_000_000_000` cap)
/// fails fast at boot rather than reserving an absurd semaphore/channel.
pub const FIX_MAX_CONNECTION_CAP: usize = 65_536;
/// The inclusive maximum for `[fix] mailbox_depth`.
pub const FIX_MAX_MAILBOX_DEPTH: usize = 65_536;
/// The inclusive minimum for `[fix] max_frame_bytes` — below this a legitimate FIX
/// frame (a logon plus header/trailer) would not fit, so a smaller value is a
/// misconfiguration.
pub const FIX_MIN_MAX_FRAME_BYTES: usize = 512;
/// The inclusive maximum for `[fix] max_frame_bytes` (`16` MiB) — a coarse ceiling
/// bounding the per-connection read buffer.
pub const FIX_MAX_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Default FIX read-idle timeout in **seconds** (`[fix] idle_timeout_secs`) — a
/// connection that sends no bytes for this long is closed, releasing its cap slot.
/// A **connection-hygiene DoS control** ([08 §5](../docs/08-threat-model.md#5-denial-of-service-posture)):
/// without it a Slowloris of silent sockets pins the connection cap. This is the
/// pre-#038 bound; the negotiated FIX heartbeat (`HeartBtInt`) refines it in #038.
pub const DEFAULT_FIX_IDLE_TIMEOUT_SECS: u64 = 30;
/// The inclusive minimum for `[fix] idle_timeout_secs` — at least `1` second (a
/// `0` timeout would close every connection instantly).
pub const FIX_MIN_IDLE_TIMEOUT_SECS: u64 = 1;
/// The inclusive maximum for `[fix] idle_timeout_secs` (`86_400` = 24h) — a coarse
/// ceiling so a typo cannot disable the hygiene bound outright.
pub const FIX_MAX_IDLE_TIMEOUT_SECS: u64 = 86_400;

/// Default FIX logon window in **seconds** (`[fix] logon_timeout_secs`) — how long
/// the acceptor session FSM waits in `AwaitingLogon` for a `Logon (A)` before
/// closing the connection (#038, [03 §5.2](../docs/03-protocol-surfaces.md#52-session-management--the-acceptor-fsm-and-checked-counters)).
pub const DEFAULT_FIX_LOGON_TIMEOUT_SECS: u64 = 10;
/// The inclusive minimum for `[fix] logon_timeout_secs` — at least `1` second.
pub const FIX_MIN_LOGON_TIMEOUT_SECS: u64 = 1;
/// The inclusive maximum for `[fix] logon_timeout_secs` (`300` = 5 min) — a coarse
/// ceiling so a typo cannot let an un-authenticated socket linger.
pub const FIX_MAX_LOGON_TIMEOUT_SECS: u64 = 300;

/// Default FIX maximum negotiated heartbeat in **seconds**
/// (`[fix] max_heart_bt_int_secs`) — the largest `HeartBtInt (108)` the acceptor
/// accepts at logon; a larger (or zero) proposal is refused (#038).
pub const DEFAULT_FIX_MAX_HEART_BT_INT_SECS: u32 = 60;
/// The inclusive minimum for `[fix] max_heart_bt_int_secs` — at least `1` second
/// (a `0` ceiling would refuse every heartbeat proposal).
pub const FIX_MIN_MAX_HEART_BT_INT_SECS: u32 = 1;
/// The inclusive maximum for `[fix] max_heart_bt_int_secs` (`3_600` = 1h) — a
/// coarse ceiling so a client cannot negotiate an effectively-dead session.
pub const FIX_MAX_MAX_HEART_BT_INT_SECS: u32 = 3_600;

/// The inclusive ceiling on the **product** `connection_cap × max_frame_bytes`
/// (`1` GiB) — the worst-case aggregate per-connection read-buffer reservation
/// across all live sessions. Each knob is range-checked independently, but nothing
/// else bounds their product; a typed [`ConfigError::BadFixValue`] at boot refuses
/// a combination whose worst case would reserve an absurd amount of memory (a
/// **DoS control**, [08 §5](../docs/08-threat-model.md#5-denial-of-service-posture)).
pub const FIX_MAX_AGGREGATE_FRAME_BYTES: usize = 1024 * 1024 * 1024;
/// Default run-level seed (`FAUXCHANGE_SEED`) — a deterministic `0`.
pub const DEFAULT_SEED: u64 = 0;
/// Default clock mode (`FAUXCHANGE_CLOCK`).
pub const DEFAULT_CLOCK: ClockMode = ClockMode::Realtime;
/// Default accelerated multiplier (`[clock] multiplier`) — `60×`, one wall second
/// per virtual minute — consumed only when the mode is `accelerated`.
pub const DEFAULT_CLOCK_MULTIPLIER: u32 = crate::simulation::DEFAULT_ACCEL_MULTIPLIER;
/// Default stepped virtual interval in **milliseconds** (`[clock]
/// step_interval_ms`) — one virtual minute per step, aligned with the price-walk
/// step — consumed only when the mode is `stepped`.
pub const DEFAULT_CLOCK_STEP_INTERVAL_MS: u64 = crate::simulation::DEFAULT_STEP_INTERVAL_MS;
/// Default log format (`FAUXCHANGE_LOG_FORMAT`) — human-readable locally; the
/// production image sets `json` ([06 §9](../docs/06-deployment.md#9-observability)).
pub const DEFAULT_LOG_FORMAT: LogFormat = LogFormat::Pretty;

/// Default **operational expiry** time-of-day (`[expiry_lifecycle] expiry_time`) —
/// `08:00:00 UTC`, the upstream `ExpiryCycleConfig` default (verified
/// `option-chain-orderbook` v0.7.0). Drives admission closure + the `Active →
/// Settling` transition; **distinct** from the `23:59:59 UTC` symbol-identity
/// instant ([01 §5](../docs/01-domain-model.md#5-instruments-and-the-symbol-grammar)).
pub const DEFAULT_EXPIRY_TIME: &str = "08:00:00";
/// Default **operational settlement** time-of-day (`[expiry_lifecycle]
/// settlement_time`) — `08:30:00 UTC`, the upstream `ExpiryCycleConfig` default.
/// Drives the `Settling → Expired` transition; must be at or after the operational
/// expiry and strictly before the identity instant.
pub const DEFAULT_SETTLEMENT_TIME: &str = "08:30:00";

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
    /// message is a **scrubbed** diagnostic — the parser's canonical error message
    /// plus a computed line/column — and **never** the source-line snippet. The
    /// upstream `toml::de::Error`'s own `Display` renders the offending source
    /// line, which for a malformed seed file could echo a `fix_password` /
    /// `bootstrap_secret` / `database_url` literal; because this error surfaces
    /// from `Config::load` **before** the redacting `tracing` subscriber is
    /// installed (`main.rs`) and prints to stderr, that snippet is stripped here
    /// so no secret can leak into a startup error / container log
    /// ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
    #[error("failed to parse config file: {message}")]
    TomlParse {
        /// The scrubbed, snippet-free diagnostic (safe to echo).
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
    /// A `[clock]` knob (`multiplier` / `step_interval_ms`) did not parse as its
    /// expected integer. Names the field and value.
    #[error("invalid clock value '{value}' for {field}: expected a positive integer")]
    BadClockValue {
        /// The config field (`multiplier` / `step_interval_ms`).
        field: &'static str,
        /// The offending value.
        value: String,
    },
    /// A `[fix]` knob (`enabled` / `connection_cap` / `mailbox_depth` /
    /// `max_frame_bytes`) did not parse, or fell outside its validated range. The
    /// FIX DoS bounds are **security controls** validated at boot, so an
    /// out-of-range value fails the process fast rather than reserving an absurd
    /// resource ([08 §5](../docs/08-threat-model.md#5-denial-of-service-posture)).
    /// Names the field, value, and the expectation.
    #[error("invalid fix value '{value}' for {field}: {reason}")]
    BadFixValue {
        /// The config field (`enabled` / `connection_cap` / `mailbox_depth` /
        /// `max_frame_bytes`).
        field: &'static str,
        /// The offending value.
        value: String,
        /// What the field expected (a type or a range).
        reason: String,
    },
    /// An `[expiry_lifecycle]` operational time (`expiry_time` / `settlement_time`)
    /// did not parse as an `HH:MM:SS` time-of-day in `00:00:00..=23:59:59`.
    #[error(
        "invalid expiry_lifecycle value '{value}' for {field}: expected an HH:MM:SS UTC time-of-day"
    )]
    BadOperationalTime {
        /// The config field (`expiry_time` / `settlement_time`).
        field: &'static str,
        /// The offending value.
        value: String,
    },
    /// The operational `settlement_time` fell **before** the operational
    /// `expiry_time`. Settlement must be at or after expiry — naming the offending
    /// combination.
    #[error(
        "expiry_lifecycle settlement_time ({settlement}) must be at or after expiry_time ({expiry})"
    )]
    OperationalSettlementBeforeExpiry {
        /// The configured operational expiry time.
        expiry: String,
        /// The configured operational settlement time (the earlier one).
        settlement: String,
    },
    /// An operational time was **not strictly before** the `23:59:59 UTC`
    /// symbol-identity instant. The operational times must not reach or cross the
    /// identity instant (which is reserved for symbol identity / aliasing, not a
    /// lifecycle transition).
    #[error(
        "expiry_lifecycle {field} ({value}) must be strictly before the 23:59:59 UTC identity instant"
    )]
    OperationalTimeNotBeforeIdentity {
        /// The config field (`expiry_time` / `settlement_time`).
        field: &'static str,
        /// The offending time-of-day.
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
    /// A seeded expiry was a relative `ExpirationDate::Days` value. It is
    /// wall-clock-relative and would map to a different calendar date on replay,
    /// so the venue requires an absolute `ExpirationDate::DateTime` (a `YYYYMMDD`
    /// date or a canonical `23:59:59 UTC` instant) ([CLAUDE.md](../CLAUDE.md)).
    #[error(
        "seed expiry '{value}' for instrument '{underlying}' is a relative Days expiry; \
         use an absolute YYYYMMDD date (it breaks replay)"
    )]
    SeedDaysExpiry {
        /// The seeded underlying the expiry belongs to.
        underlying: String,
        /// The offending expiry token.
        value: String,
    },
    /// A seeded expiry could not be resolved to a canonical venue expiry (bad
    /// date, or a non-canonical time-of-day that would alias the symbol).
    #[error("seed expiry '{value}' for instrument '{underlying}' is invalid: {reason}")]
    SeedInvalidExpiry {
        /// The seeded underlying the expiry belongs to.
        underlying: String,
        /// The offending expiry token.
        value: String,
        /// The resolution failure reason.
        reason: String,
    },
    /// A seeded instrument had a malformed strike ladder — empty, a zero strike,
    /// or a duplicate strike.
    #[error("seed instrument '{underlying}' has an invalid strike ladder: {reason}")]
    SeedInvalidStrikeLadder {
        /// The seeded underlying.
        underlying: String,
        /// What is wrong with the ladder.
        reason: String,
    },
    /// A seeded instrument field was out of range (a zero opening price, an empty
    /// expiration list, an unknown option style).
    #[error("seed instrument '{underlying}' is invalid: {reason}")]
    SeedInvalidInstrument {
        /// The seeded underlying.
        underlying: String,
        /// What is wrong with the instrument.
        reason: String,
    },
    /// A seeded account was invalid (a bad owner-hash literal, a FIX password
    /// with no username, or an empty permission set). Never carries the secret.
    #[error("seed account '{id}' is invalid: {reason}")]
    SeedInvalidAccount {
        /// The seeded account id (safe to echo — not a secret).
        id: String,
        /// What is wrong with the account.
        reason: String,
    },
    /// A seeded market-maker persona knob was non-finite or out of range, or a
    /// referenced persona was not defined.
    #[error("seed market-maker persona is invalid: {reason}")]
    SeedInvalidPersona {
        /// What is wrong with the persona configuration.
        reason: String,
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

/// The `[fix]` section: the FIX 4.4 gateway toggle, bind address, and its bounded
/// DoS-control knobs (connection cap, per-session mailbox depth, max frame size).
///
/// The acceptor spawns only when [`enabled`](Self::enabled); the caps are
/// **security controls** validated at boot, not fairness knobs
/// ([08 §5](../docs/08-threat-model.md#5-denial-of-service-posture)).
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FixConfig {
    /// Whether the FIX 4.4 acceptor is spawned (`[fix] enabled`). Disabled by
    /// default until the session FSM / order routing / market data land
    /// (#038–#040).
    pub enabled: bool,
    /// The FIX 4.4 bind address (`FAUXCHANGE_FIX_ADDR`).
    pub fix_addr: SocketAddr,
    /// The venue connection cap (`[fix] connection_cap`) — concurrent FIX sessions
    /// past this are refused, not queued.
    pub connection_cap: usize,
    /// The per-session outbound mailbox depth (`[fix] mailbox_depth`) — the bounded
    /// `mpsc` to the socket writer; a full mailbox surfaces a typed busy and closes
    /// the session.
    pub mailbox_depth: usize,
    /// The maximum on-the-wire frame size in **bytes** (`[fix] max_frame_bytes`) —
    /// an oversize frame is rejected at the framing boundary with no unbounded
    /// allocation and no panic.
    pub max_frame_bytes: usize,
    /// The read-idle timeout in **seconds** (`[fix] idle_timeout_secs`) — a
    /// connection sending no bytes for this long is closed, releasing its cap slot
    /// (connection hygiene against a silent-socket Slowloris; refined by the #038
    /// negotiated heartbeat).
    pub idle_timeout_secs: u64,
    /// The logon window in **seconds** (`[fix] logon_timeout_secs`) — how long the
    /// session FSM waits for a `Logon (A)` before closing an un-authenticated
    /// connection (#038).
    pub logon_timeout_secs: u64,
    /// The maximum negotiated `HeartBtInt (108)` in **seconds**
    /// (`[fix] max_heart_bt_int_secs`) — a logon proposing a larger (or zero)
    /// heartbeat interval is refused (#038).
    pub max_heart_bt_int_secs: u32,
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

/// The `[clock]` section: the venue clock mode and its mode-specific knobs.
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClockConfig {
    /// The clock mode (`FAUXCHANGE_CLOCK` / `--clock`).
    pub mode: ClockMode,
    /// The accelerated multiplier (`[clock] multiplier`) — virtual milliseconds per
    /// wall millisecond; consumed only when `mode == Accelerated`
    /// ([04 §5](../docs/04-market-data-and-replay.md#5-clock-control)).
    pub multiplier: u32,
    /// The stepped virtual interval (`[clock] step_interval_ms`) — the amount one
    /// stepped-clock advance moves virtual time; consumed only when
    /// `mode == Stepped`.
    pub step_interval_ms: u64,
}

impl ClockConfig {
    /// Maps this config section onto the runtime venue-clock construction
    /// parameters, folding the parameterless [`ClockMode`] token together with the
    /// mode-specific knob it selects, and pinning the virtual epoch to `start_ms`
    /// (the price-walk epoch, so the clock and the walk share one time base).
    #[must_use]
    pub fn to_venue_clock_config(&self, start_ms: u64) -> crate::simulation::VenueClockConfig {
        let mode = match self.mode {
            ClockMode::Realtime => crate::simulation::ClockMode::Realtime,
            ClockMode::Accelerated => crate::simulation::ClockMode::Accelerated {
                multiplier: self.multiplier,
            },
            ClockMode::Stepped => crate::simulation::ClockMode::Stepped {
                step_ms: self.step_interval_ms,
            },
        };
        crate::simulation::VenueClockConfig { mode, start_ms }
    }
}

/// The canonical `23:59:59 UTC` symbol-**identity** instant, as seconds since UTC
/// midnight (`86_399`). It fixes symbol round-trip and the aliasing rule and
/// **nothing else** — it is *not* an operational time. Operational expiry /
/// settlement must fall strictly before it
/// ([01 §5](../docs/01-domain-model.md#5-instruments-and-the-symbol-grammar)).
pub const IDENTITY_EXPIRY_SECS: u32 = 23 * 3_600 + 59 * 60 + 59;

/// A UTC **time-of-day** (`HH:MM:SS`), stored as whole seconds since midnight
/// (`0..=86_399`).
///
/// Dependency-free by design: the venue hand-rolls civil-time handling rather than
/// pull a date library (see [`crate::gateway`]'s RFC3339 formatter), so this needs
/// no `chrono`. The v0.5 lifecycle scheduler that *consumes* these times maps them
/// onto the upstream `ExpiryCycleConfig`'s `chrono::NaiveTime` at that seam;
/// `fauxchange` only stores and **validates** them here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OperationalTime {
    secs_since_midnight: u32,
}

impl OperationalTime {
    /// Builds a time-of-day from `hour:minute:second`, or `None` if any component
    /// is out of range (`hour > 23`, `minute > 59`, `second > 59`).
    #[must_use]
    pub const fn from_hms(hour: u32, minute: u32, second: u32) -> Option<Self> {
        if hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        Some(Self {
            secs_since_midnight: hour * 3_600 + minute * 60 + second,
        })
    }

    /// Parses an `HH:MM:SS` (24-hour, zero-padded or not) string into a
    /// time-of-day. Returns `None` on a wrong shape or an out-of-range component.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let mut parts = raw.trim().split(':');
        let hour = parts.next()?.parse::<u32>().ok()?;
        let minute = parts.next()?.parse::<u32>().ok()?;
        let second = parts.next()?.parse::<u32>().ok()?;
        if parts.next().is_some() {
            return None; // more than three colon-separated fields
        }
        Self::from_hms(hour, minute, second)
    }

    /// The seconds since UTC midnight (`0..=86_399`).
    #[must_use]
    #[inline]
    pub const fn secs_since_midnight(&self) -> u32 {
        self.secs_since_midnight
    }
}

impl std::fmt::Display for OperationalTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.secs_since_midnight;
        write!(f, "{:02}:{:02}:{:02}", s / 3_600, (s % 3_600) / 60, s % 60)
    }
}

/// The `[expiry_lifecycle]` section: the **operational** expiry / settlement
/// times-of-day, distinct from the `23:59:59 UTC` symbol-identity instant.
///
/// **Validated at startup now; not yet consumed.** The lifecycle scheduler that
/// acts on these instants (the scoped `MassCancel` + `SetInstrumentStatus(Settling
/// / Expired)` sequence) lands with the v0.5 halt-scenario work — the milestone's
/// explicit out-of-scope. Here the venue only enforces that the configured
/// combination is coherent, so an invalid one fails fast at load rather than
/// surfacing later ([01 §5](../docs/01-domain-model.md#5-instruments-and-the-symbol-grammar),
/// [02 §5.3](../docs/02-matching-architecture.md#5-determinism)).
///
/// `#[non_exhaustive]` for forward-compatible field additions (see [`ServerConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExpiryLifecycleConfig {
    /// Operational expiry time-of-day (default `08:00:00 UTC`) — admission closure
    /// and the `Active → Settling` transition.
    pub expiry_time: OperationalTime,
    /// Operational settlement time-of-day (default `08:30:00 UTC`) — the `Settling
    /// → Expired` transition. At or after `expiry_time`, strictly before the
    /// identity instant.
    pub settlement_time: OperationalTime,
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
/// downstream crates; within-crate construction (`Config::assemble`, tests) is
/// unaffected.
///
/// `Eq` is **not** derived: the [`SeedManifest`] carries the market-maker persona
/// `f64` knobs, which are only `PartialEq`. `Debug` stays safe to log — the
/// seed's account credentials redact through [`AccountProvision`]'s own `Debug`.
#[derive(Debug, Clone, PartialEq)]
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
    /// The operational expiry / settlement times-of-day (validated at load;
    /// consumed by the v0.5 lifecycle scheduler).
    pub expiry_lifecycle: ExpiryLifecycleConfig,
    /// The one run-level seed.
    pub determinism: DeterminismConfig,
    /// The token-issuance bootstrap secret.
    pub auth: AuthConfig,
    /// The log output format.
    pub logging: LoggingConfig,
    /// The resolved, validated scenario seed manifest (accounts, instruments,
    /// opening prices, personas) applied by the bounded seeding phase before the
    /// venue flips to serving. Populated **only** from the file layer (the seed
    /// sections have no env/CLI override); empty when no `--config` file (or no
    /// seed sections) is supplied ([06 §7](../docs/06-deployment.md#7-seed-data-and-scenarios)).
    pub seed: SeedManifest,
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
        // The seed sections live on the file layer only (no env/CLI override), so
        // the file config is parsed once and its raw scalar layer + resolved seed
        // manifest are extracted together (seed borrows before `into_raw` moves).
        let (file, seed) = match &cli.config_path {
            Some(path) => {
                let file_config = read_file_config(path)?;
                let seed = file_config.seed_manifest()?;
                (file_config.into_raw(), seed)
            }
            None => (RawConfig::default(), SeedManifest::default()),
        };
        let env = raw_from_env(env);
        let mut config = Self::assemble(file, env, cli.raw)?;
        config.seed = seed;
        Ok(config)
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
            "server.http_addr={http} fix.enabled={fix_enabled} fix.fix_addr={fix} \
             fix.connection_cap={fix_cap} fix.mailbox_depth={fix_mbx} \
             fix.max_frame_bytes={fix_frame} fix.idle_timeout_secs={fix_idle} \
             persistence.backend={backend} persistence.database_url={database_url} \
             persistence.pool_max_connections={pool} persistence.slow_acquire_ms={slow} \
             clock.mode={clock} \
             expiry_lifecycle.expiry_time={expiry_time} \
             expiry_lifecycle.settlement_time={settlement_time} \
             determinism.seed={seed} \
             auth.bootstrap_secret={bootstrap_secret} logging.format={log}",
            http = self.server.http_addr,
            fix_enabled = self.fix.enabled,
            fix = self.fix.fix_addr,
            fix_cap = self.fix.connection_cap,
            fix_mbx = self.fix.mailbox_depth,
            fix_frame = self.fix.max_frame_bytes,
            fix_idle = self.fix.idle_timeout_secs,
            backend = self.persistence.backend().as_str(),
            pool = self.persistence.pool_max_connections,
            slow = self.persistence.slow_acquire_ms,
            clock = self.clock.mode.as_str(),
            expiry_time = self.expiry_lifecycle.expiry_time,
            settlement_time = self.expiry_lifecycle.settlement_time,
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
    fix_enabled: Option<String>,
    fix_connection_cap: Option<String>,
    fix_mailbox_depth: Option<String>,
    fix_max_frame_bytes: Option<String>,
    fix_idle_timeout_secs: Option<String>,
    fix_logon_timeout_secs: Option<String>,
    fix_max_heart_bt_int_secs: Option<String>,
    database_url: Option<String>,
    db_pool_max_connections: Option<String>,
    db_slow_acquire_ms: Option<String>,
    clock: Option<String>,
    clock_multiplier: Option<String>,
    clock_step_interval_ms: Option<String>,
    expiry_time: Option<String>,
    settlement_time: Option<String>,
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
            fix_enabled: Some(DEFAULT_FIX_ENABLED.to_string()),
            fix_connection_cap: Some(DEFAULT_FIX_CONNECTION_CAP.to_string()),
            fix_mailbox_depth: Some(DEFAULT_FIX_MAILBOX_DEPTH.to_string()),
            fix_max_frame_bytes: Some(DEFAULT_FIX_MAX_FRAME_BYTES.to_string()),
            fix_idle_timeout_secs: Some(DEFAULT_FIX_IDLE_TIMEOUT_SECS.to_string()),
            fix_logon_timeout_secs: Some(DEFAULT_FIX_LOGON_TIMEOUT_SECS.to_string()),
            fix_max_heart_bt_int_secs: Some(DEFAULT_FIX_MAX_HEART_BT_INT_SECS.to_string()),
            database_url: None,
            db_pool_max_connections: Some(DEFAULT_DB_POOL_MAX_CONNECTIONS.to_string()),
            db_slow_acquire_ms: Some(DEFAULT_DB_SLOW_ACQUIRE_MS.to_string()),
            clock: Some(DEFAULT_CLOCK.as_str().to_string()),
            clock_multiplier: Some(DEFAULT_CLOCK_MULTIPLIER.to_string()),
            clock_step_interval_ms: Some(DEFAULT_CLOCK_STEP_INTERVAL_MS.to_string()),
            expiry_time: Some(DEFAULT_EXPIRY_TIME.to_string()),
            settlement_time: Some(DEFAULT_SETTLEMENT_TIME.to_string()),
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
        if other.fix_enabled.is_some() {
            self.fix_enabled = other.fix_enabled;
        }
        if other.fix_connection_cap.is_some() {
            self.fix_connection_cap = other.fix_connection_cap;
        }
        if other.fix_mailbox_depth.is_some() {
            self.fix_mailbox_depth = other.fix_mailbox_depth;
        }
        if other.fix_max_frame_bytes.is_some() {
            self.fix_max_frame_bytes = other.fix_max_frame_bytes;
        }
        if other.fix_idle_timeout_secs.is_some() {
            self.fix_idle_timeout_secs = other.fix_idle_timeout_secs;
        }
        if other.fix_logon_timeout_secs.is_some() {
            self.fix_logon_timeout_secs = other.fix_logon_timeout_secs;
        }
        if other.fix_max_heart_bt_int_secs.is_some() {
            self.fix_max_heart_bt_int_secs = other.fix_max_heart_bt_int_secs;
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
        if other.clock_multiplier.is_some() {
            self.clock_multiplier = other.clock_multiplier;
        }
        if other.clock_step_interval_ms.is_some() {
            self.clock_step_interval_ms = other.clock_step_interval_ms;
        }
        if other.expiry_time.is_some() {
            self.expiry_time = other.expiry_time;
        }
        if other.settlement_time.is_some() {
            self.settlement_time = other.settlement_time;
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
        let fix_enabled = parse_fix_bool("enabled", self.fix_enabled, DEFAULT_FIX_ENABLED)?;
        let fix_connection_cap = parse_fix_usize(
            "connection_cap",
            self.fix_connection_cap,
            DEFAULT_FIX_CONNECTION_CAP,
            1,
            FIX_MAX_CONNECTION_CAP,
        )?;
        let fix_mailbox_depth = parse_fix_usize(
            "mailbox_depth",
            self.fix_mailbox_depth,
            DEFAULT_FIX_MAILBOX_DEPTH,
            1,
            FIX_MAX_MAILBOX_DEPTH,
        )?;
        let fix_max_frame_bytes = parse_fix_usize(
            "max_frame_bytes",
            self.fix_max_frame_bytes,
            DEFAULT_FIX_MAX_FRAME_BYTES,
            FIX_MIN_MAX_FRAME_BYTES,
            FIX_MAX_MAX_FRAME_BYTES,
        )?;
        let fix_idle_timeout_secs = parse_fix_u64(
            "idle_timeout_secs",
            self.fix_idle_timeout_secs,
            DEFAULT_FIX_IDLE_TIMEOUT_SECS,
            FIX_MIN_IDLE_TIMEOUT_SECS,
            FIX_MAX_IDLE_TIMEOUT_SECS,
        )?;
        let fix_logon_timeout_secs = parse_fix_u64(
            "logon_timeout_secs",
            self.fix_logon_timeout_secs,
            DEFAULT_FIX_LOGON_TIMEOUT_SECS,
            FIX_MIN_LOGON_TIMEOUT_SECS,
            FIX_MAX_LOGON_TIMEOUT_SECS,
        )?;
        let fix_max_heart_bt_int_secs = u32::try_from(parse_fix_u64(
            "max_heart_bt_int_secs",
            self.fix_max_heart_bt_int_secs,
            u64::from(DEFAULT_FIX_MAX_HEART_BT_INT_SECS),
            u64::from(FIX_MIN_MAX_HEART_BT_INT_SECS),
            u64::from(FIX_MAX_MAX_HEART_BT_INT_SECS),
        )?)
        .unwrap_or(FIX_MAX_MAX_HEART_BT_INT_SECS);
        // Bound the PRODUCT `connection_cap × max_frame_bytes` (each knob is
        // range-checked alone; nothing else caps their worst-case aggregate
        // per-connection read-buffer reservation). A typed error refuses an absurd
        // combination at boot rather than reserving it. `checked_mul` treats an
        // overflow as over-ceiling.
        let over_aggregate = match fix_connection_cap.checked_mul(fix_max_frame_bytes) {
            Some(product) => product > FIX_MAX_AGGREGATE_FRAME_BYTES,
            None => true,
        };
        if over_aggregate {
            return Err(ConfigError::BadFixValue {
                field: "connection_cap",
                value: format!("{fix_connection_cap} × {fix_max_frame_bytes} max_frame_bytes"),
                reason: format!(
                    "connection_cap × max_frame_bytes exceeds the {FIX_MAX_AGGREGATE_FRAME_BYTES}-byte aggregate ceiling"
                ),
            });
        }
        let mode = parse_clock(self.clock)?;
        let clock_multiplier = parse_clock_u32(
            "multiplier",
            self.clock_multiplier,
            DEFAULT_CLOCK_MULTIPLIER,
        )?;
        let clock_step_interval_ms = parse_clock_u64(
            "step_interval_ms",
            self.clock_step_interval_ms,
            DEFAULT_CLOCK_STEP_INTERVAL_MS,
        )?;
        let expiry_lifecycle = parse_expiry_lifecycle(self.expiry_time, self.settlement_time)?;
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
            fix: FixConfig {
                enabled: fix_enabled,
                fix_addr,
                connection_cap: fix_connection_cap,
                mailbox_depth: fix_mailbox_depth,
                max_frame_bytes: fix_max_frame_bytes,
                idle_timeout_secs: fix_idle_timeout_secs,
                logon_timeout_secs: fix_logon_timeout_secs,
                max_heart_bt_int_secs: fix_max_heart_bt_int_secs,
            },
            persistence: PersistenceConfig {
                database_url: self.database_url.map(Secret::new),
                pool_max_connections,
                slow_acquire_ms,
            },
            clock: ClockConfig {
                mode,
                multiplier: clock_multiplier,
                step_interval_ms: clock_step_interval_ms,
            },
            expiry_lifecycle,
            determinism: DeterminismConfig { seed },
            auth: AuthConfig {
                bootstrap_secret: self.bootstrap_secret.map(Secret::new),
            },
            logging: LoggingConfig { format },
            // The seed manifest is not carried on `RawConfig` (it has no
            // env/CLI override); `Config::load_from` overwrites this default with
            // the parsed file manifest. Direct `assemble` callers (tests) get the
            // empty manifest.
            seed: SeedManifest::default(),
        })
    }
}

// ============================================================================
// Layer sources — file (TOML), environment, CLI
// ============================================================================

/// Reads and parses a TOML config file into the typed [`FileConfig`] document —
/// the source of **both** the scalar [`RawConfig`] layer ([`FileConfig::into_raw`])
/// and the resolved [`SeedManifest`] ([`FileConfig::seed_manifest`]).
fn read_file_config(path: &Path) -> Result<FileConfig, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::FileRead {
        path: path.display().to_string(),
        source,
    })?;
    parse_file_config(&contents)
}

/// Parses a TOML config document into the typed [`FileConfig`], enforcing
/// `deny_unknown_fields` so a typo — top-level, in a scalar section, or **inside a
/// seeded `[accounts.*]` / `[instruments.*]` / `[market_maker.*]` table** —
/// becomes a [`ConfigError::UnknownKey`] naming the key.
fn parse_file_config(contents: &str) -> Result<FileConfig, ConfigError> {
    toml::from_str(contents).map_err(|error| map_toml_error(&error, contents))
}

/// Parses a TOML config document into a [`RawConfig`] layer (dropping the seed
/// sections). Pure — the seam unit tests exercise the scalar file layer with it.
#[cfg(test)]
fn raw_from_toml_str(contents: &str) -> Result<RawConfig, ConfigError> {
    Ok(parse_file_config(contents)?.into_raw())
}

/// The config keys whose VALUES are secrets and must never appear in an error or
/// log ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
const SECRET_KEY_TOKENS: &[&str] = &[
    "fix_password",
    "bootstrap_secret",
    "database_url",
    "password",
];

/// Maps a TOML deserialize failure to a typed [`ConfigError`], extracting the
/// offending key from a `deny_unknown_fields` rejection so it is named.
///
/// **Secret-safe (SECURITY):** it builds the diagnostic from the parser's
/// **canonical message** ([`toml::de::Error::message`], snippet-free) plus a
/// line/column computed from [`toml::de::Error::span`] against `contents` — it
/// **never** uses `error.to_string()`, whose `Display` renders the offending
/// source line and could echo a `fix_password` / `bootstrap_secret` /
/// `database_url` literal into a startup error / container log. A belt-and-braces
/// [`scrub_secret_literals`] pass redacts any quoted literal if the canonical
/// message ever names a secret-bearing key.
fn map_toml_error(error: &toml::de::Error, contents: &str) -> ConfigError {
    let message = error.message();
    // A `deny_unknown_fields` rejection names the offending key in the canonical
    // message (a key name, not a value) — surface it as UnknownKey.
    if let Some(key) = extract_unknown_field(message) {
        return ConfigError::UnknownKey { key };
    }

    let scrubbed = scrub_secret_literals(message);
    let text = match error.span() {
        Some(span) => {
            let (line, column) = line_col(contents, span.start);
            format!("TOML parse error at line {line}, column {column}: {scrubbed}")
        }
        None => format!("TOML parse error: {scrubbed}"),
    };
    ConfigError::TomlParse { message: text }
}

/// Extracts the field name from serde's `` unknown field `x` `` diagnostic. Reads
/// the **canonical message** (not `Display`), so no source snippet is involved.
fn extract_unknown_field(text: &str) -> Option<String> {
    const MARKER: &str = "unknown field `";
    let start = text.find(MARKER)? + MARKER.len();
    let rest = text.get(start..)?;
    let end = rest.find('`')?;
    rest.get(..end).map(str::to_string)
}

/// The 1-based `(line, column)` of a byte offset into `contents` — a snippet-free
/// location for a scrubbed parse error (never emits the source line itself).
fn line_col(contents: &str, byte_index: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut column = 1usize;
    for (offset, ch) in contents.char_indices() {
        if offset >= byte_index {
            break;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

/// Belt-and-braces redaction (SECURITY): if the (already snippet-free) parser
/// message names a secret-bearing key ([`SECRET_KEY_TOKENS`]), replace every
/// double-quoted literal's contents with `<redacted>` so a value a future parser
/// change might fold into the message cannot ride along. Non-secret messages are
/// returned unchanged, keeping their diagnostic value.
fn scrub_secret_literals(message: &str) -> String {
    let lower = message.to_ascii_lowercase();
    if !SECRET_KEY_TOKENS.iter().any(|token| lower.contains(token)) {
        return message.to_string();
    }
    let mut out = String::with_capacity(message.len());
    let mut chars = message.chars();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            out.push('"');
            let mut had_contents = false;
            for inner in chars.by_ref() {
                if inner == '"' {
                    break;
                }
                had_contents = true;
            }
            if had_contents {
                out.push_str(REDACTED);
            }
            out.push('"');
        } else {
            out.push(ch);
        }
    }
    out
}

/// Reads the environment layer via an injected lookup. An empty value is treated
/// as **unset** (matching the venue's `AUTH_BOOTSTRAP_SECRET` convention), so it
/// does not override an earlier layer.
fn raw_from_env<F: Fn(&str) -> Option<String>>(get: F) -> RawConfig {
    let pick = |key: &str| get(key).filter(|value| !value.is_empty());
    RawConfig {
        http_addr: pick("FAUXCHANGE_HTTP_ADDR"),
        fix_addr: pick("FAUXCHANGE_FIX_ADDR"),
        // The FIX gateway toggle + DoS-control knobs are file-only `[fix]` knobs
        // (no env/CLI override), so the env layer never supplies them.
        fix_enabled: None,
        fix_connection_cap: None,
        fix_mailbox_depth: None,
        fix_max_frame_bytes: None,
        fix_idle_timeout_secs: None,
        fix_logon_timeout_secs: None,
        fix_max_heart_bt_int_secs: None,
        database_url: pick("DATABASE_URL"),
        db_pool_max_connections: pick("FAUXCHANGE_DB_MAX_CONNECTIONS"),
        db_slow_acquire_ms: pick("FAUXCHANGE_DB_SLOW_ACQUIRE_MS"),
        clock: pick("FAUXCHANGE_CLOCK"),
        // The accelerated multiplier / stepped interval are file-only knobs (no
        // env/CLI override), so the env layer never supplies them.
        clock_multiplier: None,
        clock_step_interval_ms: None,
        // The operational expiry / settlement times are file-only `[expiry_lifecycle]`
        // knobs (no env/CLI override), so the env layer never supplies them.
        expiry_time: None,
        settlement_time: None,
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

/// Parses a `[clock]` `u32` knob (`multiplier`), clamping to a usable `>= 1` (a
/// `0` multiplier would freeze the accelerated clock), failing with
/// [`ConfigError::BadClockValue`] on a non-integer.
fn parse_clock_u32(
    field: &'static str,
    value: Option<String>,
    default: u32,
) -> Result<u32, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    match value.trim().parse::<u32>() {
        Ok(parsed) => Ok(parsed.max(1)),
        Err(_) => Err(ConfigError::BadClockValue { field, value }),
    }
}

/// Parses a `[clock]` `u64` knob (`step_interval_ms`), clamping to a usable
/// `>= 1` (a `0` interval would freeze the stepped clock), failing with
/// [`ConfigError::BadClockValue`] on a non-integer.
fn parse_clock_u64(
    field: &'static str,
    value: Option<String>,
    default: u64,
) -> Result<u64, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    match value.trim().parse::<u64>() {
        Ok(parsed) => Ok(parsed.max(1)),
        Err(_) => Err(ConfigError::BadClockValue { field, value }),
    }
}

/// Parses a `[fix]` boolean knob (`enabled`), accepting `true` / `false` (the
/// TOML boolean rendering), failing with [`ConfigError::BadFixValue`] on anything
/// else.
fn parse_fix_bool(
    field: &'static str,
    value: Option<String>,
    default: bool,
) -> Result<bool, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    match value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ConfigError::BadFixValue {
            field,
            value,
            reason: "expected a boolean (true / false)".to_string(),
        }),
    }
}

/// Parses a `[fix]` `usize` DoS-control knob and **range-checks** it against
/// `[min, max]` inclusive, failing with [`ConfigError::BadFixValue`] on a
/// non-integer or an out-of-range value. The bounds are validated at boot so a
/// nonsensical cap/depth/frame-size fails the process fast rather than reserving
/// an absurd resource (a **security control**, not a fairness knob).
fn parse_fix_usize(
    field: &'static str,
    value: Option<String>,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    let parsed = value
        .trim()
        .parse::<usize>()
        .map_err(|_| ConfigError::BadFixValue {
            field,
            value: value.clone(),
            reason: "expected a non-negative integer".to_string(),
        })?;
    if parsed < min || parsed > max {
        return Err(ConfigError::BadFixValue {
            field,
            value,
            reason: format!("expected an integer in {min}..={max}"),
        });
    }
    Ok(parsed)
}

/// Parses a `[fix]` `u64` knob (`idle_timeout_secs`) and **range-checks** it
/// against `[min, max]` inclusive, failing with [`ConfigError::BadFixValue`] on a
/// non-integer or an out-of-range value.
fn parse_fix_u64(
    field: &'static str,
    value: Option<String>,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    let parsed = value
        .trim()
        .parse::<u64>()
        .map_err(|_| ConfigError::BadFixValue {
            field,
            value: value.clone(),
            reason: "expected a non-negative integer".to_string(),
        })?;
    if parsed < min || parsed > max {
        return Err(ConfigError::BadFixValue {
            field,
            value,
            reason: format!("expected an integer in {min}..={max}"),
        });
    }
    Ok(parsed)
}

/// Parses and validates the `[expiry_lifecycle]` operational times into an
/// [`ExpiryLifecycleConfig`], enforcing the identity-vs-operational rule
/// ([01 §5](../docs/01-domain-model.md#5-instruments-and-the-symbol-grammar)):
/// each time is a valid `HH:MM:SS` UTC time-of-day, `settlement_time` is at or
/// after `expiry_time`, and both are **strictly before** the `23:59:59 UTC`
/// symbol-identity instant. Fails fast at load with a typed [`ConfigError`] naming
/// the offending value / combination.
fn parse_expiry_lifecycle(
    expiry_time: Option<String>,
    settlement_time: Option<String>,
) -> Result<ExpiryLifecycleConfig, ConfigError> {
    let expiry = parse_operational_time("expiry_time", expiry_time, DEFAULT_EXPIRY_TIME)?;
    let settlement =
        parse_operational_time("settlement_time", settlement_time, DEFAULT_SETTLEMENT_TIME)?;

    // Both operational times must sit strictly before the identity instant — it is
    // reserved for symbol identity / the aliasing rule, never a lifecycle transition.
    for (field, time) in [("expiry_time", expiry), ("settlement_time", settlement)] {
        if time.secs_since_midnight() >= IDENTITY_EXPIRY_SECS {
            return Err(ConfigError::OperationalTimeNotBeforeIdentity {
                field,
                value: time.to_string(),
            });
        }
    }
    // Settlement is at or after expiry — a settlement that precedes expiry is an
    // incoherent lifecycle order.
    if settlement < expiry {
        return Err(ConfigError::OperationalSettlementBeforeExpiry {
            expiry: expiry.to_string(),
            settlement: settlement.to_string(),
        });
    }

    Ok(ExpiryLifecycleConfig {
        expiry_time: expiry,
        settlement_time: settlement,
    })
}

/// Parses one `[expiry_lifecycle]` `HH:MM:SS` time-of-day, failing with
/// [`ConfigError::BadOperationalTime`] on a wrong shape or an out-of-range field.
fn parse_operational_time(
    field: &'static str,
    value: Option<String>,
    default: &str,
) -> Result<OperationalTime, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_string());
    OperationalTime::parse(&value).ok_or(ConfigError::BadOperationalTime { field, value })
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
///
/// Deliberately **not** `Debug` (SECURITY): it holds the seeded `[accounts.*]`
/// plaintext FIX passwords and the `[auth]` / `[persistence]` secrets, so — like
/// [`RawConfig`] — it must never be `{:?}`-logged. Resolved values are moved out
/// (secrets into [`Secret`], passwords hashed at provisioning) before any log.
#[derive(Default, Deserialize)]
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
    expiry_lifecycle: Option<FileExpiryLifecycle>,
    #[serde(default)]
    determinism: Option<FileDeterminism>,
    #[serde(default)]
    auth: Option<FileAuth>,
    #[serde(default)]
    logging: Option<FileLogging>,
    // ---- seed manifest sections (real, validated as of #024) ----
    // The seed sections carry `#[serde(deny_unknown_fields)]` on their leaf
    // structs, so a typo INSIDE a seeded account / instrument / persona table now
    // aborts startup naming the key (the `IgnoredAny` placeholder swallowed it).
    // They resolve into `Config::seed` via `seed_manifest`.
    #[serde(default)]
    accounts: Option<BTreeMap<String, FileAccount>>,
    #[serde(default)]
    instruments: Option<BTreeMap<String, FileInstrument>>,
    #[serde(default)]
    market_maker: Option<FileMarketMaker>,
    // ---- remaining extension points (accepted, validated by #44–#47) ----
    // `microstructure` / `rate_limits` are still forward-looking placeholders so
    // serde ACCEPTS a forward config without rejecting an unknown top-level key;
    // the content is ignored here and validated by the microstructure issues.
    // They are read only by serde during deserialization, so Rust's dead-code
    // analysis (which does not count that) is scoped-silenced.
    #[serde(default)]
    #[allow(dead_code)]
    microstructure: Option<IgnoredAny>,
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
        // Bind the fix section once so its address and all four DoS-control knobs
        // read the same optional table (a moved-out field cannot be read twice).
        let fix = self.fix;
        let fix_addr = fix.as_ref().and_then(|section| section.fix_addr.clone());
        let fix_enabled = fix
            .as_ref()
            .and_then(|section| section.enabled)
            .map(|value| value.to_string());
        let fix_connection_cap = fix
            .as_ref()
            .and_then(|section| section.connection_cap)
            .map(|value| value.to_string());
        let fix_mailbox_depth = fix
            .as_ref()
            .and_then(|section| section.mailbox_depth)
            .map(|value| value.to_string());
        let fix_max_frame_bytes = fix
            .as_ref()
            .and_then(|section| section.max_frame_bytes)
            .map(|value| value.to_string());
        let fix_idle_timeout_secs = fix
            .as_ref()
            .and_then(|section| section.idle_timeout_secs)
            .map(|value| value.to_string());
        let fix_logon_timeout_secs = fix
            .as_ref()
            .and_then(|section| section.logon_timeout_secs)
            .map(|value| value.to_string());
        let fix_max_heart_bt_int_secs = fix
            .as_ref()
            .and_then(|section| section.max_heart_bt_int_secs)
            .map(|value| value.to_string());
        // Bind the clock section once so its mode and both mode knobs read the same
        // optional table (a moved-out field cannot be read twice).
        let clock = self.clock;
        let clock_multiplier = clock
            .as_ref()
            .and_then(|section| section.multiplier)
            .map(|value| value.to_string());
        let clock_step_interval_ms = clock
            .as_ref()
            .and_then(|section| section.step_interval_ms)
            .map(|value| value.to_string());
        // Bind the expiry-lifecycle section once so both time knobs read the same
        // optional table.
        let expiry_lifecycle = self.expiry_lifecycle;
        let expiry_time = expiry_lifecycle
            .as_ref()
            .and_then(|section| section.expiry_time.clone());
        let settlement_time = expiry_lifecycle
            .as_ref()
            .and_then(|section| section.settlement_time.clone());
        RawConfig {
            http_addr: self.server.and_then(|section| section.http_addr),
            fix_addr,
            fix_enabled,
            fix_connection_cap,
            fix_mailbox_depth,
            fix_max_frame_bytes,
            fix_idle_timeout_secs,
            fix_logon_timeout_secs,
            fix_max_heart_bt_int_secs,
            database_url,
            db_pool_max_connections,
            db_slow_acquire_ms,
            clock: clock.and_then(|section| section.mode),
            clock_multiplier,
            clock_step_interval_ms,
            expiry_time,
            settlement_time,
            seed: self
                .determinism
                .and_then(|section| section.seed)
                .map(|seed| seed.to_string()),
            bootstrap_secret: self.auth.and_then(|section| section.bootstrap_secret),
            log_format: self.logging.and_then(|section| section.format),
        }
    }

    /// Resolves and **validates** the `[accounts.*]` / `[instruments.*]` /
    /// `[market_maker.*]` sections into a [`SeedManifest`] — accounts to
    /// [`AccountProvision`]s (with derived or explicit STP owners), instruments to
    /// canonical [`Symbol`] contracts on absolute [`ExpirationDate::DateTime`]
    /// expiries (a `Days` expiry is refused here), and personas with
    /// range-checked knobs.
    ///
    /// Iterated in **sorted key order** ([`BTreeMap`]) so the manifest order is a
    /// fixed function of the file — a prerequisite for reproducible vivification
    /// ids in the bounded seeding phase
    /// ([02 §5.2](../docs/02-matching-architecture.md#5-determinism)).
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] seed variant on a `Days` expiry, a malformed strike
    /// ladder, a bad account, or an invalid/undefined persona.
    fn seed_manifest(&self) -> Result<SeedManifest, ConfigError> {
        resolve_seed_manifest(
            self.accounts.as_ref(),
            self.instruments.as_ref(),
            self.market_maker.as_ref(),
        )
    }
}

/// `[server]` — an unrecognised inner key aborts startup.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileServer {
    #[serde(default)]
    http_addr: Option<String>,
}

/// `[fix]` — an unrecognised inner key aborts startup. `enabled` /
/// `connection_cap` / `mailbox_depth` / `max_frame_bytes` are file-only knobs (no
/// env/CLI override); `fix_addr` also carries `FAUXCHANGE_FIX_ADDR` / `--fix-addr`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileFix {
    #[serde(default)]
    fix_addr: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    connection_cap: Option<usize>,
    #[serde(default)]
    mailbox_depth: Option<usize>,
    #[serde(default)]
    max_frame_bytes: Option<usize>,
    #[serde(default)]
    idle_timeout_secs: Option<u64>,
    #[serde(default)]
    logon_timeout_secs: Option<u64>,
    #[serde(default)]
    max_heart_bt_int_secs: Option<u32>,
}

/// `[persistence]` — an unrecognised inner key aborts startup. Not `Debug`
/// (SECURITY): it briefly holds the plaintext `database_url` before it is wrapped
/// in [`Secret`].
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePersistence {
    #[serde(default)]
    database_url: Option<String>,
    #[serde(default)]
    pool_max_connections: Option<u32>,
    #[serde(default)]
    slow_acquire_ms: Option<u64>,
}

/// `[clock]` — an unrecognised inner key aborts startup. `multiplier` /
/// `step_interval_ms` are file-only mode knobs (no env/CLI override).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileClock {
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    multiplier: Option<u32>,
    #[serde(default)]
    step_interval_ms: Option<u64>,
}

/// `[expiry_lifecycle]` — an unrecognised inner key aborts startup. Both
/// `expiry_time` / `settlement_time` are file-only `HH:MM:SS` UTC knobs (no
/// env/CLI override), validated at load by [`parse_expiry_lifecycle`].
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileExpiryLifecycle {
    #[serde(default)]
    expiry_time: Option<String>,
    #[serde(default)]
    settlement_time: Option<String>,
}

/// `[determinism]` — an unrecognised inner key aborts startup. The seed is a
/// TOML integer (0..=`i64::MAX`); env/CLI carry the full `u64` range as a string.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDeterminism {
    #[serde(default)]
    seed: Option<u64>,
}

/// `[auth]` — an unrecognised inner key aborts startup. Not `Debug` (SECURITY):
/// it briefly holds the plaintext `bootstrap_secret` before it is wrapped in
/// [`Secret`].
#[derive(Default, Deserialize)]
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
// Seed manifest — the real, validated [accounts.*] / [instruments.*] /
// [market_maker.*] sections (#024)
// ============================================================================

/// The default annualized volatility a **seeded** asset walk is configured with.
/// The seed only sets opening prices — the walk loop is not spawned at seed time —
/// so this is a placeholder needed to build the price-seam
/// [`AssetConfig`](crate::simulation::AssetConfig) the seeding phase sets opening
/// prices through; it never drives a seeded price on its own.
pub const DEFAULT_SEED_VOLATILITY: f64 = 0.20;

/// A market-maker persona's clamped knobs — validated at **load** against the
/// engine's ranges ([`SPREAD_MULTIPLIER_MIN`]..[`SPREAD_MULTIPLIER_MAX`], etc.),
/// so the seeding phase's apply cannot be rejected at range-check time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SeedPersona {
    /// The spread multiplier (clamped to `[0.1, 10.0]`).
    pub spread_multiplier: f64,
    /// The size scalar (clamped to `[0.0, 1.0]`).
    pub size_scalar: f64,
    /// The directional skew (clamped to `[-1.0, 1.0]`).
    pub directional_skew: f64,
}

/// One seeded underlying: its opening price in **cents**, its resolved canonical
/// [`Symbol`] contracts (a fixed `expiration → strike → style` order), and the
/// persona bound to it.
#[derive(Debug, Clone, PartialEq)]
pub struct SeedInstrumentSet {
    /// The underlying ticker.
    pub underlying: String,
    /// The opening price in integer **cents**.
    pub opening_price: Cents,
    /// The canonical contract symbols, in a fixed manifest order.
    pub contracts: Vec<Symbol>,
    /// The bound persona name (validated to exist), or the manifest default.
    pub persona: Option<String>,
}

/// The resolved, validated scenario seed manifest — the source the bounded
/// seeding phase provisions accounts, establishes the instrument set + opening
/// prices, and attaches default personas from, **before** the venue flips to
/// serving ([06 §7](../docs/06-deployment.md#7-seed-data-and-scenarios)).
///
/// Everything here is validated at **load**: expiries are absolute canonical
/// [`ExpirationDate::DateTime`] (a `Days` expiry is refused), strike ladders are
/// non-empty with distinct positive strikes, accounts carry at least one
/// permission, and persona knobs are in range. The accounts carry plaintext FIX
/// passwords transiently — [`AccountProvision`]'s redacting `Debug` keeps them out
/// of any log ([08 §7](../docs/08-threat-model.md#7-secrets-handling)).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SeedManifest {
    accounts: Vec<AccountProvision>,
    instruments: Vec<SeedInstrumentSet>,
    personas: BTreeMap<String, SeedPersona>,
    default_persona: Option<String>,
}

impl SeedManifest {
    /// Parses **and validates** a seed manifest from a TOML document (the seed
    /// sections of a config file) — the seam the seed unit/integration tests drive.
    ///
    /// # Errors
    ///
    /// A [`ConfigError`]: an unknown key inside a seed table (`deny_unknown_fields`),
    /// a `Days` expiry ([`ConfigError::SeedDaysExpiry`]), a malformed strike ladder,
    /// a bad account, or an invalid/undefined persona.
    pub fn from_toml_str(contents: &str) -> Result<Self, ConfigError> {
        parse_file_config(contents)?.seed_manifest()
    }

    /// Whether the manifest seeds nothing (no accounts, no instruments) — the
    /// default when no `--config` file (or no seed sections) is supplied.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.instruments.is_empty()
    }

    /// The accounts to provision into the registry (in sorted id order).
    #[must_use]
    #[inline]
    pub fn accounts(&self) -> &[AccountProvision] {
        &self.accounts
    }

    /// The seeded instrument sets (in sorted underlying order).
    #[must_use]
    #[inline]
    pub fn instruments(&self) -> &[SeedInstrumentSet] {
        &self.instruments
    }

    /// The defined personas, keyed by name.
    #[must_use]
    #[inline]
    pub fn personas(&self) -> &BTreeMap<String, SeedPersona> {
        &self.personas
    }

    /// The name of the persona applied globally by the seeding phase, if any.
    #[must_use]
    #[inline]
    pub fn default_persona(&self) -> Option<&str> {
        self.default_persona.as_deref()
    }

    /// The persona whose knobs the seeding phase applies to the market maker.
    ///
    /// The engine holds **one global** persona config (per-underlying persona
    /// *knobs* are a documented seam limitation — the engine differentiates only
    /// by per-symbol enable/disable), so the seeding phase applies this single
    /// default persona to the whole engine.
    #[must_use]
    pub fn effective_persona(&self) -> Option<SeedPersona> {
        self.default_persona
            .as_ref()
            .and_then(|name| self.personas.get(name))
            .copied()
    }

    /// The seeded underlyings, in sorted order (one price-seam asset each).
    #[must_use]
    pub fn underlyings(&self) -> Vec<String> {
        self.instruments
            .iter()
            .map(|set| set.underlying.clone())
            .collect()
    }

    /// The total number of canonical contracts across every instrument set.
    #[must_use]
    pub fn contract_count(&self) -> usize {
        self.instruments.iter().map(|set| set.contracts.len()).sum()
    }

    /// A secret-free one-line summary for the boot log (counts only, never a
    /// credential or a hash).
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "accounts={} underlyings={} contracts={} personas={} default_persona={}",
            self.accounts.len(),
            self.instruments.len(),
            self.contract_count(),
            self.personas.len(),
            self.default_persona.as_deref().unwrap_or("<none>"),
        )
    }
}

/// `[accounts.<id>]` — one seeded account. An unrecognised inner key aborts
/// startup; the FIX password is plaintext (hashed with Argon2id at provisioning
/// and dropped — never stored or logged).
///
/// Deliberately **not** `Debug` (SECURITY): it holds the plaintext `fix_password`,
/// so it must never be `{:?}`-logged. The resolved [`AccountProvision`] it becomes
/// has a redacting `Debug`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAccount {
    /// The permission set (`["read"]` / `["read", "trade"]` / `["admin"]`).
    #[serde(default)]
    permissions: Vec<Permission>,
    /// The optional STP owner as a 64-char (32-byte) hex string; derived
    /// deterministically from the account id when omitted.
    #[serde(default)]
    owner: Option<String>,
    /// The FIX `Username (553)` (required if a FIX password is set).
    #[serde(default)]
    fix_username: Option<String>,
    /// The FIX password in **plaintext** — hashed at provisioning, then dropped.
    #[serde(default)]
    fix_password: Option<String>,
    /// The FIX `SenderCompID (49)` half of the comp-id binding (both or neither).
    #[serde(default)]
    fix_sender_comp_id: Option<String>,
    /// The FIX `TargetCompID (56)` half of the comp-id binding (both or neither).
    #[serde(default)]
    fix_target_comp_id: Option<String>,
}

/// `[instruments.<underlying>]` — one seeded underlying's opening price and chain.
/// An unrecognised inner key (e.g. a `specs` typo) aborts startup.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileInstrument {
    /// The opening price in integer **cents** (must be positive).
    opening_price_cents: u64,
    /// The expiration ladder as `YYYYMMDD` dates (each resolved to the canonical
    /// `23:59:59 UTC` absolute instant; a relative `Days` value is refused).
    #[serde(default)]
    expirations: Vec<String>,
    /// The strike ladder in whole units (non-empty, distinct, positive).
    #[serde(default)]
    strikes: Vec<u64>,
    /// The option styles to seed (`["call", "put"]`); both when omitted.
    #[serde(default)]
    styles: Option<Vec<String>>,
    /// The persona bound to this underlying (validated to exist), or the default.
    #[serde(default)]
    persona: Option<String>,
}

/// `[market_maker]` — the persona definitions and the default binding.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileMarketMaker {
    /// The persona applied globally by the seeding phase (required to name a
    /// defined persona; inferred when exactly one persona is defined).
    #[serde(default)]
    default_persona: Option<String>,
    /// The named persona definitions (`[market_maker.personas.<name>]`).
    #[serde(default)]
    personas: Option<BTreeMap<String, FilePersona>>,
}

/// `[market_maker.personas.<name>]` — one persona's quoting knobs. An unrecognised
/// inner key aborts startup.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePersona {
    /// The spread multiplier (default `1.0`; clamped to `[0.1, 10.0]`).
    #[serde(default)]
    spread_multiplier: Option<f64>,
    /// The size scalar (default `1.0`; clamped to `[0.0, 1.0]`).
    #[serde(default)]
    size_scalar: Option<f64>,
    /// The directional skew (default `0.0`; clamped to `[-1.0, 1.0]`).
    #[serde(default)]
    directional_skew: Option<f64>,
}

/// Resolves and validates the three seed sections into a [`SeedManifest`],
/// iterating in sorted key order for a fixed, reproducible manifest order.
fn resolve_seed_manifest(
    accounts: Option<&BTreeMap<String, FileAccount>>,
    instruments: Option<&BTreeMap<String, FileInstrument>>,
    market_maker: Option<&FileMarketMaker>,
) -> Result<SeedManifest, ConfigError> {
    // ---- personas (needed to validate instrument persona bindings) ----
    let mut personas: BTreeMap<String, SeedPersona> = BTreeMap::new();
    let mut default_persona: Option<String> = None;
    if let Some(mm) = market_maker {
        default_persona = mm.default_persona.clone();
        if let Some(defs) = &mm.personas {
            for (name, file_persona) in defs {
                personas.insert(name.clone(), resolve_persona(name, file_persona)?);
            }
        }
    }
    match &default_persona {
        Some(name) if !personas.contains_key(name) => {
            return Err(ConfigError::SeedInvalidPersona {
                reason: format!(
                    "default_persona '{name}' is not defined under [market_maker.personas]"
                ),
            });
        }
        // No explicit default but exactly one persona defined: it is the default.
        None if personas.len() == 1 => {
            default_persona = personas.keys().next().cloned();
        }
        _ => {}
    }

    // ---- accounts ----
    let mut resolved_accounts: Vec<AccountProvision> = Vec::new();
    if let Some(accts) = accounts {
        for (id, file_account) in accts {
            resolved_accounts.push(resolve_account(id, file_account)?);
        }
    }

    // ---- instruments ----
    let mut resolved_instruments: Vec<SeedInstrumentSet> = Vec::new();
    if let Some(insts) = instruments {
        for (underlying, file_instrument) in insts {
            resolved_instruments.push(resolve_instrument(
                underlying,
                file_instrument,
                &personas,
                default_persona.as_deref(),
            )?);
        }
    }

    Ok(SeedManifest {
        accounts: resolved_accounts,
        instruments: resolved_instruments,
        personas,
        default_persona,
    })
}

/// Validates one persona's knobs against the engine ranges.
fn resolve_persona(name: &str, file_persona: &FilePersona) -> Result<SeedPersona, ConfigError> {
    let spread = check_persona_knob(
        "spread_multiplier",
        name,
        file_persona.spread_multiplier.unwrap_or(1.0),
        SPREAD_MULTIPLIER_MIN,
        SPREAD_MULTIPLIER_MAX,
    )?;
    let size = check_persona_knob(
        "size_scalar",
        name,
        file_persona.size_scalar.unwrap_or(1.0),
        SIZE_SCALAR_MIN,
        SIZE_SCALAR_MAX,
    )?;
    let skew = check_persona_knob(
        "directional_skew",
        name,
        file_persona.directional_skew.unwrap_or(0.0),
        DIRECTIONAL_SKEW_MIN,
        DIRECTIONAL_SKEW_MAX,
    )?;
    Ok(SeedPersona {
        spread_multiplier: spread,
        size_scalar: size,
        directional_skew: skew,
    })
}

/// Range-checks a persona knob (finite and within `[min, max]`).
fn check_persona_knob(
    knob: &str,
    persona: &str,
    value: f64,
    min: f64,
    max: f64,
) -> Result<f64, ConfigError> {
    if !value.is_finite() || value < min || value > max {
        return Err(ConfigError::SeedInvalidPersona {
            reason: format!(
                "persona '{persona}' knob {knob}={value} is not finite or is outside [{min}, {max}]"
            ),
        });
    }
    Ok(value)
}

/// Resolves one seeded account into an [`AccountProvision`], deriving the STP
/// owner from the id when not given explicitly.
fn resolve_account(id: &str, file_account: &FileAccount) -> Result<AccountProvision, ConfigError> {
    if file_account.permissions.is_empty() {
        return Err(ConfigError::SeedInvalidAccount {
            id: id.to_string(),
            reason: "at least one permission is required".to_string(),
        });
    }
    let owner = match &file_account.owner {
        Some(hex) => parse_owner_hex(id, hex)?,
        None => derive_owner(id),
    };
    if file_account.fix_password.is_some() && file_account.fix_username.is_none() {
        return Err(ConfigError::SeedInvalidAccount {
            id: id.to_string(),
            reason: "fix_password requires fix_username".to_string(),
        });
    }
    let fix_comp_ids = match (
        &file_account.fix_sender_comp_id,
        &file_account.fix_target_comp_id,
    ) {
        (Some(sender), Some(target)) => Some(CompIdBinding {
            sender_comp_id: sender.clone(),
            target_comp_id: target.clone(),
        }),
        (None, None) => None,
        _ => {
            return Err(ConfigError::SeedInvalidAccount {
                id: id.to_string(),
                reason: "fix_sender_comp_id and fix_target_comp_id must be set together"
                    .to_string(),
            });
        }
    };
    Ok(AccountProvision {
        id: AccountId::new(id),
        owner,
        permissions: file_account.permissions.clone(),
        fix_username: file_account.fix_username.clone(),
        fix_password: file_account.fix_password.clone(),
        fix_comp_ids,
    })
}

/// Parses a 64-char (32-byte) hex STP owner literal.
fn parse_owner_hex(id: &str, hex: &str) -> Result<Hash32, ConfigError> {
    let hex = hex.trim();
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ConfigError::SeedInvalidAccount {
            id: id.to_string(),
            reason: "owner must be a 64-character hex string (32 bytes)".to_string(),
        });
    }
    let mut bytes = [0u8; 32];
    for (index, slot) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        let pair = hex
            .get(start..start + 2)
            .ok_or_else(|| ConfigError::SeedInvalidAccount {
                id: id.to_string(),
                reason: "owner hex is truncated".to_string(),
            })?;
        *slot = u8::from_str_radix(pair, 16).map_err(|_| ConfigError::SeedInvalidAccount {
            id: id.to_string(),
            reason: "owner is not valid hex".to_string(),
        })?;
    }
    Ok(Hash32(bytes))
}

/// Derives a deterministic 32-byte STP owner hash from an account id, so the same
/// id always maps to the same owner (a re-seed reproduces STP grouping). Not a
/// cryptographic hash (STP grouping only), and it uses only **total** operations
/// (XOR + bit rotation + array indexing) — never `wrapping_*` arithmetic on a
/// counter. A collision with the reserved market-maker owner is caught by the
/// registry's provisioning guard, not silently accepted.
fn derive_owner(id: &str) -> Hash32 {
    let mut bytes = [0u8; 32];
    for (index, byte) in id.as_bytes().iter().enumerate() {
        let slot = index % 32;
        bytes[slot] ^= byte.rotate_left((index % 8) as u32);
    }
    // Stir each slot with its index and the id length so short ids still spread
    // across all 32 bytes and same-content-different-length ids differ.
    let len_byte = (id.len() % 256) as u8;
    for (index, slot) in bytes.iter_mut().enumerate() {
        *slot ^= len_byte ^ (index as u8);
    }
    Hash32(bytes)
}

/// Resolves one seeded instrument set: validates the opening price, expirations,
/// and strike ladder, then builds the canonical contract symbols in the fixed
/// `expiration → strike → style` order.
fn resolve_instrument(
    underlying: &str,
    file_instrument: &FileInstrument,
    personas: &BTreeMap<String, SeedPersona>,
    default_persona: Option<&str>,
) -> Result<SeedInstrumentSet, ConfigError> {
    if file_instrument.opening_price_cents == 0 {
        return Err(ConfigError::SeedInvalidInstrument {
            underlying: underlying.to_string(),
            reason: "opening_price_cents must be positive".to_string(),
        });
    }
    if file_instrument.expirations.is_empty() {
        return Err(ConfigError::SeedInvalidInstrument {
            underlying: underlying.to_string(),
            reason: "at least one expiration is required".to_string(),
        });
    }

    // Strike ladder: non-empty, distinct, positive (BTreeSet keeps sorted order).
    if file_instrument.strikes.is_empty() {
        return Err(ConfigError::SeedInvalidStrikeLadder {
            underlying: underlying.to_string(),
            reason: "the strike ladder is empty".to_string(),
        });
    }
    let mut strikes: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for &strike in &file_instrument.strikes {
        if strike == 0 {
            return Err(ConfigError::SeedInvalidStrikeLadder {
                underlying: underlying.to_string(),
                reason: "a strike is zero".to_string(),
            });
        }
        if !strikes.insert(strike) {
            return Err(ConfigError::SeedInvalidStrikeLadder {
                underlying: underlying.to_string(),
                reason: format!("duplicate strike {strike}"),
            });
        }
    }

    // Persona binding: validated to exist, or the manifest default.
    let persona = match &file_instrument.persona {
        Some(name) => {
            if !personas.contains_key(name) {
                return Err(ConfigError::SeedInvalidPersona {
                    reason: format!(
                        "instrument '{underlying}' references undefined persona '{name}'"
                    ),
                });
            }
            Some(name.clone())
        }
        None => default_persona.map(str::to_string),
    };

    // Expirations: resolve each to a canonical absolute instant, keyed by its
    // canonical YYYYMMDD for a sorted, de-duplicated, reproducible order.
    let mut expirations: Vec<(String, ExpirationDate)> = Vec::new();
    for raw in &file_instrument.expirations {
        let date = parse_seed_expiry(underlying, raw)?;
        let yyyymmdd =
            format_expiration_yyyymmdd(&date).map_err(|error| ConfigError::SeedInvalidExpiry {
                underlying: underlying.to_string(),
                value: raw.clone(),
                reason: error.to_string(),
            })?;
        expirations.push((yyyymmdd, date));
    }
    expirations.sort_by(|left, right| left.0.cmp(&right.0));
    expirations.dedup_by(|left, right| left.0 == right.0);

    let styles = resolve_styles(underlying, file_instrument.styles.as_deref())?;

    // Build the canonical contracts in a fixed expiration → strike → style order.
    let mut contracts: Vec<Symbol> = Vec::new();
    for (_, date) in &expirations {
        for &strike in &strikes {
            for &style in &styles {
                let instrument =
                    Instrument::try_new(underlying, *date, strike, style, InstrumentStatus::Active)
                        .map_err(|error| map_instrument_symbol_error(underlying, error))?;
                contracts.push(instrument.symbol().clone());
            }
        }
    }

    Ok(SeedInstrumentSet {
        underlying: underlying.to_string(),
        opening_price: Cents::new(file_instrument.opening_price_cents),
        contracts,
        persona,
    })
}

/// Resolves one seeded expiry token to a canonical absolute [`ExpirationDate`],
/// refusing a relative `Days` expiry (which breaks replay).
fn parse_seed_expiry(underlying: &str, raw: &str) -> Result<ExpirationDate, ConfigError> {
    let trimmed = raw.trim();
    // An 8-digit `YYYYMMDD` resolves through the upstream grammar to the canonical
    // 23:59:59 UTC instant; anything else routes through `optionstratlib` (a bare
    // day-count yields a relative `Days` expiry, refused below).
    let candidate = if trimmed.len() == 8 && trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        SymbolParser::parse_yyyymmdd(trimmed, "").map_err(|error| {
            ConfigError::SeedInvalidExpiry {
                underlying: underlying.to_string(),
                value: raw.to_string(),
                reason: error.to_string(),
            }
        })?
    } else {
        ExpirationDate::from_string(trimmed).map_err(|error| ConfigError::SeedInvalidExpiry {
            underlying: underlying.to_string(),
            value: raw.to_string(),
            reason: error.to_string(),
        })?
    };
    validate_venue_expiry(&candidate).map_err(|error| match error {
        SymbolError::RelativeExpiryRefused => ConfigError::SeedDaysExpiry {
            underlying: underlying.to_string(),
            value: raw.to_string(),
        },
        other => ConfigError::SeedInvalidExpiry {
            underlying: underlying.to_string(),
            value: raw.to_string(),
            reason: other.to_string(),
        },
    })
}

/// Resolves the option-style list, defaulting to `[call, put]` and emitting a
/// deterministic `call → put` order.
fn resolve_styles(
    underlying: &str,
    styles: Option<&[String]>,
) -> Result<Vec<OptionStyle>, ConfigError> {
    let Some(list) = styles else {
        return Ok(vec![OptionStyle::Call, OptionStyle::Put]);
    };
    if list.is_empty() {
        return Err(ConfigError::SeedInvalidInstrument {
            underlying: underlying.to_string(),
            reason: "styles list is empty; omit it to seed both call and put".to_string(),
        });
    }
    let mut has_call = false;
    let mut has_put = false;
    for style in list {
        match style.trim().to_ascii_lowercase().as_str() {
            "call" | "c" => has_call = true,
            "put" | "p" => has_put = true,
            other => {
                return Err(ConfigError::SeedInvalidInstrument {
                    underlying: underlying.to_string(),
                    reason: format!("unknown option style '{other}' (expected call or put)"),
                });
            }
        }
    }
    let mut out = Vec::new();
    if has_call {
        out.push(OptionStyle::Call);
    }
    if has_put {
        out.push(OptionStyle::Put);
    }
    Ok(out)
}

/// Maps a symbol-build failure (strike/underlying grammar) to a seed config error.
/// The expiry is already validated by [`parse_seed_expiry`], so the expiry arms
/// are defensive.
fn map_instrument_symbol_error(underlying: &str, error: SymbolError) -> ConfigError {
    match error {
        SymbolError::InvalidSymbol { reason, .. } => ConfigError::SeedInvalidInstrument {
            underlying: underlying.to_string(),
            reason,
        },
        SymbolError::RelativeExpiryRefused => ConfigError::SeedDaysExpiry {
            underlying: underlying.to_string(),
            value: "(resolved expiry)".to_string(),
        },
        other => ConfigError::SeedInvalidExpiry {
            underlying: underlying.to_string(),
            value: "(resolved expiry)".to_string(),
            reason: other.to_string(),
        },
    }
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

    /// The remaining extension-point sections (`microstructure` / `rate_limits`,
    /// still owned by #44–#47) are accepted and ignored, not rejected.
    #[test]
    fn test_config_extension_point_sections_are_accepted() -> Result<(), ConfigError> {
        let document = "\
[microstructure.fees]
maker_bps = -10
taker_bps = 35

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

    #[test]
    fn test_config_clock_knobs_from_file_section() -> Result<(), ConfigError> {
        let file = raw_from_toml_str(
            "[clock]\nmode = \"accelerated\"\nmultiplier = 120\nstep_interval_ms = 30000\n",
        )?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        assert_eq!(config.clock.mode, ClockMode::Accelerated);
        assert_eq!(config.clock.multiplier, 120);
        assert_eq!(config.clock.step_interval_ms, 30_000);
        // The accelerated knob folds into the runtime clock config; the stepped
        // interval is ignored for this mode.
        let venue = config.clock.to_venue_clock_config(1_000);
        assert_eq!(
            venue.mode,
            crate::simulation::ClockMode::Accelerated { multiplier: 120 }
        );
        assert_eq!(venue.start_ms, 1_000);
        Ok(())
    }

    #[test]
    fn test_config_clock_knobs_default_when_absent() -> Result<(), ConfigError> {
        // No `[clock]` knobs → the documented defaults are carried.
        let config = Config::assemble(
            RawConfig::default(),
            raw_from_env(|_| None),
            RawConfig::default(),
        )?;
        assert_eq!(config.clock.multiplier, DEFAULT_CLOCK_MULTIPLIER);
        assert_eq!(
            config.clock.step_interval_ms,
            DEFAULT_CLOCK_STEP_INTERVAL_MS
        );
        Ok(())
    }

    #[test]
    fn test_config_stepped_clock_maps_interval_to_venue_clock() -> Result<(), ConfigError> {
        let file = raw_from_toml_str("[clock]\nmode = \"stepped\"\nstep_interval_ms = 500\n")?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        let venue = config.clock.to_venue_clock_config(7_000);
        assert_eq!(
            venue.mode,
            crate::simulation::ClockMode::Stepped { step_ms: 500 }
        );
        Ok(())
    }

    #[test]
    fn test_config_bad_clock_multiplier_is_rejected() {
        // A non-integer multiplier is refused (the `[clock]` knob is a typed
        // integer at the file layer).
        let file = raw_from_toml_str("[clock]\nmultiplier = \"fast\"\n");
        assert!(file.is_err(), "a non-integer multiplier must be refused");
    }

    #[test]
    fn test_config_unknown_clock_key_names_the_key() {
        // `deny_unknown_fields` is preserved on the extended `[clock]` section.
        match raw_from_toml_str("[clock]\nmode = \"realtime\"\ntypo = 1\n") {
            Err(ConfigError::UnknownKey { key }) => assert!(key.contains("typo")),
            Err(other) => panic!("expected an unknown-key rejection, got {other}"),
            Ok(_) => panic!("expected an unknown-key rejection, got a parsed config"),
        }
    }

    // ---- #037: [fix] gateway toggle + DoS-control knobs --------------------

    #[test]
    fn test_config_fix_defaults_are_bounded_and_disabled() -> Result<(), ConfigError> {
        // No `[fix]` section: the gateway is disabled by default with the bounded
        // DoS-control defaults.
        let config = Config::load_from(std::iter::empty::<String>(), |_| None)?;
        assert!(
            !config.fix.enabled,
            "the FIX gateway is opt-in (default off)"
        );
        assert_eq!(config.fix.fix_addr.port(), 9878);
        assert_eq!(config.fix.connection_cap, DEFAULT_FIX_CONNECTION_CAP);
        assert_eq!(config.fix.mailbox_depth, DEFAULT_FIX_MAILBOX_DEPTH);
        assert_eq!(config.fix.max_frame_bytes, DEFAULT_FIX_MAX_FRAME_BYTES);
        Ok(())
    }

    #[test]
    fn test_config_fix_section_overrides_all_knobs() -> Result<(), ConfigError> {
        let file = raw_from_toml_str(
            "[fix]\nenabled = true\nfix_addr = \"127.0.0.1:19878\"\n\
             connection_cap = 8\nmailbox_depth = 16\nmax_frame_bytes = 4096\n",
        )?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        assert!(config.fix.enabled);
        assert_eq!(config.fix.fix_addr.to_string(), "127.0.0.1:19878");
        assert_eq!(config.fix.connection_cap, 8);
        assert_eq!(config.fix.mailbox_depth, 16);
        assert_eq!(config.fix.max_frame_bytes, 4096);
        Ok(())
    }

    #[test]
    fn test_config_fix_zero_connection_cap_is_out_of_range() -> Result<(), ConfigError> {
        // A `0` cap is below the `1` minimum — a validated-range rejection, not a
        // silent clamp.
        let file = raw_from_toml_str("[fix]\nconnection_cap = 0\n")?;
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::BadFixValue { field, .. }) => assert_eq!(field, "connection_cap"),
            other => panic!("expected BadFixValue(connection_cap), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_config_fix_connection_cap_over_ceiling_is_rejected() -> Result<(), ConfigError> {
        let file = raw_from_toml_str(&format!(
            "[fix]\nconnection_cap = {}\n",
            FIX_MAX_CONNECTION_CAP + 1
        ))?;
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::BadFixValue { field, reason, .. }) => {
                assert_eq!(field, "connection_cap");
                assert!(reason.contains(&FIX_MAX_CONNECTION_CAP.to_string()));
            }
            other => panic!("expected BadFixValue(connection_cap), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_config_fix_max_frame_bytes_below_floor_is_rejected() -> Result<(), ConfigError> {
        // A frame cap below the floor would not fit a legitimate FIX frame.
        let file = raw_from_toml_str(&format!(
            "[fix]\nmax_frame_bytes = {}\n",
            FIX_MIN_MAX_FRAME_BYTES - 1
        ))?;
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::BadFixValue { field, .. }) => assert_eq!(field, "max_frame_bytes"),
            other => panic!("expected BadFixValue(max_frame_bytes), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_config_fix_max_frame_bytes_over_ceiling_is_rejected() -> Result<(), ConfigError> {
        let file = raw_from_toml_str(&format!(
            "[fix]\nmax_frame_bytes = {}\n",
            FIX_MAX_MAX_FRAME_BYTES + 1
        ))?;
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::BadFixValue { field, .. }) => assert_eq!(field, "max_frame_bytes"),
            other => panic!("expected BadFixValue(max_frame_bytes), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_config_fix_unknown_key_names_the_key() {
        // `deny_unknown_fields` holds on the extended `[fix]` section.
        match raw_from_toml_str("[fix]\nenabled = true\nmax_frames = 10\n") {
            Err(ConfigError::UnknownKey { key }) => assert!(key.contains("max_frames")),
            Err(other) => panic!("expected an unknown-key rejection, got {other}"),
            Ok(_) => panic!("expected an unknown-key rejection, got a parsed config"),
        }
    }

    #[test]
    fn test_config_fix_addr_env_overrides_file() -> Result<(), ConfigError> {
        // `fix_addr` still honours the env layer (later wins), while the file-only
        // knobs are carried from the `[fix]` file section.
        let file = raw_from_toml_str("[fix]\nfix_addr = \"127.0.0.1:1111\"\nconnection_cap = 4\n")?;
        let env = raw_from_env(|key| {
            (key == "FAUXCHANGE_FIX_ADDR").then(|| "127.0.0.1:2222".to_string())
        });
        let config = Config::assemble(file, env, RawConfig::default())?;
        assert_eq!(config.fix.fix_addr.to_string(), "127.0.0.1:2222");
        assert_eq!(config.fix.connection_cap, 4);
        Ok(())
    }

    #[test]
    fn test_config_render_effective_never_omits_fix_knobs() -> Result<(), ConfigError> {
        let config = Config::load_from(std::iter::empty::<String>(), |_| None)?;
        let rendered = config.render_effective();
        assert!(rendered.contains("fix.enabled=false"));
        assert!(rendered.contains("fix.connection_cap="));
        assert!(rendered.contains("fix.mailbox_depth="));
        assert!(rendered.contains("fix.max_frame_bytes="));
        assert!(rendered.contains("fix.idle_timeout_secs="));
        Ok(())
    }

    #[test]
    fn test_config_fix_idle_timeout_default_and_override() -> Result<(), ConfigError> {
        let default = Config::load_from(std::iter::empty::<String>(), |_| None)?;
        assert_eq!(default.fix.idle_timeout_secs, DEFAULT_FIX_IDLE_TIMEOUT_SECS);
        let file = raw_from_toml_str("[fix]\nidle_timeout_secs = 5\n")?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        assert_eq!(config.fix.idle_timeout_secs, 5);
        Ok(())
    }

    #[test]
    fn test_config_fix_zero_idle_timeout_is_out_of_range() -> Result<(), ConfigError> {
        // A `0` timeout is below the `1`s minimum — rejected, not silently clamped
        // (a `0`s idle timeout would close every connection instantly).
        let file = raw_from_toml_str("[fix]\nidle_timeout_secs = 0\n")?;
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::BadFixValue { field, .. }) => assert_eq!(field, "idle_timeout_secs"),
            other => panic!("expected BadFixValue(idle_timeout_secs), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_config_fix_cap_times_frame_product_ceiling_is_enforced() -> Result<(), ConfigError> {
        // Each knob is in range individually, but their PRODUCT exceeds the
        // aggregate ceiling — refused at boot with a typed error (a DoS control, not
        // a silent accept).
        let cap = FIX_MAX_CONNECTION_CAP; // 65_536, in range
        let frame = FIX_MAX_MAX_FRAME_BYTES; // 16 MiB, in range
        assert!(
            cap * frame > FIX_MAX_AGGREGATE_FRAME_BYTES,
            "the fixture must exceed the aggregate ceiling"
        );
        let file = raw_from_toml_str(&format!(
            "[fix]\nconnection_cap = {cap}\nmax_frame_bytes = {frame}\n"
        ))?;
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::BadFixValue { reason, .. }) => {
                assert!(reason.contains("aggregate ceiling"), "reason: {reason}");
            }
            other => panic!("expected an aggregate-ceiling BadFixValue, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_config_fix_cap_times_frame_product_within_ceiling_is_accepted()
    -> Result<(), ConfigError> {
        // A generous-but-bounded combination (256 conns × 256 KiB = 64 MiB) is well
        // under the ceiling and accepted.
        let file = raw_from_toml_str("[fix]\nconnection_cap = 256\nmax_frame_bytes = 262144\n")?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        assert_eq!(config.fix.connection_cap, 256);
        assert_eq!(config.fix.max_frame_bytes, 262_144);
        Ok(())
    }

    // ---- #032: [expiry_lifecycle] operational times ------------------------

    #[test]
    fn test_operational_time_parse_accepts_and_rejects() {
        // Valid HH:MM:SS in range.
        assert_eq!(
            OperationalTime::parse("08:00:00"),
            OperationalTime::from_hms(8, 0, 0)
        );
        assert_eq!(
            OperationalTime::parse("23:59:59").map(|t| t.secs_since_midnight()),
            Some(IDENTITY_EXPIRY_SECS)
        );
        // Out-of-range components and malformed shapes are refused.
        for bad in [
            "24:00:00",
            "08:60:00",
            "08:00:60",
            "8:00",
            "8-00-00",
            "",
            "08:00:00:00",
        ] {
            assert!(
                OperationalTime::parse(bad).is_none(),
                "'{bad}' must not parse as a time-of-day"
            );
        }
    }

    #[test]
    fn test_operational_time_display_roundtrips() {
        let t = OperationalTime::from_hms(8, 30, 0).expect("valid time");
        assert_eq!(t.to_string(), "08:30:00");
    }

    #[test]
    fn test_default_operational_times_are_valid() -> Result<(), ConfigError> {
        // The documented defaults (08:00 / 08:30 UTC) are a coherent combination.
        let config = Config::assemble(
            RawConfig::default(),
            raw_from_env(|_| None),
            RawConfig::default(),
        )?;
        assert_eq!(config.expiry_lifecycle.expiry_time.to_string(), "08:00:00");
        assert_eq!(
            config.expiry_lifecycle.settlement_time.to_string(),
            "08:30:00"
        );
        Ok(())
    }

    #[test]
    fn test_operational_times_from_file_section() -> Result<(), ConfigError> {
        let file = raw_from_toml_str(
            "[expiry_lifecycle]\nexpiry_time = \"09:15:00\"\nsettlement_time = \"09:45:30\"\n",
        )?;
        let config = Config::assemble(file, raw_from_env(|_| None), RawConfig::default())?;
        assert_eq!(config.expiry_lifecycle.expiry_time.to_string(), "09:15:00");
        assert_eq!(
            config.expiry_lifecycle.settlement_time.to_string(),
            "09:45:30"
        );
        Ok(())
    }

    #[test]
    fn test_expiry_lifecycle_rejects_settlement_before_expiry() {
        // settlement_time earlier than expiry_time is an incoherent lifecycle order.
        let file = raw_from_toml_str(
            "[expiry_lifecycle]\nexpiry_time = \"08:30:00\"\nsettlement_time = \"08:00:00\"\n",
        )
        .expect("the file layer parses; validation happens at assemble");
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::OperationalSettlementBeforeExpiry { expiry, settlement }) => {
                assert_eq!(expiry, "08:30:00");
                assert_eq!(settlement, "08:00:00");
            }
            other => panic!("expected OperationalSettlementBeforeExpiry, got {other:?}"),
        }
    }

    #[test]
    fn test_expiry_lifecycle_rejects_time_at_or_after_identity_instant() {
        // A settlement AT the 23:59:59 identity instant is refused — the identity
        // instant is reserved for symbol identity, never a lifecycle transition.
        let file = raw_from_toml_str(
            "[expiry_lifecycle]\nexpiry_time = \"08:00:00\"\nsettlement_time = \"23:59:59\"\n",
        )
        .expect("the file layer parses; validation happens at assemble");
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::OperationalTimeNotBeforeIdentity { field, value }) => {
                assert_eq!(field, "settlement_time");
                assert_eq!(value, "23:59:59");
            }
            other => panic!("expected OperationalTimeNotBeforeIdentity, got {other:?}"),
        }
    }

    #[test]
    fn test_expiry_lifecycle_rejects_malformed_time() {
        // A non-HH:MM:SS value is a typed BadOperationalTime.
        let file = raw_from_toml_str(
            "[expiry_lifecycle]\nexpiry_time = \"25:00:00\"\nsettlement_time = \"08:30:00\"\n",
        )
        .expect("the file layer parses; validation happens at assemble");
        match Config::assemble(file, raw_from_env(|_| None), RawConfig::default()) {
            Err(ConfigError::BadOperationalTime { field, value }) => {
                assert_eq!(field, "expiry_time");
                assert_eq!(value, "25:00:00");
            }
            other => panic!("expected BadOperationalTime, got {other:?}"),
        }
    }

    #[test]
    fn test_expiry_lifecycle_unknown_key_names_the_key() {
        // `deny_unknown_fields` is preserved on the new `[expiry_lifecycle]` section.
        match raw_from_toml_str("[expiry_lifecycle]\nexpiry_time = \"08:00:00\"\ntypo = 1\n") {
            Err(ConfigError::UnknownKey { key }) => assert!(key.contains("typo")),
            Err(other) => panic!("expected an unknown-key rejection, got {other}"),
            Ok(_) => panic!("expected an unknown-key rejection, got a parsed config"),
        }
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

    // ---- seed manifest (#024) ---------------------------------------------

    /// A representative multi-underlying seed document (two underlyings, one
    /// DateTime expiry, a strike ladder, opening prices, a default persona, and a
    /// Read + a Trade account with a FIX credential).
    const SEED_DOC: &str = "\
[market_maker]
default_persona = \"balanced\"

[market_maker.personas.balanced]
spread_multiplier = 1.0
size_scalar = 0.5
directional_skew = 0.0

[instruments.BTC]
opening_price_cents = 5000000
expirations = [\"20260327\"]
strikes = [45000, 50000, 55000]

[instruments.ETH]
opening_price_cents = 300000
expirations = [\"20260327\"]
strikes = [2500, 3000]
styles = [\"call\"]

[accounts.market-reader]
permissions = [\"read\"]

[accounts.market-taker]
permissions = [\"read\", \"trade\"]
fix_username = \"TAKER1\"
fix_password = \"taker-secret\"
";

    #[test]
    fn test_seed_default_scenario_parses_and_validates() -> Result<(), ConfigError> {
        let manifest = SeedManifest::from_toml_str(SEED_DOC)?;
        assert!(!manifest.is_empty());
        // Two accounts, in sorted id order.
        assert_eq!(manifest.accounts().len(), 2);
        assert_eq!(manifest.accounts()[0].id.as_str(), "market-reader");
        assert_eq!(manifest.accounts()[1].id.as_str(), "market-taker");
        assert_eq!(
            manifest.accounts()[1].permissions,
            vec![Permission::Read, Permission::Trade]
        );
        assert_eq!(
            manifest.accounts()[1].fix_username.as_deref(),
            Some("TAKER1")
        );
        // Two underlyings, sorted; BTC has 3 strikes × 1 expiry × 2 styles = 6.
        assert_eq!(manifest.underlyings(), vec!["BTC", "ETH"]);
        let btc = &manifest.instruments()[0];
        assert_eq!(btc.underlying, "BTC");
        assert_eq!(btc.opening_price, Cents::new(5_000_000));
        assert_eq!(btc.contracts.len(), 6);
        // ETH: 2 strikes × 1 expiry × 1 style (call only) = 2.
        assert_eq!(manifest.instruments()[1].contracts.len(), 2);
        assert_eq!(manifest.contract_count(), 8);
        // The default persona is applied globally.
        let persona = manifest.effective_persona().expect("a default persona");
        assert_eq!(persona.size_scalar, 0.5);
        Ok(())
    }

    #[test]
    fn test_seed_contracts_are_canonical_and_in_fixed_order() -> Result<(), ConfigError> {
        let manifest = SeedManifest::from_toml_str(SEED_DOC)?;
        let btc = &manifest.instruments()[0];
        let symbols: Vec<&str> = btc.contracts.iter().map(Symbol::as_str).collect();
        // Fixed expiration → strike → style (call, put) order, canonical symbols.
        assert_eq!(
            symbols,
            vec![
                "BTC-20260327-45000-C",
                "BTC-20260327-45000-P",
                "BTC-20260327-50000-C",
                "BTC-20260327-50000-P",
                "BTC-20260327-55000-C",
                "BTC-20260327-55000-P",
            ]
        );
        Ok(())
    }

    #[test]
    fn test_seed_unknown_key_inside_instrument_is_rejected() {
        // `specs` is not an instrument field (it belongs to microstructure #44–#47);
        // the real struct now catches the typo the IgnoredAny placeholder swallowed.
        let document = "\
[instruments.BTC]
opening_price_cents = 5000000
expirations = [\"20260327\"]
strikes = [50000]

[instruments.BTC.specs]
tick_size_cents = 5
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::UnknownKey { key }) => assert_eq!(key, "specs"),
            other => panic!("expected UnknownKey(specs), got {other:?}"),
        }
    }

    #[test]
    fn test_seed_unknown_key_inside_account_is_rejected() {
        let document = "\
[accounts.reader]
permissions = [\"read\"]
role = \"admin\"
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::UnknownKey { key }) => assert_eq!(key, "role"),
            other => panic!("expected UnknownKey(role), got {other:?}"),
        }
    }

    #[test]
    fn test_seed_days_expiry_is_rejected_at_load() {
        // A bare day-count is a relative `Days` expiry — refused (it breaks replay).
        let document = "\
[instruments.BTC]
opening_price_cents = 5000000
expirations = [\"30\"]
strikes = [50000]
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedDaysExpiry { underlying, value }) => {
                assert_eq!(underlying, "BTC");
                assert_eq!(value, "30");
            }
            other => panic!("expected SeedDaysExpiry, got {other:?}"),
        }
    }

    #[test]
    fn test_seed_datetime_expiry_is_accepted() -> Result<(), ConfigError> {
        // A canonical 23:59:59 UTC absolute instant is accepted.
        let document = "\
[instruments.BTC]
opening_price_cents = 5000000
expirations = [\"2026-03-27T23:59:59Z\"]
strikes = [50000]
";
        let manifest = SeedManifest::from_toml_str(document)?;
        assert_eq!(manifest.instruments()[0].contracts.len(), 2);
        assert_eq!(
            manifest.instruments()[0].contracts[0].as_str(),
            "BTC-20260327-50000-C"
        );
        Ok(())
    }

    #[test]
    fn test_seed_empty_strike_ladder_is_rejected() {
        let document = "\
[instruments.BTC]
opening_price_cents = 5000000
expirations = [\"20260327\"]
strikes = []
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedInvalidStrikeLadder { underlying, .. }) => {
                assert_eq!(underlying, "BTC")
            }
            other => panic!("expected SeedInvalidStrikeLadder, got {other:?}"),
        }
    }

    #[test]
    fn test_seed_duplicate_strike_is_rejected() {
        let document = "\
[instruments.BTC]
opening_price_cents = 5000000
expirations = [\"20260327\"]
strikes = [50000, 50000]
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedInvalidStrikeLadder { reason, .. }) => {
                assert!(reason.contains("duplicate"), "reason: {reason}")
            }
            other => panic!("expected SeedInvalidStrikeLadder(duplicate), got {other:?}"),
        }
    }

    #[test]
    fn test_seed_zero_opening_price_is_rejected() {
        let document = "\
[instruments.BTC]
opening_price_cents = 0
expirations = [\"20260327\"]
strikes = [50000]
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedInvalidInstrument { reason, .. }) => {
                assert!(reason.contains("opening_price_cents"), "reason: {reason}")
            }
            other => panic!("expected SeedInvalidInstrument, got {other:?}"),
        }
    }

    #[test]
    fn test_seed_out_of_range_persona_is_rejected() {
        let document = "\
[market_maker.personas.wild]
spread_multiplier = 99.0
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedInvalidPersona { reason }) => {
                assert!(reason.contains("spread_multiplier"), "reason: {reason}")
            }
            other => panic!("expected SeedInvalidPersona, got {other:?}"),
        }
    }

    #[test]
    fn test_seed_undefined_persona_binding_is_rejected() {
        let document = "\
[instruments.BTC]
opening_price_cents = 5000000
expirations = [\"20260327\"]
strikes = [50000]
persona = \"ghost\"
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedInvalidPersona { reason }) => {
                assert!(reason.contains("ghost"), "reason: {reason}")
            }
            other => panic!("expected SeedInvalidPersona(ghost), got {other:?}"),
        }
    }

    #[test]
    fn test_seed_account_without_permission_is_rejected() {
        let document = "\
[accounts.ghost]
permissions = []
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedInvalidAccount { id, .. }) => assert_eq!(id, "ghost"),
            other => panic!("expected SeedInvalidAccount, got {other:?}"),
        }
    }

    #[test]
    fn test_seed_account_fix_password_requires_username() {
        let document = "\
[accounts.ghost]
permissions = [\"trade\"]
fix_password = \"secret\"
";
        match SeedManifest::from_toml_str(document) {
            Err(ConfigError::SeedInvalidAccount { reason, .. }) => {
                assert!(reason.contains("fix_username"), "reason: {reason}")
            }
            other => panic!("expected SeedInvalidAccount, got {other:?}"),
        }
    }

    #[test]
    fn test_seed_owner_is_derived_deterministically() -> Result<(), ConfigError> {
        // The same account id derives the same owner across two parses (stable),
        // and two different ids derive different owners.
        let manifest = SeedManifest::from_toml_str(SEED_DOC)?;
        let again = SeedManifest::from_toml_str(SEED_DOC)?;
        assert_eq!(manifest.accounts()[0].owner, again.accounts()[0].owner);
        assert_ne!(manifest.accounts()[0].owner, manifest.accounts()[1].owner);
        Ok(())
    }

    #[test]
    fn test_seed_empty_when_no_seed_sections() -> Result<(), ConfigError> {
        let manifest = SeedManifest::from_toml_str("[server]\nhttp_addr = \"0.0.0.0:8080\"\n")?;
        assert!(manifest.is_empty());
        assert_eq!(manifest.contract_count(), 0);
        Ok(())
    }

    /// SECURITY (P1 regression guard): a malformed seed TOML whose broken token is
    /// an **unterminated `fix_password` string** must NOT echo the password into
    /// the error. The upstream `toml::de::Error` `Display` would render that source
    /// line (the secret); our scrubbed `TomlParse` reports only line/column + the
    /// canonical parser message.
    #[test]
    fn test_seed_malformed_toml_never_echoes_a_secret() {
        const MARKER: &str = "SUPER-SECRET-PASSWORD-MARKER-024";
        // An unterminated string literal on the fix_password line: the parse error
        // span sits on that line, so the crate's Display snippet WOULD include it.
        let document = format!(
            "[accounts.taker]\n\
             permissions = [\"trade\"]\n\
             fix_username = \"t\"\n\
             fix_password = \"{MARKER}\n"
        );
        let error = match SeedManifest::from_toml_str(&document) {
            Err(error) => error,
            Ok(_) => panic!("the malformed TOML must fail to parse"),
        };
        // It is a scrubbed TomlParse (not a spurious UnknownKey).
        assert!(
            matches!(error, ConfigError::TomlParse { .. }),
            "got {error:?}"
        );
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(
            !display.contains(MARKER),
            "TOML parse error Display leaked a secret: {display}"
        );
        assert!(
            !debug.contains(MARKER),
            "TOML parse error Debug leaked a secret: {debug}"
        );
        // The scrubbed message still carries a useful, snippet-free location.
        assert!(display.contains("line"), "scrubbed message: {display}");
    }

    /// The belt-and-braces scrub redacts a quoted literal only when the parser
    /// message names a secret-bearing key, and leaves non-secret messages intact.
    #[test]
    fn test_scrub_secret_literals_redacts_only_near_a_secret_key() {
        let scrubbed = scrub_secret_literals("invalid value for fix_password: \"hunter2\"");
        assert!(!scrubbed.contains("hunter2"), "must redact: {scrubbed}");
        assert!(scrubbed.contains(REDACTED));
        // A non-secret message keeps its quoted value (diagnostic value preserved).
        let plain = scrub_secret_literals("invalid socket address \"nope\"");
        assert_eq!(plain, "invalid socket address \"nope\"");
    }

    #[test]
    fn test_config_load_from_file_populates_seed() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!(
            "fauxchange-seed-{pid}-{nanos}.toml",
            pid = std::process::id(),
            nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        ));
        std::fs::write(&path, SEED_DOC)?;
        let args = vec!["--config".to_string(), path.display().to_string()];
        let config = Config::load_from(args, |_| None)?;
        let _ = std::fs::remove_file(&path);
        // The seed rode through the layered loader onto `Config::seed`.
        assert_eq!(config.seed.contract_count(), 8);
        assert_eq!(config.seed.accounts().len(), 2);
        Ok(())
    }
}
