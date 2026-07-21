//! The durable [`DatabasePool`] — the optional `sqlx` `PgPool` lifecycle: open a
//! pool from the config `DATABASE_URL`, run the embedded `migrations/` at boot,
//! and hand the pool to the repositories.
//!
//! The pool is **OPTIONAL** ([06 §6](../../docs/06-deployment.md#6-persistence)):
//! [`AppState`](crate::state::AppState) holds `db: Option<DatabasePool>`, `None`
//! when `DATABASE_URL` is unset (the venue runs fully in-memory). The pool size
//! and the slow-`acquire` warning threshold come from
//! [`DbPoolConfig`] (venue config, #022), never hard-coded.

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::config::PersistenceConfig;
use crate::db::error::DbError;

/// The embedded migration set — the `migrations/` directory compiled into the
/// binary (`sqlx::migrate!` reads the files at **compile** time, so this needs no
/// DB at build time and no filesystem at run time). Run at boot by
/// [`DatabasePool::run_migrations`].
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// The pool-tuning knobs sourced from venue config — the pool size and the
/// slow-`acquire` warning threshold ([06 §6](../../docs/06-deployment.md#6-persistence)).
///
/// These are **config, not hard-coded** (the #023 acceptance item): they carry
/// through from [`PersistenceConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbPoolConfig {
    /// The maximum number of pooled connections.
    pub max_connections: u32,
    /// The slow-`acquire` warning threshold — a pool acquire slower than this is
    /// logged at `WARN` by `sqlx` (`rules/global_rules.md` *Logging*).
    pub slow_acquire: Duration,
}

impl DbPoolConfig {
    /// Reads the pool knobs from the venue [`PersistenceConfig`].
    #[must_use]
    #[inline]
    pub fn from_persistence(persistence: &PersistenceConfig) -> Self {
        Self {
            max_connections: persistence.pool_max_connections(),
            slow_acquire: Duration::from_millis(persistence.slow_acquire_ms()),
        }
    }
}

/// The durable persistence pool — a thin owner of the `sqlx` [`PgPool`] plus the
/// boot-time migration step.
///
/// Cloning is cheap: [`PgPool`] is an `Arc` internally, so a `DatabasePool` clone
/// shares the one underlying connection pool. Constructed **only** when
/// `DATABASE_URL` is set; the DB-less path never builds one.
#[derive(Clone)]
pub struct DatabasePool {
    pool: PgPool,
}

impl std::fmt::Debug for DatabasePool {
    /// A minimal summary — never the connection string (the `DATABASE_URL` is a
    /// [`Secret`](crate::config::Secret) and is never logged,
    /// [08 §7](../../docs/08-threat-model.md#7-secrets-handling)).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabasePool")
            .field("size", &self.pool.size())
            .field("idle", &self.pool.num_idle())
            .finish_non_exhaustive()
    }
}

impl DatabasePool {
    /// Opens a `PgPool` against `database_url` with the configured size and
    /// slow-`acquire` threshold — the durable **pool** opened at boot by `main.rs`
    /// when `DATABASE_URL` is set. (Opening the pool + running migrations is live
    /// today; feeding live fills into the durable executions store is deferred to
    /// v0.3 (#029) — see the [`crate::db`] docs.)
    ///
    /// The `database_url` is exposed by the caller (`main.rs`) from the config
    /// [`Secret`](crate::config::Secret) at this **one** legitimate consumer; it
    /// is never logged here, and a connection failure never echoes it.
    ///
    /// # Errors
    ///
    /// [`DbError::Connect`] if the pool cannot be opened (connection refused, auth
    /// / TLS failure, or an acquire timeout). The underlying `sqlx::Error` is
    /// logged server-side and never leaked through this signature.
    pub async fn connect(database_url: &str, config: DbPoolConfig) -> Result<Self, DbError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_slow_threshold(config.slow_acquire)
            .connect(database_url)
            .await
            .map_err(|error| {
                // The `sqlx::Error` for a connect failure carries host/timeout
                // detail (never the password — `sqlx` masks it) and is safe to
                // log server-side; the DATABASE_URL itself is NEVER logged.
                tracing::error!(error = %error, "failed to open the durable database pool");
                DbError::Connect
            })?;
        tracing::info!(
            max_connections = config.max_connections,
            slow_acquire_ms = config.slow_acquire.as_millis() as u64,
            "durable database pool opened"
        );
        Ok(Self { pool })
    }

    /// Runs the embedded `migrations/` against the pool — called once at boot,
    /// after [`connect`](Self::connect).
    ///
    /// Idempotent: `sqlx` tracks applied migrations in `_sqlx_migrations`, so a
    /// restart re-applies nothing. Migrations are append-only and immutable once
    /// merged (`rules/global_rules.md` *SQL & Persistence*).
    ///
    /// # Errors
    ///
    /// [`DbError::Migrate`] if a migration cannot be applied (a schema conflict or
    /// an unreachable DB). The underlying cause is logged server-side.
    pub async fn run_migrations(&self) -> Result<(), DbError> {
        MIGRATOR.run(&self.pool).await.map_err(|error| {
            tracing::error!(error = %error, "failed to run database migrations");
            DbError::Migrate
        })?;
        tracing::info!(
            migrations = MIGRATOR.iter().count(),
            "database migrations applied"
        );
        Ok(())
    }

    /// Opens the pool **and** runs the migrations — the single boot entry point.
    ///
    /// # Errors
    ///
    /// [`DbError::Connect`] / [`DbError::Migrate`] as for the two steps.
    pub async fn connect_and_migrate(
        database_url: &str,
        config: DbPoolConfig,
    ) -> Result<Self, DbError> {
        let pool = Self::connect(database_url, config).await?;
        pool.run_migrations().await?;
        Ok(pool)
    }

    /// The underlying `sqlx` pool handle for the repositories.
    #[must_use]
    #[inline]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
