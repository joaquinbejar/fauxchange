//! Application layer: the bounded **seeding phase** (#024) — the startup window in
//! which the venue applies a [`crate::config::SeedManifest`] to a
//! freshly-assembled [`AppState`] in a **fixed manifest order**, *before* it flips
//! to serving ([06 §7](../docs/06-deployment.md#7-seed-data-and-scenarios),
//! [03 §10](../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
//!
//! ## What the phase applies, and through which wired paths
//!
//! The seed manifest is validated at **load** (in [`crate::config`]); this module
//! is the *application* half. In fixed manifest order it:
//!
//! 1. applies the default market-maker **persona** knobs to the engine
//!    ([`MarketMakerEngine`](crate::market_maker::MarketMakerEngine));
//! 2. **provisions accounts** idempotently into the [`AccountRegistry`] (#012),
//!    hashing each FIX password with Argon2id and dropping the plaintext;
//! 3. **registers each contract** with the market maker (persona quoting);
//! 4. sets each underlying's **opening price** through the price seam
//!    ([`PriceSimulator::set_price`](crate::simulation::PriceSimulator::set_price),
//!    #016) — a journaled [`SimStep`](crate::exchange::VenueCommand::SimStep) plus
//!    the market maker's requote, whose `AddOrder`s **vivify** the leaf books onto
//!    the shared symbol index; then
//! 5. **settles** — a bounded wait for the off-thread requote forwarders to vivify
//!    the full chain, so the hierarchy is present before the caller flips to
//!    serving with [`AppState::begin_serving`].
//!
//! ## Population path is honest about the seam (no fabricated hierarchy-create)
//!
//! The inherited REST hierarchy-create routes (`POST /api/v1/underlyings/…`) are
//! **stubs** that refuse (manifest input): there is no sequenced hierarchy-CRUD
//! command upstream, and a leaf book only exists once an order **vivifies** it
//! (`get_or_create_*`). So the instrument set is *not* populated by REST create
//! calls — it is established by the persona registration + the opening-price seed,
//! whose market-maker quotes vivify the leaves through the **sequenced order
//! path**. This is the honest, wired population path; it is journaled and
//! replayable, and it never touches a book directly.
//!
//! ## Determinism ([02 §5.2](../docs/02-matching-architecture.md#5-determinism))
//!
//! Every step iterates the manifest in the fixed sorted order the resolved
//! [`crate::config::SeedManifest`] already fixes (sorted underlyings
//! → sorted expirations → sorted strikes → `call, put`), and the market maker's
//! requote-command forwarder is a single ordered task, so the vivification ids are
//! a reproducible function of the manifest. The settle is a bounded *completeness*
//! wait; it does not change the order.
//!
//! ## Idempotent re-seed
//!
//! Re-applying the same manifest is a no-op: [`register_instrument`] is idempotent
//! per symbol, [`set_price`] re-sets the same opening price, and
//! [`provision_seed_accounts`] treats an account already present at the **same**
//! shape (permissions + FIX username) as a no-op. A **conflicting** re-seed is a
//! typed error — a different opening price for an already-seeded underlying, or an
//! account id re-provisioned with different permissions.
//!
//! [`register_instrument`]: crate::market_maker::MarketMakerEngine::register_instrument
//! [`set_price`]: crate::simulation::PriceSimulator::set_price

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{AccountProvision, AccountRegistry, AccountStore, AuthError};
use crate::config::{DEFAULT_SEED_VOLATILITY, SeedManifest};
use crate::simulation::{AssetConfig, SimError, WalkTypeConfig};
use crate::state::AppState;

/// The bounded number of settle polls waiting for the async requote forwarders to
/// vivify the seeded chain — a DoS-free ceiling, never an unbounded spin.
const SETTLE_MAX_POLLS: usize = 400;

/// The delay between settle polls, in **milliseconds**.
const SETTLE_POLL_MS: u64 = 5;

// ============================================================================
// Errors + report
// ============================================================================

/// A failure applying the seed manifest during the bounded seeding phase.
///
/// Distinct from the load-time [`ConfigError`](crate::config::ConfigError) (which
/// validates the manifest *shape*) — these are **apply-time** failures against a
/// running venue: a provisioning collision, a conflicting re-seed, or an unhosted
/// underlying. No variant carries a credential.
#[derive(Debug, thiserror::Error)]
pub enum SeedError {
    /// Provisioning an account into the registry failed (a duplicate FIX username,
    /// or the reserved market-maker identity). The cause is the registry's own
    /// non-secret label.
    #[error("seed account provisioning failed: {0}")]
    Account(#[from] AuthError),
    /// A re-seed named an account id already present with a **different** shape
    /// (permissions or FIX username), so it is not an idempotent no-op.
    #[error(
        "seed account '{id}' conflicts with an already-provisioned account \
         (different permissions or FIX username)"
    )]
    AccountConflict {
        /// The conflicting account id (safe to echo — not a secret).
        id: String,
    },
    /// A re-seed named an underlying already seeded at a **different** opening
    /// price — a conflicting instrument spec, not an idempotent no-op.
    #[error(
        "seed instrument '{underlying}' conflicts: opening price {existing} cents already seeded, \
         the manifest requests {requested} cents"
    )]
    InstrumentPriceConflict {
        /// The conflicting underlying.
        underlying: String,
        /// The already-seeded opening price, in cents.
        existing: u64,
        /// The opening price the re-seed requested, in cents.
        requested: u64,
    },
    /// A seeded underlying is not a hosted price-seam asset — the caller built the
    /// [`AppState`] without the manifest's assets (a wiring error).
    #[error("seed underlying '{underlying}' is not a hosted price-seam asset")]
    UnknownUnderlying {
        /// The unhosted underlying.
        underlying: String,
    },
    /// A persona knob could not be applied to the engine (out of range) — should
    /// not occur, as the manifest validates persona ranges at load.
    #[error("seed persona could not be applied: {reason}")]
    Persona {
        /// The engine's range-check message.
        reason: String,
    },
}

/// A summary of what the seeding phase applied — for the boot log and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedReport {
    /// Accounts newly provisioned into the registry.
    pub accounts_provisioned: usize,
    /// Accounts already present at the same shape (idempotent no-ops).
    pub accounts_unchanged: usize,
    /// Underlyings seeded (one price-seam asset each).
    pub underlyings_seeded: usize,
    /// Canonical contracts registered with the market maker.
    pub contracts_registered: usize,
}

impl SeedReport {
    /// A secret-free one-line summary for the boot log.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "accounts_provisioned={} accounts_unchanged={} underlyings_seeded={} contracts_registered={}",
            self.accounts_provisioned,
            self.accounts_unchanged,
            self.underlyings_seeded,
            self.contracts_registered,
        )
    }
}

// ============================================================================
// Account provisioning (idempotent-aware)
// ============================================================================

/// Provisions the seeded accounts into `registry` **idempotently**: an account
/// already present at the **same** shape (permissions + FIX username) is a no-op,
/// a **different** shape is a [`SeedError::AccountConflict`], and a new account is
/// provisioned through the registry's guarded path (which hashes the plaintext FIX
/// password with Argon2id and refuses the reserved market-maker identity).
///
/// Returns `(newly_provisioned, unchanged)`.
///
/// # Errors
///
/// - [`SeedError::AccountConflict`] on a re-provision at a different shape;
/// - [`SeedError::Account`] if the registry rejects a new provision (a duplicate
///   FIX username, or the reserved market-maker id / owner).
pub fn provision_seed_accounts(
    registry: &AccountRegistry,
    provisions: &[AccountProvision],
) -> Result<(usize, usize), SeedError> {
    let mut provisioned = 0;
    let mut unchanged = 0;
    for provision in provisions {
        match registry.account(&provision.id) {
            Some(existing) => {
                // Idempotent: identical permissions + FIX username → no-op. The
                // stored password is an Argon2id hash and cannot be compared, so
                // the shape check is over the non-secret credential identity.
                if existing.permissions != provision.permissions
                    || existing.credentials.fix_username != provision.fix_username
                {
                    return Err(SeedError::AccountConflict {
                        id: provision.id.as_str().to_string(),
                    });
                }
                unchanged += 1;
            }
            None => {
                registry.provision_account(provision.clone())?;
                provisioned += 1;
            }
        }
    }
    Ok((provisioned, unchanged))
}

// ============================================================================
// Building the AppState inputs from a manifest
// ============================================================================

/// Builds the price-seam [`AssetConfig`]s the seeding phase sets opening prices
/// through — one per seeded underlying, at its opening price. The walk knobs are
/// placeholder defaults ([`DEFAULT_SEED_VOLATILITY`]); the walk loop is not
/// spawned at seed time, so they never drive a price on their own.
///
/// The caller threads these into [`AppStateConfig::with_assets`](crate::state::AppStateConfig::with_assets)
/// so [`PriceSimulator::set_price`](crate::simulation::PriceSimulator::set_price)
/// can journal the opening price for each underlying.
#[must_use]
pub fn asset_configs(manifest: &SeedManifest) -> Vec<AssetConfig> {
    manifest
        .instruments()
        .iter()
        .map(|set| {
            AssetConfig::new(
                set.underlying.clone(),
                set.opening_price,
                DEFAULT_SEED_VOLATILITY,
                WalkTypeConfig::GeometricBrownian,
            )
        })
        .collect()
}

// ============================================================================
// The seeding phase
// ============================================================================

/// Applies `manifest` to `state` in fixed manifest order (see the module docs),
/// leaving the venue **seeded but not yet serving** — the caller flips with
/// [`AppState::begin_serving`] before binding the gateways.
///
/// Idempotent: re-applying the same manifest is a no-op; a conflicting re-seed is
/// a typed error.
///
/// # Errors
///
/// A [`SeedError`] on an account collision/conflict, a conflicting instrument
/// opening price, or a seeded underlying the venue does not host as a price-seam
/// asset.
pub async fn apply_seed_phase(
    state: &Arc<AppState>,
    manifest: &SeedManifest,
) -> Result<SeedReport, SeedError> {
    let market_maker = state.market_maker();

    // 1. Personas are applied **per instrument** (step 3), not globally: each
    //    contract is bound to its resolved [`PersonaConfig`], and the engine's global
    //    config stays the **neutral overlay** (`1.0`/`1.0`/`0.0`) so a persona shapes
    //    quotes exactly once (#047). Applying the default persona to the global config
    //    *as well* would double-shape it (`persona.knob * config.knob`), so the seed
    //    phase deliberately leaves the global config untouched — runtime WS/REST
    //    controls remain the only writer of the global overlay.

    // 2. Accounts: idempotent provisioning into the #012 registry.
    let (accounts_provisioned, accounts_unchanged) =
        provision_seed_accounts(state.accounts(), manifest.accounts())?;

    // 3. Instruments: idempotent-conflict check + persona registration. Register
    //    the whole chain before any price so the first requote quotes it in full.
    //    An instrument bound to a defined persona (#047) registers with that
    //    persona's base spread / size + knobs and its seeded per-`(persona, symbol)`
    //    jitter; an unbound instrument follows the engine's global config.
    for set in manifest.instruments() {
        if let Some(existing) = market_maker.get_price(&set.underlying)
            && existing != set.opening_price.get()
        {
            return Err(SeedError::InstrumentPriceConflict {
                underlying: set.underlying.clone(),
                existing,
                requested: set.opening_price.get(),
            });
        }
        let persona = set.persona.as_ref().and_then(|name| {
            manifest
                .personas()
                .get(name)
                .map(|persona| (name.clone(), persona.to_persona_config()))
        });
        for symbol in &set.contracts {
            match &persona {
                Some((name, config)) => {
                    market_maker.register_instrument_with_persona(symbol, None, name, *config);
                }
                None => market_maker.register_instrument(symbol),
            }
        }
    }

    // 4. Opening prices → a journaled `SimStep` + the market maker's requote,
    //    whose `AddOrder`s vivify the leaf books onto the shared symbol index.
    for set in manifest.instruments() {
        state
            .simulator()
            .set_price(&set.underlying, set.opening_price)
            .map_err(|error| match error {
                SimError::UnknownUnderlying(_) => SeedError::UnknownUnderlying {
                    underlying: set.underlying.clone(),
                },
                other => SeedError::Persona {
                    reason: other.to_string(),
                },
            })?;
    }

    // 5. Settle: a bounded wait for the off-thread forwarders to vivify every
    //    seeded contract into the shared symbol index, so the hierarchy is present
    //    before the flip. (Upstream a strike node carries both its call and put
    //    book, so the index count is >= the seeded contracts — we wait for the
    //    seeded set specifically, not a raw count.)
    let expected: HashSet<&str> = manifest
        .instruments()
        .iter()
        .flat_map(|set| set.contracts.iter().map(|symbol| symbol.as_str()))
        .collect();
    settle_vivification(state, &expected).await;

    let report = SeedReport {
        accounts_provisioned,
        accounts_unchanged,
        underlyings_seeded: manifest.instruments().len(),
        contracts_registered: manifest.contract_count(),
    };
    tracing::info!(seed = %report.summary(), "bounded seeding phase applied");
    Ok(report)
}

/// A bounded wait for the async requote forwarders to vivify every seeded contract
/// into the shared symbol index. The order is deterministic (a single ordered
/// forwarder over a fixed enqueue order); this only waits for *completeness*, then
/// proceeds (logging a `WARN`) if the window elapses.
async fn settle_vivification(state: &Arc<AppState>, expected: &HashSet<&str>) {
    if expected.is_empty() {
        return;
    }
    for _ in 0..SETTLE_MAX_POLLS {
        let present: HashSet<String> = state.symbol_index().symbols().into_iter().collect();
        if expected.iter().all(|symbol| present.contains(*symbol)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(SETTLE_POLL_MS)).await;
    }
    let present = state.symbol_index().symbols().len();
    tracing::warn!(
        expected = expected.len(),
        vivified = present,
        "seed vivification did not complete within the settle window; proceeding"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AccountProvision, Argon2Hasher};
    use crate::exchange::Hash32;
    use crate::models::{AccountId, Permission};

    fn registry() -> AccountRegistry {
        AccountRegistry::new(Argon2Hasher::new(None))
    }

    fn provision(id: &str, owner: u8, permissions: Vec<Permission>) -> AccountProvision {
        AccountProvision::new(AccountId::new(id), Hash32([owner; 32]), permissions)
    }

    #[test]
    fn test_provision_seed_accounts_provisions_new() {
        let registry = registry();
        let provisions = vec![
            provision("reader", 1, vec![Permission::Read]),
            provision("trader", 2, vec![Permission::Read, Permission::Trade]),
        ];
        let (provisioned, unchanged) =
            provision_seed_accounts(&registry, &provisions).expect("provisioning");
        assert_eq!(provisioned, 2);
        assert_eq!(unchanged, 0);
        assert_eq!(registry.account_count(), 2);
    }

    #[test]
    fn test_provision_seed_accounts_is_idempotent() {
        let registry = registry();
        let provisions = vec![provision("reader", 1, vec![Permission::Read])];
        provision_seed_accounts(&registry, &provisions).expect("first");
        // Re-provisioning the same account is a no-op, not a duplicate error.
        let (provisioned, unchanged) =
            provision_seed_accounts(&registry, &provisions).expect("re-provision");
        assert_eq!(provisioned, 0);
        assert_eq!(unchanged, 1);
        assert_eq!(registry.account_count(), 1);
    }

    #[test]
    fn test_provision_seed_accounts_conflict_is_typed_error() {
        let registry = registry();
        provision_seed_accounts(&registry, &[provision("acct", 1, vec![Permission::Read])])
            .expect("first");
        // Same id, different permissions → a typed conflict, not a silent overwrite.
        let conflicting = vec![provision("acct", 1, vec![Permission::Admin])];
        match provision_seed_accounts(&registry, &conflicting) {
            Err(SeedError::AccountConflict { id }) => assert_eq!(id, "acct"),
            other => panic!("expected AccountConflict, got {other:?}"),
        }
    }
}
