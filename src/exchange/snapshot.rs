//! Book **snapshot + restore** over a consistent cut, with a fresh journal
//! epoch — the operator escape hatch that is an **explicit replay exclusion**
//! ([009](../../../milestones/v0.1-backend-core/009-snapshot-restore.md),
//! [02 §9](../../../docs/02-matching-architecture.md#9-snapshots-and-restore),
//! [03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification),
//! [01 §6.1](../../../docs/01-domain-model.md#61-order-identity-and-cross-protocol-idempotency)).
//!
//! ## What a snapshot is (and is not)
//!
//! A snapshot captures **state**, not the *sequence of decisions*. It is
//! therefore excluded from the replay contract: rather than inject a book the
//! journal never produced, a **restore** starts a *new journal epoch* over the
//! captured cut ([`crate::exchange::SnapshotRestored`]). The determinism oracle
//! ([02 §5](../../../docs/02-matching-architecture.md)) applies **within** an
//! epoch, never across a restore boundary.
//!
//! ## The consistent cut — four stores captured together
//!
//! A [`VenueSnapshot`] is an atomic cut, as of one instant, of the **four**
//! derived stores plus config/version [`SnapshotMetadata`]:
//!
//! 1. the leaf **books** — the resting orders ([`RestingOrderCapture`], read from
//!    the upstream book so partially-filled makers carry their *current* resting
//!    quantity);
//! 2. the **executions** log ([`ExecutionCapture`]);
//! 3. the **positions** fold ([`PositionCapture`]);
//! 4. the per-account **client-order-id idempotency map** ([`IdempotencyMap`] →
//!    [`IdempotencyRecord`]) — each key's payload fingerprint and terminal
//!    result.
//!
//! Capturing the idempotency map in the **same** cut is what lets a duplicate
//! `ClOrdID`/client-id retried **after** a restore return the stored terminal
//! result instead of opening a second order ([01 §6.1](../../../docs/01-domain-model.md)).
//!
//! ## Excluded, recomputed live
//!
//! Non-journaled derived analytics — mark price, unrealised P&L, Greeks, the
//! upstream `instrument_sequence` registry ids — are **not** in the cut. They
//! recompute live from the reconstructed books
//! ([02 §5.5](../../../docs/02-matching-architecture.md)).
//!
//! ## All-or-nothing swap
//!
//! A restore is atomic: in memory a pointer/content swap under actor quiescence,
//! and — the durable seam — **one PostgreSQL transaction** once the durable
//! journal/stores land (v0.3, #023/#029). A mid-restore fault rolls back all
//! four stores to their pre-restore state. This module owns the **pure** cut
//! types, the idempotency map, and the metadata contract; the single-writer
//! restore choreography that writes the epoch marker and continues the
//! `underlying_sequence` lives on the actor ([`crate::exchange::actor`]).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::exchange::boundary::{Hash32, Side, TimeInForce};
use crate::exchange::envelope::VenueOutcome;
use crate::exchange::event::EventTimestamp;
use crate::exchange::identity::{LineageId, VENUE_ENVELOPE_SCHEMA};
use crate::exchange::money::Cents;
use crate::exchange::symbol::Symbol;
use crate::models::{AccountId, ClientOrderId, ExecutionRecord, OrderType, VenueOrderId};

// ============================================================================
// Snapshot error
// ============================================================================

/// A typed snapshot/restore failure ([009](../../../milestones/v0.1-backend-core/009-snapshot-restore.md)).
///
/// A restore is **all-or-nothing**: every variant here is raised **before** any
/// store is mutated (metadata validation, then the fallible book-rebuild
/// preparation, then the epoch-marker append), so a failure leaves the four
/// stores untouched. `#013` maps these onto the REST/FIX boundary error; this is
/// the venue-domain typed error, not an `anyhow`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    /// The snapshot's config/version metadata does not match the running venue
    /// (schema, lineage, or config fingerprint) — the snapshot is refused rather
    /// than restored into an incompatible venue.
    #[error("snapshot metadata mismatch: {0}")]
    MetadataMismatch(String),
    /// Rebuilding the book image from the cut failed (a malformed capture) —
    /// raised during the **preparation** phase, before any store is swapped, so
    /// the restore rolls back cleanly.
    #[error("snapshot restore could not rebuild the book: {0}")]
    RebuildFailed(String),
    /// The epoch-marker append to the journal did not commit; the restore is
    /// abandoned and no store is swapped.
    #[error("journal unavailable while opening the restore epoch")]
    JournalUnavailable,
    /// The `underlying_sequence` cannot advance past `u64::MAX` to open the
    /// epoch — it never wraps.
    #[error("underlying_sequence exhausted while opening the restore epoch")]
    SequenceExhausted,
}

// ============================================================================
// Snapshot metadata (config/version validation)
// ============================================================================

/// The config/version metadata a restore validates against the running venue
/// ([02 §9](../../../docs/02-matching-architecture.md)).
///
/// A restore is refused unless the snapshot's `schema_version`, `lineage_id`,
/// and `config_fingerprint` all match the running venue — so a snapshot can
/// never be restored into a venue whose envelope schema, run lineage, or
/// microstructure config differs. The `config_fingerprint` is an opaque digest
/// the venue supplies; the real fingerprint is wired when the venue config lands
/// (`src/config.rs`), and this type fixes the contract now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotMetadata {
    /// The snapshot identifier.
    pub snapshot_id: String,
    /// Capture time on the venue clock, in **milliseconds**.
    pub created_at: EventTimestamp,
    /// The envelope schema the snapshot was captured under (`"venue.v1"`).
    pub schema_version: String,
    /// The run lineage — carried forward by the restore's epoch marker so ids
    /// keep minting in the same namespace.
    pub lineage_id: LineageId,
    /// An opaque digest of the venue config the snapshot was captured under.
    pub config_fingerprint: String,
}

impl SnapshotMetadata {
    /// Builds metadata stamped with the current envelope schema.
    #[must_use]
    #[inline]
    pub fn new(
        snapshot_id: impl Into<String>,
        created_at: EventTimestamp,
        lineage_id: LineageId,
        config_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            created_at,
            schema_version: VENUE_ENVELOPE_SCHEMA.to_string(),
            lineage_id,
            config_fingerprint: config_fingerprint.into(),
        }
    }

    /// Validates this snapshot's metadata against the running venue.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::MetadataMismatch`] if the schema version, run
    /// lineage, or config fingerprint differs from the running venue's — the
    /// snapshot is refused rather than restored into an incompatible venue.
    pub fn validate_against(
        &self,
        running_lineage: &LineageId,
        running_config_fingerprint: &str,
    ) -> Result<(), SnapshotError> {
        if self.schema_version != VENUE_ENVELOPE_SCHEMA {
            return Err(SnapshotError::MetadataMismatch(format!(
                "schema {} is not the running {VENUE_ENVELOPE_SCHEMA}",
                self.schema_version
            )));
        }
        if &self.lineage_id != running_lineage {
            return Err(SnapshotError::MetadataMismatch(
                "snapshot lineage does not match the running venue".to_string(),
            ));
        }
        if self.config_fingerprint != running_config_fingerprint {
            return Err(SnapshotError::MetadataMismatch(
                "snapshot config fingerprint does not match the running venue".to_string(),
            ));
        }
        Ok(())
    }
}

// ============================================================================
// Idempotency map (the fourth captured store)
// ============================================================================

/// The account-scoped idempotency key — one key across REST `client_order_id`
/// and FIX `ClOrdID (11)` ([01 §6.1](../../../docs/01-domain-model.md)).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyKey {
    /// The owning account.
    pub account: AccountId,
    /// The client-supplied idempotency key.
    pub client_order_id: ClientOrderId,
}

impl IdempotencyKey {
    /// Builds an idempotency key.
    #[must_use]
    #[inline]
    pub fn new(account: AccountId, client_order_id: ClientOrderId) -> Self {
        Self {
            account,
            client_order_id,
        }
    }
}

/// The payload fingerprint the venue persists per idempotency key — the fields
/// that make two placements "the same order" ([01 §6.1](../../../docs/01-domain-model.md)).
///
/// A retry with the **same** key and a matching fingerprint returns the stored
/// terminal result; a **different** fingerprint at the same key is a conflicting
/// reuse and is rejected rather than silently rebound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyFingerprint {
    /// The target contract symbol.
    pub symbol: Symbol,
    /// Order side.
    pub side: Side,
    /// Limit vs market.
    pub order_type: OrderType,
    /// Limit price in **cents** (absent for a market order).
    pub limit_price: Option<Cents>,
    /// Order quantity in **contracts**.
    pub quantity: u64,
    /// Time in force.
    pub time_in_force: TimeInForce,
}

/// One idempotency entry — the terminal result the venue replays to a duplicate
/// retry: the assigned venue order id and the placement's terminal
/// [`VenueOutcome`] ([01 §6.1](../../../docs/01-domain-model.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyEntry {
    /// The payload fingerprint recorded at first placement.
    pub fingerprint: IdempotencyFingerprint,
    /// The venue order id assigned to the original placement.
    pub order_id: VenueOrderId,
    /// The captured terminal outcome to replay to a matching retry.
    pub terminal: VenueOutcome,
}

/// One serialisable idempotency record — an [`IdempotencyKey`] and its
/// [`IdempotencyEntry`], the wire element captured in the cut.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyRecord {
    /// The account-scoped key.
    pub key: IdempotencyKey,
    /// The stored fingerprint + terminal result.
    pub entry: IdempotencyEntry,
}

/// The per-account **client-order-id idempotency map** — the fourth store in the
/// consistent cut ([01 §6.1](../../../docs/01-domain-model.md)).
///
/// It is owned by the single-writer executor (so lookups/records consult no
/// wall-clock, RNG, or map-iteration order on the sequenced path) and is a
/// **deterministic function of the journal** — a replay re-executes each
/// journaled `AddOrder` and re-populates the identical map. A restore rehydrates
/// it atomically with the books it refers to, so a retry after restore returns
/// the stored terminal result.
///
/// `#009` builds the map + its capture/restore and the **minimum** dedup on the
/// execute path (a matching retry returns the stored result; a conflicting reuse
/// is rejected). The full pre-journal dedup, cancel/replace `OrigClOrdID`
/// correlation, and retention-window eviction are completed by the later
/// FIX/idempotency issue.
#[derive(Debug, Clone, Default)]
pub struct IdempotencyMap {
    entries: HashMap<IdempotencyKey, IdempotencyEntry>,
}

impl IdempotencyMap {
    /// Builds an empty map.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Looks up the stored entry for `key`, if any.
    #[must_use]
    #[inline]
    pub fn lookup(&self, key: &IdempotencyKey) -> Option<&IdempotencyEntry> {
        self.entries.get(key)
    }

    /// Records (or overwrites) the terminal result for `key`. Overwriting the
    /// identical key with the identical entry is a no-op, so a deterministic
    /// re-execution on replay is idempotent.
    pub fn record(&mut self, key: IdempotencyKey, entry: IdempotencyEntry) {
        self.entries.insert(key, entry);
    }

    /// The number of stored keys.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map holds no keys.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Captures the map as a **deterministically ordered** vec of records
    /// (sorted by `(account, client_order_id)`), so a captured cut round-trips
    /// identically regardless of `HashMap` iteration order.
    #[must_use]
    pub fn capture(&self) -> Vec<IdempotencyRecord> {
        let mut records: Vec<IdempotencyRecord> = self
            .entries
            .iter()
            .map(|(key, entry)| IdempotencyRecord {
                key: key.clone(),
                entry: entry.clone(),
            })
            .collect();
        records.sort_by(|a, b| {
            a.key
                .account
                .as_str()
                .cmp(b.key.account.as_str())
                .then_with(|| {
                    a.key
                        .client_order_id
                        .as_str()
                        .cmp(b.key.client_order_id.as_str())
                })
        });
        records
    }

    /// Rebuilds a map from captured records — the restore side of [`capture`].
    ///
    /// [`capture`]: IdempotencyMap::capture
    #[must_use]
    pub fn from_records(records: &[IdempotencyRecord]) -> Self {
        let mut entries = HashMap::with_capacity(records.len());
        for record in records {
            entries.insert(record.key.clone(), record.entry.clone());
        }
        Self { entries }
    }
}

// ============================================================================
// Per-store captures
// ============================================================================

/// One resting order in the book cut, read from the **upstream book** so a
/// partially-filled maker carries its *current* resting quantity (never the
/// stale original) ([02 §9](../../../docs/02-matching-architecture.md)).
///
/// `engine_seq` is the per-underlying sequence the engine order id was minted
/// from ([`crate::exchange::MatchingExecutor`]); restoring re-adds each order
/// with `OrderId::sequential(engine_seq)` in ascending order so price-time
/// priority is reproduced and the venue↔engine id mapping stays stable (there is
/// no collision because the continued `underlying_sequence` is already past
/// every captured `engine_seq`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestingOrderCapture {
    /// The contract symbol whose leaf the order rests on.
    pub symbol: Symbol,
    /// The venue order id.
    pub order_id: VenueOrderId,
    /// The owning account.
    pub account: AccountId,
    /// The STP owner hash.
    pub owner: Hash32,
    /// The per-underlying sequence the engine order id was minted from.
    pub engine_seq: u64,
    /// The resting side.
    pub side: Side,
    /// The resting price in **cents**.
    pub price: Cents,
    /// The **current** resting quantity in **contracts**.
    pub quantity: u64,
    /// The resting time in force.
    pub time_in_force: TimeInForce,
}

/// One executions-log leg in the cut — the authoritative [`ExecutionRecord`]
/// plus its insertion-order surrogate, so restore preserves list ordering.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionCapture {
    /// The monotonic insertion index (the `SERIAL` surrogate).
    pub ord: u64,
    /// The recorded execution leg.
    pub record: ExecutionRecord,
}

/// One `(account, symbol)` position fold in the cut — the **exact integer-cents**
/// accumulators, not the derived projection (mark / unrealised P&L recompute
/// live).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionCapture {
    /// The owning account.
    pub account: AccountId,
    /// The contract symbol.
    pub symbol: Symbol,
    /// The underlying ticker.
    pub underlying: String,
    /// Net position in **signed contracts**.
    pub net_quantity: i64,
    /// Signed cost basis of the open position, in cents.
    pub basis: i128,
    /// Running `−Σ(signed_qty × price)`, in cents.
    pub cash_ex_fee: i128,
    /// Running `Σ(fee)`, in cents.
    pub fees: i128,
}

// ============================================================================
// The full venue snapshot
// ============================================================================

/// The executor's contribution to the cut — the leaf books and the idempotency
/// map, captured and restored together under the single writer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorState {
    /// The resting orders reconstructing the leaf books.
    pub resting_orders: Vec<RestingOrderCapture>,
    /// The idempotency map records.
    pub idempotency: Vec<IdempotencyRecord>,
}

/// A complete venue snapshot — the atomic consistent cut of the four derived
/// stores plus config/version metadata ([02 §9](../../../docs/02-matching-architecture.md)).
///
/// Serialisable end-to-end so the durable v0.3 store can persist it and so the
/// wire shape is golden-testable; the in-memory restore rehydrates it under
/// actor quiescence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueSnapshot {
    /// The config/version metadata validated on restore.
    pub metadata: SnapshotMetadata,
    /// The executor cut — leaf books + idempotency map.
    pub executor: ExecutorState,
    /// The executions log cut.
    pub executions: Vec<ExecutionCapture>,
    /// The positions fold cut.
    pub positions: Vec<PositionCapture>,
}

impl VenueSnapshot {
    /// The number of distinct leaf books captured (for the admin DTO's
    /// `orderbooks_saved`).
    #[must_use]
    pub fn orderbook_count(&self) -> u64 {
        let mut symbols: Vec<&str> = self
            .executor
            .resting_orders
            .iter()
            .map(|order| order.symbol.as_str())
            .collect();
        symbols.sort_unstable();
        symbols.dedup();
        symbols.len() as u64
    }

    /// The total number of resting orders captured (for the admin DTO's
    /// `orders_saved`).
    #[must_use]
    pub fn order_count(&self) -> u64 {
        self.executor.resting_orders.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::event::SequenceNumber;
    use crate::exchange::identity::LineageId;

    fn sym() -> Symbol {
        match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        }
    }

    fn fingerprint() -> IdempotencyFingerprint {
        IdempotencyFingerprint {
            symbol: sym(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 2,
            time_in_force: TimeInForce::Gtc,
        }
    }

    fn entry(order_seq: u64) -> IdempotencyEntry {
        let lineage = LineageId::new("run-1");
        IdempotencyEntry {
            fingerprint: fingerprint(),
            order_id: lineage.venue_order_id("BTC", SequenceNumber::new(order_seq), 0),
            terminal: VenueOutcome::Added {
                fills: vec![],
                resting_quantity: 2,
                stp_cancelled: vec![],
            },
        }
    }

    #[test]
    fn test_idempotency_map_records_and_looks_up() {
        let mut map = IdempotencyMap::new();
        assert!(map.is_empty());
        let key = IdempotencyKey::new(AccountId::new("acct-1"), ClientOrderId::new("c-1"));
        map.record(key.clone(), entry(0));
        assert_eq!(map.len(), 1);
        let looked = map.lookup(&key).expect("recorded entry");
        assert_eq!(looked.order_id.as_str(), "run-1:BTC:0:0");
    }

    #[test]
    fn test_idempotency_capture_is_sorted_and_round_trips() {
        let mut map = IdempotencyMap::new();
        // Insert out of order; capture must be deterministically sorted by key.
        map.record(
            IdempotencyKey::new(AccountId::new("b-acct"), ClientOrderId::new("c-9")),
            entry(9),
        );
        map.record(
            IdempotencyKey::new(AccountId::new("a-acct"), ClientOrderId::new("c-2")),
            entry(2),
        );
        map.record(
            IdempotencyKey::new(AccountId::new("a-acct"), ClientOrderId::new("c-1")),
            entry(1),
        );
        let captured = map.capture();
        let order: Vec<(&str, &str)> = captured
            .iter()
            .map(|r| (r.key.account.as_str(), r.key.client_order_id.as_str()))
            .collect();
        assert_eq!(
            order,
            vec![("a-acct", "c-1"), ("a-acct", "c-2"), ("b-acct", "c-9")]
        );
        // Round-trip through the wire records reconstructs the identical map.
        let rebuilt = IdempotencyMap::from_records(&captured);
        assert_eq!(rebuilt.capture(), captured);
    }

    #[test]
    fn test_metadata_validation_accepts_a_matching_venue() {
        let lineage = LineageId::new("run-1");
        let meta = SnapshotMetadata::new(
            "snap-1",
            EventTimestamp::new(1_700_000_000_000),
            lineage.clone(),
            "cfg-abc",
        );
        assert!(meta.validate_against(&lineage, "cfg-abc").is_ok());
    }

    #[test]
    fn test_metadata_validation_rejects_lineage_and_config_mismatch() {
        let lineage = LineageId::new("run-1");
        let meta = SnapshotMetadata::new(
            "snap-1",
            EventTimestamp::new(1_700_000_000_000),
            lineage.clone(),
            "cfg-abc",
        );
        // A different run lineage is refused.
        match meta.validate_against(&LineageId::new("run-2"), "cfg-abc") {
            Err(SnapshotError::MetadataMismatch(_)) => {}
            other => panic!("expected a lineage mismatch, got {other:?}"),
        }
        // A different config fingerprint is refused.
        match meta.validate_against(&lineage, "cfg-DIFFERENT") {
            Err(SnapshotError::MetadataMismatch(_)) => {}
            other => panic!("expected a config mismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_venue_snapshot_round_trips_through_serde() {
        let lineage = LineageId::new("run-1");
        let snapshot = VenueSnapshot {
            metadata: SnapshotMetadata::new(
                "snap-1",
                EventTimestamp::new(1_700_000_000_000),
                lineage.clone(),
                "cfg-abc",
            ),
            executor: ExecutorState {
                resting_orders: vec![RestingOrderCapture {
                    symbol: sym(),
                    order_id: lineage.venue_order_id("BTC", SequenceNumber::new(0), 0),
                    account: AccountId::new("maker"),
                    owner: Hash32([0x11; 32]),
                    engine_seq: 0,
                    side: Side::Sell,
                    price: Cents::new(50_000),
                    quantity: 3,
                    time_in_force: TimeInForce::Gtc,
                }],
                idempotency: vec![IdempotencyRecord {
                    key: IdempotencyKey::new(AccountId::new("maker"), ClientOrderId::new("c-0")),
                    entry: entry(0),
                }],
            },
            executions: vec![],
            positions: vec![],
        };
        let json = match serde_json::to_string(&snapshot) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_str::<VenueSnapshot>(&json) {
            Ok(back) => assert_eq!(back, snapshot),
            Err(e) => panic!("deserialize failed: {e}"),
        }
        assert_eq!(snapshot.orderbook_count(), 1);
        assert_eq!(snapshot.order_count(), 1);
    }
}
