//! Run-lineage identity, the deterministic composite-id grammar, and the
//! journal header ([01 §6.1](../../../docs/01-domain-model.md),
//! [ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! Every venue-minted id is namespaced by a persisted **[`LineageId`]** so ids
//! never collide *across runs*, and folds in the disambiguating underlying, the
//! journaled per-underlying sequence, and an intra-command index so the several
//! ids one command mints never collide *within* a run. The grammar is a pure,
//! deterministic function of those inputs — a restart re-derives the identical
//! id (the `lineage_id` is read back from the [`JournalHeader`], not re-minted):
//!
//! ```text
//! order_id     = "{lineage_id}:{underlying}:{underlying_sequence}:{intra_command_index}"
//! execution_id = "{lineage_id}:{underlying}:{underlying_sequence}:{fill_index}"
//! ```
//!
//! `underlying` is **required** because `underlying_sequence` is a
//! *per-underlying* namespace ([01 §9.1](../../../docs/01-domain-model.md)): so
//! `BTC` sequence 1 and `ETH` sequence 1 cannot mint the same id even though the
//! `execution_id` is the cross-surface join key. The trailing index disambiguates
//! the several ids a single command mints (a fanned-out add, a match producing
//! several fills).
//!
//! These are the venue-assigned deterministic ids; the upstream engine's
//! process-local `OrderId` (`Uuid::new_v4`) and wall-clock trade timestamps are
//! **excluded from the determinism oracle** and never surfaced or journaled
//! ([ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! The single-writer actor that assigns the sequence and writes the header is
//! #006; these are pure data + pure constructors, with no actor or store
//! dependency.

use serde::{Deserialize, Serialize};

use crate::exchange::event::SequenceNumber;
use crate::models::{ExecutionId, VenueOrderId};

/// The schema tag pinning the versioned venue envelope wire contract.
///
/// Carried on every [`crate::exchange::VenueEvent`] and in the [`JournalHeader`].
/// A bump is a **major SemVer event** ([SEMVER.md](../../../docs/SEMVER.md)): the
/// tag and its golden are mandatory, and recovery refuses to start against a
/// journal whose schema is newer than the running binary understands
/// ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
pub const VENUE_ENVELOPE_SCHEMA: &str = "venue.v1";

/// The persisted run-lineage identity that namespaces every venue-minted id.
///
/// Minted **once, at venue-registry creation** (a fresh, empty journal), written
/// into the [`JournalHeader`], rehydrated on restart, and **carried across a
/// snapshot-restore epoch** by the `SnapshotRestored` marker so restored ids stay
/// in the same namespace ([01 §6.1, §9.2](../../../docs/01-domain-model.md)). Only
/// a brand-new registry mints a new one. This DTO layer does not mint it (that is
/// the registry / actor, #006); it carries the value and encapsulates the
/// composite-id grammar.
///
/// **Grammar invariant.** The composite id is colon-delimited, so a `LineageId`
/// must not itself contain a `:` or the segmentation would be ambiguous. The
/// minting side is responsible for that (an opaque token from a colon-free
/// alphabet); the constructors here do not re-encode. On the wire it is a bare
/// string (`#[serde(transparent)]`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LineageId(String);

impl LineageId {
    /// Wraps a raw lineage token.
    ///
    /// The token should come from a colon-free alphabet (see the grammar
    /// invariant on [`LineageId`]); this constructor does not validate or
    /// re-encode it, matching the opaque-token contract in
    /// [01 §6.1](../../../docs/01-domain-model.md).
    #[must_use]
    #[inline]
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// Returns the lineage token as a string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Mints the deterministic **venue order id** for an add
    /// (`"{lineage_id}:{underlying}:{underlying_sequence}:{intra_command_index}"`,
    /// [01 §6.1](../../../docs/01-domain-model.md)).
    ///
    /// `intra_command_index` disambiguates the several order ids one command can
    /// mint (a fanned-out add); a single-order add uses `0`. The result is a pure
    /// function of the inputs, so a restart against the same
    /// (`lineage_id`, `underlying`, `underlying_sequence`) re-derives the
    /// identical id.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::{LineageId, SequenceNumber};
    /// let lineage = LineageId::new("run-1");
    /// let id = lineage.venue_order_id("BTC", SequenceNumber::new(7), 0);
    /// assert_eq!(id.as_str(), "run-1:BTC:7:0");
    /// ```
    #[must_use]
    #[inline]
    pub fn venue_order_id(
        &self,
        underlying: &str,
        underlying_sequence: SequenceNumber,
        intra_command_index: u32,
    ) -> VenueOrderId {
        VenueOrderId::new(self.composite(underlying, underlying_sequence, intra_command_index))
    }

    /// Mints the deterministic **execution id** for a fill
    /// (`"{lineage_id}:{underlying}:{underlying_sequence}:{fill_index}"`,
    /// [01 §6.1, §7](../../../docs/01-domain-model.md)).
    ///
    /// The `fill_index` enumerates matches within the aggressing command, so the
    /// **two legs of one match share the same `execution_id`** — pass the same
    /// `fill_index` for the maker and taker leg. Like [`Self::venue_order_id`] it
    /// is a pure function of its inputs.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::{LineageId, SequenceNumber};
    /// let lineage = LineageId::new("run-1");
    /// // Both legs of the first match of command 7 share this id.
    /// let maker = lineage.execution_id("BTC", SequenceNumber::new(7), 0);
    /// let taker = lineage.execution_id("BTC", SequenceNumber::new(7), 0);
    /// assert_eq!(maker, taker);
    /// ```
    #[must_use]
    #[inline]
    pub fn execution_id(
        &self,
        underlying: &str,
        underlying_sequence: SequenceNumber,
        fill_index: u32,
    ) -> ExecutionId {
        ExecutionId::new(self.composite(underlying, underlying_sequence, fill_index))
    }

    /// The shared composite-id grammar for both order ids and execution ids.
    #[must_use]
    #[inline]
    fn composite(&self, underlying: &str, sequence: SequenceNumber, index: u32) -> String {
        format!("{}:{}:{}:{}", self.0, underlying, sequence.get(), index)
    }
}

/// The journal header — the first record of a venue journal, carrying the run's
/// [`LineageId`] and the envelope [`schema_version`](JournalHeader::schema_version).
///
/// On restart, recovery reads the header **before** replaying so re-derived ids
/// land in the same namespace, and checks the schema version: a journal whose
/// schema is newer than the running binary is refused rather than
/// mis-interpreted ([ADR-0006 §3](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
/// This is the pure header type; the store that writes and reads it is #006/#029.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalHeader {
    /// The envelope schema this journal was written with — always
    /// [`VENUE_ENVELOPE_SCHEMA`] for a `venue.v1` journal.
    pub schema_version: String,
    /// The run lineage that namespaces every id in this journal.
    pub lineage_id: LineageId,
}

impl JournalHeader {
    /// Builds a header for the current envelope schema
    /// ([`VENUE_ENVELOPE_SCHEMA`]) and the given run lineage.
    #[must_use]
    #[inline]
    pub fn new(lineage_id: LineageId) -> Self {
        Self {
            schema_version: VENUE_ENVELOPE_SCHEMA.to_string(),
            lineage_id,
        }
    }

    /// Returns `true` iff this header's schema is the one the running binary
    /// understands ([`VENUE_ENVELOPE_SCHEMA`]).
    ///
    /// The recovery reducer (#006) uses this to refuse a forward-incompatible
    /// journal; here it is the pure predicate.
    #[must_use]
    #[inline]
    pub fn is_current_schema(&self) -> bool {
        self.schema_version == VENUE_ENVELOPE_SCHEMA
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_venue_order_id_follows_composite_grammar() {
        let lineage = LineageId::new("run-1");
        let id = lineage.venue_order_id("BTC", SequenceNumber::new(7), 0);
        assert_eq!(id.as_str(), "run-1:BTC:7:0");
    }

    #[test]
    fn test_execution_id_follows_composite_grammar() {
        let lineage = LineageId::new("run-1");
        let id = lineage.execution_id("BTC", SequenceNumber::new(7), 2);
        assert_eq!(id.as_str(), "run-1:BTC:7:2");
    }

    #[test]
    fn test_id_grammar_is_deterministic() {
        // The same inputs mint the identical id — the restart-stability property.
        let lineage = LineageId::new("run-1");
        let a = lineage.venue_order_id("BTC", SequenceNumber::new(7), 0);
        let b = LineageId::new("run-1").venue_order_id("BTC", SequenceNumber::new(7), 0);
        assert_eq!(a, b);
    }

    #[test]
    fn test_id_grammar_is_cross_underlying_unique() {
        // BTC sequence 1 and ETH sequence 1 must mint distinct ids because the
        // underlying segment disambiguates the per-underlying namespace.
        let lineage = LineageId::new("run-1");
        let btc = lineage.venue_order_id("BTC", SequenceNumber::new(1), 0);
        let eth = lineage.venue_order_id("ETH", SequenceNumber::new(1), 0);
        assert_ne!(btc, eth);
        assert_eq!(btc.as_str(), "run-1:BTC:1:0");
        assert_eq!(eth.as_str(), "run-1:ETH:1:0");
    }

    #[test]
    fn test_id_grammar_is_cross_run_unique() {
        // Two runs (distinct lineage ids) never mint the same id.
        let a = LineageId::new("run-1").venue_order_id("BTC", SequenceNumber::new(1), 0);
        let b = LineageId::new("run-2").venue_order_id("BTC", SequenceNumber::new(1), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn test_id_grammar_disambiguates_intra_command_index() {
        let lineage = LineageId::new("run-1");
        let first = lineage.venue_order_id("BTC", SequenceNumber::new(9), 0);
        let second = lineage.venue_order_id("BTC", SequenceNumber::new(9), 1);
        assert_ne!(first, second);
    }

    #[test]
    fn test_two_legs_of_one_match_share_execution_id() {
        // The maker and taker leg of a match carry the same fill_index, so they
        // share one execution_id — the cross-surface join key.
        let lineage = LineageId::new("run-1");
        let maker = lineage.execution_id("BTC", SequenceNumber::new(7), 0);
        let taker = lineage.execution_id("BTC", SequenceNumber::new(7), 0);
        assert_eq!(maker, taker);
        // A second match within the same command gets the next fill_index.
        let second_match = lineage.execution_id("BTC", SequenceNumber::new(7), 1);
        assert_ne!(maker, second_match);
    }

    #[test]
    fn test_order_id_and_execution_id_share_the_grammar() {
        // Both surfaces use the identical composite grammar, so an order id and an
        // execution id built from the same tuple are equal strings.
        let lineage = LineageId::new("run-1");
        let order = lineage.venue_order_id("BTC", SequenceNumber::new(3), 4);
        let exec = lineage.execution_id("BTC", SequenceNumber::new(3), 4);
        assert_eq!(order.as_str(), exec.as_str());
    }

    #[test]
    fn test_lineage_id_serialises_as_bare_string() {
        let json = match serde_json::to_string(&LineageId::new("run-1")) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "\"run-1\"");
    }

    #[test]
    fn test_journal_header_carries_schema_and_lineage() {
        let header = JournalHeader::new(LineageId::new("run-1"));
        assert_eq!(header.schema_version, VENUE_ENVELOPE_SCHEMA);
        assert_eq!(header.schema_version, "venue.v1");
        assert!(header.is_current_schema());
        assert_eq!(header.lineage_id, LineageId::new("run-1"));
    }

    #[test]
    fn test_journal_header_roundtrips_through_serde() {
        let header = JournalHeader::new(LineageId::new("run-1"));
        let json = match serde_json::to_string(&header) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_str::<JournalHeader>(&json) {
            Ok(back) => assert_eq!(back, header),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    #[test]
    fn test_journal_header_rejects_unknown_field() {
        let json = r#"{"schema_version":"venue.v1","lineage_id":"run-1","extra":true}"#;
        match serde_json::from_str::<JournalHeader>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected an unknown-field rejection, parsed {parsed:?}"),
        }
    }

    #[test]
    fn test_journal_header_detects_newer_schema() {
        let header = JournalHeader {
            schema_version: "venue.v2".to_string(),
            lineage_id: LineageId::new("run-1"),
        };
        assert!(!header.is_current_schema());
    }
}
