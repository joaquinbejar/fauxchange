//! **Adversarial fixture corpus** for the journal / replay / seed-bundle
//! deserialiser — the v0.3 **security gate** for the semi-trusted-operator (A-7)
//! decode surface
//! ([08 §4](../docs/08-threat-model.md#4-untrusted-input-hardening),
//! [08 §6](../docs/08-threat-model.md#6-fuzzing-and-adversarial-testing),
//! [TESTING.md §14](../docs/TESTING.md#14-security-testing), #034).
//!
//! ## What this proves
//!
//! Each committed hostile fixture under `tests/adversarial/` is fed to the **real**
//! deserialiser it targets and MUST produce the **correct typed reject** — asserted
//! by the **specific** [`JournalError`] / [`ReplayError`] / [`ConfigError`] variant,
//! never a blanket `is_err()` — with:
//!
//! - **no panic** (every decode runs the production parse path);
//! - **no unbounded allocation** (a per-record byte ceiling, a per-read record-count
//!   ceiling, and a total on-disk-bundle byte ceiling — all typed
//!   [`JournalError::ResourceLimit`] / [`ReplayError::ResourceLimit`] rejects);
//! - **no partial apply** — a rejected bundle/journal yields **no** `ReplayReport`
//!   (hence no reconstructed stores), so even a clean stream bundled beside a hostile
//!   one is discarded all-or-nothing.
//!
//! The corpus is committed as **files** so it also seeds the coverage-guided
//! `cargo fuzz` targets that land in v1.0 (#052); this issue is the fixture gate, the
//! fuzz harness is deliberately staged after it.
//!
//! ## Regenerating the corpus
//!
//! The committed files are (re)generated from the real envelope types with
//! `UPDATE_CORPUS=1 cargo test --test adversarial` (mirroring the `UPDATE_GOLDEN`
//! convention), then committed. The default run **reads the committed files** and
//! asserts the typed reject — it never regenerates, so a drift is a test failure.

use std::fs;
use std::path::PathBuf;

use fauxchange::OrderType;
use fauxchange::config::{ConfigError, SeedManifest};
use fauxchange::exchange::{
    ActorConfig, Cents, EventTimestamp, FixedClock, Hash32, InMemoryVenueJournal, JournalError,
    JournalHeader, JournalRecord, LineageId, MAX_JOURNAL_RECORD_BYTES, MatchingExecutor,
    NoopFanOut, STPMode, SequenceNumber, Side, TimeInForce, UnderlyingActor, VenueCommand,
    VenueEvent, VenueJournal, VenueOutcome, decode_journal_record,
};
use fauxchange::models::AccountId;
use fauxchange::simulation::{
    ClockMode, JournalStream, MAX_BUNDLE_BYTES, ReplayError, ReplayReport, RunManifest,
    ScenarioBundle, replay_bundle, replay_streams,
};

const UNDERLYING: &str = "BTC";
const CALL: &str = "BTC-20240329-50000-C";
const TS: EventTimestamp = EventTimestamp::new(1_700_000_000_000);

// ============================================================================
// Corpus file plumbing (read committed files; regenerate under UPDATE_CORPUS)
// ============================================================================

fn corpus_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/adversarial")
        .join(relative)
}

/// Returns the committed corpus bytes for `relative`, (re)writing them from
/// `produce` first when `UPDATE_CORPUS` is set. The assertion always feeds the
/// **on-disk** bytes to the deserialiser, so a committed file that drifts from its
/// producer is caught.
fn corpus(relative: &str, produce: impl FnOnce() -> String) -> String {
    let path = corpus_path(relative);
    if std::env::var_os("UPDATE_CORPUS").is_some() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create corpus dir");
        }
        fs::write(&path, produce()).expect("write corpus file");
    }
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read corpus file {}: {e}", path.display()))
}

// ============================================================================
// Fixtures — real recorded streams (valid venue.v1 bytes to mutate)
// ============================================================================

fn sym() -> fauxchange::exchange::Symbol {
    match fauxchange::exchange::Symbol::parse(CALL) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

fn add(
    lineage: &LineageId,
    sequence: u64,
    account: &str,
    side: Side,
    price: u64,
    qty: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(),
        order_id: lineage.venue_order_id(UNDERLYING, SequenceNumber::new(sequence), 0),
        account: AccountId::new(account),
        owner: Hash32([sequence as u8; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity: qty,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// Drives a command stream through a real single-writer actor and returns its
/// [`JournalStream`] — real `venue.v1` bytes for the corpus generators to mutate.
fn record_stream(
    underlying: &str,
    commands: &[VenueCommand],
    lineage: &LineageId,
) -> JournalStream {
    let header = JournalHeader::new(lineage.clone());
    let mut actor = UnderlyingActor::new(
        ActorConfig::new(underlying, lineage.clone(), 64),
        InMemoryVenueJournal::new(header.clone()),
        MatchingExecutor::new(underlying),
        NoopFanOut,
        FixedClock::new(TS),
    );
    for command in commands {
        actor.handle(command.clone()).expect("actor turn commits");
    }
    let records = actor
        .journal()
        .read_from(SequenceNumber::START)
        .expect("read journal");
    JournalStream::new(underlying, header, records)
}

/// A two-command crossing session: a resting sell (3 @ 50_000) crossed by a buy
/// (2 @ 50_000), so the stream has a real fill event at sequence 1 to tamper.
fn crossing(lineage: &LineageId) -> Vec<VenueCommand> {
    vec![
        add(lineage, 0, "maker", Side::Sell, 50_000, 3),
        add(lineage, 1, "taker", Side::Buy, 50_000, 2),
    ]
}

fn pretty(bundle: &ScenarioBundle) -> String {
    serde_json::to_string_pretty(bundle).expect("serialize bundle")
}

/// Loads a bundle from bytes and replays it — the full on-disk load path, so a
/// corpus file exercises decode + version-scoping + re-execution end to end.
fn load_and_replay(bytes: &str) -> Result<ReplayReport, ReplayError> {
    let bundle = ScenarioBundle::from_json(bytes)?;
    replay_bundle(&bundle)
}

// ============================================================================
// 1. Journal-record deserialiser — decode_journal_record -> JournalError
// ============================================================================
//
// Every malformed record class funnels to ONE typed decode reject
// (`JournalError::Backend { operation: "journal record decode" }`) — deliberately
// uniform, so the deserialiser leaks no parse-internals oracle to an attacker.
// The size class is the distinct `ResourceLimit` reject.

/// Asserts the committed hostile record file is a typed decode reject (no panic,
/// no record produced — nothing applied).
fn assert_record_decode_rejected(relative: &str, produce: impl FnOnce() -> String) {
    let bytes = corpus(relative, produce);
    match decode_journal_record(&bytes) {
        Err(JournalError::Backend { operation }) => {
            assert_eq!(
                operation, "journal record decode",
                "{relative}: uniform typed decode reject"
            );
        }
        other => panic!("{relative}: expected a typed Backend decode reject, got {other:?}"),
    }
}

#[test]
fn test_journal_corpus_truncated_record_is_typed_reject() {
    assert_record_decode_rejected("journal/truncated_record.json", || {
        r#"{"Command":{"sequence":0,"venue_ts":1700000000"#.to_string()
    });
}

#[test]
fn test_journal_corpus_unknown_field_injection_is_typed_reject() {
    // An injected unknown field inside the write-ahead command record — refused by
    // `deny_unknown_fields`, never silently ignored.
    assert_record_decode_rejected("journal/unknown_field_injection.json", || {
        r#"{"Command":{"sequence":0,"venue_ts":1700000000000,"command":{"CancelOrder":{"symbol":"BTC-20240329-50000-C","order_id":"o-1","account":"acct-1"}},"injected":"evil"}}"#.to_string()
    });
}

#[test]
fn test_journal_corpus_duplicate_field_is_typed_reject() {
    // A duplicate `sequence` key — serde rejects a duplicate struct field, so the
    // record cannot decode to an ambiguous value.
    assert_record_decode_rejected("journal/duplicate_field.json", || {
        r#"{"Command":{"sequence":0,"sequence":1,"venue_ts":1700000000000,"command":{"CancelOrder":{"symbol":"BTC-20240329-50000-C","order_id":"o-1","account":"acct-1"}}}}"#.to_string()
    });
}

#[test]
fn test_journal_corpus_missing_required_field_is_typed_reject() {
    // The write-ahead command record is missing its required `sequence`.
    assert_record_decode_rejected("journal/missing_required_field.json", || {
        r#"{"Command":{"venue_ts":1700000000000,"command":{"CancelOrder":{"symbol":"BTC-20240329-50000-C","order_id":"o-1","account":"acct-1"}}}}"#.to_string()
    });
}

#[test]
fn test_journal_corpus_unknown_variant_tag_is_typed_reject() {
    // An injected record kind that is not Command / Event / Epoch.
    assert_record_decode_rejected("journal/unknown_variant_tag.json", || {
        r#"{"Bogus":{"sequence":0,"venue_ts":1700000000000}}"#.to_string()
    });
}

#[test]
fn test_journal_corpus_malformed_symbol_is_typed_reject() {
    // A structurally valid AddOrder whose `symbol` is not a parseable venue symbol —
    // rejected at decode by the `Symbol` `try_from` validator, not at use.
    assert_record_decode_rejected("journal/malformed_symbol.json", || {
        r#"{"Command":{"sequence":0,"venue_ts":1700000000000,"command":{"AddOrder":{"symbol":"NOT-A-VENUE-SYMBOL","order_id":"o-1","account":"a","owner":"1111111111111111111111111111111111111111111111111111111111111111","client_order_id":null,"side":"BUY","order_type":"limit","limit_price":50000,"quantity":1,"time_in_force":"GTC","stp_mode":"None"}}}}"#.to_string()
    });
}

#[test]
fn test_journal_corpus_negative_cents_is_typed_reject() {
    // A negative `limit_price` — integer cents are `u64`, so a negative value is a
    // decode error, never a silent wrap.
    assert_record_decode_rejected("journal/negative_cents.json", || {
        r#"{"Command":{"sequence":0,"venue_ts":1700000000000,"command":{"AddOrder":{"symbol":"BTC-20240329-50000-C","order_id":"o-1","account":"a","owner":"1111111111111111111111111111111111111111111111111111111111111111","client_order_id":null,"side":"BUY","order_type":"limit","limit_price":-5,"quantity":1,"time_in_force":"GTC","stp_mode":"None"}}}}"#.to_string()
    });
}

#[test]
fn test_journal_corpus_overflow_quantity_is_typed_reject() {
    // A `quantity` above u64::MAX — out of the economic integer domain, a decode
    // error rather than a truncation.
    assert_record_decode_rejected("journal/overflow_quantity.json", || {
        r#"{"Command":{"sequence":0,"venue_ts":1700000000000,"command":{"AddOrder":{"symbol":"BTC-20240329-50000-C","order_id":"o-1","account":"a","owner":"1111111111111111111111111111111111111111111111111111111111111111","client_order_id":null,"side":"BUY","order_type":"limit","limit_price":50000,"quantity":99999999999999999999999,"time_in_force":"GTC","stp_mode":"None"}}}}"#.to_string()
    });
}

// ---- size class (generated inputs — a >64 KiB / >64 MiB file is not committed) --

#[test]
fn test_journal_deser_rejects_oversized_record() {
    // A record whose serialized form exceeds the per-record byte ceiling is refused
    // AT the ceiling — never decoded — so a hostile oversized record cannot drive an
    // unbounded decode allocation. Generated (a >64 KiB blob is volume, not a shape
    // seed; the shape seeds above are the committed files).
    let oversized = format!("\"{}\"", "a".repeat(MAX_JOURNAL_RECORD_BYTES));
    assert!(oversized.len() > MAX_JOURNAL_RECORD_BYTES);
    match decode_journal_record(&oversized) {
        Err(JournalError::ResourceLimit {
            limit,
            found,
            ceiling,
        }) => {
            assert_eq!(limit, "record_bytes");
            assert!(found > ceiling);
            assert_eq!(ceiling, MAX_JOURNAL_RECORD_BYTES);
        }
        other => panic!("expected a record_bytes ResourceLimit, got {other:?}"),
    }
}

// ============================================================================
// 2. Scenario-bundle deserialiser — from_json / replay_bundle -> ReplayError
// ============================================================================

#[test]
fn test_bundle_corpus_missing_manifest_is_typed_reject() {
    let bytes = corpus("bundle/missing_manifest.json", || {
        r#"{"schema":"scenario-bundle.v1","streams":[]}"#.to_string()
    });
    // The `manifest` field is required — a bundle without it does not decode.
    match ScenarioBundle::from_json(&bytes) {
        Err(ReplayError::BundleDecode(_)) => {}
        other => panic!("a manifest-less bundle must be a typed decode reject, got {other:?}"),
    }
}

#[test]
fn test_bundle_corpus_unknown_top_level_field_is_typed_reject() {
    let bytes = corpus("bundle/unknown_top_level_field.json", || {
        r#"{"schema":"scenario-bundle.v1","manifest":{"seed":0,"clock_mode":"realtime"},"streams":[],"typo":true}"#.to_string()
    });
    match ScenarioBundle::from_json(&bytes) {
        Err(ReplayError::BundleDecode(_)) => {}
        other => panic!("an unknown top-level field must be a decode reject, got {other:?}"),
    }
}

#[test]
fn test_bundle_corpus_truncated_bundle_is_typed_reject() {
    let bytes = corpus("bundle/truncated_bundle.json", || {
        r#"{"schema":"scenario-bundle.v1","manifest":{"seed":0,"#.to_string()
    });
    match ScenarioBundle::from_json(&bytes) {
        Err(ReplayError::BundleDecode(_)) => {}
        other => panic!("a truncated bundle must be a decode reject, got {other:?}"),
    }
}

#[test]
fn test_bundle_corpus_malformed_record_in_stream_is_typed_reject() {
    // A bundle that decodes at the top level but carries a malformed record (a bad
    // symbol) inside a stream — rejected during `from_json` at the nested `Symbol`.
    let bytes = corpus("bundle/malformed_record_in_stream.json", || {
        r#"{"schema":"scenario-bundle.v1","manifest":{"seed":0,"clock_mode":"realtime"},"streams":[{"underlying":"BTC","header":{"schema_version":"venue.v1","lineage_id":"run-1"},"records":[{"Command":{"sequence":0,"venue_ts":1700000000000,"command":{"CancelOrder":{"symbol":"NOT-A-SYMBOL","order_id":"o-1","account":"a"}}}}]}]}"#.to_string()
    });
    match ScenarioBundle::from_json(&bytes) {
        Err(ReplayError::BundleDecode(_)) => {}
        other => panic!("a malformed nested record must be a decode reject, got {other:?}"),
    }
}

#[test]
fn test_bundle_deser_refuses_newer_schema() {
    // A structurally valid bundle whose wire-contract `schema` is newer than this
    // binary — refused before any re-execution (the oracle holds only across a
    // matching version set).
    let bytes = corpus("bundle/newer_bundle_schema.json", || {
        let lineage = LineageId::new("run-1");
        let mut bundle = ScenarioBundle::new(
            RunManifest::new(0, ClockMode::Realtime),
            vec![record_stream(UNDERLYING, &crossing(&lineage), &lineage)],
        );
        bundle.schema = "scenario-bundle.v2".to_string();
        pretty(&bundle)
    });
    match load_and_replay(&bytes) {
        Err(ReplayError::VersionMismatch { kind, found, .. }) => {
            assert_eq!(kind, "bundle_schema");
            assert_eq!(found, "scenario-bundle.v2");
        }
        other => panic!("expected a bundle_schema VersionMismatch, got {other:?}"),
    }
}

#[test]
fn test_bundle_corpus_hostile_manifest_version_is_typed_reject() {
    // A bundle whose manifest pins a fauxchange version INCOMPATIBLE with this binary
    // under the SemVer-major load rule (a differing MINOR at the 0.x base is a breaking
    // boundary) — refused before re-execution. A benign patch bump would instead
    // replay; this fixture is a genuine incompatibility, not a mere version tweak.
    let bytes = corpus("bundle/hostile_manifest_version.json", || {
        let lineage = LineageId::new("run-1");
        let mut manifest = RunManifest::new(0, ClockMode::Realtime);
        manifest.versions.fauxchange = "0.1.0-attacker".to_string();
        pretty(&ScenarioBundle::new(
            manifest,
            vec![record_stream(UNDERLYING, &crossing(&lineage), &lineage)],
        ))
    });
    match load_and_replay(&bytes) {
        Err(ReplayError::VersionMismatch { kind, found, .. }) => {
            assert_eq!(kind, "fauxchange");
            assert_eq!(found, "0.1.0-attacker");
        }
        other => panic!("expected a manifest VersionMismatch, got {other:?}"),
    }
}

#[test]
fn test_bundle_corpus_newer_journal_schema_is_refused() {
    // A bundle whose stream header envelope schema is newer than the binary — the
    // recovery core refuses it (SchemaRefused), surfaced verbatim.
    let bytes = corpus("bundle/newer_journal_schema.json", || {
        let lineage = LineageId::new("run-1");
        let mut stream = record_stream(UNDERLYING, &crossing(&lineage), &lineage);
        stream.header = JournalHeader {
            schema_version: "venue.v2".to_string(),
            lineage_id: lineage,
        };
        pretty(&ScenarioBundle::new(
            RunManifest::new(0, ClockMode::Realtime),
            vec![stream],
        ))
    });
    match load_and_replay(&bytes) {
        Err(ReplayError::SchemaRefused { found }) => assert_eq!(found, "venue.v2"),
        other => panic!("expected a SchemaRefused reject, got {other:?}"),
    }
}

#[test]
fn test_bundle_corpus_tampered_event_halts_with_journal_corruption() {
    // A recorded crossing bundle whose STORED event at sequence 1 (the fill) has been
    // tampered on disk to a divergent outcome — the integrity oracle halts at the
    // exact (underlying, N), never a silent divergent resume.
    let bytes = corpus("bundle/tampered_event.json", || {
        let lineage = LineageId::new("run-1");
        let mut stream = record_stream(UNDERLYING, &crossing(&lineage), &lineage);
        for record in &mut stream.records {
            if let JournalRecord::Event(event) = record
                && event.underlying_sequence == SequenceNumber::new(1)
            {
                *event = VenueEvent::new(
                    event.underlying_sequence,
                    event.venue_ts,
                    event.command.clone(),
                    VenueOutcome::Rejected {
                        reason: "tampered-on-disk".to_string(),
                    },
                );
            }
        }
        pretty(&ScenarioBundle::new(
            RunManifest::new(0, ClockMode::Realtime),
            vec![stream],
        ))
    });
    match load_and_replay(&bytes) {
        Err(ReplayError::JournalCorruption {
            underlying,
            sequence,
        }) => {
            assert_eq!(underlying, UNDERLYING);
            assert_eq!(
                sequence,
                SequenceNumber::new(1),
                "the halt names the exact (underlying, N)"
            );
        }
        other => panic!("expected a JournalCorruption halt at (BTC, 1), got {other:?}"),
    }
}

// ---- size class (generated inputs) -------------------------------------------

#[test]
fn test_bundle_deser_rejects_oversized_total_bytes() {
    // The on-disk bundle path has NO transport cap (unlike the 1 MiB REST body), so
    // an over-ceiling bundle is refused BEFORE it is parsed — no unbounded parse
    // allocation. Generated (a >64 MiB file is not committed).
    let oversized = "x".repeat(MAX_BUNDLE_BYTES + 1);
    match ScenarioBundle::from_json(&oversized) {
        Err(ReplayError::ResourceLimit {
            limit,
            found,
            ceiling,
        }) => {
            assert_eq!(limit, "bundle_bytes");
            assert!(found > ceiling);
            assert_eq!(ceiling, MAX_BUNDLE_BYTES);
        }
        other => panic!("expected a bundle_bytes ResourceLimit, got {other:?}"),
    }
}

#[test]
fn test_bundle_deser_rejects_oversized_record_in_stream() {
    // A bundle under the total ceiling but carrying one record over the per-record
    // ceiling — refused when the stream's journal is built.
    let lineage = LineageId::new("run-1");
    let huge_account = "a".repeat(MAX_JOURNAL_RECORD_BYTES + 16);
    let command = add(&lineage, 0, &huge_account, Side::Sell, 50_000, 1);
    let record = JournalRecord::command(SequenceNumber::new(0), TS, command);
    let stream = JournalStream::new(
        UNDERLYING,
        JournalHeader::new(lineage.clone()),
        vec![record],
    );
    let bundle = ScenarioBundle::new(RunManifest::new(0, ClockMode::Realtime), vec![stream]);
    match replay_bundle(&bundle) {
        Err(ReplayError::ResourceLimit {
            limit,
            found,
            ceiling,
        }) => {
            assert_eq!(limit, "record_bytes");
            assert!(found > ceiling);
            assert_eq!(ceiling, MAX_JOURNAL_RECORD_BYTES);
        }
        other => panic!("expected a record_bytes ResourceLimit, got {other:?}"),
    }
}

// ============================================================================
// 3. No partial apply — a hostile stream discards the whole replay
// ============================================================================

#[test]
fn test_hostile_stream_yields_no_partial_replay_report() {
    // A bundle pairing a CLEAN BTC stream with a TAMPERED ETH stream: replay
    // processes underlyings in sorted order (BTC then ETH), and the ETH halt makes
    // the whole call return Err — so NO `ReplayReport` (hence no reconstructed
    // executions/positions stores) is produced. The clean BTC work is discarded
    // all-or-nothing: a rejected bundle is never partially applied.
    let lineage = LineageId::new("run-1");
    let clean = record_stream("BTC", &crossing(&lineage), &lineage);
    let mut tampered = record_stream("ETH", &crossing(&lineage), &lineage);
    for record in &mut tampered.records {
        if let JournalRecord::Event(event) = record
            && event.underlying_sequence == SequenceNumber::new(1)
        {
            *event = VenueEvent::new(
                event.underlying_sequence,
                event.venue_ts,
                event.command.clone(),
                VenueOutcome::Rejected {
                    reason: "tampered".to_string(),
                },
            );
        }
    }
    match replay_streams(&[clean, tampered]) {
        Err(ReplayError::JournalCorruption {
            underlying,
            sequence,
        }) => {
            assert_eq!(underlying, "ETH");
            assert_eq!(sequence, SequenceNumber::new(1));
            // The Err path returns NO ReplayReport — the clean BTC reconstruction is
            // never surfaced (no stores leaked), the no-partial-apply guarantee.
        }
        Ok(_) => panic!("a tampered stream must not yield a (partial) replay report"),
        other => panic!("expected a JournalCorruption halt, got {other:?}"),
    }
}

// ============================================================================
// 4. Seed-bundle deserialiser — SeedManifest::from_toml_str -> ConfigError
// ============================================================================

#[test]
fn test_seed_corpus_unknown_field_is_typed_reject() {
    let toml = corpus("seed/unknown_field.toml", || {
        "[instruments.BTC]\nopening_price_cents = 5000000\nexpirations = [\"20261231\"]\nstrikes = [50000]\nbogus_field = true\n".to_string()
    });
    match SeedManifest::from_toml_str(&toml) {
        Err(ConfigError::UnknownKey { key }) => assert_eq!(key, "bogus_field"),
        other => panic!("an unknown seed field must be a typed UnknownKey, got {other:?}"),
    }
}

#[test]
fn test_seed_corpus_malformed_toml_is_typed_reject() {
    let toml = corpus("seed/malformed_toml.toml", || {
        "[instruments.BTC\nopening_price_cents = ".to_string()
    });
    match SeedManifest::from_toml_str(&toml) {
        Err(ConfigError::TomlParse { .. }) => {}
        other => panic!("malformed TOML must be a typed TomlParse, got {other:?}"),
    }
}

#[test]
fn test_seed_corpus_days_expiry_is_refused() {
    // A relative Days expiry is wall-clock-relative and breaks replay — refused at
    // load with the dedicated typed error (docs/08, CLAUDE.md invariant).
    let toml = corpus("seed/days_expiry.toml", || {
        "[instruments.BTC]\nopening_price_cents = 5000000\nexpirations = [\"30\"]\nstrikes = [50000]\n".to_string()
    });
    match SeedManifest::from_toml_str(&toml) {
        Err(ConfigError::SeedDaysExpiry { underlying, .. }) => assert_eq!(underlying, "BTC"),
        other => panic!("a Days expiry must be a typed SeedDaysExpiry, got {other:?}"),
    }
}

#[test]
fn test_seed_corpus_zero_opening_price_is_typed_reject() {
    let toml = corpus("seed/zero_opening_price.toml", || {
        "[instruments.BTC]\nopening_price_cents = 0\nexpirations = [\"20261231\"]\nstrikes = [50000]\n".to_string()
    });
    match SeedManifest::from_toml_str(&toml) {
        Err(ConfigError::SeedInvalidInstrument { underlying, .. }) => assert_eq!(underlying, "BTC"),
        other => {
            panic!("a zero opening price must be a typed SeedInvalidInstrument, got {other:?}")
        }
    }
}

#[test]
fn test_seed_corpus_empty_strikes_is_typed_reject() {
    let toml = corpus("seed/empty_strikes.toml", || {
        "[instruments.BTC]\nopening_price_cents = 5000000\nexpirations = [\"20261231\"]\nstrikes = []\n".to_string()
    });
    match SeedManifest::from_toml_str(&toml) {
        Err(ConfigError::SeedInvalidStrikeLadder { underlying, .. }) => {
            assert_eq!(underlying, "BTC")
        }
        other => {
            panic!("an empty strike ladder must be a typed SeedInvalidStrikeLadder, got {other:?}")
        }
    }
}
