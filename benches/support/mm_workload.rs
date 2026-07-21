//! Shared HP-4 market-maker requote fixture
//! ([050](../../milestones/v0.5-microstructure/050-requote-budget-isolation.md)):
//! a small, realistic option chain and the persona-driven [`MarketMakerEngine`]
//! (#47) both `benches/mm_requote_hdr.rs` and `benches/alloc_profile.rs`
//! register it onto, so the quantile bench and the allocation profile measure
//! the IDENTICAL requote shape rather than two independently-constructed (and
//! possibly drifting) fixtures — the same reuse discipline
//! `benches/support/fix_fixtures.rs` gives HP-3.
//!
//! Also carries [`CountingSink`], a [`CommandSink`] that does nothing but
//! count — the "no channel, no actor" half of the HP-4 bench's two-section
//! shape (mirrors `alloc_profile.rs`'s own "direct vs round-trip" split), so
//! the PURE requote-compute cost (price update → `requote_symbol` → edge calc
//! → `update_quote`) is measured in isolation from any bounded-channel /
//! actor-mailbox cost.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use fauxchange::exchange::{LineageId, Symbol};
use fauxchange::market_maker::{CommandSink, MarketMakerEngine, PersonaConfig, Quoter};

/// The underlying every HP-4 fixture targets.
pub const MM_UNDERLYING: &str = "BTC";

/// 2025-01-01T00:00:00Z in ms — the venue-clock instant [`chain_symbols`]'s
/// far-future (`2035-12-31`) expiries are quoted against, so `days_to_expiry`
/// stays a healthy, stable positive number across an entire bench run (mirrors
/// `src/market_maker/engine.rs`'s own test fixture).
pub const MM_VENUE_NOW_MS: u64 = 1_735_689_600_000;

/// A realistic small option chain: 5 strikes × {call, put}.
const STRIKES: [u64; 5] = [45_000, 47_500, 50_000, 52_500, 55_000];

/// The number of instruments [`chain_symbols`] registers (`STRIKES.len() * 2`).
#[must_use]
pub fn chain_len() -> usize {
    STRIKES.len() * 2
}

/// Builds the fixed 10-contract chain.
///
/// # Panics
///
/// Panics if a fixed, always-valid fixture symbol fails to parse — a broken
/// build, not a runtime condition.
#[must_use]
pub fn chain_symbols() -> Vec<Symbol> {
    let mut symbols = Vec::with_capacity(chain_len());
    for strike in STRIKES {
        for style in ['C', 'P'] {
            let raw = format!("{MM_UNDERLYING}-20351231-{strike}-{style}");
            match Symbol::parse(&raw) {
                Ok(symbol) => symbols.push(symbol),
                Err(e) => panic!("HP-4 fixture symbol {raw} failed to parse: {e:?}"),
            }
        }
    }
    symbols
}

/// The shared bench persona (#47's persona-driven `update_quote` branch) —
/// finite, in-range knobs guaranteed by `try_new`.
///
/// # Panics
///
/// Panics if this fixed, always-in-range persona is rejected — a broken
/// build, not a runtime condition.
#[must_use]
pub fn bench_persona() -> PersonaConfig {
    match PersonaConfig::try_new(120, 5, 1.0, 1.0, 0.0) {
        Ok(persona) => persona,
        Err(e) => panic!("HP-4 fixture persona rejected (should be in-range): {e}"),
    }
}

/// Builds a [`MarketMakerEngine`] over `sink`, registering [`chain_symbols`]
/// with [`bench_persona`] (the persona-driven `update_quote` branch, #47) and
/// the fixed venue clock ([`MM_VENUE_NOW_MS`]).
#[must_use]
pub fn build_engine(sink: Arc<dyn CommandSink>, lineage_token: &str) -> MarketMakerEngine {
    let engine = MarketMakerEngine::new(sink, LineageId::new(lineage_token), Quoter::default())
        .with_run_seed(0xA5A5_A5A5_A5A5_A5A5);
    engine.set_venue_now_ms(MM_VENUE_NOW_MS);
    let persona = bench_persona();
    for symbol in chain_symbols() {
        engine.register_instrument_with_persona(&symbol, None, "bench", persona);
    }
    engine
}

/// A [`CommandSink`] that does nothing but count — isolates the PURE
/// requote-compute cost from the enqueue-onto-a-real-mailbox cost a
/// production [`fauxchange::market_maker::ActorCommandSink`] adds.
#[derive(Debug, Default)]
pub struct CountingSink {
    /// The number of `enqueue` calls observed so far.
    pub enqueued: AtomicU64,
}

impl CommandSink for CountingSink {
    fn enqueue(&self, _command: fauxchange::exchange::VenueCommand) {
        self.enqueued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
