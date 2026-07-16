//! Deterministic workload construction shared by the HP-1 and HP-2 benches — a
//! self-contained xorshift64 generator (no `rand` dependency) so a bench run
//! reproduces the identical command stream given the same seed, mirroring the
//! `orderbook-rs` sibling `bench-hdr` skill's convention.
//!
//! The whole `Vec<VenueCommand>` is built **before** any measured loop starts,
//! so symbol parsing / string allocation for command construction never
//! pollutes a hot-path histogram — only `ActorHandle::submit` /
//! `UnderlyingActor::handle` is timed.

use std::collections::VecDeque;

use fauxchange::exchange::{
    Cents, Hash32, LineageId, STPMode, SequenceNumber, Side, Symbol, TimeInForce, VenueCommand,
};
use fauxchange::{AccountId, ClientOrderId, OrderType, VenueOrderId};

/// The single underlying every HP-1 / HP-2 bench targets — the flagship HP-1
/// budget is explicitly **per-underlying**
/// ([07 §3](../../../docs/07-performance-budgets.md#3-latency-budgets-design-targets)),
/// so one underlying, one single-writer actor, is the right unit to measure.
pub const UNDERLYING: &str = "BTC";

/// The bench-fixed contract symbol under [`UNDERLYING`].
///
/// # Panics
///
/// Panics if this fixed, always-valid literal fails to parse — a broken build,
/// not a runtime condition.
#[must_use]
pub fn contract_symbol() -> Symbol {
    match Symbol::parse("BTC-20240329-50000-C") {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

/// A tiny deterministic xorshift64 generator — reproducible, dependency-free
/// (mirrors the `orderbook-rs` sibling `bench-hdr` skill's own workload PRNG).
pub struct Xorshift64(u64);

impl Xorshift64 {
    /// Seeds the generator; `0` is replaced with a fixed non-zero seed
    /// (xorshift cannot recover from an all-zero state).
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0xA5A5_A5A5_A5A5_A5A5
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// A value in `0..bound` (`0` when `bound == 0`).
    fn next_range(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

/// Builds `n` deterministic commands against [`UNDERLYING`] / [`contract_symbol`]:
/// mostly `AddOrder` in a tight price band around `50_000` (so a healthy
/// fraction cross the resting book and produce real fills, not just resting
/// inserts), plus roughly one `CancelOrder` in ten once the book has resting
/// orders to target. Seeded from `seed`, so a re-run with the same seed
/// reproduces the identical stream byte-for-byte.
///
/// Each `AddOrder`'s venue order id is minted from `lineage` off the command's
/// **index in this stream** (`0..n`), not the actor's eventually-assigned
/// `underlying_sequence` — the id only needs to be unique within the stream so
/// the executor's id-keyed maps never collide; nothing in this bench asserts
/// it equals the real assigned sequence.
#[must_use]
pub fn build_workload(n: usize, seed: u64, lineage: &LineageId) -> Vec<VenueCommand> {
    let mut rng = Xorshift64::new(seed);
    let symbol = contract_symbol();
    let mut recent_ids: VecDeque<VenueOrderId> = VecDeque::with_capacity(64);
    let mut commands = Vec::with_capacity(n);

    for i in 0..n {
        let is_cancel = !recent_ids.is_empty() && rng.next_range(10) == 0;
        if is_cancel {
            let idx = usize::try_from(rng.next_range(recent_ids.len() as u64)).unwrap_or(0);
            let Some(target) = recent_ids.get(idx).cloned() else {
                continue;
            };
            commands.push(VenueCommand::CancelOrder {
                symbol: symbol.clone(),
                order_id: target,
                account: AccountId::new("bench-acct"),
            });
            continue;
        }

        let order_id = lineage.venue_order_id(UNDERLYING, SequenceNumber::new(i as u64), 0);
        let side = if rng.next_range(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        // A tight ±2-cent jitter around 50_000 so buys and sells frequently
        // cross the resting book (real match cost, not a pure insert-only
        // workload).
        let jitter = i64::try_from(rng.next_range(5)).unwrap_or(0) - 2;
        let price = u64::try_from(50_000_i64 + jitter).unwrap_or(50_000);
        let quantity = 1 + rng.next_range(10);

        commands.push(VenueCommand::AddOrder {
            symbol: symbol.clone(),
            order_id: order_id.clone(),
            account: AccountId::new("bench-acct"),
            owner: Hash32([0x42; 32]),
            client_order_id: Some(ClientOrderId::new(format!("bench-{i}"))),
            side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        });

        recent_ids.push_back(order_id);
        if recent_ids.len() > 64 {
            recent_ids.pop_front();
        }
    }

    commands
}
