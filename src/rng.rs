//! Shared deterministic seeded-stream primitives — a `SplitMix64` generator over an
//! FNV-1a-folded key — the one substrate every venue-owned seeded sub-stream draws
//! from (latency injection #045, market-maker persona jitter #047).
//!
//! ## Why a hand-rolled primitive (not a `rand` dependency)
//!
//! The draws run on hot paths (per inbound message, per requote leg) and must stay
//! cheap, portable, and — above all — **replay-reproducible**: the venue forbids
//! unseeded RNG on any replayable path. A tiny, self-contained `SplitMix64` is a
//! fixed function of its seed with no process-randomised state (unlike `std`'s
//! `RandomState`), so two runs with the same seed draw identically and a seeded run
//! replays exactly ([04 §6](../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
//!
//! ## Independent sub-streams via a domain tag
//!
//! Every sub-stream folds its **own** domain tag into the seed
//! ([`SplitMix64::keyed`]) before mixing in the stream key, so two sub-streams from
//! the same run seed but different domains (latency vs persona jitter) are
//! statistically independent and never correlate. Callers own their domain tag
//! constant; this module owns the mixing.
//!
//! ## The `f64` boundary
//!
//! The unit-interval and normal draws are finite by construction: [`next_open_unit`]
//! is never `0`, so `ln` in [`standard_normal`] stays finite. A caller converting a
//! draw into integer cents / microseconds still guards its own `f64 → integer`
//! boundary (this module only guarantees finite `f64` draws).
//!
//! ## Modular arithmetic — the one audited `wrapping_*` exception
//!
//! `rules/global_rules.md` forbids `wrapping_*` because it silently hides overflow.
//! FNV-1a and `SplitMix64` are the documented exception: both are **defined over
//! `mod 2^64` arithmetic** — the `wrapping_mul` in [`fnv1a_64`] / [`mix64`] and the
//! `wrapping_add` in [`SplitMix64::next_u64`] are the algorithms' *specified*
//! behaviour, not an accidental overflow. Using `checked_*` here would either reject
//! the normal, correct wrap (breaking the hash/PRNG) or force a meaningless error on
//! a value that is pure PRNG/hash state — **never money (`Cents`), a sequence number,
//! a timestamp, or a leg/row count**, the values the arithmetic rule actually guards.
//! Each site is annotated inline; this is the single place the crate wraps on purpose.
//!
//! [`next_open_unit`]: SplitMix64::next_open_unit
//! [`fnv1a_64`]: fnv1a_64
//! [`mix64`]: mix64
//! [`SplitMix64::next_u64`]: SplitMix64::next_u64

/// The FNV-1a 64-bit offset basis — folds a byte string into a stream key with a
/// fixed, portable hash (never `std::hash::RandomState`).
pub(crate) const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// The FNV-1a 64-bit prime.
pub(crate) const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

/// The `SplitMix64` increment (the golden-ratio odd constant `⌊2^64 / φ⌋`).
pub(crate) const SPLITMIX_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// `2^53` — the divisor mapping a 53-bit mantissa slice into a unit-interval `f64`
/// without precision loss.
pub(crate) const F64_MANTISSA_SCALE: f64 = (1u64 << 53) as f64;

/// A deterministic FNV-1a 64-bit hash of `bytes` — folds a stream key string (a
/// session id, a persona name, a symbol) into the seed. A fixed, portable function.
#[inline]
#[must_use]
pub(crate) fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u64::from(byte);
        // Audited `wrapping_*` exception (see module docs): FNV-1a is defined over
        // `mod 2^64`; the wrap is the spec, and `hash` is hash state, not money/seq.
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The `SplitMix64` finaliser — the avalanche mix turning a counter value into a
/// well-distributed `u64`.
#[inline]
#[must_use]
pub(crate) fn mix64(z: u64) -> u64 {
    // Audited `wrapping_*` exception (see module docs): the SplitMix64 finaliser is
    // defined over `mod 2^64`; the wrap is the spec, and `z` is PRNG state.
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A tiny, self-contained `SplitMix64` generator — the seeded sub-stream every
/// venue-owned stochastic component draws from.
///
/// Deliberately **not** a general RNG dependency (see the module docs). Seed it from
/// a fully-mixed `u64` state with [`SplitMix64::from_state`], or fold a domain tag +
/// keys with [`SplitMix64::keyed`].
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Builds a generator from an already-avalanche-mixed `state`.
    #[inline]
    #[must_use]
    pub(crate) fn from_state(state: u64) -> Self {
        Self { state }
    }

    /// Derives an independent stream from a run seed, a `domain` tag, and any number
    /// of string `keys`, folding each through the avalanche mix in order.
    ///
    /// The `domain` tag separates this sub-stream from every other one drawn from the
    /// same `run_seed` (so latency and persona jitter never correlate); the `keys`
    /// are the stream identity (e.g. `[persona, symbol]`), folded through
    /// [`fnv1a_64`] so distinct key tuples get uncorrelated streams. Fixed, portable,
    /// and order-sensitive — the same `(run_seed, domain, keys)` always yields the
    /// same stream.
    #[inline]
    #[must_use]
    pub(crate) fn keyed(run_seed: u64, domain: u64, keys: &[&str]) -> Self {
        let mut acc = mix64(run_seed ^ domain);
        for key in keys {
            acc = mix64(acc ^ fnv1a_64(key.as_bytes()));
        }
        Self::from_state(acc)
    }

    /// The next 64-bit output.
    #[inline]
    pub(crate) fn next_u64(&mut self) -> u64 {
        // Audited `wrapping_*` exception (see module docs): SplitMix64's state advance
        // is defined over `mod 2^64`; the wrap is the spec, `state` is PRNG state.
        self.state = self.state.wrapping_add(SPLITMIX_GAMMA);
        mix64(self.state)
    }

    /// A `f64` in `[0, 1)` — the top 53 bits of one output scaled by `2^53`.
    #[inline]
    pub(crate) fn next_unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64) / F64_MANTISSA_SCALE
    }

    /// A `f64` in `(0, 1)` — never `0` (so `ln` stays finite) and never `1`.
    #[inline]
    pub(crate) fn next_open_unit(&mut self) -> f64 {
        (((self.next_u64() >> 11) as f64) + 1.0) / (F64_MANTISSA_SCALE + 1.0)
    }

    /// A standard-normal sample via the Box–Muller transform — finite by
    /// construction (`u1 ∈ (0, 1)` keeps `ln(u1)` finite and negative).
    #[inline]
    pub(crate) fn standard_normal(&mut self) -> f64 {
        let u1 = self.next_open_unit();
        let u2 = self.next_unit();
        let radius = (-2.0_f64 * u1.ln()).sqrt();
        radius * (std::f64::consts::TAU * u2).cos()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyed_is_reproducible_for_the_same_inputs() {
        let a = SplitMix64::keyed(42, 0xABCD, &["tight", "BTC-20240329-50000-C"]);
        let b = SplitMix64::keyed(42, 0xABCD, &["tight", "BTC-20240329-50000-C"]);
        let mut a = a;
        let mut b = b;
        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn test_distinct_domains_diverge_for_the_same_seed_and_keys() {
        let mut a = SplitMix64::keyed(42, 0x1111, &["p", "s"]);
        let mut b = SplitMix64::keyed(42, 0x2222, &["p", "s"]);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn test_distinct_keys_diverge_for_the_same_seed_and_domain() {
        let mut a = SplitMix64::keyed(42, 0x1111, &["tight", "BTC-A"]);
        let mut b = SplitMix64::keyed(42, 0x1111, &["tight", "BTC-B"]);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn test_distinct_seeds_diverge() {
        let mut a = SplitMix64::keyed(1, 0x1111, &["p", "s"]);
        let mut b = SplitMix64::keyed(2, 0x1111, &["p", "s"]);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn test_unit_draws_are_in_range_and_finite() {
        let mut rng = SplitMix64::keyed(7, 0x33, &["k"]);
        for _ in 0..10_000 {
            let u = rng.next_unit();
            assert!((0.0..1.0).contains(&u), "next_unit out of [0,1): {u}");
            let o = rng.next_open_unit();
            assert!(o > 0.0 && o < 1.0, "next_open_unit out of (0,1): {o}");
            let n = rng.standard_normal();
            assert!(n.is_finite(), "standard_normal must be finite: {n}");
        }
    }
}
