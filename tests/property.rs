//! Property tests for the domain boundary newtypes ([TESTING.md §3](../docs/TESTING.md)).
//!
//! - `cents_never_lossy` — the money newtypes survive a `serde` round-trip and
//!   serialise as bare integers (no float drift on any wire).
//! - `symbol_roundtrip` — a canonical symbol parses, then formats to itself.

use fauxchange::exchange::{Cents, Notional, SignedCents, Symbol};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, max_shrink_iters: 50_000, ..ProptestConfig::default() })]

    /// Every money newtype serialises as a bare integer and round-trips through
    /// JSON without loss.
    #[test]
    fn cents_never_lossy(a in any::<u64>(), b in any::<i64>(), n in any::<u128>()) {
        // Cents (u64).
        let cents = Cents::new(a);
        let cents_json = serde_json::to_string(&cents)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&cents_json, &a.to_string());
        let cents_back: Cents = serde_json::from_str(&cents_json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(cents_back, cents);

        // SignedCents (i64).
        let signed = SignedCents::new(b);
        let signed_json = serde_json::to_string(&signed)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&signed_json, &b.to_string());
        let signed_back: SignedCents = serde_json::from_str(&signed_json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(signed_back, signed);

        // Notional (u128).
        let notional = Notional::new(n);
        let notional_json = serde_json::to_string(&notional)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&notional_json, &n.to_string());
        let notional_back: Notional = serde_json::from_str(&notional_json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(notional_back, notional);
    }

    /// A canonical symbol parses, and the stored canonical form equals the
    /// input string (parse-then-format is the identity).
    #[test]
    fn symbol_roundtrip(
        underlying in "[A-Z]{1,6}",
        year in 1970u32..=2099,
        month in 1u32..=12,
        day in 1u32..=28,
        strike in 1u64..=u64::MAX,
        style in "[CP]",
    ) {
        let raw = format!("{underlying}-{year:04}{month:02}{day:02}-{strike}-{style}");
        let symbol = match Symbol::parse(&raw) {
            Ok(s) => s,
            Err(e) => return Err(TestCaseError::fail(format!("parse failed for {raw}: {e:?}"))),
        };
        prop_assert_eq!(symbol.as_str(), raw.as_str());

        // The canonical string also survives a serde round-trip as a bare JSON string.
        let json = serde_json::to_string(&symbol)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&json, &format!("\"{raw}\""));
        let back: Symbol = serde_json::from_str(&json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(back, symbol);
    }
}
