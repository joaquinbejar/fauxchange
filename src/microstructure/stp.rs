//! `[microstructure.stp]` — self-trade prevention, surfacing the upstream
//! `STPMode` as a venue token set.
//!
//! STP is **upstream**: `orderbook-rs` applies the mode at the leaf, keyed on the
//! account `owner` hash (`Hash32`), so a session's account identity determines
//! whether two of its own orders can cross
//! ([05 §6](../../../docs/05-microstructure-config.md#6-self-trade-prevention),
//! [01 §8](../../../docs/01-domain-model.md)). The venue exposes it as config with
//! a friendly snake_case token set (`off` / `cancel_taker` / `cancel_maker` /
//! `cancel_both`) because the upstream `STPMode` serialises as its variant names
//! (`None` / `CancelTaker` / …); [`StpMode::to_stp_mode`] is the single mapping.

use option_chain_orderbook::STPMode;
use serde::{Deserialize, Serialize};

/// The venue self-trade-prevention token set — a snake_case surface over the
/// upstream `STPMode`.
///
/// | token           | upstream `STPMode` | effect when an account crosses itself |
/// |-----------------|--------------------|----------------------------------------|
/// | `off`           | `None`             | self-trades allowed                    |
/// | `cancel_taker`  | `CancelTaker`      | incoming aggressor cancelled           |
/// | `cancel_maker`  | `CancelMaker`      | resting order cancelled                |
/// | `cancel_both`   | `CancelBoth`       | both cancelled                         |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StpMode {
    /// Self-trades are allowed (the default — zero STP overhead).
    #[default]
    Off,
    /// Cancel the incoming (taker) order on a self-trade.
    CancelTaker,
    /// Cancel the resting (maker) order on a self-trade and continue matching.
    CancelMaker,
    /// Cancel both the taker and the maker on a self-trade.
    CancelBoth,
}

impl StpMode {
    /// Maps the venue token to the upstream `STPMode` applied at the leaf.
    #[must_use]
    #[inline]
    pub fn to_stp_mode(self) -> STPMode {
        match self {
            StpMode::Off => STPMode::None,
            StpMode::CancelTaker => STPMode::CancelTaker,
            StpMode::CancelMaker => STPMode::CancelMaker,
            StpMode::CancelBoth => STPMode::CancelBoth,
        }
    }

    /// The stable snake_case token for this mode — the canonical spelling used in
    /// the config surface and the microstructure fingerprint
    /// ([`MicrostructureConfig::fingerprint`](crate::microstructure::MicrostructureConfig::fingerprint)).
    #[must_use]
    #[inline]
    pub const fn token(self) -> &'static str {
        match self {
            StpMode::Off => "off",
            StpMode::CancelTaker => "cancel_taker",
            StpMode::CancelMaker => "cancel_maker",
            StpMode::CancelBoth => "cancel_both",
        }
    }
}

/// `[microstructure.stp]` — the venue self-trade-prevention mode.
///
/// The default is `off` (self-trades allowed), matching the upstream
/// `STPMode::None` default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StpConfig {
    /// The self-trade-prevention mode (default `off`).
    #[serde(default)]
    pub mode: StpMode,
}

impl StpConfig {
    /// The upstream `STPMode` this config resolves to.
    #[must_use]
    #[inline]
    pub fn to_stp_mode(self) -> STPMode {
        self.mode.to_stp_mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stp_config_default_is_off() {
        assert_eq!(StpConfig::default().mode, StpMode::Off);
        assert_eq!(StpConfig::default().to_stp_mode(), STPMode::None);
    }

    #[test]
    fn test_stp_mode_maps_every_variant_to_upstream() {
        assert_eq!(StpMode::Off.to_stp_mode(), STPMode::None);
        assert_eq!(StpMode::CancelTaker.to_stp_mode(), STPMode::CancelTaker);
        assert_eq!(StpMode::CancelMaker.to_stp_mode(), STPMode::CancelMaker);
        assert_eq!(StpMode::CancelBoth.to_stp_mode(), STPMode::CancelBoth);
    }

    #[test]
    fn test_stp_config_deserialises_snake_case_tokens() {
        for (token, expected) in [
            ("off", STPMode::None),
            ("cancel_taker", STPMode::CancelTaker),
            ("cancel_maker", STPMode::CancelMaker),
            ("cancel_both", STPMode::CancelBoth),
        ] {
            let config: StpConfig = toml::from_str(&format!("mode = \"{token}\"\n"))
                .unwrap_or_else(|error| panic!("token '{token}' must parse: {error}"));
            assert_eq!(config.to_stp_mode(), expected);
        }
    }

    #[test]
    fn test_stp_config_rejects_unknown_token() {
        let error = toml::from_str::<StpConfig>("mode = \"cancel_none\"\n");
        assert!(error.is_err(), "an unknown STP token must be rejected");
    }

    #[test]
    fn test_stp_config_rejects_unknown_field() {
        let error = toml::from_str::<StpConfig>("modus = \"off\"\n");
        assert!(error.is_err(), "an unknown field must be rejected");
    }
}
