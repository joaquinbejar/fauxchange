//! Shared boundary: canonical domain types and REST/WS DTOs (`serde` +
//! `utoipa::ToSchema`). Prices are integer cents; this module carries no
//! business logic. Re-exported at the crate root.
//!
//! Governed by `docs/01-domain-model.md`.
//!
//! Only [`Permission`] lands in this issue (#003) — the venue permission enum
//! the boundary error [`crate::error::VenueError::Forbidden`] carries. The full
//! DTO surface (orders, fills, positions, execution reports, and the
//! `utoipa::ToSchema` projection) lands with the DTO layer (#004);
//! [`Permission`] is placed here now because [01 §8](../docs/01-domain-model.md)
//! is the canonical home for it and the error boundary must name it.

use serde::{Deserialize, Serialize};

/// A venue permission carried by the authenticated session across every
/// protocol surface — REST/WS via the JWT `Claims`, FIX via the logon
/// credentials — under **one** permission model
/// ([01 §8](../docs/01-domain-model.md), [03 §6](../docs/03-protocol-surfaces.md)).
///
/// `Admin` **implies** `Read` and `Trade`; that implication is enforced by the
/// auth layer (#011), not encoded structurally here. The wire casing is
/// **lowercase** (`"read"` / `"trade"` / `"admin"`), inherited verbatim from
/// `option-chain-orderbook-backend` v0.4.0 and pinned as a wire contract
/// ([01 §10](../docs/01-domain-model.md)); changing it is a breaking wire
/// change that must move the DTO examples, OpenAPI, and golden tests together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    /// Read-only access: query the hierarchy, prices, market data, and
    /// public prints. The minimum any authenticated session holds.
    Read,
    /// Order entry: place, cancel, and replace orders (REST + FIX). Implies the
    /// ability to observe the resulting fills.
    Trade,
    /// Administrative access: venue controls, snapshots, and every lower
    /// permission. `Admin` implies `Read` and `Trade`.
    Admin,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_serializes_lowercase() {
        for (permission, expected) in [
            (Permission::Read, "\"read\""),
            (Permission::Trade, "\"trade\""),
            (Permission::Admin, "\"admin\""),
        ] {
            match serde_json::to_string(&permission) {
                Ok(json) => assert_eq!(json, expected),
                Err(e) => panic!("serialize failed for {permission:?}: {e}"),
            }
        }
    }

    #[test]
    fn test_permission_deserializes_from_lowercase() {
        match serde_json::from_str::<Permission>("\"trade\"") {
            Ok(permission) => assert_eq!(permission, Permission::Trade),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }
}
