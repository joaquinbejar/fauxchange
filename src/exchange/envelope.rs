//! The versioned `VenueCommand` / `VenueEvent` v1 envelope and its lossless
//! outcome shapes — the venue's own internal instruction set and durable record
//! ([ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
//! [ADR-0009](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md),
//! [02 §4](../../../docs/02-matching-architecture.md)).
//!
//! The upstream `OptionChainCommand::AddOrder` carries **no** account / owner /
//! TIF / order-type / STP identity, and its `OrderAdded { order_id }` discards
//! the `MatchResult` (its fills) — both verified in source
//! ([ADR-0006](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//! `fauxchange` therefore journals its **own**, explicitly versioned envelope
//! that carries that identity **in** ([`VenueCommand`]) and the captured fills
//! **out** ([`VenueEvent`] / [`VenueOutcome`]), while invoking the upstream
//! matching engine **unchanged**. These are pure data types; the single-writer
//! actor that assigns the sequence, drives matching, and writes the journal is
//! #006/#007.
//!
//! ## Seam vs DTO
//!
//! The envelope is the **matching seam** ([01 §4](../../../docs/01-domain-model.md)),
//! so it names the **upstream** newtypes (`Side`, `TimeInForce`, `STPMode`,
//! `Hash32`, `InstrumentStatus`) alongside the venue-owned money / clock / symbol
//! newtypes and the venue-assigned identity strings (`VenueOrderId` /
//! `ExecutionId` / `AccountId`, reused from #004). It is a **different
//! projection** from the `src/models.rs` DTO layer: the DTO
//! [`Fill`](crate::Fill) is the account-scoped wire projection, whereas the
//! [`Fill`] here is the **lossless internal projection** the journal needs — it
//! additionally carries the STP `owner: Hash32` per leg
//! ([01 §7](../../../docs/01-domain-model.md), [ADR-0009 §2](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
//! Because `crate::exchange` is not glob-re-exported at the crate root, this
//! [`Fill`] (`fauxchange::exchange::Fill`) and the DTO `fauxchange::Fill` never
//! collide.
//!
//! ## Wire contract
//!
//! Both envelopes carry the mandatory `schema` tag
//! ([`VENUE_ENVELOPE_SCHEMA`] = `"venue.v1"`). Following the upstream journal
//! convention, every envelope enum
//! pins variant tags to **`PascalCase`**, struct-variant fields to
//! **`snake_case`**, and **rejects unknown fields**, so a renamed/dropped field
//! is a hard decode error rather than a silent replay corruption
//! ([01 §10](../../../docs/01-domain-model.md)). A schema bump is a major SemVer
//! event and moves the golden with it.

use serde::{Deserialize, Serialize};

use crate::exchange::boundary::{
    ExpirationDate, Hash32, InstrumentStatus, STPMode, Side, TimeInForce,
};
use crate::exchange::event::{EventTimestamp, SequenceNumber};
use crate::exchange::identity::VENUE_ENVELOPE_SCHEMA;
use crate::exchange::money::{Cents, SignedCents};
use crate::exchange::symbol::Symbol;
use crate::models::{
    AccountId, ClientOrderId, ExecutionId, LiquidityFlag, OrderType, VenueOrderId,
};

// ============================================================================
// Mass-cancel scope / type (venue envelope mirror of the upstream enums)
// ============================================================================

/// The hierarchy scope of a [`VenueCommand::MassCancel`] — the venue envelope's
/// owned mirror of the upstream `option_chain_orderbook::MassCancelScope`.
///
/// The upstream enum is gated behind the `option-chain-orderbook` **`sequencer`**
/// feature, which pulls the on-disk journal machinery (`memmap2`) that #005 is
/// scoped to exclude ("pure types, no actor, no store"). The venue therefore owns
/// this envelope-side representation and the single-writer actor (#006, which
/// enables the `sequencer` feature) maps it **1:1** onto the upstream
/// `MassCancelScope` at the `submit_mass_cancel` seam. Its wire form matches the
/// upstream journal convention (`PascalCase` variants, `snake_case` fields,
/// unknown fields rejected). Expiries are `ExpirationDate::DateTime` only
/// ([01 §4](../../../docs/01-domain-model.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "PascalCase",
    rename_all_fields = "snake_case"
)]
pub enum MassCancelScope {
    /// Cancel across the entire underlying (all expirations, all strikes).
    Underlying,
    /// Cancel within a specific expiration.
    Expiration(ExpirationDate),
    /// Cancel within a specific strike of an expiration.
    Strike {
        /// The target expiration instant.
        expiration: ExpirationDate,
        /// The strike in **whole units**.
        strike: u64,
    },
    /// Cancel within a specific option book (call or put), named by its symbol.
    Book(Symbol),
}

/// The filter of a [`VenueCommand::MassCancel`] — the venue envelope's owned
/// mirror of the upstream `option_chain_orderbook::MassCancelType`.
///
/// Owned venue-side for the same reason as [`MassCancelScope`] (the upstream type
/// is behind the `sequencer` feature), and mapped 1:1 onto the upstream
/// `MassCancelType` by the #006 actor. `ByUser` scopes on the upstream STP owner
/// [`Hash32`] ([01 §8](../../../docs/01-domain-model.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "PascalCase",
    rename_all_fields = "snake_case"
)]
pub enum MassCancelType {
    /// Cancel every order in scope.
    All,
    /// Cancel only orders on a specific side.
    BySide(Side),
    /// Cancel only orders owned by a specific user (STP owner hash).
    ByUser(Hash32),
}

// ============================================================================
// Cancellation reason (venue-owned outcome vocabulary)
// ============================================================================

/// Why an order was removed from the book, recorded per affected id so replay
/// reproduces exactly which order each mutation cancelled and why
/// ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
///
/// This is venue-owned **outcome vocabulary**, not upstream matching logic — the
/// engine still performs the cancellation; the envelope records its cause. Wire
/// form is the `PascalCase` variant name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CancelReason {
    /// Swept by a [`VenueCommand::MassCancel`] whose scope/type matched — this
    /// includes the scoped contract-expiry sweep
    /// ([01 §5](../../../docs/01-domain-model.md)).
    MassCancel,
    /// Removed by self-trade prevention (a resting maker leg an incoming
    /// aggressor from the same owner would have crossed,
    /// [05 §6](../../../docs/05-microstructure-config.md)).
    SelfTradePrevention,
    /// Evicted because its time-in-force expired (a `Day` / `Gtd` sweep via
    /// [`VenueCommand::EvictExpiredOrders`]).
    TimeInForceExpiry,
    /// Removed by the cancel leg of a non-atomic [`VenueCommand::Replace`].
    Replace,
}

// ============================================================================
// Fill — one account-attributed leg of a match (the lossless internal form)
// ============================================================================

/// One **account-attributed leg** of a match — the venue's **lossless internal**
/// fill projection ([01 §7](../../../docs/01-domain-model.md),
/// [ADR-0009 §2](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
///
/// A single match produces **two linked legs** — a maker leg and a taker leg
/// **sharing one `execution_id`** — each carrying its own `account`, `owner`,
/// `side`, `liquidity` flag, `quantity`, `price`, and **its own `fee`** (a maker
/// rebate is negative). The counterparty leg is recovered by joining on
/// `execution_id`; the maker leg's identity is recovered from the journaled
/// [`VenueCommand`] that added the resting order (which carries `account` /
/// `owner`), not from live book state.
///
/// This is a **distinct projection** from the DTO [`Fill`](crate::Fill): it names
/// the upstream [`Side`] at the matching seam and adds the STP `owner: Hash32`
/// each leg needs, which the account-scoped wire `Fill` omits. The DTO
/// `ExecutionRecord` / anonymised WS `fill` are deterministic projections of
/// these legs. Its wire form pins `snake_case` fields and rejects unknown ones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fill {
    /// The composite execution id ([01 §6.1](../../../docs/01-domain-model.md)),
    /// **shared** by the two legs of this match.
    pub execution_id: ExecutionId,
    /// The venue order id of **this** leg.
    pub order_id: VenueOrderId,
    /// The owning account of **this** leg — for the per-account projection.
    pub account: AccountId,
    /// The STP owner hash of **this** leg, recovered from the journaled add
    /// command; the join key for by-user mass cancel and self-trade prevention.
    pub owner: Hash32,
    /// This leg's side (the upstream matching-seam [`Side`]).
    pub side: Side,
    /// This leg's role (maker or taker).
    pub liquidity: LiquidityFlag,
    /// Execution price in **cents**.
    pub price: Cents,
    /// Executed quantity in **contracts**.
    pub quantity: u64,
    /// This leg's fee in **cents** — a maker rebate is negative.
    pub fee: SignedCents,
}

// ============================================================================
// Cancellation record — one affected order in a mass cancel
// ============================================================================

/// One order removed by a mass cancel, with its owner and the reason — the
/// ordered element of [`VenueOutcome::MassCancelled`]
/// ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
///
/// Recorded as a named struct (an object on the wire) rather than a positional
/// tuple so the journal stays field-named and `deny_unknown_fields`-strict. It
/// carries the resting order's own `symbol` + `side` alongside the ADR-0009
/// `(order_id, owner, reason)` triple: a gateway rendering a per-order
/// cancellation report (the FIX `ExecutionReport (8) Canceled`) needs the
/// instrument + side, and reverse-resolving them from a per-session correlation
/// map would silently drop any order the current session did not place (a REST
/// placement by the same account, or a prior FIX session). The executor
/// populates both losslessly from the resting-order registry, so they are
/// journaled in the outcome and a replay reproduces them (#97).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelledLeg {
    /// The venue order id that was cancelled.
    pub order_id: VenueOrderId,
    /// The STP owner hash of the cancelled order.
    pub owner: Hash32,
    /// The cancelled order's contract symbol — the instrument a per-order
    /// cancellation report renders against.
    pub symbol: Symbol,
    /// The cancelled order's side (the upstream matching-seam [`Side`]).
    pub side: Side,
    /// Why it was cancelled.
    pub reason: CancelReason,
}

// ============================================================================
// Reject discriminant (venue-owned typed reject vocabulary)
// ============================================================================

/// The **typed** discriminant of a captured [`VenueOutcome::Rejected`] /
/// [`AddOutcome::Rejected`], so a gateway keys its client rendering on a **type**,
/// not a fragile string-match of the human `reason` (#132). The human `reason`
/// stays alongside for the journal + `tracing`; the wire mapping never parses it.
///
/// **Security (BOLA/IDOR mask, #132/#118).** `VenueOrderId`s are minted
/// deterministically/sequentially, so a distinct not-owner vs not-found reply would
/// let an authenticated caller enumerate which ids hold a live resting order owned
/// by *another* account. The gateway therefore collapses [`RejectKind::NotOwner`],
/// [`RejectKind::NotFound`], and [`RejectKind::NotResting`] to ONE indistinguishable
/// client reject on the cancel/replace path (see
/// [`VenueError::masked_cancel_reject`](crate::VenueError::masked_cancel_reject)),
/// while the true kind — especially `NotOwner` — is journaled and traced as a
/// detective control for repeated cross-account attempts. Because the mapping keys
/// on this enum, refactoring the human `reason` string can never silently break the
/// mask.
///
/// Wire form is the `PascalCase` variant name (this enum is carried in the durable
/// journal, so it round-trips symmetrically like [`CancelReason`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum RejectKind {
    /// The referenced order (or its leaf) does not exist on the venue.
    NotFound,
    /// The referenced order exists but is owned by a **different** account — an
    /// authorization failure enforced on the shared sequenced path **before** any
    /// book mutation. Masked as [`RejectKind::NotFound`] at the client boundary so
    /// it is not a cross-account existence/ownership oracle; the true kind stays
    /// internal (journal + `tracing`).
    NotOwner,
    /// The referenced order is owned by the caller but is no longer resting
    /// (already filled / cancelled / gone). Masked as [`RejectKind::NotFound`] at
    /// the client boundary.
    NotResting,
    /// An order targeted an instrument that is not `Active` (halted / settling /
    /// expired) and so is not accepting orders — the sequenced instrument-status
    /// gate.
    InstrumentNotActive,
    /// A business-validation failure minted on the sequenced path (a reused
    /// `client_order_id`, a missing limit price, an unresolvable / cross-underlying
    /// symbol, an illegal instrument-status transition, an unresolvable mass-cancel
    /// scope).
    InvalidOrder,
    /// A marketable order that could neither fill nor rest — a killed `IOC` / `FOK`,
    /// or the add leg of a replace that did not become marketable.
    NotFillable,
    /// A failure surfaced from the upstream matching stack as a reject reason; its
    /// cause is redacted at the client boundary. Also the [`Default`] — the kind a
    /// **pre-#132 journal record** (which carried only a `reason`, no `kind`) decodes
    /// to, so an older durable journal still replays (`#[serde(default)]` on the
    /// `kind` field); the kind is cosmetic on replay (masking renders no client
    /// output), and `Internal` is the most-redacted, safest neutral for an unknown
    /// legacy reject.
    #[default]
    Internal,
}

// ============================================================================
// Add outcome — the add leg of a (possibly non-atomic) placement
// ============================================================================

/// The outcome of the **add** leg of a placement — used standalone for a plain
/// add and as the `add` half of a non-atomic [`VenueOutcome::Replace`]
/// ([ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// Every branch is lossless: `Filled` carries the fills of a fully-consumed add,
/// `Rested` carries any partial fills **and** the resting remainder, and
/// `Rejected` carries the reason with **no book mutation**. `Filled` / `Rested`
/// additionally carry any `stp_cancelled` resting legs the incoming aggressor
/// removed via self-trade prevention in the same turn
/// ([ADR-0009 §2, §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md));
/// `Rejected` has none because an STP removal *is* a book mutation, so the
/// outcome is never a bare `Rejected` (which is reserved for a genuine no-op).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "PascalCase",
    rename_all_fields = "snake_case"
)]
pub enum AddOutcome {
    /// The add crossed and filled in full; no remainder rests.
    Filled {
        /// The captured fill legs (two per match).
        fills: Vec<Fill>,
        /// The resting legs this add removed via self-trade prevention
        /// (`cancel_maker` / `cancel_both`), in the deterministic sweep order —
        /// empty when no STP fired ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
        stp_cancelled: Vec<CancelledLeg>,
    },
    /// The add rests in the book, possibly after partial fills.
    Rested {
        /// Any fill legs captured before the remainder rested (empty if none).
        fills: Vec<Fill>,
        /// The quantity left resting in the book, in **contracts**.
        resting_quantity: u64,
        /// The resting legs this add removed via self-trade prevention
        /// (`cancel_maker` / `cancel_both`), in the deterministic sweep order —
        /// empty when no STP fired.
        stp_cancelled: Vec<CancelledLeg>,
    },
    /// The add was rejected; nothing rests, nothing filled, and — because an STP
    /// removal is itself a book mutation — no resting leg was removed either.
    Rejected {
        /// The **typed** reject discriminant the gateway keys its client rendering
        /// on (#132).
        #[serde(default)] // a pre-#132 record has no `kind` → RejectKind::Internal
        kind: RejectKind,
        /// The human reason, for the journal + `tracing` — never string-matched.
        reason: String,
    },
}

impl AddOutcome {
    /// Constructs a rejected add leg with a typed [`RejectKind`] and the human
    /// `reason` (journal + `tracing`). The gateway keys on `kind`, not the string.
    #[must_use]
    pub fn rejected(kind: RejectKind, reason: impl Into<String>) -> Self {
        AddOutcome::Rejected {
            kind,
            reason: reason.into(),
        }
    }
}

// ============================================================================
// VenueOutcome — the lossless captured result of a command
// ============================================================================

/// The lossless captured outcome of a [`VenueCommand`], carried by
/// [`VenueEvent`] ([ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
/// [ADR-0009](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
///
/// Every branch is representable without loss so both accounts' positions, fees,
/// and P&L fold deterministically from the journal alone: bilateral fill legs
/// with per-leg fees, the resting legs an incoming aggressor removed via
/// self-trade prevention, the empty-book zero-fill market order, the ordered
/// affected-id list of a mass cancel, and the explicit partial state of a
/// non-atomic replace.
///
/// **Empty-vec convention.** Every `Vec` field is **always serialised** (an empty
/// array when nothing applies) and required on decode — matching `fills` — so a
/// journal record round-trips symmetrically and a dropped field is a hard decode
/// error, never a silent replay corruption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "PascalCase",
    rename_all_fields = "snake_case"
)]
pub enum VenueOutcome {
    /// A limit [`VenueCommand::AddOrder`]: the captured fill legs plus the
    /// resting remainder (`resting_quantity == 0` for a fully-filled add, and
    /// `fills` empty for an add that rested untouched), along with any resting
    /// legs the add removed via self-trade prevention in the same turn.
    Added {
        /// The captured fill legs (two per match).
        fills: Vec<Fill>,
        /// The quantity left resting in the book, in **contracts**.
        resting_quantity: u64,
        /// The resting legs this add removed via self-trade prevention
        /// (`cancel_maker` / `cancel_both`), in the deterministic sweep order —
        /// empty when no STP fired. Because an STP removal happens inside the one
        /// add turn (one sequence, one event, no separate cancel command), this
        /// is where the affected resting leg(s) are recorded losslessly
        /// ([ADR-0009 §2, §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
        stp_cancelled: Vec<CancelledLeg>,
    },
    /// A **market** [`VenueCommand::AddOrder`] (the upstream true non-resting
    /// primitive): fills plus the **cancelled** unfilled remainder — it never
    /// rests and is never assigned an invented price. Against an empty book this
    /// is `fills: []` with `unfilled_quantity` = the whole order
    /// ([ADR-0009 §3](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    Market {
        /// The captured fill legs (two per match; empty on an empty book).
        fills: Vec<Fill>,
        /// The unfilled remainder that was cancelled, in **contracts**.
        unfilled_quantity: u64,
        /// The resting legs this market order removed via self-trade prevention,
        /// in the deterministic sweep order — empty when no STP fired
        /// ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
        stp_cancelled: Vec<CancelledLeg>,
    },
    /// A [`VenueCommand::CancelOrder`] succeeded.
    Cancelled {
        /// The venue order id that was cancelled.
        order_id: VenueOrderId,
    },
    /// A **non-atomic** [`VenueCommand::Replace`], executed as cancel-then-add in
    /// one actor turn and recorded as one event: whether the cancel leg
    /// succeeded, and the [`AddOutcome`] of the add leg. If the add is rejected
    /// after the cancel succeeded, the old order is gone and no new order rests —
    /// a defined, replayable state, **not** rolled back
    /// ([ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
    Replace {
        /// Whether the cancel leg removed the original order.
        cancelled: bool,
        /// The outcome of the add leg.
        add: AddOutcome,
    },
    /// A [`VenueCommand::MassCancel`]: the **ordered** list of affected orders
    /// (id, owner, reason) in the deterministic sweep order. The
    /// `cancelled_count` is derived from its length — never stored as a bare
    /// count ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    MassCancelled {
        /// The affected orders, in the deterministic sweep order.
        affected: Vec<CancelledLeg>,
    },
    /// A [`VenueCommand::SetInstrumentStatus`] transition was applied.
    InstrumentStatusChanged {
        /// The instrument whose status changed.
        symbol: Symbol,
        /// The status it transitioned to.
        status: InstrumentStatus,
    },
    /// A [`VenueCommand::EvictExpiredOrders`] sweep: the evicted venue order ids
    /// in the deterministic sweep order.
    Evicted {
        /// The evicted venue order ids.
        evicted: Vec<VenueOrderId>,
    },
    /// A control-plane command ([`VenueCommand::MarketMakerControl`] /
    /// [`VenueCommand::Clock`] / [`VenueCommand::SimStep`]) was applied. Its
    /// *derived* effects (e.g. market-maker requotes) are journaled as their own
    /// sequenced commands ([02 §4.1](../../../docs/02-matching-architecture.md)).
    ///
    /// For a market-maker **kill** (`MarketMakerControl { enabled: Some(false),
    /// .. }`) the `swept` legs are the owner-scoped market-maker cancellations
    /// captured **losslessly in the same turn as the control** — the sweep is
    /// coupled into the kill control's own sequenced turn, per underlying, so it
    /// is crash-consistent (one journal event that is both "control applied" AND
    /// "these MM-owner orders cancelled", never a separate follow-on command a
    /// crash could skip) and re-runs deterministically against the rebuilt
    /// resting set on replay ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    /// For every **other** control (a parameter change, `enabled: Some(true)`) and
    /// for [`VenueCommand::Clock`] / [`VenueCommand::SimStep`], `swept` is
    /// **empty**. Following the enum's empty-vec convention it is always serialised
    /// (an empty array when nothing was swept) and required on decode.
    ControlApplied {
        /// The owner-scoped market-maker legs cancelled in the same turn as a
        /// kill control, in the deterministic sweep order — empty for every
        /// non-kill control and for `Clock` / `SimStep`.
        swept: Vec<CancelledLeg>,
    },
    /// The command was rejected; the book is untouched and no fill executed
    /// ([ADR-0009 §1](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    Rejected {
        /// The **typed** reject discriminant the gateway keys its client rendering
        /// on — never a string-match of `reason` (#132). The authorization-sensitive
        /// existence kinds ([`RejectKind::NotOwner`] / [`RejectKind::NotFound`] /
        /// [`RejectKind::NotResting`]) are masked identically at the client boundary.
        #[serde(default)] // a pre-#132 record has no `kind` → RejectKind::Internal
        kind: RejectKind,
        /// The human reason, kept for the journal + `tracing` — never on the wire
        /// verbatim for a masked kind, and never string-matched by a gateway.
        reason: String,
    },
    /// A **client-order-id idempotent retry** (#099): the account + `ClOrdID` key
    /// already resolved to a committed placement, so this turn created **no** new
    /// order, executed **no** new fill, and touched the book not at all. It carries
    /// the **original** placement's identity and captured terminal outcome so a
    /// gateway renders the true original terminal report (its order id, sequence,
    /// and fills), while every fan-out projection treats it as a no-op — the
    /// economic effects were already folded into the stores and published on the
    /// WS at first placement, and replaying them here would double-count positions
    /// and re-print phantom fills/depth. The boxed [`terminal`](Self::Duplicate::terminal)
    /// is always a genuine terminal outcome (never itself a `Duplicate`), because
    /// only a **fresh** placement is ever recorded under the key.
    Duplicate {
        /// The venue order id the **original** placement was assigned — the id
        /// that actually entered the book, echoed on the retry so a client never
        /// receives a freshly-minted id that never rested.
        original_order_id: VenueOrderId,
        /// The `underlying_sequence` of the **original** placement's committed
        /// turn — the terminal sequence a gateway renders, not this retry turn's.
        original_sequence: SequenceNumber,
        /// The original placement's captured terminal outcome, replayed for
        /// rendering only.
        terminal: Box<VenueOutcome>,
    },
}

impl VenueOutcome {
    /// Constructs a rejected outcome with a typed [`RejectKind`] discriminant and
    /// the human `reason` (journal + `tracing`). The gateway keys on `kind`, never
    /// the string, so refactoring the reason text can never silently change the
    /// wire mapping / mask (#132).
    #[must_use]
    pub fn rejected(kind: RejectKind, reason: impl Into<String>) -> Self {
        VenueOutcome::Rejected {
            kind,
            reason: reason.into(),
        }
    }

    /// The just-submitted aggressor's own **taker** fill legs as `(price,
    /// quantity)` pairs in capture order — the immediate execution of *this*
    /// add, projected directly from the captured terminal outcome.
    ///
    /// A gateway renders an order-entry response's fills from this, **never** a
    /// store read-back keyed on the freshly-minted order id. On an idempotent
    /// resend the executor returns the **stored** terminal outcome — the original
    /// order's fills, carrying the canonical order and execution ids — so this
    /// surfaces the true original terminal report, not an empty fresh read-back
    /// keyed on the resend's fresh order id and sequence (#099). It reads the
    /// already-captured [`VenueOutcome`], so it recomputes nothing and adds no
    /// wall-clock or RNG: it stays a deterministic function of the journal, and
    /// the fills it returns are exactly those [`StoreFanOut`](crate::exchange::StoreFanOut)
    /// folded into the executions store from this same event.
    ///
    /// Only the **taker** legs (the aggressor's own executions) are returned; the
    /// paired maker legs belong to the resting counterparties. Empty for a
    /// non-filling outcome (a pure rest, a reject, a cancel, or a control).
    #[must_use]
    pub fn taker_fill_legs(&self) -> Vec<(Cents, u64)> {
        // An idempotent Duplicate carries the ORIGINAL terminal outcome; render its
        // fills, never an empty read-back keyed on the retry's fresh id (#099).
        if let Self::Duplicate { terminal, .. } = self {
            return terminal.taker_fill_legs();
        }
        let fills: &[Fill] = match self {
            Self::Added { fills, .. } | Self::Market { fills, .. } => fills,
            _ => &[],
        };
        fills
            .iter()
            .filter(|fill| fill.liquidity == LiquidityFlag::Taker)
            .map(|fill| (fill.price, fill.quantity))
            .collect()
    }

    /// The **effective terminal outcome** a gateway renders — the stored terminal
    /// of an idempotent [`Duplicate`](Self::Duplicate), else `self`. Unwrapping the
    /// Duplicate lets a handler match the ORIGINAL placement's reject/fill state
    /// (e.g. a stored `Rejected`) instead of misreading a retry as a fresh accept
    /// (#099). A Duplicate's terminal is never itself a Duplicate, so this unwraps
    /// at most once.
    #[must_use]
    #[inline]
    pub fn terminal(&self) -> &VenueOutcome {
        match self {
            Self::Duplicate { terminal, .. } => terminal,
            other => other,
        }
    }

    /// The order identity a gateway renders on the placement response: on an
    /// idempotent [`Duplicate`](Self::Duplicate) the **original** placement's id +
    /// terminal sequence (the id that actually entered the book), else the
    /// freshly-minted `fresh_order_id` + `fresh_sequence` of this turn (#099). This
    /// closes the phantom-identity gap where a retry echoed an id that never rested.
    #[must_use]
    pub fn rendered_identity<'a>(
        &'a self,
        fresh_order_id: &'a VenueOrderId,
        fresh_sequence: SequenceNumber,
    ) -> (&'a VenueOrderId, SequenceNumber) {
        match self {
            Self::Duplicate {
                original_order_id,
                original_sequence,
                ..
            } => (original_order_id, *original_sequence),
            _ => (fresh_order_id, fresh_sequence),
        }
    }
}

// ============================================================================
// Journaled market-maker control knobs — finite, range-validated on decode
// ============================================================================
//
// The three [`VenueCommand::MarketMakerControl`] knobs are **dimensionless
// `f64` multipliers** (the documented float exception — not money,
// [01 §3](../../../docs/01-domain-model.md)). A journaled `f64` is carried
// verbatim and read back byte-identical, so a `NaN` / `±Inf` / out-of-range
// value in a hostile or corrupt journal record would poison replay and JSON
// persistence (`serde_json` renders a non-finite `f64` as `null`). Decode is
// therefore the venue's re-validation boundary: each present knob is checked
// finite and in its documented range, so a bad record is a **typed decode
// error**, never a silent poison. Valid values decode unchanged, so the wire
// form is stable.
//
// The ranges mirror the market-maker substrate's documented clamps
// (`market_maker::config`); they are owned here independently because the
// `exchange` layer is below `market_maker` (which depends on it) and cannot
// import back without a cycle. The gateway validates the same ranges on the
// write side before a control is journaled ([05 §8](../../../docs/05-microstructure-config.md)).

/// Minimum accepted spread multiplier.
const SPREAD_MULTIPLIER_MIN: f64 = 0.1;
/// Maximum accepted spread multiplier.
const SPREAD_MULTIPLIER_MAX: f64 = 10.0;
/// Minimum accepted size scalar.
const SIZE_SCALAR_MIN: f64 = 0.0;
/// Maximum accepted size scalar.
const SIZE_SCALAR_MAX: f64 = 1.0;
/// Minimum accepted directional skew.
const DIRECTIONAL_SKEW_MIN: f64 = -1.0;
/// Maximum accepted directional skew.
const DIRECTIONAL_SKEW_MAX: f64 = 1.0;

/// Decodes an optional control knob and rejects a non-finite / out-of-range
/// value.
///
/// `RangeInclusive::contains` rejects `NaN` (every comparison with `NaN` is
/// false) and both infinities (outside any finite range), so one containment
/// check covers finiteness and range together. A `None` (unchanged) knob is
/// admitted unchecked. The error is a typed serde decode error — the journal
/// read fails loudly rather than admitting a poison value.
#[inline]
fn deserialize_ranged_knob<'de, D>(
    deserializer: D,
    field: &'static str,
    min: f64,
    max: f64,
) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)?;
    if let Some(v) = value
        && !(min..=max).contains(&v)
    {
        return Err(serde::de::Error::custom(format!(
            "{field} must be finite and within [{min}, {max}], got {v}"
        )));
    }
    Ok(value)
}

/// Field validator for `spread_multiplier` — see [`deserialize_ranged_knob`].
#[inline]
fn deserialize_spread_multiplier<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_ranged_knob(
        deserializer,
        "spread_multiplier",
        SPREAD_MULTIPLIER_MIN,
        SPREAD_MULTIPLIER_MAX,
    )
}

/// Field validator for `size_scalar` — see [`deserialize_ranged_knob`].
#[inline]
fn deserialize_size_scalar<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_ranged_knob(
        deserializer,
        "size_scalar",
        SIZE_SCALAR_MIN,
        SIZE_SCALAR_MAX,
    )
}

/// Field validator for `directional_skew` — see [`deserialize_ranged_knob`].
#[inline]
fn deserialize_directional_skew<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_ranged_knob(
        deserializer,
        "directional_skew",
        DIRECTIONAL_SKEW_MIN,
        DIRECTIONAL_SKEW_MAX,
    )
}

// ============================================================================
// VenueCommand — the venue's own instruction set
// ============================================================================

/// The venue's own, versioned instruction set — one command per sequenced actor
/// turn ([ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md),
/// [03 §10](../../../docs/03-protocol-surfaces.md)).
///
/// It wraps the intent **plus the identity the upstream `OptionChainCommand`
/// loses** — `account`, `owner`, `client_order_id`, `order_type`,
/// `time_in_force`, `stp_mode` — so a journal → replay → fan-out round-trip
/// preserves who placed each order and how. The venue order ids are
/// venue-assigned deterministically **before** the write-ahead append
/// ([01 §6.1](../../../docs/01-domain-model.md)); the engine's process-local
/// `OrderId` is excluded from the oracle and never carried here.
///
/// `PartialEq` (not `Eq`) because the market-maker control knobs are `f64`
/// (dimensionless multipliers, not money — [01 §3](../../../docs/01-domain-model.md))
/// and an expiry is an `ExpirationDate`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "PascalCase",
    rename_all_fields = "snake_case"
)]
pub enum VenueCommand {
    /// Place a limit or market order. `order_type` selects the leaf path — a
    /// limit add (`limit_price` present) or the upstream true market primitive
    /// (`limit_price` absent, [ADR-0009 §3](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    AddOrder {
        /// The target contract symbol.
        symbol: Symbol,
        /// The venue-assigned order id, minted before the write-ahead append.
        order_id: VenueOrderId,
        /// The owning account.
        account: AccountId,
        /// The STP owner hash used for by-user mass cancel and STP grouping.
        owner: Hash32,
        /// The client-supplied idempotency key, when one was provided (always
        /// serialised — `null` when absent — so encode/decode stay symmetric).
        client_order_id: Option<ClientOrderId>,
        /// Order side (upstream matching-seam [`Side`]).
        side: Side,
        /// Limit vs market (reuses the #004 [`OrderType`]).
        order_type: OrderType,
        /// Limit price in **cents** — present for a limit order, absent for a
        /// market order.
        limit_price: Option<Cents>,
        /// Order quantity in **contracts**.
        quantity: u64,
        /// Time in force (upstream [`TimeInForce`]).
        time_in_force: TimeInForce,
        /// Self-trade-prevention mode (upstream [`STPMode`]).
        stp_mode: STPMode,
    },
    /// Cancel a resting order.
    CancelOrder {
        /// The target contract symbol.
        symbol: Symbol,
        /// The venue order id to cancel.
        order_id: VenueOrderId,
        /// The requesting account.
        account: AccountId,
    },
    /// Replace a resting order (**non-atomic** cancel-then-add in one turn — the
    /// upstream engine has no atomic replace, [ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
    Replace {
        /// The target contract symbol.
        symbol: Symbol,
        /// The venue order id of the resting order to replace.
        order_id: VenueOrderId,
        /// The venue-assigned order id of the replacement add leg.
        new_order_id: VenueOrderId,
        /// The requesting account.
        account: AccountId,
        /// The **replacement's** client-order id (the new `ClOrdID`), when the
        /// gateway supplied one — journaled so #085 boot recovery rebuilds the
        /// cross-session `(account, ClOrdID) → new_order_id` correlation (#098) for
        /// the replacement exactly as the live actor published it post-journal.
        /// `#[serde(default)]` so a pre-#098 record with no field decodes to `None`
        /// (a legacy replace simply carries no rebuildable correlation).
        #[serde(default)]
        client_order_id: Option<ClientOrderId>,
        /// The **original's** client-order id (the retired `OrigClOrdID`), when the
        /// gateway supplied one — journaled so recovery **retires** the stale
        /// `(account, OrigClOrdID)` entry (its order was cancelled by the replace's
        /// cancel leg) exactly as the live actor did. `#[serde(default)]` for the
        /// same backward-compatibility reason as `client_order_id`.
        #[serde(default)]
        orig_client_order_id: Option<ClientOrderId>,
        /// The replacement's side.
        side: Side,
        /// The replacement's limit price in **cents**, when it is a limit.
        limit_price: Option<Cents>,
        /// The replacement's quantity in **contracts**.
        quantity: u64,
        /// The replacement's time in force.
        time_in_force: TimeInForce,
        /// The replacement's self-trade-prevention mode.
        stp_mode: STPMode,
    },
    /// Mass cancel across a hierarchy scope, filtered by type
    /// ([ADR-0009 §4](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
    MassCancel {
        /// The hierarchy scope to sweep.
        scope: MassCancelScope,
        /// The filter within that scope.
        cancel_type: MassCancelType,
        /// The requesting account.
        account: AccountId,
    },
    /// Transition an instrument's lifecycle status (halt / resume / settle /
    /// expire), journaled so replay reconstructs halted and expired instruments
    /// ([01 §5](../../../docs/01-domain-model.md)).
    SetInstrumentStatus {
        /// The target contract symbol.
        symbol: Symbol,
        /// The status to transition to (upstream [`InstrumentStatus`]).
        status: InstrumentStatus,
    },
    /// Evict every resting order whose time-in-force has expired at `now_ms`
    /// (`Day` / `Gtd`), in the hierarchy's deterministic sweep order. The
    /// journaled `now_ms` is the sole deterministic input — replay applies it
    /// rather than reading the replay clock.
    EvictExpiredOrders {
        /// The venue-clock cutoff in **milliseconds**.
        now_ms: EventTimestamp,
    },
    /// A market-maker control change (spread / size / skew / kill / enable),
    /// journaled so replay reproduces later requotes
    /// ([03 §10](../../../docs/03-protocol-surfaces.md)). The knobs are
    /// **dimensionless** `f64` multipliers, not money
    /// ([01 §3](../../../docs/01-domain-model.md)); each is `None` when this
    /// command leaves it unchanged. The detailed persona/quoter application is
    /// simulation-owned — this is the journaled control payload. A journaled
    /// `f64` is carried verbatim and read back byte-identical on replay, so it is
    /// replay-stable (it is never recomputed).
    MarketMakerControl {
        /// New global spread multiplier (`0.1`–`10.0`), when changing it.
        /// Re-validated finite and in range on decode.
        #[serde(deserialize_with = "deserialize_spread_multiplier")]
        spread_multiplier: Option<f64>,
        /// New global size scalar (`0.0`–`1.0`), when changing it.
        /// Re-validated finite and in range on decode.
        #[serde(deserialize_with = "deserialize_size_scalar")]
        size_scalar: Option<f64>,
        /// New global directional skew (`-1.0`–`1.0`), when changing it.
        /// Re-validated finite and in range on decode.
        #[serde(deserialize_with = "deserialize_directional_skew")]
        directional_skew: Option<f64>,
        /// New master-enabled (kill / enable) state, when changing it.
        enabled: Option<bool>,
    },
    /// Advance the venue clock to `now_ms` (a stepped / replay clock tick). The
    /// value is carried in the command, never read from the replay clock
    /// ([02 §4.1](../../../docs/02-matching-architecture.md)).
    Clock {
        /// The venue-clock instant to advance to, in **milliseconds**.
        now_ms: EventTimestamp,
    },
    /// A simulated step — including a manual underlying-price override
    /// (`POST /api/v1/prices` is wrapped as a `SimStep`-class command so it is
    /// journaled and replays, [03 §10](../../../docs/03-protocol-surfaces.md)).
    /// The richer simulated-walk payload is simulation-owned; this carries the
    /// documented price-override case.
    SimStep {
        /// The step's venue-clock instant, in **milliseconds**.
        now_ms: EventTimestamp,
        /// The underlying ticker whose price is set.
        underlying: String,
        /// The new underlying price in **cents**.
        price: Cents,
        /// Optional bid in **cents**.
        bid: Option<Cents>,
        /// Optional ask in **cents**.
        ask: Option<Cents>,
    },
}

// ============================================================================
// VenueEvent — the durable envelope: command + captured outcome
// ============================================================================

/// The durable venue envelope — one journaled record per committed command
/// ([ADR-0006 §1](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// It pairs the [`VenueCommand`] with the losslessly captured [`VenueOutcome`],
/// stamped with the journaled `underlying_sequence` (the only journaled total
/// order, per underlying) and the venue-clock `venue_ts`, and carries the
/// mandatory `schema` tag. It is the replay input and the fan-out source; the
/// engine's derived analytics (mark prices) are **not** part of it and are
/// recomputed ([01 §7](../../../docs/01-domain-model.md)).
///
/// `PartialEq` (not `Eq`) because it embeds a [`VenueCommand`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct VenueEvent {
    /// The mandatory schema tag — always [`VENUE_ENVELOPE_SCHEMA`]
    /// (`"venue.v1"`); a missing `schema` is a hard decode error.
    pub schema: String,
    /// The journaled per-underlying sequence — the only journaled total order.
    pub underlying_sequence: SequenceNumber,
    /// The venue-clock timestamp, in **milliseconds** (venue-assigned, not the
    /// engine's wall clock).
    pub venue_ts: EventTimestamp,
    /// The command that was executed.
    pub command: VenueCommand,
    /// The losslessly captured outcome.
    pub outcome: VenueOutcome,
}

impl VenueEvent {
    /// Builds a `venue.v1` event, stamping the mandatory
    /// [`schema`](VenueEvent::schema) tag.
    #[must_use]
    #[inline]
    pub fn new(
        underlying_sequence: SequenceNumber,
        venue_ts: EventTimestamp,
        command: VenueCommand,
        outcome: VenueOutcome,
    ) -> Self {
        Self {
            schema: VENUE_ENVELOPE_SCHEMA.to_string(),
            underlying_sequence,
            venue_ts,
            command,
            outcome,
        }
    }

    /// Returns `true` iff the event's `schema` tag is the one the running binary
    /// understands ([`VENUE_ENVELOPE_SCHEMA`]).
    ///
    /// The recovery reducer (#006) refuses a forward-incompatible schema; here it
    /// is the pure predicate.
    #[must_use]
    #[inline]
    pub fn is_current_schema(&self) -> bool {
        self.schema == VENUE_ENVELOPE_SCHEMA
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::LineageId;

    /// A deterministic non-zero STP owner hash for fixtures.
    fn owner(byte: u8) -> Hash32 {
        Hash32([byte; 32])
    }

    /// A parsed fixture symbol, panicking (never `unwrap`) on an unexpected
    /// parse failure.
    fn sym(raw: &str) -> Symbol {
        match Symbol::parse(raw) {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
        }
    }

    /// A resting leg removed by self-trade prevention, for the `stp_cancelled`
    /// fixtures.
    fn stp_leg(lineage: &LineageId, seq: SequenceNumber) -> CancelledLeg {
        CancelledLeg {
            order_id: lineage.venue_order_id("BTC", seq, 0),
            owner: owner(0x22),
            symbol: sym("BTC-20240329-50000-C"),
            side: Side::Sell,
            reason: CancelReason::SelfTradePrevention,
        }
    }

    /// The two linked legs of one match, sharing one execution id.
    fn match_legs(lineage: &LineageId, seq: SequenceNumber, fill_index: u32) -> (Fill, Fill) {
        let execution_id = lineage.execution_id("BTC", seq, fill_index);
        let maker = Fill {
            execution_id: execution_id.clone(),
            order_id: lineage.venue_order_id("BTC", SequenceNumber::new(1), 0),
            account: AccountId::new("maker-acct"),
            owner: owner(0x11),
            side: Side::Sell,
            liquidity: LiquidityFlag::Maker,
            price: Cents::new(50_000),
            quantity: 2,
            fee: SignedCents::new(-10),
        };
        let taker = Fill {
            execution_id,
            order_id: lineage.venue_order_id("BTC", seq, 0),
            account: AccountId::new("taker-acct"),
            owner: owner(0x22),
            side: Side::Buy,
            liquidity: LiquidityFlag::Taker,
            price: Cents::new(50_000),
            quantity: 2,
            fee: SignedCents::new(15),
        };
        (maker, taker)
    }

    fn add_order_command(lineage: &LineageId) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: lineage.venue_order_id("BTC", SequenceNumber::new(7), 0),
            account: AccountId::new("taker-acct"),
            owner: owner(0x22),
            client_order_id: Some(ClientOrderId::new("client-42")),
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 2,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        }
    }

    /// Round-trips a value through JSON and asserts identity.
    fn assert_serde_identity<T>(value: &T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let json = match serde_json::to_string(value) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        match serde_json::from_str::<T>(&json) {
            Ok(back) => assert_eq!(&back, value),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    // ---- Fill / two-leg model ---------------------------------------------

    #[test]
    fn test_two_legs_of_one_match_share_execution_id() {
        let lineage = LineageId::new("run-1");
        let (maker, taker) = match_legs(&lineage, SequenceNumber::new(7), 0);
        assert_eq!(maker.execution_id, taker.execution_id);
        // But each leg keeps its own account, side, liquidity, and fee.
        assert_ne!(maker.account, taker.account);
        assert_ne!(maker.side, taker.side);
        assert_ne!(maker.liquidity, taker.liquidity);
        assert_ne!(maker.fee, taker.fee);
    }

    #[test]
    fn test_fill_serialises_with_integer_cents_and_seam_side() {
        let lineage = LineageId::new("run-1");
        let (_, taker) = match_legs(&lineage, SequenceNumber::new(7), 0);
        let value = match serde_json::to_value(&taker) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert!(value["price"].is_u64(), "price must be integer cents");
        assert!(value["fee"].is_i64(), "fee must be integer cents");
        // The seam Side serialises as the upstream form (BUY/SELL), not the DTO
        // lowercase form.
        assert_eq!(value["side"], serde_json::json!("BUY"));
        assert_eq!(value["liquidity"], serde_json::json!("taker"));
        assert_serde_identity(&taker);
    }

    #[test]
    fn test_fill_rejects_unknown_field() {
        let json = r#"{"execution_id":"a","order_id":"b","account":"c","owner":"0000000000000000000000000000000000000000000000000000000000000000","side":"BUY","liquidity":"taker","price":1,"quantity":1,"fee":0,"typo":true}"#;
        match serde_json::from_str::<Fill>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected unknown-field rejection, parsed {parsed:?}"),
        }
    }

    // ---- Command variant construction + serde ------------------------------

    #[test]
    fn test_add_order_command_carries_dropped_identity() {
        let lineage = LineageId::new("run-1");
        let command = add_order_command(&lineage);
        // The identity the upstream OptionChainCommand::AddOrder drops.
        match &command {
            VenueCommand::AddOrder {
                account,
                owner,
                client_order_id,
                time_in_force,
                stp_mode,
                order_type,
                ..
            } => {
                assert_eq!(account, &AccountId::new("taker-acct"));
                assert_eq!(owner, &self::owner(0x22));
                assert_eq!(client_order_id, &Some(ClientOrderId::new("client-42")));
                assert_eq!(time_in_force, &TimeInForce::Gtc);
                assert_eq!(stp_mode, &STPMode::None);
                assert_eq!(order_type, &OrderType::Limit);
            }
            other => panic!("expected AddOrder, got {other:?}"),
        }
        assert_serde_identity(&command);
    }

    #[test]
    fn test_cancel_order_command_roundtrips() {
        let lineage = LineageId::new("run-1");
        let command = VenueCommand::CancelOrder {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: lineage.venue_order_id("BTC", SequenceNumber::new(7), 0),
            account: AccountId::new("acct-1"),
        };
        assert_serde_identity(&command);
    }

    #[test]
    fn test_replace_command_roundtrips() {
        let lineage = LineageId::new("run-1");
        let command = VenueCommand::Replace {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: lineage.venue_order_id("BTC", SequenceNumber::new(7), 0),
            new_order_id: lineage.venue_order_id("BTC", SequenceNumber::new(8), 0),
            account: AccountId::new("acct-1"),
            client_order_id: Some(ClientOrderId::new("cl-new")),
            orig_client_order_id: Some(ClientOrderId::new("cl-orig")),
            side: Side::Sell,
            limit_price: Some(Cents::new(50_100)),
            quantity: 3,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::CancelTaker,
        };
        assert_serde_identity(&command);
    }

    #[test]
    fn test_replace_command_client_order_ids_default_when_absent() {
        // A pre-#098 record with no ClOrdID fields must still decode (they are
        // `#[serde(default)]`), yielding `None` for both — a legacy replace simply
        // carries no rebuildable cross-session correlation.
        let json = r#"{"Replace":{"symbol":"BTC-20240329-50000-C","order_id":"o","new_order_id":"n","account":"a","side":"BUY","limit_price":50100,"quantity":3,"time_in_force":"GTC","stp_mode":"None"}}"#;
        match serde_json::from_str::<VenueCommand>(json) {
            Ok(VenueCommand::Replace {
                client_order_id,
                orig_client_order_id,
                ..
            }) => {
                assert_eq!(client_order_id, None);
                assert_eq!(orig_client_order_id, None);
            }
            other => panic!("expected a Replace decoding both ClOrdIDs to None, got {other:?}"),
        }
    }

    #[test]
    fn test_mass_cancel_command_roundtrips_and_maps_scope_type() {
        let command = VenueCommand::MassCancel {
            scope: MassCancelScope::Underlying,
            cancel_type: MassCancelType::ByUser(owner(0x33)),
            account: AccountId::new("acct-1"),
        };
        assert_serde_identity(&command);
        // A book-scoped, by-side variant also round-trips.
        let scoped = VenueCommand::MassCancel {
            scope: MassCancelScope::Book(sym("BTC-20240329-50000-C")),
            cancel_type: MassCancelType::BySide(Side::Sell),
            account: AccountId::new("acct-1"),
        };
        assert_serde_identity(&scoped);
    }

    #[test]
    fn test_set_instrument_status_command_roundtrips() {
        let command = VenueCommand::SetInstrumentStatus {
            symbol: sym("BTC-20240329-50000-C"),
            status: InstrumentStatus::Halted,
        };
        assert_serde_identity(&command);
    }

    #[test]
    fn test_evict_expired_orders_command_roundtrips() {
        let command = VenueCommand::EvictExpiredOrders {
            now_ms: EventTimestamp::new(1_700_000_000_000),
        };
        assert_serde_identity(&command);
    }

    #[test]
    fn test_market_maker_control_command_roundtrips_exact_floats() {
        // Exactly-representable f64 values round-trip losslessly through JSON.
        let command = VenueCommand::MarketMakerControl {
            spread_multiplier: Some(1.5),
            size_scalar: Some(0.5),
            directional_skew: Some(-0.25),
            enabled: Some(false),
        };
        assert_serde_identity(&command);
    }

    #[test]
    fn test_market_maker_control_rejects_out_of_range_and_non_finite() {
        // A hostile / corrupt journal record with an out-of-range or non-finite
        // persona knob must be a typed decode error, not a silent poison — a
        // non-finite `f64` would break replay-equality and JSON persistence.
        // Documented ranges: spread [0.1, 10.0], size [0.0, 1.0], skew [-1.0, 1.0].
        let out_of_range = [
            r#"{"MarketMakerControl":{"spread_multiplier":999.0,"size_scalar":null,"directional_skew":null,"enabled":null}}"#,
            r#"{"MarketMakerControl":{"spread_multiplier":0.0,"size_scalar":null,"directional_skew":null,"enabled":null}}"#,
            r#"{"MarketMakerControl":{"spread_multiplier":null,"size_scalar":2.0,"directional_skew":null,"enabled":null}}"#,
            r#"{"MarketMakerControl":{"spread_multiplier":null,"size_scalar":-0.5,"directional_skew":null,"enabled":null}}"#,
            r#"{"MarketMakerControl":{"spread_multiplier":null,"size_scalar":null,"directional_skew":1.5,"enabled":null}}"#,
            r#"{"MarketMakerControl":{"spread_multiplier":null,"size_scalar":null,"directional_skew":-5.0,"enabled":null}}"#,
        ];
        for json in out_of_range {
            match serde_json::from_str::<VenueCommand>(json) {
                Err(_) => {}
                Ok(parsed) => panic!("expected an out-of-range knob rejection, parsed {parsed:?}"),
            }
        }
        // A non-finite literal (`NaN` / `Infinity`) is not valid JSON and is
        // refused at the tokenizer, so no non-finite value can survive decode.
        for json in [
            r#"{"MarketMakerControl":{"spread_multiplier":NaN,"size_scalar":null,"directional_skew":null,"enabled":null}}"#,
            r#"{"MarketMakerControl":{"spread_multiplier":null,"size_scalar":null,"directional_skew":Infinity,"enabled":null}}"#,
        ] {
            match serde_json::from_str::<VenueCommand>(json) {
                Err(_) => {}
                Ok(parsed) => panic!("expected a non-finite knob rejection, parsed {parsed:?}"),
            }
        }
        // A valid, in-range control still decodes — the wire form is stable for
        // valid data (no golden churn).
        let ok = r#"{"MarketMakerControl":{"spread_multiplier":1.5,"size_scalar":0.5,"directional_skew":-0.25,"enabled":false}}"#;
        match serde_json::from_str::<VenueCommand>(ok) {
            Ok(VenueCommand::MarketMakerControl {
                spread_multiplier,
                size_scalar,
                directional_skew,
                enabled,
            }) => {
                assert_eq!(spread_multiplier, Some(1.5));
                assert_eq!(size_scalar, Some(0.5));
                assert_eq!(directional_skew, Some(-0.25));
                assert_eq!(enabled, Some(false));
            }
            other => panic!("expected a valid MarketMakerControl to decode, got {other:?}"),
        }
    }

    #[test]
    fn test_clock_and_sim_step_commands_roundtrip() {
        assert_serde_identity(&VenueCommand::Clock {
            now_ms: EventTimestamp::new(1_700_000_000_000),
        });
        assert_serde_identity(&VenueCommand::SimStep {
            now_ms: EventTimestamp::new(1_700_000_000_000),
            underlying: "BTC".to_string(),
            price: Cents::new(4_200_000),
            bid: Some(Cents::new(4_199_500)),
            ask: Some(Cents::new(4_200_500)),
        });
    }

    #[test]
    fn test_command_uses_pascal_case_variant_tags() {
        let lineage = LineageId::new("run-1");
        let value = match serde_json::to_value(add_order_command(&lineage)) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        // Externally tagged: the variant tag is the top-level key, PascalCase.
        assert!(value.get("AddOrder").is_some());
        // Fields inside the variant are snake_case.
        assert!(value["AddOrder"].get("time_in_force").is_some());
        assert!(value["AddOrder"].get("stp_mode").is_some());
    }

    #[test]
    fn test_command_rejects_unknown_field() {
        // A stray field inside a known variant is a hard decode error.
        let json = r#"{"CancelOrder":{"symbol":"BTC-20240329-50000-C","order_id":"x","account":"a","typo":1}}"#;
        match serde_json::from_str::<VenueCommand>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected unknown-field rejection, parsed {parsed:?}"),
        }
    }

    // ---- Outcome variant construction + serde ------------------------------

    #[test]
    fn test_added_outcome_carries_fills_and_remainder() {
        let lineage = LineageId::new("run-1");
        let (maker, taker) = match_legs(&lineage, SequenceNumber::new(7), 0);
        let outcome = VenueOutcome::Added {
            fills: vec![maker, taker],
            resting_quantity: 0,
            stp_cancelled: vec![],
        };
        assert_serde_identity(&outcome);
    }

    #[test]
    fn test_added_outcome_records_stp_cancelled_makers() {
        // An aggressor whose STP mode is cancel_maker / cancel_both removes a
        // same-owner resting leg inside the one add turn — there is no separate
        // cancel command, so the affected leg is recorded on the add outcome.
        let lineage = LineageId::new("run-1");
        let (maker, taker) = match_legs(&lineage, SequenceNumber::new(7), 0);
        let removed = stp_leg(&lineage, SequenceNumber::new(3));
        let outcome = VenueOutcome::Added {
            fills: vec![maker, taker],
            resting_quantity: 0,
            stp_cancelled: vec![removed.clone()],
        };
        // The removed resting leg is recorded losslessly with the STP reason.
        if let VenueOutcome::Added { stp_cancelled, .. } = &outcome {
            assert_eq!(stp_cancelled.len(), 1);
            assert_eq!(stp_cancelled[0].reason, CancelReason::SelfTradePrevention);
        }
        assert_serde_identity(&outcome);
        let value = match serde_json::to_value(&outcome) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(
            value["Added"]["stp_cancelled"][0]["reason"],
            serde_json::json!("SelfTradePrevention")
        );
    }

    #[test]
    fn test_market_outcome_empty_book_is_zero_fill() {
        // The empty-book market order: zero fills, fully unfilled — representable.
        let outcome = VenueOutcome::Market {
            fills: vec![],
            unfilled_quantity: 10,
            stp_cancelled: vec![],
        };
        assert_serde_identity(&outcome);
        let value = match serde_json::to_value(&outcome) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        // Every Vec field is always present, even when empty.
        assert_eq!(value["Market"]["fills"], serde_json::json!([]));
        assert_eq!(value["Market"]["stp_cancelled"], serde_json::json!([]));
        assert_eq!(value["Market"]["unfilled_quantity"], serde_json::json!(10));
    }

    #[test]
    fn test_market_outcome_records_stp_cancelled() {
        let lineage = LineageId::new("run-1");
        let (maker, taker) = match_legs(&lineage, SequenceNumber::new(9), 0);
        let outcome = VenueOutcome::Market {
            fills: vec![maker, taker],
            unfilled_quantity: 1,
            stp_cancelled: vec![stp_leg(&lineage, SequenceNumber::new(4))],
        };
        assert_serde_identity(&outcome);
    }

    #[test]
    fn test_replace_outcome_non_atomic_cancel_succeeded_add_rejected() {
        // The defined partial state: cancel succeeded, add rejected — not rolled
        // back.
        let outcome = VenueOutcome::Replace {
            cancelled: true,
            add: AddOutcome::rejected(RejectKind::NotFillable, "post-only would cross"),
        };
        assert_serde_identity(&outcome);
    }

    #[test]
    fn test_replace_outcome_add_leg_can_fill_or_rest() {
        let lineage = LineageId::new("run-1");
        let (maker, taker) = match_legs(&lineage, SequenceNumber::new(8), 0);
        assert_serde_identity(&VenueOutcome::Replace {
            cancelled: true,
            add: AddOutcome::Filled {
                fills: vec![maker.clone(), taker.clone()],
                stp_cancelled: vec![],
            },
        });
        assert_serde_identity(&VenueOutcome::Replace {
            cancelled: true,
            add: AddOutcome::Rested {
                fills: vec![maker, taker],
                resting_quantity: 1,
                stp_cancelled: vec![stp_leg(&lineage, SequenceNumber::new(2))],
            },
        });
    }

    #[test]
    fn test_mass_cancelled_outcome_is_ordered_and_count_derives() {
        let lineage = LineageId::new("run-1");
        let affected = vec![
            CancelledLeg {
                order_id: lineage.venue_order_id("BTC", SequenceNumber::new(1), 0),
                owner: owner(0x11),
                symbol: sym("BTC-20240329-50000-C"),
                side: Side::Buy,
                reason: CancelReason::MassCancel,
            },
            CancelledLeg {
                order_id: lineage.venue_order_id("BTC", SequenceNumber::new(2), 0),
                owner: owner(0x11),
                symbol: sym("BTC-20240329-50000-C"),
                side: Side::Sell,
                reason: CancelReason::MassCancel,
            },
        ];
        let outcome = VenueOutcome::MassCancelled {
            affected: affected.clone(),
        };
        // The count is derived from the ordered list's length, never stored.
        if let VenueOutcome::MassCancelled { affected } = &outcome {
            assert_eq!(affected.len(), 2);
        }
        assert_serde_identity(&outcome);
    }

    #[test]
    fn test_remaining_outcomes_roundtrip() {
        let lineage = LineageId::new("run-1");
        assert_serde_identity(&VenueOutcome::Cancelled {
            order_id: lineage.venue_order_id("BTC", SequenceNumber::new(7), 0),
        });
        assert_serde_identity(&VenueOutcome::InstrumentStatusChanged {
            symbol: sym("BTC-20240329-50000-C"),
            status: InstrumentStatus::Settling,
        });
        assert_serde_identity(&VenueOutcome::Evicted {
            evicted: vec![lineage.venue_order_id("BTC", SequenceNumber::new(1), 0)],
        });
        assert_serde_identity(&VenueOutcome::ControlApplied { swept: vec![] });
        assert_serde_identity(&VenueOutcome::rejected(
            RejectKind::InstrumentNotActive,
            "instrument halted",
        ));
    }

    #[test]
    fn test_reject_kind_round_trips_pascal_case() {
        for kind in [
            RejectKind::NotFound,
            RejectKind::NotOwner,
            RejectKind::NotResting,
            RejectKind::InstrumentNotActive,
            RejectKind::InvalidOrder,
            RejectKind::NotFillable,
            RejectKind::Internal,
        ] {
            let outcome = VenueOutcome::rejected(kind, "reason text");
            assert_serde_identity(&outcome);
        }
        // The wire form is the PascalCase variant name.
        assert_eq!(
            serde_json::to_value(RejectKind::NotOwner).ok(),
            Some(serde_json::json!("NotOwner"))
        );
    }

    #[test]
    fn test_pre_132_rejected_record_without_kind_decodes_to_internal() {
        // A durable journal record written by a PRE-#132 binary carried a `Rejected`
        // with only a `reason` and no `kind`. `#[serde(default)]` on the field keeps
        // that record decodable (→ RejectKind::Internal) so an older journal still
        // replays, rather than failing decode. New records always carry the kind.
        let legacy = serde_json::json!({ "Rejected": { "reason": "some old reason" } });
        let decoded: VenueOutcome = serde_json::from_value(legacy).expect("legacy decodes");
        assert_eq!(
            decoded,
            VenueOutcome::Rejected {
                kind: RejectKind::Internal,
                reason: "some old reason".to_string()
            }
        );
        let legacy_add = serde_json::json!({ "Rejected": { "reason": "old add reason" } });
        let decoded_add: AddOutcome =
            serde_json::from_value(legacy_add).expect("legacy add decodes");
        assert_eq!(
            decoded_add,
            AddOutcome::Rejected {
                kind: RejectKind::Internal,
                reason: "old add reason".to_string()
            }
        );
    }

    #[test]
    fn test_control_applied_swept_follows_empty_vec_convention() {
        // A non-kill control carries an empty `swept`, always serialised as [].
        let outcome = VenueOutcome::ControlApplied { swept: vec![] };
        assert_serde_identity(&outcome);
        let value = match serde_json::to_value(&outcome) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(value["ControlApplied"]["swept"], serde_json::json!([]));
    }

    #[test]
    fn test_control_applied_kill_records_swept_owner_scoped_legs() {
        // A kill couples the owner-scoped MM sweep into the control's own turn:
        // the cancelled MM legs are recorded losslessly on `ControlApplied`.
        let lineage = LineageId::new("run-1");
        let swept = vec![CancelledLeg {
            order_id: lineage.venue_order_id("BTC", SequenceNumber::new(1), 0),
            owner: owner(0xEE),
            symbol: sym("BTC-20240329-50000-C"),
            side: Side::Buy,
            reason: CancelReason::MassCancel,
        }];
        let outcome = VenueOutcome::ControlApplied {
            swept: swept.clone(),
        };
        if let VenueOutcome::ControlApplied { swept } = &outcome {
            assert_eq!(swept.len(), 1);
            assert_eq!(swept[0].reason, CancelReason::MassCancel);
        }
        assert_serde_identity(&outcome);
    }

    #[test]
    fn test_cancel_reason_serialises_pascal_case() {
        assert_eq!(
            serde_json::to_value(CancelReason::SelfTradePrevention).ok(),
            Some(serde_json::json!("SelfTradePrevention"))
        );
    }

    // ---- VenueEvent (schema tag mandatory) ---------------------------------

    #[test]
    fn test_venue_event_stamps_schema_and_roundtrips() {
        let lineage = LineageId::new("run-1");
        let (maker, taker) = match_legs(&lineage, SequenceNumber::new(7), 0);
        let event = VenueEvent::new(
            SequenceNumber::new(7),
            EventTimestamp::new(1_700_000_000_000),
            add_order_command(&lineage),
            VenueOutcome::Added {
                fills: vec![maker, taker],
                resting_quantity: 0,
                stp_cancelled: vec![],
            },
        );
        assert_eq!(event.schema, VENUE_ENVELOPE_SCHEMA);
        assert!(event.is_current_schema());
        assert_serde_identity(&event);
    }

    #[test]
    fn test_venue_event_schema_tag_is_present_on_the_wire() {
        let lineage = LineageId::new("run-1");
        let event = VenueEvent::new(
            SequenceNumber::new(7),
            EventTimestamp::new(1),
            add_order_command(&lineage),
            VenueOutcome::Added {
                fills: vec![],
                resting_quantity: 2,
                stp_cancelled: vec![],
            },
        );
        let value = match serde_json::to_value(&event) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(value["schema"], serde_json::json!("venue.v1"));
    }

    #[test]
    fn test_venue_event_missing_schema_is_a_decode_error() {
        // The schema tag is mandatory: an envelope without it fails to decode.
        let json = r#"{"underlying_sequence":7,"venue_ts":1,"command":{"CancelOrder":{"symbol":"BTC-20240329-50000-C","order_id":"x","account":"a"}},"outcome":{"Cancelled":{"order_id":"x"}}}"#;
        match serde_json::from_str::<VenueEvent>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected a missing-schema decode error, parsed {parsed:?}"),
        }
    }

    #[test]
    fn test_venue_event_rejects_unknown_field() {
        let json = r#"{"schema":"venue.v1","underlying_sequence":7,"venue_ts":1,"command":{"CancelOrder":{"symbol":"BTC-20240329-50000-C","order_id":"x","account":"a"}},"outcome":{"Cancelled":{"order_id":"x"}},"typo":true}"#;
        match serde_json::from_str::<VenueEvent>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected an unknown-field rejection, parsed {parsed:?}"),
        }
    }
}
