//! The acceptor-side FIX session FSM, its logon authentication, the immutable
//! account ↔ CompID binding, heartbeat cadence, checked non-wrapping sequence
//! counters, and session-level resend / `SequenceReset`
//! ([03 §5.2](../../../docs/03-protocol-surfaces.md#52-session-management--the-acceptor-fsm-and-checked-counters),
//! [ADR-0007](../../../docs/adr/0007-fix-credentials-and-account-model.md),
//! [ADR-0010](../../../docs/adr/0010-fix-session-account-binding.md)).
//!
//! ## Why the acceptor owns this
//!
//! IronFix's `Session<S>` typestate models the **initiator** only
//! (`Connecting::send_logon` → `LogonSent::on_logon_ack`), which cannot express
//! *receive a logon, authenticate it, send the ack*, and its `SequenceManager`
//! increments with wrapping `fetch_add`. So the acceptor FSM
//! ([`SessionPhase`]) and the checked counters ([`super::store::SessionCounters`])
//! are new venue work; IronFix supplies framing, codec, and the
//! `MsgType`/`CompId`/`SeqNum` vocabulary only.
//!
//! ## One permission model, no second auth system
//!
//! A `Logon (A)` resolves to the **same** [`AccountId`] and permission set a JWT
//! for that account resolves to ([ADR-0007](../../../docs/adr/0007-fix-credentials-and-account-model.md)):
//! plaintext `Username (553)` / `Password (554)` are verified against the venue
//! account registry's Argon2id hash (a CPU-bound verify run under
//! [`tokio::task::spawn_blocking`] so it can never stall the acceptor's accept
//! loop or graceful drain), then the presented `(SenderCompID, TargetCompID)`
//! tuple is checked against the account's immutable binding before the session
//! reaches [`SessionPhase::Active`]. Session admin needs no permission; trading
//! (`D`/`F`/`G`/`q`) needs `Trade`; market data / status (`V`/`H`) needs `Read`;
//! there is **no FIX `Admin` row** — the control plane is not on FIX.
//!
//! ## Redaction
//!
//! The plaintext `Password (554)` is verified and dropped; it is **never**
//! logged, echoed in a `Text (58)`, or stored except as its Argon2id hash. The
//! venue's own `Logon (A)` ack is hand-built **without** `553`/`554`, so no
//! credential is ever emitted.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use ironfix_core::types::{CompId, SeqNum};

use crate::auth::{AccountStore, RateLimitKey, RateLimitTier, RevocationOracle};
use crate::error::{FixReject, FixRejectContext, FixRejectReason, VenueError};
use crate::exchange::{
    AddOutcome, Cents, FanoutSummary, MassCancelScope, MassCancelType, RejectKind, Symbol,
    SymbolParser, TimeInForce as SeamTif, VenueCommand, VenueOutcome,
};
use crate::gateway::rest::support::{
    immediate_execution_records, mint_order_id, owner_for, taker_legs_for_order,
};
use crate::microstructure::IngressStamp;
use crate::models::{AccountId, ClientOrderId, ExecutionId, Permission, VenueOrderId};
use crate::state::{AppState, SweptLeg};

use super::codec::{FieldWriter, tags};
use super::enums::{
    CxlRejResponseTo, ExecType, MassCancelRequestType, MassCancelResponse, OrdStatus, OrdType,
    OrderSide, SubscriptionRequestType, TimeInForce as FixTif,
};
use super::error::SessionRejectReason;
use super::execution::{
    BusinessMessageReject, ExecutionReport, OrderCancelReject, OrderMassCancelReport,
};
use super::header::{StandardHeader, UtcTimestamp};
use super::marketdata::{
    IncrementalEntry, MarketDataIncrementalRefresh, MarketDataRequest, MarketDataRequestReject,
    MarketDataSnapshotFullRefresh, SnapshotEntry,
};
use super::md_projection::{self, RequestedSides};
use super::order::{
    NewOrderSingle, OrderCancelReplaceRequest, OrderCancelRequest, OrderMassCancelRequest,
    OrderStatusRequest,
};
use super::order_flow::{self, ExecReportSpec};
use super::store::{
    FixSessionStore, ResetTrigger, SequenceResetEvent, SessionCounters, SessionKey, StoredOutbound,
};
use super::{DecodedMessage, FixBody, session};
use crate::exchange::event::{EventTimestamp, SequenceNumber};
use crate::models::WsMessage;
use tokio::sync::broadcast;

use super::acceptor::{FixSession, FixSessionFactory, SessionControl, SessionOutbound};

/// The FIX `MsgType (35)` for the venue-built `Logon (A)` ack.
const MSG_TYPE_LOGON: &str = "A";

/// The `OrdRejReason (103)` the venue emits for a permission-denied order — `0`
/// (`Broker / Exchange option`), the FIX 4.4 catch-all for a venue-policy reject;
/// the redacted `Text (58)` names the cause without leaking a secret. Distinct
/// from [`order_flow::ORD_REJ_REASON_DUPLICATE`] (`6`, `Duplicate Order`) so a
/// compliant client can tell "not authorized" from "duplicate `ClOrdID`" by the
/// reason code alone (`6` is `Duplicate Order` in FIX 4.4, not `Unknown Order`).
const ORD_REJ_REASON_NOT_AUTHORIZED: u16 = 0;

/// The `CxlRejReason (102)` the venue emits for a permission-denied cancel /
/// replace — `6` (`Duplicate ClOrdID` is not it; `2` is broker/exchange option),
/// the generic exchange-option code, with the reason in the redacted `Text (58)`.
const CXL_REJ_REASON_NOT_AUTHORIZED: u16 = 2;

/// A short, **non-secret** reason string for a permission-denied application
/// message — safe to echo in a `Text (58)` (it names a policy, not a credential).
const TEXT_NOT_AUTHORIZED: &str = "insufficient permission";

/// `MDReqRejReason (281) = 1` — Duplicate subscription: a `V` (Subscribe) whose
/// `MDReqID (262)` already backs a live subscription, **or** whose `Symbol (55)` is
/// already subscribed on this session under any `MDReqID`. FIX 4.4 defines `1` as
/// "Duplicate MDReqID"; the venue extends it to the symbol-duplicate case (a
/// re-subscribe of an already-live symbol) because both are the same error class —
/// the client asked for a subscription it already holds — and FIX 4.4 carries no
/// distinct "duplicate symbol subscription" reason. A symbol re-subscribe is
/// rejected whole (never a silent overwrite of the prior [`MdSymbolSub`], which
/// would orphan the earlier `MDReqID`); the redacted `Text (58)` disambiguates the
/// two cases ([fix-dialect §2.3](../../../docs/specs/fix-dialect.md#23-market-data-subscription-surfaces-03-54)).
const MD_REJ_REASON_DUPLICATE: u16 = 1;

/// `MDReqRejReason (281) = 2` — Insufficient bandwidth: the per-session
/// market-data subscription set is at its [`MAX_MD_SYMBOLS_PER_SESSION`] ceiling.
const MD_REJ_REASON_INSUFFICIENT_BANDWIDTH: u16 = 2;

/// `MDReqRejReason (281) = 3` — Insufficient permissions: a `V` on a session that
/// does not hold `Read`. Every authenticated account normally grants `Read`
/// (`Trade`/`Admin` imply it), so this only fires for an empty permission set.
const MD_REJ_REASON_INSUFFICIENT_PERMISSIONS: u16 = 3;

/// `MDReqRejReason (281) = 8` — Unsupported `MDEntryType (269)`: the request
/// carried a Trade entry type (`269 = 2`) — alone (a trade-tape-only `V`) **or**
/// mixed with a book side — which the FIX MD orderbook surface does not serve. The
/// trade tape is **permanently out** of FIX MD (a trade print has no book snapshot
/// and rides its own separate `instrument_sequence`, distinct from the orderbook's
/// `RptSeq (83)`, so `W`/`X` cannot carry it under one `MDReqID`); the whole `V` is
/// rejected, never a silent serve of the book side with the Trade entry type dropped
/// ([fix-dialect §2.3](../../../docs/specs/fix-dialect.md#23-market-data-subscription-surfaces-03-54)).
/// Best-bid/offer "quotes" are the depth-bounded (`MarketDepth (264) = 1`) book
/// projection — the same `269 = 0/1` `W`/`X`, not a separate channel.
const MD_REJ_REASON_UNSUPPORTED_ENTRY_TYPE: u16 = 8;

/// The ceiling on the per-session market-data subscription set — a memory DoS
/// bound so a long-lived session cannot grow an unbounded symbol map (the FIX
/// analogue of the WS `MAX_SUBSCRIPTIONS_PER_CONNECTION`). A `V` that would exceed
/// it is rejected whole with `Y` (`MDReqRejReason = 2`), never partially applied.
const MAX_MD_SYMBOLS_PER_SESSION: usize = 256;

/// The ceiling on market-data frames drained onto the outbound mailbox in one
/// dispatch/tick cycle — bounds the work a single burst can do; the remainder
/// stays buffered on the broadcast for the next cycle (and a slow reader lags and
/// re-snapshots, never stalling the producer).
const MAX_MD_FRAMES_PER_CYCLE: usize = 512;

/// The per-session tuning derived from the validated `[fix]` config section.
#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    /// How long the acceptor waits in [`SessionPhase::AwaitingLogon`] for a
    /// `Logon (A)` before closing the connection.
    pub logon_timeout_ms: u64,
    /// The maximum `HeartBtInt (108)` the venue negotiates; a logon proposing a
    /// larger (or a zero) interval is refused.
    pub max_heart_bt_int_secs: u32,
}

impl SessionConfig {
    /// Maps the validated `[fix]` config onto the per-session tuning.
    #[must_use]
    pub fn from_config(fix: &crate::config::FixConfig) -> Self {
        Self {
            logon_timeout_ms: fix.logon_timeout_secs.saturating_mul(1_000),
            max_heart_bt_int_secs: fix.max_heart_bt_int_secs,
        }
    }
}

/// The acceptor's per-connection session phase
/// ([03 §5.2](../../../docs/03-protocol-surfaces.md#52-session-management--the-acceptor-fsm-and-checked-counters)).
///
/// `Listen` is the acceptor-wide accept loop (#037); a spawned connection starts
/// in [`AwaitingLogon`](Self::AwaitingLogon). `Authenticating` is the transient
/// verify-and-bind computation inside the logon handler (a single `await`), not a
/// resting state, so it is represented while [`handle_logon`](VenueFixSession)
/// runs rather than stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    /// Connection accepted; awaiting the first `Logon (A)`.
    AwaitingLogon,
    /// Credentials verified, tuple bound, permissions resolved — serving.
    Active,
    /// A `MsgSeqNum` gap was detected inbound; a `ResendRequest (2)` was sent and
    /// the gap is being filled.
    AwaitingResend,
    /// A terminal reject / logout / seal was emitted; the connection is closing.
    Closing,
}

/// A failure that seals the session — a checked-counter exhaustion or a durable
/// store failure. Neither can wrap or be ignored: both drive the session to
/// [`SessionPhase::Closing`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    /// A `MsgSeqNum` counter increment would overflow `u64` — the session is
    /// sealed, **never** wrapped ([03 §5.2](../../../docs/03-protocol-surfaces.md#52-session-management--the-acceptor-fsm-and-checked-counters)).
    #[error("fix MsgSeqNum sequence exhausted; sealing session")]
    SequenceExhausted,
    /// The durable session store failed (a bound was hit or a backend error) —
    /// the session closes rather than proceed on unpersisted state.
    #[error("fix session store error: {0}")]
    Store(#[from] super::store::SessionStoreError),
    /// An outbound frame was built before the peer's CompIDs were known — an
    /// internal invariant violation (a reply is only ever emitted after an
    /// inbound message sets the reply identity).
    #[error("fix session has no bound peer to reply to")]
    NoPeer,
}

/// The frames to emit and whether to keep the connection open — one FSM step's
/// output. Returned by the [`SessionFsm`] transition methods so a test can drive
/// the state machine directly (no socket).
#[derive(Debug)]
pub struct Reaction {
    frames: Vec<Vec<u8>>,
    control: SessionControl,
}

impl Reaction {
    /// The complete pre-framed FIX frames this step emits, in order.
    #[must_use]
    #[inline]
    pub fn frames(&self) -> &[Vec<u8>] {
        &self.frames
    }

    /// Whether this step keeps the connection open or closes it.
    #[must_use]
    #[inline]
    pub fn control(&self) -> SessionControl {
        self.control
    }

    /// Continue serving, emitting nothing.
    fn cont() -> Self {
        Self {
            frames: Vec::new(),
            control: SessionControl::Continue,
        }
    }

    /// Close the connection, emitting nothing (used when we cannot address a
    /// reply — e.g. a logon-timeout before any message set the peer identity).
    fn close_silent() -> Self {
        Self {
            frames: Vec::new(),
            control: SessionControl::Close,
        }
    }

    /// Emit `frames` and continue.
    fn emit(frames: Vec<Vec<u8>>) -> Self {
        Self {
            frames,
            control: SessionControl::Continue,
        }
    }

    /// Emit `frames` and close.
    fn emit_close(frames: Vec<Vec<u8>>) -> Self {
        Self {
            frames,
            control: SessionControl::Close,
        }
    }
}

/// The outcome of one in-order [`SessionFsm::handle_active`] step: either a
/// fully-resolved synchronous [`Reaction`] (session admin, gap resend, a
/// permission / attribution reject) or a **permitted, in-order, attributed**
/// order-entry message the async [`VenueFixSession`] must route onto the
/// sequenced order path (`D`/`F`/`G`/`q`/`H`, #039).
///
/// Splitting the synchronous session mechanics (which the FSM owns and unit-tests
/// drive directly) from the async order submission (which awaits the single-writer
/// actor) is what lets the FSM stay socket-free while the order path stays on the
/// same `AppState::submit` seam REST uses. When a message routes, the inbound
/// `MsgSeqNum` counter has already been advanced — the message is consumed.
#[derive(Debug)]
pub enum ActiveDisposition {
    /// A synchronous reply the FSM has already fully built.
    Reacted(Reaction),
    /// A permitted order-entry message to submit onto the sequenced path (boxed —
    /// a [`DecodedMessage`] is large relative to a [`Reaction`]).
    Route(Box<DecodedMessage>),
}

/// The synchronous, testable core state machine — everything except the async
/// credential verify (which the [`VenueFixSession`] wrapper runs under
/// [`tokio::task::spawn_blocking`] before calling [`admit_logon`](Self::admit_logon)).
pub struct SessionFsm {
    phase: SessionPhase,
    config: SessionConfig,
    store: Arc<dyn FixSessionStore>,

    /// The bound account (set at admit).
    account: Option<AccountId>,
    /// The account's permission set (set at admit).
    permissions: Vec<Permission>,
    /// The account revocation epoch observed at logon; a live session is dropped
    /// when the account's current epoch rises above it.
    session_epoch: u64,
    /// The account-keyed session store key (set at admit).
    key: Option<SessionKey>,

    /// The venue's CompID (outbound `SenderCompID (49)`) — the presented
    /// `TargetCompID (56)` of the last inbound message.
    venue_comp: Option<CompId>,
    /// The client's CompID (outbound `TargetCompID (56)`) — the presented
    /// `SenderCompID (49)` of the last inbound message.
    client_comp: Option<CompId>,

    /// The checked inbound/outbound `MsgSeqNum` counters.
    counters: SessionCounters,
    /// The negotiated heartbeat interval, in ms (`0` disables cadence checks).
    heart_bt_int_ms: u64,

    accepted_at_ms: u64,
    last_inbound_ms: u64,
    last_outbound_ms: u64,
    awaiting_test_req_since_ms: Option<u64>,
}

impl SessionFsm {
    /// Builds a fresh FSM for an accepted connection (phase
    /// [`SessionPhase::AwaitingLogon`]).
    #[must_use]
    pub fn new(
        config: SessionConfig,
        store: Arc<dyn FixSessionStore>,
        accepted_at_ms: u64,
    ) -> Self {
        Self {
            phase: SessionPhase::AwaitingLogon,
            config,
            store,
            account: None,
            permissions: Vec::new(),
            session_epoch: 0,
            key: None,
            venue_comp: None,
            client_comp: None,
            counters: SessionCounters::default(),
            heart_bt_int_ms: 0,
            accepted_at_ms,
            last_inbound_ms: accepted_at_ms,
            last_outbound_ms: accepted_at_ms,
            awaiting_test_req_since_ms: None,
        }
    }

    /// The current phase (observability / tests).
    #[must_use]
    #[inline]
    pub fn phase(&self) -> SessionPhase {
        self.phase
    }

    /// The current counters (observability / tests).
    #[must_use]
    #[inline]
    pub fn counters(&self) -> SessionCounters {
        self.counters
    }

    /// The bound account, once admitted (observability / tests).
    #[must_use]
    #[inline]
    pub fn account(&self) -> Option<&AccountId> {
        self.account.as_ref()
    }

    /// Records the reply identity from an inbound message (swapping sender/target)
    /// and marks inbound liveness (clearing any outstanding `TestRequest` wait).
    /// Public so a test can seat the peer identity before [`admit_logon`](Self::admit_logon).
    pub fn on_inbound(&mut self, header: &StandardHeader, now_ms: u64) {
        // Reply to whoever addressed us: venue = the presented TargetCompID,
        // client = the presented SenderCompID.
        self.venue_comp = Some(header.target_comp_id.clone());
        self.client_comp = Some(header.sender_comp_id.clone());
        self.last_inbound_ms = now_ms;
        self.awaiting_test_req_since_ms = None;
    }

    /// Builds one outbound frame at the next sender `MsgSeqNum`, persists it for
    /// resend, advances the checked counter, and returns the bytes.
    ///
    /// # Errors
    ///
    /// [`SessionError::NoPeer`] if no inbound message has set the reply identity;
    /// [`SessionError::SequenceExhausted`] if the outbound counter would overflow;
    /// [`SessionError::Store`] on a durable-store failure.
    fn emit(
        &mut self,
        now_ms: u64,
        build: impl FnOnce(StandardHeader) -> Vec<u8>,
    ) -> Result<Vec<u8>, SessionError> {
        let sender = self.venue_comp.clone().ok_or(SessionError::NoPeer)?;
        let target = self.client_comp.clone().ok_or(SessionError::NoPeer)?;
        let seq = self.counters.next_sender_seq;
        let header = StandardHeader::new(
            sender,
            target,
            SeqNum::new(seq),
            UtcTimestamp::from_epoch_ms(now_ms),
        );
        let frame = build(header);
        // Checked, non-wrapping: an increment past u64::MAX seals the session —
        // computed BEFORE the durable write so an overflow never touches the store.
        let next = seq.checked_add(1).ok_or(SessionError::SequenceExhausted)?;
        if let Some(key) = self.key.clone() {
            // ONE atomic durable op stores the frame at `seq` AND advances the
            // outbound counter to `next` (#149 finding 1B): a crash can no longer
            // leave the frame stored with the counter un-advanced, which would make
            // the next frame REUSE `seq` (a duplicate outbound `MsgSeqNum`, a FIX
            // session-fatal violation). Only the outbound counter is persisted here;
            // the inbound counter's durable advance is deferred to the post-effect
            // persist (finding 1A). The in-memory counter advances only AFTER the
            // store commits, so a store failure leaves in-memory and durable in step.
            self.store
                .store_outbound_and_advance(&key, seq, &frame, next)?;
        }
        self.counters.next_sender_seq = next;
        self.last_outbound_ms = now_ms;
        Ok(frame)
    }

    /// Persists the current counters if the session is bound (a no-op pre-admit).
    fn persist_counters(&self) -> Result<(), SessionError> {
        if let Some(key) = &self.key {
            self.store.save_counters(key, self.counters)?;
        }
        Ok(())
    }

    /// Durably persists the current counters as the **deferred inbound advance** for
    /// a routed order-entry mutation — called by the async router ONLY after the
    /// exchange effect is durably committed (`AppState::submit` returned `Ok`), so the
    /// durable `next_target_seq` becomes permanent exactly when (and only when) the
    /// effect does ([#149](https://github.com/joaquinbejar/fauxchange/issues/149)
    /// finding 1A). Before this point a crash leaves the durable `next_target_seq` at
    /// the routed message's own seq, so the client's resend is re-admitted and
    /// idempotently reprocessed (the merged exchange-side `ClOrdID` guard dedups a
    /// resent order) — never a silently-dropped order.
    pub(crate) fn persist_inbound(&self) -> Result<(), SessionError> {
        self.persist_counters()
    }

    /// Advances the inbound (target) counter for a consumed message **in memory
    /// only**, checked — WITHOUT a durable persist. Used for a routed order-entry
    /// mutation whose durable inbound advance is deferred until its exchange effect
    /// commits (finding 1A): gap detection of subsequent inbound frames within the
    /// live session reads the advanced in-memory counter, while the durable persist
    /// waits for [`persist_inbound`](Self::persist_inbound). Mirrors
    /// [`consume_inbound`](Self::consume_inbound)'s phase transition.
    fn advance_inbound_in_memory(&mut self) -> Result<(), SessionError> {
        let next = self
            .counters
            .next_target_seq
            .checked_add(1)
            .ok_or(SessionError::SequenceExhausted)?;
        self.counters.next_target_seq = next;
        if self.phase == SessionPhase::AwaitingResend {
            self.phase = SessionPhase::Active;
        }
        Ok(())
    }

    /// Advances the inbound (target) counter for a consumed message, checked, and
    /// persists it durably (the immediate path for synchronously-handled messages —
    /// session admin, permission / attribution rejects, and reads that mutate nothing
    /// durable).
    fn advance_inbound(&mut self) -> Result<(), SessionError> {
        let next = self
            .counters
            .next_target_seq
            .checked_add(1)
            .ok_or(SessionError::SequenceExhausted)?;
        self.counters.next_target_seq = next;
        self.persist_counters()
    }

    /// A redacted `Logout (5)` frame followed by a close — the terminal reject for
    /// a logon or session-integrity failure. The `Text (58)` names only a policy
    /// reason, never a credential.
    fn logout_close(
        &mut self,
        now_ms: u64,
        reason: &'static str,
    ) -> Result<Reaction, SessionError> {
        self.phase = SessionPhase::Closing;
        let text = reason.to_string();
        let frame = self.emit(now_ms, |header| {
            session::Logout {
                header,
                text: Some(text),
            }
            .encode()
        })?;
        Ok(Reaction::emit_close(vec![frame]))
    }

    /// Admits a verified, bound logon: resolves the session key, **validates the
    /// presented `MsgSeqNum` against the durable inbound expectation**, resumes (or
    /// — only under `ResetSeqNumFlag=Y` — resets) the counters, moves to
    /// [`SessionPhase::Active`] (or [`SessionPhase::AwaitingResend`] on a gap), and
    /// emits the credential-free `Logon (A)` ack.
    ///
    /// Reconnect sequence validation (the auth/replay-integrity guard) compares the
    /// logon's `logon_seq` to the stored `next_target_seq`: **equal** proceeds
    /// in-order; **greater** admits and issues a `ResendRequest (2)` for the missing
    /// range (leaving the counter in place); **less** (without `ResetSeqNumFlag`) is
    /// a backward jump that would replay already-consumed messages and is rejected
    /// with a `Logout`. The stored counter is **never** silently overwritten
    /// downward — only `ResetSeqNumFlag=Y` moves it back, and that path journals a
    /// `SequenceReset` audit event.
    ///
    /// # Errors
    ///
    /// [`SessionError`] on a store failure or a counter exhaustion during the ack.
    #[allow(clippy::too_many_arguments)]
    pub fn admit_logon(
        &mut self,
        account: AccountId,
        permissions: Vec<Permission>,
        session_epoch: u64,
        heart_bt_int_secs: u32,
        reset_flag: bool,
        logon_seq: u64,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let sender = self.venue_comp.clone().ok_or(SessionError::NoPeer)?;
        let target = self.client_comp.clone().ok_or(SessionError::NoPeer)?;
        // The key is the authenticated account plus its bound tuple (as presented
        // inbound): client SenderCompID (49) = our `client_comp`, venue
        // TargetCompID (56) = our `venue_comp`. ADR-0010 rule 2.
        let key = SessionKey::new(
            account.clone(),
            target.as_str().to_string(),
            sender.as_str().to_string(),
        );

        // The durable inbound expectation a reconnect resumes from — read BEFORE any
        // session state is bound, so a backward-jump reject leaves the store (and its
        // counters) provably untouched.
        let stored = self.store.load_counters(&key)?;

        // Reconnect sequence validation (the auth/replay-integrity guard): a
        // NON-reset logon presenting a `MsgSeqNum` BELOW the stored inbound
        // expectation is a backward jump that would replay already-consumed
        // messages. Reject it with a `Logout` and NEVER overwrite the stored counter
        // downward — only `ResetSeqNumFlag=Y` may move it back (audited, below). The
        // check runs before the identity is bound, so nothing is persisted for the
        // rejected logon (`logout_close` emits at the default outbound seq and does
        // not touch the durable store, as `self.key` is still `None`).
        if !reset_flag && logon_seq < stored.next_target_seq {
            tracing::warn!(
                expected = stored.next_target_seq,
                presented = logon_seq,
                "fix reconnect MsgSeqNum below the stored expectation without ResetSeqNumFlag; rejecting"
            );
            return self.logout_close(now_ms, "MsgSeqNum too low");
        }

        self.account = Some(account);
        self.permissions = permissions;
        self.session_epoch = session_epoch;
        self.heart_bt_int_ms = u64::from(heart_bt_int_secs).saturating_mul(1_000);
        self.key = Some(key.clone());

        // Resolve the inbound counter and whether a resend gap must be filled.
        let mut counters = stored;
        let mut gap_begin: Option<u64> = None;
        if reset_flag {
            // `ResetSeqNumFlag=Y` — the ONLY path that may move the counter backward.
            // It journals a `SequenceReset` audit event within THIS account key only.
            let event = SequenceResetEvent {
                at_ms: now_ms,
                trigger: ResetTrigger::LogonReset,
                old_next_sender_seq: counters.next_sender_seq,
                old_next_target_seq: counters.next_target_seq,
                new_next_sender_seq: super::store::FIRST_SEQ_NUM,
                new_next_target_seq: super::store::FIRST_SEQ_NUM,
            };
            counters = SessionCounters::default();
            self.store.record_reset(&key, event, counters)?;
            // The reset logon itself consumed inbound seq 1.
            counters.next_target_seq = super::store::FIRST_SEQ_NUM
                .checked_add(1)
                .ok_or(SessionError::SequenceExhausted)?;
        } else if logon_seq == counters.next_target_seq {
            // In-order reconnect / first logon: the logon consumed the expected
            // inbound seq, so advance the expectation past it.
            counters.next_target_seq = logon_seq
                .checked_add(1)
                .ok_or(SessionError::SequenceExhausted)?;
        } else {
            // `logon_seq > next_target_seq`: a gap (missed inbound messages, NOT a
            // replay). Admit and ack, but leave the stored inbound expectation in
            // place and request a resend of `[next_target_seq, ∞)` — the same
            // machinery `handle_active` drives. The counter is NEVER advanced past
            // the gap (that would silently drop the missing messages).
            gap_begin = Some(counters.next_target_seq);
        }
        self.counters = counters;
        self.store.save_counters(&key, self.counters)?;
        self.phase = if gap_begin.is_some() {
            SessionPhase::AwaitingResend
        } else {
            SessionPhase::Active
        };

        // The venue's Logon ack is hand-built WITHOUT Username(553)/Password(554):
        // an acceptor never echoes the client's credential onto the wire. On a gap,
        // a `ResendRequest (2)` for the missing range follows the ack, on the same
        // checked outbound counter.
        let ack = self.emit(now_ms, |header| {
            encode_logon_ack(&header, heart_bt_int_secs, reset_flag)
        })?;
        let mut frames = vec![ack];
        if let Some(begin) = gap_begin {
            let resend = self.emit(now_ms, |header| {
                session::ResendRequest {
                    header,
                    begin_seq_no: SeqNum::new(begin),
                    end_seq_no: SeqNum::new(0),
                }
                .encode()
            })?;
            frames.push(resend);
        }
        Ok(Reaction::emit(frames))
    }

    /// Handles a decoded message once [`Active`](SessionPhase::Active) — sequence
    /// validation, revocation, the permission gate, `Account (1)` enforcement, and
    /// the session-admin replies.
    ///
    /// `revoked` is the caller's per-message revocation read (the FSM stays free of
    /// the registry).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure (the session seals).
    pub fn handle_active(
        &mut self,
        message: DecodedMessage,
        now_ms: u64,
        revoked: bool,
    ) -> Result<ActiveDisposition, SessionError> {
        self.on_inbound(header_of(&message), now_ms);

        // Per-message revocation: a revoke bumps the epoch and drops the session.
        if revoked {
            return Ok(ActiveDisposition::Reacted(
                self.logout_close(now_ms, "account revoked")?,
            ));
        }

        let seq = header_of(&message).msg_seq_num.value();

        // `SequenceReset (4)` is processed regardless of a gap (it repairs one).
        if let DecodedMessage::SequenceReset(reset) = &message {
            return Ok(ActiveDisposition::Reacted(
                self.handle_sequence_reset(reset, now_ms)?,
            ));
        }

        // Sequence-gap detection on the inbound stream.
        match seq.cmp(&self.counters.next_target_seq) {
            std::cmp::Ordering::Greater => {
                // A gap: request a resend of [expected, ∞) and await the fill. The
                // body is NOT processed and the counter is NOT advanced.
                self.phase = SessionPhase::AwaitingResend;
                let begin = self.counters.next_target_seq;
                let frame = self.emit(now_ms, |header| {
                    session::ResendRequest {
                        header,
                        begin_seq_no: SeqNum::new(begin),
                        end_seq_no: SeqNum::new(0),
                    }
                    .encode()
                })?;
                return Ok(ActiveDisposition::Reacted(Reaction::emit(vec![frame])));
            }
            std::cmp::Ordering::Less => {
                // Already seen (a duplicate / too-low): ignore, do not advance.
                tracing::debug!(
                    expected = self.counters.next_target_seq,
                    got = seq,
                    "fix inbound MsgSeqNum below expected; ignoring"
                );
                return Ok(ActiveDisposition::Reacted(Reaction::cont()));
            }
            std::cmp::Ordering::Equal => {}
        }

        // An in-order application message is gated (permission + `Account (1)`
        // attribution) here; a permitted order-entry message is then handed to the
        // async order router (#039), and market data (`V`) is admitted for #040.
        // Everything else is a synchronous session-admin reply.
        if is_application_message(&message) {
            return self.gate_application(message, now_ms);
        }

        let reaction = self.dispatch_active_body(message, now_ms)?;
        self.consume_inbound()?;
        Ok(ActiveDisposition::Reacted(reaction))
    }

    /// Advances the checked inbound counter for a consumed in-order message and
    /// leaves [`AwaitingResend`](SessionPhase::AwaitingResend) once the gap is
    /// filled.
    fn consume_inbound(&mut self) -> Result<(), SessionError> {
        self.advance_inbound()?;
        if self.phase == SessionPhase::AwaitingResend {
            self.phase = SessionPhase::Active;
        }
        Ok(())
    }

    /// The per-message permission gate + `Account (1)` attribution for an in-order
    /// application message, then either a synchronous reject / market-data
    /// admission or a [`Route`](ActiveDisposition::Route) onto the order path.
    fn gate_application(
        &mut self,
        message: DecodedMessage,
        now_ms: u64,
    ) -> Result<ActiveDisposition, SessionError> {
        let required = required_permission(&message);
        if !self.has_permission(required) {
            // Refuse in the message's own context (order-level), never a bare
            // Reject (3) — ADR-0007 §2, 03 §8.
            let reaction = self.permission_reject(&message, now_ms)?;
            self.consume_inbound()?;
            return Ok(ActiveDisposition::Reacted(reaction));
        }

        // `Account (1)` must be absent or equal to the authenticated account
        // (ADR-0010 rule 4). Only `NewOrderSingle (D)` fields tag 1 in the v0.4
        // dialect (F/G/q/H do not parse it), so the check is D-scoped — a mismatch
        // is a session-level `Reject (3)` (no delegation).
        if let DecodedMessage::NewOrderSingle(order) = &message
            && let Some(named) = &order.account
            && Some(named) != self.account.as_ref()
        {
            let reaction = self.session_reject(
                now_ms,
                SessionRejectReason::ValueIsIncorrect,
                Some(tags::ACCOUNT),
            )?;
            self.consume_inbound()?;
            return Ok(ActiveDisposition::Reacted(reaction));
        }

        // Consume the inbound seq for the routed message. For an order-entry
        // MUTATION (D/F/G/q) the DURABLE inbound advance is DEFERRED (#149 finding
        // 1A): advance the in-memory counter now (so gap detection of subsequent
        // frames is correct within the live session), but persist it durably only
        // AFTER the async route's `submit` durably commits the exchange effect, via
        // [`persist_inbound`](Self::persist_inbound). A crash before that leaves the
        // durable `next_target_seq` at this message's seq, so the client's resend is
        // re-admitted and idempotently reprocessed — never a silently-dropped order.
        // A READ (`H`/`V`) mutates nothing durable, so it consumes (advances +
        // persists) immediately, exactly like a synchronous message.
        if is_order_entry_mutation(&message) {
            self.advance_inbound_in_memory()?;
        } else {
            self.consume_inbound()?;
        }

        // D/F/G/q/H → the async order router (#039); `V` → the async market-data
        // router (#040). Both need `AppState` (the sequenced order path / the shared
        // subscription manager), which the socket-free FSM does not hold, so both
        // are routed to the async [`VenueFixSession`].
        Ok(ActiveDisposition::Route(Box::new(message)))
    }

    /// Routes an in-order session-admin (or defensively, an unexpected
    /// venue-out / application) message body. Application order-entry and market
    /// data are gated and routed in [`gate_application`](Self::gate_application)
    /// before this is reached, so those arms here are defensive.
    fn dispatch_active_body(
        &mut self,
        message: DecodedMessage,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        match message {
            // `SequenceReset (4)` is intercepted in `handle_active` before this
            // dispatch; this arm is defensive (never reached).
            DecodedMessage::SequenceReset(_)
            | DecodedMessage::Heartbeat(_)
            | DecodedMessage::Reject(_) => Ok(Reaction::cont()),
            DecodedMessage::TestRequest(test) => {
                let id = test.test_req_id;
                let frame = self.emit(now_ms, |header| {
                    session::Heartbeat {
                        header,
                        test_req_id: Some(id),
                    }
                    .encode()
                })?;
                Ok(Reaction::emit(vec![frame]))
            }
            DecodedMessage::ResendRequest(resend) => self.serve_resend(&resend, now_ms),
            DecodedMessage::Logout(_) => {
                // A client logout: reply with a Logout and close.
                self.phase = SessionPhase::Closing;
                let frame = self.emit(now_ms, |header| {
                    session::Logout { header, text: None }.encode()
                })?;
                Ok(Reaction::emit_close(vec![frame]))
            }
            DecodedMessage::Logon(_) => {
                // A second logon on a live session is a protocol violation.
                self.session_reject(now_ms, SessionRejectReason::InvalidMsgType, Some(35))
            }
            // Application order-entry / market data are gated + routed before this
            // dispatch (defensive; never reached), and venue-out messages must
            // never arrive inbound — both are a session-level protocol violation.
            DecodedMessage::NewOrderSingle(_)
            | DecodedMessage::OrderCancelRequest(_)
            | DecodedMessage::OrderCancelReplaceRequest(_)
            | DecodedMessage::OrderMassCancelRequest(_)
            | DecodedMessage::OrderStatusRequest(_)
            | DecodedMessage::MarketDataRequest(_)
            | DecodedMessage::ExecutionReport(_)
            | DecodedMessage::OrderCancelReject(_)
            | DecodedMessage::OrderMassCancelReport(_)
            | DecodedMessage::BusinessMessageReject(_)
            | DecodedMessage::MarketDataSnapshotFullRefresh(_)
            | DecodedMessage::MarketDataIncrementalRefresh(_)
            | DecodedMessage::MarketDataRequestReject(_) => {
                self.session_reject(now_ms, SessionRejectReason::InvalidMsgType, Some(35))
            }
        }
    }

    /// Builds the order-context reject for a permission-denied application message.
    fn permission_reject(
        &mut self,
        message: &DecodedMessage,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        match message {
            DecodedMessage::NewOrderSingle(order) => {
                let symbol = order.symbol.clone();
                let side = order.side;
                let seq = self.counters.next_sender_seq;
                let frame = self.emit(now_ms, |header| {
                    ExecutionReport {
                        header,
                        order_id: VenueOrderId::new("NONE"),
                        exec_id: ExecutionId::new(format!("REJECT:{seq}")),
                        exec_type: ExecType::Rejected,
                        ord_status: OrdStatus::Rejected,
                        symbol,
                        side,
                        leaves_qty: 0,
                        cum_qty: 0,
                        last_qty: None,
                        last_px: None,
                        price: order.price,
                        secondary_exec_id: SequenceNumber::new(0),
                        transact_time: UtcTimestamp::from_epoch_ms(now_ms),
                        commission: None,
                        comm_type: None,
                        last_liquidity_ind: None,
                        ord_rej_reason: Some(ORD_REJ_REASON_NOT_AUTHORIZED),
                        text: Some(TEXT_NOT_AUTHORIZED.to_string()),
                    }
                    .encode()
                })?;
                Ok(Reaction::emit(vec![frame]))
            }
            DecodedMessage::OrderCancelRequest(cancel) => self.cancel_reject(
                cancel.orig_cl_ord_id.clone(),
                cancel.cl_ord_id.clone(),
                CxlRejResponseTo::OrderCancelRequest,
                now_ms,
            ),
            DecodedMessage::OrderCancelReplaceRequest(replace) => self.cancel_reject(
                replace.orig_cl_ord_id.clone(),
                replace.cl_ord_id.clone(),
                CxlRejResponseTo::OrderCancelReplaceRequest,
                now_ms,
            ),
            DecodedMessage::OrderMassCancelRequest(_) => {
                let frame = self.emit(now_ms, |header| {
                    OrderMassCancelReport {
                        header,
                        mass_cancel_response: MassCancelResponse::Rejected,
                        total_affected_orders: 0,
                        affected_orders: Vec::new(),
                    }
                    .encode()
                })?;
                Ok(Reaction::emit(vec![frame]))
            }
            // `V` requires `Read`; a session without it (an empty permission set)
            // is refused in the market-data context — a `Y`, never a silent drop or
            // a bare `Reject (3)` (03 §8).
            DecodedMessage::MarketDataRequest(request) => self.emit_md_request_reject(
                request.md_req_id.clone(),
                MD_REJ_REASON_INSUFFICIENT_PERMISSIONS,
                Some(TEXT_NOT_AUTHORIZED.to_string()),
                now_ms,
            ),
            // `H` requires only `Read`; a denied read has no order context to answer
            // and is not order-entry, so it continues without a frame.
            _ => Ok(Reaction::cont()),
        }
    }

    /// Builds an [`OrderCancelReject (9)`](OrderCancelReject) for a permission
    /// denial on `F`/`G`.
    fn cancel_reject(
        &mut self,
        orig_cl_ord_id: crate::models::ClientOrderId,
        cl_ord_id: crate::models::ClientOrderId,
        response_to: CxlRejResponseTo,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let frame = self.emit(now_ms, |header| {
            OrderCancelReject {
                header,
                order_id: VenueOrderId::new("NONE"),
                cl_ord_id,
                orig_cl_ord_id,
                ord_status: OrdStatus::Rejected,
                cxl_rej_response_to: response_to,
                cxl_rej_reason: CXL_REJ_REASON_NOT_AUTHORIZED,
                text: Some(TEXT_NOT_AUTHORIZED.to_string()),
            }
            .encode()
        })?;
        Ok(Reaction::emit(vec![frame]))
    }

    /// Emits a committed [`ExecReportSpec`] stream as sequenced, resend-persisted
    /// `ExecutionReport (8)` frames — the render side of the order path (#039).
    ///
    /// Each report is stamped with the next checked sender `MsgSeqNum` and stored
    /// for resend exactly like every other venue-originated frame.
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure while emitting.
    pub(crate) fn emit_report_specs(
        &mut self,
        specs: Vec<ExecReportSpec>,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let mut frames = Vec::with_capacity(specs.len());
        for spec in specs {
            let frame = self.emit(now_ms, |header| spec.into_report(header).encode())?;
            frames.push(frame);
        }
        Ok(Reaction::emit(frames))
    }

    /// Emits one sequenced, resend-persisted `MarketDataSnapshotFullRefresh (W)` —
    /// the `orderbook_snapshot` twin (#040). `rpt_seq` is the per-instrument
    /// `instrument_sequence` (a **distinct** namespace from the frame's own
    /// `MsgSeqNum (34)`, which `emit` stamps and stores for session resend).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure while emitting.
    pub(crate) fn emit_md_snapshot(
        &mut self,
        md_req_id: String,
        symbol: Symbol,
        rpt_seq: SequenceNumber,
        entries: Vec<SnapshotEntry>,
        now_ms: u64,
    ) -> Result<Vec<u8>, SessionError> {
        self.emit(now_ms, |header| {
            MarketDataSnapshotFullRefresh {
                header,
                md_req_id,
                symbol,
                rpt_seq,
                entries,
            }
            .encode()
        })
    }

    /// Emits one sequenced, resend-persisted `MarketDataIncrementalRefresh (X)` —
    /// the `orderbook_delta` twin (#040), carrying the same per-instrument
    /// `instrument_sequence` as `rpt_seq`.
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure while emitting.
    pub(crate) fn emit_md_incremental(
        &mut self,
        md_req_id: String,
        rpt_seq: SequenceNumber,
        entries: Vec<IncrementalEntry>,
        now_ms: u64,
    ) -> Result<Vec<u8>, SessionError> {
        self.emit(now_ms, |header| {
            MarketDataIncrementalRefresh {
                header,
                md_req_id,
                rpt_seq,
                entries,
            }
            .encode()
        })
    }

    /// Emits a `MarketDataRequestReject (Y)` for an unsupported/invalid
    /// `MarketDataRequest (V)` — the market-data-context reject (never a bare
    /// `Reject (3)`, 03 §8). `Text (58)` names a policy, never a secret.
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure while emitting.
    pub(crate) fn emit_md_request_reject(
        &mut self,
        md_req_id: String,
        reason: u16,
        text: Option<String>,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let frame = self.emit(now_ms, |header| {
            MarketDataRequestReject {
                header,
                md_req_id,
                md_req_rej_reason: reason,
                text,
            }
            .encode()
        })?;
        Ok(Reaction::emit(vec![frame]))
    }

    /// Emits an order-context `ExecutionReport (8)` `Rejected` for a runtime
    /// [`VenueError`] on a `NewOrderSingle (D)` / `OrderStatusRequest (H)` — the
    /// reject **message** is fixed by the `NewOrder` context, the numeric
    /// `OrdRejReason (103)` comes from the error's reason category, and the
    /// `Text (58)` is redacted (03 §8).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure.
    pub(crate) fn emit_order_rejected(
        &mut self,
        symbol: Symbol,
        side: OrderSide,
        price: Option<Cents>,
        reject: &FixReject,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        self.emit_order_rejected_code(
            symbol,
            side,
            price,
            order_flow::ord_rej_reason(reject.reason),
            reject.text.clone(),
            now_ms,
        )
    }

    /// Emits an order-context `ExecutionReport (8)` `Rejected` with an **explicit**
    /// `OrdRejReason (103)` code — the primitive [`emit_order_rejected`](Self::emit_order_rejected)
    /// delegates to, and the seam the gateway uses for a reject whose reason is not
    /// carried by a [`FixReject`] (a conflicting-`ClOrdID` reuse → `Duplicate Order`).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure.
    pub(crate) fn emit_order_rejected_code(
        &mut self,
        symbol: Symbol,
        side: OrderSide,
        price: Option<Cents>,
        ord_rej_reason: u16,
        text: Option<String>,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        // A rejected order was never sequenced, so there is no venue order id or
        // execution id; the ExecID is a session-local marker at the reply seq.
        let seq = self.counters.next_sender_seq;
        let frame = self.emit(now_ms, |header| {
            ExecutionReport {
                header,
                order_id: VenueOrderId::new("NONE"),
                exec_id: ExecutionId::new(format!("REJECT:{seq}")),
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                symbol,
                side,
                leaves_qty: 0,
                cum_qty: 0,
                last_qty: None,
                last_px: None,
                price,
                secondary_exec_id: SequenceNumber::new(0),
                transact_time: UtcTimestamp::from_epoch_ms(now_ms),
                commission: None,
                comm_type: None,
                last_liquidity_ind: None,
                ord_rej_reason: Some(ord_rej_reason),
                text,
            }
            .encode()
        })?;
        Ok(Reaction::emit(vec![frame]))
    }

    /// Emits an `OrderCancelReject (9)` for a runtime [`VenueError`] on an
    /// `OrderCancelRequest (F)` / `OrderCancelReplaceRequest (G)` — the numeric
    /// `CxlRejReason (102)` comes from the error's reason category and
    /// `CxlRejResponseTo (434)` from the request kind (03 §8).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn emit_cancel_reject_error(
        &mut self,
        order_id: VenueOrderId,
        orig_cl_ord_id: ClientOrderId,
        cl_ord_id: ClientOrderId,
        response_to: CxlRejResponseTo,
        ord_status: OrdStatus,
        reject: &FixReject,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let reason = order_flow::cxl_rej_reason(reject.reason);
        let text = reject.text.clone();
        let frame = self.emit(now_ms, |header| {
            OrderCancelReject {
                header,
                order_id,
                cl_ord_id,
                orig_cl_ord_id,
                ord_status,
                cxl_rej_response_to: response_to,
                cxl_rej_reason: reason,
                text,
            }
            .encode()
        })?;
        Ok(Reaction::emit(vec![frame]))
    }

    /// Emits an `OrderMassCancelReport (r)` `Rejected` — the honest response to an
    /// `OrderMassCancelRequest (q)` the venue refused (a rate-limited caller, an
    /// unresolved owner, or a sweep every targeted underlying rejected). It carries
    /// no affected orders, so it never discloses any account's book (#97).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure.
    pub(crate) fn emit_mass_cancel_rejected(
        &mut self,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let frame = self.emit(now_ms, |header| {
            OrderMassCancelReport {
                header,
                mass_cancel_response: MassCancelResponse::Rejected,
                total_affected_orders: 0,
                affected_orders: Vec::new(),
            }
            .encode()
        })?;
        Ok(Reaction::emit(vec![frame]))
    }

    /// Emits an accepted `OrderMassCancelReport (r)` (echoing the request scope in
    /// `MassCancelResponse (531)`) carrying the full swept `affected_orders` set,
    /// then one sequenced, resend-persisted `ExecutionReport (8) Canceled` per
    /// `spec` — the render side of a committed
    /// `OrderMassCancelRequest (q)` ([03 §5.3](../../../docs/03-protocol-surfaces.md#53-order-entry-and-execution-reports)).
    ///
    /// The `r` is emitted **first** (the acknowledgement), then the per-order
    /// reports; every frame is stamped with the next checked sender `MsgSeqNum` and
    /// stored for resend exactly like every other venue-originated frame.
    /// `total_affected_orders` is the length of the swept set — the honest count of
    /// every order cancelled, independent of how many the session could render an
    /// `8` for.
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure while emitting.
    pub(crate) fn emit_mass_cancel_accepted(
        &mut self,
        response: MassCancelResponse,
        affected_orders: Vec<VenueOrderId>,
        specs: Vec<ExecReportSpec>,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        // Checked (rule 9/13): the advertised `TotalAffectedOrders (533)` MUST agree
        // with the encoded `affected_orders` list, so a count that will not fit the
        // FIX `u32` field is a propagated typed error (sealing the session) — NEVER
        // a silent `u32::MAX` clamp that would disagree with the list. Unreachable in
        // practice (the swept set is bounded by resting orders), a fail-safe.
        let total_affected_orders =
            u32::try_from(affected_orders.len()).map_err(|_| SessionError::SequenceExhausted)?;
        let report = self.emit(now_ms, |header| {
            OrderMassCancelReport {
                header,
                mass_cancel_response: response,
                total_affected_orders,
                affected_orders,
            }
            .encode()
        })?;
        // Capacity is a hint only (not correctness): a `checked_add` that would
        // overflow falls back to `specs.len()`, which never implies a wrong count —
        // the authoritative `total_affected_orders` above is separately checked.
        let mut frames = Vec::with_capacity(specs.len().checked_add(1).unwrap_or(specs.len()));
        frames.push(report);
        for spec in specs {
            let frame = self.emit(now_ms, |header| spec.into_report(header).encode())?;
            frames.push(frame);
        }
        Ok(Reaction::emit(frames))
    }

    /// Emits a `BusinessMessageReject (j)` for a well-formed application message
    /// the venue cannot business-process (an unsupported application `MsgType`).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure.
    pub(crate) fn emit_business_reject(
        &mut self,
        ref_seq_num: u64,
        ref_msg_type: String,
        business_reject_reason: u16,
        text: Option<String>,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let frame = self.emit(now_ms, |header| {
            BusinessMessageReject {
                header,
                ref_seq_num: SequenceNumber::new(ref_seq_num),
                ref_msg_type,
                business_reject_reason,
                text,
            }
            .encode()
        })?;
        Ok(Reaction::emit(vec![frame]))
    }

    /// Serves a client `ResendRequest (2)` by replaying the durable outbound log
    /// for `[BeginSeqNo, EndSeqNo]` (an `EndSeqNo` of `0` means "to the latest"),
    /// gap-filling any `MsgSeqNum` the bounded log has evicted with a
    /// `SequenceReset (4)`/`GapFillFlag=Y`.
    ///
    /// This is **session-level** resend only — a market-data `RptSeq` gap is
    /// repaired by a fresh `MarketDataRequest (V)`, never here ([03 §5.4](../../../docs/03-protocol-surfaces.md#54-market-data)).
    /// The replayed frames are the original bytes at their original `MsgSeqNum`
    /// and do **not** advance the sender counter or re-enter the store.
    fn serve_resend(
        &mut self,
        resend: &session::ResendRequest,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let begin = resend.begin_seq_no.value();
        let end = resend.end_seq_no.value();
        let key = self.key.clone().ok_or(SessionError::NoPeer)?;
        let stored = self.store.outbound_range(&key, begin, end)?;
        // Clamp the served range to what the venue has actually sent. A resend
        // can never cover a seq the acceptor never emitted (`EndSeqNo=0` means
        // "to the last sent"), so `upper` is capped at `last_sent = next_sender
        // - 1`. This is both correct FIX semantics AND the DoS ceiling: a hostile
        // `EndSeqNo` (up to `u64::MAX`) can no longer size the loop, which is
        // fully synchronous — there is no `.await` here, so the `MAX_DISPATCH`
        // timeout guarding dispatch cannot preempt a spin. The `.max(FIRST_SEQ_NUM)`
        // keeps the `- 1` underflow-free without a `saturating_*` on a sequence.
        // A `begin > upper` request then does zero iterations (a correct no-op).
        let last_sent = self
            .counters
            .next_sender_seq
            .max(super::store::FIRST_SEQ_NUM)
            - 1;
        let upper = if end == 0 {
            last_sent
        } else {
            end.min(last_sent)
        };

        let mut frames = Vec::new();
        let mut seq = begin;
        let mut cursor = stored.into_iter().peekable();
        while seq <= upper {
            let present: Option<StoredOutbound> = match cursor.peek() {
                Some(entry) if entry.seq == seq => cursor.next(),
                _ => None,
            };
            if let Some(entry) = present {
                // Replay the original frame verbatim (its MsgSeqNum is already
                // `seq`). NOTE: standard FIX stamps PossDupFlag=Y on a resend; the
                // #036 vocabulary does not model it, so the venue replays the
                // original bytes (a documented test-venue simplification).
                frames.push(entry.frame);
                seq = seq.checked_add(1).ok_or(SessionError::SequenceExhausted)?;
            } else {
                // A gap the bounded log cannot serve → one GapFill spanning to the
                // next stored entry (or one past `upper` when the cursor is spent).
                // Computed in O(1) from the sorted cursor, never walked one seq at
                // a time — so the work is O(stored entries), not O(requested range),
                // even for a legitimately large `last_sent` with an evicted prefix.
                let gap_end = match cursor.peek() {
                    Some(entry) if entry.seq <= upper => entry.seq,
                    _ => upper
                        .checked_add(1)
                        .ok_or(SessionError::SequenceExhausted)?,
                };
                frames.push(self.gap_fill_frame(seq, gap_end, now_ms)?);
                seq = gap_end;
            }
        }
        Ok(Reaction::emit(frames))
    }

    /// A `SequenceReset (4)`/`GapFillFlag=Y` frame at `at_seq` announcing the
    /// client should jump its inbound expectation to `new_seq_no`. Built with an
    /// explicit historical `MsgSeqNum` (a resend does not consume a new seq or
    /// re-enter the store).
    fn gap_fill_frame(
        &self,
        at_seq: u64,
        new_seq_no: u64,
        now_ms: u64,
    ) -> Result<Vec<u8>, SessionError> {
        let sender = self.venue_comp.clone().ok_or(SessionError::NoPeer)?;
        let target = self.client_comp.clone().ok_or(SessionError::NoPeer)?;
        let header = StandardHeader::new(
            sender,
            target,
            SeqNum::new(at_seq),
            UtcTimestamp::from_epoch_ms(now_ms),
        );
        Ok(session::SequenceReset {
            header,
            new_seq_no: SeqNum::new(new_seq_no),
            gap_fill_flag: Some(true),
        }
        .encode())
    }

    /// Applies an inbound `SequenceReset (4)`: a `GapFillFlag=Y` advances the
    /// inbound expectation past a resend gap; an administrative reset
    /// (`GapFillFlag` absent/`N`) sets it and is journaled as a `SequenceReset`
    /// session event **within the bound account only** ([ADR-0010 §5](../../../docs/adr/0010-fix-session-account-binding.md)).
    fn handle_sequence_reset(
        &mut self,
        reset: &session::SequenceReset,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let new_seq = reset.new_seq_no.value();
        let key = self.key.clone().ok_or(SessionError::NoPeer)?;

        if reset.gap_fill_flag == Some(true) {
            // Gap fill: only ever advances the inbound expectation forward.
            if new_seq >= self.counters.next_target_seq {
                self.counters.next_target_seq = new_seq;
                self.store.save_counters(&key, self.counters)?;
                if self.phase == SessionPhase::AwaitingResend {
                    self.phase = SessionPhase::Active;
                }
            }
            return Ok(Reaction::cont());
        }

        // Administrative reset (scoped to this account key, journaled).
        let event = SequenceResetEvent {
            at_ms: now_ms,
            trigger: ResetTrigger::SequenceReset,
            old_next_sender_seq: self.counters.next_sender_seq,
            old_next_target_seq: self.counters.next_target_seq,
            new_next_sender_seq: self.counters.next_sender_seq,
            new_next_target_seq: new_seq,
        };
        self.counters.next_target_seq = new_seq;
        self.store.record_reset(&key, event, self.counters)?;
        if self.phase == SessionPhase::AwaitingResend {
            self.phase = SessionPhase::Active;
        }
        Ok(Reaction::cont())
    }

    /// A session-level `Reject (3)` at the current expected inbound seq.
    fn session_reject(
        &mut self,
        now_ms: u64,
        reason: SessionRejectReason,
        ref_tag: Option<u16>,
    ) -> Result<Reaction, SessionError> {
        let ref_seq = SeqNum::new(self.counters.next_target_seq);
        let frame = self.emit(now_ms, |header| {
            session::Reject {
                header,
                ref_seq_num: ref_seq,
                session_reject_reason: Some(reason),
                ref_tag_id: ref_tag,
                // No Text: never echo untrusted inbound bytes into a reject.
                text: None,
            }
            .encode()
        })?;
        Ok(Reaction::emit(vec![frame]))
    }

    /// A session-level `Reject (3)` for a post-framing decode failure, classified
    /// by the error's own reject route. Does not advance the inbound counter (the
    /// message was malformed).
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure.
    pub fn reject_decode_error(
        &mut self,
        error: &super::FixDecodeError,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        use super::FixRejectRoute;
        match error.reject_route() {
            FixRejectRoute::SessionReject { reason, ref_tag } => {
                self.session_reject(now_ms, reason, ref_tag)
            }
            // A well-formed application `MsgType` the venue has no handler for →
            // `BusinessMessageReject (j)` (never a bare `Reject (3)` for an
            // application message, 03 §8). `RefMsgType (372)` is the unsupported
            // type the decode error carries; `RefSeqNum (45)` is the expected
            // inbound seq (the malformed frame's own seq is not recovered); the
            // `Text (58)` is a fixed, non-secret policy string.
            FixRejectRoute::BusinessMessageReject => {
                let ref_msg_type = match error {
                    super::FixDecodeError::UnsupportedApplicationMsgType { msg_type } => {
                        msg_type.clone()
                    }
                    _ => String::new(),
                };
                let ref_seq = self.counters.next_target_seq;
                self.emit_business_reject(
                    ref_seq,
                    ref_msg_type,
                    order_flow::BUSINESS_REJECT_UNSUPPORTED_MSG_TYPE,
                    Some("unsupported message type".to_string()),
                    now_ms,
                )
            }
        }
    }

    /// The logon-timeout / heartbeat-cadence / revocation tick, on the injected
    /// venue clock.
    ///
    /// # Errors
    ///
    /// [`SessionError`] on counter exhaustion or a store failure while emitting a
    /// heartbeat / test request / logout.
    pub fn on_tick(&mut self, now_ms: u64, revoked: bool) -> Result<Reaction, SessionError> {
        match self.phase {
            SessionPhase::AwaitingLogon => {
                if now_ms.saturating_sub(self.accepted_at_ms) >= self.config.logon_timeout_ms {
                    // No logon in the window: close (we have no peer identity to
                    // address a Logout to).
                    self.phase = SessionPhase::Closing;
                    Ok(Reaction::close_silent())
                } else {
                    Ok(Reaction::cont())
                }
            }
            SessionPhase::Active | SessionPhase::AwaitingResend => {
                if revoked {
                    return self.logout_close(now_ms, "account revoked");
                }
                if self.heart_bt_int_ms == 0 {
                    return Ok(Reaction::cont());
                }
                let mut frames = Vec::new();

                // Outbound cadence: a heartbeat every HeartBtInt of silence.
                if now_ms.saturating_sub(self.last_outbound_ms) >= self.heart_bt_int_ms {
                    frames.push(self.emit(now_ms, |header| {
                        session::Heartbeat {
                            header,
                            test_req_id: None,
                        }
                        .encode()
                    })?);
                }

                // Inbound liveness: a TestRequest after HeartBtInt of inbound
                // silence, a close after another HeartBtInt without a reply.
                match self.awaiting_test_req_since_ms {
                    None if now_ms.saturating_sub(self.last_inbound_ms) >= self.heart_bt_int_ms => {
                        // The TestReqID is the (checked, monotonic) sender seq the
                        // probe will carry — unique per outbound frame, no counter.
                        let id = format!("TR-{}", self.counters.next_sender_seq);
                        frames.push(self.emit(now_ms, |header| {
                            session::TestRequest {
                                header,
                                test_req_id: id,
                            }
                            .encode()
                        })?);
                        self.awaiting_test_req_since_ms = Some(now_ms);
                    }
                    Some(since) if now_ms.saturating_sub(since) >= self.heart_bt_int_ms => {
                        frames.push(
                            self.logout_close(now_ms, "heartbeat timeout")?
                                .frames
                                .into_iter()
                                .next()
                                .unwrap_or_default(),
                        );
                        return Ok(Reaction::emit_close(frames));
                    }
                    _ => {}
                }
                Ok(Reaction::emit(frames))
            }
            SessionPhase::Closing => Ok(Reaction::close_silent()),
        }
    }

    /// Whether the session holds `required` (applying `Admin ⇒ Read + Trade`).
    fn has_permission(&self, required: Option<Permission>) -> bool {
        match required {
            None => true,
            Some(required) => self.permissions.iter().any(|held| held.grants(required)),
        }
    }

    /// The durable `SequenceReset` audit trail for the bound session (tests).
    #[must_use]
    pub fn reset_events(&self) -> Vec<SequenceResetEvent> {
        self.key
            .as_ref()
            .and_then(|key| self.store.reset_events(key).ok())
            .unwrap_or_default()
    }
}

/// Whether `message` is an inbound **application** message (order entry or market
/// data) — the messages that pass through the permission gate + `Account (1)`
/// attribution, as opposed to session admin (authenticated by the logon).
#[must_use]
fn is_application_message(message: &DecodedMessage) -> bool {
    is_order_entry_message(message) || matches!(message, DecodedMessage::MarketDataRequest(_))
}

/// Whether `message` is an inbound **order-entry** message the #039 order router
/// submits onto the sequenced path (`D`/`F`/`G`/`q`/`H`). Market data (`V`) is
/// application but routed by #040, not here.
#[must_use]
fn is_order_entry_message(message: &DecodedMessage) -> bool {
    matches!(
        message,
        DecodedMessage::NewOrderSingle(_)
            | DecodedMessage::OrderCancelRequest(_)
            | DecodedMessage::OrderCancelReplaceRequest(_)
            | DecodedMessage::OrderMassCancelRequest(_)
            | DecodedMessage::OrderStatusRequest(_)
    )
}

/// Whether `message` is an order-entry **mutation** routed onto the sequenced
/// exchange path (`D`/`F`/`G`/`q`) — the messages whose durable inbound-seq
/// consumption is DEFERRED until the exchange effect commits (#149 finding 1A).
/// `OrderStatusRequest (H)` and `MarketDataRequest (V)` are reads (no durable
/// mutation) and are excluded, so they consume their inbound seq immediately.
#[must_use]
fn is_order_entry_mutation(message: &DecodedMessage) -> bool {
    matches!(
        message,
        DecodedMessage::NewOrderSingle(_)
            | DecodedMessage::OrderCancelRequest(_)
            | DecodedMessage::OrderCancelReplaceRequest(_)
            | DecodedMessage::OrderMassCancelRequest(_)
    )
}

/// The permission a message class requires, or `None` for session admin
/// ([ADR-0007 §2](../../../docs/adr/0007-fix-credentials-and-account-model.md),
/// [03 §6](../../../docs/03-protocol-surfaces.md#6-authentication)). There is **no
/// FIX `Admin` row** — the control plane is not on FIX.
#[must_use]
pub fn required_permission(message: &DecodedMessage) -> Option<Permission> {
    match message {
        // Session admin: authenticated by the logon itself.
        DecodedMessage::Logon(_)
        | DecodedMessage::Logout(_)
        | DecodedMessage::Heartbeat(_)
        | DecodedMessage::TestRequest(_)
        | DecodedMessage::ResendRequest(_)
        | DecodedMessage::SequenceReset(_)
        | DecodedMessage::Reject(_) => None,
        // Trading.
        DecodedMessage::NewOrderSingle(_)
        | DecodedMessage::OrderCancelRequest(_)
        | DecodedMessage::OrderCancelReplaceRequest(_)
        | DecodedMessage::OrderMassCancelRequest(_) => Some(Permission::Trade),
        // Market data / order status (reads).
        DecodedMessage::OrderStatusRequest(_) | DecodedMessage::MarketDataRequest(_) => {
            Some(Permission::Read)
        }
        // Venue-out messages should never be classified inbound; treat as read so
        // the reject path (not the permission path) handles them.
        DecodedMessage::ExecutionReport(_)
        | DecodedMessage::OrderCancelReject(_)
        | DecodedMessage::OrderMassCancelReport(_)
        | DecodedMessage::BusinessMessageReject(_)
        | DecodedMessage::MarketDataSnapshotFullRefresh(_)
        | DecodedMessage::MarketDataIncrementalRefresh(_)
        | DecodedMessage::MarketDataRequestReject(_) => Some(Permission::Read),
    }
}

/// The underlying ticker of a validated [`Symbol`], via the upstream
/// [`SymbolParser`] — used only to synthesize composite `ExecID`s / mint order
/// ids. The symbol is validated at decode, so the parse succeeds; the empty
/// fallback on the impossible failure keeps the caller total without an `unwrap`.
#[must_use]
fn underlying_of_symbol(symbol: &Symbol) -> String {
    SymbolParser::parse(symbol.as_str())
        .map(|parsed| parsed.underlying().to_string())
        .unwrap_or_default()
}

/// The standard header of any decoded message.
fn header_of(message: &DecodedMessage) -> &StandardHeader {
    match message {
        DecodedMessage::Logon(m) => m.header(),
        DecodedMessage::Logout(m) => m.header(),
        DecodedMessage::Heartbeat(m) => m.header(),
        DecodedMessage::TestRequest(m) => m.header(),
        DecodedMessage::ResendRequest(m) => m.header(),
        DecodedMessage::SequenceReset(m) => m.header(),
        DecodedMessage::Reject(m) => m.header(),
        DecodedMessage::NewOrderSingle(m) => m.header(),
        DecodedMessage::OrderCancelRequest(m) => m.header(),
        DecodedMessage::OrderCancelReplaceRequest(m) => m.header(),
        DecodedMessage::OrderMassCancelRequest(m) => m.header(),
        DecodedMessage::OrderStatusRequest(m) => m.header(),
        DecodedMessage::ExecutionReport(m) => m.header(),
        DecodedMessage::OrderCancelReject(m) => m.header(),
        DecodedMessage::OrderMassCancelReport(m) => m.header(),
        DecodedMessage::BusinessMessageReject(m) => m.header(),
        DecodedMessage::MarketDataRequest(m) => m.header(),
        DecodedMessage::MarketDataSnapshotFullRefresh(m) => m.header(),
        DecodedMessage::MarketDataIncrementalRefresh(m) => m.header(),
        DecodedMessage::MarketDataRequestReject(m) => m.header(),
    }
}

/// Hand-builds the venue's `Logon (A)` ack — `EncryptMethod (98)=0`,
/// `HeartBtInt (108)`, and `ResetSeqNumFlag (141)` when the client reset —
/// **without** `Username (553)` / `Password (554)`: an acceptor never echoes a
/// credential.
fn encode_logon_ack(header: &StandardHeader, heart_bt_int_secs: u32, reset_flag: bool) -> Vec<u8> {
    let mut writer = FieldWriter::new(MSG_TYPE_LOGON);
    header.encode(&mut writer);
    writer.u64(tags::ENCRYPT_METHOD, 0);
    writer.u64(tags::HEART_BT_INT, u64::from(heart_bt_int_secs));
    if reset_flag {
        writer.opt_bool(tags::RESET_SEQ_NUM_FLAG, Some(true));
    }
    writer.finish()
}

// ============================================================================
// The async session wrapper — the acceptor `FixSession` seam
// ============================================================================

/// The set of FIX [`SessionKey`]s with a currently-live session — the per-key
/// single-active-session lease registry that enforces logon exclusivity
/// ([ADR-0010](../../../docs/adr/0010-fix-session-account-binding.md)).
///
/// Without it, two concurrent logons for the **same** authenticated
/// `(account_id, comp_id_tuple)` would each [`load_counters`](FixSessionStore::load_counters),
/// emit duplicate outbound `MsgSeqNum`s, and race a last-writer-wins
/// [`save_counters`](FixSessionStore::save_counters) — a counter-corruption /
/// message-loss hole. This registry admits at most one live session per key: a
/// second concurrent logon finds the lease held and is refused with a `Logout`.
///
/// The lease is claimed at logon-admission time (once the account is authenticated
/// and the CompID tuple is bound) and released on session end via the RAII
/// [`SessionLease`] guard — reliably, even on an abrupt disconnect, because the
/// guard drops when the per-connection [`VenueFixSession`] drops (the acceptor owns
/// the session and drops it on every exit path). The live-lease set is inherently
/// bounded by the venue connection cap (a lease is only ever held by a live
/// session), so it needs no separate size bound. The single [`Mutex`] is held only
/// across the O(1) set mutation, never across an `.await`.
#[derive(Debug, Default)]
pub struct SessionLeaseRegistry {
    live: Mutex<HashSet<SessionKey>>,
}

impl SessionLeaseRegistry {
    /// Builds an empty lease registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically claims the single-active-session lease for `key`, returning an
    /// RAII [`SessionLease`] on success, or `None` if a live session already holds
    /// it (the caller then refuses the second logon). The claim is a single
    /// check-then-insert under one lock, so two racing logons can never both win.
    fn try_claim(self: &Arc<Self>, key: SessionKey) -> Option<SessionLease> {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // `HashSet::insert` returns `false` when the key is already present — the
        // key is already leased by a live session, so the claim is refused.
        if live.insert(key.clone()) {
            Some(SessionLease {
                registry: Arc::clone(self),
                key,
            })
        } else {
            None
        }
    }

    /// Releases the lease for `key` (invoked from [`SessionLease`]'s drop).
    fn release(&self, key: &SessionKey) {
        self.live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(key);
    }

    /// Whether a live session currently holds the lease for `key` (tests /
    /// observability).
    #[must_use]
    pub fn is_live(&self, key: &SessionKey) -> bool {
        self.live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(key)
    }
}

/// The RAII single-active-session lease for one [`SessionKey`] — releasing the key
/// from the [`SessionLeaseRegistry`] when the owning [`VenueFixSession`] drops, so a
/// later reconnect can re-admit even after an abrupt disconnect.
#[derive(Debug)]
pub struct SessionLease {
    registry: Arc<SessionLeaseRegistry>,
    key: SessionKey,
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        self.registry.release(&self.key);
    }
}

/// The ceiling on the per-session `(ClOrdID → order)` correlation map — a memory
/// DoS bound so a long-lived session that places without cancelling cannot grow an
/// unbounded map. Once full, further placements still submit and report, but are
/// no longer cancel/replace/status-correlatable by the gateway (an untracked
/// `OrigClOrdID` then answers `OrderCancelReject (9)` / an unknown-order status).
const MAX_TRACKED_ORDERS_PER_SESSION: usize = 100_000;

/// A gateway-tracked order the session placed — the correlation the FIX client
/// namespace (`ClOrdID`) needs to reach the venue order id the gateway minted.
///
/// `OrderCancelRequest (F)` / `OrderCancelReplaceRequest (G)` carry `OrigClOrdID`
/// (the client's id), but the sequenced order path cancels by the venue
/// [`VenueOrderId`] the gateway minted for the original `D`; this per-session map
/// bridges the two. It is session-scoped: a cancel referencing an order placed on
/// a *prior* connection is answered unknown-order (a documented v0.4 limitation —
/// the durable `(account, ClOrdID) → order_id` index lands with a later
/// idempotency issue).
#[derive(Debug, Clone)]
struct PlacedOrder {
    order_id: VenueOrderId,
    symbol: Symbol,
    side: OrderSide,
    quantity: u64,
    /// The economic payload of the placing message, so a same-`ClOrdID` retry can
    /// be classified byte-identical (re-render the real order) vs conflicting
    /// (reject) — the gateway-side cross-protocol idempotency guard (#039).
    fingerprint: OrderFingerprint,
}

impl PlacedOrder {
    /// The cancel/replace/status correlation fields, dropping the idempotency
    /// fingerprint (which only the new-order dedup path reads).
    fn resolved(&self) -> ResolvedOrder {
        ResolvedOrder {
            order_id: self.order_id.clone(),
            symbol: self.symbol.clone(),
            side: self.side,
            quantity: self.quantity,
        }
    }
}

/// The order a cancel / replace / status resolves an `OrigClOrdID` to — from
/// either this session's [`PlacedOrder`] map or the venue-wide, account-scoped
/// `(account, ClOrdID) → order_id` index (#098) that reaches **across** sessions.
/// Carries only what the sequenced cancel command and its report render need
/// (never the idempotency fingerprint).
#[derive(Debug, Clone)]
struct ResolvedOrder {
    order_id: VenueOrderId,
    symbol: Symbol,
    side: OrderSide,
    quantity: u64,
}

impl ResolvedOrder {
    /// Builds a resolved order from a cross-session index record, mapping the
    /// upstream matching-seam [`crate::exchange::Side`] onto the FIX wire
    /// [`OrderSide`] (a `Buy`/`Sell` bijection).
    fn from_index(record: crate::exchange::ClOrdIdRecord) -> Self {
        let side = match record.side {
            crate::exchange::Side::Buy => OrderSide::Buy,
            crate::exchange::Side::Sell => OrderSide::Sell,
        };
        Self {
            order_id: record.order_id,
            symbol: record.symbol,
            side,
            quantity: record.quantity,
        }
    }
}

/// The economically-meaningful fields of a `NewOrderSingle (D)` (or the add leg of
/// a `G`) — everything that determines the derived `VenueCommand`, and nothing
/// that changes across a legitimate retry (no `MsgSeqNum`, no `SendingTime`, no
/// `TransactTime`). Two placements with the same fingerprint derive the same
/// command, so a same-`ClOrdID` resend of one is a byte-identical retry, not a new
/// order ([fix-dialect §4](../../../docs/specs/fix-dialect.md#4-identifiers-correlation-and-idempotency)).
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderFingerprint {
    symbol: Symbol,
    side: OrderSide,
    ord_type: OrdType,
    price: Option<Cents>,
    order_qty: u64,
    time_in_force: FixTif,
    expire_time: Option<UtcTimestamp>,
}

impl OrderFingerprint {
    /// The fingerprint of a `NewOrderSingle (D)`.
    fn of_new_order(order: &NewOrderSingle) -> Self {
        Self {
            symbol: order.symbol.clone(),
            side: order.side,
            ord_type: order.ord_type,
            price: order.price,
            order_qty: order.order_qty,
            time_in_force: order.time_in_force,
            expire_time: order.expire_time.clone(),
        }
    }

    /// The fingerprint of the add leg of an `OrderCancelReplaceRequest (G)` (the
    /// replacement rests as `GTC`; a `G` carries no `ExpireTime`).
    fn of_replace(replace: &OrderCancelReplaceRequest) -> Self {
        Self {
            symbol: replace.symbol.clone(),
            side: replace.side,
            ord_type: replace.ord_type,
            price: replace.price,
            order_qty: replace.order_qty,
            time_in_force: FixTif::Gtc,
            expire_time: None,
        }
    }
}

/// One symbol's live market-data subscription state (#040): the `MDReqID (262)`
/// the client subscribed it under, the requested book sides, and the snapshot
/// depth. Keyed by [`Symbol`] in [`MdSubscription::symbols`].
#[derive(Debug, Clone)]
struct MdSymbolSub {
    /// The `MDReqID (262)` of the `V` that subscribed this symbol — echoed on every
    /// `W`/`X` for it.
    md_req_id: String,
    /// Which book sides (Bid / Offer) the request asked for.
    sides: RequestedSides,
    /// The requested snapshot depth (`MarketDepth (264) = 0` ⇒ full book / `None`).
    depth: Option<usize>,
    /// The last `instrument_sequence` already reflected in the client's stream — the
    /// delivered `W` baseline, then each emitted `X`. An `orderbook_delta` at or
    /// below it is already in the client's snapshot and is dropped, so the FIX and
    /// WS streams are identical at the subscribe boundary and `RptSeq (83)` stays
    /// **strictly increasing** across the `W → X` boundary (mirrors the WS filter,
    /// `src/gateway/ws/mod.rs`).
    baseline: u64,
}

/// The session's live FIX market-data subscription (#040): one venue-wide
/// broadcast receiver (created lazily on the first `V`) and the per-symbol
/// subscription set, bounded at [`MAX_MD_SYMBOLS_PER_SESSION`].
///
/// FIX MD is a thin projection of the **same** [`crate::subscription::OrderbookSubscriptionManager`]
/// the WS surface reads: the receiver carries the committed `orderbook_delta`
/// stream, and each `X` is drained from it in `MsgSeqNum` order onto the same
/// bounded outbound mailbox. A lagged receiver re-snapshots (the WS gap contract),
/// never stalling the producer.
struct MdSubscription {
    /// The venue-wide market-data broadcast receiver for this connection.
    receiver: broadcast::Receiver<WsMessage>,
    /// The per-symbol subscription set (`Symbol → MDReqID + sides + depth`).
    symbols: HashMap<Symbol, MdSymbolSub>,
}

/// The real per-connection FIX session — the [`FixSession`] the acceptor drives,
/// wrapping the synchronous [`SessionFsm`] with the async credential verify, the
/// async order-path routing (#039), and the market-data projection (#040).
pub struct VenueFixSession {
    peer: SocketAddr,
    state: Arc<AppState>,
    fsm: SessionFsm,
    /// The shared per-`SessionKey` single-active-session lease registry — claimed at
    /// logon admission, released on drop.
    leases: Arc<SessionLeaseRegistry>,
    /// The held lease for this session's `SessionKey`, set once the logon is
    /// admitted; its RAII drop releases the key so a later reconnect can re-admit.
    lease: Option<SessionLease>,
    /// `ClOrdID → placed order` for this session's cancel/replace/status
    /// correlation (bounded at [`MAX_TRACKED_ORDERS_PER_SESSION`]).
    placed: HashMap<ClientOrderId, PlacedOrder>,
    /// The live market-data subscription (`None` until the first `MarketDataRequest
    /// (V)`), #040.
    market_data: Option<MdSubscription>,
}

impl VenueFixSession {
    /// Builds a session over the shared venue state, session store, and config.
    #[must_use]
    pub fn new(
        peer: SocketAddr,
        state: Arc<AppState>,
        store: Arc<dyn FixSessionStore>,
        config: SessionConfig,
        leases: Arc<SessionLeaseRegistry>,
    ) -> Self {
        let accepted_at_ms = state.clock().now_ms().get();
        let fsm = SessionFsm::new(config, store, accepted_at_ms);
        Self {
            peer,
            state,
            fsm,
            leases,
            lease: None,
            placed: HashMap::new(),
            market_data: None,
        }
    }

    /// The venue-clock instant (ms) — the same injected clock the sequenced path
    /// and the rate limiter read.
    fn now_ms(&self) -> u64 {
        self.state.clock().now_ms().get()
    }

    /// The per-message revocation read: `true` if the bound account's current
    /// epoch has risen above the logon-time epoch, or the account is now unknown.
    fn revoked(&self) -> bool {
        match self.fsm.account() {
            None => false,
            Some(account) => match self.state.accounts().current_revocation_epoch(account) {
                Some(current) => current > self.fsm.session_epoch,
                None => true,
            },
        }
    }

    /// Sends a [`Reaction`]'s frames onto the bounded outbound mailbox and returns
    /// the control decision. A mailbox failure (full / closed) closes the session;
    /// a [`SessionError`] seals it.
    fn flush(
        &self,
        result: Result<Reaction, SessionError>,
        out: &SessionOutbound,
    ) -> SessionControl {
        let reaction = match result {
            Ok(reaction) => reaction,
            Err(SessionError::SequenceExhausted) => {
                tracing::warn!(peer = %self.peer, "fix session sequence exhausted; sealing session");
                return SessionControl::Close;
            }
            Err(error) => {
                tracing::warn!(peer = %self.peer, %error, "fix session error; closing");
                return SessionControl::Close;
            }
        };
        for frame in reaction.frames {
            if out.send(frame).is_err() {
                return SessionControl::Close;
            }
        }
        reaction.control
    }

    /// Handles an inbound `Logon (A)`: rate-limit, HeartBtInt negotiation,
    /// Argon2id verify (under [`spawn_blocking`](tokio::task::spawn_blocking)),
    /// revocation, and the account ↔ CompID binding, then admit or reject.
    async fn handle_logon(
        &mut self,
        logon: session::Logon,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        // Logon rate limit (pre-auth): keyed on the peer IP (there is no account
        // yet), the same limiter the REST/WS pre-token path falls back to.
        let decision = self
            .state
            .auth()
            .rate_limiter()
            .check_and_record_status(&RateLimitKey::Peer(self.peer.ip()));
        if !decision.allowed {
            return self.fsm.logout_close(now_ms, "logon rate limited");
        }

        // HeartBtInt negotiation: reject a zero or over-ceiling proposal.
        if logon.heart_bt_int == 0 || logon.heart_bt_int > self.fsm.config.max_heart_bt_int_secs {
            return self
                .fsm
                .logout_close(now_ms, "unacceptable heartbeat interval");
        }

        // Argon2id verify runs on the blocking pool — it is deliberately slow and
        // CPU-bound, so it must never occupy an async worker (or it would stall the
        // accept loop / graceful drain). The plaintext copy is dropped in the task.
        let state = Arc::clone(&self.state);
        let username = logon.username.clone();
        let password = logon.password.expose().to_string();
        let outcome = tokio::task::spawn_blocking(move || {
            state.accounts().verify_fix_password(&username, &password)
        })
        .await;

        let account_id = match outcome {
            Ok(crate::auth::FixLoginOutcome::Authenticated { account, .. }) => account,
            // A wrong username, no credential, wrong password, or a panicked verify
            // are all indistinguishable — never leak which.
            Ok(crate::auth::FixLoginOutcome::Rejected) | Err(_) => {
                return self.fsm.logout_close(now_ms, "authentication failed");
            }
        };

        // Resolve the account row for its binding, permissions, and epoch.
        let Some(account) = self.state.accounts().account(&account_id) else {
            return self.fsm.logout_close(now_ms, "authentication failed");
        };

        // Revocation: a revoked account (a bumped epoch) cannot log in.
        if account.revocation_epoch > 0 {
            return self.fsm.logout_close(now_ms, "account revoked");
        }

        // Binding (ADR-0010 rule 3): the presented (SenderCompID, TargetCompID)
        // must equal the account's immutable bound tuple. An unbound account, or a
        // tuple bound to a DIFFERENT account, is a SessionBindingViolation.
        let presented_sender = logon.header.sender_comp_id.as_str();
        let presented_target = logon.header.target_comp_id.as_str();
        let bound = matches!(
            &account.credentials.fix_comp_ids,
            Some(binding)
                if binding.sender_comp_id == presented_sender
                    && binding.target_comp_id == presented_target
        );
        if !bound {
            return self.fsm.logout_close(now_ms, "session binding violation");
        }

        let logon_seq = logon.header.msg_seq_num.value();
        let reset_flag = logon.reset_seq_num_flag.unwrap_or(false);

        // Per-`SessionKey` exclusivity (ADR-0010): claim the single-active-session
        // lease for this authenticated `(account, CompID tuple)` BEFORE admitting.
        // Two concurrent logons for the same key would otherwise each load the shared
        // durable counters, emit duplicate outbound `MsgSeqNum`s, and race a
        // last-writer-wins `save_counters`; the atomic claim admits exactly one, so
        // a second concurrent logon is refused with a `Logout`. The lease key is
        // built identically to the durable store key `admit_logon` resolves — the
        // authenticated account plus the presented `(SenderCompID, TargetCompID)`.
        let session_key = SessionKey::new(
            account.id.clone(),
            logon.header.sender_comp_id.as_str().to_string(),
            logon.header.target_comp_id.as_str().to_string(),
        );
        let Some(lease) = self.leases.try_claim(session_key) else {
            tracing::warn!(
                peer = %self.peer,
                "fix logon refused: a session for this account/CompID tuple is already active"
            );
            return self.fsm.logout_close(now_ms, "session already active");
        };

        // Admit (or, for a stale-seq reconnect, a `Logout` that still closes). On any
        // earlier `?` error the local `lease` drops here and frees the key; on
        // success it is held for the session's lifetime and released by the RAII
        // guard's drop when the connection ends (covering an abrupt disconnect).
        let reaction = self.fsm.admit_logon(
            account.id,
            account.permissions,
            account.revocation_epoch,
            logon.heart_bt_int,
            reset_flag,
            logon_seq,
            now_ms,
        )?;
        self.lease = Some(lease);
        Ok(reaction)
    }

    // ------------------------------------------------------------------------
    // Order-path routing (#039) — the async side of the sequenced order path
    // ------------------------------------------------------------------------

    /// The per-op rate-limit decision for a mutating order (`D`/`F`/`G`), keyed on
    /// the bound account + its revocation epoch — the **same** sliding window
    /// REST/WS enforce, so throttling is identical across surfaces. Records the hit.
    fn rate_limited(&self) -> bool {
        let Some(account) = self.fsm.account.clone() else {
            return true;
        };
        !self
            .state
            .auth()
            .rate_limiter()
            .check_and_record_status(&RateLimitKey::Account {
                account,
                revocation_epoch: self.fsm.session_epoch,
                // The bound account's tier (#046) — the same per-window budget the
                // REST/WS surfaces resolve for this account, so throttling is
                // identical across surfaces.
                tier: RateLimitTier::from_permissions(&self.fsm.permissions),
            })
            .allowed
    }

    /// Tracks a placed order for cancel/replace/status correlation, bounded at
    /// [`MAX_TRACKED_ORDERS_PER_SESSION`].
    #[allow(clippy::too_many_arguments)]
    fn track_placed(
        &mut self,
        cl_ord_id: ClientOrderId,
        order_id: VenueOrderId,
        symbol: Symbol,
        side: OrderSide,
        quantity: u64,
        fingerprint: OrderFingerprint,
    ) {
        if self.placed.len() >= MAX_TRACKED_ORDERS_PER_SESSION
            && !self.placed.contains_key(&cl_ord_id)
        {
            tracing::debug!(
                peer = %self.peer,
                "fix order-tracking map is full; order placed but not cancel-correlatable"
            );
            return;
        }
        self.placed.insert(
            cl_ord_id,
            PlacedOrder {
                order_id,
                symbol,
                side,
                quantity,
                fingerprint,
            },
        );
    }

    /// Resolves an `OrigClOrdID` to the order it names, on the **authenticated
    /// account** (#098). Consults this session's tracking map first (the fast local
    /// path, which also carries replace-minted client ids), then falls back to the
    /// venue-wide, account-scoped `(account, ClOrdID) → order_id` index — so a
    /// cancel/replace/status referencing an order placed on a **prior** connection
    /// resolves instead of answering `9 Unknown order`.
    ///
    /// Account isolation: the index key includes the account, so a colliding
    /// `ClOrdID` under another account is a different key and resolves to [`None`]
    /// — indistinguishable at the client boundary from a genuinely unknown id, so
    /// one account can never resolve or cancel another's order.
    ///
    /// A synchronous point read — no lock is held across an `.await`.
    fn resolve_order(
        &self,
        account: &AccountId,
        cl_ord_id: &ClientOrderId,
    ) -> Option<ResolvedOrder> {
        if let Some(placed) = self.placed.get(cl_ord_id) {
            return Some(placed.resolved());
        }
        self.state
            .resolve_client_order_id(account, cl_ord_id)
            .map(ResolvedOrder::from_index)
    }

    /// Re-renders the true current state of a tracked order from the committed
    /// executions store (`New` when resting/unfilled, `PartiallyFilled`/`Filled`
    /// per the folded fills) — the byte-identical-retry response and the
    /// `OrderStatusRequest (H)` response, so neither fabricates state.
    fn render_tracked_status(
        &mut self,
        order: &ResolvedOrder,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let Some(account) = self.fsm.account.clone() else {
            return Ok(Reaction::cont());
        };
        let legs = taker_legs_for_order(&self.state, &account, &order.order_id);
        let cum: u64 = legs
            .iter()
            .map(|leg| leg.quantity)
            .fold(0u64, |acc, quantity| {
                acc.checked_add(quantity).unwrap_or(acc)
            });
        let underlying = underlying_of_symbol(&order.symbol);
        let spec = order_flow::render_status_report(
            order.symbol.clone(),
            order.side,
            order.order_id.clone(),
            order.quantity,
            cum,
            EventTimestamp::new(now_ms),
            legs.last(),
            self.state.lineage_id(),
            &underlying,
        );
        self.fsm.emit_report_specs(vec![spec], now_ms)
    }

    /// Routes a permitted, attributed application message onto its async path
    /// (`handle_active` has already gated permission + attribution and consumed the
    /// inbound seq): order entry (`D`/`F`/`G`/`q`/`H`) onto the sequenced path
    /// (#039), and market data (`V`) onto the shared subscription manager (#040).
    async fn route_order(
        &mut self,
        message: DecodedMessage,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        // #111: stamp the message's `(SenderCompID, MsgSeqNum)` identity BEFORE the
        // match consumes it, so an order-entry command carries the seeded
        // latency-draw key onto the deterministic ingress-reorder buffer.
        let stamp = self.ingress_stamp(&message);
        match message {
            DecodedMessage::NewOrderSingle(order) => {
                self.route_new_order(order, stamp, now_ms).await
            }
            DecodedMessage::OrderCancelRequest(cancel) => {
                self.route_cancel(cancel, stamp, now_ms).await
            }
            DecodedMessage::OrderCancelReplaceRequest(replace) => {
                self.route_replace(replace, stamp, now_ms).await
            }
            DecodedMessage::OrderMassCancelRequest(request) => {
                self.route_mass_cancel(request, now_ms).await
            }
            DecodedMessage::OrderStatusRequest(status) => self.route_status(status, now_ms),
            DecodedMessage::MarketDataRequest(request) => self.route_market_data(request, now_ms),
            // `handle_active` only routes application messages here (unreachable).
            _ => Ok(Reaction::cont()),
        }
    }

    /// The message's `(SenderCompID, MsgSeqNum)` ingress identity (#111) — the key
    /// the seeded latency draw and the deadline tie-break consume when the venue has
    /// latency injection configured. The `MsgSeqNum` is read from the frame header
    /// (its stable per-message id); the `SenderCompID` is the resolved client comp.
    fn ingress_stamp(&self, message: &DecodedMessage) -> IngressStamp {
        let session_id = self.fsm.client_comp.as_ref().map_or("fix", CompId::as_str);
        let msg_seq = header_of(message).msg_seq_num.value();
        IngressStamp::new(session_id, msg_seq)
    }

    /// `NewOrderSingle (D)` → the identical [`VenueCommand::AddOrder`] REST
    /// produces, submitted through the same order-path seam, with the committed fills
    /// rendered as `ExecutionReport (8)`. Order entry passes through the deterministic
    /// ingress-reorder buffer ([`AppState::submit_with_ingress`], #111) so a
    /// latency-injected venue reshapes arrival order; with no latency it is plain
    /// FIFO, identical to REST. Any pre-submit or runtime failure is a context-correct
    /// `8 Rejected` with the reason from the error seam.
    async fn route_new_order(
        &mut self,
        order: NewOrderSingle,
        stamp: IngressStamp,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let Some(account) = self.fsm.account.clone() else {
            return Ok(Reaction::cont());
        };

        // Cross-protocol idempotency, resolved at the gateway BEFORE any mint /
        // submit. A same-session business retry re-sends the same `ClOrdID` with a
        // NEW `MsgSeqNum` (the standard retry after a dropped ack) — the transport
        // dup-seq guard does not catch it, so without this it would mint a fresh
        // order id, journal a phantom command, overwrite the real correlation, and
        // render a fabricated `New`. Instead:
        //   - byte-identical retry → re-render the REAL order's current state, no
        //     submit, correlation untouched (a later `F` still cancels the real
        //     order);
        //   - conflicting reuse (same `ClOrdID`, different economics) → reject
        //     (`Duplicate Order`), never overwriting the real correlation.
        // A cross-session resend on a fresh connection has an empty `placed` and
        // falls through to a normal placement — the deferred Receipt-seam
        // limitation tracked as
        // [#99](https://github.com/joaquinbejar/fauxchange/issues/99).
        let fingerprint = OrderFingerprint::of_new_order(&order);
        if let Some(placed) = self.placed.get(&order.cl_ord_id).cloned() {
            if placed.fingerprint == fingerprint {
                return self.render_tracked_status(&placed.resolved(), now_ms);
            }
            return self.fsm.emit_order_rejected_code(
                order.symbol.clone(),
                order.side,
                order.price,
                order_flow::ORD_REJ_REASON_DUPLICATE,
                Some(order_flow::DUPLICATE_CLORDID_TEXT.to_string()),
                now_ms,
            );
        }

        if self.rate_limited() {
            return self.reject_new_order(&order, &VenueError::RateLimited, now_ms);
        }
        let owner = match owner_for(&self.state, &account) {
            Ok(owner) => owner,
            Err(error) => return self.reject_new_order(&order, &error, now_ms),
        };
        let underlying = match SymbolParser::parse(order.symbol.as_str()) {
            Ok(parsed) => parsed.underlying().to_string(),
            Err(error) => return self.reject_new_order(&order, &VenueError::from(error), now_ms),
        };
        // The effective resolved TIF (market is the non-resting IOC primitive; a
        // limit resolves + validates its GTD expiry) — used to render the terminal
        // report and byte-identical to the command's own TIF.
        let tif = match order.ord_type {
            OrdType::Market => SeamTif::Ioc,
            OrdType::Limit => {
                match order_flow::seam_time_in_force(
                    order.time_in_force,
                    order.expire_time.as_ref(),
                ) {
                    Ok(tif) => tif,
                    Err(error) => return self.reject_new_order(&order, &error, now_ms),
                }
            }
        };
        let order_id = mint_order_id(self.state.lineage_id(), &underlying);
        let command =
            match order_flow::to_add_command(&order, order_id.clone(), account.clone(), owner) {
                Ok(command) => command,
                Err(error) => return self.reject_new_order(&order, &error, now_ms),
            };
        match self.state.submit_with_ingress(command, stamp).await {
            Ok(receipt) => {
                // Render the OBSERVED outcome (#118), never a false accept: a place into
                // a halted / `Settling` / `Expired` instrument (or another journaled
                // place rejection) is an `Ok(Receipt)` whose captured `VenueOutcome` is
                // `Rejected` — REST renders that as `Rejected`, so FIX MUST emit
                // `8 Rejected`, NOT a `New`. A rejected order is NOT tracked as placed,
                // so no phantom `F`/`G` correlation is created.
                let reaction = if let Some(VenueOutcome::Rejected { reason, .. }) = &receipt.outcome
                {
                    self.reject_new_order_outcome(&order, reason, now_ms)?
                } else {
                    self.track_placed(
                        order.cl_ord_id.clone(),
                        order_id.clone(),
                        order.symbol.clone(),
                        order.side,
                        order.order_qty,
                        fingerprint,
                    );
                    let legs = immediate_execution_records(
                        &self.state,
                        &account,
                        &order_id,
                        receipt.underlying_sequence,
                    );
                    let specs = order_flow::render_new_order_reports(
                        &order,
                        &order_id,
                        receipt.underlying_sequence,
                        receipt.venue_ts,
                        self.state.lineage_id(),
                        &underlying,
                        tif,
                        &legs,
                    );
                    self.fsm.emit_report_specs(specs, now_ms)?
                };
                // The exchange effect (a journaled place OR a journaled place-rejection)
                // is now durably committed AND the reports/reject are durably stored, so
                // persist the DEFERRED inbound-seq consumption (#149 finding 1A). A crash
                // before here re-admits the client's resend for idempotent reprocessing;
                // a crash after is safe (effect durable, resend dedups). A submit `Err`
                // (below) does NOT persist — the effect never committed, so the resend
                // must reprocess.
                self.fsm.persist_inbound()?;
                Ok(reaction)
            }
            Err(error) => self.reject_new_order(&order, &error, now_ms),
        }
    }

    /// The `ExecutionReport (8) Rejected` for a `NewOrderSingle (D)` whose **observed**
    /// sequenced outcome was a [`VenueOutcome::Rejected`] (a halted / `Settling` /
    /// `Expired` instrument, or another journaled place rejection) — REST ≡ FIX order
    /// entry (#118). It carries the same journaled, client-safe `reason` REST renders as
    /// its `Rejected` message: a place reject's reason is instrument-/order-level (never
    /// per-account), so it is safe to name in `Text (58)`. The `OrdRejReason (103)` is
    /// the venue's business-validation code (`11`), the same code the error seam maps a
    /// business rejection to.
    fn reject_new_order_outcome(
        &mut self,
        order: &NewOrderSingle,
        reason: &str,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        self.fsm.emit_order_rejected_code(
            order.symbol.clone(),
            order.side,
            order.price,
            order_flow::ord_rej_reason(FixRejectReason::Invalid),
            Some(reason.to_string()),
            now_ms,
        )
    }

    /// The `8 Rejected` for a `NewOrderSingle` failure — the reject message is
    /// fixed by the `NewOrder` context, the reason by the error.
    fn reject_new_order(
        &mut self,
        order: &NewOrderSingle,
        error: &VenueError,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let reject = error.fix_reject(FixRejectContext::NewOrder);
        self.fsm.emit_order_rejected(
            order.symbol.clone(),
            order.side,
            order.price,
            &reject,
            now_ms,
        )
    }

    /// `OrderMassCancelRequest (q)` → the **owner-scoped**
    /// [`VenueCommand::MassCancel`] (#97): the caller can only ever sweep ITS OWN
    /// resting orders ([`MassCancelType::ByUser`] keyed on the account's STP owner),
    /// never another account's — the executor's owner filter enforces the isolation,
    /// so the reject/report never discloses another account's orders. `All (530=7)`
    /// sweeps the account's whole resting set across the venue; `Security (530=1)`
    /// sweeps one book (`Symbol (55)`). A committed sweep renders
    /// `OrderMassCancelReport (r)` accepted plus one `ExecutionReport (8) Canceled`
    /// per swept order ([03 §5.3](../../../docs/03-protocol-surfaces.md#53-order-entry-and-execution-reports));
    /// a rate-limited, unresolved-owner, or failed request renders `r Rejected`.
    async fn route_mass_cancel(
        &mut self,
        request: OrderMassCancelRequest,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let Some(account) = self.fsm.account.clone() else {
            return Ok(Reaction::cont());
        };
        // Rate limit every mutating op on every protocol (rule 6): a throttled `q`
        // is an honest `r Rejected`, never a silent sweep.
        if self.rate_limited() {
            return self.fsm.emit_mass_cancel_rejected(now_ms);
        }
        let owner = match owner_for(&self.state, &account) {
            Ok(owner) => owner,
            Err(_) => return self.fsm.emit_mass_cancel_rejected(now_ms),
        };
        // Map the request scope onto the venue command scope + the echoed response
        // label. Both scopes are owner-scoped by `ByUser(owner)`.
        let (scope, response) = match request.mass_cancel_request_type {
            MassCancelRequestType::All => (MassCancelScope::Underlying, MassCancelResponse::All),
            MassCancelRequestType::Security => {
                // Decode guarantees a `Symbol` for a per-security scope; be total.
                let Some(symbol) = request.symbol.clone() else {
                    return self.fsm.emit_mass_cancel_rejected(now_ms);
                };
                (MassCancelScope::Book(symbol), MassCancelResponse::Security)
            }
        };
        let command = VenueCommand::MassCancel {
            scope,
            cancel_type: MassCancelType::ByUser(owner),
            account,
        };
        match self.state.submit_mass_cancel(command).await {
            Ok(delivery) => {
                let reaction =
                    self.render_mass_cancel(response, &delivery.swept, delivery.fanout, now_ms)?;
                // The mass-cancel sweep is durably committed; persist the deferred
                // inbound-seq consumption post-effect (#149 finding 1A). The submit
                // `Err` arm (below) does NOT persist.
                self.fsm.persist_inbound()?;
                Ok(reaction)
            }
            Err(_) => self.fsm.emit_mass_cancel_rejected(now_ms),
        }
    }

    /// Renders an accepted [`OrderMassCancelReport (r)`] plus one
    /// [`ExecutionReport (8) Canceled`] per swept order ([03 §5.3](../../../docs/03-protocol-surfaces.md#53-order-entry-and-execution-reports)).
    ///
    /// The affected set is the deterministic, venue-id-sorted [`MassCancelled`]
    /// outcome; the `r` report carries **every** swept order id (the honest count).
    /// Each per-order `8` renders the order's `Symbol`/`Side` **directly from the
    /// swept leg** — the [`CancelledLeg`](crate::exchange::CancelledLeg) now carries
    /// the resting order's own `symbol`/`side`, journaled in the outcome — so EVERY
    /// swept order gets its `8`, including one this session did not place (a REST
    /// placement by the same account, or a prior FIX session). Any matching
    /// session-tracked entry is still dropped from `self.placed` (the order is gone
    /// from the book), so a later `F` on it is a masked reject.
    ///
    /// `fanout` is the venue-global delivery summary. On a NON-full fan-out the
    /// per-order `8`s for what WAS swept are still emitted (those cancellations are
    /// real) and the `r` carries the honest affected count, but the partial delivery
    /// is `WARN`-logged — FIX has no structured partial-delivery field, so it is not
    /// presented as a clean full success; the structured signal lives on REST.
    fn render_mass_cancel(
        &mut self,
        response: MassCancelResponse,
        affected: &[SweptLeg],
        fanout: FanoutSummary,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        // A partial venue-wide fan-out is a reportable state (#97 finding 2): warn
        // rather than present the sweep as a clean full success. FIX has no
        // structured partial-delivery field; the per-order `8`s below are still the
        // real cancellations, and the structured `fully_applied` signal is on REST.
        if !fanout.fully_applied() {
            tracing::warn!(
                ok_underlyings = fanout.ok_count,
                total_underlyings = fanout.total,
                swept = affected.len(),
                "FIX mass-cancel fan-out was partial across underlyings; live orders \
                 may remain on the rejected underlyings"
            );
        }
        let lineage = self.state.lineage_id().clone();
        let affected_ids: Vec<VenueOrderId> = affected
            .iter()
            .map(|swept| swept.leg.order_id.clone())
            .collect();
        // Render each swept leg's `8` directly from the journaled `symbol`/`side` on
        // the outcome — no session-tracked reverse resolution, so no swept order is
        // ever silently skipped. The composite `ExecID` is collision-free (per-leg
        // underlying + the swept command's `underlying_sequence` join key + index).
        let mut specs: Vec<ExecReportSpec> = Vec::with_capacity(affected.len());
        let mut index: u32 = 0;
        for swept in affected {
            // Drop any session-tracked correlation for this order: it is gone from
            // the book, so a later `F`/`G` on it must masked-reject.
            let tracked = self
                .placed
                .iter()
                .find(|(_, placed)| placed.order_id == swept.leg.order_id)
                .map(|(cl_ord_id, _)| cl_ord_id.clone());
            if let Some(cl_ord_id) = tracked {
                self.placed.remove(&cl_ord_id);
            }
            let underlying = underlying_of_symbol(&swept.leg.symbol);
            specs.push(order_flow::render_mass_cancel_leg_report(
                swept.leg.symbol.clone(),
                order_flow::fix_side(swept.leg.side),
                swept.leg.order_id.clone(),
                swept.sequence,
                // The aggregated swept legs carry no per-leg commit `venue_ts`, so a
                // mass-cancel leg's `TransactTime (60)` is the render-turn venue-clock
                // instant (`now_ms`, the injected venue clock the FIX handler runs on —
                // never wall-clock, deterministic on replay) (#104).
                EventTimestamp::new(now_ms),
                &lineage,
                &underlying,
                index,
            ));
            // Checked (rule 9); the affected set is bounded by resting orders.
            index = index
                .checked_add(1)
                .ok_or(SessionError::SequenceExhausted)?;
        }
        self.fsm
            .emit_mass_cancel_accepted(response, affected_ids, specs, now_ms)
    }

    /// `OrderCancelRequest (F)` → [`VenueCommand::CancelOrder`], resolving the
    /// client `OrigClOrdID` to the venue order id the gateway minted. A committed
    /// cancel renders `ExecutionReport (8) Canceled`; an unknown order or a runtime
    /// failure renders `OrderCancelReject (9)`.
    async fn route_cancel(
        &mut self,
        cancel: OrderCancelRequest,
        stamp: IngressStamp,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let Some(account) = self.fsm.account.clone() else {
            return Ok(Reaction::cont());
        };
        if self.rate_limited() {
            return self.reject_cancel(
                VenueOrderId::new("NONE"),
                &cancel.orig_cl_ord_id,
                &cancel.cl_ord_id,
                CxlRejResponseTo::OrderCancelRequest,
                &VenueError::RateLimited,
                now_ms,
            );
        }
        // Resolve `OrigClOrdID` cross-session (#098): this session's map first, then
        // the account-scoped venue index — so a cancel of an order placed on a prior
        // connection succeeds instead of answering `9 Unknown order`. An unknown id
        // (or one owned by another account) is an indistinguishable masked reject
        // (never revealing not-found vs not-owner vs already-gone, #118).
        let Some(resolved) = self.resolve_order(&account, &cancel.orig_cl_ord_id) else {
            return self.reject_cancel(
                VenueOrderId::new("NONE"),
                &cancel.orig_cl_ord_id,
                &cancel.cl_ord_id,
                CxlRejResponseTo::OrderCancelRequest,
                &VenueError::NotFound(order_flow::CANCEL_REJECT_MASKED_REASON.to_string()),
                now_ms,
            );
        };
        let command = order_flow::to_cancel_command(&cancel, account, resolved.order_id.clone());
        match self.state.submit_with_ingress(command, stamp).await {
            Ok(receipt) => {
                // Render the OBSERVED outcome (#118), never a false `Canceled`: a cancel
                // of an unowned / already-gone order (including a STALE cross-session
                // index entry pointing at an order that has since filled/cancelled) is
                // an `Ok(Receipt)` whose captured outcome is `Rejected` — emit a masked
                // `OrderCancelReject (9)`, not a `Canceled`, and keep the session
                // correlation (do not drop tracking). Identity is sourced from the
                // cross-session `resolved` (#098), not a session-local `placed`.
                let reaction =
                    if let Some(VenueOutcome::Rejected { kind, reason }) = &receipt.outcome {
                        let masked = Self::masked_cancel_error(&resolved.order_id, *kind, reason);
                        self.reject_cancel(
                            resolved.order_id.clone(),
                            &cancel.orig_cl_ord_id,
                            &cancel.cl_ord_id,
                            CxlRejResponseTo::OrderCancelRequest,
                            &masked,
                            now_ms,
                        )?
                    } else {
                        let underlying = underlying_of_symbol(&resolved.symbol);
                        let spec = order_flow::render_cancel_report(
                            resolved.symbol.clone(),
                            resolved.side,
                            resolved.order_id.clone(),
                            receipt.underlying_sequence,
                            receipt.venue_ts,
                            self.state.lineage_id(),
                            &underlying,
                        );
                        // Drop this session's local tracking of the cancelled id. The
                        // shared index stays journal-derived; a re-cancel resolves the
                        // same order id and the sequenced cancel then rejects it as
                        // no-longer-resting (a masked reject via the branch above).
                        self.placed.remove(&cancel.orig_cl_ord_id);
                        self.fsm.emit_report_specs(vec![spec], now_ms)?
                    };
                // The cancel's exchange effect is durably committed; persist the
                // deferred inbound-seq consumption post-effect (#149 finding 1A). The
                // submit `Err` arm (below) does NOT persist.
                self.fsm.persist_inbound()?;
                Ok(reaction)
            }
            Err(error) => self.reject_cancel(
                resolved.order_id.clone(),
                &cancel.orig_cl_ord_id,
                &cancel.cl_ord_id,
                CxlRejResponseTo::OrderCancelRequest,
                &error,
                now_ms,
            ),
        }
    }

    /// `OrderCancelReplaceRequest (G)` → the non-atomic [`VenueCommand::Replace`].
    /// A committed replace (add leg `Filled`/`Rested`) renders `ExecutionReport (8)
    /// Replaced` + the add leg's fills. A **whole-replace refusal** (cancel leg never
    /// removed the original) and a **partial-replace failure** (cancel succeeded, add
    /// rejected — the original is gone, no new order rests) both render
    /// `OrderCancelReject (9)`; only the whole refusal keeps the original tracked, and
    /// neither tracks the rejected replacement (no phantom `F`/`G` correlation, #118).
    async fn route_replace(
        &mut self,
        replace: OrderCancelReplaceRequest,
        stamp: IngressStamp,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let Some(account) = self.fsm.account.clone() else {
            return Ok(Reaction::cont());
        };
        if self.rate_limited() {
            return self.reject_cancel(
                VenueOrderId::new("NONE"),
                &replace.orig_cl_ord_id,
                &replace.cl_ord_id,
                CxlRejResponseTo::OrderCancelReplaceRequest,
                &VenueError::RateLimited,
                now_ms,
            );
        }
        // Resolve `OrigClOrdID` cross-session (#098): this session's map first, then
        // the account-scoped venue index — so a replace of an order placed on a prior
        // connection succeeds instead of `9 Unknown order`. An unknown id (or one
        // owned by another account) is an indistinguishable masked reject (#118).
        let Some(resolved) = self.resolve_order(&account, &replace.orig_cl_ord_id) else {
            return self.reject_cancel(
                VenueOrderId::new("NONE"),
                &replace.orig_cl_ord_id,
                &replace.cl_ord_id,
                CxlRejResponseTo::OrderCancelReplaceRequest,
                &VenueError::NotFound(order_flow::CANCEL_REJECT_MASKED_REASON.to_string()),
                now_ms,
            );
        };
        let underlying = underlying_of_symbol(&replace.symbol);
        let new_order_id = mint_order_id(self.state.lineage_id(), &underlying);
        let command = order_flow::to_replace_command(
            &replace,
            account.clone(),
            resolved.order_id.clone(),
            new_order_id.clone(),
        );
        match self.state.submit_with_ingress(command, stamp).await {
            Ok(receipt) => {
                // Render the OBSERVED outcome (#118), never a false `Replaced`. A replace has
                // TWO distinct failure shapes the order path captures losslessly, and each
                // renders differently because the ORIGINAL order ends in a different state:
                let reaction = match &receipt.outcome {
                    // Whole-replace refusal (the cancel leg never removed the target: unknown
                    // / unowned / already-gone original). The ORIGINAL still rests untouched,
                    // so keep its tracking (do not re-key) and emit the TYPED masked
                    // `OrderCancelReject (9)` — a not-owner reject is masked as not-found so
                    // the reject can never distinguish not-found vs not-owner (#118/#132).
                    Some(VenueOutcome::Rejected { kind, reason }) => {
                        let masked = Self::masked_cancel_error(&resolved.order_id, *kind, reason);
                        self.reject_cancel(
                            resolved.order_id.clone(),
                            &replace.orig_cl_ord_id,
                            &replace.cl_ord_id,
                            CxlRejResponseTo::OrderCancelReplaceRequest,
                            &masked,
                            now_ms,
                        )?
                    }
                    // Partial-replace failure (cancel succeeded, replacement add rejected —
                    // the defined non-atomic `Replace { cancelled: true, add: Rejected }`
                    // state, ADR-0009). The ORIGINAL is gone and NO new order rests, so drop
                    // the now-stale original tracking and do NOT track the rejected new order
                    // (that phantom entry was the bug: it fabricated an `F`/`G` correlation to
                    // an order that never entered the book, and a false `Replaced`). The add
                    // leg's reason is order-/instrument-level — the cancel leg already proved
                    // ownership, so this is NOT the cross-account mask (#132) and it is named.
                    Some(VenueOutcome::Replace {
                        cancelled,
                        add: AddOutcome::Rejected { reason, .. },
                    }) => {
                        if *cancelled {
                            self.placed.remove(&replace.orig_cl_ord_id);
                        }
                        self.reject_replace_add(
                            resolved.order_id.clone(),
                            &replace.orig_cl_ord_id,
                            &replace.cl_ord_id,
                            reason,
                            now_ms,
                        )?
                    }
                    _ => {
                        // The old order is replaced; re-key tracking under the new ClOrdID.
                        self.placed.remove(&replace.orig_cl_ord_id);
                        self.track_placed(
                            replace.cl_ord_id.clone(),
                            new_order_id.clone(),
                            replace.symbol.clone(),
                            replace.side,
                            replace.order_qty,
                            OrderFingerprint::of_replace(&replace),
                        );
                        let legs = immediate_execution_records(
                            &self.state,
                            &account,
                            &new_order_id,
                            receipt.underlying_sequence,
                        );
                        let specs = order_flow::render_replace_reports(
                            &replace,
                            &new_order_id,
                            receipt.underlying_sequence,
                            receipt.venue_ts,
                            self.state.lineage_id(),
                            &underlying,
                            &legs,
                        );
                        self.fsm.emit_report_specs(specs, now_ms)?
                    }
                };
                // The replace's exchange effect is durably committed; persist the
                // deferred inbound-seq consumption post-effect (#149 finding 1A). The
                // submit `Err` arm (below) does NOT persist.
                self.fsm.persist_inbound()?;
                Ok(reaction)
            }
            Err(error) => self.reject_cancel(
                resolved.order_id.clone(),
                &replace.orig_cl_ord_id,
                &replace.cl_ord_id,
                CxlRejResponseTo::OrderCancelReplaceRequest,
                &error,
                now_ms,
            ),
        }
    }

    /// The `OrderCancelReject (9)` for an `F`/`G` failure.
    fn reject_cancel(
        &mut self,
        order_id: VenueOrderId,
        orig_cl_ord_id: &ClientOrderId,
        cl_ord_id: &ClientOrderId,
        response_to: CxlRejResponseTo,
        error: &VenueError,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let reject = error.fix_reject(FixRejectContext::CancelReplace);
        self.fsm.emit_cancel_reject_error(
            order_id,
            orig_cl_ord_id.clone(),
            cl_ord_id.clone(),
            response_to,
            OrdStatus::Rejected,
            &reject,
            now_ms,
        )
    }

    /// The **masked** [`VenueError`] for an observed [`VenueOutcome::Rejected`] on an
    /// `F`/`G` (#118/#132), keyed on the **typed** [`RejectKind`] — never the human
    /// reason string. The order path's specific journaled reason (`order not found`,
    /// the not-owner reason, or `order is not resting`) is logged internally, but the
    /// authorization-sensitive existence kinds ([`RejectKind::NotOwner`] /
    /// [`RejectKind::NotFound`] / [`RejectKind::NotResting`]) all resolve to the SAME
    /// masked not-found error, so the resulting `OrderCancelReject (9)` carries the
    /// uniform `Text (58)` + `CxlRejReason (102) = 1` identical to the never-placed
    /// reject and can never distinguish not-found vs not-owner vs already-gone (a
    /// cross-account enumeration oracle). The caller emits the reject via
    /// [`reject_cancel`](Self::reject_cancel) with the client's own placed
    /// `OrderID (37)`, not a cross-account leak.
    fn masked_cancel_error(
        order_id: &VenueOrderId,
        kind: RejectKind,
        internal_reason: &str,
    ) -> VenueError {
        tracing::info!(
            order_id = %order_id.as_str(),
            reject_kind = ?kind,
            internal_reason = %internal_reason,
            "cancel/replace rejected on the sequenced order path; emitting a uniform \
             client-safe reject (not-owner ≡ not-found, #132)"
        );
        VenueError::masked_cancel_reject(kind, order_flow::CANCEL_REJECT_MASKED_REASON)
    }

    /// The `OrderCancelReject (9)` for a **partial-replace failure** — the cancel leg
    /// removed the original but the replacement add was rejected
    /// ([`VenueOutcome::Replace { add: AddOutcome::Rejected, .. }`](crate::exchange::VenueOutcome::Replace),
    /// ADR-0009). Unlike [`reject_cancel_masked`](Self::reject_cancel_masked) this NAMES
    /// the reason in `Text (58)`: the cancel leg already proved ownership, so the add-leg
    /// rejection is order-/instrument-level (bad replacement price, halted/settling
    /// instrument) and carries no cross-account signal to mask (#132). It is reported as
    /// a rejected `G` (`OrderCancelReject (9)`, [03 §5](../../../docs/03-protocol-surfaces.md))
    /// carrying `OrdStatus (39) = Canceled` — the cancel leg committed, so the original's
    /// terminal state IS canceled, not `Rejected`; the caller has already dropped the
    /// original tracking because the order is gone.
    fn reject_replace_add(
        &mut self,
        order_id: VenueOrderId,
        orig_cl_ord_id: &ClientOrderId,
        cl_ord_id: &ClientOrderId,
        reason: &str,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        tracing::info!(
            order_id = %order_id.as_str(),
            reason = %reason,
            "partial replace: cancel leg committed but the replacement add was rejected; \
             original is gone, no new order rests"
        );
        // The cancel leg already committed, so the original order's terminal state is
        // `Canceled` — reporting `Rejected` here would falsely describe it as never
        // accepted and diverge the client's FIX state from the venue (#132).
        let reject = VenueError::InvalidOrder(reason.to_string())
            .fix_reject(FixRejectContext::CancelReplace);
        self.fsm.emit_cancel_reject_error(
            order_id,
            orig_cl_ord_id.clone(),
            cl_ord_id.clone(),
            CxlRejResponseTo::OrderCancelReplaceRequest,
            OrdStatus::Canceled,
            &reject,
            now_ms,
        )
    }

    /// `OrderStatusRequest (H)` → an `ExecutionReport (8)` current status folded
    /// from the order's committed fills. The gateway cannot read the resting book,
    /// so a resting-but-unfilled order reports `New`; an order not tracked this
    /// session (unknown, prior-session, or past the tracking cap) is an honest
    /// unknown-order rejection.
    fn route_status(
        &mut self,
        status: OrderStatusRequest,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let Some(account) = self.fsm.account.clone() else {
            return Ok(Reaction::cont());
        };
        let resolved = match (&status.order_id, &status.cl_ord_id) {
            // Status by `OrderID (37)`: a per-session reverse lookup (there is no
            // venue-wide `order_id → ClOrdID` index; the #098 index is keyed on
            // `ClOrdID`, so cross-session status is by `ClOrdID` below).
            (Some(order_id), _) => self
                .placed
                .values()
                .find(|placed| &placed.order_id == order_id)
                .map(PlacedOrder::resolved),
            // Status by `ClOrdID (11)`: resolved cross-session on the authenticated
            // account (#098).
            (None, Some(cl_ord_id)) => self.resolve_order(&account, cl_ord_id),
            // Decode requires one of OrderID(37)/ClOrdID(11); this is defensive.
            (None, None) => None,
        };
        let Some(resolved) = resolved else {
            let reject = VenueError::NotFound("order not found or not readable".to_string())
                .fix_reject(FixRejectContext::NewOrder);
            return self.fsm.emit_order_rejected(
                status.symbol.clone(),
                OrderSide::Buy,
                None,
                &reject,
                now_ms,
            );
        };
        self.render_tracked_status(&resolved, now_ms)
    }

    // ------------------------------------------------------------------------
    // Market-data routing (#040) — a thin projection of the shared #014 manager
    // ------------------------------------------------------------------------

    /// `MarketDataRequest (V)` → a subscription onto the **same**
    /// [`OrderbookSubscriptionManager`](crate::subscription::OrderbookSubscriptionManager)
    /// the WS surface reads. A snapshot request emits one `W` baseline per symbol
    /// and streams `X` deltas; an unsubscribe tears the symbols down. An
    /// unsupported request is a `MarketDataRequestReject (Y)`, never a bare
    /// `Reject (3)` (03 §8). `handle_active` has already gated `Read` + consumed
    /// the inbound seq.
    fn route_market_data(
        &mut self,
        request: MarketDataRequest,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        match request.subscription_request_type {
            SubscriptionRequestType::SnapshotPlusUpdates => {
                self.subscribe_market_data(request, now_ms)
            }
            SubscriptionRequestType::Unsubscribe => self.unsubscribe_market_data(&request),
        }
    }

    /// Whether an `MDReqID (262)` already backs a live subscription on this session
    /// (a duplicate id is rejected with `Y`).
    fn md_req_id_active(&self, md_req_id: &str) -> bool {
        self.market_data
            .as_ref()
            .is_some_and(|md| md.symbols.values().any(|sub| sub.md_req_id == md_req_id))
    }

    /// Subscribes the request's symbols (`SubscriptionRequestType = 1`), emitting
    /// one `W` snapshot baseline per symbol. Validates the request first, whole,
    /// never a partial subscribe — each of a Trade entry type (`269 = 2`, the
    /// permanently-out trade tape), a duplicate `MDReqID`, a re-subscribe of an
    /// already-live symbol, or a request past the per-session subscription ceiling
    /// is a `Y`.
    fn subscribe_market_data(
        &mut self,
        request: MarketDataRequest,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let sides = RequestedSides::from_entry_types(&request.entry_types);
        // A `V` carrying a Trade entry type (`269 = 2`) — a trade-tape-only request
        // OR a mixed request pairing Trade with a book side — is not served by the
        // FIX MD orderbook surface: the trade tape is permanently out (no book
        // snapshot, and its own separate `instrument_sequence` namespace that
        // `W`/`X`'s single `RptSeq` cannot carry under one `MDReqID`, #101). Reject the whole
        // request with `Y`, never silently serving the book side while dropping the
        // Trade entry type; per-fill detail reaches a FIX client via
        // `ExecutionReport (8)`.
        if md_projection::requests_trade_tape(&request.entry_types) {
            return self.fsm.emit_md_request_reject(
                request.md_req_id,
                MD_REJ_REASON_UNSUPPORTED_ENTRY_TYPE,
                Some(
                    "Trade (269=2) market data is not served over FIX; request only Bid/Offer"
                        .to_string(),
                ),
                now_ms,
            );
        }
        // Defensive: with Trade rejected above and every decoded `MDEntryType` one of
        // Bid/Offer/Trade over a non-empty group, a book side is always present here —
        // but guard the empty case explicitly rather than emit an empty subscription.
        if !sides.any() {
            return self.fsm.emit_md_request_reject(
                request.md_req_id,
                MD_REJ_REASON_UNSUPPORTED_ENTRY_TYPE,
                Some("only Bid/Offer market data is served over FIX".to_string()),
                now_ms,
            );
        }
        // Duplicate `MDReqID`: an id already backing a live subscription.
        if self.md_req_id_active(&request.md_req_id) {
            return self.fsm.emit_md_request_reject(
                request.md_req_id,
                MD_REJ_REASON_DUPLICATE,
                Some("duplicate MDReqID".to_string()),
                now_ms,
            );
        }
        // Intra-request duplicate symbols (#101): a single `V` that names the SAME
        // symbol twice would emit duplicate `W` snapshots, overwrite its own map
        // entry, and double-count the symbol against the per-session ceiling. Validate
        // uniqueness ACROSS `request.symbols` BEFORE the live-set check below (the live
        // check only catches a symbol already subscribed under a prior `MDReqID`, not a
        // self-duplicate). Reject the whole request with `Y`, exactly like the
        // already-live-symbol case.
        {
            let mut seen = HashSet::with_capacity(request.symbols.len());
            if let Some(duplicate) = request
                .symbols
                .iter()
                .find(|symbol| !seen.insert((*symbol).clone()))
            {
                tracing::debug!(
                    peer = %self.peer,
                    symbol = duplicate.as_str(),
                    "fix market-data request names a symbol more than once; rejecting (no duplicate snapshot / double-count)"
                );
                return self.fsm.emit_md_request_reject(
                    request.md_req_id,
                    MD_REJ_REASON_DUPLICATE,
                    Some("duplicate symbol within the market-data request".to_string()),
                    now_ms,
                );
            }
        }
        // Re-subscribe of an already-live symbol (security P3, #101): a second `V`
        // (a NEW `MDReqID`) naming a symbol already subscribed on this session — under
        // this or ANY other `MDReqID` — must NOT silently overwrite the prior
        // [`MdSymbolSub`], which would orphan the earlier `MDReqID` with no `Y` or
        // signal. Reject the whole request with `Y`, leaving the existing
        // subscription (and its `MDReqID`) live and untouched; a client that wants to
        // change a subscription unsubscribes (`263 = 2`) first, then re-subscribes.
        if let Some(existing) = self.market_data.as_ref()
            && let Some(duplicate) = request
                .symbols
                .iter()
                .find(|symbol| existing.symbols.contains_key(*symbol))
        {
            tracing::debug!(
                peer = %self.peer,
                symbol = duplicate.as_str(),
                "fix market-data re-subscribe of an already-subscribed symbol; rejecting (no silent overwrite)"
            );
            return self.fsm.emit_md_request_reject(
                request.md_req_id,
                MD_REJ_REASON_DUPLICATE,
                Some(
                    "symbol already subscribed on this session; unsubscribe (263=2) before re-subscribing"
                        .to_string(),
                ),
                now_ms,
            );
        }
        // Bandwidth ceiling: the request would grow the subscription set past the
        // per-session cap (count only symbols not already tracked).
        let current = self.market_data.as_ref().map_or(0, |md| md.symbols.len());
        let new_symbols = request
            .symbols
            .iter()
            .filter(|symbol| {
                self.market_data
                    .as_ref()
                    .is_none_or(|md| !md.symbols.contains_key(*symbol))
            })
            .count();
        if current.saturating_add(new_symbols) > MAX_MD_SYMBOLS_PER_SESSION {
            return self.fsm.emit_md_request_reject(
                request.md_req_id,
                MD_REJ_REASON_INSUFFICIENT_BANDWIDTH,
                Some("market-data subscription limit reached".to_string()),
                now_ms,
            );
        }

        let depth = market_depth_to_option(request.market_depth);

        // Subscribe to the venue-wide broadcast BEFORE reading any snapshot, so no
        // committed delta between the snapshot read and the receiver's creation is
        // lost — the client de-dups by `instrument_sequence`, exactly as on WS.
        if self.market_data.is_none() {
            self.market_data = Some(MdSubscription {
                receiver: self.state.subscriptions().subscribe(),
                symbols: HashMap::new(),
            });
        }

        // Register each symbol and emit its `W` baseline snapshot. The subscription's
        // `baseline` is seeded to the delivered `W`'s `instrument_sequence`, so the
        // first `X` the drain emits is strictly after it (the race-window delta
        // already folded into the snapshot is not re-sent as a redundant `X`).
        let mut frames = Vec::with_capacity(request.symbols.len());
        for symbol in &request.symbols {
            let snapshot = self.state.subscriptions().orderbook_snapshot(symbol, depth);
            let projection = md_projection::snapshot_projection(&snapshot, sides);
            let baseline = projection.as_ref().map_or(0, |(sequence, _)| *sequence);
            if let Some(md) = self.market_data.as_mut() {
                md.symbols.insert(
                    symbol.clone(),
                    MdSymbolSub {
                        md_req_id: request.md_req_id.clone(),
                        sides,
                        depth,
                        baseline,
                    },
                );
            }
            if let Some((sequence, entries)) = projection {
                let frame = self.fsm.emit_md_snapshot(
                    request.md_req_id.clone(),
                    symbol.clone(),
                    SequenceNumber::new(sequence),
                    entries,
                    now_ms,
                )?;
                frames.push(frame);
            }
        }
        Ok(Reaction::emit(frames))
    }

    /// Unsubscribes the request's symbols (`SubscriptionRequestType = 2`). FIX MD
    /// has no unsubscribe ack — the client simply stops receiving `W`/`X`; the
    /// receiver is torn down once the subscription set is empty.
    fn unsubscribe_market_data(
        &mut self,
        request: &MarketDataRequest,
    ) -> Result<Reaction, SessionError> {
        if let Some(md) = self.market_data.as_mut() {
            for symbol in &request.symbols {
                md.symbols.remove(symbol);
            }
            if md.symbols.is_empty() {
                self.market_data = None;
            }
        }
        Ok(Reaction::cont())
    }

    /// Drains the committed market-data broadcast into sequenced `X` frames for the
    /// subscribed symbols (bounded at [`MAX_MD_FRAMES_PER_CYCLE`] per cycle). A
    /// lagged receiver recovers by a fresh `W` per subscription (the WS gap
    /// contract); a closed broadcast (venue shutdown) tears the subscription down.
    fn drain_market_data(&mut self, now_ms: u64) -> Result<Vec<Vec<u8>>, SessionError> {
        // Phase 1: pull the pending messages, borrowing only the receiver.
        let mut pending: Vec<WsMessage> = Vec::new();
        let mut lagged = false;
        {
            let Some(md) = self.market_data.as_mut() else {
                return Ok(Vec::new());
            };
            loop {
                if pending.len() >= MAX_MD_FRAMES_PER_CYCLE {
                    break;
                }
                match md.receiver.try_recv() {
                    Ok(message) => pending.push(message),
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Lagged(_)) => {
                        // A structural bound (not timing-dependent): a gap means the
                        // whole cycle re-snapshots and discards `pending` regardless,
                        // so stop draining now rather than spin on the receiver.
                        lagged = true;
                        break;
                    }
                    Err(broadcast::error::TryRecvError::Closed) => {
                        self.market_data = None;
                        return Ok(Vec::new());
                    }
                }
            }
        }

        // Phase 2 — recovery: a lagged receiver dropped committed deltas, so the
        // `RptSeq` stream has a gap. Per the WS gap contract, recover by a fresh `W`
        // baseline per subscription and discard the post-gap deltas — never a
        // `ResendRequest` (that repairs only session `MsgSeqNum`, a distinct
        // namespace).
        if lagged {
            return self.resnapshot_subscriptions(now_ms);
        }

        // Phase 3: project each committed `orderbook_delta` onto an `X` for a
        // subscribed symbol, in order, onto the same outbound counter.
        let mut frames = Vec::new();
        for message in pending {
            let WsMessage::OrderbookDelta {
                symbol,
                sequence,
                changes,
            } = &message
            else {
                // fill / trade / price prints are not the orderbook projection (#040).
                continue;
            };
            let Some((baseline, sides, md_req_id)) = self
                .market_data
                .as_ref()
                .and_then(|md| md.symbols.get(symbol))
                .map(|sub| (sub.baseline, sub.sides, sub.md_req_id.clone()))
            else {
                continue; // not a symbol this session subscribed
            };
            // Baseline filter (mirrors `src/gateway/ws/mod.rs`): a delta at or below
            // the last-reflected `instrument_sequence` is already in the client's
            // snapshot / prior `X` — drop it so the FIX and WS streams are identical
            // and `RptSeq` strictly increases across the `W → X` boundary.
            if *sequence <= baseline {
                continue;
            }
            // Advance the baseline for every subscribed-symbol delta we advance past
            // (seen, exactly as WS does), whether or not it projects to a requested
            // side.
            if let Some(md) = self.market_data.as_mut()
                && let Some(entry) = md.symbols.get_mut(symbol)
            {
                entry.baseline = *sequence;
            }
            let entries = md_projection::incremental_entries(symbol, changes, sides);
            if entries.is_empty() {
                continue; // the delta touched no requested side
            }
            let frame = self.fsm.emit_md_incremental(
                md_req_id,
                SequenceNumber::new(*sequence),
                entries,
                now_ms,
            )?;
            frames.push(frame);
        }
        Ok(frames)
    }

    /// Emits a fresh `W` snapshot for every live subscription — the market-data gap
    /// recovery (a lagged broadcast) and the WS re-snapshot contract's FIX twin.
    fn resnapshot_subscriptions(&mut self, now_ms: u64) -> Result<Vec<Vec<u8>>, SessionError> {
        let subs: Vec<(Symbol, MdSymbolSub)> = match self.market_data.as_ref() {
            Some(md) => md
                .symbols
                .iter()
                .map(|(symbol, sub)| (symbol.clone(), sub.clone()))
                .collect(),
            None => return Ok(Vec::new()),
        };
        tracing::debug!(
            peer = %self.peer,
            subscriptions = subs.len(),
            "fix market-data receiver lagged; re-snapshotting (fresh W, not a resend)"
        );
        let mut frames = Vec::with_capacity(subs.len());
        for (symbol, sub) in subs {
            let snapshot = self
                .state
                .subscriptions()
                .orderbook_snapshot(&symbol, sub.depth);
            if let Some((sequence, entries)) =
                md_projection::snapshot_projection(&snapshot, sub.sides)
            {
                let frame = self.fsm.emit_md_snapshot(
                    sub.md_req_id,
                    symbol.clone(),
                    SequenceNumber::new(sequence),
                    entries,
                    now_ms,
                )?;
                frames.push(frame);
                // Re-baseline the live subscription to the fresh `W`'s sequence, so a
                // subsequent `X` is strictly after the new snapshot (WS re-snapshot
                // semantics).
                if let Some(md) = self.market_data.as_mut()
                    && let Some(entry) = md.symbols.get_mut(&symbol)
                {
                    entry.baseline = sequence;
                }
            }
        }
        Ok(frames)
    }

    /// Appends any pending market-data frames onto a step's [`Reaction`] so `W`/`X`
    /// ride the same bounded outbound mailbox as the session/order replies, sharing
    /// one monotonic outbound `MsgSeqNum`. A closing step or a session with no live
    /// subscription is left untouched.
    fn append_market_data(
        &mut self,
        result: Result<Reaction, SessionError>,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let mut reaction = result?;
        if reaction.control == SessionControl::Close || self.market_data.is_none() {
            return Ok(reaction);
        }
        let md_frames = self.drain_market_data(now_ms)?;
        reaction.frames.extend(md_frames);
        Ok(reaction)
    }
}

/// Maps `MarketDepth (264)` to the snapshot depth: `0` = full book (`None`), else
/// the top `N` levels.
fn market_depth_to_option(market_depth: u32) -> Option<usize> {
    if market_depth == 0 {
        None
    } else {
        Some(market_depth as usize)
    }
}

impl FixSession for VenueFixSession {
    async fn on_message(
        &mut self,
        message: DecodedMessage,
        out: &SessionOutbound,
    ) -> SessionControl {
        let now_ms = self.now_ms();
        let result = match self.fsm.phase() {
            SessionPhase::AwaitingLogon => match message {
                DecodedMessage::Logon(logon) => {
                    // Set the reply identity from the logon before any reject can
                    // address the peer.
                    self.fsm.on_inbound(&logon.header, now_ms);
                    self.handle_logon(logon, now_ms).await
                }
                other => {
                    // A non-logon before logon is a protocol violation: address a
                    // Logout to the peer and close.
                    self.fsm.on_inbound(header_of(&other), now_ms);
                    self.fsm.logout_close(now_ms, "expected Logon")
                }
            },
            SessionPhase::Active | SessionPhase::AwaitingResend => {
                let revoked = self.revoked();
                match self.fsm.handle_active(message, now_ms, revoked) {
                    // A synchronous session-admin reply or a gated reject.
                    Ok(ActiveDisposition::Reacted(reaction)) => Ok(reaction),
                    // A permitted order-entry message: submit onto the sequenced
                    // path and render its reports on the same outbound counter.
                    Ok(ActiveDisposition::Route(order)) => self.route_order(*order, now_ms).await,
                    Err(error) => Err(error),
                }
            }
            SessionPhase::Closing => Ok(Reaction::close_silent()),
        };
        // Drain any pending market-data deltas onto the same outbound counter, so a
        // subscribed session sees `X` on any inbound activity (and on `on_tick`).
        let result = self.append_market_data(result, now_ms);
        self.flush(result, out)
    }

    async fn on_decode_error(
        &mut self,
        error: &super::FixDecodeError,
        out: &SessionOutbound,
    ) -> SessionControl {
        let now_ms = self.now_ms();
        let result = match self.fsm.phase() {
            // A malformed frame before logon cannot establish a session: close.
            SessionPhase::AwaitingLogon | SessionPhase::Closing => Ok(Reaction::close_silent()),
            SessionPhase::Active | SessionPhase::AwaitingResend => {
                self.fsm.reject_decode_error(error, now_ms)
            }
        };
        self.flush(result, out)
    }

    async fn on_tick(&mut self, out: &SessionOutbound) -> SessionControl {
        let now_ms = self.now_ms();
        let revoked = self.revoked();
        let result = self.fsm.on_tick(now_ms, revoked);
        // The steady-state market-data pump: a passive subscriber (one that sends no
        // frames) receives its `X` deltas on the session cadence tick.
        let result = self.append_market_data(result, now_ms);
        self.flush(result, out)
    }
}

/// The [`FixSessionFactory`] that builds a real [`VenueFixSession`] per accepted
/// connection — the seam that replaces the #037 [`StubSessionFactory`](super::StubSessionFactory).
///
/// It holds the shared [`AppState`] (auth / registry / clock the gateway reaches
/// the venue through), the shared durable [`FixSessionStore`], and the shared
/// [`SessionLeaseRegistry`] enforcing per-`SessionKey` logon exclusivity across
/// every connection; the gateway depends on `AppState`, never the reverse.
#[derive(Clone)]
pub struct VenueFixSessionFactory {
    state: Arc<AppState>,
    store: Arc<dyn FixSessionStore>,
    config: SessionConfig,
    /// The venue-wide single-active-session lease registry, shared by every session
    /// this factory (and its clones) creates.
    leases: Arc<SessionLeaseRegistry>,
}

impl VenueFixSessionFactory {
    /// Wires a factory over the shared venue state, session store, and config,
    /// creating the venue-wide lease registry every created session shares.
    #[must_use]
    pub fn new(
        state: Arc<AppState>,
        store: Arc<dyn FixSessionStore>,
        config: SessionConfig,
    ) -> Self {
        Self {
            state,
            store,
            config,
            leases: Arc::new(SessionLeaseRegistry::new()),
        }
    }
}

impl FixSessionFactory for VenueFixSessionFactory {
    type Session = VenueFixSession;

    fn admit(&self) -> bool {
        // The AppState seam: do not admit FIX sessions before the venue is serving.
        self.state.is_serving()
    }

    fn create(&self, peer: SocketAddr) -> VenueFixSession {
        VenueFixSession::new(
            peer,
            Arc::clone(&self.state),
            Arc::clone(&self.store),
            self.config,
            Arc::clone(&self.leases),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::enums::{OrdType, OrderSide, TimeInForce};
    use super::super::{decode, order};
    use super::*;
    use crate::exchange::Symbol;
    use crate::models::ClientOrderId;
    use std::sync::Arc;

    const CLIENT: &str = "CLIENT";
    const VENUE: &str = "FAUXCHANGE";

    fn config() -> SessionConfig {
        SessionConfig {
            logon_timeout_ms: 10_000,
            max_heart_bt_int_secs: 60,
        }
    }

    fn store() -> Arc<dyn FixSessionStore> {
        Arc::new(super::super::store::InMemoryFixSessionStore::new())
    }

    fn header(sender: &str, target: &str, seq: u64) -> StandardHeader {
        StandardHeader::new(
            CompId::new(sender).expect("sender comp id"),
            CompId::new(target).expect("target comp id"),
            SeqNum::new(seq),
            UtcTimestamp::from_epoch_ms(0),
        )
    }

    /// A fresh FSM admitted to `Active` for `permissions`, with the given store.
    fn active_fsm(store: Arc<dyn FixSessionStore>, permissions: Vec<Permission>) -> SessionFsm {
        let mut fsm = SessionFsm::new(config(), store, 0);
        let logon_header = header(CLIENT, VENUE, 1);
        fsm.on_inbound(&logon_header, 0);
        fsm.admit_logon(AccountId::new("acct-1"), permissions, 0, 30, false, 1, 0)
            .expect("admit");
        fsm
    }

    /// Unwraps a synchronous [`ActiveDisposition::Reacted`], panicking on a
    /// [`Route`](ActiveDisposition::Route) (the message unexpectedly routed).
    fn reacted(disposition: ActiveDisposition) -> Reaction {
        match disposition {
            ActiveDisposition::Reacted(reaction) => reaction,
            ActiveDisposition::Route(message) => {
                panic!("expected a synchronous Reacted, got Route({message:?})")
            }
        }
    }

    fn heartbeat(seq: u64) -> DecodedMessage {
        DecodedMessage::Heartbeat(session::Heartbeat {
            header: header(CLIENT, VENUE, seq),
            test_req_id: None,
        })
    }

    fn new_order(seq: u64, account: Option<AccountId>) -> DecodedMessage {
        DecodedMessage::NewOrderSingle(order::NewOrderSingle {
            header: header(CLIENT, VENUE, seq),
            cl_ord_id: ClientOrderId::new("c-1"),
            account,
            symbol: Symbol::parse("BTC-20240329-50000-C").expect("symbol"),
            side: OrderSide::Buy,
            transact_time: UtcTimestamp::from_epoch_ms(0),
            ord_type: OrdType::Market,
            price: None,
            order_qty: 1,
            time_in_force: TimeInForce::Gtc,
            expire_time: None,
        })
    }

    #[test]
    fn test_admit_logon_transitions_to_active_and_acks_without_credentials() {
        let fsm = active_fsm(store(), vec![Permission::Trade]);
        assert_eq!(fsm.phase(), SessionPhase::Active);
        // The ack consumed sender seq 1; the logon consumed inbound seq 1.
        assert_eq!(fsm.counters().next_sender_seq, 2);
        assert_eq!(fsm.counters().next_target_seq, 2);
    }

    #[test]
    fn test_logon_ack_never_carries_a_password_field() {
        let store = store();
        let mut fsm = SessionFsm::new(config(), store, 0);
        let logon_header = header(CLIENT, VENUE, 1);
        fsm.on_inbound(&logon_header, 0);
        let reaction = fsm
            .admit_logon(
                AccountId::new("acct-1"),
                vec![Permission::Read],
                0,
                30,
                false,
                1,
                0,
            )
            .expect("admit");
        let ack = &reaction.frames()[0];
        let text = String::from_utf8_lossy(ack);
        // Tags 553 (Username) and 554 (Password) are absent from the ack.
        assert!(
            !text.contains("\u{1}553="),
            "ack must not carry Username(553)"
        );
        assert!(
            !text.contains("\u{1}554="),
            "ack must not carry Password(554)"
        );
    }

    #[test]
    fn test_admit_logon_reset_flag_journals_a_sequence_reset_event() {
        let store = store();
        let mut fsm = SessionFsm::new(config(), Arc::clone(&store), 0);
        let logon_header = header(CLIENT, VENUE, 1);
        fsm.on_inbound(&logon_header, 0);
        fsm.admit_logon(
            AccountId::new("acct-1"),
            vec![Permission::Read],
            0,
            30,
            true,
            1,
            1_000,
        )
        .expect("admit");
        let events = fsm.reset_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trigger, ResetTrigger::LogonReset);
        assert_eq!(events[0].at_ms, 1_000);
    }

    // ---- Reconnect MsgSeqNum validation (#96/#112, Bug 1): a non-reset logon is
    // validated against the STORED inbound expectation; the counter is never
    // silently overwritten downward.

    /// The durable [`SessionKey`] `admit_logon` resolves for the test account +
    /// `header(CLIENT, VENUE, _)` tuple — used to pre-seed and inspect the store.
    fn reconnect_key() -> SessionKey {
        SessionKey::new(AccountId::new("acct-1"), CLIENT, VENUE)
    }

    /// Drives a fresh FSM through a NON-reset `admit_logon` at `logon_seq` against a
    /// store pre-seeded to expect inbound `stored_target` (a prior session's state).
    fn admit_reconnect(
        stored_target: u64,
        logon_seq: u64,
    ) -> (
        SessionFsm,
        Arc<dyn FixSessionStore>,
        Result<Reaction, SessionError>,
    ) {
        let store = store();
        store
            .save_counters(
                &reconnect_key(),
                SessionCounters {
                    next_sender_seq: stored_target,
                    next_target_seq: stored_target,
                },
            )
            .expect("seed counters");
        let mut fsm = SessionFsm::new(config(), Arc::clone(&store), 0);
        fsm.on_inbound(&header(CLIENT, VENUE, logon_seq), 0);
        let result = fsm.admit_logon(
            AccountId::new("acct-1"),
            vec![Permission::Trade],
            0,
            30,
            false,
            logon_seq,
            0,
        );
        (fsm, store, result)
    }

    #[test]
    fn test_reconnect_below_stored_seq_without_reset_is_rejected_and_never_overwrites() {
        // Stored expectation is 5; a reconnect presenting seq 1 (no ResetSeqNumFlag)
        // is a backward jump that would replay consumed messages → Logout, closing,
        // and the stored inbound counter is NOT overwritten downward.
        let (fsm, store, result) = admit_reconnect(5, 1);
        let reaction = result.expect("admit returns a reaction");
        assert_eq!(
            reaction.control(),
            SessionControl::Close,
            "a stale-seq reconnect closes"
        );
        assert!(
            matches!(decode(&reaction.frames()[0]), Ok(DecodedMessage::Logout(_))),
            "a stale-seq reconnect is a Logout(5)"
        );
        assert_ne!(
            fsm.phase(),
            SessionPhase::Active,
            "a stale-seq reconnect never reaches Active"
        );
        assert_eq!(
            store
                .load_counters(&reconnect_key())
                .expect("load")
                .next_target_seq,
            5,
            "the durable inbound expectation is untouched (no downward overwrite)"
        );
    }

    #[test]
    fn test_reconnect_at_stored_seq_proceeds_to_active() {
        // Stored expectation is 5; a reconnect presenting exactly seq 5 is in-order.
        let (fsm, store, result) = admit_reconnect(5, 5);
        result.expect("admit ok");
        assert_eq!(fsm.phase(), SessionPhase::Active);
        assert_eq!(
            fsm.counters().next_target_seq,
            6,
            "the in-order logon consumed seq 5"
        );
        assert_eq!(
            store
                .load_counters(&reconnect_key())
                .expect("load")
                .next_target_seq,
            6
        );
    }

    #[test]
    fn test_reconnect_above_stored_seq_triggers_resend_request_and_awaits_resend() {
        // Stored expectation is 5; a reconnect presenting seq 8 has a gap [5, 7].
        let (fsm, store, result) = admit_reconnect(5, 8);
        let reaction = result.expect("admit ok");
        assert_eq!(
            fsm.phase(),
            SessionPhase::AwaitingResend,
            "a gap logon awaits resend"
        );
        assert_eq!(
            fsm.counters().next_target_seq,
            5,
            "the inbound expectation is NOT advanced past the gap"
        );
        assert_eq!(
            store
                .load_counters(&reconnect_key())
                .expect("load")
                .next_target_seq,
            5
        );
        // The ack (credential-free, undecodable inbound) is followed by a
        // ResendRequest beginning at the stored expectation.
        assert_eq!(reaction.frames().len(), 2, "ack + resend request");
        match decode(&reaction.frames()[1]) {
            Ok(DecodedMessage::ResendRequest(r)) => {
                assert_eq!(
                    r.begin_seq_no.value(),
                    5,
                    "resend begins at the stored expectation"
                );
                assert_eq!(r.end_seq_no.value(), 0, "EndSeqNo 0 = to the latest");
            }
            other => panic!("expected a ResendRequest after the ack, got {other:?}"),
        }
    }

    #[test]
    fn test_reset_flag_still_resets_even_below_stored_seq_and_audits() {
        // Even with a high stored expectation (5), ResetSeqNumFlag=Y legitimately
        // moves the counter back to 1 — the ONLY sanctioned backward path — reaches
        // Active, and journals the reset for audit.
        let store = store();
        store
            .save_counters(
                &reconnect_key(),
                SessionCounters {
                    next_sender_seq: 5,
                    next_target_seq: 5,
                },
            )
            .expect("seed counters");
        let mut fsm = SessionFsm::new(config(), Arc::clone(&store), 0);
        fsm.on_inbound(&header(CLIENT, VENUE, 1), 0);
        fsm.admit_logon(
            AccountId::new("acct-1"),
            vec![Permission::Trade],
            0,
            30,
            true,
            1,
            1_000,
        )
        .expect("admit");
        assert_eq!(fsm.phase(), SessionPhase::Active);
        assert_eq!(
            fsm.counters().next_target_seq,
            2,
            "the reset logon consumed inbound seq 1"
        );
        let events = fsm.reset_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trigger, ResetTrigger::LogonReset);
    }

    // ---- Per-SessionKey exclusivity lease (#96/#112, Bug 2).

    #[test]
    fn test_session_lease_is_exclusive_per_key_and_released_on_drop() {
        let registry = Arc::new(SessionLeaseRegistry::new());
        let key_a = SessionKey::new(AccountId::new("acct-1"), CLIENT, VENUE);
        let key_b = SessionKey::new(AccountId::new("acct-2"), CLIENT, VENUE);

        let lease_a = registry
            .try_claim(key_a.clone())
            .expect("first claim for A");
        assert!(registry.is_live(&key_a));
        // A second concurrent claim for the SAME key is refused (no double-lease).
        assert!(
            registry.try_claim(key_a.clone()).is_none(),
            "a live key cannot be leased twice"
        );
        // A DIFFERENT key is unaffected — it claims freely and concurrently.
        let lease_b = registry
            .try_claim(key_b.clone())
            .expect("a different key claims freely");
        assert!(registry.is_live(&key_b));

        // Dropping A's lease releases the key so a later claim succeeds (reconnect).
        drop(lease_a);
        assert!(!registry.is_live(&key_a));
        let lease_a2 = registry
            .try_claim(key_a.clone())
            .expect("re-claim after release");
        assert!(registry.is_live(&key_a));

        drop(lease_a2);
        drop(lease_b);
        assert!(!registry.is_live(&key_a));
        assert!(!registry.is_live(&key_b));
    }

    #[test]
    fn test_outbound_counter_exhaustion_returns_sequence_exhausted() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        fsm.counters.next_sender_seq = u64::MAX;
        // A TestRequest forces an outbound Heartbeat, whose seq increment overflows.
        let test = DecodedMessage::TestRequest(session::TestRequest {
            header: header(CLIENT, VENUE, 2),
            test_req_id: "TR-1".to_string(),
        });
        let result = fsm.handle_active(test, 0, false);
        assert!(matches!(result, Err(SessionError::SequenceExhausted)));
    }

    #[test]
    fn test_inbound_counter_exhaustion_returns_sequence_exhausted() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        fsm.counters.next_target_seq = u64::MAX;
        // An in-order message at u64::MAX overflows the inbound advance.
        let result = fsm.handle_active(heartbeat(u64::MAX), 0, false);
        assert!(matches!(result, Err(SessionError::SequenceExhausted)));
    }

    #[test]
    fn test_in_order_heartbeat_advances_inbound_counter() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let reaction = reacted(fsm.handle_active(heartbeat(2), 0, false).expect("ok"));
        assert!(reaction.frames().is_empty());
        assert_eq!(fsm.counters().next_target_seq, 3);
    }

    #[test]
    fn test_test_request_is_answered_with_heartbeat_echoing_the_id() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let test = DecodedMessage::TestRequest(session::TestRequest {
            header: header(CLIENT, VENUE, 2),
            test_req_id: "PING-42".to_string(),
        });
        let reaction = reacted(fsm.handle_active(test, 0, false).expect("ok"));
        match decode(&reaction.frames()[0]) {
            Ok(DecodedMessage::Heartbeat(hb)) => {
                assert_eq!(hb.test_req_id.as_deref(), Some("PING-42"));
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn test_inbound_gap_triggers_resend_request_and_awaiting_resend() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        // Expected seq 2; a seq-5 message is a gap.
        let reaction = reacted(fsm.handle_active(heartbeat(5), 0, false).expect("ok"));
        assert_eq!(fsm.phase(), SessionPhase::AwaitingResend);
        // The inbound counter is NOT advanced past the gap.
        assert_eq!(fsm.counters().next_target_seq, 2);
        match decode(&reaction.frames()[0]) {
            Ok(DecodedMessage::ResendRequest(r)) => {
                assert_eq!(r.begin_seq_no.value(), 2);
                assert_eq!(r.end_seq_no.value(), 0);
            }
            other => panic!("expected ResendRequest, got {other:?}"),
        }
    }

    #[test]
    fn test_permission_gate_refuses_order_from_read_only_session_order_level() {
        let mut fsm = active_fsm(store(), vec![Permission::Read]);
        let reaction = reacted(fsm.handle_active(new_order(2, None), 0, false).expect("ok"));
        match decode(&reaction.frames()[0]) {
            Ok(DecodedMessage::ExecutionReport(report)) => {
                assert_eq!(report.ord_status, OrdStatus::Rejected);
                assert_eq!(report.exec_type, ExecType::Rejected);
            }
            other => panic!("expected an order-level ExecutionReport Rejected, got {other:?}"),
        }
        // The message is still consumed at the session level.
        assert_eq!(fsm.counters().next_target_seq, 3);
    }

    #[test]
    fn test_permission_gate_routes_order_from_trade_session() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        // A permitted, attributed order routes to the async order path (#039); the
        // inbound seq is consumed and no synchronous reject frame is produced.
        match fsm.handle_active(new_order(2, None), 0, false).expect("ok") {
            ActiveDisposition::Route(message) => {
                assert!(matches!(*message, DecodedMessage::NewOrderSingle(_)));
            }
            other => panic!("expected Route(NewOrderSingle), got {other:?}"),
        }
        assert_eq!(fsm.counters().next_target_seq, 3);
    }

    #[test]
    fn test_account_field_mismatch_is_a_session_reject() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let foreign = Some(AccountId::new("someone-else"));
        let reaction = reacted(
            fsm.handle_active(new_order(2, foreign), 0, false)
                .expect("ok"),
        );
        match decode(&reaction.frames()[0]) {
            Ok(DecodedMessage::Reject(reject)) => {
                assert_eq!(reject.ref_tag_id, Some(tags::ACCOUNT));
                assert_eq!(
                    reject.session_reject_reason,
                    Some(SessionRejectReason::ValueIsIncorrect)
                );
            }
            other => panic!("expected a session Reject, got {other:?}"),
        }
    }

    #[test]
    fn test_account_field_equal_to_authenticated_routes() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let same = Some(AccountId::new("acct-1"));
        match fsm.handle_active(new_order(2, same), 0, false).expect("ok") {
            ActiveDisposition::Route(message) => {
                assert!(matches!(*message, DecodedMessage::NewOrderSingle(_)));
            }
            other => panic!("expected Route(NewOrderSingle), got {other:?}"),
        }
    }

    /// A store that can be told to FAIL [`store_outbound_and_advance`](FixSessionStore::store_outbound_and_advance)
    /// atomically (persisting nothing), delegating every other call to an inner
    /// in-memory store — so a test can prove `emit` never reuses a `MsgSeqNum` across
    /// a mid-emit failure (#149 finding 1B).
    #[derive(Debug)]
    struct FailAdvanceStore {
        inner: super::super::store::InMemoryFixSessionStore,
        fail: std::sync::atomic::AtomicBool,
    }

    impl FailAdvanceStore {
        fn new() -> Self {
            Self {
                inner: super::super::store::InMemoryFixSessionStore::new(),
                fail: std::sync::atomic::AtomicBool::new(false),
            }
        }
        fn set_fail(&self, fail: bool) {
            self.fail.store(fail, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl FixSessionStore for FailAdvanceStore {
        fn load_counters(
            &self,
            key: &SessionKey,
        ) -> Result<SessionCounters, super::super::store::SessionStoreError> {
            self.inner.load_counters(key)
        }
        fn save_counters(
            &self,
            key: &SessionKey,
            counters: SessionCounters,
        ) -> Result<(), super::super::store::SessionStoreError> {
            self.inner.save_counters(key, counters)
        }
        fn store_outbound(
            &self,
            key: &SessionKey,
            seq: u64,
            frame: &[u8],
        ) -> Result<(), super::super::store::SessionStoreError> {
            self.inner.store_outbound(key, seq, frame)
        }
        fn store_outbound_and_advance(
            &self,
            key: &SessionKey,
            seq: u64,
            frame: &[u8],
            next_sender_seq: u64,
        ) -> Result<(), super::super::store::SessionStoreError> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                // Atomic failure: persist NOTHING — no frame, no counter advance.
                return Err(super::super::store::SessionStoreError::Backend(
                    "injected mid-emit failure",
                ));
            }
            self.inner
                .store_outbound_and_advance(key, seq, frame, next_sender_seq)
        }
        fn outbound_range(
            &self,
            key: &SessionKey,
            begin: u64,
            end: u64,
        ) -> Result<Vec<StoredOutbound>, super::super::store::SessionStoreError> {
            self.inner.outbound_range(key, begin, end)
        }
        fn record_reset(
            &self,
            key: &SessionKey,
            event: SequenceResetEvent,
            counters: SessionCounters,
        ) -> Result<(), super::super::store::SessionStoreError> {
            self.inner.record_reset(key, event, counters)
        }
        fn reset_events(
            &self,
            key: &SessionKey,
        ) -> Result<Vec<SequenceResetEvent>, super::super::store::SessionStoreError> {
            self.inner.reset_events(key)
        }
    }

    #[test]
    fn test_emit_never_reuses_msg_seq_num_across_a_failed_store() {
        // #149 finding 1B: `emit` stores the frame AND advances the outbound counter
        // in ONE atomic store op. When that op fails, NEITHER is applied, so a retry
        // reuses the same seq WITHOUT ever producing two frames at one MsgSeqNum — the
        // duplicate-MsgSeqNum window the old `store_outbound` + `save_counters` pair
        // left open is closed.
        let store = Arc::new(FailAdvanceStore::new());
        let mut fsm = SessionFsm::new(config(), Arc::clone(&store) as Arc<dyn FixSessionStore>, 0);
        fsm.on_inbound(&header(CLIENT, VENUE, 1), 0);
        fsm.admit_logon(
            AccountId::new("acct-1"),
            vec![Permission::Trade],
            0,
            30,
            false,
            1,
            0,
        )
        .expect("admit");
        // After the logon ack, the next outbound seq is 2.
        assert_eq!(fsm.counters().next_sender_seq, 2);

        // Inject a mid-emit store failure: a TestRequest would emit a Heartbeat.
        store.set_fail(true);
        let test = DecodedMessage::TestRequest(session::TestRequest {
            header: header(CLIENT, VENUE, 2),
            test_req_id: "TR-1".to_string(),
        });
        let err = fsm
            .handle_active(test, 0, false)
            .expect_err("the emit must fail");
        assert!(
            matches!(err, SessionError::Store(_)),
            "a store failure seals the session, got {err:?}"
        );
        // The outbound counter did NOT advance, and NO frame was stored at seq 2.
        assert_eq!(
            fsm.counters().next_sender_seq,
            2,
            "no advance on a failed atomic emit"
        );
        assert!(
            store
                .outbound_range(&reconnect_key(), 2, 2)
                .expect("range")
                .is_empty(),
            "no frame stored at the un-advanced seq"
        );

        // Recover: the same inbound seq 2 (never consumed) drives one clean emit at
        // outbound seq 2 — used exactly once, never a duplicate.
        store.set_fail(false);
        let test2 = DecodedMessage::TestRequest(session::TestRequest {
            header: header(CLIENT, VENUE, 2),
            test_req_id: "TR-2".to_string(),
        });
        fsm.handle_active(test2, 0, false).expect("emit ok");
        assert_eq!(
            fsm.counters().next_sender_seq,
            3,
            "outbound seq advanced exactly once after recovery"
        );
        let at_2 = store.outbound_range(&reconnect_key(), 2, 2).expect("range");
        assert_eq!(
            at_2.len(),
            1,
            "exactly one frame at MsgSeqNum 2 — the seq is never reused"
        );
    }

    #[test]
    fn test_routed_mutation_defers_durable_inbound_advance_until_effect_commits() {
        // #149 finding 1A: a routed order-entry MUTATION (D/F/G/q) advances the
        // inbound counter IN MEMORY (so gap detection of the next frame is correct)
        // but must NOT persist it durably until the exchange effect commits. Proven at
        // the FSM seam: after `handle_active` routes the `D`, the in-memory counter is
        // advanced yet the DURABLE counter still points at the routed message's own
        // seq — so a crash here re-admits the client's resend (which the merged
        // exchange-side ClOrdID idempotency reprocesses), never dropping it as
        // already-seen.
        let store = store();
        let mut fsm = active_fsm(Arc::clone(&store), vec![Permission::Trade]);
        // After admit at logon seq 1: durable + in-memory next_target = 2.
        assert_eq!(
            store
                .load_counters(&reconnect_key())
                .expect("load")
                .next_target_seq,
            2,
            "logon consumed inbound seq 1, durably"
        );

        // Route a `D` at the expected inbound seq 2.
        match fsm
            .handle_active(new_order(2, None), 0, false)
            .expect("handle")
        {
            ActiveDisposition::Route(message) => {
                assert!(matches!(*message, DecodedMessage::NewOrderSingle(_)));
            }
            other => panic!("expected Route(NewOrderSingle), got {other:?}"),
        }

        // In memory: advanced (gap detection of the NEXT frame sees 3).
        assert_eq!(
            fsm.counters().next_target_seq,
            3,
            "in-memory advanced for gap detection"
        );
        // Durable: NOT advanced (deferred) — the store still admits a resend at seq 2.
        assert_eq!(
            store
                .load_counters(&reconnect_key())
                .expect("load")
                .next_target_seq,
            2,
            "durable inbound advance is DEFERRED until the exchange effect commits"
        );

        // The async router persists the deferred advance ONLY after `submit` commits.
        fsm.persist_inbound().expect("persist post-effect");
        assert_eq!(
            store
                .load_counters(&reconnect_key())
                .expect("load")
                .next_target_seq,
            3,
            "post-effect persist makes the inbound consumption durable"
        );
    }

    #[test]
    fn test_revoked_session_is_logged_out_and_closed() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let reaction = reacted(fsm.handle_active(heartbeat(2), 0, true).expect("ok"));
        assert_eq!(reaction.control(), SessionControl::Close);
        assert!(matches!(
            decode(&reaction.frames()[0]),
            Ok(DecodedMessage::Logout(_))
        ));
    }

    // ---- Market data (#040) ------------------------------------------------

    fn market_data_request(
        seq: u64,
        entry_types: Vec<super::super::enums::MdEntryType>,
    ) -> DecodedMessage {
        DecodedMessage::MarketDataRequest(MarketDataRequest {
            header: header(CLIENT, VENUE, seq),
            md_req_id: "MDR-1".to_string(),
            subscription_request_type: SubscriptionRequestType::SnapshotPlusUpdates,
            market_depth: 0,
            entry_types,
            symbols: vec![Symbol::parse("BTC-20240329-50000-C").expect("symbol")],
        })
    }

    #[test]
    fn test_market_data_request_routes_from_a_read_session() {
        // `V` requires `Read`; a Read session admits it and routes it to the async
        // market-data path (the subscription/W-X rendering needs `AppState`).
        let mut fsm = active_fsm(store(), vec![Permission::Read]);
        use super::super::enums::MdEntryType;
        match fsm
            .handle_active(
                market_data_request(2, vec![MdEntryType::Bid, MdEntryType::Offer]),
                0,
                false,
            )
            .expect("ok")
        {
            ActiveDisposition::Route(message) => {
                assert!(matches!(*message, DecodedMessage::MarketDataRequest(_)));
            }
            other => panic!("expected Route(MarketDataRequest), got {other:?}"),
        }
        // Consumed at the session level like any application message.
        assert_eq!(fsm.counters().next_target_seq, 3);
    }

    #[test]
    fn test_market_data_request_without_read_is_a_market_data_reject() {
        // A session with an empty permission set (no `Read`) is refused in the
        // market-data context — a `Y` (MDReqRejReason = 3), never a bare `Reject (3)`.
        let mut fsm = active_fsm(store(), Vec::new());
        use super::super::enums::MdEntryType;
        let reaction = reacted(
            fsm.handle_active(market_data_request(2, vec![MdEntryType::Bid]), 0, false)
                .expect("ok"),
        );
        match decode(&reaction.frames()[0]) {
            Ok(DecodedMessage::MarketDataRequestReject(reject)) => {
                assert_eq!(reject.md_req_id, "MDR-1");
                assert_eq!(
                    reject.md_req_rej_reason,
                    MD_REJ_REASON_INSUFFICIENT_PERMISSIONS
                );
            }
            other => panic!("expected MarketDataRequestReject, got {other:?}"),
        }
        // Still consumed at the session level.
        assert_eq!(fsm.counters().next_target_seq, 3);
    }

    #[test]
    fn test_emit_md_snapshot_encodes_a_decodable_w_carrying_rpt_seq() {
        let mut fsm = active_fsm(store(), vec![Permission::Read]);
        let frame = fsm
            .emit_md_snapshot(
                "MDR-1".to_string(),
                Symbol::parse("BTC-20240329-50000-C").expect("symbol"),
                SequenceNumber::new(42),
                vec![SnapshotEntry {
                    entry_type: super::super::enums::MdEntryType::Bid,
                    price: Cents::new(49_995),
                    size: 10,
                }],
                0,
            )
            .expect("emit W");
        match decode(&frame) {
            Ok(DecodedMessage::MarketDataSnapshotFullRefresh(w)) => {
                assert_eq!(
                    w.rpt_seq.get(),
                    42,
                    "RptSeq(83) carries the instrument_sequence"
                );
                assert_eq!(w.entries.len(), 1);
                assert_eq!(w.entries[0].price, Cents::new(49_995));
            }
            other => panic!("expected W, got {other:?}"),
        }
    }

    #[test]
    fn test_emit_md_incremental_encodes_a_decodable_x_with_resulting_quantity() {
        let mut fsm = active_fsm(store(), vec![Permission::Read]);
        let sym = Symbol::parse("BTC-20240329-50000-C").expect("symbol");
        let frame = fsm
            .emit_md_incremental(
                "MDR-1".to_string(),
                SequenceNumber::new(43),
                vec![IncrementalEntry {
                    update_action: super::super::enums::MdUpdateAction::Delete,
                    entry_type: super::super::enums::MdEntryType::Offer,
                    symbol: sym,
                    price: Cents::new(50_005),
                    size: 0,
                }],
                0,
            )
            .expect("emit X");
        match decode(&frame) {
            Ok(DecodedMessage::MarketDataIncrementalRefresh(x)) => {
                assert_eq!(x.rpt_seq.get(), 43);
                assert_eq!(
                    x.entries[0].size, 0,
                    "0 = level removed (resulting quantity)"
                );
            }
            other => panic!("expected X, got {other:?}"),
        }
    }

    #[test]
    fn test_market_depth_to_option_maps_zero_to_full_book() {
        assert_eq!(market_depth_to_option(0), None);
        assert_eq!(market_depth_to_option(5), Some(5));
    }

    // ---- Market data (#040): the session-level subscription DoS bounds, the
    // baseline filter, and the re-snapshot re-baseline, driven on a real
    // `VenueFixSession` over a live `AppState`'s shared subscription manager.

    /// A minimal serving `AppState` (dev auth, no accounts — the market-data path
    /// admits the FSM directly, so no logon over the wire).
    fn md_state() -> Arc<AppState> {
        let auth = crate::state::AuthConfig::dev().expect("dev auth");
        AppState::new(
            crate::state::AppStateConfig::new(["BTC"])
                .with_serving(true)
                .with_auth(auth),
        )
        .expect("AppState")
    }

    /// A `VenueFixSession` admitted straight to `Active` with `Read` (no logon
    /// round-trip), so the market-data router can be driven directly.
    fn active_session(state: Arc<AppState>) -> VenueFixSession {
        let store: Arc<dyn FixSessionStore> =
            Arc::new(super::super::store::InMemoryFixSessionStore::new());
        let peer = "127.0.0.1:9000".parse().expect("peer addr");
        let leases = Arc::new(SessionLeaseRegistry::new());
        let mut session = VenueFixSession::new(peer, state, store, config(), leases);
        let logon_header = header(CLIENT, VENUE, 1);
        session.fsm.on_inbound(&logon_header, 0);
        session
            .fsm
            .admit_logon(
                AccountId::new("acct-1"),
                vec![Permission::Read],
                0,
                30,
                false,
                1,
                0,
            )
            .expect("admit");
        session
    }

    /// The `i`-th distinct valid contract symbol (a unique strike per index).
    fn md_symbol(i: usize) -> Symbol {
        Symbol::parse(&format!("BTC-20240329-{}-C", 50_000 + i)).expect("symbol")
    }

    fn md_symbols(n: usize) -> Vec<Symbol> {
        (0..n).map(md_symbol).collect()
    }

    fn md_request_msg(
        seq: u64,
        md_req_id: &str,
        sub_type: SubscriptionRequestType,
        symbols: Vec<Symbol>,
    ) -> MarketDataRequest {
        use super::super::enums::MdEntryType;
        MarketDataRequest {
            header: header(CLIENT, VENUE, seq),
            md_req_id: md_req_id.to_string(),
            subscription_request_type: sub_type,
            market_depth: 0,
            entry_types: vec![MdEntryType::Bid, MdEntryType::Offer],
            symbols,
        }
    }

    /// A committed user-driven resting ask on `BTC-20240329-50000-C` — folded into
    /// the shared manager to bump the per-instrument sequence and broadcast a delta.
    fn committed_ask(order_id: &str, price: u64) -> crate::exchange::VenueEvent {
        use crate::exchange::{EventTimestamp, VenueCommand, VenueEvent, VenueOutcome};
        let command = VenueCommand::AddOrder {
            symbol: Symbol::parse("BTC-20240329-50000-C").expect("symbol"),
            order_id: VenueOrderId::new(order_id),
            account: AccountId::new("acct"),
            owner: crate::exchange::Hash32([1; 32]),
            client_order_id: None,
            side: crate::exchange::Side::Sell,
            order_type: crate::models::OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity: 1,
            time_in_force: SeamTif::Gtc,
            stp_mode: crate::exchange::STPMode::None,
        };
        VenueEvent::new(
            SequenceNumber::new(1),
            EventTimestamp::new(1_700_000_000_000),
            command,
            VenueOutcome::Added {
                fills: vec![],
                resting_quantity: 1,
                stp_cancelled: vec![],
            },
        )
    }

    #[tokio::test]
    async fn test_market_data_over_cap_request_rejects_before_registering() {
        // A single `V` whose symbol count exceeds the per-session ceiling is a
        // `Y (281=2)`, and NOTHING is registered — the whole `V` is rejected before
        // the subscribe loop (no partial subscription, no receiver created).
        let state = md_state();
        let mut session = active_session(Arc::clone(&state));
        let over_cap = md_symbols(MAX_MD_SYMBOLS_PER_SESSION + 1);
        let reaction = session
            .route_market_data(
                md_request_msg(
                    2,
                    "MDR-BIG",
                    SubscriptionRequestType::SnapshotPlusUpdates,
                    over_cap,
                ),
                0,
            )
            .expect("route");
        assert_eq!(reaction.frames().len(), 1, "one Y, no W");
        match decode(&reaction.frames()[0]) {
            Ok(DecodedMessage::MarketDataRequestReject(y)) => {
                assert_eq!(y.md_req_rej_reason, MD_REJ_REASON_INSUFFICIENT_BANDWIDTH);
            }
            other => panic!("expected Y, got {other:?}"),
        }
        assert!(
            session.market_data.is_none(),
            "an over-cap V registers no subscription"
        );
    }

    #[tokio::test]
    async fn test_market_data_unsubscribe_frees_a_subscription_slot() {
        // Filling the cap, one more is rejected; an unsubscribe frees a slot so a
        // later subscribe succeeds.
        let state = md_state();
        let mut session = active_session(Arc::clone(&state));
        let full = md_symbols(MAX_MD_SYMBOLS_PER_SESSION);
        let filled = session
            .route_market_data(
                md_request_msg(
                    2,
                    "MDR-1",
                    SubscriptionRequestType::SnapshotPlusUpdates,
                    full.clone(),
                ),
                0,
            )
            .expect("fill");
        assert_eq!(
            filled.frames().len(),
            MAX_MD_SYMBOLS_PER_SESSION,
            "one W per symbol"
        );

        let extra = md_symbol(MAX_MD_SYMBOLS_PER_SESSION);
        let rejected = session
            .route_market_data(
                md_request_msg(
                    3,
                    "MDR-2",
                    SubscriptionRequestType::SnapshotPlusUpdates,
                    vec![extra.clone()],
                ),
                0,
            )
            .expect("over cap");
        match decode(&rejected.frames()[0]) {
            Ok(DecodedMessage::MarketDataRequestReject(y)) => {
                assert_eq!(y.md_req_rej_reason, MD_REJ_REASON_INSUFFICIENT_BANDWIDTH);
            }
            other => panic!("expected Y at the cap, got {other:?}"),
        }

        session
            .route_market_data(
                md_request_msg(
                    4,
                    "MDR-3",
                    SubscriptionRequestType::Unsubscribe,
                    vec![full[0].clone()],
                ),
                0,
            )
            .expect("unsubscribe");

        let resub = session
            .route_market_data(
                md_request_msg(
                    5,
                    "MDR-4",
                    SubscriptionRequestType::SnapshotPlusUpdates,
                    vec![extra],
                ),
                0,
            )
            .expect("resubscribe");
        assert!(
            resub.frames().iter().any(|f| matches!(
                decode(f),
                Ok(DecodedMessage::MarketDataSnapshotFullRefresh(_))
            )),
            "the freed slot admits a new subscribe"
        );
    }

    #[tokio::test]
    async fn test_market_data_drain_drops_deltas_at_or_below_baseline() {
        // The baseline filter (mirrors WS): after a `W` at the baseline sequence, a
        // delta at or below it is dropped (already in the client's snapshot), and the
        // first `X` is strictly after it — so `RptSeq` strictly increases across the
        // `W → X` boundary and there is no redundant `X` at the baseline.
        let state = md_state();
        let mut session = active_session(Arc::clone(&state));
        let sym = md_symbol(0);
        session
            .route_market_data(
                md_request_msg(
                    2,
                    "MDR-1",
                    SubscriptionRequestType::SnapshotPlusUpdates,
                    vec![sym.clone()],
                ),
                0,
            )
            .expect("subscribe");
        // Simulate a delivered `W` at sequence 3 (three folded mutations).
        session
            .market_data
            .as_mut()
            .expect("md")
            .symbols
            .get_mut(&sym)
            .expect("sub")
            .baseline = 3;
        // Commit four deltas → the manager broadcasts instrument_sequence 1,2,3,4.
        for i in 1..=4u64 {
            state
                .subscriptions()
                .on_committed_event(&committed_ask(&format!("o{i}"), 50_000 + i));
        }
        let frames = session.drain_market_data(0).expect("drain");
        assert_eq!(
            frames.len(),
            1,
            "only the delta strictly above the baseline emits an X"
        );
        match decode(&frames[0]) {
            Ok(DecodedMessage::MarketDataIncrementalRefresh(x)) => {
                assert_eq!(
                    x.rpt_seq.get(),
                    4,
                    "the first X is strictly after the baseline"
                );
            }
            other => panic!("expected X, got {other:?}"),
        }
        assert_eq!(
            session.market_data.as_ref().expect("md").symbols[&sym].baseline,
            4,
            "the baseline advanced to the emitted X's sequence"
        );
    }

    #[tokio::test]
    async fn test_market_data_resnapshot_rebaselines_each_subscription() {
        // A session-level lagged receiver recovers by a fresh `W` per subscription
        // AND re-baselines to it — so a subsequent `X` is strictly after the new `W`.
        let state = md_state();
        let mut session = active_session(Arc::clone(&state));
        let sym = md_symbol(0);
        session
            .route_market_data(
                md_request_msg(
                    2,
                    "MDR-1",
                    SubscriptionRequestType::SnapshotPlusUpdates,
                    vec![sym.clone()],
                ),
                0,
            )
            .expect("subscribe");
        // Fold three deltas → the manager's instrument_sequence is now 3.
        for i in 1..=3u64 {
            state
                .subscriptions()
                .on_committed_event(&committed_ask(&format!("o{i}"), 50_000 + i));
        }
        let frames = session.resnapshot_subscriptions(0).expect("resnapshot");
        assert_eq!(frames.len(), 1, "one fresh W per subscription");
        match decode(&frames[0]) {
            Ok(DecodedMessage::MarketDataSnapshotFullRefresh(w)) => {
                assert_eq!(
                    w.rpt_seq.get(),
                    3,
                    "the fresh W re-baselines at the current sequence"
                );
            }
            other => panic!("expected a fresh W, got {other:?}"),
        }
        assert_eq!(
            session.market_data.as_ref().expect("md").symbols[&sym].baseline,
            3,
            "the subscription re-baselines to the fresh W's sequence"
        );
    }

    #[test]
    fn test_admin_sequence_reset_journals_event_and_sets_inbound_expectation() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let reset = DecodedMessage::SequenceReset(session::SequenceReset {
            header: header(CLIENT, VENUE, 2),
            new_seq_no: SeqNum::new(9),
            gap_fill_flag: None,
        });
        fsm.handle_active(reset, 2_000, false).expect("ok");
        assert_eq!(fsm.counters().next_target_seq, 9);
        let events = fsm.reset_events();
        assert!(
            events
                .iter()
                .any(|e| e.trigger == ResetTrigger::SequenceReset)
        );
    }

    #[test]
    fn test_serve_resend_replays_the_durable_outbound_log() {
        let store = store();
        let mut fsm = active_fsm(Arc::clone(&store), vec![Permission::Trade]);
        // Drive some outbound traffic to populate the resend log (each TestRequest
        // reply is a stored Heartbeat).
        for seq in 2..=4 {
            let test = DecodedMessage::TestRequest(session::TestRequest {
                header: header(CLIENT, VENUE, seq),
                test_req_id: format!("TR-{seq}"),
            });
            fsm.handle_active(test, 0, false).expect("ok");
        }
        // Client requests a resend of [1, 3].
        let resend = DecodedMessage::ResendRequest(session::ResendRequest {
            header: header(CLIENT, VENUE, 5),
            begin_seq_no: SeqNum::new(1),
            end_seq_no: SeqNum::new(3),
        });
        let reaction = reacted(fsm.handle_active(resend, 0, false).expect("ok"));
        // The replay covers seqs 1..=3 (some as gap-fills, some as replayed frames).
        assert!(!reaction.frames().is_empty());
    }

    #[test]
    fn test_serve_resend_hostile_end_seq_no_is_bounded_not_a_cpu_sink() {
        // A `ResendRequest` whose `EndSeqNo` is attacker-chosen up to `u64::MAX`
        // must not drive an unbounded synchronous loop — `serve_resend` has no
        // `.await`, so the `MAX_DISPATCH` timeout cannot preempt a spin, and one
        // such request from a bare `Read` session would pin a worker thread. The
        // served range is clamped to last-sent, so the frame count is bounded by
        // the stored outbound log regardless of the requested range. The test
        // completing at all proves the loop terminates; the bound proves the clamp.
        for hostile_end in [u64::MAX, u64::MAX / 2, 1_000_000_000_000_000_000] {
            let store = store();
            let mut fsm = active_fsm(Arc::clone(&store), vec![Permission::Trade]);
            for seq in 2..=4 {
                let test = DecodedMessage::TestRequest(session::TestRequest {
                    header: header(CLIENT, VENUE, seq),
                    test_req_id: format!("TR-{seq}"),
                });
                fsm.handle_active(test, 0, false).expect("ok");
            }
            let resend = DecodedMessage::ResendRequest(session::ResendRequest {
                header: header(CLIENT, VENUE, 5),
                begin_seq_no: SeqNum::new(1),
                end_seq_no: SeqNum::new(hostile_end),
            });
            let reaction = reacted(fsm.handle_active(resend, 0, false).expect("ok"));
            assert!(
                reaction.frames().len() <= 8,
                "resend to EndSeqNo={hostile_end} produced {} frames — not clamped to last-sent",
                reaction.frames().len()
            );
        }
    }

    #[test]
    fn test_on_tick_closes_an_un_logged_on_connection_past_the_window() {
        let mut fsm = SessionFsm::new(config(), store(), 0);
        // No logon; past the 10 s window.
        let reaction = fsm.on_tick(11_000, false).expect("ok");
        assert_eq!(reaction.control(), SessionControl::Close);
        assert!(reaction.frames().is_empty());
    }

    #[test]
    fn test_on_tick_emits_heartbeat_after_the_negotiated_interval() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        // HeartBtInt was 30 s; a tick 31 s later is due a heartbeat.
        let reaction = fsm.on_tick(31_000, false).expect("ok");
        assert!(
            reaction
                .frames()
                .iter()
                .any(|frame| matches!(decode(frame), Ok(DecodedMessage::Heartbeat(_))))
        );
    }

    #[test]
    fn test_on_tick_drops_a_revoked_active_session() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let reaction = fsm.on_tick(1_000, true).expect("ok");
        assert_eq!(reaction.control(), SessionControl::Close);
    }

    #[test]
    fn test_non_logon_before_logon_is_rejected() {
        // A message arriving in AwaitingLogon that is not a Logon → Logout + close.
        let mut fsm = SessionFsm::new(config(), store(), 0);
        let hb_header = header(CLIENT, VENUE, 1);
        fsm.on_inbound(&hb_header, 0);
        let reaction = fsm.logout_close(0, "expected Logon").expect("ok");
        assert_eq!(reaction.control(), SessionControl::Close);
        assert!(matches!(
            decode(&reaction.frames()[0]),
            Ok(DecodedMessage::Logout(_))
        ));
    }
}
