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

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use ironfix_core::types::{CompId, SeqNum};

use crate::auth::{AccountStore, RateLimitKey, RevocationOracle};
use crate::models::{AccountId, ExecutionId, Permission, VenueOrderId};
use crate::state::AppState;

use super::codec::{FieldWriter, tags};
use super::enums::{CxlRejResponseTo, ExecType, MassCancelResponse, OrdStatus};
use super::error::SessionRejectReason;
use super::execution::{ExecutionReport, OrderCancelReject, OrderMassCancelReport};
use super::header::{StandardHeader, UtcTimestamp};
use super::store::{
    FixSessionStore, ResetTrigger, SequenceResetEvent, SessionCounters, SessionKey, StoredOutbound,
};
use super::{DecodedMessage, FixBody, session};
use crate::exchange::event::SequenceNumber;

use super::acceptor::{FixSession, FixSessionFactory, SessionControl, SessionOutbound};

/// The FIX `MsgType (35)` for the venue-built `Logon (A)` ack.
const MSG_TYPE_LOGON: &str = "A";

/// The `OrdRejReason (103)` the venue emits for a permission-denied order — `6`
/// (`Unknown Order`) is the closest standard code for "this session may not act";
/// the redacted `Text (58)` names the cause without leaking a secret.
const ORD_REJ_REASON_NOT_AUTHORIZED: u16 = 6;

/// The `CxlRejReason (102)` the venue emits for a permission-denied cancel /
/// replace — `6` (`Duplicate ClOrdID` is not it; `2` is broker/exchange option),
/// the generic exchange-option code, with the reason in the redacted `Text (58)`.
const CXL_REJ_REASON_NOT_AUTHORIZED: u16 = 2;

/// A short, **non-secret** reason string for a permission-denied application
/// message — safe to echo in a `Text (58)` (it names a policy, not a credential).
const TEXT_NOT_AUTHORIZED: &str = "insufficient permission";

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
        if let Some(key) = self.key.clone() {
            self.store.store_outbound(&key, seq, &frame)?;
        }
        // Checked, non-wrapping: an increment past u64::MAX seals the session.
        let next = seq.checked_add(1).ok_or(SessionError::SequenceExhausted)?;
        self.counters.next_sender_seq = next;
        if let Some(key) = self.key.clone() {
            self.store.save_counters(&key, self.counters)?;
        }
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

    /// Advances the inbound (target) counter for a consumed message, checked.
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
    ) -> Result<Reaction, SessionError> {
        self.on_inbound(header_of(&message), now_ms);

        // Per-message revocation: a revoke bumps the epoch and drops the session.
        if revoked {
            return self.logout_close(now_ms, "account revoked");
        }

        let seq = header_of(&message).msg_seq_num.value();

        // `SequenceReset (4)` is processed regardless of a gap (it repairs one).
        if let DecodedMessage::SequenceReset(reset) = &message {
            return self.handle_sequence_reset(reset, now_ms);
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
                return Ok(Reaction::emit(vec![frame]));
            }
            std::cmp::Ordering::Less => {
                // Already seen (a duplicate / too-low): ignore, do not advance.
                tracing::debug!(
                    expected = self.counters.next_target_seq,
                    got = seq,
                    "fix inbound MsgSeqNum below expected; ignoring"
                );
                return Ok(Reaction::cont());
            }
            std::cmp::Ordering::Equal => {}
        }

        let reaction = self.dispatch_active_body(message, now_ms)?;
        // Consume the in-order message: advance the checked inbound counter.
        self.advance_inbound()?;
        if self.phase == SessionPhase::AwaitingResend {
            self.phase = SessionPhase::Active;
        }
        Ok(reaction)
    }

    /// Routes an in-order [`Active`](SessionPhase::Active) message body.
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
            DecodedMessage::NewOrderSingle(_)
            | DecodedMessage::OrderCancelRequest(_)
            | DecodedMessage::OrderCancelReplaceRequest(_)
            | DecodedMessage::OrderMassCancelRequest(_)
            | DecodedMessage::OrderStatusRequest(_)
            | DecodedMessage::MarketDataRequest(_) => self.handle_application(message, now_ms),
            // Venue-out messages must never arrive inbound.
            DecodedMessage::ExecutionReport(_)
            | DecodedMessage::OrderCancelReject(_)
            | DecodedMessage::OrderMassCancelReport(_)
            | DecodedMessage::MarketDataSnapshotFullRefresh(_)
            | DecodedMessage::MarketDataIncrementalRefresh(_)
            | DecodedMessage::MarketDataRequestReject(_) => {
                self.session_reject(now_ms, SessionRejectReason::InvalidMsgType, Some(35))
            }
        }
    }

    /// The per-message permission gate + `Account (1)` enforcement for an
    /// application message.
    fn handle_application(
        &mut self,
        message: DecodedMessage,
        now_ms: u64,
    ) -> Result<Reaction, SessionError> {
        let required = required_permission(&message);
        if !self.has_permission(required) {
            // Refuse in the message's own context (order-level), never a bare
            // Reject (3) — ADR-0007 §2, 03 §8.
            return self.permission_reject(&message, now_ms);
        }

        // `Account (1)` must be absent or equal to the authenticated account
        // (ADR-0010 rule 4). In the v0.4 dialect only `NewOrderSingle (D)` fields
        // tag 1 (F/G/q/H do not parse it), so the check is D-scoped here — there
        // is no attribution hole. Any future F/G/q coverage lands with the order
        // path (#039).
        if let DecodedMessage::NewOrderSingle(order) = &message
            && let Some(named) = &order.account
            && Some(named) != self.account.as_ref()
        {
            return self.session_reject(
                now_ms,
                SessionRejectReason::ValueIsIncorrect,
                Some(tags::ACCOUNT),
            );
        }

        // Permitted and correctly attributed: the D/F/G order path (#039) and the
        // V market-data path (#040) plug in here. Until then the message is
        // accepted at the session boundary and not yet routed.
        tracing::debug!(
            msg_type = super::message_type_str(&message),
            "fix application message admitted at the session boundary (routing lands in #039/#040)"
        );
        Ok(Reaction::cont())
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
            // `H`/`V` require only `Read`, which every authenticated session holds,
            // so they are never permission-denied here.
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
        let (reason, ref_tag) = match error.reject_route() {
            FixRejectRoute::SessionReject { reason, ref_tag } => (reason, ref_tag),
            // No typed BusinessMessageReject (j) yet (that message lands with #039
            // order routing); classify as an invalid MsgType session reject.
            FixRejectRoute::BusinessMessageReject => {
                (SessionRejectReason::InvalidMsgType, Some(35))
            }
        };
        self.session_reject(now_ms, reason, ref_tag)
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
        | DecodedMessage::MarketDataSnapshotFullRefresh(_)
        | DecodedMessage::MarketDataIncrementalRefresh(_)
        | DecodedMessage::MarketDataRequestReject(_) => Some(Permission::Read),
    }
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

/// The real per-connection FIX session — the [`FixSession`] the acceptor drives,
/// wrapping the synchronous [`SessionFsm`] with the async credential verify.
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
                self.fsm.handle_active(message, now_ms, revoked)
            }
            SessionPhase::Closing => Ok(Reaction::close_silent()),
        };
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
        let reaction = fsm.handle_active(heartbeat(2), 0, false).expect("ok");
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
        let reaction = fsm.handle_active(test, 0, false).expect("ok");
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
        let reaction = fsm.handle_active(heartbeat(5), 0, false).expect("ok");
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
        let reaction = fsm.handle_active(new_order(2, None), 0, false).expect("ok");
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
    fn test_permission_gate_admits_order_from_trade_session() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let reaction = fsm.handle_active(new_order(2, None), 0, false).expect("ok");
        // Admitted at the session boundary; routing is #039, so no reject frame.
        assert!(reaction.frames().is_empty());
        assert_eq!(fsm.counters().next_target_seq, 3);
    }

    #[test]
    fn test_account_field_mismatch_is_a_session_reject() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let foreign = Some(AccountId::new("someone-else"));
        let reaction = fsm
            .handle_active(new_order(2, foreign), 0, false)
            .expect("ok");
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
    fn test_account_field_equal_to_authenticated_is_accepted() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let same = Some(AccountId::new("acct-1"));
        let reaction = fsm.handle_active(new_order(2, same), 0, false).expect("ok");
        assert!(reaction.frames().is_empty());
    }

    #[test]
    fn test_revoked_session_is_logged_out_and_closed() {
        let mut fsm = active_fsm(store(), vec![Permission::Trade]);
        let reaction = fsm.handle_active(heartbeat(2), 0, true).expect("ok");
        assert_eq!(reaction.control(), SessionControl::Close);
        assert!(matches!(
            decode(&reaction.frames()[0]),
            Ok(DecodedMessage::Logout(_))
        ));
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
        let reaction = fsm.handle_active(resend, 0, false).expect("ok");
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
            let reaction = fsm.handle_active(resend, 0, false).expect("ok");
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
