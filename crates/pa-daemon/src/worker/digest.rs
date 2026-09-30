//! The digest inbox lane for agent messages (swarm PRs C + D, TS
//! `core/agent-message-inbox.ts` + `core/agent-message-digest-controller.ts`).
//!
//! With the digest lane enabled for a session, an inbound agent message from
//! a non-parent sender lands in a durable inbox instead of prompting; one
//! coalesced notice per batch wakes the recipient, which pulls contents with
//! `rlm.inbox.list()` / `rlm.inbox.read()`. Payloads persist as generic
//! session `custom` entries (`agent_message_inbox`), never replayed into
//! model context, so unread entries survive restarts.
//!
//! Lane ownership (PR D): senders never choose the lane — a sender always
//! prefers steering its recipient — so the receiving side decides from
//! per-recipient counters, with hysteresis (one crossed trigger switches
//! push -> digest; only every trigger relaxed below half switches back).
//! The TS daemon held one process-wide map keyed by recipient; in the Rust
//! split every session is its own worker, so the receiving worker owns the
//! controller and drops it with the session. A user pin ("push"/"digest")
//! suspends the controller entirely; "auto" returns control to it.
//!
//! Default off: push delivery keeps the exact current flow until the lane
//! is enabled (the config flag, the controller, or a pin).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::sync::Notify;

use super::{QueueCheckpoint, QueuePriority, QueuedItem, SessionCore, TurnPolicy};

/// TS `AGENT_MESSAGE_INBOX_ENTRY_CUSTOM_TYPE`: the durable inbox row.
pub(crate) const AGENT_MESSAGE_INBOX_ENTRY_CUSTOM_TYPE: &str = "agent_message_inbox";
/// TS `AGENT_MESSAGE_INBOX_READ_ENTRY_CUSTOM_TYPE`: the durable read marker.
pub(crate) const AGENT_MESSAGE_INBOX_READ_ENTRY_CUSTOM_TYPE: &str = "agent_message_inbox_read";
/// TS `AGENT_MESSAGE_DIGEST_NOTICE_CUSTOM_TYPE`: the one-per-batch wake row.
pub(crate) const AGENT_MESSAGE_DIGEST_NOTICE_CUSTOM_TYPE: &str = "agent_message_digest_notice";

/// TS `PREVIEW_MAX_CHARS`: inbox previews cap at this many chars.
const PREVIEW_MAX_CHARS: usize = 120;
/// TS digest notice sender list cap.
const DIGEST_NOTICE_MAX_SENDERS: usize = 5;
/// The trailing-5-minute window the controller's pending pressure reads
/// (TS `MessagingStats.arrivals.last5m`).
const ARRIVALS_WINDOW_MS: u64 = 5 * 60 * 1000;
/// The chars-per-token heuristic of the ingestion share (TS
/// `estimateMessagingTokens`: chars / 4 over the working context).
const CONTEXT_TOKENS_PER_CHAR: f64 = 4.0;

/// The user pin for the lane (PR D): "auto" hands control to the daemon-side
/// controller; a pinned lane never flips. The Rust port ships the controller
/// DORMANT: sessions start push-pinned (the default-off requirement — an
/// auto-armed default would flip long-running orchestrators onto the digest
/// lane and break the established parent-child reply protocol), and
/// `rlm.inbox.configure("auto")` arms the controller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum DigestLanePin {
    Auto,
    #[default]
    Push,
    Digest,
}

impl DigestLanePin {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            DigestLanePin::Auto => "auto",
            DigestLanePin::Push => "push",
            DigestLanePin::Digest => "digest",
        }
    }
}

// ---------------------------------------------------------------------------
// Durable inbox entries (TS `AgentMessageInboxEntryData`)
// ---------------------------------------------------------------------------

/// The durable inbox entry payload (the `data` of the `agent_message_inbox`
/// custom entry). TS shape, camelCase; `kind` distinguishes delivered agent
/// messages (PR C) from watch events routed onto the digest lane (PR E).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InboxEntryData {
    pub message_id: String,
    pub content: String,
    pub from: InboxEndpoint,
    pub from_relationship: String,
    pub target: InboxTarget,
    pub received_at: String,
    /// `"agent_message"` for delivered reports; `"watch"` for watch events.
    #[serde(default = "default_inbox_kind")]
    pub kind: String,
    /// Present for watch entries: which watch produced the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch: Option<String>,
}

fn default_inbox_kind() -> String {
    "agent_message".to_string()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InboxEndpoint {
    active_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InboxTarget {
    active_session_id: String,
    session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_name: Option<String>,
}

/// One inbox record: the durable entry plus its read state.
#[derive(Debug, Clone)]
pub(crate) struct InboxRecord {
    id: String,
    data: InboxEntryData,
    read: bool,
}

impl InboxRecord {
    /// TS `AgentMessageInboxEntryView` (`rlm.inbox.list()` rows).
    #[must_use]
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "messageId": self.data.message_id,
            "from": endpoint_value(&self.data.from),
            "fromRelationship": self.data.from_relationship,
            "receivedAt": self.data.received_at,
            "read": self.read,
            "preview": preview(&self.data.content),
            "content": self.data.content,
            "kind": self.data.kind,
            "watch": self.data.watch,
        })
    }
}

fn endpoint_value(endpoint: &InboxEndpoint) -> Value {
    let mut value = json!({ "activeSessionId": endpoint.active_session_id });
    if let Some(name) = &endpoint.session_name {
        value["sessionName"] = json!(name);
    }
    value
}

/// TS `preview`: the first `PREVIEW_MAX_CHARS` chars plus an ellipsis.
fn preview(content: &str) -> String {
    if content.chars().count() > PREVIEW_MAX_CHARS {
        let clipped: String = content.chars().take(PREVIEW_MAX_CHARS).collect();
        format!("{clipped}...")
    } else {
        content.to_string()
    }
}

/// The store-backed inbox state: lazily loaded records keyed to the store
/// identity (a replacement session reloads from its own file).
#[derive(Debug, Default)]
struct InboxState {
    loaded_key: Option<(PathBuf, String)>,
    records: Vec<InboxRecord>,
}

impl InboxState {
    /// Load (or reload) the records from the store's durable entries: the
    /// inbox rows in file order plus the read-marker message ids.
    fn load_from(&mut self, store: &crate::session_store::SessionFile) {
        let key = (store.path.clone(), store.session_id().to_string());
        if self.loaded_key.as_ref() == Some(&key) {
            return;
        }
        let mut read_message_ids = std::collections::HashSet::new();
        let mut pending: Vec<InboxRecord> = Vec::new();
        for entry in store.entries() {
            if entry.type_ != "custom" {
                continue;
            }
            let custom_type = entry
                .fields
                .get("customType")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if custom_type == AGENT_MESSAGE_INBOX_READ_ENTRY_CUSTOM_TYPE {
                if let Some(message_id) = entry
                    .fields
                    .get("data")
                    .and_then(|data| data.get("messageId"))
                    .and_then(Value::as_str)
                {
                    read_message_ids.insert(message_id.to_string());
                }
            } else if custom_type == AGENT_MESSAGE_INBOX_ENTRY_CUSTOM_TYPE {
                if let Ok(data) = serde_json::from_value::<InboxEntryData>(
                    entry.fields.get("data").cloned().unwrap_or(Value::Null),
                ) {
                    if data.message_id.is_empty() {
                        continue;
                    }
                    pending.push(InboxRecord {
                        id: entry.id.clone(),
                        data,
                        read: false,
                    });
                }
            }
        }
        for record in &mut pending {
            record.read = read_message_ids.contains(&record.data.message_id);
        }
        self.records = pending;
        self.loaded_key = Some(key);
    }
}

// ---------------------------------------------------------------------------
// The digest-lane controller (PR D, TS `AgentMessageDigestController`)
// ---------------------------------------------------------------------------

/// The pre-registered trigger thresholds (PR D's design values; the
/// starvation eval exists to verify they sit at the measured crossing
/// point). Defaults in [`DigestLaneController::new`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct DigestControllerOptions {
    /// Switch to digest when the arrivals EMA reaches this value (default 5).
    pub pending_ema_trigger: f64,
    /// Switch to digest when the agent-message context share reaches this
    /// value (default 0.2).
    pub ingestion_share_trigger: f64,
    /// Switch to digest when the ingestion-turn share reaches this value
    /// (default 0.3).
    pub ingestion_turn_share_trigger: f64,
    /// EMA smoothing factor per evaluation (default 0.3).
    pub ema_alpha: f64,
}

impl Default for DigestControllerOptions {
    fn default() -> Self {
        DigestControllerOptions {
            pending_ema_trigger: 5.0,
            ingestion_share_trigger: 0.2,
            ingestion_turn_share_trigger: 0.3,
            ema_alpha: 0.3,
        }
    }
}

/// The lane the session currently delivers on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigestLaneMode {
    Push,
    Digest,
}

/// One controller evaluation's inputs (TS `AgentMessageDigestEvaluation`):
/// `None` shares are unmeasured, not crossed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DigestEvaluation {
    /// Agent-message arrivals in the trailing 5-minute window.
    pub pending: u64,
    /// Agent-message share of working context, when measurable.
    pub ingestion_share: Option<f64>,
    /// Ingestion turns over all model turns, when measurable.
    pub ingestion_turn_share: Option<f64>,
    pub current_mode: DigestLaneMode,
}

/// Why the controller answered the way it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigestDecisionReason {
    PendingPressure,
    IngestionContextShare,
    IngestionTurnShare,
    Recovered,
    Hold,
}

/// One controller decision (TS `AgentMessageDigestDecision`). The reason
/// and the smoothed EMA keep the TS decision shape for the controller's
/// tests; production reads `mode` and `changed`.
pub(crate) struct DigestDecision {
    pub mode: DigestLaneMode,
    pub changed: bool,
    /// The TS decision shape's diagnostic fields (test-asserted).
    #[allow(dead_code)]
    pub reason: DigestDecisionReason,
    #[allow(dead_code)]
    pub pending_ema: f64,
}

/// The hysteresis controller: one crossed trigger switches push -> digest;
/// only every trigger relaxed below half its value switches back, so the
/// lane cannot flap on a borderline observation.
#[derive(Debug)]
pub(crate) struct DigestLaneController {
    options: DigestControllerOptions,
    pending_ema: f64,
    observed: bool,
}

impl Default for DigestLaneController {
    fn default() -> Self {
        DigestLaneController::new(DigestControllerOptions::default())
    }
}

impl DigestLaneController {
    #[must_use]
    pub fn new(options: DigestControllerOptions) -> Self {
        DigestLaneController {
            options,
            pending_ema: 0.0,
            observed: false,
        }
    }

    /// Evaluate one delivery window. Event-driven: the delivery path calls
    /// this before each inbound agent-message delivery, never on a timer.
    pub fn evaluate(&mut self, input: DigestEvaluation) -> DigestDecision {
        self.pending_ema = if self.observed {
            self.options.ema_alpha * input.pending as f64
                + (1.0 - self.options.ema_alpha) * self.pending_ema
        } else {
            input.pending as f64
        };
        self.observed = true;

        if input.current_mode == DigestLaneMode::Push {
            // Any single trigger crossed: switch before starvation compounds.
            if self.pending_ema >= self.options.pending_ema_trigger {
                return DigestDecision {
                    mode: DigestLaneMode::Digest,
                    changed: true,
                    reason: DigestDecisionReason::PendingPressure,
                    pending_ema: self.pending_ema,
                };
            }
            if input
                .ingestion_share
                .is_some_and(|share| share >= self.options.ingestion_share_trigger)
            {
                return DigestDecision {
                    mode: DigestLaneMode::Digest,
                    changed: true,
                    reason: DigestDecisionReason::IngestionContextShare,
                    pending_ema: self.pending_ema,
                };
            }
            if input
                .ingestion_turn_share
                .is_some_and(|share| share >= self.options.ingestion_turn_share_trigger)
            {
                return DigestDecision {
                    mode: DigestLaneMode::Digest,
                    changed: true,
                    reason: DigestDecisionReason::IngestionTurnShare,
                    pending_ema: self.pending_ema,
                };
            }
            return DigestDecision {
                mode: DigestLaneMode::Push,
                changed: false,
                reason: DigestDecisionReason::Hold,
                pending_ema: self.pending_ema,
            };
        }

        // Digest -> push only when every trigger is relaxed below half value.
        let recovered = self.pending_ema < self.options.pending_ema_trigger / 2.0
            && input
                .ingestion_share
                .is_none_or(|share| share < self.options.ingestion_share_trigger / 2.0)
            && input
                .ingestion_turn_share
                .is_none_or(|share| share < self.options.ingestion_turn_share_trigger / 2.0);
        DigestDecision {
            mode: if recovered {
                DigestLaneMode::Push
            } else {
                DigestLaneMode::Digest
            },
            changed: recovered,
            reason: if recovered {
                DigestDecisionReason::Recovered
            } else {
                DigestDecisionReason::Hold
            },
            pending_ema: self.pending_ema,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-session counters (the controller's trigger inputs)
// ---------------------------------------------------------------------------

/// The receiving worker's trigger counters. The TS controller read the
/// instrumentation counters of PR A (`messaging_stats`); that
/// instrumentation is not part of this port, so the digest lane owns the
/// minimal counters it needs: the arrivals window (a 5-minute ring), the
/// model/ingestion turn counts (turn-granular, the worker-side equivalent
/// of the TS assistant-step counters), and the controller state itself.
#[derive(Debug, Default)]
struct DigestCounters {
    arrivals: VecDeque<u64>,
    model_turns: u64,
    ingestion_turns: u64,
    controller: DigestLaneController,
}

impl DigestCounters {
    fn record_arrival(&mut self, now_ms: u64) {
        self.arrivals.push_back(now_ms);
        self.prune_arrivals(now_ms);
    }

    fn prune_arrivals(&mut self, now_ms: u64) {
        while self
            .arrivals
            .front()
            .is_some_and(|stamp| now_ms.saturating_sub(*stamp) > ARRIVALS_WINDOW_MS)
        {
            self.arrivals.pop_front();
        }
    }

    fn arrivals_last_5m(&mut self, now_ms: u64) -> u64 {
        self.prune_arrivals(now_ms);
        self.arrivals.len() as u64
    }

    fn note_model_turn(&mut self, ingestion: bool) {
        self.model_turns += 1;
        self.ingestion_turns += u64::from(ingestion);
    }

    fn ingestion_turn_share(&self) -> Option<f64> {
        (self.model_turns > 0).then(|| self.ingestion_turns as f64 / self.model_turns as f64)
    }
}

// ---------------------------------------------------------------------------
// The digest lane manager (receiving-worker side)
// ---------------------------------------------------------------------------

/// The receiving worker's digest lane: the durable inbox, the user pin,
/// the controller with its counters, and the notice admission/withdrawal
/// through the worker's queue lanes.
pub(crate) struct AgentMessageDigest {
    core: Arc<Mutex<SessionCore>>,
    recovery: Arc<Mutex<Option<crate::journal::WorkerRecoveryJournal>>>,
    work_notify: Arc<Notify>,
    inbox: Mutex<InboxState>,
    counters: Mutex<DigestCounters>,
}

impl AgentMessageDigest {
    pub(crate) fn new(
        core: Arc<Mutex<SessionCore>>,
        recovery: Arc<Mutex<Option<crate::journal::WorkerRecoveryJournal>>>,
        work_notify: Arc<Notify>,
    ) -> Self {
        AgentMessageDigest {
            core,
            recovery,
            work_notify,
            inbox: Mutex::new(InboxState::default()),
            counters: Mutex::new(DigestCounters::default()),
        }
    }

    /// Record one accepted inbound agent message (both lanes, TS
    /// `MessagingStats`' arrivals definition: every accepted arrival).
    pub(crate) fn record_arrival(&self, now_ms: u64) {
        self.counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_arrival(now_ms);
    }

    /// Count one model turn (an assistant row the worker persisted, TS's
    /// per-step counter at turn granularity); an ingestion turn is one
    /// whose turn was driven by an agent-message delivery.
    pub(crate) fn note_model_turn(&self, ingestion: bool) {
        self.counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .note_model_turn(ingestion);
    }

    /// The user pin (`rlm.inbox.configure`, PR D): "push"/"digest" fixes
    /// delivery and suspends the controller; "auto" returns control.
    ///
    /// # Errors
    ///
    /// Returns an error when `mode` is not `"auto"`, `"push"`, or `"digest"`.
    pub(crate) fn configure_pin(&self, mode: &str) -> anyhow::Result<Value> {
        let pin = match mode {
            "auto" => DigestLanePin::Auto,
            "push" => DigestLanePin::Push,
            "digest" => DigestLanePin::Digest,
            _ => {
                anyhow::bail!("rlm.inbox.configure mode must be \"auto\", \"push\", or \"digest\"")
            }
        };
        let digest = {
            let mut core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            core.agent_message_digest_pin = pin;
            if pin != DigestLanePin::Auto {
                core.agent_message_digest_mode = pin == DigestLanePin::Digest;
            }
            core.agent_message_digest_mode
        };
        Ok(json!({
            "mode": pin.as_str(),
            "pinned": pin != DigestLanePin::Auto,
            "digest": digest,
        }))
    }

    /// The daemon-side lane decision (PR D): evaluate the controller before
    /// one inbound delivery, flip the session's lane when it says so, then
    /// decide whether THIS delivery digests. Parent-to-child instructions
    /// always stay push; a user pin suspends the controller entirely.
    fn evaluate_and_decide(&self, now_ms: u64, sender_is_parent: bool) -> bool {
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut core = self
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if core.agent_message_digest_pin == DigestLanePin::Auto {
            let pending = counters.arrivals_last_5m(now_ms);
            let ingestion_share = context_share_locked(&core);
            let ingestion_turn_share = counters.ingestion_turn_share();
            let decision = counters.controller.evaluate(DigestEvaluation {
                pending,
                ingestion_share,
                ingestion_turn_share,
                current_mode: if core.agent_message_digest_mode {
                    DigestLaneMode::Digest
                } else {
                    DigestLaneMode::Push
                },
            });
            if decision.changed {
                core.agent_message_digest_mode = decision.mode == DigestLaneMode::Digest;
            }
        }
        // The hard boundary regardless of the lane: a parent steering this
        // session is never digested.
        core.agent_message_digest_mode && !sender_is_parent
    }

    /// The full pre-delivery routing for one inbound agent message (PR C):
    /// record the arrival, run the daemon-side lane decision, and — on the
    /// digest lane — store the payload durably and ensure the one-per-batch
    /// notice. Returns `None` for the push lane (the caller runs the
    /// existing delivery unchanged).
    pub(crate) fn route_inbound_message(
        &self,
        message_id: &str,
        message: &str,
        sender: &Value,
        from_relationship: Option<&str>,
    ) -> Option<Value> {
        let now_ms = crate::util::now_ms();
        self.record_arrival(now_ms);
        let sender_is_parent = {
            let core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            sender_is_parent_of(sender, &core)
        };
        if !self.evaluate_and_decide(now_ms, sender_is_parent) {
            return None;
        }
        let (target, digest_at) =
            self.append_inbox_message(message_id, message, sender, from_relationship);
        self.ensure_digest_notice();
        Some(json!({
            "target": target,
            "digestAt": digest_at,
        }))
    }

    /// Store one agent message durably and return the receipt's target
    /// endpoint plus the digest timestamp.
    fn append_inbox_message(
        &self,
        message_id: &str,
        message: &str,
        sender: &Value,
        from_relationship: Option<&str>,
    ) -> (Value, String) {
        let mut inbox = self
            .inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut core = self
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(store) = core.store.as_ref() {
            inbox.load_from(store);
        }
        let received_at = crate::util::now_iso();
        let from = InboxEndpoint {
            active_session_id: sender
                .get("activeSessionId")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            session_name: sender
                .get("sessionName")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .map(str::to_string),
        };
        let target = Self::target_value_locked(&core);
        let data = InboxEntryData {
            message_id: message_id.to_string(),
            content: message.to_string(),
            from,
            from_relationship: from_relationship.unwrap_or("sibling").to_string(),
            target: InboxTarget {
                active_session_id: target
                    .get("activeSessionId")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
                session_id: target
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
                session_name: target
                    .get("sessionName")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            },
            received_at: received_at.clone(),
            kind: "agent_message".to_string(),
            watch: None,
        };
        Self::append_entry_locked(&mut inbox, &mut core, &data);
        (target, received_at)
    }

    /// The receiving session's endpoint (TS `createAgentSessionMessageEndpoint`):
    /// the receipt's `target` and the inbox entry's target share this shape.
    fn target_value_locked(core: &SessionCore) -> Value {
        let store = core.store.as_ref();
        let mut target = json!({
            "activeSessionId": core.active_session_id,
            "sessionId": store.map(|store| store.session_id().to_string()).unwrap_or_default(),
            "runtimeKind": core.runtime_kind,
        });
        if let Some(name) = store
            .and_then(|store| store.session_name())
            .filter(|name| !name.is_empty())
        {
            target["sessionName"] = json!(name);
        }
        target
    }

    /// Append one inbox entry durably (the `agent_message_inbox` custom
    /// entry) and to the in-memory records. A worker without a session
    /// store keeps the entry in memory only (the records still serve).
    fn append_entry_locked(inbox: &mut InboxState, core: &mut SessionCore, data: &InboxEntryData) {
        let id = core
            .store
            .as_mut()
            .and_then(|store| {
                store
                    .persist_entry(
                        "custom",
                        json!({
                            "customType": AGENT_MESSAGE_INBOX_ENTRY_CUSTOM_TYPE,
                            "data": data,
                        }),
                    )
                    .ok()
            })
            .unwrap_or_default();
        inbox.records.push(InboxRecord {
            id,
            data: data.clone(),
            read: false,
        });
    }

    /// Store one watch event (agent or job) on the digest lane (PR E):
    /// a `watch`-kinded inbox entry.
    fn append_watch_locked(&self, inbox: &mut InboxState, watch: &str, content: &str) {
        let received_at = crate::util::now_iso();
        let data = InboxEntryData {
            message_id: format!(
                "watch-{watch}-{}-{}",
                crate::util::now_ms(),
                uuid::Uuid::new_v4().simple()
            ),
            content: content.to_string(),
            from: InboxEndpoint {
                active_session_id: "watch".to_string(),
                session_name: None,
            },
            from_relationship: "watch".to_string(),
            target: InboxTarget {
                active_session_id: "watch".to_string(),
                session_id: "watch".to_string(),
                session_name: None,
            },
            received_at,
            kind: "watch".to_string(),
            watch: Some(watch.to_string()),
        };
        let mut core = self
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::append_entry_locked(inbox, &mut core, &data);
    }

    /// The unread/total counts of the durable inbox.
    fn inbox_records(&self) -> Vec<InboxRecord> {
        let mut inbox = self
            .inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let core = self
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(store) = core.store.as_ref() {
            inbox.load_from(store);
        }
        inbox.records.clone()
    }

    /// The `rlm.inbox.list()` snapshot: entries, unread, total.
    pub(crate) fn inbox_snapshot(&self) -> Value {
        let records = self.inbox_records();
        let unread = records.iter().filter(|record| !record.read).count();
        json!({
            "entries": records.iter().map(InboxRecord::view).collect::<Vec<_>>(),
            "unread": unread,
            "total": records.len(),
        })
    }

    /// The `rlm.inbox.read()` body (PR C): mark entries read (durable
    /// marker), return their full contents, and cancel a still-pending
    /// notice once everything is read. Without ids, reads every unread
    /// entry; unknown ids are ignored.
    pub(crate) fn read_inbox(&self, ids: Option<Vec<String>>) -> Value {
        let mut inbox = self
            .inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        {
            let core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(store) = core.store.as_ref() {
                inbox.load_from(store);
            }
        }
        let wanted = ids.map(|ids| ids.into_iter().collect::<std::collections::HashSet<_>>());
        let mut entries: Vec<Value> = Vec::new();
        {
            let mut core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for record in &mut inbox.records {
                let matches = match &wanted {
                    Some(wanted) => wanted.contains(&record.id),
                    None => !record.read,
                };
                if !matches {
                    continue;
                }
                if !record.read {
                    let _ = core.store.as_mut().map(|store| {
                        store.persist_entry(
                            "custom",
                            json!({
                                "customType": AGENT_MESSAGE_INBOX_READ_ENTRY_CUSTOM_TYPE,
                                "data": { "messageId": record.data.message_id },
                            }),
                        )
                    });
                    record.read = true;
                }
                entries.push(record.view());
            }
        }
        let unread = inbox.records.iter().filter(|record| !record.read).count();
        if unread == 0 {
            self.withdraw_pending_digest_notices();
        }
        json!({ "entries": entries, "unread": unread })
    }

    /// TS `_ensureAgentMessageDigestNotice`: one live notice covers the
    /// whole batch; later arrivals wait for the recipient to pull them
    /// with `rlm.inbox.read()` in that turn. Quiet (queue-invisible,
    /// injected) on the follow-up lane; an idle session wakes on it.
    fn ensure_digest_notice(&self) {
        let content = {
            let mut inbox = self
                .inbox
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(store) = core.store.as_ref() {
                inbox.load_from(store);
            }
            let unread: Vec<&InboxRecord> =
                inbox.records.iter().filter(|record| !record.read).collect();
            if unread.is_empty() {
                return;
            }
            let senders: Vec<String> = {
                let mut senders = Vec::new();
                for record in &unread {
                    let sender = record
                        .data
                        .from
                        .session_name
                        .clone()
                        .unwrap_or_else(|| record.data.from.active_session_id.clone());
                    if !senders.contains(&sender) {
                        senders.push(sender);
                    }
                }
                senders
            };
            digest_notice_content(unread.len(), &senders)
        };
        let mut core = self
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if core.shutdown_requested {
            return;
        }
        // The one-live-notice check: a pending digest notice covers the batch.
        let notice_pending = core
            .follow_up
            .iter()
            .chain(core.steering.iter())
            .any(is_digest_notice_item);
        if notice_pending {
            return;
        }
        let row = json!({
            "role": "custom",
            "customType": AGENT_MESSAGE_DIGEST_NOTICE_CUSTOM_TYPE,
            "content": content,
            "display": false,
            "details": Value::Null,
            "timestamp": crate::util::now_ms(),
        });
        core.follow_up.push_back(QueuedItem {
            priority: QueuePriority::Background,
            preview: None,
            message: content,
            custom_message: Some(row),
            agent_message: None,
            queue_key: None,
            admission_id: None,
            images: Vec::new(),
            done: None,
            // TS `queueVisible: false`: the notice never shows as a queue
            // row; the turn still wakes an idle session.
            queue_visible: false,
            policy: TurnPolicy::Injected,
            forced_batch: false,
        });
        drop(core);
        super::checkpoint_queue_recovery(
            &self.recovery,
            &self.core,
            QueueCheckpoint::Admitted {
                operation: "follow_up_queued",
            },
        );
        self.work_notify.notify_one();
    }

    /// TS `_cancelPendingDigestNotices`: withdraw every undelivered digest
    /// notice once the inbox is fully read (a read-before-delivery cancels
    /// the pending wake).
    fn withdraw_pending_digest_notices(&self) {
        let removed = {
            let mut core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let before = core.steering.len() + core.follow_up.len();
            core.steering.retain(|item| !is_digest_notice_item(item));
            core.follow_up.retain(|item| !is_digest_notice_item(item));
            before != core.steering.len() + core.follow_up.len()
        };
        if removed {
            super::checkpoint_queue_recovery(
                &self.recovery,
                &self.core,
                QueueCheckpoint::Settle {
                    operation: "queue_purged",
                },
            );
        }
    }

    /// Route one watch event (agent or job, PR E) through the notice
    /// pipeline: on the digest lane the event lands in the inbox (one
    /// coalesced notice per batch); on the push lane it injects the same
    /// quiet notice the async-bash completions ride (queue-if-busy,
    /// resume-if-idle), never content beyond the range.
    pub(crate) fn emit_watch_notice(&self, watch: &str, content: &str) {
        let digest = {
            let core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            core.agent_message_digest_mode
        };
        if digest {
            {
                let mut inbox = self
                    .inbox
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.append_watch_locked(&mut inbox, watch, content);
            }
            self.ensure_digest_notice();
            return;
        }
        let mut core = self
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if core.shutdown_requested {
            return;
        }
        let row = json!({
            "role": "custom",
            "customType": crate::agent_watch::AGENT_WATCH_NOTICE_CUSTOM_TYPE,
            "content": content,
            "display": false,
            "details": { "watch": watch },
            "timestamp": crate::util::now_ms(),
        });
        let (policy, queue_visible) = if core.busy {
            (TurnPolicy::Queued, true)
        } else {
            (TurnPolicy::Injected, false)
        };
        core.steering.push_back(QueuedItem {
            priority: QueuePriority::Background,
            preview: None,
            message: content.to_string(),
            custom_message: Some(row),
            agent_message: None,
            queue_key: None,
            admission_id: None,
            images: Vec::new(),
            done: None,
            queue_visible,
            policy,
            forced_batch: false,
        });
        drop(core);
        super::checkpoint_queue_recovery(
            &self.recovery,
            &self.core,
            QueueCheckpoint::Admitted {
                operation: "steer_queued",
            },
        );
        self.work_notify.notify_one();
    }
}

/// Whether one queued item is an undelivered digest notice.
fn is_digest_notice_item(item: &QueuedItem) -> bool {
    item.custom_message
        .as_ref()
        .and_then(|row| row.get("customType"))
        .and_then(Value::as_str)
        == Some(AGENT_MESSAGE_DIGEST_NOTICE_CUSTOM_TYPE)
}

/// TS `createAgentMessageDigestNoticeContent` (post-PR-E wording: the inbox
/// carries agent messages and watch events): the one-per-batch wake text.
#[must_use]
pub(crate) fn digest_notice_content(unread_count: usize, senders: &[String]) -> String {
    let sender_list = senders
        .iter()
        .take(DIGEST_NOTICE_MAX_SENDERS)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let from = if sender_list.is_empty() {
        String::new()
    } else if senders.len() > DIGEST_NOTICE_MAX_SENDERS {
        format!(" (from: {sender_list}, ...)")
    } else {
        format!(" (from: {sender_list})")
    };
    format!(
        "You have {unread_count} unread inbox item{} in your inbox{from}.\n\
         List them with `await rlm.inbox.list()` or read all of them with `await rlm.inbox.read()`.",
        if unread_count == 1 { "" } else { "s" }
    )
}

/// Whether the delivery's sender is THIS session's parent (the durable
/// parent edge): parent-to-child instructions always stay push.
fn sender_is_parent_of(sender: &Value, core: &SessionCore) -> bool {
    let sender_id = |key: &str| {
        sender
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
    };
    if let Some(parent) = core.parent_session_id.as_deref() {
        if sender_id("sessionId") == Some(parent) {
            return true;
        }
    }
    if let Some(parent) = core.parent_active_session_id.as_deref() {
        if sender_id("activeSessionId") == Some(parent) {
            return true;
        }
    }
    false
}

/// The ingestion context share (the TS heuristic estimate of PR A): agent-
/// message tokens (chars/4 over the delivered `agent_message` custom rows
/// in the working window) over the last assistant usage's context tokens.
/// `None` when either side is unmeasured — unmeasured never triggers.
fn context_share_locked(core: &SessionCore) -> Option<f64> {
    let store = core.store.as_ref()?;
    let mut context_tokens: Option<u64> = None;
    let mut agent_message_chars: usize = 0;
    for entry in store.entries().iter().rev() {
        if context_tokens.is_none() && entry.type_ == "message" {
            let message = entry.fields.get("message");
            if message
                .and_then(|message| message.get("role"))
                .and_then(Value::as_str)
                == Some("assistant")
                && message
                    .and_then(|message| message.get("usage"))
                    .is_some_and(|usage| !usage.is_null())
            {
                let usage = message
                    .and_then(|message| message.get("usage"))
                    .expect("usage checked above");
                context_tokens = Some(
                    usage
                        .get("input")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        .saturating_add(usage.get("cacheRead").and_then(Value::as_u64).unwrap_or(0))
                        .saturating_add(
                            usage.get("cacheWrite").and_then(Value::as_u64).unwrap_or(0),
                        ),
                );
            }
        }
        if entry.type_ == "custom_message"
            && entry.fields.get("customType").and_then(Value::as_str)
                == Some(pa_core::session_engine::agent_messaging::AGENT_MESSAGE_CUSTOM_TYPE)
        {
            let content = entry.fields.get("content").cloned().unwrap_or(Value::Null);
            let text = match &content {
                Value::String(text) => Some(text.clone()),
                Value::Object(map) => map.get("text").and_then(Value::as_str).map(str::to_string),
                _ => None,
            };
            if let Some(text) = text {
                agent_message_chars += text.chars().count();
            }
        }
        if context_tokens.is_some() {
            break;
        }
    }
    let context_tokens = context_tokens.filter(|tokens| *tokens > 0)?;
    let estimated = agent_message_chars as f64 / CONTEXT_TOKENS_PER_CHAR;
    Some(estimated / context_tokens as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evaluation(
        pending: u64,
        ingestion_share: Option<f64>,
        ingestion_turn_share: Option<f64>,
        current_mode: DigestLaneMode,
    ) -> DigestEvaluation {
        DigestEvaluation {
            pending,
            ingestion_share,
            ingestion_turn_share,
            current_mode,
        }
    }

    #[test]
    fn switches_to_digest_on_the_first_crossed_trigger_from_any_trigger() {
        let mut controller = DigestLaneController::default();
        let decision = controller.evaluate(evaluation(6, None, None, DigestLaneMode::Push));
        assert!(decision.changed);
        assert_eq!(decision.mode, DigestLaneMode::Digest);
        assert_eq!(decision.reason, DigestDecisionReason::PendingPressure);

        let mut controller = DigestLaneController::default();
        let decision = controller.evaluate(evaluation(0, Some(0.25), None, DigestLaneMode::Push));
        assert!(decision.changed);
        assert_eq!(decision.reason, DigestDecisionReason::IngestionContextShare);

        let mut controller = DigestLaneController::default();
        let decision = controller.evaluate(evaluation(0, None, Some(0.4), DigestLaneMode::Push));
        assert!(decision.changed);
        assert_eq!(decision.reason, DigestDecisionReason::IngestionTurnShare);
    }

    #[test]
    fn holds_digest_until_every_trigger_relaxes_below_half_its_value() {
        let mut controller = DigestLaneController::default();
        controller.evaluate(evaluation(6, None, None, DigestLaneMode::Push));
        // One quiet observation is not recovery: the EMA relaxes below 2.5
        // only after several.
        let decision = controller.evaluate(evaluation(1, None, None, DigestLaneMode::Digest));
        assert!(!decision.changed);
        assert_eq!(decision.mode, DigestLaneMode::Digest);
        // Relaxed shares but the EMA still above half: hold.
        let decision =
            controller.evaluate(evaluation(0, Some(0.05), Some(0.1), DigestLaneMode::Digest));
        assert!(!decision.changed);
        assert_eq!(decision.reason, DigestDecisionReason::Hold);
        // Fully quiet at last: recover.
        let decision =
            controller.evaluate(evaluation(0, Some(0.05), Some(0.1), DigestLaneMode::Digest));
        assert!(decision.changed);
        assert_eq!(decision.mode, DigestLaneMode::Push);
        assert_eq!(decision.reason, DigestDecisionReason::Recovered);
    }

    #[test]
    fn never_flaps_on_a_single_borderline_observation() {
        let mut controller = DigestLaneController::default();
        controller.evaluate(evaluation(6, None, None, DigestLaneMode::Push));
        // Borderline values (above half, below the trigger) hold digest.
        let decision =
            controller.evaluate(evaluation(2, Some(0.12), Some(0.2), DigestLaneMode::Digest));
        assert!(!decision.changed);
        let decision =
            controller.evaluate(evaluation(2, Some(0.12), Some(0.2), DigestLaneMode::Digest));
        assert!(!decision.changed);
        assert_eq!(decision.mode, DigestLaneMode::Digest);
    }

    #[test]
    fn unknown_shares_are_unmeasured_never_crossed() {
        let mut controller = DigestLaneController::default();
        let decision = controller.evaluate(evaluation(3, None, None, DigestLaneMode::Push));
        assert!(!decision.changed);
        assert_eq!(decision.mode, DigestLaneMode::Push);
        assert_eq!(decision.reason, DigestDecisionReason::Hold);
    }

    #[test]
    fn smooths_pending_pressure_with_an_ema_before_the_trigger() {
        let mut controller = DigestLaneController::new(DigestControllerOptions {
            ema_alpha: 0.5,
            ..DigestControllerOptions::default()
        });
        let decision = controller.evaluate(evaluation(4, None, None, DigestLaneMode::Push));
        assert!(!decision.changed);
        assert!((decision.pending_ema - 4.0).abs() < f64::EPSILON);
        let decision = controller.evaluate(evaluation(8, None, None, DigestLaneMode::Push));
        assert!(decision.changed);
        assert!((decision.pending_ema - 6.0).abs() < f64::EPSILON);
    }

    #[test]
    fn digest_notice_content_matches_the_ts_wording() {
        let one = digest_notice_content(1, &["sender".to_string()]);
        assert!(one.contains("You have 1 unread inbox item in your inbox (from: sender)."));
        assert!(one.contains("await rlm.inbox.list()"));
        let many = digest_notice_content(
            7,
            &[
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
                "f".to_string(),
            ],
        );
        assert!(many.contains("7 unread inbox items"));
        // TS caps the sender list at five: "e, ..." trails the shown set.
        assert!(many.contains("e, ..."));
        assert!(!many.contains("e, f"));
    }

    #[test]
    fn preview_caps_at_120_chars() {
        assert_eq!(preview("short"), "short");
        let long = "x".repeat(200);
        let clipped = preview(&long);
        assert_eq!(clipped.chars().count(), 123);
        assert!(clipped.ends_with("..."));
    }

    #[test]
    fn sender_is_parent_of_reads_the_durable_parent_edge() {
        let mut core = super::super::SessionCore::test_core(None, "/tmp".to_string());
        assert!(!sender_is_parent_of(
            &json!({ "activeSessionId": "someone" }),
            &core
        ));
        core.parent_active_session_id = Some("parent-active".to_string());
        assert!(sender_is_parent_of(
            &json!({ "activeSessionId": "parent-active" }),
            &core
        ));
        core.parent_session_id = Some("parent-session".to_string());
        assert!(sender_is_parent_of(
            &json!({ "sessionId": "parent-session", "activeSessionId": "other" }),
            &core
        ));
    }

    /// The durable-replay contract (TS `agent-message-inbox`): entries and
    /// read markers persist as `custom` session entries, so unread entries
    /// survive restarts and a fresh inbox over the same store replays them.
    #[test]
    fn inbox_entries_and_read_markers_replay_from_the_durable_store() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("session.jsonl");
        let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
        store.set_path(path.clone());
        store.rewrite().unwrap();
        let digest = AgentMessageDigest::new(
            std::sync::Arc::new(std::sync::Mutex::new(super::super::SessionCore::test_core(
                Some(store),
                "/tmp".to_string(),
            ))),
            std::sync::Arc::new(std::sync::Mutex::new(None)),
            std::sync::Arc::new(tokio::sync::Notify::new()),
        );
        assert!(digest
            .configure_pin("digest")
            .unwrap()
            .get("digest")
            .and_then(Value::as_bool)
            .unwrap_or(false));
        let digested = digest
            .route_inbound_message(
                "agentmsg_1",
                "REPORT 481",
                &json!({
                    "activeSessionId": "child-active",
                    "sessionName": "child-1",
                }),
                Some("child"),
            )
            .expect("digested");
        assert!(digested.get("digestAt").is_some());
        let snapshot = digest.inbox_snapshot();
        assert_eq!(snapshot["unread"], json!(1));
        assert_eq!(snapshot["total"], json!(1));
        assert_eq!(snapshot["entries"][0]["content"], json!("REPORT 481"));
        assert_eq!(snapshot["entries"][0]["kind"], json!("agent_message"));

        // A fresh manager over a reloaded store keeps the entry and the
        // read state (the read markers are durable too).
        let read = digest.read_inbox(None);
        assert_eq!(read["unread"], json!(0));
        assert!(read["entries"][0]["read"].as_bool().unwrap());
        assert_eq!(read["entries"][0]["content"], json!("REPORT 481"));

        let reloaded_store = crate::session_store::SessionFile::open(&path).unwrap();
        let digest = AgentMessageDigest::new(
            std::sync::Arc::new(std::sync::Mutex::new(super::super::SessionCore::test_core(
                Some(reloaded_store),
                "/tmp".to_string(),
            ))),
            std::sync::Arc::new(std::sync::Mutex::new(None)),
            std::sync::Arc::new(tokio::sync::Notify::new()),
        );
        let snapshot = digest.inbox_snapshot();
        assert_eq!(snapshot["total"], json!(1));
        assert_eq!(snapshot["unread"], json!(0));
        assert_eq!(snapshot["entries"][0]["read"], json!(true));
    }
}
