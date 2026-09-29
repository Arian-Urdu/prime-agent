//! The per-child record: run state, roster/collect projections, the
//! deleted-child tombstone, and the roster text helpers — the in-process
//! equivalent of the daemon host's `ChildRecord` (same wire semantics,
//! live child introspection instead of worker round trips).

use pa_types::ai::Usage;
use pa_types::session::ChildUsageOrigin;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

use super::InProcessRlmHost;
use crate::session_engine::engine::SessionEngine;
use crate::session_engine::rlm_host::{RlmChildResult, RlmSubagentActivity, RlmSubagentEntry};

/// Cap on the answer preview handed to the parent model (TS
/// `compactRlmText`).
pub(crate) const ANSWER_PREVIEW_MAX_CHARS: usize = 160;
/// Cap on the one-line task label shown in kernel rosters.
pub(crate) const LABEL_MAX_CHARS: usize = 200;
/// A running child with no tracked activity for this long reports
/// `activity_stale_ms` (TS `RLM_CHILD_STALE_ACTIVITY_THRESHOLD_MS`).
pub(crate) const STALE_ACTIVITY_THRESHOLD_MS: u64 = 10 * 60_000;
const ELLIPSIS: &str = "...";

/// One resident child identity for the family roster join.
#[derive(Debug, Clone)]
pub struct ChildIdentity {
    pub rlm_child_id: String,
    pub session_id: String,
    pub session_name: String,
}

/// The live child activity (TS `RlmChildRun.activity`): `waiting` while a
/// run streams without tools, `writing` while an assistant message
/// streams, `executing` while tools run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildActivity {
    pub kind: &'static str,
    pub tool_name: Option<String>,
}

/// One tracked in-process child: the engine (the runtime, retained until
/// delete or close), the child's own children host (grandchildren spawn
/// through it), and the mutable run state.
pub struct InProcessChildRecord {
    pub(crate) rlm_child_id: String,
    pub(crate) session_name: String,
    /// The child session's durable id: its roster identity and the
    /// agent-message selector family members address it by.
    pub(crate) session_id: String,
    pub(crate) session_dir: String,
    pub(crate) label: String,
    pub(crate) started_at_ms: u64,
    /// The child session engine — the in-process runtime itself.
    pub(crate) engine: Arc<SessionEngine>,
    /// The child's own children host (its recursive descendants).
    pub(crate) child_host: Arc<InProcessRlmHost>,
    /// The settle signal `collect` waits on.
    pub(crate) settled_tx: watch::Sender<bool>,
    state: Mutex<ChildRunState>,
}

/// The mutable run state (the daemon `ChildRecord`'s mutable half, plus
/// the live introspection the in-process host can afford). No `Debug`:
/// the retained agent subscription has no debug form.
// The mirrored TS API shape is deliberate (the booleans are the product's
// own surface, not a refactor target).
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct ChildRunState {
    /// Terminal state (`done` | `error` | `cancelled`); running while
    /// absent.
    pub(crate) settled_status: Option<&'static str>,
    pub(crate) answer_preview: Option<String>,
    pub(crate) error: Option<String>,
    /// An agent message from this child reached the parent since its task
    /// was admitted (TS `_parentReplyCount`): the no-reply terminal notice
    /// is withheld once set.
    pub(crate) replied_since_task: bool,
    /// Exactly one of the settle arm, the delete arm, or a late natural
    /// settle delivers the terminal notice (double-claim races collapse).
    pub(crate) notice_delivered: bool,
    /// The task prompt was admitted. Readers must not settle a pre-prompt
    /// child: it is idle by construction.
    pub(crate) prompt_admitted: bool,
    /// The parent session closed while this child ran: the run arm owes no
    /// notice and the watcher stops.
    pub(crate) closed_by_parent: bool,
    pub(crate) tool_use_count: u64,
    /// Concurrent tool executions (activity flips back to `waiting` at 0).
    pub(crate) running_tools: u32,
    pub(crate) activity: Option<ChildActivity>,
    /// Wall-clock ms of the last tracked activity; seeded at admission.
    pub(crate) last_activity_at_ms: u64,
    /// Pending per-origin usage batches (TS `pendingChildUsage`), flushed
    /// at child run ends and at settlement.
    pub(crate) pending_usage: Vec<(ChildUsageOrigin, Usage)>,
    /// The child agent event subscription (`Agent::subscribe` keeps the
    /// listener until it is explicitly removed — TS semantics). Taken and
    /// unsubscribed at the run task's end and on delete/close, so the
    /// record (and the engine, and the kernel) release once the registry
    /// drops them instead of leaking through the agent's listener list.
    pub(crate) listener: Option<pa_agent::agent::Subscription>,
}

impl InProcessChildRecord {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        rlm_child_id: String,
        session_name: String,
        session_id: String,
        session_dir: String,
        label: String,
        started_at_ms: u64,
        engine: Arc<SessionEngine>,
        child_host: Arc<InProcessRlmHost>,
    ) -> Self {
        let (settled_tx, _) = watch::channel(false);
        Self {
            rlm_child_id,
            session_name,
            session_id,
            session_dir,
            label,
            started_at_ms,
            engine,
            child_host,
            settled_tx,
            state: Mutex::new(ChildRunState {
                settled_status: None,
                answer_preview: None,
                error: None,
                replied_since_task: false,
                notice_delivered: false,
                prompt_admitted: false,
                closed_by_parent: false,
                tool_use_count: 0,
                running_tools: 0,
                activity: Some(ChildActivity {
                    kind: "waiting",
                    tool_name: None,
                }),
                last_activity_at_ms: started_at_ms,
                pending_usage: Vec::new(),
                listener: None,
            }),
        }
    }

    /// The mutable run state (the record's identity half is immutable).
    pub(crate) async fn state(&self) -> tokio::sync::MutexGuard<'_, ChildRunState> {
        self.state.lock().await
    }

    /// Raw run status: `running` | `done` | `error` | `cancelled`.
    fn status(state: &ChildRunState) -> &'static str {
        state.settled_status.unwrap_or("running")
    }

    /// Kernel-roster status: `running` | `completed` | `error` |
    /// `cancelled` (TS keeps a cancelled run's status verbatim).
    fn roster_status(state: &ChildRunState) -> &'static str {
        match Self::status(state) {
            "done" => "completed",
            other => other,
        }
    }

    /// Whether the run is still unsettled.
    pub(crate) async fn is_running(&self) -> bool {
        self.state().await.settled_status.is_none()
    }

    /// Record a terminal state. Idempotent per record: the first settle
    /// wins (a cancelled run's later natural settle keeps the cancelled
    /// verdict). Does NOT wake `collect` waiters — the run arm publishes
    /// the settle signal only after its accounting and notice admission
    /// landed (TS awaits terminal-message retention before resolving run
    /// settlement), so a collect result never precedes the parent's
    /// notice.
    pub(crate) async fn settle_as(&self, status: &'static str, error: Option<String>) {
        let mut state = self.state().await;
        if state.settled_status.is_some() {
            return;
        }
        state.settled_status = Some(status);
        if state.error.is_none() {
            state.error = error;
        }
        state.activity = None;
    }

    /// Wake `collect` waiters: the terminal verdict, accounting, and
    /// notice admission have all landed.
    pub(crate) fn publish_settled(&self) {
        let _ = self.settled_tx.send(true);
    }

    /// Take and unsubscribe the child event listener (idempotent). The
    /// agent's listener list is the last edge that keeps this record
    /// (and its engine, and its kernel) alive after the registry drops
    /// it.
    pub(crate) async fn unsubscribe_listener(&self) {
        if let Some(listener) = self.state().await.listener.take() {
            listener.unsubscribe().await;
        }
    }

    /// Claim the terminal notice exactly once: `true` for the caller that
    /// must deliver it. A failed claim leaves the delivered flag
    /// untouched (an already-delivered notice never re-arms for a later
    /// claimant).
    pub(crate) async fn claim_notice(&self) -> bool {
        let mut state = self.state().await;
        if state.notice_delivered {
            return false;
        }
        state.notice_delivered = true;
        true
    }

    /// The roster row (live introspection: real activity, tool counts, the
    /// child's latest progress note, and staleness).
    pub(crate) async fn entry(&self, now_ms: u64) -> RlmSubagentEntry {
        let state = self.state().await;
        let running = state.settled_status.is_none();
        let progress_note = self
            .engine
            .rlm
            .notes
            .latest_note()
            .await
            .map(|(note, _)| note);
        let activity_stale_ms = running
            .then(|| {
                let stale_candidate = state
                    .activity
                    .as_ref()
                    .is_none_or(|activity| activity.kind != "executing");
                stale_candidate.then(|| now_ms.saturating_sub(state.last_activity_at_ms))
            })
            .flatten()
            .filter(|stale| *stale >= STALE_ACTIVITY_THRESHOLD_MS);
        RlmSubagentEntry {
            rlm_child_id: self.rlm_child_id.clone(),
            active_session_id: Some(self.session_id.clone()),
            session_id: Some(self.session_id.clone()),
            session_name: self.session_name.clone(),
            session_dir: self.session_dir.clone(),
            status: Self::roster_status(&state),
            activity: running.then(|| RlmSubagentActivity {
                kind: state
                    .activity
                    .as_ref()
                    .map_or("waiting", |activity| activity.kind),
                tool_name: state
                    .activity
                    .as_ref()
                    .and_then(|activity| activity.tool_name.clone()),
            }),
            tool_use_count: Some(state.tool_use_count),
            duration_ms: Some(now_ms.saturating_sub(self.started_at_ms)),
            answer_preview: state.answer_preview.clone(),
            replied_since_task: Some(state.replied_since_task),
            progress_note,
            label: (!self.label.is_empty()).then(|| self.label.clone()),
            last_activity_at: Some(state.last_activity_at_ms),
            activity_stale_ms,
        }
    }

    /// One collect envelope.
    pub(crate) async fn collect_result(&self, now_ms: u64) -> RlmChildResult {
        let state = self.state().await;
        RlmChildResult {
            rlm_child_id: self.rlm_child_id.clone(),
            session_name: Some(self.session_name.clone()),
            session_dir: Some(self.session_dir.clone()),
            status: Self::status(&state),
            settled: state.settled_status.is_some(),
            answer_preview: state.answer_preview.clone(),
            error: state.error.clone(),
            duration_ms: Some(now_ms.saturating_sub(self.started_at_ms)),
            tool_use_count: Some(state.tool_use_count),
            replied_since_task: Some(state.replied_since_task),
        }
    }
}

/// A deleted child's retained identity (TS `_deletedRlmChildRuns`): the
/// delete receipt promised a collectable cancelled envelope, and only the
/// fields that envelope reads survive the registry removal.
#[derive(Debug, Clone)]
pub(crate) struct DeletedChild {
    pub rlm_child_id: String,
    pub session_id: String,
    pub session_name: String,
    pub session_dir: String,
    pub started_at_ms: u64,
    pub answer_preview: Option<String>,
    pub error: String,
}

impl DeletedChild {
    /// The selector set a live record answered to (TS
    /// `_rlmDeletedRunMatchesTarget`).
    pub(crate) fn matches(&self, target: &str) -> bool {
        self.rlm_child_id == target || self.session_id == target || self.session_name == target
    }

    /// The settled cancelled envelope (TS `_rlmDeletedCollectEntryForRun`).
    pub(crate) fn collect_result(&self) -> RlmChildResult {
        RlmChildResult {
            rlm_child_id: self.rlm_child_id.clone(),
            session_name: Some(self.session_name.clone()),
            session_dir: Some(self.session_dir.clone()),
            status: "cancelled",
            settled: true,
            answer_preview: self.answer_preview.clone(),
            error: Some(self.error.clone()),
            duration_ms: Some(super::now_ms().saturating_sub(self.started_at_ms)),
            tool_use_count: None,
            replied_since_task: None,
        }
    }
}

/// Whether a live record answers to `target`: child id, session id, or
/// name (the TS selector set).
pub(crate) fn record_matches(record: &InProcessChildRecord, target: &str) -> bool {
    record.rlm_child_id == target || record.session_id == target || record.session_name == target
}

/// Collapse whitespace and cap at the roster limit (TS `compactRlmText`).
#[must_use]
pub(crate) fn compact_rlm_text(text: &str) -> String {
    let compact: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    cap_text(&compact, ANSWER_PREVIEW_MAX_CHARS)
}

/// One-line task label: collapsed prompt, capped for roster rows (TS
/// `rlmChildLabel`).
#[must_use]
pub(crate) fn rlm_child_label(prompt: &str) -> String {
    let collapsed: String = prompt.split_whitespace().collect::<Vec<_>>().join(" ");
    let collapsed = if collapsed.is_empty() {
        "child agent".to_string()
    } else {
        collapsed
    };
    cap_text(&collapsed, LABEL_MAX_CHARS)
}

/// Whitespace-collapsed text capped at `max` chars with an ellipsis.
fn cap_text(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let kept: String = text.chars().take(max - ELLIPSIS.len()).collect();
    format!("{}{}", kept.trim_end(), ELLIPSIS)
}

/// Registry mutation over the host's state: the tombstone and
/// spawn-name operations the host's own spawn/delete paths drive (the
/// daemon keeps the same split in its `rlm_children/registry.rs`).
impl super::InProcessRlmHost {
    /// Record a delete receipt's tombstone (TS #2388): the cancelled
    /// collect envelope reads only these fields, so the retained identity
    /// stays bounded.
    pub(crate) async fn remember_deleted_child(&self, record: &InProcessChildRecord) {
        let (error, answer_preview) = {
            let state = record.state().await;
            (state.error.clone(), state.answer_preview.clone())
        };
        let deleted = DeletedChild {
            rlm_child_id: record.rlm_child_id.clone(),
            session_id: record.session_id.clone(),
            session_name: record.session_name.clone(),
            session_dir: record.session_dir.clone(),
            started_at_ms: record.started_at_ms,
            answer_preview,
            error: error.unwrap_or_else(|| "Deleted by parent orchestrator".to_string()),
        };
        self.inner
            .deleted_children
            .lock()
            .expect("deleted children lock")
            .insert(deleted.rlm_child_id.clone(), deleted);
    }

    /// The delete tombstones matching one selector (the collect path's
    /// just-deleted envelopes).
    pub(crate) fn deleted_children_matching(&self, target: &str) -> Vec<DeletedChild> {
        self.inner
            .deleted_children
            .lock()
            .expect("deleted children lock")
            .values()
            .filter(|deleted| deleted.matches(target))
            .cloned()
            .collect()
    }

    /// Reserve a requested spawn name (TS #2396): `false` when another
    /// admission of this parent session already holds it, so a racing
    /// spawn fails closed before any engine build.
    pub(crate) fn reserve_spawn_name(&self, name: &str) -> bool {
        self.inner
            .pending_spawn_names
            .lock()
            .expect("spawn name lock")
            .insert(name.to_string())
    }

    /// Release one spawn-name reservation (the admission settled or
    /// failed).
    pub(crate) fn release_spawn_name(&self, name: &str) {
        self.inner
            .pending_spawn_names
            .lock()
            .expect("spawn name lock")
            .remove(name);
    }

    /// Whether a requested spawn name is currently reserved (the TS test
    /// peek).
    ///
    /// # Panics
    ///
    /// Panics when the spawn-name lock is poisoned.
    #[must_use]
    pub fn spawn_name_reserved(&self, name: &str) -> bool {
        self.inner
            .pending_spawn_names
            .lock()
            .expect("spawn name lock")
            .contains(name)
    }

    /// A child session name conflicts when any retained or live child of
    /// this parent already holds it (the TS
    /// `_assertRlmSubagentSessionNameAvailable` parent-side half).
    pub(crate) async fn assert_name_available(&self, name: &str, depth: u32) -> anyhow::Result<()> {
        let children = self.children().await;
        for record in &children {
            if record.session_name == name {
                anyhow::bail!(spawn_name_unavailable(name, depth));
            }
        }
        Ok(())
    }
}

/// The spawn-name-unavailability error (TS
/// `formatAgentSessionNameUnavailable`): one source so the reservation
/// refusal and the availability check stay byte-identical.
pub(crate) fn spawn_name_unavailable(name: &str, depth: u32) -> String {
    format!("Agent name \"{name}\" is unavailable: an agent of that name already exists at depth {depth} under this parent")
}
