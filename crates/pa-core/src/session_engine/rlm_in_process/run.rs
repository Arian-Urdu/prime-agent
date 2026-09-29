//! The detached child run task and the host's roster/collect/delete
//! surface. The run task subscribes the child agent's events (live
//! activity, tool counts, answer previews, per-origin usage batches — the
//! introspection the daemon host gets from worker summaries), prompts the
//! child behind the parent's turn boundary, waits for the task run to
//! settle, and delivers the parent's terminal notice when the child never
//! replied.

use std::sync::Arc;
use std::time::Duration;

use pa_agent::types::{AgentEvent, StopReason};
use pa_types::session::{ChildUsageOrigin, FileEntry};

use super::registry::{compact_rlm_text, record_matches, InProcessChildRecord};
use super::{now_ms, InProcessRlmHost};
use crate::session_engine::agent_messaging::AGENT_MESSAGE_CUSTOM_TYPE;
use crate::session_engine::rlm_host::{
    RlmChildResult, RlmDeleteSubagentResult, RlmHostFuture, RlmSubagentEntry,
};
/// Settle-poll slice while the child still runs (the daemon watcher's
/// cadence).
const SETTLE_POLL_SLICE_MS: u64 = 250;
/// Grace between the first idle observation and the settle decision (the
/// daemon watcher's stability re-check).
const SETTLE_GRACE_MS: u64 = 250;

/// The detached child run task (TS `_startRlmChildRun`'s detached arm).
/// Errors never propagate: every terminal state lands on the record.
pub(super) async fn run_child_task(
    host: InProcessRlmHost,
    record: Arc<InProcessChildRecord>,
    prompt: String,
    turn_generation: u64,
) {
    // The parent's continuation request for the spawning turn is in
    // flight before the child's first model turn starts (the daemon
    // host's deterministic ordering).
    host.wait_turn_done(turn_generation).await;
    if record.state().await.closed_by_parent {
        return;
    }
    subscribe_child_events(&record).await;
    record.state().await.prompt_admitted = true;
    // The daemon prompt shape: the raw task text as the child's first
    // user row (the in-process host matches the established Rust child
    // surface; the TS custom-row spawn label is a documented divergence).
    let admission = record
        .engine
        .session
        .prompt(&prompt, crate::session_engine::PromptOptions::default())
        .await;
    if let Err(error) = admission {
        let error = error.to_string();
        record.settle_as("error", Some(error.clone())).await;
        flush_pending_usage(&host, &record).await;
        super::notices::deliver_failure_notice(&host, &record, &error).await;
        return;
    }
    wait_for_task_settle(&record).await;
    flush_pending_usage(&host, &record).await;
    let (status, error, replied) = {
        let state = record.state().await;
        (
            state.settled_status,
            state.error.clone(),
            state.replied_since_task,
        )
    };
    // A cancel or close claimed the verdict and its notice already; a
    // failed run reports its failure, a completed one without an explicit
    // reply reports the no-reply notice.
    if status == Some("error") {
        if let Some(error) = error {
            super::notices::deliver_failure_notice(&host, &record, &error).await;
        }
    } else if status == Some("done") && !replied {
        super::notices::deliver_no_reply_notice(&host, &record).await;
    }
}

/// Subscribe the child agent's events into the record: activity, tool
/// counts, answer previews, last-activity clock, and the per-origin
/// usage batches that flush at run ends.
async fn subscribe_child_events(record: &Arc<InProcessChildRecord>) {
    let agent = record.engine.session.agent().clone();
    let run_record = Arc::clone(record);
    agent
        .subscribe(move |event, _signal| {
            let run_record = Arc::clone(&run_record);
            Box::pin(async move {
                observe_child_event(&run_record, event).await;
                Ok(())
            })
        })
        .await;
}

/// One child event's record updates (TS `_startRlmChildRun`'s child
/// subscription).
async fn observe_child_event(record: &Arc<InProcessChildRecord>, event: AgentEvent) {
    let now = now_ms();
    let engine = Arc::clone(&record.engine);
    let mut state = record.state().await;
    match event {
        AgentEvent::AgentStart => {
            state.activity = Some(super::registry::ChildActivity {
                kind: "waiting",
                tool_name: None,
            });
            state.last_activity_at_ms = now;
        }
        AgentEvent::MessageStart { message } => {
            observe_streaming_assistant(&mut state, &message, now);
        }
        AgentEvent::MessageUpdate { message, .. } => {
            observe_streaming_assistant(&mut state, &message, now);
        }
        AgentEvent::MessageEnd { message } => {
            if let pa_agent::types::AgentMessage::Standard(pa_agent::types::Message::Assistant(
                assistant,
            )) = &message
            {
                if !matches!(
                    assistant.stop_reason,
                    StopReason::Error | StopReason::Aborted
                ) {
                    let origin = child_usage_origin(&engine).await;
                    // The producer folds the shared wire-shape usage: the
                    // agent-loop event's usage crosses through the same
                    // JSON round trip every session boundary uses.
                    if let Some(wire_usage) = super::super::provider_adapter::json_round_trip::<
                        _,
                        pa_types::ai::AssistantMessage,
                    >(assistant)
                    .map(|wire| wire.usage)
                    {
                        match state
                            .pending_usage
                            .iter_mut()
                            .find(|(bucket_origin, _)| *bucket_origin == origin)
                        {
                            Some((_, usage)) => {
                                crate::session_engine::rlm_usage::add_assistant_usage(
                                    usage,
                                    &wire_usage,
                                );
                            }
                            None => state.pending_usage.push((origin, wire_usage)),
                        }
                    }
                }
                let text = compact_rlm_text(&assistant_text(assistant));
                if !text.is_empty() {
                    state.answer_preview = Some(text);
                }
                state.last_activity_at_ms = now;
            }
        }
        AgentEvent::ToolExecutionStart { tool_name, .. } => {
            state.tool_use_count += 1;
            state.running_tools += 1;
            state.activity = Some(super::registry::ChildActivity {
                kind: "executing",
                tool_name: Some(tool_name),
            });
            state.last_activity_at_ms = now;
        }
        AgentEvent::ToolExecutionEnd { .. } => {
            state.running_tools = state.running_tools.saturating_sub(1);
            if state.running_tools == 0 {
                state.activity = Some(super::registry::ChildActivity {
                    kind: "waiting",
                    tool_name: None,
                });
            }
            state.last_activity_at_ms = now;
        }
        AgentEvent::AgentEnd { .. } => {
            state.activity = None;
            state.last_activity_at_ms = now;
        }
        AgentEvent::TurnStart
        | AgentEvent::TurnEnd { .. }
        | AgentEvent::ToolExecutionUpdate { .. } => {}
    }
}

/// A streaming assistant message's preview update (TS updates
/// `answerPreview` and the `writing` activity on message start/update).
fn observe_streaming_assistant(
    state: &mut super::registry::ChildRunState,
    message: &pa_agent::types::AgentMessage,
    now: u64,
) {
    let Some(assistant) = streaming_assistant(message) else {
        return;
    };
    let text = compact_rlm_text(&assistant_text(assistant));
    if !text.is_empty() {
        state.answer_preview = Some(text);
    }
    state.activity = Some(super::registry::ChildActivity {
        kind: "writing",
        tool_name: None,
    });
    state.last_activity_at_ms = now;
}

/// The assistant message behind a streaming event, when it is one.
fn streaming_assistant(
    message: &pa_agent::types::AgentMessage,
) -> Option<&pa_agent::types::AssistantMessage> {
    match message {
        pa_agent::types::AgentMessage::Standard(pa_agent::types::Message::Assistant(assistant)) => {
            Some(assistant)
        }
        _ => None,
    }
}

/// The assistant message's text blocks joined (TS `readAssistantText`).
fn assistant_text(assistant: &pa_agent::types::AssistantMessage) -> String {
    assistant
        .content
        .iter()
        .filter_map(|block| match block {
            pa_agent::types::AssistantContent::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect()
}

/// The completion's usage origin (TS `rlmChildUsageOrigin`): the nearest
/// preceding user or agent-message row labels it — the task prompt (the
/// child file's first user row) labels `spawn_task`, agent-message rows
/// label `agent_message`, any other custom row and later user rows
/// `direct_user`. Completions with an error or aborted stop reason fold
/// nowhere (the caller filters them first).
async fn child_usage_origin(
    engine: &Arc<crate::session_engine::engine::SessionEngine>,
) -> ChildUsageOrigin {
    let persistence = engine.session.shared_persistence();
    let entries = {
        let session = persistence.lock().await;
        session.get_entries()
    };
    let first_user_row = entries.iter().position(|entry| {
        matches!(
            entry,
            FileEntry::Message {
                message: pa_types::session::AgentMessage::User(_),
                ..
            }
        )
    });
    // Walk back from the row before the just-appended assistant row; the
    // nearest user or custom row labels this completion. Any other row is
    // not a label row and the walk keeps going.
    for (index, entry) in entries.iter().enumerate().rev().skip(1) {
        if let FileEntry::Message {
            message: pa_types::session::AgentMessage::User(_),
            ..
        } = entry
        {
            return if Some(index) == first_user_row {
                ChildUsageOrigin::SpawnTask
            } else {
                ChildUsageOrigin::DirectUser
            };
        }
        if let FileEntry::CustomMessage { payload, .. } = entry {
            let message_id = payload
                .details
                .as_ref()
                .and_then(|details| details.get("id"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            return if payload.custom_type == AGENT_MESSAGE_CUSTOM_TYPE {
                if message_id.starts_with("spawn:") {
                    ChildUsageOrigin::SpawnTask
                } else {
                    ChildUsageOrigin::AgentMessage
                }
            } else {
                ChildUsageOrigin::DirectUser
            };
        }
    }
    ChildUsageOrigin::DirectUser
}

/// Wait until the child's task run settles: the agent is idle, its queues
/// are empty, and its own children (grandchildren of the spawning parent)
/// settled too (TS `waitForRlmQuiescence`). Follow-up turns a delivered
/// agent message starts keep the task unsettled until they drain.
async fn wait_for_task_settle(record: &Arc<InProcessChildRecord>) {
    loop {
        let agent = record.engine.session.agent();
        agent.wait_for_idle().await;
        let child_host = record.child_host.clone();
        let stable = !agent.has_queued_messages() && !child_host.any_running().await;
        if stable {
            tokio::time::sleep(Duration::from_millis(SETTLE_GRACE_MS)).await;
            let still_idle = !agent.state().await.is_streaming;
            let still_empty = !agent.has_queued_messages() && !child_host.any_running().await;
            if still_idle && still_empty {
                break;
            }
            continue;
        }
        tokio::time::sleep(Duration::from_millis(SETTLE_POLL_SLICE_MS)).await;
    }
    record.settle_as("done", None).await;
}

/// Flush the child's per-origin usage batches into the parent's
/// attribution producer (TS `flushPendingChildUsageAttribution` at the
/// settle boundary; one durable row per origin batch). A released parent
/// has no durable target and the batches drop, exactly like TS folding
/// into a session that is already gone.
async fn flush_pending_usage(host: &InProcessRlmHost, record: &Arc<InProcessChildRecord>) {
    let batches = std::mem::take(&mut record.state().await.pending_usage);
    if batches.is_empty() {
        return;
    }
    let Some(parent) = host.parent_engine() else {
        return;
    };
    parent
        .rlm_usage
        .record_child_usage(crate::session_engine::rlm_usage::RlmChildUsageReport {
            rlm_child_id: record.rlm_child_id.clone(),
            batches,
        })
        .await;
}

/// The roster: every live child's row (the tombstones answer `collect`
/// selectors only).
pub(super) fn list_subagents(host: InProcessRlmHost) -> RlmHostFuture<Vec<RlmSubagentEntry>> {
    Box::pin(async move {
        let children = host.children().await;
        let now = now_ms();
        let mut entries = Vec::with_capacity(children.len());
        for record in children {
            entries.push(record.entry(now).await);
        }
        Ok(entries)
    })
}

/// Resolve one target against the live registry with the TS selector
/// errors.
async fn resolve_record(
    host: &InProcessRlmHost,
    target: &str,
    kind: &str,
) -> anyhow::Result<Arc<InProcessChildRecord>> {
    let children = host.children().await;
    let matches: Vec<Arc<InProcessChildRecord>> = children
        .into_iter()
        .filter(|record| record_matches(record, target))
        .collect();
    match matches.len() {
        0 => {
            anyhow::bail!("No direct RLM {kind} matches \"{target}\" in the current parent session")
        }
        1 => Ok(matches.into_iter().next().expect("one match")),
        _ => anyhow::bail!(
            "RLM {kind} selector \"{target}\" is ambiguous in the current parent session"
        ),
    }
}

/// `rlm.collect`: typed snapshots of the selected children (default:
/// every child), bounded by the timeout — a timeout returns snapshots,
/// never errors. A target whose delete receipt already returned resolves
/// to its settled cancelled envelope (TS #2388).
pub(super) fn collect(
    host: InProcessRlmHost,
    targets: Vec<String>,
    timeout_ms: u64,
) -> RlmHostFuture<Vec<RlmChildResult>> {
    Box::pin(async move {
        let mut records: Vec<Arc<InProcessChildRecord>> = if targets.is_empty() {
            host.children().await
        } else {
            Vec::with_capacity(targets.len())
        };
        let mut deleted_results = Vec::new();
        for target in &targets {
            match resolve_record(&host, target, "child").await {
                Ok(record) => records.push(record),
                Err(miss) => {
                    let matches = host.deleted_children_matching(target);
                    match matches.len() {
                        0 => return Err(miss),
                        1 => {
                            deleted_results.push(matches[0].collect_result());
                        }
                        _ => anyhow::bail!(
                            "RLM child selector \"{target}\" is ambiguous in the current parent session"
                        ),
                    }
                }
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
        let mut results = Vec::with_capacity(records.len());
        for record in &records {
            if record.is_running().await {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let mut settled = record.settled_tx.subscribe();
                let _ = tokio::time::timeout(remaining, settled.wait_for(|settled| *settled)).await;
            }
            results.push(record.collect_result(now_ms()).await);
        }
        // Live entries first, the deleted generations' envelopes after
        // (TS `collectRlmChildren` result order).
        results.extend(deleted_results);
        Ok(results)
    })
}

/// `rlm.delete_subagent`: cancel a running child (abort + cancelled
/// verdict + the cancelled terminal notice), tear a settled one down, and
/// leave the tombstone behind so a just-deleted selector still collects
/// (TS #2388). The receipt's outcome is `deleted`.
pub(super) fn delete_subagent(
    host: InProcessRlmHost,
    target: String,
) -> RlmHostFuture<RlmDeleteSubagentResult> {
    Box::pin(async move {
        let record = resolve_record(&host, &target, "subagent").await?;
        let was_running = record.is_running().await;
        {
            let mut state = record.state().await;
            state.closed_by_parent = true;
            state.notice_delivered = true;
        }
        if was_running {
            // No recorded error: the tombstone's envelope reads the TS
            // fallback reason ("Deleted by parent orchestrator"), the
            // same text the cancelled notice carries.
            record.settle_as("cancelled", None).await;
            super::notices::deliver_cancelled_notice(
                &host,
                &record,
                "Deleted by parent orchestrator",
            )
            .await;
            record.engine.session.agent().abort();
        }
        host.remember_deleted_child(&record).await;
        let entry = record.entry(now_ms()).await;
        host.remove_child(&record).await;
        Ok(RlmDeleteSubagentResult {
            subagent: entry,
            outcome: Some("deleted"),
        })
    })
}
