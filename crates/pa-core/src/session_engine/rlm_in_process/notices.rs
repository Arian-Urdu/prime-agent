//! The parent's terminal notices: the retained custom rows a child
//! run's end leaves on its parent — no-reply completion, failure, and
//! the delete-path cancellation (TS `createRlmChildTerminalNoticeMessage`
//! / `createRlmChildFailureMessage`).
//!
//! Delivery is one coherent contract, no spawns, no retries, no
//! volatile fallbacks: the row is retained on the parent BEFORE the
//! caller records the settled state. An idle parent receives it as its
//! own admitted turn (the loop persists the row through its
//! `message_end`); a busy parent gets the row appended to its session
//! file immediately (the durable store: visible to `/resume`, the agents
//! view, offline inspection, and the next context rebuild). Live
//! delivery into the CURRENT turn's context is the embedding's queue
//! pump — the daemon's lanes, a guest input surface — and is a reported
//! boundary of this host, not a hidden untracked task.

use std::sync::Arc;

use super::registry::InProcessChildRecord;
use super::{now_ms, InProcessRlmHost};
use crate::session_engine::engine::SessionEngine;
use crate::session_engine::rlm_notices::{
    create_rlm_child_failure_message, create_rlm_child_terminal_notice, RlmChildTerminalNotice,
};

/// Retain one terminal notice row on the parent.
///
/// Idle parent: admit the row as its own turn through the admission-only
/// prompt — the run is live, its `message_end` persists the row, and the
/// caller's settle follows the acknowledged admission. Busy parent (or a
/// turn racing the idle check, or a genuine admission failure): append the
/// row to the parent's session file NOW — the durable retention TS
/// achieves with its admitted action; no future user turn is required
/// for the row to exist. The parent engine is held for the whole
/// retention (a strong Arc across the append), so a concurrent parent
/// drop cannot lose the row mid-delivery.
async fn deliver_notice_row(host: &InProcessRlmHost, row: pa_types::session::CustomMessage) {
    let Some(parent) = host.parent_engine() else {
        return;
    };
    let session = &parent.session;
    if !session.agent().state().await.is_streaming
        && session
            .prompt_injected_message_until_accepted(&row)
            .await
            .is_ok()
    {
        return;
    }
    retain_notice_row(&parent, &row).await;
}

/// Append the row to the parent's session file (the durable store for a
/// busy or unadmittable notice). Failures are swallowed like TS swallows
/// a failed `_deferRlmTerminalNotice` fence: the retention is
/// best-effort against I/O, never against ownership.
async fn retain_notice_row(parent: &Arc<SessionEngine>, row: &pa_types::session::CustomMessage) {
    let persistence = parent.session.shared_persistence();
    let mut session = persistence.lock().await;
    let _ = session.append_custom_message(
        &row.custom_type,
        row.content.clone(),
        row.display,
        row.details.clone(),
    );
}

/// The child failed: `[child-failed child:<name>]` (TS
/// `createRlmChildFailureMessage`). The caller holds the claimed notice.
pub(super) async fn deliver_failure_notice(
    host: &InProcessRlmHost,
    record: &Arc<InProcessChildRecord>,
    error: &str,
) {
    let row = create_rlm_child_failure_message(
        &record.rlm_child_id,
        &record.session_name,
        error,
        now_ms(),
    );
    deliver_notice_row(host, row).await;
}

/// The child finished without an agent-message reply: `[child-exited:
/// no-reply child:<name>]` with the last assistant text (TS
/// `completed_without_reply`). The caller holds the claimed notice.
pub(super) async fn deliver_no_reply_notice(
    host: &InProcessRlmHost,
    record: &Arc<InProcessChildRecord>,
) {
    let preview = record.state().await.answer_preview.clone();
    let row = create_rlm_child_terminal_notice(
        &RlmChildTerminalNotice::CompletedWithoutReply {
            child_id: record.rlm_child_id.clone(),
            session_name: record.session_name.clone(),
            last_assistant_text_preview: preview,
        },
        now_ms(),
    );
    deliver_notice_row(host, row).await;
}

/// The parent deleted a still-running child: `[child-exited: cancelled
/// child:<name>]` (TS `completeDeletion`). The delete path holds the
/// claimed notice.
pub(super) async fn deliver_cancelled_notice(
    host: &InProcessRlmHost,
    record: &Arc<InProcessChildRecord>,
    reason: &str,
) {
    let row = create_rlm_child_terminal_notice(
        &RlmChildTerminalNotice::Cancelled {
            child_id: record.rlm_child_id.clone(),
            session_name: record.session_name.clone(),
            reason: Some(reason.to_string()),
        },
        now_ms(),
    );
    deliver_notice_row(host, row).await;
}
