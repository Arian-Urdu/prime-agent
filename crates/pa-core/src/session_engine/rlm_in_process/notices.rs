//! The parent's terminal notices: the durable custom rows a child run's
//! end delivers to its parent — no-reply completion, failure, and the
//! delete-path cancellation (TS `createRlmChildTerminalNoticeMessage` /
//! `createRlmChildFailureMessage`, `_deferRlmTerminalNotice`'s two arms).

use std::sync::{Arc, Weak};

use super::registry::InProcessChildRecord;
use super::{now_ms, InProcessRlmHost};
use crate::session_engine::engine::SessionEngine;
use crate::session_engine::rlm_notices::{
    create_rlm_child_failure_message, create_rlm_child_terminal_notice, RlmChildTerminalNotice,
};

/// How many idle windows a deferred notice admission may lose to a busy
/// race before the volatile next-turn mailbox becomes the terminal
/// fallback (a genuine admission failure, not a busy race, drops to it
/// immediately).
const DEFERRED_ADMISSION_ATTEMPTS: u8 = 3;

/// Deliver one terminal custom row to the parent session (TS's deferred
/// immediate action): while the parent is idle, the row is admitted as
/// its own turn through the admission-only prompt (TS
/// `_enqueueRlmTerminalNoticeAction`); while the parent runs, the row is
/// parked and a deferred admission task — owned by this host, holding
/// only a weak parent edge — admits it as the parent's own turn when the
/// current run ends (an abort included), so no future user turn is ever
/// required. The caller may settle the child once this returns: the row
/// is admitted or durably scheduled.
async fn deliver_notice_row(host: &InProcessRlmHost, row: pa_types::session::CustomMessage) {
    let Some(parent) = host.parent_engine() else {
        return;
    };
    let session = &parent.session;
    if session.agent().state().await.is_streaming {
        defer_notice_row(Arc::downgrade(&parent), row);
        return;
    }
    match session.prompt_injected_message_until_accepted(&row).await {
        Ok(_) => {}
        Err(error) => {
            if session.agent().state().await.is_streaming {
                // A turn raced the idle check: the deferred action owns
                // the busy arm, same as above.
                defer_notice_row(Arc::downgrade(&parent), row);
            } else {
                // A genuine admission failure (a digest capture, say) is
                // exceptional: the next-turn mailbox is the terminal
                // fallback so the row is never dropped outright.
                let _ = error;
                session.queue_next_turn_row(row);
            }
        }
    }
}

/// Park the row and admit it as the parent's own turn once the current
/// run ends (TS `_deferRlmTerminalNotice`'s admitted action, without the
/// steer lane's drain-only-while-looping failure: an aborted run ends
/// without draining, and this task still delivers). Weak-held: a parent
/// that goes away cancels the deferral with it.
fn defer_notice_row(parent_weak: Weak<SessionEngine>, row: pa_types::session::CustomMessage) {
    tokio::spawn(async move {
        for _ in 0..DEFERRED_ADMISSION_ATTEMPTS {
            let Some(parent) = parent_weak.upgrade() else {
                return;
            };
            let agent = parent.session.agent().clone();
            drop(parent);
            agent.wait_for_idle().await;
            let Some(parent) = parent_weak.upgrade() else {
                return;
            };
            if parent
                .session
                .prompt_injected_message_until_accepted(&row)
                .await
                .is_ok()
            {
                return;
            }
            if parent.session.agent().state().await.is_streaming {
                // A new turn started between the idle window and the
                // admission: wait for the next one.
                continue;
            }
            // Exceptional (non-busy) failure: the durable next-turn
            // mailbox as the terminal fallback.
            parent.session.queue_next_turn_row(row);
            return;
        }
    });
}

/// The child failed: `[child-failed child:<name>]` (TS
/// `createRlmChildFailureMessage`), claimed exactly once.
pub(super) async fn deliver_failure_notice(
    host: &InProcessRlmHost,
    record: &Arc<InProcessChildRecord>,
    error: &str,
) {
    if !record.claim_notice().await {
        return;
    }
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
/// `completed_without_reply`), claimed exactly once.
pub(super) async fn deliver_no_reply_notice(
    host: &InProcessRlmHost,
    record: &Arc<InProcessChildRecord>,
) {
    if !record.claim_notice().await {
        return;
    }
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
/// child:<name>]` (TS `completeDeletion`). The delete path owns this
/// notice; the run arm already suppressed its own through the claim.
pub(super) async fn deliver_cancelled_notice(
    host: &InProcessRlmHost,
    record: &Arc<InProcessChildRecord>,
    reason: &str,
) {
    if !record.claim_notice().await {
        return;
    }
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
