//! The parent's terminal notices: the durable custom rows a child run's
//! end delivers to its parent — no-reply completion, failure, and the
//! delete-path cancellation (TS `createRlmChildTerminalNoticeMessage` /
//! `createRlmChildFailureMessage`, `_deferRlmTerminalNotice`'s two arms).

use std::sync::Arc;

use super::registry::InProcessChildRecord;
use super::{now_ms, InProcessRlmHost};
use crate::session_engine::rlm_notices::{
    create_rlm_child_failure_message, create_rlm_child_terminal_notice, RlmChildTerminalNotice,
};

/// Deliver one terminal custom row to the parent session: admitted as its
/// own turn while the parent is idle (TS `_enqueueRlmTerminalNoticeAction`,
/// via the admission-only prompt so the settle signal never waits out the
/// parent's model turn), or deferred onto the next admitted turn while the
/// parent runs (TS `_deferRlmTerminalNotice`'s `_pendingNextTurnMessages`
/// arm).
async fn deliver_notice_row(host: &InProcessRlmHost, row: pa_types::session::CustomMessage) {
    let Some(parent) = host.parent_engine() else {
        return;
    };
    let session = &parent.session;
    if session.agent().state().await.is_streaming {
        // TS's deferred immediate action: the busy parent admits the row
        // through the steering lane, so the CURRENT run delivers it at its
        // next boundary — no future user turn is required (the volatile
        // next-turn mailbox would strand it on a quiet session).
        super::family::steer_custom_row(session, &row);
        return;
    }
    match session.prompt_injected_message_until_accepted(&row).await {
        Ok(_) => {}
        Err(error) => {
            if session.agent().state().await.is_streaming {
                // A turn raced the idle check: the steering lane owns the
                // fallback, same as the busy arm.
                super::family::steer_custom_row(session, &row);
            } else {
                // A genuine admission failure is exceptional (a digest
                // capture failing, say): the next-turn mailbox is the
                // last resort so the row is never dropped outright.
                let _ = error;
                session.queue_next_turn_row(row);
            }
        }
    }
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
