//! Construct one first-wins terminal row and admit it through the parent
//! session inbox. The host never writes the parent's file or agent queue.

use std::sync::Arc;

use super::now_ms;
use super::registry::{InProcessChildRecord, NoticeKind};
use crate::session_engine::rlm_notices::{
    create_rlm_child_failure_message, create_rlm_child_terminal_notice, RlmChildTerminalNotice,
};

pub(super) fn terminal_row(
    record: &InProcessChildRecord,
    kind: NoticeKind,
    error: Option<&str>,
    preview: Option<String>,
) -> Option<pa_types::session::CustomMessage> {
    match kind {
        NoticeKind::Done => Some(create_rlm_child_terminal_notice(
            &RlmChildTerminalNotice::CompletedWithoutReply {
                child_id: record.rlm_child_id.clone(),
                session_name: record.session_name.clone(),
                last_assistant_text_preview: preview,
            },
            now_ms(),
        )),
        NoticeKind::Error => Some(create_rlm_child_failure_message(
            &record.rlm_child_id,
            &record.session_name,
            error.unwrap_or("Child run failed"),
            now_ms(),
        )),
        NoticeKind::Cancelled => Some(create_rlm_child_terminal_notice(
            &RlmChildTerminalNotice::Cancelled {
                child_id: record.rlm_child_id.clone(),
                session_name: record.session_name.clone(),
                reason: Some("Deleted by parent orchestrator".to_string()),
            },
            now_ms(),
        )),
        NoticeKind::DoneReplied | NoticeKind::Closed | NoticeKind::ParentGone => None,
    }
}

/// Keep the winning claim pending on every admission failure. Retrying the
/// exact same row is safe: the parent session's stable key is idempotent.
/// A closed parent is handled by the closer's separate first-wins claim.
pub(super) async fn admit_claimed_notice(
    record: &Arc<InProcessChildRecord>,
    parent: Arc<crate::session_engine::engine::SessionEngine>,
    row: pa_types::session::CustomMessage,
) -> bool {
    const BURST_ATTEMPTS: u32 = 3;
    const PARKED_RETRY_MS: u64 = 5_000;
    let mut failures = 0_u32;
    let mut held_parent = Some(parent);
    let mut closed = record.closed_tx.subscribe();
    record.clear_parked().await;
    loop {
        // The original parent stays alive for the whole transaction. Close
        // waits until this transaction commits or parks; once closed, no
        // second live registration is attempted against that generation.
        if failures > 0 && *closed.borrow_and_update() {
            record.mark_parked().await;
            return false;
        }
        let Some(parent) = held_parent
            .take()
            .or_else(|| record.parent_engine.upgrade())
        else {
            record
                .admission_failed(&anyhow::anyhow!(
                    "original parent engine was released before retry; child outcome is unknown"
                ))
                .await;
            record.mark_parked().await;
            return false;
        };
        let result = parent
            .session
            .admit_terminal_notice(&record.parent_session_id, &record.rlm_child_id, &row)
            .await;
        drop(parent);
        match result {
            Ok(()) => return true,
            Err(error) => {
                record.admission_failed(&error).await;
                if error.downcast_ref::<crate::session_engine::terminal_inbox::ClosedTerminalGeneration>().is_some()
                    || error.downcast_ref::<crate::session_engine::terminal_inbox::MismatchedTerminalGeneration>().is_some()
                    || *closed.borrow_and_update()
                {
                    record.mark_parked().await;
                    return false;
                }
            }
        }
        failures += 1;
        if failures >= BURST_ATTEMPTS {
            // A parked record remains pending and visible. The original
            // run task makes sparse retry attempts while the parent stays
            // open; close wakes it and releases its listener/engine.
            record.mark_parked().await;
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_millis(PARKED_RETRY_MS)) => {}
                changed = closed.changed() => {
                    if changed.is_err() || *closed.borrow_and_update() { return false; }
                }
            }
            if *closed.borrow_and_update() {
                return false;
            }
            record.clear_parked().await;
            failures = 0;
            continue;
        }
        let delay_ms = 250_u64 * u64::from(failures);
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_millis(delay_ms)) => {}
            changed = closed.changed() => {
                if changed.is_err() || *closed.borrow_and_update() {
                    record.mark_parked().await;
                    return false;
                }
            }
        }
    }
}
