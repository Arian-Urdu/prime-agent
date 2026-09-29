//! The production cross-boundary delivery seam: one cloud family agent
//! message delivered into the LOCAL family through the real supervisor
//! (the registry, the roster, the worker route), with the receiver-side
//! idempotent admission keyed by the guest request id.
//!
//! Three layers make a duplicate impossible:
//!
//! 1. The seam inbox ([`CloudInboxLog`]) durably binds the request id to
//!    its delivery parameters before the delivery is attempted, and the
//!    receiver-admitted receipt after the target answered — the
//!    reconcile truth for both crash gaps.
//! 2. The receiver inbox (the worker's `cloud_inbox_admission`) admits
//!    the request id in the SAME flush as the queue snapshot that made
//!    the message visible — so a re-drive of an admitted request id can
//!    never produce a second visible message.
//! 3. [`CloudFamilyDelivery::lookup_agent_message`] reconciles: a
//!    recorded receipt answers `Admitted` without re-delivering, and an
//!    admitted-without-receipt request (a crash interrupted the first
//!    handling) is re-driven — safely, because the receiver inbox
//!    dedupes — and the fresh receipt answers `Admitted`.
//!
//! Family reach is asserted over the real roster rows (the durable
//! parent edges): the source resolves like the TS registry
//! (`resolveActive`, by active session id or session id) and the target
//! like the TS supervisor (`findWorker`, by any accepted selector), and
//! the pure TS policy ([`crate::cloud_family::family`]) decides
//! parent/sibling/child. No fabricated transport: the delivery rides
//! the same `worker_deliver_message` route the local supervisor arm
//! uses, and a receipt exists only after the target admitted the
//! message.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use pa_types::daemon::cloud::{CloudAgentMessageReceipt, CloudFamilyRow, CloudFamilyRowStatus};
use serde_json::{json, Map, Value};

use pa_types::daemon::cloud::{CloudFamilyEvent, CloudFamilyEventPayload};

use super::family::{agent_family_relationship, family_row_from_summary, AGENT_FAMILY_REACH_ERROR};
use super::inbox::CloudInboxLog;
use super::{AgentMessageLookup, CloudFamilyDelivery, IncomingCloudMessage};
use crate::lease::canonical_session_path;
use crate::registry::ResidentWorker;
use crate::supervisor::Supervisor;
use std::path::Path;

/// The worker round-trip budget for the delivery route (TS
/// `WORKER_REQUEST_TIMEOUT_MS`).
const WORKER_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// The production [`CloudFamilyDelivery`]: the real supervisor's
/// registry, roster, and worker route, plus the seam's request-id keyed
/// durable inbox journal.
pub struct LocalFamilyDelivery {
    supervisor: Arc<Supervisor>,
    inbox: Mutex<CloudInboxLog>,
}

impl LocalFamilyDelivery {
    /// Build the delivery seam over the live supervisor, with the inbox
    /// journal at `inbox_path` (the cloud registry attachment owns the
    /// placement; the journal survives supervisor restarts).
    ///
    /// # Errors
    ///
    /// Returns an error when the inbox journal cannot be opened.
    pub fn new(supervisor: Arc<Supervisor>, inbox_path: PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            supervisor,
            inbox: Mutex::new(CloudInboxLog::open(&inbox_path)?),
        })
    }

    /// Deliver one cross-boundary message, idempotently by request id.
    async fn deliver_idempotent(
        &self,
        message: &IncomingCloudMessage,
    ) -> Result<CloudAgentMessageReceipt, String> {
        // The recorded receipt is the delivery truth: a duplicate event
        // (a wire replay) answers it without any new delivery.
        {
            let mut inbox = self.locked_inbox();
            if let Some(receipt) = inbox.receipt(&message.request_id) {
                return Ok(receipt);
            }
            // Durable admission BEFORE the delivery (the crash-gap
            // protocol): the re-drive input survives the crash.
            inbox
                .admit(message)
                .map_err(|error| format!("admit {}: {error:#}", message.request_id))?;
        }
        let receipt = self.deliver_to_local_family(message).await?;
        {
            let mut inbox = self.locked_inbox();
            inbox
                .record_receipt(&message.request_id, receipt.clone())
                .map_err(|error| {
                    format!("record the receipt for {}: {error:#}", message.request_id)
                })?;
        }
        Ok(receipt)
    }

    /// The TS `deliverCloudAgentMessage` local arm: resolve the source
    /// and the target from the real registry and roster, assert the
    /// nuclear-family reach, and route `worker_deliver_message` with the
    /// idempotency key.
    async fn deliver_to_local_family(
        &self,
        message: &IncomingCloudMessage,
    ) -> Result<CloudAgentMessageReceipt, String> {
        // The source resolves first, like the TS registry handler
        // (`resolveActive`): an unknown source answers with the TS error.
        let source_summary = self
            .remote_summary(&message.from_remote_session_id)
            .ok_or_else(|| {
                format!(
                    "Unknown cloud message source: {}",
                    message.from_remote_session_id
                )
            })?;
        let target = self
            .supervisor
            .registry
            .resolve(&message.target_selector)
            .await
            .map_err(|error| error.to_string())?;
        let target_summary = self
            .target_summary(&target, &message.target_selector)
            .await?;
        let source_row = family_row_from_summary(&source_summary);
        let target_row = family_row_from_summary(&target_summary);
        // The nuclear-family reach assert (TS `assertAgentFamilyReach`).
        if agent_family_relationship(&source_row, &target_row).is_none() {
            return Err(AGENT_FAMILY_REACH_ERROR.to_string());
        }
        // The TS self-target guard.
        let source_active = source_summary
            .get("activeSessionId")
            .and_then(Value::as_str)
            .or_else(|| source_summary.get("id").and_then(Value::as_str))
            .unwrap_or_default();
        // TS `target.summary.activeSessionId ?? target.summary.id`.
        let target_active = target_summary
            .get("activeSessionId")
            .and_then(Value::as_str)
            .or_else(|| target_summary.get("id").and_then(Value::as_str))
            .unwrap_or_default();
        if source_active == target_active {
            return Err("Agent messaging cannot target the sending session".to_string());
        }
        // The TS sender block for a cloud source: the endpoint fields,
        // no parent edges (the local arm of `deliverCloudAgentMessage`).
        let mut sender = json!({
            "activeSessionId": source_active,
            "sessionId": source_summary
                .get("sessionId")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "runtimeKind": source_summary
                .get("runtimeKind")
                .and_then(Value::as_str)
                .unwrap_or("top-level"),
        });
        if let Some(name) = source_summary
            .get("sessionName")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
        {
            sender["sessionName"] = json!(name);
        }
        let mut rest = Map::new();
        rest.insert("cloudRequestId".to_string(), json!(message.request_id));
        let delivery = pa_types::daemon::DaemonWorkerCommand::WorkerDeliverMessage {
            id: None,
            target_active_session_id: target_active.to_string(),
            message: message.message.clone(),
            sender,
            delivery_mode: None,
            rest,
        };
        let payload = serde_json::to_value(&delivery)
            .map_err(|error| format!("invalid delivery command: {error}"))?;
        let response = self
            .supervisor
            .route_command_typed(
                &target,
                "worker_deliver_message",
                payload,
                WORKER_REQUEST_TIMEOUT_MS,
                crate::backpressure::RouteAdmission::SupervisorInternal,
            )
            .await
            .map_err(|error| error.to_string())?;
        if !response.success {
            return Err(response
                .error
                .unwrap_or_else(|| "delivery failed".to_string()));
        }
        let data = response.data.unwrap_or(Value::Null);
        // TS receipt validation: `id` and `deliveryStatus` must be
        // strings, else the TS invalid-receipt error.
        if data.get("id").and_then(Value::as_str).is_none()
            || data.get("deliveryStatus").and_then(Value::as_str).is_none()
        {
            return Err("Session worker returned an invalid agent-message receipt".to_string());
        }
        serde_json::from_value::<CloudAgentMessageReceipt>(data)
            .map_err(|_| "Session worker returned an invalid agent-message receipt".to_string())
    }

    /// One remote session's roster summary (TS `resolveActive`): by its
    /// active session id first, then by its session id (the roster agent
    /// id of a top-level row).
    fn remote_summary(&self, remote_session_id: &str) -> Option<Value> {
        let roster = self
            .supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roster
            .by_active_session_id(remote_session_id)
            .or_else(|| roster.get(remote_session_id))
            .map(|entry| entry.summary.clone())
    }

    /// The resolved target's roster summary (the TS `findWorker`
    /// target: the roster entry of the resident's root session).
    async fn target_summary(
        &self,
        target: &Arc<ResidentWorker>,
        selector: &str,
    ) -> Result<Value, String> {
        let (root_active_session_id, _, _) = target.labels().await;
        let roster = self
            .supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roster
            .by_active_session_id(&root_active_session_id)
            .map(|entry| entry.summary.clone())
            .ok_or_else(|| format!("Unknown active session: {selector}"))
    }

    fn locked_inbox(&self) -> std::sync::MutexGuard<'_, CloudInboxLog> {
        self.inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl CloudFamilyDelivery for LocalFamilyDelivery {
    async fn deliver_agent_message(
        &self,
        message: IncomingCloudMessage,
    ) -> Result<CloudAgentMessageReceipt, String> {
        self.deliver_idempotent(&message).await
    }

    /// The reconciliation lookup: the recorded receipt answers
    /// `Admitted`; an admitted-without-receipt request (a crash
    /// interrupted the first handling) is re-driven — safe, because the
    /// receiver inbox dedupes by request id — and its fresh receipt
    /// answers `Admitted`; a request this receiver never admitted
    /// answers `Unknown` (the honest limit the responder surfaces as
    /// uncertain).
    async fn lookup_agent_message(&self, request_id: &str) -> AgentMessageLookup {
        let (admission, receipt) = {
            let inbox = self.locked_inbox();
            (inbox.admission(request_id), inbox.receipt(request_id))
        };
        if let Some(receipt) = receipt {
            return AgentMessageLookup::Admitted(receipt);
        }
        let Some(message) = admission else {
            return AgentMessageLookup::Unknown;
        };
        match self.deliver_idempotent(&message).await {
            Ok(receipt) => AgentMessageLookup::Admitted(receipt),
            // The re-drive failed (the target is gone, the reach is now
            // refused): the request stays reconcilable — answer the
            // honest Unknown so the responder leaves it uncertain for
            // the wiring layer's next pass, never claiming a receipt
            // that does not exist.
            Err(_) => AgentMessageLookup::Unknown,
        }
    }

    /// The TS `cloudFamilyRowsFor` port over the real roster: the
    /// requesting remote session's own row, its parent (through the
    /// durable session-file edge), and its siblings (same parent file,
    /// same depth). A session without a parent edge answers its own row
    /// alone (TS returns early); a resolution failure degrades to empty
    /// rows.
    async fn family_roster(
        &self,
        for_remote_session_id: &str,
    ) -> Result<Vec<CloudFamilyRow>, String> {
        let Some(source_summary) = self.remote_summary(for_remote_session_id) else {
            return Ok(Vec::new());
        };
        let roster = self
            .supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let self_row = family_row_from_summary(&source_summary);
        let mut rows = vec![self_row.clone()];
        let Some(parent_path) = self_row.parent_session_path.clone() else {
            return Ok(rows);
        };
        // The parent row: the roster entry for the parent's session
        // file, else the bare file identity (TS falls back to the spawn
        // record's parent id, then the file name).
        if let Some(parent) = roster.by_session_file(&parent_path) {
            let parent_row = family_row_from_summary(&parent.summary);
            rows.push(parent_row);
        } else {
            let file_name = std::path::Path::new(&parent_path).file_name().map_or_else(
                || parent_path.clone(),
                |name| name.to_string_lossy().to_string(),
            );
            rows.push(CloudFamilyRow {
                id: file_name,
                name: None,
                depth: self_row.depth.saturating_sub(1),
                status: CloudFamilyRowStatus::Running,
                parent_session_id: None,
                parent_session_path: None,
                session_path: Some(parent_path.clone()),
            });
        }
        // Siblings: same parent file, same depth, any runtime kind (the
        // scan runs whatever the parent lookup found, exactly like TS).
        let self_session_id = source_summary
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        for entry in roster.entries() {
            let summary = entry.summary;
            if summary.get("sessionId").and_then(Value::as_str) == Some(self_session_id) {
                continue;
            }
            let depth = summary
                .get("rlmDepth")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| {
                    usize::from(
                        summary
                            .get("parentSessionPath")
                            .and_then(Value::as_str)
                            .is_some_and(|path| !path.is_empty()),
                    )
                    .try_into()
                    .unwrap_or(0)
                });
            if depth != self_row.depth {
                continue;
            }
            let sibling_parent = summary
                .get("parentSessionPath")
                .and_then(Value::as_str)
                .filter(|path| !path.is_empty())
                .map(|path| {
                    canonical_session_path(Path::new(path))
                        .to_string_lossy()
                        .to_string()
                });
            if sibling_parent.as_deref() != Some(parent_path.as_str()) {
                continue;
            }
            rows.push(family_row_from_summary(&summary));
        }
        Ok(rows)
    }
}

/// The wiring layer's uncertain-request reconcile (TS parity for the
/// outcome, Rust parity for the crash gaps the TS side cannot close): one
/// pass over the responder's admitted-without-answer ids, driving each
/// through the receiver's idempotent seam and recording the answer so
/// the next replay re-submits it. Two crash gaps close here:
///
/// - A receiver that recorded the admission answers the LOOKUP with the
///   recorded receipt — no re-delivery, no duplicate visible message.
/// - A request the seam never admitted was provably never attempted
///   (the seam admits durably BEFORE any delivery), so re-delivering is
///   safe — and the receiver inbox dedupes by request id, so even an
///   in-flight first attempt cannot double-deliver.
///
/// Returns the request ids reconciled (their answers are durable; the
/// next replay of the event re-submits them without delivery).
pub async fn reconcile_uncertain<D: CloudFamilyDelivery>(
    responder: &crate::cloud_family::CloudFamilyResponder,
    delivery: &D,
    events: &[CloudFamilyEvent],
) -> Vec<String> {
    use pa_types::daemon::cloud::{CloudFamilyCommand, CloudFamilyCommandPayload};

    let uncertain = responder.uncertain();
    let mut reconciled = Vec::new();
    for request_id in uncertain {
        // The replayed event carrying the payload (an id whose event is
        // not in this batch stays for the next pass).
        let Some(event) = events.iter().find(|event| event.request_id() == request_id) else {
            continue;
        };
        let CloudFamilyEventPayload::AgentMessageRequest {
            request_id,
            from_remote_session_id,
            target_selector,
            message,
        } = &event.payload
        else {
            continue;
        };
        let incoming = IncomingCloudMessage {
            request_id: request_id.clone(),
            from_remote_session_id: from_remote_session_id.clone(),
            target_selector: target_selector.clone(),
            message: message.clone(),
        };
        let (ok, receipt, error) = match delivery.lookup_agent_message(request_id).await {
            // The receiver admitted: its receipt is the delivery truth.
            AgentMessageLookup::Admitted(receipt) => (true, Some(receipt), None),
            // The seam never admitted this id: no delivery was ever
            // attempted through it — re-drive safely.
            AgentMessageLookup::Unknown => match delivery.deliver_agent_message(incoming).await {
                Ok(receipt) => (true, Some(receipt), None),
                Err(error) => (false, None, Some(error)),
            },
        };
        responder
            .record_answer(CloudFamilyCommand {
                payload: CloudFamilyCommandPayload::AgentMessageResult {
                    request_id: request_id.clone(),
                    ok,
                    receipt,
                    error,
                },
            })
            .ok();
        reconciled.push(request_id.clone());
    }
    reconciled
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
