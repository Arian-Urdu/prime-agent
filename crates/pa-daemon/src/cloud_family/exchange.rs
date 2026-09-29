//! The journaled request/response exchange over the family wire slice.
//!
//! Two roles, both driven by durable state (TS `cloud-daemon.ts` guest-side
//! pending requests + `cloud-session-registry.ts` remote-request arms):
//!
//! - [`CloudFamilyRequester`] (guest side): one request is appended to the
//!   durable [`FamilyRequestLog`] BEFORE anything is awaited — the append is
//!   the admission gate, and a stalled log fails the send honestly. The
//!   answer arrives only as a journaled `family_roster_result` /
//!   `agent_message_result` command; until then the send is `Pending`,
//!   never "queued" or "delivered". While the laptop is offline the request
//!   sits durably in the log and no receiver truth exists; on reconnect the
//!   answer rides the journal back.
//! - [`CloudFamilyResponder`] (local side): one request event is deduped,
//!   delivered through the [`CloudFamilyDelivery`] seam (which owns the
//!   family-reach assert and the durable local inbox admission), and
//!   answered through the [`FamilyResultSubmitter`] seam with the answer
//!   durably recorded first, so a replayed request re-submits the same
//!   answer without re-delivering.
//!
//! Neither role fabricates a delivery path: the delivery and submit seams
//! are wired by the cloud registry attachment; nothing here falls back to
//! local delivery for a cloud target, and a receipt exists only after the
//! receiver admitted the message.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use pa_types::daemon::cloud::{
    CloudAgentMessageReceipt, CloudFamilyCommand, CloudFamilyCommandPayload, CloudFamilyEvent,
    CloudFamilyEventPayload, CloudFamilyRow,
};
use tokio::sync::oneshot;

use super::log::{FamilyRequestLog, FamilyResultLog};
use super::{
    MAX_REMEMBERED_REQUESTS, MESSAGE_REQUEST_PREFIX, REMOTE_REQUEST_TIMEOUT, ROSTER_REQUEST_PREFIX,
};

/// The sender's view of one cross-boundary send. `Answered` is the only
/// branch that ever carries the receiver's delivery truth: it exists only
/// after the journaled result command arrived, which requires the tunnel
/// up and the target having admitted the message. `Pending` is durable
/// admission without an answer — the request is not lost, but nothing may
/// report it delivered or queued (the wiring layer surfaces it as the TS
/// `cross-boundary request <id> timed out` failure).
#[derive(Debug, Clone, PartialEq)]
pub enum CloudFamilyRequestOutcome<T> {
    /// The journaled answer arrived: the receiver's admitted receipt (or
    /// roster rows).
    Answered(T),
    /// Durably admitted to the request log; no journaled answer within the
    /// request timeout. A late answer for this id is dropped as unknown.
    Pending { request_id: String },
}

/// Why one cross-boundary send failed. None of these variants ever carries
/// or implies a delivery receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudFamilyRequestError {
    /// The durable request log refused the append (full or unwritable):
    /// the request was never admitted, nothing is pending (TS:
    /// `the guest event log is stalled; ...`).
    Stalled(String),
    /// The journaled answer carried an error (TS default:
    /// `the supervisor could not deliver the agent message`).
    Rejected(String),
    /// The exchange was released while the request was pending.
    Released,
}

/// Guest-side outcome of feeding one journaled answer back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// The pending request received its journaled answer.
    Resolved,
    /// No pending request knew the id (an already-timed-out request, or a
    /// replay after restart): a harmless failed dispatch, exactly like TS.
    UnknownRequestId,
}

/// One guest agent message after wire validation and dedupe, as the local
/// delivery seam receives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingCloudMessage {
    pub request_id: String,
    pub from_remote_session_id: String,
    pub target_selector: String,
    pub message: String,
}

/// The local-side delivery seam for cross-boundary family traffic: the
/// cloud registry attachment wires this once cloud rows join the roster.
/// The implementation owns the nuclear-family reach assert and the durable
/// local inbox admission; it returns the receiver-admitted receipt, so
/// nothing in this substrate can claim delivery on its behalf.
pub trait CloudFamilyDelivery: Send + Sync {
    /// Deliver one guest agent message into the local family. The returned
    /// error is reported to the requester verbatim (TS slices it to 2000
    /// UTF-16 units in the answer).
    fn deliver_agent_message(
        &self,
        message: IncomingCloudMessage,
    ) -> impl Future<Output = Result<CloudAgentMessageReceipt, String>> + Send;

    /// Cross-boundary family rows for the requesting remote session (self,
    /// parent, siblings). An error degrades to empty rows, like the TS
    /// registry handler.
    fn family_roster(
        &self,
        for_remote_session_id: &str,
    ) -> impl Future<Output = Result<Vec<CloudFamilyRow>, String>> + Send;
}

/// The journaled-command submit seam for answers (the registry's
/// `submitResident` path): one deterministic command id per request id, so
/// a re-submitted answer dedupes at the receiver's journal.
pub trait FamilyResultSubmitter: Send + Sync {
    /// Submit one answer command under `command_id`. A repeat of the same
    /// id must be a no-op at the receiver's journal.
    fn submit_family_result(
        &self,
        command_id: &str,
        command: &CloudFamilyCommand,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Responder-side outcome of handling one request event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOutcome {
    /// Delivered through the seam; the answer is durable and submitted.
    Answered,
    /// A duplicate request whose answer is already durable: the same
    /// journaled answer re-submitted, no re-delivery.
    DuplicateResubmitted,
    /// A duplicate while the first handling is still in flight: dropped
    /// (the first handling owns the answer).
    AlreadyProcessing,
    /// The submit seam failed after the answer was recorded durably: the
    /// next replay re-submits the same answer.
    SubmitFailed(String),
}

/// The result value one pending request waits for.
enum PendingKind {
    Roster(oneshot::Sender<Result<Vec<CloudFamilyRow>, String>>),
    Message(oneshot::Sender<Result<CloudAgentMessageReceipt, String>>),
}

/// Guest-side half of the exchange: durable requests out, journaled answers
/// in.
pub struct CloudFamilyRequester {
    log: Mutex<FamilyRequestLog>,
    pending: Mutex<HashMap<String, PendingKind>>,
    request_timeout: Duration,
}

impl CloudFamilyRequester {
    /// Build a requester over an opened request log with the TS
    /// [`REMOTE_REQUEST_TIMEOUT`] (30s) pending window.
    #[must_use]
    pub fn new(log: FamilyRequestLog) -> Self {
        Self {
            log: Mutex::new(log),
            pending: Mutex::new(HashMap::new()),
            request_timeout: REMOTE_REQUEST_TIMEOUT,
        }
    }

    /// Same as [`new`](Self::new) with an explicit pending window (the
    /// tests drive short windows; production wiring uses the TS constant).
    #[must_use]
    pub fn with_request_timeout(log: FamilyRequestLog, request_timeout: Duration) -> Self {
        Self {
            log: Mutex::new(log),
            pending: Mutex::new(HashMap::new()),
            request_timeout,
        }
    }

    /// Send one agent message across the boundary. The request is appended
    /// to the durable log first; the returned future resolves `Answered`
    /// only when the journaled receipt arrives, `Pending` at the timeout,
    /// and an error when the log stalls or the answer rejects.
    ///
    /// While the tunnel is down the request rides the durable log; the
    /// send resolves `Pending` (or errors on a stalled log) — it never
    /// reports delivered or queued without a receiver-admitted receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable request log stalls the append or
    /// the journaled answer rejects the delivery; the outcome is never a
    /// fabricated receipt.
    ///
    /// # Panics
    ///
    /// Panics when an internal lock is poisoned (a writer panicked while
    /// holding it).
    pub async fn send_agent_message(
        &self,
        from_remote_session_id: &str,
        target_selector: &str,
        message: &str,
    ) -> Result<CloudFamilyRequestOutcome<CloudAgentMessageReceipt>, CloudFamilyRequestError> {
        let request_id = format!("{MESSAGE_REQUEST_PREFIX}{}", uuid::Uuid::new_v4());
        self.append_request(
            CloudFamilyEventPayload::AgentMessageRequest {
                request_id: request_id.clone(),
                from_remote_session_id: from_remote_session_id.to_string(),
                target_selector: target_selector.to_string(),
                message: message.to_string(),
            },
            "the guest event log is stalled; agent messaging is unavailable",
        )?;
        self.await_answer(request_id, PendingKind::Message).await
    }

    /// Ask the local side for this session's cross-boundary family rows.
    /// Same admission and answer contract as
    /// [`send_agent_message`](Self::send_agent_message); the TS guest
    /// degrades an unanswered roster to an empty list plus a warning.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable request log stalls the append or
    /// the journaled answer rejects the roster fetch.
    ///
    /// # Panics
    ///
    /// Panics when an internal lock is poisoned (a writer panicked while
    /// holding it).
    pub async fn request_family_roster(
        &self,
        from_remote_session_id: &str,
    ) -> Result<CloudFamilyRequestOutcome<Vec<CloudFamilyRow>>, CloudFamilyRequestError> {
        let request_id = format!("{ROSTER_REQUEST_PREFIX}{}", uuid::Uuid::new_v4());
        self.append_request(
            CloudFamilyEventPayload::FamilyRosterRequest {
                request_id: request_id.clone(),
                from_remote_session_id: from_remote_session_id.to_string(),
            },
            "the guest event log is stalled; the family roster is unavailable",
        )?;
        self.await_answer(request_id, PendingKind::Roster).await
    }

    /// Feed one journaled answer command back to its pending request. The
    /// TS default rejection applies when an `agent_message_result` carries
    /// no error.
    ///
    /// # Panics
    ///
    /// Panics when an internal lock is poisoned (a writer panicked while
    /// holding it).
    pub fn resolve_result(&self, command: &CloudFamilyCommand) -> ResolveOutcome {
        let mut pending = self.pending.lock().expect("pending map poisoned");
        let Some(sender) = pending.remove(command.request_id()) else {
            return ResolveOutcome::UnknownRequestId;
        };
        match (command.payload.clone(), sender) {
            (
                CloudFamilyCommandPayload::FamilyRosterResult { entries, .. },
                PendingKind::Roster(sender),
            ) => {
                let _ = sender.send(Ok(entries));
            }
            (
                CloudFamilyCommandPayload::AgentMessageResult {
                    ok, receipt, error, ..
                },
                PendingKind::Message(sender),
            ) => {
                if ok {
                    if let Some(receipt) = receipt {
                        let _ = sender.send(Ok(receipt));
                        return ResolveOutcome::Resolved;
                    }
                }
                let _ = sender.send(Err(error.unwrap_or_else(|| {
                    "the supervisor could not deliver the agent message".to_string()
                })));
            }
            (_, PendingKind::Roster(sender)) => {
                let _ = sender.send(Err(
                    "a family roster request received an agent message result".to_string(),
                ));
            }
            (_, PendingKind::Message(sender)) => {
                let _ = sender.send(Err(
                    "an agent message request received a family roster result".to_string(),
                ));
            }
        }
        ResolveOutcome::Resolved
    }

    /// Reject every pending request (shutdown): dropping the senders closes
    /// their channels, so each awaiter gets
    /// [`CloudFamilyRequestError::Released`]. New answers for released ids
    /// resolve as unknown.
    ///
    /// # Panics
    ///
    /// Panics when an internal lock is poisoned (a writer panicked while
    /// holding it).
    pub fn release(&self) {
        self.pending.lock().expect("pending map poisoned").clear();
    }

    /// The sequence of the newest admitted request (transport
    /// observability).
    ///
    /// # Panics
    ///
    /// Panics when an internal lock is poisoned (a writer panicked while
    /// holding it).
    #[must_use]
    pub fn tail_sequence(&self) -> u64 {
        self.log
            .lock()
            .expect("request log poisoned")
            .tail_sequence()
    }

    /// Admitted requests after `sequence`, oldest first — the replay view
    /// the transport pushes across the boundary on (re)connect. A
    /// `sequence` beyond the tail is a cursor error.
    ///
    /// # Errors
    ///
    /// Returns the TS cursor problem string when `sequence` is beyond the
    /// event tail.
    ///
    /// # Panics
    ///
    /// Panics when an internal lock is poisoned (a writer panicked while
    /// holding it).
    pub fn events_after(&self, sequence: u64) -> Result<Vec<CloudFamilyEvent>, String> {
        self.log
            .lock()
            .expect("request log poisoned")
            .events_after(sequence)
    }

    fn append_request(
        &self,
        payload: CloudFamilyEventPayload,
        stall_message: &str,
    ) -> Result<(), CloudFamilyRequestError> {
        self.log
            .lock()
            .expect("request log poisoned")
            .append(payload)
            .map(|_| ())
            .map_err(|_| CloudFamilyRequestError::Stalled(stall_message.to_string()))
    }

    async fn await_answer<T>(
        &self,
        request_id: String,
        register: impl FnOnce(oneshot::Sender<Result<T, String>>) -> PendingKind,
    ) -> Result<CloudFamilyRequestOutcome<T>, CloudFamilyRequestError> {
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .expect("pending map poisoned")
            .insert(request_id.clone(), register(sender));
        let answer = tokio::time::timeout(self.request_timeout, receiver).await;
        match answer {
            Ok(Ok(Ok(value))) => Ok(CloudFamilyRequestOutcome::Answered(value)),
            Ok(Ok(Err(error))) => Err(CloudFamilyRequestError::Rejected(error)),
            Ok(Err(_)) => {
                // The sender half was dropped without an answer (release
                // paths drop the map entry only through this send).
                self.pending
                    .lock()
                    .expect("pending map poisoned")
                    .remove(&request_id);
                Err(CloudFamilyRequestError::Released)
            }
            Err(_elapsed) => {
                // Timed out: the request is durably admitted but has no
                // journaled answer. Never report it delivered or queued.
                self.pending
                    .lock()
                    .expect("pending map poisoned")
                    .remove(&request_id);
                Ok(CloudFamilyRequestOutcome::Pending { request_id })
            }
        }
    }
}

/// Local-side half of the exchange: request events in, journaled answers
/// out through the delivery and submit seams.
pub struct CloudFamilyResponder {
    results: Mutex<FamilyResultLog>,
    processed: Mutex<BoundedProcessedRequests>,
}

impl CloudFamilyResponder {
    /// Build a responder over an opened result log.
    #[must_use]
    pub fn new(results: FamilyResultLog) -> Self {
        Self {
            results: Mutex::new(results),
            processed: Mutex::new(BoundedProcessedRequests::new(MAX_REMEMBERED_REQUESTS)),
        }
    }

    /// Handle one admitted family request event: dedupe, deliver through
    /// the seam, record the answer durably, then submit it. Duplicate
    /// replays never re-deliver — they re-submit the same durable answer.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable answer record cannot be written
    /// (a disk-level failure: the request is not answered, and the
    /// requester times out — at-least-once, TS parity).
    ///
    /// # Panics
    ///
    /// Panics when an internal lock is poisoned (a writer panicked while
    /// holding it).
    pub async fn handle_event<D: CloudFamilyDelivery, S: FamilyResultSubmitter>(
        &self,
        event: &CloudFamilyEvent,
        delivery: &D,
        submitter: &S,
    ) -> Result<HandleOutcome, String> {
        let request_id = event.request_id().to_string();
        let durable_answer = self
            .results
            .lock()
            .expect("result log poisoned")
            .get(&request_id);
        if let Some(answer) = durable_answer {
            return match submitter
                .submit_family_result(&answer.journal_command_id(), &answer)
                .await
            {
                Ok(()) => Ok(HandleOutcome::DuplicateResubmitted),
                Err(error) => Ok(HandleOutcome::SubmitFailed(error)),
            };
        }
        if self
            .processed
            .lock()
            .expect("processed set poisoned")
            .mark(&request_id)
        {
            return Ok(HandleOutcome::AlreadyProcessing);
        }
        let command = match &event.payload {
            CloudFamilyEventPayload::AgentMessageRequest {
                request_id,
                from_remote_session_id,
                target_selector,
                message,
            } => {
                let delivered = delivery
                    .deliver_agent_message(IncomingCloudMessage {
                        request_id: request_id.clone(),
                        from_remote_session_id: from_remote_session_id.clone(),
                        target_selector: target_selector.clone(),
                        message: message.clone(),
                    })
                    .await;
                match delivered {
                    Ok(receipt) => CloudFamilyCommand {
                        payload: CloudFamilyCommandPayload::AgentMessageResult {
                            request_id: request_id.clone(),
                            ok: true,
                            receipt: Some(receipt),
                            error: None,
                        },
                    },
                    Err(error) => CloudFamilyCommand {
                        payload: CloudFamilyCommandPayload::AgentMessageResult {
                            request_id: request_id.clone(),
                            ok: false,
                            receipt: None,
                            error: Some(truncate_utf16(error, 2000)),
                        },
                    },
                }
            }
            CloudFamilyEventPayload::FamilyRosterRequest {
                from_remote_session_id,
                ..
            } => {
                // The roster answer degrades to empty rows on error (TS
                // parity: the guest renders a degraded local family).
                let rows = delivery
                    .family_roster(from_remote_session_id)
                    .await
                    .unwrap_or_default();
                CloudFamilyCommand {
                    payload: CloudFamilyCommandPayload::FamilyRosterResult {
                        request_id,
                        entries: rows,
                    },
                }
            }
        };
        self.results
            .lock()
            .expect("result log poisoned")
            .record(command.clone())
            .map_err(|error| format!("record answer for {}: {error}", event.request_id()))?;
        let outcome = match submitter
            .submit_family_result(&command.journal_command_id(), &command)
            .await
        {
            Ok(()) => HandleOutcome::Answered,
            Err(error) => HandleOutcome::SubmitFailed(error),
        };
        Ok(outcome)
    }
}

/// Insertion-ordered bounded id set (TS `processedRemoteRequests`: the
/// oldest id is evicted past the bound, so an old replay re-processes).
struct BoundedProcessedRequests {
    ids: Vec<String>,
    max: usize,
}

impl BoundedProcessedRequests {
    fn new(max: usize) -> Self {
        Self {
            ids: Vec::new(),
            max,
        }
    }

    /// Returns false the first time `id` is seen (and records it), true on
    /// a duplicate.
    fn mark(&mut self, id: &str) -> bool {
        if self.ids.iter().any(|seen| seen == id) {
            return true;
        }
        self.ids.push(id.to_string());
        if self.ids.len() > self.max {
            self.ids.remove(0);
        }
        false
    }
}

/// TS `message.slice(0, 2000)`: truncate at a UTF-16 unit boundary so the
/// answer error never exceeds the TS slice, whatever the error's content.
fn truncate_utf16(text: String, units: usize) -> String {
    let mut result = String::new();
    let mut count = 0usize;
    for character in text.chars() {
        let width = character.len_utf16();
        if count + width > units {
            break;
        }
        count += width;
        result.push(character);
    }
    result
}
