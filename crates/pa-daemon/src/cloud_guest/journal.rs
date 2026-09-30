//! Durable guest command journal (TS `command-journal.ts` port).
//!
//! Admissions and transitions are fsynced before the call acknowledges
//! them, so a crash after any return value can never lose state.
//! Claiming fsyncs the running transition before the request is handed
//! out, so a command restored `accepted` was never dispatched and stays
//! pending; a command restored `running` may have started executing and
//! is marked uncertain: claiming skips it until the host explicitly
//! requeues or settles it. A retry with the same command id and the same
//! request digest replays the stored receipt; the same id with a
//! different request is a conflict and is never re-admitted.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use pa_types::daemon::cloud::{
    canonical_json, cloud_digest, cloud_id_problem, cloud_request_digest, cloud_request_problem,
    is_cloud_digest, CloudCommandId, CloudCommandReceipt, CloudCommandRequest, CloudCommandState,
    CLOUD_MAX_ERROR_CHARS, CLOUD_MAX_RECEIPT_RESULT_CHARS,
};
use serde_json::{json, Value};

use crate::cloud_guest::now_iso;
use crate::journal::{append_record, rewrite_records, Finalize};

/// Rewrite the journal atomically once this many records have accumulated
/// (TS `COMPACT_AFTER_RECORDS`).
pub const COMPACT_AFTER_RECORDS: usize = 4096;

/// What `admit` found for one command id (TS `CloudAdmitStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestAdmission {
    /// This call is the first durable admission of the command.
    New,
    /// The command id and digest match a journaled admission: replay the
    /// stored receipt, never re-execute.
    Duplicate,
    /// The command id names a different request: refuse, never re-admit.
    Conflict,
}

/// One admitted command's durable state.
struct JournalEntry {
    digest: String,
    request: String,
    submitted_at: String,
    updated_at: String,
    state: CloudCommandState,
    uncertain: bool,
    error: Option<String>,
    result: Option<String>,
}

/// A claimed command: the receipt plus the canonical request (TS
/// `CloudClaimedCommand`).
pub struct ClaimedCommand {
    pub receipt: CloudCommandReceipt,
    pub request: String,
}

/// The durable append-only command journal for one guest session.
pub struct GuestCommandJournal {
    path: PathBuf,
    entries: HashMap<CloudCommandId, JournalEntry>,
    order: Vec<CloudCommandId>,
    record_count: usize,
    compact_after_records: usize,
}

impl GuestCommandJournal {
    /// Open (or create) the journal at `path`, replaying admitted
    /// commands and their transitions. A crash-truncated or malformed
    /// tail is skipped, like the recovery journals; a command restored
    /// `running` without a terminal record becomes uncertain.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent directory cannot be created.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let mut journal = Self {
            path: path.to_path_buf(),
            entries: HashMap::new(),
            order: Vec::new(),
            record_count: 0,
            compact_after_records: COMPACT_AFTER_RECORDS,
        };
        journal.load()?;
        Ok(journal)
    }

    /// Admit a command. The admit record is fsynced before the receipt
    /// is returned, so a duplicate or conflict response always reflects
    /// durable state.
    ///
    /// # Errors
    ///
    /// Returns the TS problem string when the id or the request is
    /// invalid, or an error when the durable append fails.
    pub fn admit(
        &mut self,
        command_id: &str,
        request: &Value,
    ) -> Result<(GuestAdmission, CloudCommandReceipt)> {
        if let Some(problem) = cloud_id_problem(Some(&json!(command_id)), "commandId") {
            return Err(anyhow!("{problem}"));
        }
        if let Some(problem) = cloud_request_problem(request) {
            return Err(anyhow!("invalid command request: {problem}"));
        }
        let request_json = canonical_json(request).map_err(|reason| anyhow!("{reason}"))?;
        let digest = cloud_request_digest(request)
            .map_err(|reason| anyhow!("request is not canonical JSON: {reason}"))?;
        if let Some(existing) = self.entries.get(command_id) {
            let admission = if existing.digest == digest {
                GuestAdmission::Duplicate
            } else {
                GuestAdmission::Conflict
            };
            return Ok((admission, receipt_of(command_id, existing)));
        }
        let recorded_at = now_iso();
        append_record(
            &self.path,
            &json!({
                "version": 1,
                "type": "admit",
                "commandId": command_id,
                "digest": digest,
                "request": request_json,
                "recordedAt": recorded_at,
            }),
        )?;
        let entry = JournalEntry {
            digest,
            request: request_json,
            submitted_at: recorded_at.clone(),
            updated_at: recorded_at,
            state: CloudCommandState::Accepted,
            uncertain: false,
            error: None,
            result: None,
        };
        self.record_count += 1;
        self.entries.insert(command_id.to_string(), entry);
        self.order.push(command_id.to_string());
        self.maybe_compact()?;
        let entry = self
            .entries
            .get(command_id)
            .ok_or_else(|| anyhow!("journal entry vanished after admit"))?;
        Ok((GuestAdmission::New, receipt_of(command_id, entry)))
    }

    /// Claim the oldest dispatchable command; the running transition is
    /// fsynced before the request is handed out. Uncertain commands are
    /// never claimed.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable running transition fails.
    pub fn claim_next_pending(&mut self) -> Result<Option<ClaimedCommand>> {
        let claimable = self
            .order
            .iter()
            .filter(|command_id| {
                self.entries.get(*command_id).is_some_and(|entry| {
                    entry.state == CloudCommandState::Accepted && !entry.uncertain
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let Some(command_id) = claimable.into_iter().next() else {
            return Ok(None);
        };
        self.transition(&command_id, CloudCommandState::Running, None, None)?;
        let entry = self
            .entries
            .get(&command_id)
            .ok_or_else(|| anyhow!("journal entry vanished after claim"))?;
        Ok(Some(ClaimedCommand {
            receipt: receipt_of(&command_id, entry),
            request: entry.request.clone(),
        }))
    }

    /// Settle the claimed command as completed, optionally carrying the
    /// terminal result payload (TS `complete`).
    ///
    /// # Errors
    ///
    /// Returns the TS error when the command is unknown, already
    /// terminal, or the result payload is out of bounds, or an error when
    /// the durable transition fails.
    pub fn complete(&mut self, command_id: &str, result: Option<&str>) -> Result<()> {
        if let Some(result) = result {
            if result.is_empty() || result.chars().count() > CLOUD_MAX_RECEIPT_RESULT_CHARS {
                return Err(anyhow!(
                    "result must be a string of at most {CLOUD_MAX_RECEIPT_RESULT_CHARS} characters"
                ));
            }
        }
        self.transition(command_id, CloudCommandState::Completed, None, result)
    }

    /// Settle the claimed command as failed (TS `fail`).
    ///
    /// # Errors
    ///
    /// Returns the TS error when the command is unknown, already
    /// terminal, or the error string is out of bounds, or an error when
    /// the durable transition fails.
    pub fn fail(&mut self, command_id: &str, error: Option<&str>) -> Result<()> {
        if let Some(error) = error {
            if error.is_empty() || error.chars().count() > CLOUD_MAX_ERROR_CHARS {
                return Err(anyhow!(
                    "failure error must be a string of at most {CLOUD_MAX_ERROR_CHARS} characters"
                ));
            }
        }
        self.transition(command_id, CloudCommandState::Failed, error, None)
    }

    /// Settle the claimed command as cancelled (TS `cancel`).
    ///
    /// # Errors
    ///
    /// Returns the TS error when the command is unknown or already
    /// terminal, or an error when the durable transition fails.
    pub fn cancel(&mut self, command_id: &str) -> Result<()> {
        self.transition(command_id, CloudCommandState::Cancelled, None, None)
    }

    /// Host assertion that an uncertain command never started: makes it
    /// dispatchable again (TS `requeue`).
    ///
    /// # Errors
    ///
    /// Returns the TS error when the command is unknown or not uncertain,
    /// or an error when the durable transition fails.
    pub fn requeue(&mut self, command_id: &str) -> Result<()> {
        let Some(entry) = self.entries.get(command_id) else {
            return Err(anyhow!("unknown command: {command_id}"));
        };
        if !entry.uncertain {
            return Err(anyhow!("command {command_id} is not uncertain"));
        }
        self.transition(command_id, CloudCommandState::Accepted, None, None)
    }

    /// The durable receipt of one command.
    #[must_use]
    pub fn receipt(&self, command_id: &str) -> Option<CloudCommandReceipt> {
        self.entries
            .get(command_id)
            .map(|entry| receipt_of(command_id, entry))
    }

    /// Receipts of commands restored without a terminal record, awaiting
    /// host reconciliation.
    #[must_use]
    pub fn list_uncertain(&self) -> Vec<CloudCommandReceipt> {
        self.order
            .iter()
            .filter_map(|command_id| {
                self.entries
                    .get(command_id)
                    .filter(|entry| entry.uncertain)
                    .map(|entry| receipt_of(command_id, entry))
            })
            .collect()
    }

    /// Receipts of accepted commands the dispatcher may claim, oldest
    /// first.
    #[must_use]
    pub fn list_pending(&self) -> Vec<CloudCommandReceipt> {
        self.order
            .iter()
            .filter_map(|command_id| {
                self.entries
                    .get(command_id)
                    .filter(|entry| entry.state == CloudCommandState::Accepted && !entry.uncertain)
                    .map(|entry| receipt_of(command_id, entry))
            })
            .collect()
    }

    /// Apply one transition: guards first, then the fsynced append, then
    /// the in-memory fold (TS `transitionTo`).
    fn transition(
        &mut self,
        command_id: &str,
        next: CloudCommandState,
        error: Option<&str>,
        result: Option<&str>,
    ) -> Result<()> {
        let Some(entry) = self.entries.get_mut(command_id) else {
            return Err(anyhow!("unknown command: {command_id}"));
        };
        if entry.state.is_terminal() {
            return Err(anyhow!(
                "command {command_id} already reached terminal state {}",
                state_str(entry.state)
            ));
        }
        if next == CloudCommandState::Running && entry.uncertain {
            return Err(anyhow!(
                "command {command_id} is uncertain after restore; requeue or settle it first"
            ));
        }
        if next == CloudCommandState::Running && entry.state == CloudCommandState::Running {
            return Ok(());
        }
        let recorded_at = now_iso();
        let mut record = json!({
            "version": 1,
            "type": "transition",
            "commandId": command_id,
            "state": state_str(next),
            "recordedAt": recorded_at,
        });
        if let Some(error) = error {
            record["error"] = json!(error);
        }
        if let Some(result) = result {
            record["result"] = json!(result);
        }
        append_record(&self.path, &record)?;
        entry.state = next;
        entry.updated_at = recorded_at;
        entry.error = error.map(str::to_string);
        entry.result = result.map(str::to_string);
        // Both requeue (accepted) and terminal transitions settle
        // uncertainty.
        entry.uncertain = false;
        self.record_count += 1;
        self.maybe_compact()
    }

    /// Load and fold the durable records (TS `load`/`foldRecord`).
    /// Reads bytes, never lossy strings: only a torn FINAL line (a
    /// crash mid-append; the appends always end in a newline) may be
    /// skipped — any other unreadable file or any non-final record
    /// that fails to parse is corruption the guest must refuse to serve
    /// under, because silently folding it away could re-admit a
    /// completed command as new and run it twice.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be read (anything but
    /// a missing file) or a complete record is corrupt.
    fn load(&mut self) -> Result<()> {
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(anyhow!("read {}: {error}", self.path.display()));
            }
        };
        let content = String::from_utf8_lossy(&bytes);
        let lines: Vec<&str> = content.split('\n').collect();
        let torn_tail = !bytes.ends_with(b"\n");
        let complete = if torn_tail {
            // A crash may leave only the final append truncated: drop
            // that one line; every earlier record stays authoritative.
            lines[..lines.len() - 1].to_vec()
        } else {
            lines
        };
        for line in complete {
            if line.is_empty() {
                continue;
            }
            let record = serde_json::from_str::<Value>(line)
                .map_err(|_| anyhow!("command journal record is corrupt"))?;
            self.fold_record(&record);
        }
        // A claim fsyncs the running transition before the request
        // leaves the journal, so an accepted command on disk was never
        // dispatched: it restores as pending. A running command may have
        // started executing; it is uncertain and must never be replayed
        // automatically.
        for entry in self.entries.values_mut() {
            if entry.state == CloudCommandState::Running {
                entry.uncertain = true;
            }
        }
        Ok(())
    }

    fn fold_record(&mut self, record: &Value) {
        if record.get("version").and_then(Value::as_u64) != Some(1) {
            return;
        }
        self.record_count += 1;
        let kind = record.get("type").and_then(Value::as_str);
        if kind == Some("admit") {
            self.fold_admit(record);
        } else if kind == Some("transition") {
            self.fold_transition(record);
        }
    }

    fn fold_admit(&mut self, record: &Value) {
        let Some(command_id) = record.get("commandId").and_then(Value::as_str) else {
            return;
        };
        if cloud_id_problem(Some(&json!(command_id)), "commandId").is_some() {
            return;
        }
        let Some(digest) = record.get("digest").and_then(Value::as_str) else {
            return;
        };
        if !is_cloud_digest(digest) {
            return;
        }
        let Some(request) = record.get("request").and_then(Value::as_str) else {
            return;
        };
        let Some(recorded_at) = record.get("recordedAt").and_then(Value::as_str) else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<Value>(request) else {
            return;
        };
        if cloud_request_problem(&parsed).is_some() {
            return;
        }
        // The stored digest must match the stored request; a mismatched
        // pair is corruption.
        let Ok(canonical) = canonical_json(&parsed) else {
            return;
        };
        if cloud_digest(&canonical) != digest {
            return;
        }
        // Admits are create-only; a repeated admit line (hand-edited
        // journal) keeps the first.
        if self.entries.contains_key(command_id) {
            return;
        }
        let entry = JournalEntry {
            digest: digest.to_string(),
            request: canonical,
            submitted_at: recorded_at.to_string(),
            updated_at: recorded_at.to_string(),
            state: CloudCommandState::Accepted,
            uncertain: false,
            error: None,
            result: None,
        };
        self.entries.insert(command_id.to_string(), entry);
        self.order.push(command_id.to_string());
    }

    fn fold_transition(&mut self, record: &Value) {
        let Some(command_id) = record.get("commandId").and_then(Value::as_str) else {
            return;
        };
        let Some(state) = record
            .get("state")
            .and_then(Value::as_str)
            .and_then(parse_state)
        else {
            return;
        };
        let Some(recorded_at) = record.get("recordedAt").and_then(Value::as_str) else {
            return;
        };
        let error = record.get("error").and_then(Value::as_str);
        if let Some(error) = error {
            if error.is_empty() || error.chars().count() > CLOUD_MAX_ERROR_CHARS {
                return;
            }
        }
        // A transition without an admit line cannot be trusted; drop it.
        let Some(entry) = self.entries.get_mut(command_id) else {
            return;
        };
        let result = record.get("result").and_then(Value::as_str);
        if let Some(result) = result {
            if result.is_empty() || result.chars().count() > CLOUD_MAX_RECEIPT_RESULT_CHARS {
                return;
            }
        }
        entry.state = state;
        entry.updated_at = recorded_at.to_string();
        entry.uncertain = record.get("uncertain").and_then(Value::as_bool) == Some(true);
        entry.error = error.map(str::to_string);
        entry.result = result.map(str::to_string);
    }

    /// Compaction must observe the caller's completed state mutation, so
    /// it never runs inside the append (TS `maybeCompact`).
    fn maybe_compact(&mut self) -> Result<()> {
        if self.record_count < self.compact_after_records {
            return Ok(());
        }
        let mut records = Vec::new();
        for command_id in &self.order {
            let Some(entry) = self.entries.get(command_id) else {
                continue;
            };
            records.push(json!({
                "version": 1,
                "type": "admit",
                "commandId": command_id,
                "digest": entry.digest,
                "request": entry.request,
                "recordedAt": entry.submitted_at,
            }));
            if entry.state != CloudCommandState::Accepted
                || entry.uncertain
                || entry.error.is_some()
                || entry.result.is_some()
            {
                let mut record = json!({
                    "version": 1,
                    "type": "transition",
                    "commandId": command_id,
                    "state": state_str(entry.state),
                    "recordedAt": entry.updated_at,
                });
                if entry.uncertain {
                    record["uncertain"] = json!(true);
                }
                if let Some(error) = &entry.error {
                    record["error"] = json!(error);
                }
                if let Some(result) = &entry.result {
                    record["result"] = json!(result);
                }
                records.push(record);
            }
        }
        rewrite_records(&self.path, &records, Finalize::RetryBusy)?;
        self.record_count = records.len();
        Ok(())
    }
}

impl GuestCommandJournal {
    /// The folded record count (the compaction window's test seam).
    #[cfg(test)]
    #[must_use]
    pub(crate) fn record_count_for_tests(&self) -> usize {
        self.record_count
    }

    /// Shrink the compaction window so the rewrite path runs in-test.
    #[cfg(test)]
    pub(crate) fn compact_after_records_for_tests(&mut self, window: usize) {
        self.compact_after_records = window;
    }
}

fn receipt_of(command_id: &str, entry: &JournalEntry) -> CloudCommandReceipt {
    CloudCommandReceipt {
        command_id: command_id.to_string(),
        digest: entry.digest.clone(),
        state: entry.state,
        submitted_at: entry.submitted_at.clone(),
        updated_at: entry.updated_at.clone(),
        uncertain: entry.uncertain,
        error: entry.error.clone(),
        result: entry.result.clone(),
    }
}

fn state_str(state: CloudCommandState) -> &'static str {
    match state {
        CloudCommandState::Accepted => "accepted",
        CloudCommandState::Running => "running",
        CloudCommandState::Completed => "completed",
        CloudCommandState::Failed => "failed",
        CloudCommandState::Cancelled => "cancelled",
    }
}

fn parse_state(state: &str) -> Option<CloudCommandState> {
    match state {
        "accepted" => Some(CloudCommandState::Accepted),
        "running" => Some(CloudCommandState::Running),
        "completed" => Some(CloudCommandState::Completed),
        "failed" => Some(CloudCommandState::Failed),
        "cancelled" => Some(CloudCommandState::Cancelled),
        _ => None,
    }
}

/// The typed request of a claimed command, parsed back from the
/// canonical journal record.
///
/// # Errors
///
/// Returns an error when the stored canonical request no longer parses
/// as a valid command request.
pub fn parse_claimed_request(canonical: &str) -> Result<CloudCommandRequest> {
    let value: Value = serde_json::from_str(canonical).map_err(|error| anyhow!("{error}"))?;
    if let Some(problem) = cloud_request_problem(&value) {
        return Err(anyhow!("invalid command request: {problem}"));
    }
    serde_json::from_value(value).map_err(|error| anyhow!("{error}"))
}

#[cfg(test)]
mod tests;
