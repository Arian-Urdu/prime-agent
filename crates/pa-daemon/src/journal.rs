//! Append-only recovery journals (ports of command-recovery-journal.ts and
//! worker-recovery-journal.ts).
//!
//! The command journal makes supervisor mutations exactly-once: a received
//! record is durable before dispatch, a missing result after a crash is
//! reported as uncertain and never replayed. The worker journal records the
//! latest busy/operation state per session so a replacement can mark
//! interrupted work instead of guessing.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

const COMPACT_AFTER_RECORDS: usize = 4096;

pub(crate) fn append_record(path: &Path, record: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open journal {}", path.display()))?;
    let mut line = serde_json::to_string(record)?;
    line.push('\n');
    file.write_all(line.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Append several records as ONE durable write: one open, all lines in one
/// `write_all`, one `fsync`. The records land together or not at all — a
/// batched checkpoint keeps its all-or-nothing shape (the busy verdict
/// never publishes without the queue snapshot it describes), and the
/// journal's on-disk bytes are exactly what the same records appended one
/// by one would produce.
///
/// # Errors
///
/// Returns an error when the parent directory, the open, a serialization,
/// the write, or the sync fails; a partial write may leave truncated
/// trailing lines, which the loader skips like any crash-truncated record.
pub(crate) fn append_records(path: &Path, records: &[Value]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open journal {}", path.display()))?;
    let mut lines = Vec::new();
    for record in records {
        serde_json::to_writer(&mut lines, record)?;
        lines.push(b'\n');
    }
    file.write_all(&lines)?;
    file.sync_all()?;
    Ok(())
}

/// How the temp journal lands on its path, and whether its data rides a
/// full sync before the swap: the two are one seam — each variant is the
/// sync class its TS counterpart (or Rust-native owner) carries.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Finalize {
    /// Rename through `rename_onto`: the bounded win32 destination-busy
    /// retry (TS `writeFileAtomicSync` -> `renameOntoSync`), with the
    /// temp file synced before the swap (TS `fsync: true`).
    RetryBusy,
    /// Bare rename with the temp file synced before the swap: every
    /// failure surfaces immediately. The Rust-native terminal-compaction
    /// journal (no TS counterpart) keeps its belt.
    Synced,
    /// Bare rename with an UNSYNCED temp (TS
    /// `worker-recovery-journal.ts` compact: `writeFileSync` + plain
    /// `renameSync` — no retry, no temp fsync): the OS carries the temp
    /// data to the rename. Durability is owned by the append path — the
    /// compacted form holds only records the append path already made
    /// durable, so a lost compact falls back to the append-only history,
    /// which replays identically.
    Bare,
}

pub(crate) fn rewrite_records(path: &Path, records: &[Value], finalize: Finalize) -> Result<()> {
    let temp = path.with_extension(format!("jsonl.tmp-{}", std::process::id()));
    {
        let file = File::create(&temp).with_context(|| format!("create {}", temp.display()))?;
        let mut writer = BufWriter::new(file);
        for record in records {
            let mut line = serde_json::to_string(record)?;
            line.push('\n');
            writer.write_all(line.as_bytes())?;
        }
        writer.flush()?;
        if !matches!(finalize, Finalize::Bare) {
            writer.get_ref().sync_all()?;
        }
    }
    let rename = match finalize {
        Finalize::RetryBusy => pa_core::platform::rename_onto(&temp, path),
        Finalize::Synced | Finalize::Bare => fs::rename(&temp, path),
    };
    rename.with_context(|| format!("persist {}", path.display()))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandJournalEntry {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Value>,
}

/// Port of `CommandRecoveryJournal`.
pub struct CommandRecoveryJournal {
    path: std::path::PathBuf,
    entries: HashMap<String, CommandJournalEntry>,
    record_count: usize,
}

impl CommandRecoveryJournal {
    /// Open the journal at `path` (creating the parent directory as needed)
    /// and load the pending receipts from any existing records.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent directory cannot be created; a
    /// missing journal loads as empty, and the record load itself never
    /// errors (lines truncated by a crash are skipped).
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut journal = CommandRecoveryJournal {
            path: path.to_path_buf(),
            entries: HashMap::new(),
            record_count: 0,
        };
        journal.load()?;
        Ok(journal)
    }

    fn key(client_id: &str, command_id: &str) -> String {
        serde_json::json!([client_id, command_id]).to_string()
    }

    #[must_use]
    pub fn lookup(&self, client_id: &str, command_id: &str) -> Option<CommandJournalEntry> {
        self.entries.get(&Self::key(client_id, command_id)).cloned()
    }

    /// Record durable receipt before dispatch. Returns the prior state when the
    /// command was already journaled.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt record cannot be appended (the
    /// parent directory, the journal open, the serialization, the write,
    /// or the sync fails).
    pub fn begin(
        &mut self,
        client_id: &str,
        command_id: &str,
        command_type: &str,
    ) -> Result<Option<CommandJournalEntry>> {
        if let Some(existing) = self.lookup(client_id, command_id) {
            return Ok(Some(existing));
        }
        let record = serde_json::json!({
            "version": 1,
            "type": "received",
            "key": Self::key(client_id, command_id),
            "clientId": client_id,
            "commandId": command_id,
            "commandType": command_type,
            "recordedAt": crate::util::now_iso(),
        });
        append_record(&self.path, &record)?;
        self.record_count += 1;
        self.entries.insert(
            Self::key(client_id, command_id),
            CommandJournalEntry {
                status: "pending".to_string(),
                response: None,
            },
        );
        Ok(None)
    }

    /// Record the settled command result; a later replay of the command
    /// answers from it.
    ///
    /// # Errors
    ///
    /// Returns an error when no receipt was journaled for the command (a
    /// result cannot be recorded first), when the result record cannot be
    /// appended, or when the post-append compaction fails.
    pub fn record_result(
        &mut self,
        client_id: &str,
        command_id: &str,
        response: &Value,
    ) -> Result<()> {
        let key = Self::key(client_id, command_id);
        if !self.entries.contains_key(&key) {
            return Err(anyhow::anyhow!(
                "Cannot record a result before command receipt: {key}"
            ));
        }
        let record = serde_json::json!({
            "version": 1,
            "type": "result",
            "key": key,
            "response": response,
            "recordedAt": crate::util::now_iso(),
        });
        append_record(&self.path, &record)?;
        self.record_count += 1;
        self.entries.insert(
            key,
            CommandJournalEntry {
                status: "complete".to_string(),
                response: Some(response.clone()),
            },
        );
        if self.record_count >= COMPACT_AFTER_RECORDS {
            self.compact()?;
        }
        Ok(())
    }

    /// Acknowledge the command: the durable receipt is no longer needed.
    /// Acknowledging an unknown command is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error when the acknowledgment record cannot be appended
    /// or the post-acknowledge compaction fails.
    pub fn acknowledge(&mut self, client_id: &str, command_id: &str) -> Result<()> {
        let key = Self::key(client_id, command_id);
        if !self.entries.contains_key(&key) {
            return Ok(());
        }
        let record = serde_json::json!({
            "version": 1,
            "type": "acknowledged",
            "key": key,
            "recordedAt": crate::util::now_iso(),
        });
        append_record(&self.path, &record)?;
        self.entries.remove(&key);
        if self.entries.is_empty() || self.record_count >= COMPACT_AFTER_RECORDS {
            self.compact()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let mut records = Vec::new();
        for (key, entry) in &self.entries {
            let mut received = serde_json::json!({
                "version": 1,
                "type": "received",
                "key": key,
            });
            if let Some(response) = &entry.response {
                received["response"] = response.clone();
            }
            records.push(received);
        }
        rewrite_records(&self.path, &records, Finalize::RetryBusy)?;
        self.record_count = records.len();
        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        let Ok(content) = fs::read_to_string(&self.path) else {
            return Ok(());
        };
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            let Ok(record) = serde_json::from_str::<Value>(line) else {
                // A crash may leave only the final append truncated.
                continue;
            };
            if record.get("version").and_then(Value::as_u64) != Some(1) {
                continue;
            }
            self.record_count += 1;
            let key = record
                .get("key")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            match record.get("type").and_then(Value::as_str) {
                Some("received") => {
                    self.entries.insert(
                        key,
                        CommandJournalEntry {
                            status: "pending".to_string(),
                            response: None,
                        },
                    );
                }
                Some("acknowledged") => {
                    self.entries.remove(&key);
                }
                Some("result") => {
                    if let Some(entry) = self.entries.get_mut(&key) {
                        entry.status = "complete".to_string();
                        entry.response = record.get("response").cloned();
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerRecoveryRecord {
    pub active_session_id: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_file: Option<String>,
    pub busy: bool,
    pub operation: String,
    pub recorded_at: String,
}

/// One checkpoint transaction record (the cloud-keyed delivery path's
/// durable commit unit): the queue snapshot, the optional busy verdict,
/// and the optional request-id admission riding ONE NDJSON line, sealed
/// with a digest over their canonical JSON. A torn or partial write
/// leaves one unparsable line the scan drops all-or-nothing; a
/// complete-but-corrupted line fails the digest and drops the same way.
/// One line is the whole transaction — a crash can never leave the
/// message visible without its request-id admission, or the admission
/// without the queue row that made it visible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCheckpointTransactionRecord {
    pub version: u32,
    pub r#type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<WorkerRecoveryRecord>,
    pub snapshot: WorkerQueueSnapshotRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_admission: Option<CloudInboxAdmissionRecord>,
    pub digest: String,
}

/// The record-type tag of a checkpoint transaction line.
const CHECKPOINT_TRANSACTION_RECORD_TYPE: &str = "queue_checkpoint_transaction";
/// The checkpoint-transaction record version.
const CHECKPOINT_TRANSACTION_VERSION: u32 = 1;

/// The transaction digest: sha256 over the canonical JSON of the
/// carried records, so a corrupted-but-parseable line drops instead of
/// replaying half a transaction.
///
/// # Errors
///
/// Returns an error when the records cannot be canonicalized.
fn checkpoint_transaction_digest(
    verdict: &Option<WorkerRecoveryRecord>,
    snapshot: &WorkerQueueSnapshotRecord,
    cloud_admission: &Option<CloudInboxAdmissionRecord>,
) -> Result<String> {
    let payload = serde_json::json!({
        "verdict": verdict,
        "snapshot": snapshot,
        "cloudAdmission": cloud_admission,
    });
    let canonical = pa_types::daemon::cloud::canonical_json(&payload)
        .map_err(|reason| anyhow::anyhow!("canonical JSON: {reason}"))?;
    Ok(Sha256::digest(canonical.as_bytes())
        .iter()
        .fold(String::new(), |mut key, byte| {
            use std::fmt::Write;
            write!(key, "{byte:02x}").expect("write to String");
            key
        }))
}

/// How the journal scan classified the file's unparsable lines: a torn
/// trailing append (repairable by truncation) or mid-file corruption
/// (fail closed — history is never silently dropped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JournalCorruption {
    /// Every line parses (or the journal is absent).
    Clean,
    /// The unparsable lines form a contiguous run at the end of the
    /// file — the crash-torn tail of one interrupted append. The
    /// repair drops exactly that run and keeps every valid line.
    TornTail,
    /// An unparsable line sits before a valid one: not an interrupted
    /// append but real corruption. The journal fails closed.
    MidFile,
}

/// One classified journal line: a valid record (legacy or
/// transaction-carried) or unparsable.
#[allow(clippy::large_enum_variant)]
enum ScannedLine {
    /// A legacy busy/operation verdict record.
    Verdict(WorkerRecoveryRecord),
    /// A legacy queue-snapshot record.
    Snapshot(WorkerQueueSnapshotRecord),
    /// A legacy cloud inbox admission record.
    Admission(CloudInboxAdmissionRecord),
    /// One checkpoint transaction: its carried records, digest verified.
    Transaction {
        verdict: Option<WorkerRecoveryRecord>,
        snapshot: WorkerQueueSnapshotRecord,
        admission: Option<CloudInboxAdmissionRecord>,
    },
    /// A valid-JSON line of an unknown record type (a future
    /// subsystem's records): skipped, not corruption.
    UnknownType,
    /// An unparsable line: torn, glued, or corrupted.
    Malformed,
}

/// The unified ordered journal scan (the one parser every reader goes
/// through): classifies each line once, decomposes checkpoint
/// transactions into the same structures the legacy lines feed, and
/// reports the corruption verdict. The valid original line strings are
/// retained so the torn-tail repair can rewrite the file byte-for-byte
/// without them.
struct JournalScan {
    lines: Vec<ScannedLine>,
    /// The original text of every line that classified as a known valid
    /// record or an unknown type (preserved for the repair rewrite).
    valid_line_text: Vec<String>,
    corruption: JournalCorruption,
}

fn scan_worker_journal(path: &Path) -> Result<JournalScan> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(JournalScan {
                lines: Vec::new(),
                valid_line_text: Vec::new(),
                corruption: JournalCorruption::Clean,
            });
        }
        Err(error) => {
            return Err(anyhow::anyhow!(
                "read worker journal {}: {error}",
                path.display()
            ))
        }
    };
    let mut lines = Vec::new();
    let mut valid_line_text = Vec::new();
    let mut malformed_seen = false;
    let mut corruption = JournalCorruption::Clean;
    for line in contents.split('\n') {
        if line.is_empty() {
            // The trailing newline (or padding): not a record, not
            // corruption.
            continue;
        }
        let classified = classify_journal_line(line);
        let is_valid = !matches!(classified, ScannedLine::Malformed);
        if !is_valid {
            if malformed_seen {
                // A second unparsable line after a valid one: mid-file
                // corruption (an interrupted append tears only the LAST
                // line, never two with a valid line between them).
                corruption = JournalCorruption::MidFile;
            } else {
                malformed_seen = true;
            }
        } else if malformed_seen {
            // A valid line after an unparsable one: the unparsable line
            // was not the torn tail of an interrupted append.
            corruption = JournalCorruption::MidFile;
            malformed_seen = false;
        } else {
            valid_line_text.push(line.to_string());
        }
        lines.push(classified);
    }
    if corruption == JournalCorruption::Clean && malformed_seen {
        corruption = JournalCorruption::TornTail;
    }
    Ok(JournalScan {
        lines,
        valid_line_text,
        corruption,
    })
}

/// Classify one journal line. A `queue_checkpoint_transaction` line is
/// verified against its digest; a digest mismatch reads as malformed
/// (a corrupted transaction replays as nothing, never as half of one).
/// The dispatch is by the line's JSON `type` tag first, so a record of
/// one type carrying another's field names can never misparse as a
/// different record (and the version-1 snapshot's bare-string lanes
/// keep their legacy tolerance, exactly like the old single-purpose
/// parser).
fn classify_journal_line(line: &str) -> ScannedLine {
    let Ok(record) = serde_json::from_str::<Value>(line) else {
        return ScannedLine::Malformed;
    };
    let Some(record_type) = record.get("type").and_then(Value::as_str) else {
        // Valid JSON without a type tag: the legacy busy/operation
        // verdict record is a bare field shape — anything else is a
        // foreign record the journal does not own.
        return match serde_json::from_value::<WorkerRecoveryRecord>(record) {
            Ok(verdict) => ScannedLine::Verdict(verdict),
            Err(_) => ScannedLine::UnknownType,
        };
    };
    match record_type {
        CHECKPOINT_TRANSACTION_RECORD_TYPE => {
            let Ok(transaction) =
                serde_json::from_value::<WorkerCheckpointTransactionRecord>(record)
            else {
                return ScannedLine::Malformed;
            };
            if transaction.version != CHECKPOINT_TRANSACTION_VERSION
                || checkpoint_transaction_digest(
                    &transaction.verdict,
                    &transaction.snapshot,
                    &transaction.cloud_admission,
                )
                .is_ok_and(|digest| digest == transaction.digest)
            {
                return ScannedLine::Transaction {
                    verdict: transaction.verdict,
                    snapshot: transaction.snapshot,
                    admission: transaction.cloud_admission,
                };
            }
            // A transaction line that fails its own seal replays as
            // nothing: half a transaction is never a transaction.
            ScannedLine::Malformed
        }
        QUEUE_SNAPSHOT_RECORD_TYPE => {
            let version = record.get("version").and_then(Value::as_u64);
            if version != Some(1) && version != Some(u64::from(QUEUE_SNAPSHOT_VERSION)) {
                return ScannedLine::UnknownType;
            }
            let Some(active_session_id) = record.get("active_session_id").and_then(Value::as_str)
            else {
                return ScannedLine::UnknownType;
            };
            ScannedLine::Snapshot(WorkerQueueSnapshotRecord {
                version: QUEUE_SNAPSHOT_VERSION,
                r#type: QUEUE_SNAPSHOT_RECORD_TYPE.to_string(),
                active_session_id: active_session_id.to_string(),
                steering: parse_snapshot_lane(record.get("steering")),
                follow_up: parse_snapshot_lane(record.get("follow_up")),
                recorded_at: record
                    .get("recorded_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
        }
        CLOUD_INBOX_RECORD_TYPE => {
            match serde_json::from_value::<CloudInboxAdmissionRecord>(record) {
                Ok(admission) if admission.version == CLOUD_INBOX_VERSION => {
                    ScannedLine::Admission(admission)
                }
                _ => ScannedLine::Malformed,
            }
        }
        // Any other type tag: a foreign record the journal does not
        // own — skipped, not corruption.
        _ => ScannedLine::UnknownType,
    }
}

/// The fold of one journal scan: the per-session busy verdicts, the
/// per-session queue snapshots, and the request-id-keyed cloud inbox
/// with its retention-window order.
struct FoldedJournal {
    latest: HashMap<String, WorkerRecoveryRecord>,
    queue_snapshots: HashMap<String, WorkerQueueSnapshotRecord>,
    cloud_inbox: HashMap<String, Value>,
    cloud_inbox_order: std::collections::VecDeque<String>,
}

/// Fold the scanned lines into the journal's in-memory structures, in
/// file order (the cloud inbox order needs it for the retention window).
fn fold_journal_scan(scan: &JournalScan) -> FoldedJournal {
    let mut latest = HashMap::new();
    let mut queue_snapshots = HashMap::new();
    let mut cloud_inbox = HashMap::new();
    let mut cloud_inbox_order = std::collections::VecDeque::new();
    for line in &scan.lines {
        match line {
            ScannedLine::Verdict(record) => {
                latest.insert(record.active_session_id.clone(), record.clone());
            }
            ScannedLine::Snapshot(record) => {
                queue_snapshots.insert(record.active_session_id.clone(), record.clone());
            }
            ScannedLine::Admission(record) => {
                cloud_inbox_order.push_back(record.request_id.clone());
                cloud_inbox.insert(record.request_id.clone(), record.receipt.clone());
            }
            ScannedLine::Transaction {
                verdict,
                snapshot,
                admission,
            } => {
                if let Some(verdict) = verdict {
                    latest.insert(verdict.active_session_id.clone(), verdict.clone());
                }
                queue_snapshots.insert(snapshot.active_session_id.clone(), snapshot.clone());
                if let Some(admission) = admission {
                    cloud_inbox_order.push_back(admission.request_id.clone());
                    cloud_inbox.insert(admission.request_id.clone(), admission.receipt.clone());
                }
            }
            ScannedLine::UnknownType | ScannedLine::Malformed => {}
        }
    }
    while cloud_inbox_order.len() > CLOUD_INBOX_WINDOW {
        if let Some(oldest) = cloud_inbox_order.pop_front() {
            cloud_inbox.remove(&oldest);
        }
    }
    FoldedJournal {
        latest,
        queue_snapshots,
        cloud_inbox,
        cloud_inbox_order,
    }
}

/// One parked queue row in a worker queue snapshot: the delivery payload a
/// respawned worker needs — the message text, the labeled preview, the
/// injected custom row, the queue key, and the visibility flag — so a
/// restored queued heartbeat still delivers as the `heartbeat_prompt`
/// component (and keeps its `Heartbeat prompt:` row) instead of
/// collapsing into a plain user message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerQueueItemRecord {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<crate::worker::QueuePriority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_message: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_key: Option<String>,
    #[serde(default = "queue_visible_default")]
    pub queue_visible: bool,
    /// The item's turn-execution class ("queued"/"injected"/"direct", see
    /// `worker::TurnPolicy)`: the batch gathering's compatibility gate. A
    /// record written before the field existed restores as "queued" — the
    /// dominant lane class, and the only one a fresh snapshot can batch.
    #[serde(default = "queue_policy_default")]
    pub policy: String,
}

fn queue_visible_default() -> bool {
    true
}

fn queue_policy_default() -> String {
    "queued".to_string()
}

impl WorkerQueueItemRecord {
    /// The record's turn-execution class; an unknown value restores as
    /// the dominant "queued" class.
    pub(crate) fn policy(&self) -> crate::worker::TurnPolicy {
        match self.policy.as_str() {
            "injected" => crate::worker::TurnPolicy::Injected,
            "direct" => crate::worker::TurnPolicy::Direct,
            _ => crate::worker::TurnPolicy::Queued,
        }
    }
}

/// A worker queue snapshot record: the pending steering/follow-up lanes so a
/// respawned worker restores its queues. Lives in the worker recovery journal
/// (TS keeps its session files free of daemon bookkeeping; queue recovery is
/// worker-private state, so it rides the journal next to the busy records).
/// Version 2 lanes carry the full item records; a version-1 lane (written
/// before the item payload existed) is a bare message-text array and
/// restores as a plain row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerQueueSnapshotRecord {
    pub version: u32,
    pub r#type: String,
    pub active_session_id: String,
    pub steering: Vec<WorkerQueueItemRecord>,
    pub follow_up: Vec<WorkerQueueItemRecord>,
    pub recorded_at: String,
}

/// One request-id-keyed cloud inbox admission: the receipt the receiver
/// answered when a cross-boundary agent message became visible in this
/// session's inbox, durably recorded in the SAME flush as the queue
/// snapshot that made it visible (a crash can never split "visible" from
/// "admitted", so an idempotent replay answers this receipt instead of
/// enqueueing a second visible message). The family exchange keys
/// deliveries by the guest request id; the dedupe window matches the
/// request outbox's record cap so every replayable request finds its
/// admission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudInboxAdmissionRecord {
    pub version: u32,
    pub r#type: String,
    pub request_id: String,
    pub receipt: Value,
    pub recorded_at: String,
}

/// The record-type tag of a cloud inbox admission line.
const CLOUD_INBOX_RECORD_TYPE: &str = "cloud_inbox_admission";
/// The cloud-inbox record version.
const CLOUD_INBOX_VERSION: u32 = 1;
/// The newest cloud inbox admissions retained across compaction, aligned
/// with the family request outbox's record cap (`DEFAULT_OUTBOX_RECORDS`)
/// so the largest replay span always finds its receiver admission.
const CLOUD_INBOX_WINDOW: usize = 50_000;

/// Port of `WorkerRecoveryJournal`: latest busy/operation per active session,
/// plus the latest queue snapshot per session, plus the request-id-keyed
/// cloud inbox admissions (the cross-boundary receiver dedupe).
pub struct WorkerRecoveryJournal {
    path: std::path::PathBuf,
    latest: HashMap<String, WorkerRecoveryRecord>,
    queue_snapshots: HashMap<String, WorkerQueueSnapshotRecord>,
    cloud_inbox: HashMap<String, Value>,
    cloud_inbox_order: std::collections::VecDeque<String>,
}

impl WorkerRecoveryJournal {
    /// Open the worker journal at `path` (creating the parent directory as
    /// needed) and load the latest busy records and queue snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent directory cannot be created, or
    /// when the journal exists but the queue-snapshot pass cannot read it
    /// (a missing journal loads as empty).
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let scan = scan_worker_journal(path)?;
        // The torn tail of an interrupted append is repaired BEFORE any
        // subsequent append can glue a valid record onto the unparsable
        // fragment (which would strand that record forever): the file is
        // rewritten with exactly its valid lines, byte-for-byte. Mid-file
        // corruption fails closed — the history is never silently
        // dropped.
        match scan.corruption {
            JournalCorruption::Clean => {}
            JournalCorruption::TornTail => {
                repair_journal_tail(path, &scan)?;
            }
            JournalCorruption::MidFile => {
                return Err(anyhow::anyhow!(
                    "worker journal {} is corrupted mid-file; refusing to rewrite history",
                    path.display()
                ));
            }
        }
        let folded = fold_journal_scan(&scan);
        Ok(WorkerRecoveryJournal {
            path: path.to_path_buf(),
            latest: folded.latest,
            queue_snapshots: folded.queue_snapshots,
            cloud_inbox: folded.cloud_inbox,
            cloud_inbox_order: folded.cloud_inbox_order,
        })
    }

    /// The receipt recorded when the cloud inbox admitted `request_id`
    /// (the idempotent duplicate answer), when one exists.
    #[must_use]
    pub fn cloud_inbox_receipt(&self, request_id: &str) -> Option<&Value> {
        self.cloud_inbox.get(request_id)
    }

    /// Read the latest worker record per active session straight from a
    /// journal file.
    ///
    /// # Errors
    ///
    /// Never errors: a missing or unreadable journal reads as an empty
    /// set (the `Result` wrapper keeps the reading seam uniform).
    pub fn read_latest(path: &Path) -> Result<Vec<WorkerRecoveryRecord>> {
        let scan = scan_worker_journal(path)?;
        if scan.corruption == JournalCorruption::MidFile {
            return Err(anyhow::anyhow!(
                "worker journal {} is corrupted mid-file",
                path.display()
            ));
        }
        Ok(fold_journal_scan(&scan).latest.into_values().collect())
    }

    /// Does the journal prove live work at the worker's last exit? A plain
    /// supervisor startup adopts a dead worker only when this holds (a
    /// restart must not mass-revive historical sessions): a latest `busy`
    /// record marks an in-flight turn or an admitted-but-undelivered
    /// prompt/queue lane. An unreadable journal proves nothing —
    /// uncertainty must not revive a session.
    #[must_use]
    pub fn read_interrupted(path: &Path) -> bool {
        Self::read_latest(path).is_ok_and(|records| records.iter().any(|record| record.busy))
    }

    /// The newest `busy` record's `recorded_at`, when the journal proves
    /// live work: the timestamp the boot-revival gate ages the evidence
    /// against (an old busy record is residue of an era that already
    /// ended, not interrupted work this boot must heal). A journal with
    /// no busy record answers `None`.
    #[must_use]
    pub fn latest_busy_recorded_at(path: &Path) -> Option<String> {
        Self::read_latest(path)
            .ok()?
            .iter()
            .filter(|record| record.busy)
            .map(|record| record.recorded_at.clone())
            .max()
    }

    /// Settle every busy session to idle with `operation` (the give-up
    /// belt): a supervisor that gave up on a worker records the verdict
    /// in the same journal a later boot would read as revival evidence —
    /// stale busy evidence must not outlive the give-up that superseded
    /// it, or every boot re-storms the slot the cap already condemned.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be opened or a settle
    /// record cannot be appended.
    pub fn settle_busy_records(path: &Path, operation: &str) -> Result<()> {
        let mut journal = Self::open(path)?;
        let busy: Vec<WorkerRecoveryRecord> = journal
            .get_latest()
            .into_iter()
            .filter(|record| record.busy)
            .collect();
        for record in busy {
            journal.record(
                &record.active_session_id,
                &record.session_id,
                record.session_file.as_deref(),
                false,
                operation,
            )?;
        }
        Ok(())
    }

    /// Record the latest busy/operation state for an active session; an
    /// unchanged record is skipped.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be serialized or appended,
    /// or when the all-idle compaction fails.
    pub fn record(
        &mut self,
        active_session_id: &str,
        session_id: &str,
        session_file: Option<&str>,
        busy: bool,
        operation: &str,
    ) -> Result<()> {
        if let Some(previous) = self.latest.get(active_session_id) {
            if previous.busy == busy
                && previous.operation == operation
                && previous.session_file.as_deref() == session_file
            {
                return Ok(());
            }
        }
        let record = WorkerRecoveryRecord {
            active_session_id: active_session_id.to_string(),
            session_id: session_id.to_string(),
            session_file: session_file.map(str::to_string),
            busy,
            operation: operation.to_string(),
            recorded_at: crate::util::now_iso(),
        };
        append_record(&self.path, &serde_json::to_value(&record)?)?;
        self.latest.insert(active_session_id.to_string(), record);
        // TS parity: the all-idle check includes the just-landed record
        // (TS `record` runs `[...this.latest.values()].every(!busy)`
        // AFTER `set`). Checking before the insert let the session's own
        // busy admission record block its settle's compaction, so a
        // single-session journal never compacted and grew append-only
        // for the session's lifetime; the compaction now fires at every
        // changed-idle record like TS, keeping the file bounded.
        if self.latest.values().all(|entry| !entry.busy) {
            self.compact()?;
        }
        Ok(())
    }

    #[must_use]
    pub fn get_latest(&self) -> Vec<WorkerRecoveryRecord> {
        self.latest.values().cloned().collect()
    }

    /// Persist the pending queue lanes; latest record wins per session.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot record cannot be serialized or
    /// appended.
    pub fn record_queue_snapshot(
        &mut self,
        active_session_id: &str,
        steering: &[WorkerQueueItemRecord],
        follow_up: &[WorkerQueueItemRecord],
    ) -> Result<()> {
        let record = WorkerQueueSnapshotRecord {
            version: QUEUE_SNAPSHOT_VERSION,
            r#type: QUEUE_SNAPSHOT_RECORD_TYPE.to_string(),
            active_session_id: active_session_id.to_string(),
            steering: steering.to_vec(),
            follow_up: follow_up.to_vec(),
            recorded_at: crate::util::now_iso(),
        };
        append_record(&self.path, &serde_json::to_value(&record)?)?;
        self.queue_snapshots
            .insert(active_session_id.to_string(), record);
        Ok(())
    }

    /// Record the queue snapshot and the busy/operation verdict in ONE
    /// durable append (the queue-checkpoint pair `checkpoint_queue_recovery`
    /// writes): the snapshot line and the verdict line share a single open,
    /// write, and `fsync`, so a checkpoint costs one journal flush instead
    /// of two. The on-disk order matches the sequential form exactly — the
    /// snapshot record first, then the verdict — and the verdict still
    /// never publishes over a snapshot that did not persist (the batch is
    /// all-or-nothing). An unchanged verdict appends the snapshot alone,
    /// like the sequential pair does.
    ///
    /// # Errors
    ///
    /// Returns an error when either record cannot be serialized or the
    /// batched append fails, or when the all-idle compaction fails after a
    /// changed verdict landed.
    #[allow(clippy::too_many_arguments)]
    pub fn record_queue_checkpoint(
        &mut self,
        active_session_id: &str,
        session_id: &str,
        session_file: Option<&str>,
        busy: bool,
        operation: &str,
        steering: &[WorkerQueueItemRecord],
        follow_up: &[WorkerQueueItemRecord],
        cloud_admission: Option<(&str, &Value)>,
    ) -> Result<()> {
        let snapshot = WorkerQueueSnapshotRecord {
            version: QUEUE_SNAPSHOT_VERSION,
            r#type: QUEUE_SNAPSHOT_RECORD_TYPE.to_string(),
            active_session_id: active_session_id.to_string(),
            steering: steering.to_vec(),
            follow_up: follow_up.to_vec(),
            recorded_at: crate::util::now_iso(),
        };
        let verdict_unchanged = self.latest.get(active_session_id).is_some_and(|previous| {
            previous.busy == busy
                && previous.operation == operation
                && previous.session_file.as_deref() == session_file
        });
        let record = if verdict_unchanged {
            None
        } else {
            Some(WorkerRecoveryRecord {
                active_session_id: active_session_id.to_string(),
                session_id: session_id.to_string(),
                session_file: session_file.map(str::to_string),
                busy,
                operation: operation.to_string(),
                recorded_at: crate::util::now_iso(),
            })
        };
        let admission = cloud_admission.map(|(request_id, receipt)| CloudInboxAdmissionRecord {
            version: CLOUD_INBOX_VERSION,
            r#type: CLOUD_INBOX_RECORD_TYPE.to_string(),
            request_id: request_id.to_string(),
            receipt: receipt.clone(),
            recorded_at: crate::util::now_iso(),
        });
        // The cloud admission rides ONE digest-sealed transaction line with
        // the queue snapshot and the busy verdict (the commit unit the
        // scan replays all-or-nothing): a crash can never leave the
        // message visible without its request-id admission (a duplicate
        // would re-deliver) or the admission without visibility (the
        // receipt would claim a message that never landed). The unkeyed
        // local path keeps its two-line batch (its pre-existing
        // verdict-ordering tolerance is unchanged).
        if admission.is_some() {
            let transaction = WorkerCheckpointTransactionRecord {
                version: CHECKPOINT_TRANSACTION_VERSION,
                r#type: CHECKPOINT_TRANSACTION_RECORD_TYPE.to_string(),
                verdict: record.clone(),
                snapshot: snapshot.clone(),
                cloud_admission: admission.clone(),
                digest: checkpoint_transaction_digest(&record, &snapshot, &admission)?,
            };
            append_record(&self.path, &serde_json::to_value(&transaction)?)?;
        } else {
            let mut batch = Vec::with_capacity(2);
            batch.push(serde_json::to_value(&snapshot)?);
            if let Some(record) = &record {
                batch.push(serde_json::to_value(record)?);
            }
            append_records(&self.path, &batch)?;
        }
        if let Some(admission) = admission {
            self.cloud_inbox_order
                .push_back(admission.request_id.clone());
            self.cloud_inbox
                .insert(admission.request_id, admission.receipt);
            while self.cloud_inbox_order.len() > CLOUD_INBOX_WINDOW {
                if let Some(oldest) = self.cloud_inbox_order.pop_front() {
                    self.cloud_inbox.remove(&oldest);
                }
            }
        }
        self.queue_snapshots
            .insert(active_session_id.to_string(), snapshot);
        if let Some(record) = record {
            self.latest.insert(active_session_id.to_string(), record);
            // TS parity (the same post-insert check as `record`): the
            // settle's compaction fires on the all-idle map that includes
            // the just-landed verdict, never blocked by the session's own
            // busy admission record.
            if self.latest.values().all(|entry| !entry.busy) {
                self.compact()?;
            }
        }
        Ok(())
    }

    /// The latest persisted queue rows for `active_session_id`.
    #[must_use]
    pub fn latest_queue_snapshot(
        &self,
        active_session_id: &str,
    ) -> Option<(Vec<WorkerQueueItemRecord>, Vec<WorkerQueueItemRecord>)> {
        self.queue_snapshots
            .get(active_session_id)
            .map(|record| (record.steering.clone(), record.follow_up.clone()))
    }

    /// Read the latest queue snapshot for a session straight from a journal
    /// file (worker restore on a fresh process).
    ///
    /// # Errors
    ///
    /// Returns an error when the journal exists but cannot be read (a
    /// missing journal answers `Ok(None)`).
    pub fn read_queue_snapshot(
        path: &Path,
        active_session_id: &str,
    ) -> Result<Option<(Vec<WorkerQueueItemRecord>, Vec<WorkerQueueItemRecord>)>> {
        let scan = scan_worker_journal(path)?;
        if scan.corruption == JournalCorruption::MidFile {
            return Err(anyhow::anyhow!(
                "worker journal {} is corrupted mid-file",
                path.display()
            ));
        }
        Ok(fold_journal_scan(&scan)
            .queue_snapshots
            .remove(active_session_id)
            .map(|record| (record.steering, record.follow_up)))
    }

    fn compact(&self) -> Result<()> {
        let mut records: Vec<Value> = self
            .latest
            .values()
            .map(serde_json::to_value)
            .collect::<std::result::Result<_, _>>()?;
        let snapshots: Vec<Value> = self
            .queue_snapshots
            .values()
            .map(serde_json::to_value)
            .collect::<std::result::Result<_, _>>()?;
        records.extend(snapshots);
        // The cloud inbox admissions survive compaction (their newest
        // window): a settled journal must never strand a delivered cloud
        // message's dedupe key, or a replayed request would re-deliver a
        // visible message.
        for request_id in &self.cloud_inbox_order {
            if let Some(receipt) = self.cloud_inbox.get(request_id) {
                let admission = CloudInboxAdmissionRecord {
                    version: CLOUD_INBOX_VERSION,
                    r#type: CLOUD_INBOX_RECORD_TYPE.to_string(),
                    request_id: request_id.clone(),
                    receipt: receipt.clone(),
                    recorded_at: crate::util::now_iso(),
                };
                records.push(serde_json::to_value(&admission)?);
            }
        }
        rewrite_records(&self.path, &records, Finalize::Bare)
    }
}

/// The record-type tag of a queue snapshot line.
const QUEUE_SNAPSHOT_RECORD_TYPE: &str = "queue_snapshot";
/// The current queue-snapshot record version: the lanes carry the full
/// item records.
const QUEUE_SNAPSHOT_VERSION: u32 = 2;

/// The torn-tail repair: rewrite the journal with exactly its valid
/// lines (the original bytes, in order), durably (temp file, fsync,
/// rename), so the next append writes onto a clean record boundary.
///
/// # Errors
/// Returns an error when the rewrite cannot be written or renamed.
fn repair_journal_tail(path: &Path, scan: &JournalScan) -> Result<()> {
    let mut records: Vec<Value> = Vec::with_capacity(scan.valid_line_text.len());
    for line in &scan.valid_line_text {
        // The valid lines are byte-for-byte JSON; re-parse each so the
        // rewrite path stays the one serialization.
        records.push(
            serde_json::from_str::<Value>(line)
                .map_err(|error| anyhow::anyhow!("repair parse: {error}"))?,
        );
    }
    rewrite_records(path, &records, Finalize::Synced)
}

/// One snapshot lane: a version-2 entry is the full item record, while a
/// version-1 entry is the bare message text and restores as a plain row
/// (no preview, no injected custom row — the pre-item payload).
fn parse_snapshot_lane(value: Option<&Value>) -> Vec<WorkerQueueItemRecord> {
    value
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| match entry {
                    Value::String(message) => Some(WorkerQueueItemRecord {
                        message: message.clone(),
                        priority: None,
                        preview: None,
                        custom_message: None,
                        queue_key: None,
                        queue_visible: true,
                        policy: queue_policy_default(),
                    }),
                    Value::Object(_) => serde_json::from_value(entry.clone()).ok(),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("pa-daemon-journal-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn command_journal_survives_restart_with_uncertainty() {
        let path = temp_path("command-journal.jsonl");
        let mut journal = CommandRecoveryJournal::open(&path).unwrap();
        assert!(journal.begin("client", "c1", "create").unwrap().is_none());
        let response =
            serde_json::json!({"type": "response", "command": "create", "success": true});
        journal.record_result("client", "c1", &response).unwrap();

        let mut reloaded = CommandRecoveryJournal::open(&path).unwrap();
        let entry = reloaded.lookup("client", "c1").unwrap();
        assert_eq!(entry.status, "complete");
        assert_eq!(entry.response, Some(response));

        // Pending (received, no result) is reported but not replayed.
        reloaded.begin("client", "c2", "kill").unwrap();
        let reloaded2 = CommandRecoveryJournal::open(&path).unwrap();
        assert_eq!(reloaded2.lookup("client", "c2").unwrap().status, "pending");
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn worker_journal_keeps_latest_per_session() {
        let path = temp_path("worker.recovery.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        journal
            .record("s1", "sess1", Some("/a.jsonl"), true, "prompt")
            .unwrap();
        journal.record("s2", "sess2", None, false, "ready").unwrap();
        journal
            .record("s1", "sess1", Some("/a.jsonl"), false, "idle")
            .unwrap();
        let latest = WorkerRecoveryJournal::read_latest(&path).unwrap();
        assert_eq!(latest.len(), 2);
        let s1 = latest.iter().find(|r| r.active_session_id == "s1").unwrap();
        assert!(!s1.busy);
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn worker_journal_interrupted_evidence_tracks_latest_busy() {
        let path = temp_path("interrupted.recovery.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        // Idle sessions prove nothing: no interrupted work to revive.
        journal
            .record("s1", "sess1", None, false, "shutdown")
            .unwrap();
        journal.record("s2", "sess2", None, false, "ready").unwrap();
        assert!(!WorkerRecoveryJournal::read_interrupted(&path));
        // One busy session is durable evidence of interrupted work.
        journal
            .record("s2", "sess2", Some("/b.jsonl"), true, "create")
            .unwrap();
        assert!(WorkerRecoveryJournal::read_interrupted(&path));
        // The latest record per session decides: s2 settles back to idle.
        journal
            .record("s2", "sess2", None, false, "shutdown")
            .unwrap();
        assert!(!WorkerRecoveryJournal::read_interrupted(&path));
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// The batched queue checkpoint and the sequential form produce the
    /// same journal: same lines in the same order, same latest records,
    /// same restorable queue snapshots (the `recorded_at` stamps differ only
    /// because the two runs cannot share a clock instant).
    #[test]
    fn worker_journal_batched_checkpoint_matches_sequential_form() {
        let sequential_path = temp_path("sequential.recovery.jsonl");
        let batched_path = temp_path("batched.recovery.jsonl");
        let mut sequential = WorkerRecoveryJournal::open(&sequential_path).unwrap();
        let mut batched = WorkerRecoveryJournal::open(&batched_path).unwrap();
        let item = WorkerQueueItemRecord {
            message: "steer me".to_string(),
            priority: Some(crate::worker::QueuePriority::Human),
            preview: Some("preview".to_string()),
            custom_message: None,
            queue_key: None,
            queue_visible: true,
            policy: queue_policy_default(),
        };
        // Admitted (snapshot + busy verdict), settle (snapshot + idle
        // verdict + compaction), then an unchanged-verdict checkpoint whose
        // snapshot lands alone in both forms.
        sequential
            .record_queue_snapshot("s1", std::slice::from_ref(&item), &[])
            .unwrap();
        sequential
            .record("s1", "sess1", Some("/a.jsonl"), true, "prompt_accepted")
            .unwrap();
        sequential.record_queue_snapshot("s1", &[], &[]).unwrap();
        sequential
            .record("s1", "sess1", Some("/a.jsonl"), false, "turn_end")
            .unwrap();
        // An unchanged verdict: the snapshot still lands, alone.
        sequential.record_queue_snapshot("s1", &[], &[]).unwrap();
        sequential
            .record("s1", "sess1", Some("/a.jsonl"), false, "turn_end")
            .unwrap();
        batched
            .record_queue_checkpoint(
                "s1",
                "sess1",
                Some("/a.jsonl"),
                true,
                "prompt_accepted",
                std::slice::from_ref(&item),
                &[],
                None,
            )
            .unwrap();
        batched
            .record_queue_checkpoint(
                "s1",
                "sess1",
                Some("/a.jsonl"),
                false,
                "turn_end",
                &[],
                &[],
                None,
            )
            .unwrap();
        batched
            .record_queue_checkpoint(
                "s1",
                "sess1",
                Some("/a.jsonl"),
                false,
                "turn_end",
                &[],
                &[],
                None,
            )
            .unwrap();

        let strip_stamps = |path: &std::path::Path| -> Vec<Value> {
            std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| {
                    let mut value: Value = serde_json::from_str(line).unwrap();
                    if let Some(object) = value.as_object_mut() {
                        object.remove("recordedAt");
                        object.remove("recorded_at");
                    }
                    value
                })
                .collect()
        };
        assert_eq!(
            strip_stamps(&sequential_path),
            strip_stamps(&batched_path),
            "the batched checkpoint writes the same journal lines as the sequential form"
        );
        let latest_a = sequential.get_latest();
        let latest_b = batched.get_latest();
        assert_eq!(latest_a.len(), latest_b.len());
        assert_eq!(latest_a[0].busy, latest_b[0].busy);
        assert_eq!(latest_a[0].operation, latest_b[0].operation);
        let restored = WorkerRecoveryJournal::read_queue_snapshot(&batched_path, "s1").unwrap();
        assert_eq!(restored, Some((Vec::new(), Vec::new())));
        let _ = fs::remove_dir_all(sequential_path.parent().unwrap());
        let _ = fs::remove_dir_all(batched_path.parent().unwrap());
    }

    /// The busy verdict rides the snapshot's single flush: a checkpoint
    /// whose batched append fails lands NEITHER record (no verdict over an
    /// unpersisted snapshot, and no snapshot without its flush).
    #[test]
    fn worker_journal_batched_checkpoint_is_all_or_nothing() {
        let path = temp_path("allornothing.recovery.jsonl");
        fs::write(&path, "").unwrap();
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        journal.record("s1", "sess1", None, false, "ready").unwrap();
        // Replace the journal with a directory: every open for append now
        // fails, so the checkpoint cannot land either record.
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let result = journal.record_queue_checkpoint(
            "s1",
            "sess1",
            None,
            true,
            "prompt_accepted",
            &[],
            &[],
            None,
        );
        assert!(result.is_err());
        // The in-memory verdict did not advance over the failed append.
        assert!(journal.latest.get("s1").is_some_and(|record| !record.busy));
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// TS parity oracle: a single session's settle compacts (the post-
    /// insert all-idle check). The OLD pre-insert check let the session's
    /// own busy admission record block the compaction, so a single-session
    /// journal grew append-only forever; TS compacts at every changed-idle
    /// record and so does the port now.
    #[test]
    fn worker_journal_settle_compacts_single_session() {
        let path = temp_path("settle-compacts.recovery.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        // Two busy/idle cycles through the plain `record` path: the first
        // settle compacts to one line, so the second admission starts from
        // a one-line file (two lines mid-flight, one after the settle) —
        // without the settle compaction the file would grow 2 lines per
        // cycle.
        journal
            .record("s1", "sess1", Some("/a.jsonl"), true, "prompt_accepted")
            .unwrap();
        journal
            .record("s1", "sess1", Some("/a.jsonl"), false, "turn_end")
            .unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap().lines().count(),
            1,
            "the first settle compacted to the latest record"
        );
        journal
            .record("s1", "sess1", Some("/a.jsonl"), true, "prompt_accepted")
            .unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap().lines().count(),
            2,
            "the second admission grows the compacted file"
        );
        journal
            .record("s1", "sess1", Some("/a.jsonl"), false, "turn_end")
            .unwrap();
        // The settle compacted: the file holds exactly the latest record.
        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 1, "the settle compacts to the latest record");
        let record: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(record["busy"], false);
        assert_eq!(record["operation"], "turn_end");
        // The compacted journal replays the same latest state.
        let reopened = WorkerRecoveryJournal::open(&path).unwrap();
        let latest = reopened.get_latest();
        assert_eq!(latest.len(), 1);
        assert!(!latest[0].busy);
        assert!(!WorkerRecoveryJournal::read_interrupted(&path));
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// The settle through the batched checkpoint compacts to the same
    /// two lines (idle verdict + latest snapshot) and restores the same
    /// queue lanes a pre-compact append-only history would.
    #[test]
    fn worker_journal_batched_settle_compacts_and_restores() {
        let path = temp_path("batched-settle.recovery.jsonl");
        let item = WorkerQueueItemRecord {
            message: "steer me".to_string(),
            priority: Some(crate::worker::QueuePriority::Human),
            preview: Some("preview".to_string()),
            custom_message: None,
            queue_key: None,
            queue_visible: true,
            policy: queue_policy_default(),
        };
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        // Two turns: each admission batch grows the file; each settle
        // compacts it back — without the compaction the second admission
        // would stack on the first turn's history (6 lines by the end).
        journal
            .record_queue_checkpoint(
                "s1",
                "sess1",
                Some("/a.jsonl"),
                true,
                "prompt_accepted",
                std::slice::from_ref(&item),
                &[],
                None,
            )
            .unwrap();
        journal
            .record_queue_checkpoint(
                "s1",
                "sess1",
                Some("/a.jsonl"),
                false,
                "turn_end",
                &[],
                &[],
                None,
            )
            .unwrap();
        let after_first_settle = fs::read_to_string(&path).unwrap().lines().count();
        journal
            .record_queue_checkpoint(
                "s1",
                "sess1",
                Some("/a.jsonl"),
                true,
                "prompt_accepted",
                std::slice::from_ref(&item),
                &[],
                None,
            )
            .unwrap();
        let after_second_admission = fs::read_to_string(&path).unwrap().lines().count();
        journal
            .record_queue_checkpoint(
                "s1",
                "sess1",
                Some("/a.jsonl"),
                false,
                "turn_end",
                &[],
                &[],
                None,
            )
            .unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 2, "the settle compacts to verdict + snapshot");
        assert_eq!(after_first_settle, 2, "the first settle compacted");
        assert_eq!(
            after_second_admission, 4,
            "the second admission grew the file"
        );
        let verdict: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(verdict["busy"], false);
        assert_eq!(verdict["operation"], "turn_end");
        let snapshot: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(snapshot["type"], "queue_snapshot");
        // the compact keeps the LATEST snapshot per session: the settle's
        // (empty) lanes, not the admission's parked row.
        assert_eq!(snapshot["steering"].as_array().map(Vec::len), Some(0));
        // The reopened journal restores the settled verdict and the
        // settle's (empty) lanes exactly like the append-only history.
        let reopened = WorkerRecoveryJournal::open(&path).unwrap();
        assert!(!WorkerRecoveryJournal::read_interrupted(&path));
        let restored = reopened.latest_queue_snapshot("s1").unwrap();
        assert_eq!(restored.0, Vec::new());
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// An unchanged verdict appends the snapshot alone and never compacts
    /// (TS `record` early-returns before its compaction check): the
    /// compaction belongs to changed-idle records only.
    #[test]
    fn worker_journal_unchanged_verdict_does_not_compact() {
        let path = temp_path("unchanged-nocompact.recovery.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        journal
            .record_queue_checkpoint("s1", "sess1", None, true, "prompt_accepted", &[], &[], None)
            .unwrap();
        journal
            .record_queue_checkpoint("s1", "sess1", None, false, "turn_end", &[], &[], None)
            .unwrap();
        let lines_after_settle = fs::read_to_string(&path).unwrap().lines().count();
        // The unchanged settle: the snapshot lands, the verdict does not,
        // and no compaction runs (the map never changed).
        journal
            .record_queue_checkpoint("s1", "sess1", None, false, "turn_end", &[], &[], None)
            .unwrap();
        let lines_after_unchanged = fs::read_to_string(&path).unwrap().lines().count();
        assert_eq!(lines_after_unchanged, lines_after_settle + 1);
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn worker_journal_missing_or_unreadable_file_is_not_interrupted() {
        let path = temp_path("missing.recovery.jsonl");
        // No journal: no evidence, so no revival on uncertainty.
        assert!(!WorkerRecoveryJournal::read_interrupted(&path));
        std::fs::write(&path, "not json").unwrap();
        assert!(!WorkerRecoveryJournal::read_interrupted(&path));
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }
    // -----------------------------------------------------------------------
    // Checkpoint transactions (the cloud-keyed delivery's commit unit)
    // -----------------------------------------------------------------------

    /// One full keyed checkpoint: the transaction line carries the
    /// snapshot, the busy verdict, and the cloud admission together, and
    /// the reopen replays all three (the lane restore and the inbox key
    /// land together or not at all).
    #[test]
    fn checkpoint_transaction_replays_snapshot_and_admission_together() {
        let path = temp_path("transaction.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        let receipt = serde_json::json!({
            "id": "agentmsg_tx1",
            "deliveryStatus": "delivered",
            "deliveryMode": "steer",
        });
        journal
            .record_queue_checkpoint(
                "sess-a",
                "sess-a-file",
                None,
                true,
                "steer_queued",
                &[WorkerQueueItemRecord {
                    message: "cloud note".to_string(),
                    priority: None,
                    preview: None,
                    custom_message: None,
                    queue_key: None,
                    queue_visible: true,
                    policy: "injected".to_string(),
                }],
                &[],
                Some(("msgreq_tx1", &receipt)),
            )
            .unwrap();
        let reloaded = WorkerRecoveryJournal::open(&path).unwrap();
        assert_eq!(
            reloaded.cloud_inbox_receipt("msgreq_tx1"),
            Some(&receipt),
            "the admission replays"
        );
        let (steering, _) = crate::worker::restore_queue_snapshot(&reloaded, "sess-a");
        assert_eq!(
            steering.len(),
            1,
            "the queue row replays with the admission"
        );
        assert_eq!(steering[0].message, "cloud note");
        assert!(
            reloaded
                .latest
                .get("sess-a")
                .is_some_and(|record| record.busy),
            "the busy verdict replays"
        );
        // One transaction line on disk, sealed with its digest.
        let content = fs::read_to_string(&path).unwrap();
        let transaction_lines = content
            .lines()
            .filter(|line| line.contains("queue_checkpoint_transaction"))
            .count();
        assert_eq!(transaction_lines, 1, "one transaction line: {content}");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// A crash-torn transaction append (the last line truncated mid-JSON)
    /// drops the WHOLE transaction — the queue row never replays without
    /// its request-id admission — and the repair truncates the fragment
    /// so the next append cannot glue onto it.
    #[test]
    fn torn_transaction_tail_drops_all_of_it_and_repairs_the_file() {
        let path = temp_path("torn-transaction.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        let receipt = serde_json::json!({ "id": "agentmsg_tx2", "deliveryStatus": "delivered" });
        journal
            .record_queue_checkpoint(
                "sess-b",
                "sess-b-file",
                None,
                true,
                "steer_queued",
                &[],
                &[],
                Some(("msgreq_tx2", &receipt)),
            )
            .unwrap();
        // The crash: the final append lands only partially.
        let content = fs::read_to_string(&path).unwrap();
        let torn: String = content.lines().last().unwrap().chars().take(40).collect();
        fs::write(&path, &torn).unwrap();
        // The reload: the torn transaction replays as nothing (no
        // snapshot, no key) and the file is repaired (the fragment gone).
        let reloaded = WorkerRecoveryJournal::open(&path).unwrap();
        assert!(
            reloaded.cloud_inbox_receipt("msgreq_tx2").is_none(),
            "a torn transaction never leaves its admission"
        );
        assert!(
            reloaded.latest_queue_snapshot("sess-b").is_none(),
            "a torn transaction never leaves its queue row"
        );
        let repaired = fs::read_to_string(&path).unwrap();
        assert!(
            !repaired.contains(&torn[..20]),
            "the torn fragment was truncated: {repaired}"
        );
        // The next append writes onto the clean boundary and replays.
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        journal
            .record_queue_checkpoint(
                "sess-b",
                "sess-b-file",
                None,
                true,
                "steer_queued",
                &[],
                &[],
                Some(("msgreq_tx3", &receipt)),
            )
            .unwrap();
        let reloaded = WorkerRecoveryJournal::open(&path).unwrap();
        assert_eq!(
            reloaded.cloud_inbox_receipt("msgreq_tx3"),
            Some(&receipt),
            "the post-repair append replays cleanly"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// Mid-file corruption (an unparsable line followed by a valid one)
    /// fails closed: the open refuses, the revival evidence reads as
    /// nothing, and the file is left byte-for-byte alone — history is
    /// never silently dropped.
    #[test]
    fn mid_file_corruption_fails_closed_and_preserves_the_file() {
        let path = temp_path("mid-file.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        journal
            .record("sess-c", "sess-c-file", None, true, "prompt_accepted")
            .unwrap();
        let valid = fs::read_to_string(&path).unwrap();
        // The corruption: a bad line, then a valid append after it.
        fs::write(&path, format!("{{this is not json\n{valid}")).unwrap();
        let content_before = fs::read_to_string(&path).unwrap();
        assert!(
            WorkerRecoveryJournal::open(&path).is_err(),
            "mid-file corruption fails closed"
        );
        assert!(
            !WorkerRecoveryJournal::read_interrupted(&path),
            "corrupted evidence proves nothing (uncertainty must not revive)"
        );
        assert!(
            WorkerRecoveryJournal::read_latest(&path).is_err(),
            "the read seam fails closed too"
        );
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            content_before,
            "the corrupted file is never rewritten"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// A corrupted-but-parseable transaction line (the digest does not
    /// match its records) replays as nothing — half a transaction is
    /// never a transaction.
    #[test]
    fn a_digest_mismatched_transaction_replays_as_nothing() {
        let path = temp_path("bad-digest.jsonl");
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        let receipt = serde_json::json!({ "id": "agentmsg_tx4", "deliveryStatus": "delivered" });
        journal
            .record_queue_checkpoint(
                "sess-d",
                "sess-d-file",
                None,
                true,
                "steer_queued",
                &[],
                &[],
                Some(("msgreq_tx4", &receipt)),
            )
            .unwrap();
        // Corrupt the digest in place (a complete line, a wrong seal).
        let content = fs::read_to_string(&path).unwrap();
        let corrupted = content.replace("\"digest\":\"", "\"digest\":\"deadbeef");
        assert_ne!(corrupted, content, "the digest must be corruptible");
        fs::write(&path, corrupted).unwrap();
        // The scan drops the whole transaction and repairs the tail.
        let reloaded = WorkerRecoveryJournal::open(&path).unwrap();
        assert!(
            reloaded.cloud_inbox_receipt("msgreq_tx4").is_none(),
            "a mismatched seal never replays its admission"
        );
        assert!(
            reloaded.latest_queue_snapshot("sess-d").is_none(),
            "a mismatched seal never replays its queue row"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
