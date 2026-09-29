//! Durable journals for the cloud family exchange.
//!
//! The request log is the guest-side slice of the TS `DurableCloudEventOutbox`
//! (`event-outbox.ts`): every request is fsync'd to NDJSON before the caller
//! may treat it as admitted, a full log stalls honestly, and a reload skips
//! the crash-truncated tail. The result log is the responder-side durable
//! record of one journaled answer per request id (the durable half of TS
//! `markRemoteRequestProcessed`): a duplicate request re-submits the same
//! answer without re-delivering.
//!
//! Cursor generations and ack-trimming stay with the cloud protocol server
//! port; this slice is append + replay with a fixed generation, bounded by
//! the TS record cap, so an unacked full log stalls exactly like TS.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use pa_types::daemon::cloud::{
    canonical_json, CloudFamilyCommand, CloudFamilyEvent, CloudFamilyEventPayload,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::MAX_REMEMBERED_REQUESTS;
use crate::util::now_iso;

/// Fixed event-log epoch for this slice (TS starts every outbox at
/// generation 1; epochs advance only on a committed trim).
const GENERATION: u64 = 1;
const EVENTS_FILE: &str = "outbox-events.ndjson";

/// Durable guest-side request log for the family exchange: one canonical
/// NDJSON envelope per request event, fsync'd on append before admission is
/// reported. A crash may leave only the final append truncated; the reload
/// repairs it by dropping the partial line.
pub struct FamilyRequestLog {
    directory: PathBuf,
    session_id: String,
    events: Vec<CloudFamilyEvent>,
    max_records: usize,
    max_event_bytes: usize,
}

impl FamilyRequestLog {
    /// Open (or create) the request log under `directory`, loading and
    /// validating the durable events.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created, the log is
    /// corrupt (digest, envelope, or sequence gap), or the repair write of a
    /// crash-truncated tail fails.
    pub fn open(directory: &Path, session_id: &str, max_records: usize) -> Result<Self> {
        fs::create_dir_all(directory).with_context(|| format!("create {}", directory.display()))?;
        let mut log = Self {
            directory: directory.to_path_buf(),
            session_id: session_id.to_string(),
            events: Vec::new(),
            max_records,
            max_event_bytes: pa_types::daemon::cloud::CLOUD_MAX_MESSAGE_BYTES,
        };
        log.load()?;
        Ok(log)
    }

    /// Append one request durably: the event is built, canonicalized, size
    /// checked, written, and fsync'd before it is returned. Only after this
    /// returns may a caller treat the request as admitted.
    ///
    /// # Errors
    ///
    /// Returns an error when the log is full (the TS stall), the event is
    /// over the frame bound, or the durable append fails.
    pub fn append(&mut self, payload: CloudFamilyEventPayload) -> Result<CloudFamilyEvent> {
        if self.events.len() >= self.max_records {
            return Err(anyhow!(
                "Cloud event outbox reached {} records",
                self.max_records
            ));
        }
        let sequence = self.tail_sequence() + 1;
        let event = CloudFamilyEvent {
            sequence,
            recorded_at: now_iso(),
            payload,
        };
        let envelope = self.envelope(&event)?;
        if envelope.len() >= self.max_event_bytes {
            return Err(anyhow!(
                "Cloud event exceeds {} bytes",
                self.max_event_bytes
            ));
        }
        let mut line = envelope;
        line.push('\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.events_path())
            .with_context(|| format!("open {}", self.events_path().display()))?;
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        self.events.push(event.clone());
        Ok(event)
    }

    /// The sequence of the newest admitted event (0 when empty).
    #[must_use]
    pub fn tail_sequence(&self) -> u64 {
        self.events.last().map_or(0, |event| event.sequence)
    }

    /// Admitted events after `sequence`, oldest first. A `sequence` beyond
    /// the tail is a cursor error, not an empty batch.
    ///
    /// # Errors
    ///
    /// Returns the TS cursor problem string when `sequence` is beyond the
    /// event tail.
    pub fn events_after(&self, sequence: u64) -> Result<Vec<CloudFamilyEvent>, String> {
        if sequence > self.tail_sequence() {
            return Err("Cloud cursor is beyond the event tail".to_string());
        }
        Ok(self
            .events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    /// Number of admitted (untrimmed) events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// True when no event has been admitted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn events_path(&self) -> PathBuf {
        self.directory.join(EVENTS_FILE)
    }

    fn envelope(&self, event: &CloudFamilyEvent) -> Result<String> {
        let event_value = serde_json::to_value(event)?;
        let canonical = canonical_json(&json!({
            "sessionId": self.session_id,
            "generation": GENERATION,
            "event": event_value,
        }))
        .map_err(|reason| anyhow!("canonical JSON: {reason}"))?;
        let hex =
            Sha256::digest(canonical.as_bytes())
                .iter()
                .fold(String::new(), |mut key, byte| {
                    use std::fmt::Write;
                    write!(key, "{byte:02x}").expect("write to String");
                    key
                });
        canonical_json(&json!({
            "eventId": format!("evt_{hex}"),
            "generation": GENERATION,
            "event": event_value,
        }))
        .map_err(|reason| anyhow!("canonical JSON: {reason}"))
    }

    fn load(&mut self) -> Result<()> {
        let path = self.events_path();
        let Ok(content) = fs::read_to_string(&path) else {
            File::create(&path).with_context(|| format!("create {}", path.display()))?;
            return Ok(());
        };
        let mut lines: Vec<&str> = content.split('\n').collect();
        let ended = content.ends_with('\n');
        if lines.last() == Some(&"") {
            lines.pop();
        }
        if !ended && !lines.is_empty() {
            // A crash truncated the final append: drop it and repair the
            // file to the last complete record.
            lines.pop();
            self.rewrite(lines.iter().map(|line| (*line).to_string()).collect())?;
        }
        for (index, line) in lines.iter().enumerate() {
            let record: Value = serde_json::from_str(line)
                .map_err(|_| anyhow!("Cloud event outbox record is corrupt"))?;
            let event_value = record
                .get("event")
                .filter(|event| event.is_object())
                .ok_or_else(|| anyhow!("Cloud event outbox record is corrupt"))?;
            if record.get("generation").and_then(Value::as_u64) != Some(GENERATION) {
                return Err(anyhow!("Cloud event outbox record is corrupt"));
            }
            let event: CloudFamilyEvent = serde_json::from_value(event_value.clone())
                .map_err(|_| anyhow!("Cloud event outbox record has an invalid event"))?;
            let expected = self.envelope(&event)?;
            let stored = canonical_json(&record)
                .map_err(|_| anyhow!("Cloud event outbox record is corrupt"))?;
            if expected != stored {
                return Err(anyhow!("Cloud event outbox record digest is corrupt"));
            }
            if event.sequence != (index as u64) + 1 {
                return Err(anyhow!("Cloud event outbox has a sequence gap"));
            }
            self.events.push(event);
        }
        Ok(())
    }

    /// Rewrite the log with the given canonical envelope lines, durably
    /// (temp file, fsync, rename), repairing a truncated tail in place.
    fn rewrite(&mut self, lines: Vec<String>) -> Result<()> {
        let path = self.events_path();
        let temp = path.with_extension("ndjson.tmp");
        {
            let file = File::create(&temp).with_context(|| format!("create {}", temp.display()))?;
            let mut writer = BufWriter::new(file);
            for line in &lines {
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        fs::rename(&temp, &path).with_context(|| format!("persist {}", path.display()))?;
        Ok(())
    }
}

/// One durable answer record per remembered request id (the responder side).
/// First writer wins: a repeat record is a no-op, so the journaled answer is
/// exactly-once per request id even when the guest replays the request
/// event.
pub struct FamilyResultLog {
    path: PathBuf,
    remembered: VecDeque<CloudFamilyCommand>,
    max_remembered: usize,
}

impl FamilyResultLog {
    /// Open (or create) the result log at `path`, replaying the remembered
    /// answers. A crash-truncated or malformed tail is skipped, like the
    /// recovery journals.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent directory cannot be created.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut log = Self {
            path: path.to_path_buf(),
            remembered: VecDeque::new(),
            max_remembered: MAX_REMEMBERED_REQUESTS,
        };
        log.load();
        Ok(log)
    }

    /// The journaled answer for `request_id`, newest first.
    #[must_use]
    pub fn get(&self, request_id: &str) -> Option<CloudFamilyCommand> {
        self.remembered
            .iter()
            .rev()
            .find(|command| command.request_id() == request_id)
            .cloned()
    }

    /// Record one answer durably. An id that already has an answer is a
    /// no-op (first writer wins). The oldest answer beyond
    /// [`MAX_REMEMBERED_REQUESTS`] is evicted — a replayed request past the
    /// window re-delivers (at-least-once, TS parity), and the guest's
    /// journal still dedupes the answer command.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable append or the post-append
    /// compaction fails.
    pub fn record(&mut self, command: CloudFamilyCommand) -> Result<()> {
        if self.get(command.request_id()).is_some() {
            return Ok(());
        }
        let record = json!({"version": 1, "command": command});
        crate::journal::append_record(&self.path, &record)?;
        self.remembered.push_back(command);
        while self.remembered.len() > self.max_remembered {
            self.remembered.pop_front();
            self.compact()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let records: Vec<Value> = self
            .remembered
            .iter()
            .map(|command| json!({"version": 1, "command": command}))
            .collect();
        crate::journal::rewrite_records(&self.path, &records, crate::journal::Finalize::Synced)?;
        Ok(())
    }

    fn load(&mut self) {
        let Ok(content) = fs::read_to_string(&self.path) else {
            return;
        };
        for line in content.lines() {
            let Ok(record) = serde_json::from_str::<Value>(line) else {
                // A crash may leave only the final append truncated.
                continue;
            };
            if record.get("version").and_then(Value::as_u64) != Some(1) {
                continue;
            }
            let Ok(command) = serde_json::from_value::<CloudFamilyCommand>(
                record.get("command").cloned().unwrap_or(Value::Null),
            ) else {
                continue;
            };
            self.remembered.push_back(command);
            while self.remembered.len() > self.max_remembered {
                self.remembered.pop_front();
            }
        }
    }
}
