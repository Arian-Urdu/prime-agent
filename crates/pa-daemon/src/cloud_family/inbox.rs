//! The delivery seam's request-id keyed receiver inbox journal: the
//! durable binding of one cross-boundary request id to its delivery
//! (the target selector, the source, the message) and to the receipt the
//! receiver once admitted. Two phases, exactly like the responder's
//! [`crate::cloud_family::FamilyResultLog`]: `admit` durably records the
//! delivery parameters BEFORE any delivery, `record_receipt` durably
//! records the receiver-admitted receipt after the target answered. The
//! lookup reconciles both crash gaps through the same record: a receipt
//! answers the delivery truth without re-delivering, and an
//! admitted-without-receipt record is safe to re-drive because the
//! receiver inbox is itself idempotent by request id
//! (`cloud_inbox_admission` in the worker recovery journal).
//!
//! The retention window matches the family request outbox's record cap,
//! so every replayable request finds its journal state.

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use pa_types::daemon::cloud::CloudAgentMessageReceipt;
use serde_json::{json, Value};

use super::{log::Admission, IncomingCloudMessage, DEFAULT_OUTBOX_RECORDS};

/// One durably-admitted delivery slot: the request id plus its delivery
/// parameters, and the receiver-admitted receipt once one exists.
struct InboxSlot {
    request_id: String,
    target_selector: String,
    from_remote_session_id: String,
    message: String,
    receipt: Option<CloudAgentMessageReceipt>,
}

/// The delivery seam's two-phase receiver inbox journal.
pub struct CloudInboxLog {
    path: PathBuf,
    slots: VecDeque<InboxSlot>,
    max_remembered: usize,
}

impl CloudInboxLog {
    /// Open (or create) the inbox journal at `path`, replaying the
    /// admitted requests and their recorded receipts. A crash-truncated
    /// or malformed tail is skipped, like the recovery journals.
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
            slots: VecDeque::new(),
            max_remembered: DEFAULT_OUTBOX_RECORDS,
        };
        log.load();
        Ok(log)
    }

    /// The receiver-admitted receipt for `request_id`, when one is
    /// recorded.
    #[must_use]
    pub fn receipt(&self, request_id: &str) -> Option<CloudAgentMessageReceipt> {
        self.slot(request_id).and_then(|slot| slot.receipt.clone())
    }

    /// The durably-admitted delivery parameters for `request_id` — the
    /// re-drive input when a crash interrupted the first handling.
    #[must_use]
    pub fn admission(&self, request_id: &str) -> Option<IncomingCloudMessage> {
        self.slot(request_id).map(|slot| IncomingCloudMessage {
            request_id: slot.request_id.clone(),
            from_remote_session_id: slot.from_remote_session_id.clone(),
            target_selector: slot.target_selector.clone(),
            message: slot.message.clone(),
        })
    }

    /// Durably admit one cross-boundary delivery BEFORE it is attempted:
    /// the append is fsync'd before `First` is returned.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable admission append fails.
    pub fn admit(&mut self, message: &IncomingCloudMessage) -> Result<Admission> {
        if self.slot(&message.request_id).is_some() {
            return Ok(Admission::Already);
        }
        crate::journal::append_record(
            &self.path,
            &json!({
                "version": 1,
                "type": "admitted",
                "requestId": message.request_id,
                "targetSelector": message.target_selector,
                "fromRemoteSessionId": message.from_remote_session_id,
                "message": message.message,
            }),
        )?;
        self.push_slot(InboxSlot {
            request_id: message.request_id.clone(),
            target_selector: message.target_selector.clone(),
            from_remote_session_id: message.from_remote_session_id.clone(),
            message: message.message.clone(),
            receipt: None,
        });
        Ok(Admission::First)
    }

    /// Durably record the receiver-admitted receipt for an admitted
    /// request. First writer wins: an id that already has a receipt is a
    /// no-op.
    ///
    /// # Errors
    ///
    /// Returns an error when the request was never admitted or the
    /// durable receipt append fails.
    pub fn record_receipt(
        &mut self,
        request_id: &str,
        receipt: CloudAgentMessageReceipt,
    ) -> Result<()> {
        let Some(index) = self
            .slots
            .iter()
            .rposition(|slot| slot.request_id == request_id)
        else {
            return Err(anyhow!(
                "cannot record a receipt before admitting {request_id}"
            ));
        };
        if self.slots[index].receipt.is_some() {
            return Ok(());
        }
        crate::journal::append_record(
            &self.path,
            &json!({
                "version": 1,
                "type": "receipt",
                "requestId": request_id,
                "receipt": receipt,
            }),
        )
        .context("append the inbox receipt record")?;
        self.slots[index].receipt = Some(receipt);
        Ok(())
    }

    fn slot(&self, request_id: &str) -> Option<&InboxSlot> {
        self.slots
            .iter()
            .rev()
            .find(|slot| slot.request_id == request_id)
    }

    fn push_slot(&mut self, slot: InboxSlot) {
        self.slots.push_back(slot);
        while self.slots.len() > self.max_remembered {
            self.slots.pop_front();
            self.compact();
        }
    }

    /// Rewrite the journal to the live window, durably (temp file, fsync,
    /// rename). Admits without receipts survive compaction as admits, so
    /// a compact can never strand an uncertain request.
    fn compact(&mut self) {
        let records: Vec<Value> = self
            .slots
            .iter()
            .flat_map(|slot| {
                let admitted = json!({
                    "version": 1,
                    "type": "admitted",
                    "requestId": slot.request_id,
                    "targetSelector": slot.target_selector,
                    "fromRemoteSessionId": slot.from_remote_session_id,
                    "message": slot.message,
                });
                let receipt = slot.receipt.as_ref().map(|receipt| {
                    json!({
                        "version": 1,
                        "type": "receipt",
                        "requestId": slot.request_id,
                        "receipt": receipt,
                    })
                });
                std::iter::once(admitted).chain(receipt)
            })
            .collect();
        let _ =
            crate::journal::rewrite_records(&self.path, &records, crate::journal::Finalize::Synced);
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
            let Some(request_id) = record.get("requestId").and_then(Value::as_str) else {
                continue;
            };
            match record.get("type").and_then(Value::as_str) {
                Some("admitted") => {
                    if self.slot(request_id).is_none() {
                        self.push_slot(InboxSlot {
                            request_id: request_id.to_string(),
                            target_selector: record
                                .get("targetSelector")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            from_remote_session_id: record
                                .get("fromRemoteSessionId")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            message: record
                                .get("message")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            receipt: None,
                        });
                    }
                }
                Some("receipt") => {
                    let Ok(receipt) = serde_json::from_value::<CloudAgentMessageReceipt>(
                        record.get("receipt").cloned().unwrap_or(Value::Null),
                    ) else {
                        continue;
                    };
                    if let Some(slot) = self
                        .slots
                        .iter_mut()
                        .rev()
                        .find(|slot| slot.request_id == request_id)
                    {
                        slot.receipt.get_or_insert(receipt);
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(request_id: &str, target: &str) -> IncomingCloudMessage {
        IncomingCloudMessage {
            request_id: request_id.to_string(),
            from_remote_session_id: "remote-1".to_string(),
            target_selector: target.to_string(),
            message: "cross the boundary".to_string(),
        }
    }

    fn receipt(id: &str) -> CloudAgentMessageReceipt {
        serde_json::from_value(json!({
            "id": id,
            "deliveryStatus": "delivered",
            "deliveryMode": "steer",
        }))
        .unwrap()
    }

    /// The two-phase round trip: admit, record, reopen — the receipt and
    /// the delivery parameters survive, and a duplicate admit is
    /// `Already`.
    #[test]
    fn two_phase_round_trip_survives_the_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cloud-inbox.jsonl");
        let mut log = CloudInboxLog::open(&path).unwrap();
        assert_eq!(
            log.admit(&message("msgreq_1", "target-a")).unwrap(),
            Admission::First
        );
        assert_eq!(
            log.admit(&message("msgreq_1", "target-a")).unwrap(),
            Admission::Already
        );
        log.record_receipt("msgreq_1", receipt("agentmsg_1"))
            .unwrap();
        // Recording twice is a no-op (first writer wins).
        log.record_receipt("msgreq_1", receipt("agentmsg_2"))
            .unwrap();
        drop(log);
        let reloaded = CloudInboxLog::open(&path).unwrap();
        assert_eq!(
            reloaded.receipt("msgreq_1").expect("receipt"),
            receipt("agentmsg_1")
        );
        assert_eq!(
            reloaded.admission("msgreq_1").expect("admission"),
            message("msgreq_1", "target-a")
        );
        assert!(reloaded.admission("msgreq_unknown").is_none());
        // A receipt before admission is a protocol error.
        let mut fresh = CloudInboxLog::open(&dir.path().join("other.jsonl")).unwrap();
        assert!(fresh
            .record_receipt("never-admitted", receipt("x"))
            .is_err());
    }

    /// The crash gap on disk: an admitted record without its receipt is
    /// the re-drive input, and a crash-truncated tail line is skipped.
    #[test]
    fn admitted_without_receipt_is_the_re_drive_input() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cloud-inbox.jsonl");
        let mut log = CloudInboxLog::open(&path).unwrap();
        log.admit(&message("msgreq_gap", "target-b")).unwrap();
        log.record_receipt("msgreq_gap", receipt("agentmsg_gap"))
            .unwrap();
        // Simulate the crash: the receipt append never landed (truncate
        // the last line to its half, the crash-truncated tail).
        let content = fs::read_to_string(&path).unwrap();
        let first_line = content.lines().next().expect("the admitted record");
        // The crash-truncated tail: the receipt record lands only as a
        // partial line the reload must skip.
        let truncated = format!("{first_line}\n{{\"half");
        fs::write(&path, truncated).unwrap();
        let reloaded = CloudInboxLog::open(&path).unwrap();
        assert_eq!(
            reloaded
                .admission("msgreq_gap")
                .expect("admission survives"),
            message("msgreq_gap", "target-b")
        );
        assert!(
            reloaded.receipt("msgreq_gap").is_none(),
            "the receipt record died with the crash"
        );
    }

    /// The retention window: the newest `DEFAULT_OUTBOX_RECORDS` slots
    /// survive compaction; an admitted-without-receipt slot survives as
    /// an admit.
    #[test]
    fn window_compaction_keeps_admits_without_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cloud-inbox.jsonl");
        let mut log = CloudInboxLog::open(&path).unwrap();
        log.max_remembered = 4;
        for index in 0..6 {
            log.admit(&message(&format!("msgreq_{index}"), "target"))
                .unwrap();
            if index % 2 == 0 {
                log.record_receipt(&format!("msgreq_{index}"), receipt(&format!("r{index}")))
                    .unwrap();
            }
        }
        assert_eq!(log.slots.len(), 4, "the window slides to the newest slots");
        assert!(log.receipt("msgreq_0").is_none(), "out of window");
        assert!(log.receipt("msgreq_4").is_some(), "in window");
        // The compacted file reloads with the same live window.
        let reloaded = CloudInboxLog::open(&path).unwrap();
        assert!(reloaded.receipt("msgreq_4").is_some());
        assert!(reloaded.receipt("msgreq_0").is_none());
        assert!(
            reloaded
                .admission("msgreq_3")
                .is_some_and(|m| m.request_id == "msgreq_3"),
            "an admit without a receipt survives compaction"
        );
    }
}
