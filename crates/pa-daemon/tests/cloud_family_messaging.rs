//! Cloud family messaging substrate verifier: the journaled request/response
//! exchange over real durable logs, with the honesty contracts the design
//! pins — a receipt exists only after receiver admission, `Pending` is
//! durable-admitted-but-unanswered (never "queued"/"delivered"), a stalled
//! log fails the send, duplicates and restarts never re-deliver, and the
//! on-disk request envelope is the TS outbox record byte-for-byte.
//!
//! The delivery and submitter doubles implement the real seams; production
//! wiring (cloud registry attachment) is a later PR — nothing here fakes a
//! transport.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pa_daemon::cloud_family::{
    CloudFamilyDelivery, CloudFamilyRequestError, CloudFamilyRequestOutcome, CloudFamilyRequester,
    CloudFamilyResponder, FamilyRequestLog, FamilyResultLog, FamilyResultSubmitter, HandleOutcome,
    IncomingCloudMessage, ResolveOutcome,
};
use pa_types::daemon::cloud::{
    canonical_json, CloudAgentMessageDeliveryStatus, CloudAgentMessageReceipt, CloudFamilyCommand,
    CloudFamilyEventPayload, CloudFamilyRow,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const SHORT_WINDOW: Duration = Duration::from_millis(150);

struct TestDelivery {
    admitted: Mutex<Vec<IncomingCloudMessage>>,
    roster_calls: Mutex<Vec<String>>,
    errors_for: Mutex<HashMap<String, String>>,
    roster_error: Mutex<Option<String>>,
    rows: Vec<CloudFamilyRow>,
}

impl TestDelivery {
    fn new() -> Self {
        Self {
            admitted: Mutex::new(Vec::new()),
            roster_calls: Mutex::new(Vec::new()),
            errors_for: Mutex::new(HashMap::new()),
            roster_error: Mutex::new(None),
            rows: vec![CloudFamilyRow {
                id: "sess_local_1".to_string(),
                name: Some("local parent".to_string()),
                depth: 0,
                status: pa_types::daemon::cloud::CloudFamilyRowStatus::Running,
                parent_session_id: None,
                parent_session_path: None,
                session_path: None,
            }],
        }
    }

    fn deliveries(&self) -> usize {
        self.admitted.lock().unwrap().len()
    }
}

impl CloudFamilyDelivery for TestDelivery {
    async fn deliver_agent_message(
        &self,
        message: IncomingCloudMessage,
    ) -> Result<CloudAgentMessageReceipt, String> {
        self.admitted.lock().unwrap().push(message.clone());
        if let Some(error) = self.errors_for.lock().unwrap().get(&message.request_id) {
            return Err(error.clone());
        }
        Ok(CloudAgentMessageReceipt {
            id: format!("agentmsg_{}", message.request_id),
            delivery_status: CloudAgentMessageDeliveryStatus::Delivered,
            rest: {
                let mut map = serde_json::Map::new();
                map.insert("message".to_string(), json!(message.message));
                map.insert("deliveryMode".to_string(), json!("steer"));
                map
            },
        })
    }

    async fn family_roster(
        &self,
        for_remote_session_id: &str,
    ) -> Result<Vec<CloudFamilyRow>, String> {
        self.roster_calls
            .lock()
            .unwrap()
            .push(for_remote_session_id.to_string());
        if let Some(error) = self.roster_error.lock().unwrap().clone() {
            return Err(error);
        }
        Ok(self.rows.clone())
    }
}

struct TestSubmitter {
    submitted: Mutex<Vec<(String, CloudFamilyCommand)>>,
    fail: AtomicBool,
}

impl TestSubmitter {
    fn new() -> Self {
        Self {
            submitted: Mutex::new(Vec::new()),
            fail: AtomicBool::new(false),
        }
    }

    fn calls(&self) -> Vec<(String, CloudFamilyCommand)> {
        self.submitted.lock().unwrap().clone()
    }
}

impl FamilyResultSubmitter for TestSubmitter {
    async fn submit_family_result(
        &self,
        command_id: &str,
        command: &CloudFamilyCommand,
    ) -> Result<(), String> {
        if self.fail.load(Ordering::SeqCst) {
            return Err("tunnel detached".to_string());
        }
        self.submitted
            .lock()
            .unwrap()
            .push((command_id.to_string(), command.clone()));
        Ok(())
    }
}

fn open_request_log(dir: &std::path::Path, max_records: usize) -> FamilyRequestLog {
    FamilyRequestLog::open(dir, "sess_cloud_1", max_records).unwrap()
}

fn requester(log: FamilyRequestLog) -> Arc<CloudFamilyRequester> {
    Arc::new(CloudFamilyRequester::with_request_timeout(
        log,
        SHORT_WINDOW,
    ))
}

/// Poll the requester's durable log until one admitted request is
/// replayable — the append is synchronous, so one yielded task suffices in
/// practice; the loop keeps the test honest.
async fn wait_for_event(
    requester: &CloudFamilyRequester,
) -> pa_types::daemon::cloud::CloudFamilyEvent {
    for _ in 0..100 {
        if let Ok(events) = requester.events_after(0) {
            if let Some(event) = events.first() {
                return event.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("no request event was admitted to the durable log");
}

#[tokio::test]
async fn send_resolves_answered_only_after_receiver_admission() {
    let dir = tempfile::tempdir().unwrap();
    // A long window: without an answer the send stays open the whole test,
    // so the receipt below can only come from receiver admission.
    let requester = Arc::new(CloudFamilyRequester::with_request_timeout(
        open_request_log(dir.path(), 50),
        Duration::from_secs(10),
    ));

    let sender = Arc::clone(&requester);
    let task = tokio::spawn(async move {
        sender
            .send_agent_message("remote_child", "sibling-worker", "status update")
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!task.is_finished(), "no answer may claim the send early");

    let event = wait_for_event(&requester).await;
    let results = dir.path().join("results.ndjson");
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    let delivery = TestDelivery::new();
    let submitter = TestSubmitter::new();
    let outcome = responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    assert_eq!(outcome, HandleOutcome::Answered);
    assert_eq!(delivery.deliveries(), 1);
    assert_eq!(
        delivery.admitted.lock().unwrap()[0].message,
        "status update"
    );

    let (command_id, command) = submitter.calls()[0].clone();
    assert!(command_id.starts_with("msgres_msgreq_"));
    assert_eq!(command.journal_command_id(), command_id);

    // The journaled answer is the only path to a receipt.
    assert_eq!(requester.resolve_result(&command), ResolveOutcome::Resolved);
    let receipt = match task.await.unwrap().unwrap() {
        CloudFamilyRequestOutcome::Answered(receipt) => receipt,
        CloudFamilyRequestOutcome::Pending { request_id } => {
            panic!("expected Answered, got Pending ({request_id})")
        }
    };
    assert_eq!(
        receipt.delivery_status,
        CloudAgentMessageDeliveryStatus::Delivered
    );
    assert!(receipt.id.starts_with("agentmsg_msgreq_"));
}

#[tokio::test]
async fn unanswered_send_is_pending_and_a_late_answer_is_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));

    let outcome = requester
        .send_agent_message("remote_child", "sibling-worker", "hello")
        .await
        .unwrap();
    // Offline honesty: durably admitted, no journaled answer — Pending, and
    // the outcome carries no delivery claim at all.
    let request_id = match outcome {
        CloudFamilyRequestOutcome::Pending { request_id } => request_id,
        CloudFamilyRequestOutcome::Answered(receipt) => {
            panic!(
                "expected Pending, got Answered ({:?})",
                receipt.delivery_status
            )
        }
    };
    assert!(request_id.starts_with("msgreq_"));
    assert!(!requester.events_after(0).unwrap().is_empty());

    // A late answer for the expired request is a harmless failed dispatch.
    let late: CloudFamilyCommand = serde_json::from_value(json!({
        "kind": "agent_message_result",
        "requestId": request_id,
        "ok": true,
        "receipt": {"id": "agentmsg_late", "deliveryStatus": "delivered"},
    }))
    .unwrap();
    assert_eq!(
        requester.resolve_result(&late),
        ResolveOutcome::UnknownRequestId
    );
}

#[tokio::test]
async fn a_stalled_log_fails_the_send_honestly() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 1));

    let first = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move {
            sender
                .send_agent_message("remote_child", "sibling", "one")
                .await
        }
    });
    assert!(matches!(
        first.await.unwrap().unwrap(),
        CloudFamilyRequestOutcome::Pending { .. }
    ));

    let error = requester
        .send_agent_message("remote_child", "sibling", "two")
        .await
        .unwrap_err();
    assert_eq!(
        error,
        CloudFamilyRequestError::Stalled(
            "the guest event log is stalled; agent messaging is unavailable".to_string()
        )
    );
    // The second request was never admitted.
    assert_eq!(requester.events_after(0).unwrap().len(), 1);
}

#[tokio::test]
async fn duplicate_replay_never_redelivers() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));
    let task = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move {
            sender
                .send_agent_message("remote_child", "sibling", "hello")
                .await
        }
    });
    let event = wait_for_event(&requester).await;

    let results = dir.path().join("results.ndjson");
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    let delivery = TestDelivery::new();
    let submitter = TestSubmitter::new();

    let first = responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    assert_eq!(first, HandleOutcome::Answered);

    // The same event replaying (reconnect, unacked tail) is a duplicate: no
    // re-delivery, the same journaled answer re-submitted.
    let second = responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    assert_eq!(second, HandleOutcome::DuplicateResubmitted);
    assert_eq!(delivery.deliveries(), 1);
    let calls = submitter.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, calls[1].0);

    requester.resolve_result(&calls[0].1.clone());
    match task.await.unwrap().unwrap() {
        CloudFamilyRequestOutcome::Answered(_) => {}
        CloudFamilyRequestOutcome::Pending { request_id } => {
            panic!("expected Answered, got Pending ({request_id})")
        }
    }
}

#[tokio::test]
async fn responder_restart_replays_without_redelivery() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));
    let task = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move {
            sender
                .send_agent_message("remote_child", "sibling", "hello")
                .await
        }
    });
    let event = wait_for_event(&requester).await;

    let results = dir.path().join("results.ndjson");
    let delivery = TestDelivery::new();
    let submitter = TestSubmitter::new();
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    // The durable answer outlives the responder.
    drop(responder);

    let restarted = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    let outcome = restarted
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    assert_eq!(outcome, HandleOutcome::DuplicateResubmitted);
    assert_eq!(delivery.deliveries(), 1);
    assert_eq!(submitter.calls().len(), 2);

    let (_, command) = submitter.calls()[0].clone();
    requester.resolve_result(&command);
    match task.await.unwrap().unwrap() {
        CloudFamilyRequestOutcome::Answered(_) => {}
        CloudFamilyRequestOutcome::Pending { request_id } => {
            panic!("expected Answered, got Pending ({request_id})")
        }
    }
}

#[tokio::test]
async fn submit_failure_is_honest_and_a_replay_resubmits_the_durable_answer() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));
    let task = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move {
            sender
                .send_agent_message("remote_child", "sibling", "hello")
                .await
        }
    });
    let event = wait_for_event(&requester).await;

    let results = dir.path().join("results.ndjson");
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    let delivery = TestDelivery::new();
    let submitter = TestSubmitter::new();
    // The tunnel is down for the submit leg: no answer may be claimed.
    submitter.fail.store(true, Ordering::SeqCst);
    let outcome = responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        HandleOutcome::SubmitFailed("tunnel detached".to_string())
    );
    assert_eq!(delivery.deliveries(), 1);
    assert!(submitter.calls().is_empty());

    // The tunnel returns; the replay re-submits the durable answer without
    // re-delivering.
    submitter.fail.store(false, Ordering::SeqCst);
    let outcome = responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    assert_eq!(outcome, HandleOutcome::DuplicateResubmitted);
    assert_eq!(delivery.deliveries(), 1);

    let (_, command) = submitter.calls()[0].clone();
    requester.resolve_result(&command);
    match task.await.unwrap().unwrap() {
        CloudFamilyRequestOutcome::Answered(_) => {}
        CloudFamilyRequestOutcome::Pending { request_id } => {
            panic!("expected Answered, got Pending ({request_id})")
        }
    }
}

#[tokio::test]
async fn roster_request_answers_rows_and_degrades_to_empty_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));
    let task = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move { sender.request_family_roster("remote_child").await }
    });
    let event = wait_for_event(&requester).await;
    assert!(event.request_id().starts_with("famreq_"));

    let results = dir.path().join("results.ndjson");
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    let delivery = TestDelivery::new();
    *delivery.roster_error.lock().unwrap() = Some("roster build failed".to_string());
    let submitter = TestSubmitter::new();
    responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();

    let (command_id, command) = submitter.calls()[0].clone();
    // The roster answer degrades to empty rows, exactly like the TS handler.
    assert!(command_id.starts_with("fam_famreq_"));
    requester.resolve_result(&command);
    match task.await.unwrap().unwrap() {
        CloudFamilyRequestOutcome::Answered(rows) => assert!(rows.is_empty()),
        CloudFamilyRequestOutcome::Pending { request_id } => {
            panic!("expected Answered, got Pending ({request_id})")
        }
    }
}

#[tokio::test]
async fn a_roster_answer_carries_the_local_rows() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));
    let task = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move { sender.request_family_roster("remote_child").await }
    });
    let event = wait_for_event(&requester).await;

    let results = dir.path().join("results.ndjson");
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    let delivery = TestDelivery::new();
    let submitter = TestSubmitter::new();
    responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();
    assert_eq!(
        delivery.roster_calls.lock().unwrap().as_slice(),
        ["remote_child"]
    );

    let (_, command) = submitter.calls()[0].clone();
    requester.resolve_result(&command);
    match task.await.unwrap().unwrap() {
        CloudFamilyRequestOutcome::Answered(rows) => {
            assert_eq!(rows, delivery.rows);
        }
        CloudFamilyRequestOutcome::Pending { request_id } => {
            panic!("expected Answered, got Pending ({request_id})")
        }
    }
}

#[tokio::test]
async fn release_rejects_pending_requests() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));
    let task = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move {
            sender
                .send_agent_message("remote_child", "sibling", "hello")
                .await
        }
    });
    wait_for_event(&requester).await;
    requester.release();
    assert_eq!(
        task.await.unwrap().unwrap_err(),
        CloudFamilyRequestError::Released
    );
}

#[tokio::test]
async fn delivery_errors_slice_to_2000_utf16_units() {
    let dir = tempfile::tempdir().unwrap();
    let requester = requester(open_request_log(dir.path(), 50));
    let task = tokio::spawn({
        let sender = Arc::clone(&requester);
        async move {
            sender
                .send_agent_message("remote_child", "sibling", "hello")
                .await
        }
    });
    let event = wait_for_event(&requester).await;

    let results = dir.path().join("results.ndjson");
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&results).unwrap());
    let delivery = TestDelivery::new();
    delivery.errors_for.lock().unwrap().insert(
        event.request_id().to_string(),
        "\u{1F600}".repeat(1501), // 3002 UTF-16 units
    );
    let submitter = TestSubmitter::new();
    responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .unwrap();

    let (_, command) = submitter.calls()[0].clone();
    let error = match &command.payload {
        pa_types::daemon::cloud::CloudFamilyCommandPayload::AgentMessageResult {
            ok: false,
            error: Some(error),
            ..
        } => error.clone(),
        other => panic!("expected an error result, got {other:?}"),
    };
    assert_eq!(error.encode_utf16().count(), 2000);

    requester.resolve_result(&command);
    match task.await.unwrap() {
        Err(CloudFamilyRequestError::Rejected(rejection)) => {
            assert_eq!(rejection.encode_utf16().count(), 2000);
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[tokio::test]
async fn request_log_repairs_a_crash_truncated_tail() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = open_request_log(dir.path(), 50);
        log.append(agent_message_event_payload("one")).unwrap();
        log.append(agent_message_event_payload("two")).unwrap();
    }
    // Simulate a crash mid-append: chop the final record.
    let path = dir.path().join("outbox-events.ndjson");
    let content = std::fs::read(&path).unwrap();
    std::fs::write(&path, &content[..content.len() - 10]).unwrap();

    let mut log = open_request_log(dir.path(), 50);
    assert_eq!(log.len(), 1);
    assert_eq!(log.events_after(0).unwrap().len(), 1);
    let third = log.append(agent_message_event_payload("three")).unwrap();
    assert_eq!(third.sequence, 2);

    // The repaired file is whole again: every line ends in a newline.
    let repaired = std::fs::read_to_string(&path).unwrap();
    assert!(repaired.ends_with('\n'));
    assert_eq!(repaired.lines().count(), 2);
}

#[tokio::test]
async fn request_log_replays_after_reopen_and_bounds_cursors() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = open_request_log(dir.path(), 50);
        log.append(agent_message_event_payload("one")).unwrap();
        log.append(agent_message_event_payload("two")).unwrap();
    }
    let log = open_request_log(dir.path(), 50);
    assert_eq!(log.events_after(0).unwrap().len(), 2);
    assert_eq!(log.events_after(1).unwrap().len(), 1);
    assert_eq!(log.events_after(1).unwrap()[0].sequence, 2);
    assert_eq!(log.events_after(2).unwrap(), Vec::new());
    assert_eq!(
        log.events_after(3).unwrap_err(),
        "Cloud cursor is beyond the event tail"
    );
}

#[tokio::test]
async fn request_envelope_is_the_ts_outbox_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = open_request_log(dir.path(), 50);
    let event = log
        .append(CloudFamilyEventPayload::AgentMessageRequest {
            request_id: "msgreq_fixed".to_string(),
            from_remote_session_id: "remote_child".to_string(),
            target_selector: "sibling-worker".to_string(),
            message: "status update".to_string(),
        })
        .unwrap();
    drop(log);

    let path = dir.path().join("outbox-events.ndjson");
    let line = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();
    let envelope: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(envelope["generation"], json!(1));
    let event_value = &envelope["event"];
    // Exact TS wire field names on the family event.
    assert_eq!(event_value["kind"], json!("agent_message_request"));
    assert_eq!(event_value["sequence"], json!(event.sequence));
    assert_eq!(event_value["requestId"], json!("msgreq_fixed"));
    assert_eq!(event_value["fromRemoteSessionId"], json!("remote_child"));
    assert_eq!(event_value["targetSelector"], json!("sibling-worker"));
    assert_eq!(event_value["message"], json!("status update"));
    assert!(event_value["recordedAt"]
        .as_str()
        .is_some_and(|at| !at.is_empty()));

    // eventId is the TS digest: sha256 over the canonical {sessionId,
    // generation, event} JSON, `evt_` prefixed.
    let canonical = canonical_json(&json!({
        "sessionId": "sess_cloud_1",
        "generation": 1,
        "event": event_value,
    }))
    .unwrap();
    let digest =
        Sha256::digest(canonical.as_bytes())
            .iter()
            .fold(String::new(), |mut key, byte| {
                use std::fmt::Write;
                write!(key, "{byte:02x}").expect("write to String");
                key
            });
    assert_eq!(envelope["eventId"], json!(format!("evt_{digest}")));
}

fn agent_message_event_payload(message: &str) -> CloudFamilyEventPayload {
    CloudFamilyEventPayload::AgentMessageRequest {
        request_id: format!("msgreq_{message}"),
        from_remote_session_id: "remote_child".to_string(),
        target_selector: "sibling".to_string(),
        message: message.to_string(),
    }
}
