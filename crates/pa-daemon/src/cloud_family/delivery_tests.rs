//! The production cross-boundary delivery seam over a REAL in-process
//! supervisor: real registry, real roster store, real worker socket,
//! real handshake, real route — and the receiver inbox keyed by request
//! id. The crash-gap protocol is exercised by truncating the seam
//! journal (the receipt append that never landed) and by re-opening the
//! seam (the process that died).
use std::sync::Arc;
use std::time::Duration;

use pa_types::daemon::cloud::CloudFamilyRowStatus;
use serde_json::{json, Map, Value};

use crate::registry::ResidentWorker;
use crate::worker::{Worker, WorkerConfig};

use super::{IncomingCloudMessage, LocalFamilyDelivery};
use crate::cloud_family::{
    Admission, AgentMessageLookup, CloudFamilyDelivery, CloudFamilyResponder, CloudInboxLog,
    FamilyResultLog, FamilyResultSubmitter, HandleOutcome,
};
use crate::supervisor::{Supervisor, SupervisorOptions};
use pa_types::daemon::cloud::{
    CloudFamilyCommand, CloudFamilyCommandPayload, CloudFamilyEvent, CloudFamilyEventPayload,
};

/// One seeded roster row: the family-identity shape the roster pushes
/// carry (the slim summary).
fn roster_summary(
    active_id: &str,
    session_id: &str,
    name: &str,
    rlm_depth: u64,
    parent_session_path: Option<&str>,
) -> Value {
    let mut summary = json!({
        "id": active_id,
        "activeSessionId": active_id,
        "sessionId": session_id,
        "sessionName": name,
        "runtimeKind": "top-level",
        "rlmDepth": rlm_depth,
        "activity": "idle",
        "isSessionActive": false,
        "cwd": "/tmp",
        "isStreaming": false,
    });
    if let Some(parent) = parent_session_path {
        summary["parentSessionPath"] = json!(parent);
    }
    summary
}

/// The remote source row: a depth-1 child of `<dir>/parent.jsonl` (its
/// siblings share that parent edge).
fn source_summary(parent_file: &str) -> Value {
    roster_summary(
        "remote-cloud-1",
        "sess-cloud-1",
        "cloud kid",
        1,
        Some(parent_file),
    )
}

/// One real in-process worker: a live socket serve loop plus a created
/// session, registered with the supervisor as a resident over a
/// synthetic descriptor carrying the real socket path and token (the
/// messaging.rs test resident shape).
struct Harness {
    supervisor: Arc<Supervisor>,
    worker: Arc<Worker>,
    dir: std::path::PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn worker_config(dir: &std::path::Path, active_id: &str) -> WorkerConfig {
    WorkerConfig {
        socket_path: dir.join(format!("{active_id}.sock")),
        supervisor_socket_path: std::path::PathBuf::new(),
        token: format!("token-{active_id}"),
        worker_instance_id: String::new(),
        active_session_id: active_id.to_string(),
        agent_dir: dir.join("agent"),
        recovery_journal_path: dir.join(format!("{active_id}-recovery.jsonl")),
        telemetry_disabled: None,
        script: Some(json!({ "responses": ["ack"] })),
    }
}

fn resident_for(config: &WorkerConfig) -> Arc<ResidentWorker> {
    use pa_types::daemon::{
        DaemonWorkerDescriptor, DaemonWorkerLifecycle, DurableDaemonCreateCommand,
    };
    ResidentWorker::new(
        config.active_session_id.clone(),
        DaemonWorkerDescriptor {
            version: 2,
            worker_id: config.active_session_id.clone(),
            pid: 1,
            process_start_id: None,
            socket_path: config.socket_path.to_string_lossy().to_string(),
            recovery_journal_path: config.recovery_journal_path.to_string_lossy().to_string(),
            orphan_process_journal_path: None,
            supervisor_socket_path: "/s.sock".to_string(),
            authentication_token: config.token.clone(),
            worker_instance_id: None,
            root_active_session_id: config.active_session_id.clone(),
            owner_client_id: None,
            root_session_id: None,
            session_file: None,
            session_dir: None,
            telemetry_disabled: None,
            created_at: "t".to_string(),
            updated_at: "t".to_string(),
            lifecycle: DaemonWorkerLifecycle::Ready,
            create_command: DurableDaemonCreateCommand {
                session_path: None,
                no_session: None,
                rest: Map::default(),
            },
            consecutive_failures: 0,
            stop_requested_at: None,
            archive_on_stop: None,
            last_failure_at: None,
            last_error: None,
            rest: Map::default(),
        },
        std::path::PathBuf::from("/d.json"),
    )
}

/// The seeded target row: a depth-1 child of the same parent (the
/// source's sibling) — the reach the default harness exercises.
async fn harness() -> Harness {
    harness_with_target(1, None).await
}

/// The harness with a custom target row (the reach matrix): the worker
/// is always "target-root-1"; `target_depth` and `target_parent` decide
/// its family identity. `target_parent` `None` inherits the source's
/// parent file (the sibling case).
async fn harness_with_target(target_depth: u64, target_parent: Option<&str>) -> Harness {
    // A short path: the worker socket must fit the AF_UNIX 107-byte
    // limit (the macOS $TMPDIR prefix is already long).
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::create_dir_all(dir.path()).expect("temp dir");
    let dir = dir.keep();
    let supervisor = Arc::new(
        Supervisor::new(SupervisorOptions {
            socket_path: dir.join("supervisor.sock"),
            agent_dir: dir.join("agent"),
        })
        .expect("supervisor"),
    );
    let parent_file = dir.join("parent.jsonl");
    let parent_file = parent_file.to_string_lossy().to_string();
    let target_parent = target_parent.map_or_else(|| parent_file.clone(), str::to_string);
    {
        let mut roster = supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roster.write_summary(source_summary(&parent_file), None, None);
    }
    // The target worker: a real in-process serve loop.
    let config = worker_config(&dir, "target-root-1");
    let worker = Arc::new(Worker::new(config.clone(), None));
    let served = Arc::clone(&worker);
    tokio::spawn(async move {
        let _ = served.serve().await;
    });
    // The created session (before routing anything at it).
    for _ in 0..100 {
        let created = worker
            .dispatch(
                "create",
                &json!({ "noSession": true, "cwd": "/tmp", "name": "target" }),
            )
            .await;
        if created.success {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A held input pause parks the turn runner (its pickup gate), so
    // deliveries park in the lane and the visibility assertions are
    // deterministic (an unparked runner would drain the lane the moment
    // the delivery wakes it).
    let paused = worker
        .dispatch(
            "acquire_session_input_pause",
            &json!({ "clientId": "seam-test", "leaseKey": "seam-pause" }),
        )
        .await;
    assert!(paused.success, "the input pause must hold: {paused:?}");
    // Wait for the serve loop to bind the socket (the dial below needs
    // a bound listener).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !config.socket_path.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the worker socket never bound: {}",
            config.socket_path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The resident + the real handshake.
    let resident = resident_for(&config);
    supervisor.registry.insert(Arc::clone(&resident)).await;
    let target_summary = roster_summary(
        "target-root-1",
        "sess-target-1",
        "target",
        target_depth,
        Some(&target_parent),
    );
    {
        let mut roster = supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roster.write_summary(target_summary, Some(&config.active_session_id), None);
    }
    supervisor
        .connect_worker(
            &resident,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect("connect the worker");
    Harness {
        supervisor,
        worker,
        dir,
    }
}

fn cloud_message(request_id: &str, target_selector: &str) -> IncomingCloudMessage {
    IncomingCloudMessage {
        request_id: request_id.to_string(),
        from_remote_session_id: "remote-cloud-1".to_string(),
        target_selector: target_selector.to_string(),
        message: "cross the boundary".to_string(),
    }
}

fn seam(harness: &Harness) -> LocalFamilyDelivery {
    LocalFamilyDelivery::new(
        Arc::clone(&harness.supervisor),
        harness.dir.join("cloud-inbox.jsonl"),
    )
    .expect("the delivery seam")
}

/// The count of visible steering-lane items carrying the test message in
/// the target worker.
fn visible_message_count(harness: &Harness) -> usize {
    let core = harness.worker.core.lock().unwrap();
    core.steering
        .iter()
        .filter(|item| item.message.contains("cross the boundary"))
        .count()
}

/// Drop the last line of the seam journal (the crash simulation: the
/// final append never landed).
fn truncate_journal_tail(harness: &Harness) {
    let path = harness.dir.join("cloud-inbox.jsonl");
    let content = std::fs::read_to_string(&path).expect("the seam journal");
    let mut lines: Vec<&str> = content.lines().collect();
    lines.pop();
    std::fs::write(&path, format!("{}\n", lines.join("\n"))).expect("truncate");
}

/// The full chain: deliver, receipt, one visible lane item; the
/// duplicate answers the recorded receipt with no second visible
/// message.
#[tokio::test]
async fn delivers_into_the_real_worker_and_answers_duplicates_idempotently() {
    let harness = harness().await;
    let delivery = seam(&harness);
    let receipt = delivery
        .deliver_agent_message(cloud_message("msgreq_1", "target-root-1"))
        .await
        .expect("delivery");
    assert!(receipt.id.starts_with("agentmsg_"), "receipt: {receipt:?}");
    assert_eq!(receipt.rest["deliveryMode"], json!("steer"));
    assert_eq!(visible_message_count(&harness), 1);
    // The idempotent duplicate: the same receipt, no second message.
    let duplicate = delivery
        .deliver_agent_message(cloud_message("msgreq_1", "target-root-1"))
        .await
        .expect("duplicate delivery");
    assert_eq!(
        duplicate, receipt,
        "the duplicate answers the recorded receipt"
    );
    assert_eq!(
        visible_message_count(&harness),
        1,
        "exactly one visible message"
    );
}

/// The lookup answers the recorded receipt; a request this receiver
/// never admitted answers `Unknown` (the honest limit the responder
/// surfaces as uncertain).
#[tokio::test]
async fn lookup_answers_the_recorded_receipt() {
    let harness = harness().await;
    let delivery = seam(&harness);
    assert_eq!(
        delivery.lookup_agent_message("msgreq_never").await,
        AgentMessageLookup::Unknown,
        "a request this receiver never admitted answers Unknown"
    );
    let receipt = delivery
        .deliver_agent_message(cloud_message("msgreq_2", "target-root-1"))
        .await
        .expect("delivery");
    assert_eq!(
        delivery.lookup_agent_message("msgreq_2").await,
        AgentMessageLookup::Admitted(receipt)
    );
}

/// Crash AFTER the receiver admitted (the worker answered) but BEFORE
/// the seam recorded the receipt: the replay re-drives, the receiver
/// inbox's durable key answers the RECORDED receipt, and no second
/// visible message appears.
#[tokio::test]
async fn crash_after_receiver_admission_reconciles_without_a_duplicate() {
    let harness = harness().await;
    let first = seam(&harness);
    let delivered = first
        .deliver_agent_message(cloud_message("msgreq_3", "target-root-1"))
        .await
        .expect("delivery");
    // The seam died after the worker answered, before the receipt
    // record landed (the journal's last line is the receipt record).
    truncate_journal_tail(&harness);
    let seam_process_died = seam(&harness);
    match seam_process_died.lookup_agent_message("msgreq_3").await {
        AgentMessageLookup::Admitted(reconciled) => assert_eq!(
            reconciled, delivered,
            "the reconciled receipt is the receiver's recorded one"
        ),
        AgentMessageLookup::Unknown | AgentMessageLookup::Uncertain => {
            panic!("the lookup must reconcile, answered the non-admitted/unresolvable arm")
        }
    }
    assert_eq!(
        visible_message_count(&harness),
        1,
        "the crash after admission must not duplicate the visible message"
    );
}

/// Crash BEFORE the receiver admitted: the seam journal holds the
/// admission, the delivery never happened — the lookup re-drives, the
/// receiver delivers exactly once.
#[tokio::test]
async fn crash_before_receiver_admission_re_drives_exactly_once() {
    let harness = harness().await;
    // The seam admitted durably, then the process died before the
    // delivery: the journal holds the dead process's admission record.
    {
        let mut inbox = CloudInboxLog::open(&harness.dir.join("cloud-inbox.jsonl")).unwrap();
        assert_eq!(
            inbox
                .admit(&cloud_message("msgreq_4", "target-root-1"))
                .unwrap(),
            Admission::First
        );
    }
    let delivery = seam(&harness);
    match delivery.lookup_agent_message("msgreq_4").await {
        AgentMessageLookup::Admitted(receipt) => {
            assert!(
                receipt.id.starts_with("agentmsg_"),
                "the re-drive delivered: {receipt:?}"
            );
        }
        AgentMessageLookup::Unknown | AgentMessageLookup::Uncertain => {
            panic!("the re-drive must answer Admitted, answered the non-admitted/unresolvable arm")
        }
    }
    assert_eq!(
        visible_message_count(&harness),
        1,
        "the re-drive delivers exactly once"
    );
}

/// The refusal matrix over the real registry and roster: an unknown
/// source, an unknown target — and the reach refusal for a target
/// outside the source's nuclear family (nothing becomes visible).
#[tokio::test]
async fn refuses_with_the_ts_errors() {
    let harness = harness().await;
    let delivery = seam(&harness);
    // Unknown source (TS: `Unknown cloud message source: <id>`).
    let mut unknown_source = cloud_message("msgreq_s", "target-root-1");
    unknown_source.from_remote_session_id = "remote-ghost".to_string();
    assert_eq!(
        delivery
            .deliver_agent_message(unknown_source)
            .await
            .unwrap_err(),
        "Unknown cloud message source: remote-ghost"
    );
    // Unknown target (TS findWorker's error).
    assert_eq!(
        delivery
            .deliver_agent_message(cloud_message("msgreq_t", "no-such-session"))
            .await
            .unwrap_err(),
        "Unknown active session: no-such-session"
    );
    // Self-addressed: the source row and the target row are the same
    // roster row, so the reach assert refuses first (TS parity: the
    // cloud arm asserts reach before its self-target guard, and
    // `agentFamilyRelationship(self, self)` answers none).
    let mut self_addressed = cloud_message("msgreq_u", "target-root-1");
    self_addressed.from_remote_session_id = "target-root-1".to_string();
    assert_eq!(
        delivery
            .deliver_agent_message(self_addressed)
            .await
            .unwrap_err(),
        crate::cloud_family::family::AGENT_FAMILY_REACH_ERROR
    );
    assert_eq!(visible_message_count(&harness), 0, "nothing became visible");
}

/// The reach refusal: a target outside the source's nuclear family (a
/// grandchild under a different parent) is refused with the TS reach
/// error.
#[tokio::test]
async fn refuses_reach_outside_the_nuclear_family() {
    let harness = harness_with_target(2, Some("/other/family/child.jsonl")).await;
    let delivery = seam(&harness);
    assert_eq!(
        delivery
            .deliver_agent_message(cloud_message("msgreq_r", "target-root-1"))
            .await
            .unwrap_err(),
        crate::cloud_family::family::AGENT_FAMILY_REACH_ERROR
    );
    assert_eq!(visible_message_count(&harness), 0, "nothing became visible");
}

/// The family-roster rows over the real roster: the requesting session's
/// own row, its parent, and its siblings; an unknown requester answers
/// empty rows (the TS degradation).
#[tokio::test]
async fn family_roster_rows_over_the_real_roster() {
    let harness = harness().await;
    // A second sibling under the same parent.
    let parent_file = harness.dir.join("parent.jsonl");
    let sibling = roster_summary(
        "remote-cloud-2",
        "sess-cloud-2",
        "cloud sibling",
        1,
        Some(parent_file.to_string_lossy().as_ref()),
    );
    {
        let mut roster = harness
            .supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roster.write_summary(sibling, None, None);
    }
    let delivery = seam(&harness);
    let rows = delivery
        .family_roster("remote-cloud-1")
        .await
        .expect("rows");
    let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
    assert!(ids.contains(&"sess-cloud-1"), "the self row: {ids:?}");
    assert!(ids.contains(&"sess-cloud-2"), "the sibling row: {ids:?}");
    let sibling_row = rows
        .iter()
        .find(|row| row.id == "sess-cloud-2")
        .expect("the sibling row");
    assert_eq!(sibling_row.depth, 1);
    assert_eq!(sibling_row.status, CloudFamilyRowStatus::Idle);
    assert!(
        delivery
            .family_roster("remote-ghost")
            .await
            .expect("rows")
            .is_empty(),
        "an unknown requester answers empty rows"
    );
}

/// A recording submitter (the tunnel seam double — the journaled-answer
/// submission is the tunnel attachment's to wire, like the substrate's
/// own tests).
struct RecordingSubmitter {
    submitted: std::sync::Mutex<Vec<(String, CloudFamilyCommand)>>,
}

impl FamilyResultSubmitter for RecordingSubmitter {
    async fn submit_family_result(
        &self,
        command_id: &str,
        command: &CloudFamilyCommand,
    ) -> Result<(), String> {
        self.submitted
            .lock()
            .unwrap()
            .push((command_id.to_string(), command.clone()));
        Ok(())
    }
}

fn agent_message_event(request_id: &str, target_selector: &str) -> CloudFamilyEvent {
    CloudFamilyEvent {
        sequence: 1,
        recorded_at: "2026-09-29T00:00:00Z".to_string(),
        payload: CloudFamilyEventPayload::AgentMessageRequest {
            request_id: request_id.to_string(),
            from_remote_session_id: "remote-cloud-1".to_string(),
            target_selector: target_selector.to_string(),
            message: "cross the boundary".to_string(),
        },
    }
}

/// The full crash-gap chain at the responder level, crash BEFORE the
/// receiver admitted: the responder's journal holds only the admission
/// and the seam journal holds nothing (no delivery was ever attempted).
/// The replayed event surfaces `Uncertain` — the substrate's honest
/// never-re-deliver contract — and the wiring reconcile
/// (`reconcile_uncertain`) closes the gap: the lookup answers Unknown
/// (provably never attempted), the re-drive delivers exactly once (the
/// receiver inbox admits it), the answer is journaled, and the NEXT
/// replay re-submits it without any delivery.
#[tokio::test]
async fn responder_crash_before_receiver_admission_reconciles_by_re_driving() {
    let harness = harness().await;
    let delivery = seam(&harness);
    // The dead responder's journal: admitted, never answered.
    let responder_path = harness.dir.join("family-results.jsonl");
    {
        let mut results = FamilyResultLog::open(&responder_path).unwrap();
        results.admit("msgreq_c1").unwrap();
    }
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&responder_path).unwrap());
    let submitter = RecordingSubmitter {
        submitted: std::sync::Mutex::new(Vec::new()),
    };
    let event = agent_message_event("msgreq_c1", "target-root-1");
    // The replay surfaces Uncertain: the substrate never re-delivers an
    // admitted-without-answer request.
    let outcome = responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .expect("the replayed event");
    assert_eq!(
        outcome,
        HandleOutcome::Uncertain,
        "the substrate leaves it to the wiring"
    );
    assert_eq!(
        visible_message_count(&harness),
        0,
        "nothing was delivered yet"
    );
    // The wiring reconcile: the re-drive delivers exactly once.
    let outcome = crate::cloud_family::delivery::reconcile_uncertain(
        &responder,
        &delivery,
        std::slice::from_ref(&event),
    )
    .await;
    assert_eq!(
        outcome.reconciled,
        vec!["msgreq_c1".to_string()],
        "the re-drive reconciled the request"
    );
    assert!(outcome.unanswered.is_empty(), "nothing stayed uncertain");
    assert_eq!(
        visible_message_count(&harness),
        1,
        "exactly one visible message"
    );
    // The next replay: the journaled answer re-submits, no delivery.
    let replayed = responder
        .handle_event(&event, &delivery, &submitter)
        .await
        .expect("the second replay");
    assert_eq!(replayed, HandleOutcome::DuplicateResubmitted);
    assert_eq!(
        visible_message_count(&harness),
        1,
        "no duplicate visible message"
    );
    let submitted = submitter.submitted.lock().unwrap().clone();
    let (command_id, command) = submitted.first().expect("the answer was submitted");
    assert_eq!(command_id, "msgres_msgreq_c1");
    match &command.payload {
        CloudFamilyCommandPayload::AgentMessageResult {
            request_id,
            ok,
            receipt,
            ..
        } => {
            assert_eq!(request_id, "msgreq_c1");
            assert!(ok, "the reconciled answer carries the receipt");
            let receipt = receipt.as_ref().expect("the receipt");
            assert!(
                receipt.id.starts_with("agentmsg_"),
                "the re-drive delivered: {receipt:?}"
            );
        }
        CloudFamilyCommandPayload::FamilyRosterResult { .. } => {
            panic!("the answer must be an agent_message_result")
        }
    }
}

/// The crash AFTER the receiver admitted: the responder journal holds
/// only the admission, the seam journal holds the recorded receipt (the
/// worker answered before the crash) — the replay reconciles through the
/// LOOKUP without re-delivering, the answer carries the recorded
/// receipt, and the visible message count stays one.
#[tokio::test]
async fn responder_crash_after_receiver_admission_reconciles_through_the_lookup() {
    let harness = harness().await;
    let delivery = seam(&harness);
    // The first pass: the delivery completed (the seam journal holds the
    // receipt), but the responder crashed between its admission and its
    // answer record.
    let responder_path = harness.dir.join("family-results.jsonl");
    {
        let mut results = FamilyResultLog::open(&responder_path).unwrap();
        results.admit("msgreq_c2").unwrap();
    }
    // The seam's journal gets the receipt the dead pass recorded: deliver
    // through the seam (the delivery the dead responder drove).
    let recorded = delivery
        .deliver_agent_message(cloud_message("msgreq_c2", "target-root-1"))
        .await
        .expect("the delivery the dead responder drove");
    // The replayed responder over the crashed journal.
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&responder_path).unwrap());
    let submitter = RecordingSubmitter {
        submitted: std::sync::Mutex::new(Vec::new()),
    };
    let outcome = responder
        .handle_event(
            &agent_message_event("msgreq_c2", "target-root-1"),
            &delivery,
            &submitter,
        )
        .await
        .expect("the replayed event");
    assert_eq!(outcome, HandleOutcome::Reconciled);
    assert_eq!(
        visible_message_count(&harness),
        1,
        "no duplicate visible message"
    );
    let submitted = submitter.submitted.lock().unwrap().clone();
    let (_, command) = submitted.first().expect("the answer was submitted");
    match &command.payload {
        CloudFamilyCommandPayload::AgentMessageResult { receipt, .. } => {
            let receipt = receipt.as_ref().expect("the receipt");
            assert_eq!(
                *receipt, recorded,
                "the reconciled answer carries the receiver's recorded receipt"
            );
        }
        CloudFamilyCommandPayload::FamilyRosterResult { .. } => {
            panic!("the answer must be an agent_message_result")
        }
    }
}

/// The replay with a journaled answer re-submits it (the substrate's
/// duplicate contract) through the production seam: no delivery at all.
#[tokio::test]
async fn responder_duplicate_with_a_journaled_answer_resubmits_without_delivery() {
    let harness = harness().await;
    let delivery = seam(&harness);
    let responder_path = harness.dir.join("family-results.jsonl");
    let submitter = RecordingSubmitter {
        submitted: std::sync::Mutex::new(Vec::new()),
    };
    // A complete first pass over the production seam.
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&responder_path).unwrap());
    let answered = responder
        .handle_event(
            &agent_message_event("msgreq_c3", "target-root-1"),
            &delivery,
            &submitter,
        )
        .await
        .expect("the first pass");
    assert_eq!(answered, HandleOutcome::Answered);
    assert_eq!(visible_message_count(&harness), 1);
    // The duplicate: the same event replayed over the journaled answer.
    let replayed = responder
        .handle_event(
            &agent_message_event("msgreq_c3", "target-root-1"),
            &delivery,
            &submitter,
        )
        .await
        .expect("the duplicate");
    assert_eq!(replayed, HandleOutcome::DuplicateResubmitted);
    assert_eq!(
        visible_message_count(&harness),
        1,
        "no second visible message"
    );
    let submitted = submitter.submitted.lock().unwrap().clone();
    assert_eq!(
        submitted.len(),
        2,
        "both passes submitted the same journaled answer"
    );
    assert_eq!(
        submitted[0].1, submitted[1].1,
        "the duplicate re-submits the same answer"
    );
}

/// Finding-4 contract: a durably-admitted request whose outcome the
/// re-drive cannot resolve (the target's roster row is gone) answers
/// `Uncertain` — never `Unknown` (which would read as never-attempted)
/// and never a durable negative. The reconcile leaves it unanswered,
/// nothing is recorded, and the recovery — once the target is reachable
/// again — resolves to the receiver's recorded receipt with no
/// duplicate visible message.
#[tokio::test]
async fn an_unresolvable_admitted_request_stays_uncertain_never_negative() {
    let harness = harness().await;
    let delivery = seam(&harness);
    // The delivery completed (the worker holds the inbox key and the
    // visible item), then the crash: the seam's receipt record never
    // landed.
    let recorded = delivery
        .deliver_agent_message(cloud_message("msgreq_u1", "target-root-1"))
        .await
        .expect("the delivery the dead pass drove")
        .clone();
    truncate_journal_tail(&harness);
    let seam_process_died = seam(&harness);
    // The target disappears from the roster: the re-drive cannot resolve.
    {
        let mut roster = harness
            .supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roster.delete("sess-target-1");
    }
    assert_eq!(
        seam_process_died.lookup_agent_message("msgreq_u1").await,
        AgentMessageLookup::Uncertain,
        "an admitted-but-unresolvable request answers Uncertain, never Unknown"
    );
    // The responder holds the same crash-gap state: the reconcile must
    // leave it uncertain and NEVER record a negative answer for a
    // message the receiver already holds.
    let responder_path = harness.dir.join("family-results.jsonl");
    {
        let mut results = FamilyResultLog::open(&responder_path).unwrap();
        results.admit("msgreq_u1").unwrap();
    }
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&responder_path).unwrap());
    let event = agent_message_event("msgreq_u1", "target-root-1");
    let submitter = RecordingSubmitter {
        submitted: std::sync::Mutex::new(Vec::new()),
    };
    // The replay surfaces Uncertain; the wiring reconcile cannot resolve
    // it either — it stays unanswered, nothing recorded.
    let outcome = responder
        .handle_event(&event, &seam_process_died, &submitter)
        .await
        .expect("the replay");
    assert_eq!(outcome, HandleOutcome::Uncertain);
    let reconcile = crate::cloud_family::delivery::reconcile_uncertain(
        &responder,
        &seam_process_died,
        std::slice::from_ref(&event),
    )
    .await;
    assert!(
        reconcile.reconciled.is_empty(),
        "an unresolvable request is never reconciled"
    );
    assert_eq!(reconcile.unanswered, vec!["msgreq_u1".to_string()]);
    assert!(
        submitter.submitted.lock().unwrap().is_empty(),
        "no answer was submitted for the uncertain request"
    );
    assert_eq!(responder.uncertain(), vec!["msgreq_u1".to_string()]);
    // The target returns: the next resolve answers the receiver's
    // recorded receipt (the worker's durable inbox key), one visible
    // message, reconciled.
    {
        let mut roster = harness
            .supervisor
            .roster
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roster.write_summary(
            roster_summary(
                "target-root-1",
                "sess-target-1",
                "target",
                1,
                Some(harness.dir.join("parent.jsonl").to_string_lossy().as_ref()),
            ),
            Some("target-root-1"),
            None,
        );
    }
    assert_eq!(
        seam_process_died.lookup_agent_message("msgreq_u1").await,
        AgentMessageLookup::Admitted(recorded.clone()),
        "the recovery resolves to the receiver's recorded receipt"
    );
    let recovered = crate::cloud_family::delivery::reconcile_uncertain(
        &responder,
        &seam_process_died,
        std::slice::from_ref(&event),
    )
    .await;
    assert_eq!(recovered.reconciled, vec!["msgreq_u1".to_string()]);
    assert!(recovered.unanswered.is_empty());
    assert_eq!(
        visible_message_count(&harness),
        1,
        "the recovery never duplicated the visible message"
    );
    // The answer is durable; the NEXT replay re-submits it through the
    // submit seam without any delivery.
    let replayed = responder
        .handle_event(&event, &seam_process_died, &submitter)
        .await
        .expect("the post-reconcile replay");
    assert_eq!(replayed, HandleOutcome::DuplicateResubmitted);
    let submitted = submitter.submitted.lock().unwrap().clone();
    let (_, command) = submitted.first().expect("the answer was submitted");
    match &command.payload {
        CloudFamilyCommandPayload::AgentMessageResult { ok, receipt, .. } => {
            assert!(*ok, "the recovered answer is positive: {command:?}");
            let receipt = receipt.as_ref().expect("the recorded receipt");
            assert_eq!(receipt.id, recorded.id);
        }
        CloudFamilyCommandPayload::FamilyRosterResult { .. } => {
            panic!("the answer must be an agent_message_result")
        }
    }
    assert_eq!(
        visible_message_count(&harness),
        1,
        "the replay after the reconcile delivered nothing"
    );
}

/// Finding-3 contract: only a durably-recorded answer reconciles a
/// request. A failed answer append (the responder's journal is
/// unwritable) leaves the id unanswered and uncertain — the wiring can
/// retry the same pass once the disk recovers, and the retry reconciles.
#[tokio::test]
async fn a_failed_answer_append_leaves_the_request_unanswered_for_retry() {
    let harness = harness().await;
    let delivery = seam(&harness);
    let responder_path = harness.dir.join("family-results.jsonl");
    {
        let mut results = FamilyResultLog::open(&responder_path).unwrap();
        results.admit("msgreq_r1").unwrap();
    }
    let responder = CloudFamilyResponder::new(FamilyResultLog::open(&responder_path).unwrap());
    let event = agent_message_event("msgreq_r1", "target-root-1");
    // The resolve needs a durable answer target: sabotage the results
    // journal path so the answer append fails (a directory at the path —
    // the append opens for write and hits EISDIR/ENOTDIR).
    std::fs::remove_file(&responder_path).unwrap();
    std::fs::create_dir(&responder_path).unwrap();
    let outcome = crate::cloud_family::delivery::reconcile_uncertain(
        &responder,
        &delivery,
        std::slice::from_ref(&event),
    )
    .await;
    assert!(
        outcome.reconciled.is_empty(),
        "a failed answer append never reports reconciled"
    );
    assert_eq!(outcome.unanswered, vec!["msgreq_r1".to_string()]);
    assert_eq!(responder.uncertain(), vec!["msgreq_r1".to_string()]);
    // The disk recovers: the retry reconciles the same request.
    std::fs::remove_dir(&responder_path).unwrap();
    let retry = crate::cloud_family::delivery::reconcile_uncertain(
        &responder,
        &delivery,
        std::slice::from_ref(&event),
    )
    .await;
    assert_eq!(retry.reconciled, vec!["msgreq_r1".to_string()]);
    assert!(retry.unanswered.is_empty());
    assert!(
        responder.uncertain().is_empty(),
        "the answer is durable now"
    );
}
