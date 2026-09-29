//! Cloud-keyed agent-message inbox tests: the request-id durable receiver
//! admission (`cloudRequestId` on `worker_deliver_message`), the
//! idempotent duplicate answer, and the restart survival of both the
//! dedupe key and the visible lane.
use super::agent_message_tests::{created_worker, queue_texts};
use super::*;

fn keyed_payload(message: &str, request_id: &str) -> Value {
    json!({
        "targetActiveSessionId": "target-session",
        "message": message,
        "cloudRequestId": request_id,
        "sender": {
            "activeSessionId": "remote-cloud-1",
            "sessionId": "sess-cloud-1",
            "sessionName": "cloud kid",
            "runtimeKind": "top-level",
        },
    })
}

/// The full keyed delivery flow: the receipt, the visible lane item, and
/// the durable admission record in the journal (the same flush as the
/// queue snapshot).
#[tokio::test]
async fn keyed_delivery_records_the_admission_in_one_flush() {
    let worker = created_worker().await;
    let response = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("cloud hello", "msgreq_1"),
        )
        .await;
    assert!(response.success, "deliver failed: {response:?}");
    let data = response.data.expect("receipt");
    let id = data["id"].as_str().expect("receipt id").to_string();
    assert!(id.starts_with("agentmsg_"), "receipt id: {data}");
    assert_eq!(data["deliveryStatus"], "delivered");
    // The rendered prompt is on the steering lane (one visible message).
    assert_eq!(
        queue_texts(&worker.core, Lane::Steering),
        vec!["[agent-message from cloud kid]\n\ncloud hello"]
    );
    // The journal holds the queue snapshot AND the cloud admission for
    // the request id, one durable batch.
    let receipt = {
        let recovery = worker.recovery.lock().unwrap();
        let journal = recovery.as_ref().expect("journal opened by the keyed path");
        journal
            .cloud_inbox_receipt("msgreq_1")
            .expect("recorded admission")
            .clone()
    };
    assert_eq!(receipt["id"], json!(id));
    // The on-disk file carries the admission record beside the snapshot.
    let content = std::fs::read_to_string(&worker.config.recovery_journal_path).unwrap();
    assert!(
        content.contains("\"cloud_inbox_admission\""),
        "no admission record on disk: {content}"
    );
}

/// The idempotent duplicate: a repeat of the same request id answers the
/// RECORDED receipt and never enqueues a second visible message — the
/// wire may replay the event (at-least-once) without duplicating the
/// delivery.
#[tokio::test]
async fn duplicate_request_answers_the_recorded_receipt_without_re_delivering() {
    let worker = created_worker().await;
    // Busy: the deliveries park in the lane (deterministic visibility —
    // the idle runner would drain them).
    worker.core.lock().unwrap().busy = true;
    let first = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("once only", "msgreq_2"),
        )
        .await;
    assert!(first.success, "first deliver failed: {first:?}");
    let first_data = first.data.expect("receipt");
    // The duplicate answers the recorded receipt with no second visible
    // message.
    let duplicate = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("once only", "msgreq_2"),
        )
        .await;
    assert!(duplicate.success, "duplicate failed: {duplicate:?}");
    assert_eq!(
        duplicate.data.expect("receipt"),
        first_data,
        "the duplicate must answer the recorded receipt"
    );
    assert_eq!(queue_texts(&worker.core, Lane::Steering).len(), 1);
    // A DIFFERENT request id is a fresh delivery (two visible messages).
    let second = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("another one", "msgreq_3"),
        )
        .await;
    assert!(second.success, "second deliver failed: {second:?}");
    assert_ne!(
        second.data.expect("receipt")["id"],
        first_data["id"],
        "a fresh request id mints a fresh receipt"
    );
    assert_eq!(queue_texts(&worker.core, Lane::Steering).len(), 2);
    // The recorded receipt outranks the admission gates: even paused
    // (the pause clears the queued items), a replay of the admitted
    // request id answers the recorded receipt — the message was
    // admitted; refusing the duplicate would claim it was not.
    worker.dispatch("agent_messages_pause", &json!({})).await;
    let paused_duplicate = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("once only", "msgreq_2"),
        )
        .await;
    assert!(
        paused_duplicate.success,
        "the paused duplicate must answer the recorded receipt: {paused_duplicate:?}"
    );
    assert_eq!(paused_duplicate.data.expect("receipt"), first_data);
}

/// The crash/restart contract: the admission (the dedupe key) and the
/// queued message (the lane snapshot) are one durable flush, so a
/// respawned worker restores the visible message AND answers a duplicate
/// request id with the recorded receipt — never a second visible
/// message.
#[tokio::test]
async fn restart_restores_the_lane_and_the_inbox_key() {
    let dir = std::env::temp_dir().join(format!("pa-worker-cloud-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = WorkerConfig {
        socket_path: dir.join("worker.sock"),
        supervisor_socket_path: PathBuf::new(),
        token: "token".to_string(),
        worker_instance_id: String::new(),
        active_session_id: "target-session".to_string(),
        agent_dir: dir.join("agent"),
        recovery_journal_path: dir.join("recovery.jsonl"),
        telemetry_disabled: None,
        script: Some(json!({ "responses": ["ack"] })),
    };
    let first_worker = Arc::new(Worker::new(config.clone(), None));
    let created = first_worker
        .dispatch(
            "create",
            &json!({ "noSession": true, "cwd": "/tmp", "name": "target" }),
        )
        .await;
    assert!(created.success, "create failed: {created:?}");
    // Busy: the delivery parks in the lane (queued), the state a crash
    // must revive.
    first_worker.core.lock().unwrap().busy = true;
    let parked = first_worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("parked cloud note", "msgreq_4"),
        )
        .await;
    assert!(parked.success, "parked deliver failed: {parked:?}");
    let parked_receipt = parked.data.expect("receipt");
    assert_eq!(parked_receipt["deliveryStatus"], "queued");
    drop(first_worker);
    // The respawn: a fresh worker over the same recovery journal (the
    // serve loop opens the journal; the test installs it the same way).
    let respawned = Arc::new(Worker::new(config, None));
    *respawned.recovery.lock().unwrap() = Some(
        crate::journal::WorkerRecoveryJournal::open(&respawned.config.recovery_journal_path)
            .unwrap(),
    );
    let re_created = respawned
        .dispatch(
            "create",
            &json!({ "noSession": true, "cwd": "/tmp", "name": "target" }),
        )
        .await;
    assert!(re_created.success, "re-create failed: {re_created:?}");
    // The visible message survived the restart: exactly one restored lane
    // item.
    assert_eq!(
        queue_texts(&respawned.core, Lane::Steering),
        vec!["[agent-message from cloud kid]\n\nparked cloud note"]
    );
    // The dedupe key survived too: a replayed duplicate answers the
    // recorded receipt, never a second visible message.
    let duplicate = respawned
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("parked cloud note", "msgreq_4"),
        )
        .await;
    assert!(
        duplicate.success,
        "duplicate after restart failed: {duplicate:?}"
    );
    assert_eq!(
        duplicate.data.expect("receipt"),
        parked_receipt,
        "the restart must answer the recorded receipt"
    );
    assert_eq!(queue_texts(&respawned.core, Lane::Steering).len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A keyed delivery is refused by the same gates as the local path when
/// the request id was never admitted (the fresh-delivery arm).
#[tokio::test]
async fn fresh_keyed_delivery_respects_the_paused_gate() {
    let worker = created_worker().await;
    worker.dispatch("agent_messages_pause", &json!({})).await;
    let refused = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("while paused", "msgreq_5"),
        )
        .await;
    assert!(!refused.success, "paused gate must refuse: {refused:?}");
    assert_eq!(
        refused.error.as_deref(),
        Some("Agent messaging is paused"),
        "the TS paused error: {refused:?}"
    );
}

/// An unkeyed delivery is untouched: no admission record lands in the
/// journal for the legacy local path (the journal, when it exists, holds
/// only the queue snapshot and verdict records the local path writes).
#[tokio::test]
async fn unkeyed_delivery_stays_untracked() {
    let worker = created_worker().await;
    let response = worker
        .dispatch(
            "worker_deliver_message",
            &json!({
                "targetActiveSessionId": "target-session",
                "message": "local hello",
                "sender": { "activeSessionId": "source-session" },
            }),
        )
        .await;
    assert!(response.success, "local deliver failed: {response:?}");
    let content = std::fs::read_to_string(&worker.config.recovery_journal_path).unwrap_or_default();
    assert!(
        !content.contains("cloud_inbox_admission"),
        "an unkeyed delivery must not write an admission record: {content}"
    );
    let keyed = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("cloud hello", "msgreq_6"),
        )
        .await;
    assert!(keyed.success, "keyed deliver failed: {keyed:?}");
    let content = std::fs::read_to_string(&worker.config.recovery_journal_path).unwrap_or_default();
    assert!(
        content.contains("cloud_inbox_admission"),
        "the keyed delivery must write the admission record: {content}"
    );
}

/// Finding-1 contract: a keyed delivery whose durable commit fails
/// fails CLOSED — no receipt, the enqueue rolled back (exactly this
/// delivery's item, the neighboring queue work untouched), and the same
/// request id still deliverable once the journal recovers.
#[tokio::test]
async fn keyed_delivery_fails_closed_and_rolls_back_when_the_append_fails() {
    let worker = created_worker().await;
    // Neighboring queue work: an unkeyed local delivery, plus one keyed
    // delivery that committed cleanly.
    let local = worker
        .dispatch(
            "worker_deliver_message",
            &json!({
                "targetActiveSessionId": "target-session",
                "message": "local neighbor",
                "sender": { "activeSessionId": "source-session" },
            }),
        )
        .await;
    assert!(local.success, "the local neighbor delivery: {local:?}");
    let committed = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("committed", "msgreq_f1"),
        )
        .await;
    assert!(
        committed.success,
        "the committed keyed delivery: {committed:?}"
    );
    // Sabotage the journal path (a directory where the file was): the
    // next append fails, and the recovery read fails the same way.
    let journal_path = worker.config.recovery_journal_path.clone();
    std::fs::remove_file(&journal_path).unwrap();
    std::fs::create_dir(&journal_path).unwrap();
    let refused = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("while broken", "msgreq_f2"),
        )
        .await;
    assert!(
        !refused.success,
        "the delivery must fail closed when the commit fails: {refused:?}"
    );
    assert!(
        refused
            .error
            .as_deref()
            .is_some_and(|error| error.contains("cloud inbox journal")),
        "the TS-honest failure names the journal: {refused:?}"
    );
    // Exactly the failed delivery's item is gone; the neighbors and
    // their order survive.
    assert_eq!(
        queue_texts(&worker.core, Lane::Steering),
        vec![
            "[agent-message from source-session]\n\nlocal neighbor",
            "[agent-message from cloud kid]\n\ncommitted",
        ],
        "the rollback removed exactly the failed delivery"
    );
    // The journal recovers: the SAME request id delivers once.
    std::fs::remove_dir(&journal_path).unwrap();
    let retried = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("while broken", "msgreq_f2"),
        )
        .await;
    assert!(retried.success, "the retry after the repair: {retried:?}");
    assert_eq!(
        queue_texts(&worker.core, Lane::Steering).len(),
        3,
        "the retry delivered exactly once"
    );
    // The retry's receipt is durable: a duplicate answers it.
    let retried_receipt = retried.data.expect("receipt");
    let duplicate = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("while broken", "msgreq_f2"),
        )
        .await;
    assert_eq!(duplicate.data.expect("receipt"), retried_receipt);
    assert_eq!(queue_texts(&worker.core, Lane::Steering).len(), 3);
}

/// The commit-outcome recovery: a failed append whose record actually
/// landed (the fsync error was advisory) is answered with the receipt —
/// never contradicted, never rolled back. The decision reads the
/// journal state from disk.
#[tokio::test]
async fn a_landed_write_behind_a_failed_commit_is_answered_not_rolled_back() {
    let worker = created_worker().await;
    // One committed keyed delivery establishes the journal.
    let committed = worker
        .dispatch(
            "worker_deliver_message",
            &keyed_payload("first", "msgreq_g1"),
        )
        .await;
    assert!(committed.success);
    let committed_receipt = committed.data.expect("receipt");
    // The commit of a SECOND delivery reports failure, but its write
    // landed on disk (the disk state carries the transaction). The
    // recover decision reads the disk: the recorded receipt for the
    // request id proves the transaction is THIS delivery's.
    let reloaded =
        crate::journal::WorkerRecoveryJournal::open(&worker.config.recovery_journal_path).unwrap();
    assert_eq!(
        reloaded.cloud_inbox_receipt("msgreq_g1"),
        Some(&committed_receipt),
        "the committed transaction is on disk"
    );
    let recovered = crate::worker::input::recover_commit_outcome(
        &worker.config.recovery_journal_path,
        "msgreq_g1",
        &committed_receipt,
    );
    assert!(
        recovered.is_ok_and(|journal| journal.is_some()),
        "a landed write is recovered, not contradicted"
    );
    // A receipt that is NOT on disk answers None (the rollback arm).
    let ghost = crate::worker::input::recover_commit_outcome(
        &worker.config.recovery_journal_path,
        "msgreq_g1",
        &json!({ "id": "agentmsg_ghost", "deliveryStatus": "delivered" }),
    );
    assert!(ghost.is_ok_and(|journal| journal.is_none()));
    // A request id the journal never heard of answers None too.
    let unknown = crate::worker::input::recover_commit_outcome(
        &worker.config.recovery_journal_path,
        "msgreq_unknown",
        &committed_receipt,
    );
    assert!(unknown.is_ok_and(|journal| journal.is_none()));
}
