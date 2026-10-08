//! Attach snapshots read stored selected-branch state before lazy engine build.
use super::super::*;
use crate::session_store::SessionFile;
use pa_types::slash_commands::DECISION_API_STATUS_CUSTOM_TYPE;
use std::path::Path;

fn lazy_worker(dir: &Path) -> Worker {
    let worker = Worker::new(
        WorkerConfig {
            socket_path: dir.join("worker.sock"),
            supervisor_socket_path: PathBuf::new(),
            token: "test-token".to_string(),
            worker_instance_id: "snapshot-instance".to_string(),
            active_session_id: "snapshot-session".to_string(),
            agent_dir: dir.join("agent"),
            recovery_journal_path: dir.join("recovery.jsonl"),
            telemetry_disabled: Some(true),
            script: Some(json!({"engine":"faux", "responses":["unused"]})),
        },
        None,
    );
    {
        let mut core = worker.core.lock().unwrap();
        core.created = true;
        core.cwd = dir.display().to_string();
    }
    worker
}

fn compacted_file(dir: &Path, statuses: &[Value]) -> SessionFile {
    let mut file = SessionFile::create("/tmp", None, 0);
    file.set_path(dir.join(format!("{}.jsonl", uuid::Uuid::new_v4())));
    let root = file.append_message(&json!({"role":"user", "content":"root"}));
    for details in statuses {
        file.append_entry("custom_message", json!({
            "customType":DECISION_API_STATUS_CUSTOM_TYPE, "content":"status", "display":false, "details":details,
        }));
    }
    let selected = file.leaf_id().unwrap().to_string();
    // A physically newer sibling must never override the selected status.
    file.leaf_id = Some(root);
    file.append_entry("custom_message", json!({
        "customType":DECISION_API_STATUS_CUSTOM_TYPE, "content":"sibling", "display":false, "details":{"enabled":true},
    }));
    file.leaf_id = Some(selected);
    let mut first_kept = String::new();
    for index in 0..220 {
        let id = file.append_message(&json!({"role":"user", "content":format!("row {index}")}));
        if index == 210 {
            first_kept = id;
        }
    }
    file.append_entry(
        "compaction",
        json!({"summary":"summary", "firstKeptEntryId":first_kept, "tokensBefore":10000}),
    );
    file.append_message(&json!({"role":"user", "content":"after compaction"}));
    file.rewrite().unwrap();
    let windowed = SessionFile::open_windowed(&file.path).unwrap();
    assert!(windowed.window.is_some());
    assert!(!windowed.branch().iter().any(|entry| entry
        .fields
        .get("customType")
        .and_then(Value::as_str)
        == Some(DECISION_API_STATUS_CUSTOM_TYPE)));
    windowed
}

fn attach_state(worker: &Worker) -> Value {
    let response = worker
        .handle_attach(&json!({"clientId":"snapshot-client", "capabilities":["slim_attach"]}));
    assert!(response.success, "{response:?}");
    let data = response.data.unwrap();
    data["snapshot"]["state"].clone()
}

#[tokio::test]
async fn attach_restores_compacted_decision_state_before_building_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let worker = lazy_worker(dir.path());
    let engine = worker
        .agent_engine
        .as_ref()
        .expect("the real lazy faux engine");
    assert!(engine.session.try_lock().unwrap().is_none());
    // The newest status decides; off, malformed, or absent details read
    // off (the wire omits the field).
    let cases = [
        (vec![json!({"enabled":true})], json!(true)),
        (vec![json!({"enabled":false})], Value::Null),
        (
            vec![json!({"enabled":true}), json!({"enabled":false})],
            Value::Null,
        ),
        (
            vec![json!({"enabled":true}), json!("malformed")],
            Value::Null,
        ),
        (vec![json!({"enabled":true}), Value::Null], Value::Null),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (statuses, state_value) in cases {
        worker.core.lock().unwrap().store = Some(compacted_file(dir.path(), &statuses));
        let state = attach_state(&worker);
        actual.push(state.get("decisionApi").cloned().unwrap_or(Value::Null));
        expected.push(state_value);
        assert!(engine.session.try_lock().unwrap().is_none());
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn branch_replacement_before_engine_build_refreshes_the_attach_provider() {
    let dir = tempfile::tempdir().unwrap();
    let worker = lazy_worker(dir.path());
    let mut first = compacted_file(dir.path(), &[json!({"enabled":true})]);
    // A newer selected status wins over pre-window metadata, including off.
    first.append_entry("custom_message", json!({
        "customType":DECISION_API_STATUS_CUSTOM_TYPE, "content":"off", "details":{"enabled":false},
    }));
    worker.core.lock().unwrap().store = Some(first);
    let before = attach_state(&worker);
    worker.core.lock().unwrap().store =
        Some(compacted_file(dir.path(), &[json!({"enabled":true})]));
    let after = attach_state(&worker);
    assert_eq!(
        (before.get("decisionApi"), after.get("decisionApi")),
        (None, Some(&json!(true)))
    );
    assert!(worker
        .agent_engine
        .as_ref()
        .unwrap()
        .session
        .try_lock()
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_new_rooted_branch_does_not_inherit_discarded_window_status() {
    let dir = tempfile::tempdir().unwrap();
    let worker = lazy_worker(dir.path());
    let mut file = compacted_file(dir.path(), &[json!({"enabled":true})]);
    file.leaf_id = None;
    file.append_message(&json!({"role":"user", "content":"new root"}));
    worker.core.lock().unwrap().store = Some(file);
    assert!(attach_state(&worker).get("decisionApi").is_none());
}
