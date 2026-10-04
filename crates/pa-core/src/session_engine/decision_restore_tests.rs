//! Decision-provider state is branch metadata, independent of the prompt window.

use std::path::Path;

use pa_types::session::FileEntry;
use pa_types::slash_commands::{DecisionApiProvider, DECISION_API_STATUS_CUSTOM_TYPE};
use serde_json::json;

use super::engine::{create_session, SessionEngine, SessionEngineConfig};
use crate::session::manager::SessionManager;

fn compacted_history(provider: Option<DecisionApiProvider>) -> String {
    let mut rows = vec![
        json!({"type":"session","id":"s","version":3,"cwd":"/tmp","timestamp":"2026-01-01T00:00:00Z"}),
        json!({"type":"custom_message","id":"older","parentId":null,"customType":DECISION_API_STATUS_CUSTOM_TYPE,"content":"enabled","display":false,"details":{"provider":"jev"}}),
        json!({"type":"custom_message","id":"state","parentId":"older","customType":DECISION_API_STATUS_CUSTOM_TYPE,"content":"selected","display":false,"details":{"provider":provider.map(DecisionApiProvider::id)}}),
        // A physically newer sibling must not override the selected branch.
        json!({"type":"custom_message","id":"sibling","parentId":"older","customType":DECISION_API_STATUS_CUSTOM_TYPE,"content":"sibling","display":false,"details":{"provider":"jev"}}),
    ];
    let mut parent = "state".to_string();
    for i in 0..220 {
        let id = format!("u{i}");
        rows.push(json!({"type":"message","id":id,"parentId":parent,"message":{"role":"user","content":format!("message {i}"),"timestamp":0}}));
        parent = id;
    }
    rows.push(json!({"type":"compaction","id":"compact","parentId":parent,"summary":"summary","firstKeptEntryId":"u210","tokensBefore":999}));
    rows.push(json!({"type":"message","id":"leaf","parentId":"compact","message":{"role":"user","content":"latest","timestamp":0}}));
    rows.into_iter().map(|row| row.to_string() + "\n").collect()
}

async fn build_session(root: &Path, manager: SessionManager) -> SessionEngine {
    let model = pa_agent::types::Model {
        id: "m".into(),
        name: "m".into(),
        api: "test".into(),
        provider: "test".into(),
        base_url: "http://localhost".into(),
        reasoning: false,
        cost: pa_agent::types::UsageCost::default(),
        context_window: 1_000,
        max_tokens: 100,
    };
    let provider = std::sync::Arc::new(pa_agent::scripted::ScriptedProvider::new(model.clone()));
    let conversation_log_path = manager.get_session_file().map(Path::to_path_buf);
    create_session(SessionEngineConfig {
        cwd: root.to_path_buf(),
        agent_dir: root.join("agent"),
        session_manager: Some(manager),
        conversation_log_path,
        model: Some(model),
        stream_fn: Some(provider.stream_fn()),
        ..Default::default()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn decision_api_constructor_restores_compacted_windowed_restarted_and_forked_sessions() {
    for provider in [Some(DecisionApiProvider::Clef), None] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.jsonl");
        let session_dir = dir.path().join("sessions");
        std::fs::create_dir(&session_dir).unwrap();
        std::fs::write(&path, compacted_history(provider)).unwrap();
        let full = SessionManager::open(dir.path(), &session_dir, &path);
        let window = SessionManager::open_windowed(dir.path(), &session_dir, &path)
            .await
            .unwrap();
        assert!(!window.is_full_history());
        // Reopening exercises the cached window metadata as well as its cold walk.
        let restarted = SessionManager::open_windowed(dir.path(), &session_dir, &path)
            .await
            .unwrap();
        let forked =
            SessionManager::fork_from(&path, dir.path(), &dir.path().join("forks")).unwrap();
        for manager in [full, window, restarted, forked] {
            assert_eq!(manager.decision_api_provider(), provider);
            assert!(manager.active_context().messages.iter().all(|message| {
                !matches!(message, pa_types::session::AgentMessage::Custom(custom)
                    if custom.custom_type == DECISION_API_STATUS_CUSTOM_TYPE)
            }));
            let engine = build_session(dir.path(), manager).await;
            assert_eq!(engine.decision_api_switch().provider(), provider);
            assert_eq!(
                engine.system_prompt().contains("<name>decision-api</name>"),
                provider.is_some()
            );
            assert_eq!(
                engine.session.agent().state().await.system_prompt,
                engine.system_prompt()
            );
            {
                let persistence = engine.session.shared_persistence();
                let manager = persistence.lock().await;
                assert_eq!(
                    (manager.has_thinking_level(), manager.has_service_tier()),
                    (true, true),
                );
            }
            engine.dispose_kernel().await;
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let manager = SessionManager::in_memory(dir.path());
    assert_eq!(
        (
            manager.active_context().model,
            manager.has_thinking_level(),
            manager.has_service_tier()
        ),
        (None, false, false),
    );
    let fresh = build_session(dir.path(), manager).await;
    {
        let persistence = fresh.session.shared_persistence();
        let manager = persistence.lock().await;
        assert_eq!(
            (
                manager.active_context().model,
                manager.has_thinking_level(),
                manager.has_service_tier()
            ),
            (Some(("test".to_string(), "m".to_string())), true, true),
        );
    }
    fresh.dispose_kernel().await;
}

#[tokio::test]
async fn decision_api_branch_navigation_restores_metadata_before_the_compaction_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let entries =
        crate::session::parse_session_entries(&compacted_history(Some(DecisionApiProvider::Clef)));
    let mut manager = SessionManager::in_memory(dir.path());
    manager.adopt_entries(entries.clone());
    let engine = build_session(dir.path(), manager).await;
    let mut sibling = SessionManager::in_memory(dir.path());
    sibling.adopt_entries(entries.clone());
    sibling.branch("sibling");
    let sibling_entries: Vec<FileEntry> = sibling.get_branch(None).into_iter().cloned().collect();
    engine
        .session
        .rebuild_branch_context(sibling_entries)
        .await
        .unwrap();
    engine.sync_decision_api_from_session().await;
    assert_eq!(
        engine.decision_api_switch().provider(),
        Some(DecisionApiProvider::Jev)
    );
    engine
        .session
        .rebuild_branch_context(entries)
        .await
        .unwrap();
    engine.sync_decision_api_from_session().await;
    assert_eq!(
        engine.decision_api_switch().provider(),
        Some(DecisionApiProvider::Clef)
    );
    engine
        .session
        .rebuild_branch_context(Vec::new())
        .await
        .unwrap();
    engine.sync_decision_api_from_session().await;
    assert_eq!(engine.decision_api_switch().provider(), None);
    assert!(!engine.system_prompt().contains("<name>decision-api</name>"));
    engine.dispose_kernel().await;
}

// Real fixtures own their process-wide kernel registry and environment.
#[tracing::instrument]
async fn run_runtime_fixture_in_child(name: &str) -> bool {
    const CHILD_FIXTURE: &str = "PA_DECISION_API_RUNTIME_FIXTURE_CHILD";
    let test = format!("session_engine::decision_restore_tests::{name}");
    if std::env::var(CHILD_FIXTURE).as_deref() == Ok(test.as_str()) {
        return false;
    }
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", &test, "--nocapture"])
        .env(CHILD_FIXTURE, &test)
        .env("RUST_TEST_THREADS", "1")
        .kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(240), command.output())
        .await
        .expect("isolated real fixture deadline")
        .expect("isolated real fixture starts");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stdout.lines().chain(stderr.lines()) {
        eprintln!("[decision fixture child] {line}");
    }
    assert!(
        output.status.success(),
        "isolated fixture {test}: {}",
        output.status
    );
    assert!(
        stdout.contains("running 1 test"),
        "the exact child fixture must run"
    );
    assert!(
        stdout.contains(&format!("DECISION_API_REAL_FIXTURE_EXECUTED: {name}"))
            || stderr.contains("no installed kernel Python"),
        "fixture must report execution or its explicit runtime skip",
    );
    true
}

#[tokio::test]
async fn decision_api_real_kernel_restores_the_switch_and_changes_preimports_on_and_off() {
    use crate::kernel::shared::{ExecuteOptions, ExecuteStatus};
    if run_runtime_fixture_in_child(
        "decision_api_real_kernel_restores_the_switch_and_changes_preimports_on_and_off",
    )
    .await
    {
        return;
    }
    // This verifier needs an installed runtime; hermetic hosts can run the
    // metadata regressions without downloading a Python environment.
    let installed = std::env::var_os("PRIME_AGENT_KERNEL_PYTHON")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| {
                std::path::PathBuf::from(home).join(".prime/agent/kernel-venv/bin/python")
            })
        });
    if !installed.is_some_and(|path| path.exists()) {
        eprintln!("skipping decision API real-kernel verifier: no installed kernel Python");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.jsonl");
    let session_dir = dir.path().join("sessions");
    std::fs::create_dir(&session_dir).unwrap();
    std::fs::write(&path, compacted_history(Some(DecisionApiProvider::Clef))).unwrap();
    let manager = SessionManager::open(dir.path(), &session_dir, &path);
    let engine = build_session(dir.path(), manager).await;
    for (provider, available) in [
        (Some(DecisionApiProvider::Clef), true),
        (None, false),
        (Some(DecisionApiProvider::Jev), true),
    ] {
        engine.set_decision_api(provider).await;
        let kernel = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            engine.provisioner.ensure(None, None),
        )
        .await
        .expect("kernel bootstrap deadline")
        .expect("real kernel must start");
        let code = format!(
            "assert ('decision_api' in globals()) == {available}\n\
             answer = globals().get('answer', 41) + 1\n\
             print(answer)",
            available = if available { "True" } else { "False" },
        );
        let result = kernel
            .execute(&code, ExecuteOptions::default())
            .await
            .unwrap();
        assert_eq!(
            result.status,
            ExecuteStatus::Ok,
            "{:?}\n{}\n{}",
            result.error,
            result.stdout,
            result.stderr
        );
        let expected = match provider {
            Some(DecisionApiProvider::Clef) => "42\n",
            None => "43\n",
            Some(DecisionApiProvider::Jev) => "44\n",
        };
        assert_eq!(result.stdout, expected);
    }
    engine.dispose_kernel().await;
    println!("DECISION_API_REAL_FIXTURE_EXECUTED: decision_api_real_kernel_restores_the_switch_and_changes_preimports_on_and_off");
}

#[tokio::test]
async fn decision_api_real_runtime_loop_routes_decisions_and_child_delivery_then_cleans_up() {
    use std::sync::{Arc, Mutex};

    use crate::kernel::manager::{KernelStartOptions, ReplKernelManager};
    use crate::kernel::shared::{
        host_handler, ExecuteOptions, ExecuteStatus, HostRequestHandlers, KernelManagerOptions,
        KernelShutdownOptions,
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FixtureRequest {
        Decide,
        Spawn,
        Send,
        Goal,
        Delete,
        AwaitChild,
    }
    if run_runtime_fixture_in_child(
        "decision_api_real_runtime_loop_routes_decisions_and_child_delivery_then_cleans_up",
    )
    .await
    {
        return;
    }
    // This is a real REPL/skill bridge with synthetic provider and child
    // handlers. It never invokes a paid provider or starts a daemon child.
    let python = std::env::var_os("PRIME_AGENT_KERNEL_PYTHON")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| {
                std::path::PathBuf::from(home).join(".prime/agent/kernel-venv/bin/python")
            })
        });
    let Some(python) = python.filter(|path| path.exists()) else {
        eprintln!("skipping decision API real-runtime fixture: no installed kernel Python");
        return;
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let goal_message = Arc::new(Mutex::new(serde_json::Value::Null));
    let child_ready = Arc::new(tokio::sync::Notify::new());
    let mut handlers = HostRequestHandlers::new();
    for (name, kind) in [
        ("decision_api.decide", FixtureRequest::Decide),
        ("rlm.run", FixtureRequest::Spawn),
        ("agent_message.send", FixtureRequest::Send),
        ("decision_api.goal", FixtureRequest::Goal),
        ("rlm.delete_subagent", FixtureRequest::Delete),
        ("fixture.child_ready", FixtureRequest::AwaitChild),
    ] {
        let events = Arc::clone(&events);
        let goal_message = Arc::clone(&goal_message);
        let child_ready = Arc::clone(&child_ready);
        handlers.register(name, host_handler(move |payload| {
            let events = Arc::clone(&events);
            let goal_message = Arc::clone(&goal_message);
            let child_ready = Arc::clone(&child_ready);
            Box::pin(async move {
                events.lock().unwrap().push((kind, payload.data.clone()));
                match kind {
                    FixtureRequest::Decide => Ok(json!({
                        "model":"synthetic-decision",
                        "answers":{"action":{"choice":"left","confidence":0.9,"probabilities":{"left":0.9,"right":0.1}}}
                    })),
                    FixtureRequest::Spawn => {
                        Ok(json!({"rlm_child_id":"fixture-child","name":payload.data["kwargs"]["name"],"session_dir":"/tmp/fixture-child","model":"synthetic/child"}))
                    }
                    FixtureRequest::Send => {
                        let text = payload.data["message"].as_str().unwrap();
                        let observation: serde_json::Value = serde_json::from_str(text.split_once("First message:\n").unwrap().1)?;
                        assert_eq!(observation["seq"], 0);
                        *goal_message.lock().unwrap() = json!({"seq":0,"goal":"follow fixture strategy"});
                        child_ready.notify_one();
                        Ok(json!({"deliveryStatus":"sent"}))
                    }
                    FixtureRequest::Delete => Ok(json!({"subagent":{
                        "rlm_child_id":"fixture-child","session_name":payload.data["target"],
                        "session_dir":"/tmp/fixture-child","status":"completed"
                    },"outcome":"deleted"})),
                    FixtureRequest::Goal => Ok(goal_message.lock().unwrap().take()),
                    FixtureRequest::AwaitChild => {
                        child_ready.notified().await;
                        Ok(json!({"ready":true}))
                    }
                }
            })
        }));
    }
    let dir = tempfile::tempdir().unwrap();
    let manager = ReplKernelManager::new(KernelManagerOptions {
        python: Some(python),
        cwd: Some(dir.path().to_path_buf()),
        env: std::collections::HashMap::new(),
        session_id: Some("decision-runtime-fixture".to_string()),
        host_handlers: handlers,
        python_skills: Vec::new(),
        on_background_work_settled: None,
        snapshot: None,
        bootstrap_code: None,
        stderr_log_path: None,
    });
    manager.start(KernelStartOptions::default()).await.unwrap();
    let source =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../skills/decision-api/src");
    let source = serde_json::to_string(&source.display().to_string()).unwrap();
    let code = format!(
        r#"
import sys, asyncio, pathlib
sys.path.insert(0, {source})
import decision_api
assert pathlib.Path(decision_api.__file__).resolve().is_relative_to(pathlib.Path({source}).resolve())
assert decision_api.rlm.__file__ and decision_api.agent_message.__file__
actions_taken = []
observations_seen = 0
async def observe_fixture():
    global observations_seen
    observations_seen += 1
    if observations_seen == 2:
        await decision_api.rlm.host_request("fixture.child_ready")
        loop._system2_ready.set()
        while not loop.goal_updates:
            await asyncio.sleep(0)
    if observations_seen > 2:
        return None
    return {{"frame": observations_seen}}
loop = decision_api.Loop(observe_fixture, actions_taken.append, {{"left":"go left", "right":"go right"}},
    objective="fixture objective", system2=decision_api.System2(interval=3600))
status = await asyncio.wait_for(loop.run(), 10)
assert actions_taken == ["left", "left"], actions_taken
assert loop.goal == "follow fixture strategy", loop.status()
assert loop.errors == [], loop.errors
assert not status["running"]
print("fixture passed")
"#
    );
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        manager.execute(&code, ExecuteOptions::default()),
    )
    .await
    .expect("real runtime fixture deadline")
    .expect("real runtime fixture execution");
    manager
        .shutdown(KernelShutdownOptions::default())
        .await
        .unwrap();
    assert_eq!(
        result.status,
        ExecuteStatus::Ok,
        "{:?}\n{}\n{}",
        result.error,
        result.stdout,
        result.stderr
    );
    assert_eq!(result.stdout, "fixture passed\n");
    let events = events.lock().unwrap();
    let mut decisions: Vec<_> = events
        .iter()
        .filter(|(kind, _)| *kind == FixtureRequest::Decide)
        .map(|(_, payload)| payload["request"]["state"].clone())
        .collect();
    assert_eq!(decisions.len(), 2);
    // Latency depends on scheduler timing; the request state is deterministic.
    decisions[1]["recent_actions"][0]
        .as_object_mut()
        .unwrap()
        .remove("latency_ms");
    assert_eq!(
        decisions,
        vec![
            json!({"observation":{"frame":1},"goal":"fixture objective"}),
            json!({"observation":{"frame":2},"goal":"follow fixture strategy","recent_actions":[{"step":0,"action":"left","confidence":0.9}]}),
        ]
    );
    let spawned = events
        .iter()
        .find(|(kind, _)| *kind == FixtureRequest::Spawn)
        .unwrap();
    let sent = events
        .iter()
        .find(|(kind, _)| *kind == FixtureRequest::Send)
        .unwrap();
    let deleted = events
        .iter()
        .find(|(kind, _)| *kind == FixtureRequest::Delete)
        .unwrap();
    assert_eq!(
        (
            sent.1["receiver_role"].clone(),
            sent.1["receiver_name"].clone(),
            deleted.1["target"].clone()
        ),
        (
            json!("child"),
            spawned.1["kwargs"]["name"].clone(),
            spawned.1["kwargs"]["name"].clone()
        )
    );
    assert_eq!(*goal_message.lock().unwrap(), serde_json::Value::Null);
    println!("DECISION_API_REAL_FIXTURE_EXECUTED: decision_api_real_runtime_loop_routes_decisions_and_child_delivery_then_cleans_up");
}
