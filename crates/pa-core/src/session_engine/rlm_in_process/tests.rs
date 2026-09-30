//! The in-process host battery: hermetic engines over scripted providers
//! (no kernel boots — the scripted models never call tools), driving the
//! host trait and the family controllers directly.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use pa_agent::scripted::ScriptedProvider;
use pa_agent::stream::StreamFn;
use pa_agent::types::Model as AgentModel;
use serde_json::json;

use super::family::{FamilySelf, InProcessFamilyController};
use super::{InProcessRlmHost, InProcessRlmHostConfig, DEFAULT_RLM_MAX_DEPTH};
use crate::models::registry::ModelRegistry;
use crate::session::manager::{NewSessionOptions, SessionManager};
use crate::session_engine::agent_messaging::{
    AgentMessageController, AgentMessageSendInput, AgentObserveController,
};
use crate::session_engine::engine::{create_session, SessionEngine, SessionEngineConfig};
use crate::session_engine::rlm_host::{
    RlmChildResult, RlmCreateSessionRequest, RlmSpawnRequest, RlmSubagentHost,
};
use crate::session_engine::rlm_in_process::StreamFnFactory;

/// A per-model scripted stream catalog: the parent and every child run on
/// their own scripted provider keyed by model id.
struct ScriptCatalog {
    providers: Mutex<HashMap<String, Arc<ScriptedProvider>>>,
}

impl ScriptCatalog {
    fn new() -> Self {
        Self {
            providers: Mutex::new(HashMap::new()),
        }
    }

    fn provider(self: &Arc<Self>, model_id: &str) -> Arc<ScriptedProvider> {
        Arc::clone(
            self.providers
                .lock()
                .unwrap()
                .entry(model_id.to_string())
                .or_insert_with(|| Arc::new(ScriptedProvider::new(script_model(model_id)))),
        )
    }

    fn stream_fn(self: &Arc<Self>, model_id: &str) -> StreamFn {
        self.provider(model_id).stream_fn()
    }

    fn factory(self: &Arc<Self>) -> StreamFnFactory {
        let catalog = Arc::clone(self);
        Arc::new(move |model: &AgentModel| catalog.stream_fn(&model.id))
    }
}

/// The minimal agent model shape the scripted provider needs.
fn script_model(id: &str) -> AgentModel {
    serde_json::from_value(json!({
        "id": id, "name": id, "api": "openai-completions", "provider": "test-provider",
        "baseUrl": "http://localhost", "reasoning": false, "input": ["text"],
        "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
        "contextWindow": 1000, "maxTokens": 100
    }))
    .unwrap()
}

/// One resident guest rig: a bound parent engine, its in-process host, and
/// the scripted catalog its children spawn through.
struct TestRig {
    _dir: tempfile::TempDir,
    agent_dir: PathBuf,
    catalog: Arc<ScriptCatalog>,
    host: Arc<InProcessRlmHost>,
    engine: Arc<SessionEngine>,
}

impl TestRig {
    async fn new() -> Self {
        Self::with_depth(0, DEFAULT_RLM_MAX_DEPTH).await
    }

    async fn with_depth(depth: u32, max_depth: u32) -> Self {
        // Hermetic credential source (the ambient PRIME_API_KEY must not
        // unlock built-in providers, same discipline as the rlm-host tests).
        struct NoEnvCredentials;
        impl crate::auth::manager::EnvCredentialSource for NoEnvCredentials {
            fn key_names(&self, _provider: &str) -> Option<Vec<String>> {
                None
            }
            fn api_key(&self, _provider: &str) -> Option<String> {
                None
            }
            fn prime_team_id(&self) -> Option<String> {
                None
            }
            fn ambient_identity_material(&self, _provider: &str) -> String {
                String::new()
            }
        }

        let dir = tempfile::TempDir::new().unwrap();
        let agent_dir = dir.path().join("agent");
        std::fs::create_dir_all(&agent_dir).unwrap();
        std::fs::write(
            agent_dir.join("models.json"),
            json!({
                "providers": {
                    "test-provider": {
                        "baseUrl": "http://localhost:9",
                        "apiKey": "test-key",
                        "api": "openai-completions",
                        "models": [
                            { "id": "glm-5.3", "name": "GLM 5.3", "contextWindow": 1000, "maxTokens": 100 },
                            { "id": "glm-5.3-turbo", "name": "GLM Turbo", "contextWindow": 1000, "maxTokens": 100 }
                        ]
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        let auth = crate::auth::AuthStorage::in_memory_with_env(
            &crate::auth::types::AuthStorageData::default(),
            Arc::new(crate::auth::NoOAuth),
            Arc::new(NoEnvCredentials),
        );
        let registry = Arc::new(ModelRegistry::create(auth, agent_dir.join("models.json")));
        let catalog = Arc::new(ScriptCatalog::new());
        let host = Arc::new(InProcessRlmHost::new(InProcessRlmHostConfig {
            agent_dir: agent_dir.clone(),
            registry: Arc::clone(&registry),
            stream_fn_factory: catalog.factory(),
            rlm_depth: depth,
            rlm_max_depth: max_depth,
            default_thinking: None,
            remote_family: None,
            root_runtime_kind: None,
        }));
        let sessions_dir = agent_dir.join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();
        let mut session_manager = SessionManager::persisted(dir.path(), &sessions_dir);
        session_manager.new_session(&NewSessionOptions {
            id: None,
            parent_session: None,
            rlm_depth: Some(u64::from(depth)),
        });
        let engine = Arc::new(
            create_session(SessionEngineConfig {
                cwd: dir.path().to_path_buf(),
                agent_dir: agent_dir.clone(),
                model: Some(script_model("glm-5.3")),
                stream_fn: Some(catalog.stream_fn("glm-5.3")),
                session_manager: Some(session_manager),
                rlm_depth: Some(depth),
                rlm_subagent_host: Some(Arc::clone(&host) as Arc<dyn RlmSubagentHost>),
                extra_host_handlers: Some(host.family_host_handlers()),
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        host.bind_parent(Arc::clone(&engine)).await;
        Self {
            _dir: dir,
            agent_dir,
            catalog,
            host,
            engine,
        }
    }

    /// One parent turn: queues a scripted parent reply, admits it, and
    /// waits for the run to settle — bumping the turn boundary the
    /// detached child prompts wait on.
    async fn run_parent_turn(&self, reply: &str) {
        self.catalog.provider("glm-5.3").push_text_turn(reply);
        self.engine
            .session
            .prompt("continue", crate::session_engine::PromptOptions::default())
            .await
            .unwrap();
        self.engine.session.agent().wait_for_idle().await;
    }

    /// The first child record (tests spawn at most one live child before
    /// reading it).
    async fn first_child(&self) -> Arc<super::registry::InProcessChildRecord> {
        self.host.children().await.remove(0)
    }

    /// The parent's persisted message/custom rows.
    async fn parent_rows(&self) -> Vec<pa_types::session::FileEntry> {
        self.engine
            .session
            .shared_persistence()
            .lock()
            .await
            .get_entries()
    }
}

fn spawn_request(name: Option<&str>, model: Option<&str>) -> RlmSpawnRequest {
    RlmSpawnRequest {
        prompt: "ship the lane".to_string(),
        name: name.map(str::to_string),
        model: model.map(str::to_string),
        thinking: None,
        cell_source_code: None,
    }
}

/// The resolved entry of one `rlm.collect` reply.
fn one(collected: &[RlmChildResult]) -> &RlmChildResult {
    collected.first().expect("one collect result")
}

#[tokio::test]
async fn spawn_admits_settles_and_collects() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_text_turn("child answer");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("worker"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    assert!(handle.rlm_child_id.starts_with("sub-"), "{handle:?}");
    assert_eq!(handle.name, "worker");
    assert_eq!(handle.model, "test-provider/glm-5.3-turbo");
    assert!(Path::new(&handle.session_dir).is_dir());
    // The roster answers immediately: the child is registered and running
    // — no parent turn gates it (TS starts the detached runtime at once).
    let roster = rig.host.list_subagents().await.unwrap();
    assert_eq!(roster.len(), 1);
    assert_eq!(roster[0].status, "running");
    // The child runs and settles on its own; collect waits for the settle.
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    let result = one(&collected);
    assert_eq!(result.status, "done");
    assert!(result.settled);
    assert_eq!(result.answer_preview.as_deref(), Some("child answer"));
    assert_eq!(result.session_name.as_deref(), Some("worker"));
    // The roster row settles too, with the child's durable identity.
    let roster = rig.host.list_subagents().await.unwrap();
    assert_eq!(roster[0].status, "completed");
    assert_eq!(roster[0].tool_use_count, Some(0));
    assert!(roster[0].session_id.is_some());
    // The child session persisted under the parent's artifacts tree with
    // the durable parent edge and depth.
    let header = std::fs::read_to_string(
        Path::new(&handle.session_dir)
            .join(format!("{}.jsonl", roster[0].session_id.clone().unwrap())),
    )
    .unwrap();
    let header: serde_json::Value = serde_json::from_str(header.lines().next().unwrap()).unwrap();
    assert_eq!(header["type"], "session");
    assert_eq!(header["rlmDepth"], 1);
    assert_eq!(
        header["parentSession"],
        rig.engine
            .session
            .shared_persistence()
            .lock()
            .await
            .get_session_file()
            .unwrap()
            .display()
            .to_string()
    );
    // Every child selector form collects.
    for target in [handle.rlm_child_id.as_str(), "worker"] {
        let collected = rig.host.collect(vec![target.to_string()], 0).await.unwrap();
        assert_eq!(one(&collected).status, "done");
    }
    // The unknown selector keeps the TS error.
    let error = rig
        .host
        .collect(vec!["ghost".to_string()], 0)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "No direct RLM child matches \"ghost\" in the current parent session"
    );
}

#[tokio::test]
async fn duplicate_names_refuse_and_failed_admissions_release() {
    let rig = TestRig::new().await;
    rig.catalog.provider("glm-5.3-turbo").push_text_turn("ok");
    rig.host
        .spawn(spawn_request(
            Some("dup"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let error = rig
        .host
        .spawn(spawn_request(
            Some("dup"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Agent name \"dup\" is unavailable: an agent of that name already exists at depth 1 under this parent"
    );
    // A failed admission released its reservation: the name admits after
    // the failure.
    let error = rig
        .host
        .spawn(spawn_request(Some("fresh"), Some("missing/model")))
        .await
        .unwrap_err();
    assert!(
        error.to_string().starts_with(
            "Requested subagent model \"missing/model\" is unavailable, unauthenticated, or expired"
        ),
        "{error}"
    );
    rig.host
        .spawn(spawn_request(
            Some("fresh"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn depth_gate_refuses_with_the_ts_error() {
    // A parent already at the bound cannot spawn (children sit one level
    // deeper).
    let rig = TestRig::with_depth(DEFAULT_RLM_MAX_DEPTH, DEFAULT_RLM_MAX_DEPTH).await;
    let error = rig
        .host
        .spawn(spawn_request(None, Some("test-provider/glm-5.3-turbo")))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "RLM recursion depth limit reached (RLM_DEPTH=2, RLM_MAX_DEPTH=2)"
    );
}

/// Poll an async probe until it returns `true`, or panic with `what`
/// after the deadline (settle paths run detached).
async fn eventually<F, Fut>(what: &str, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if probe().await {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn model_resolution_and_thinking_errors_match_ts() {
    let rig = TestRig::new().await;
    // A short-form reference resolves through the catalog.
    let handle = rig
        .host
        .spawn(spawn_request(None, Some("glm-5.3-turbo")))
        .await
        .unwrap();
    assert_eq!(handle.model, "test-provider/glm-5.3-turbo");
    // A requested thinking level the model does not support fails with
    // the TS message (the catalog models are non-reasoning).
    let error = rig
        .host
        .spawn(RlmSpawnRequest {
            thinking: Some("high".to_string()),
            ..spawn_request(None, Some("test-provider/glm-5.3-turbo"))
        })
        .await
        .unwrap_err();
    assert!(
        error.to_string().starts_with(
            "Requested thinking level \"high\" is not supported by model \"test-provider/glm-5.3-turbo\"; supported levels:"
        ),
        "{error}"
    );
    // An unbound host refuses with the binding error.
    let unbound = InProcessRlmHost::new(InProcessRlmHostConfig {
        agent_dir: rig.agent_dir.clone(),
        registry: Arc::clone(&rig.host.config().registry),
        stream_fn_factory: rig.catalog.factory(),
        rlm_depth: 0,
        rlm_max_depth: 0,
        default_thinking: None,
        remote_family: None,
        root_runtime_kind: None,
    });
    let error = unbound.spawn(spawn_request(None, None)).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "the in-process RLM host has no parent session bound yet"
    );
}

#[tokio::test]
async fn create_session_refuses_with_the_ts_error() {
    let rig = TestRig::new().await;
    let error = rig
        .host
        .create_session(RlmCreateSessionRequest {
            prompt: "root".to_string(),
            name: None,
            model: None,
            thinking: None,
            cwd: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "rlm.create_session requires a daemon-backed depth-0 session"
    );
}

#[tokio::test]
async fn collect_timeout_returns_running_snapshots() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("partial");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("slow"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    // The stalled child starts immediately and stays running: a
    // zero-timeout collect returns its snapshot, never an error.
    let collected = rig.host.collect(vec![], 0).await.unwrap();
    let result = one(&collected);
    assert_eq!(result.status, "running");
    assert!(!result.settled);
    // A bounded collect on the still-running child also returns its
    // snapshot after the budget.
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 50)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "running");
    // Cleanup: the delete path aborts the stalled run.
    let deleted = rig
        .host
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .unwrap();
    assert_eq!(deleted.outcome, Some("deleted"));
    assert_eq!(deleted.subagent.status, "cancelled");
    // The tombstone answers the deleted selector with the settled
    // cancelled envelope.
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .unwrap();
    let result = one(&collected);
    assert_eq!(result.status, "cancelled");
    assert!(result.settled);
    assert_eq!(
        result.error.as_deref(),
        Some("Deleted by parent orchestrator")
    );
    assert!(rig.host.list_subagents().await.unwrap().is_empty());
}

#[tokio::test]
async fn deleting_a_running_child_delivers_the_cancelled_notice() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("partial");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("doomed"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    // The delete path owns the cancellation row: it claims the notice (no
    // pre-marked flag suppresses it) and admits it on the idle parent.
    rig.catalog
        .provider("glm-5.3")
        .push_text_turn("notice turn");
    // The engine probe captures its weak before the delete clears the
    // registry: the deleted RUNNING child must also release (the abort
    // ends its run, the run task releases the listener, the engine tears
    // down).
    let engine_weak = {
        let child = rig.first_child().await;
        let weak = Arc::downgrade(&child.engine);
        assert!(weak.upgrade().is_some());
        weak
    };
    let deleted = rig
        .host
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .unwrap();
    assert_eq!(deleted.outcome, Some("deleted"));
    // The cancellation notice admitted BEFORE the settled state landed, so
    // the delete receipt itself implies it: at the receipt's return the
    // notice turn is streaming or its row already persisted — no
    // polling-ahead window exists for a concurrent collect.
    let streaming = rig.engine.session.agent().state().await.is_streaming;
    let notice_admitted = rig.parent_rows().await.iter().any(|entry| {
        matches!(
            entry,
            pa_types::session::FileEntry::CustomMessage { payload, .. }
                if payload.custom_type
                    == crate::session_engine::rlm_notices::RLM_CHILD_TERMINAL_NOTICE_CUSTOM_TYPE
        )
    });
    assert!(
        streaming || notice_admitted,
        "the delete receipt implies the cancelled notice admission"
    );
    eventually("the deleted running child's engine to drop", || {
        let probe = engine_weak.clone();
        async move { probe.upgrade().is_none() }
    })
    .await;
    let parent = Arc::clone(&rig.engine);
    let target_id = handle.rlm_child_id.clone();
    eventually("the cancelled notice row", move || {
        let parent = Arc::clone(&parent);
        let target_id = target_id.clone();
        async move {
            let rows = parent
                .session
                .shared_persistence()
                .lock()
                .await
                .get_entries();
            rows.iter().any(|entry| {
                matches!(
                    entry,
                    pa_types::session::FileEntry::CustomMessage { payload, .. }
                        if payload.custom_type
                            == crate::session_engine::rlm_notices::RLM_CHILD_TERMINAL_NOTICE_CUSTOM_TYPE
                            && payload.details.as_ref()
                                .and_then(|details| details.get("childId"))
                                .and_then(serde_json::Value::as_str)
                                == Some(target_id.as_str())
                )
            })
        }
    })
    .await;
}

#[tokio::test]
async fn a_deleted_settled_child_releases_its_engine() {
    let rig = TestRig::new().await;
    rig.catalog.provider("glm-5.3-turbo").push_text_turn("done");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("ephemeral"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "done");
    // The child engine is reachable through the record before the delete.
    let engine_weak = {
        let child = rig.first_child().await;
        let weak = Arc::downgrade(&child.engine);
        assert!(weak.upgrade().is_some());
        weak
    };
    rig.host
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .unwrap();
    // The registry dropped the record and the run task released the event
    // listener: the engine (and its kernel) tears down instead of leaking
    // through the agent's listener list.
    eventually("the deleted child's engine to drop", || {
        let probe = engine_weak.clone();
        async move { probe.upgrade().is_none() }
    })
    .await;
}

#[tokio::test]
async fn dropping_the_parent_engine_closes_the_running_subtree() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("child partial");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("subtree"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let child = rig.first_child().await;
    // A grandchild runs under the child (the cascade must reach it too).
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("grand partial");
    let _grand = child
        .child_host
        .spawn(spawn_request(
            Some("grand"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let child_weak = Arc::downgrade(&child.engine);
    // Scope the strong probe Arcs: the weaks must be the only way the
    // test observes the engines after the teardown.
    let grand_weak = {
        let grand_engine = child
            .child_host
            .children()
            .await
            .first()
            .map(|record| Arc::clone(&record.engine))
            .expect("the grandchild");
        Arc::downgrade(&grand_engine)
    };
    drop(child);
    // Parent teardown: nothing calls close_children — the binding weak
    // dies with the engine, and the detached run tasks close their own
    // runs and their descendant subtrees within a settle slice.
    drop(rig);
    eventually("the child engine to drop", || {
        let probe = child_weak.clone();
        async move { probe.upgrade().is_none() }
    })
    .await;
    eventually("the grandchild engine to drop", || {
        let probe = grand_weak.clone();
        async move { probe.upgrade().is_none() }
    })
    .await;
    let _ = handle;
}

#[tokio::test]
async fn deleting_a_child_closes_its_own_running_descendants() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("child partial");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("parent-of-grand"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    // A grandchild runs under the child; the parent-side delete only
    // closes the child record — the closed watch must make the child's
    // run task tear down its own descendant subtree.
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("grand partial");
    let child = rig.first_child().await;
    child
        .child_host
        .spawn(spawn_request(
            Some("grand"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let child_weak = Arc::downgrade(&child.engine);
    let grand_weak = {
        let grand = child
            .child_host
            .children()
            .await
            .first()
            .map(|record| Arc::clone(&record.engine))
            .expect("the grandchild");
        Arc::downgrade(&grand)
    };
    drop(child);
    rig.host
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .unwrap();
    eventually("the deleted child's engine to drop", || {
        let probe = child_weak.clone();
        async move { probe.upgrade().is_none() }
    })
    .await;
    eventually("the grandchild engine to drop through the cascade", || {
        let probe = grand_weak.clone();
        async move { probe.upgrade().is_none() }
    })
    .await;
}

#[tokio::test]
async fn a_notice_to_a_busy_parent_delivers_with_no_future_user_turn() {
    let rig = TestRig::new().await;
    // The parent runs a stalled turn; the child fails instantly, so its
    // failure notice fires while the parent is busy. The notice must NOT
    // ride the steering lane (an aborted run never drains it) nor wait
    // for a future user prompt: the host's deferred admission task admits
    // it as the parent's own turn once the current run ends — here by
    // ABORT, with no further prompt from the test.
    rig.catalog.provider("glm-5.3").push_stalled_turn("busy");
    rig.engine
        .session
        .prompt(
            "hold the line",
            crate::session_engine::PromptOptions {
                return_after_accepted: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let parent = Arc::clone(&rig.engine);
    eventually("the parent to stream its stalled turn", move || {
        let parent = Arc::clone(&parent);
        async move { parent.session.agent().state().await.is_streaming }
    })
    .await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_fail_start_turn("child exploded");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("failing"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    // Abort the busy parent — the ONLY thing that ends its run. No user
    // turn is ever prompted afterwards; the deferred admission task must
    // deliver the notice on its own.
    rig.catalog
        .provider("glm-5.3")
        .push_text_turn("notice reply");
    rig.engine.session.agent().abort();
    let parent = Arc::clone(&rig.engine);
    let target_id = handle.rlm_child_id.clone();
    eventually("the failure notice row after the abort", move || {
        let parent = Arc::clone(&parent);
        let target_id = target_id.clone();
        async move {
            let rows = parent
                .session
                .shared_persistence()
                .lock()
                .await
                .get_entries();
            rows.iter().any(|entry| {
                matches!(
                    entry,
                    pa_types::session::FileEntry::CustomMessage { payload, .. }
                        if payload.custom_type
                            == crate::session_engine::rlm_notices::RLM_CHILD_FAILURE_CUSTOM_TYPE
                            && payload.details.as_ref()
                                .and_then(|details| details.get("childId"))
                                .and_then(serde_json::Value::as_str)
                                == Some(target_id.as_str())
                )
            })
        }
    })
    .await;
    // The delivered row never rode the steering lane (the failed steer
    // design is gone): nothing is queued on it.
    assert!(rig.engine.session.agent().steering_previews().is_empty());
}

#[tokio::test]
async fn a_delete_on_a_busy_parent_settles_then_delivers_the_cancelled_notice() {
    let rig = TestRig::new().await;
    // The parent is busy when the delete's cancellation notice fires, so
    // the notice is durably scheduled (the deferred admission task) and
    // the delete settles its receipt; the notice delivers once the busy
    // turn ends — no user turn after the abort.
    rig.catalog.provider("glm-5.3").push_stalled_turn("busy");
    rig.engine
        .session
        .prompt(
            "hold the line",
            crate::session_engine::PromptOptions {
                return_after_accepted: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let parent = Arc::clone(&rig.engine);
    eventually("the parent to stream its stalled turn", move || {
        let parent = Arc::clone(&parent);
        async move { parent.session.agent().state().await.is_streaming }
    })
    .await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("child partial");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("doomed"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let deleted = rig
        .host
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .unwrap();
    // The receipt settles (the notice is durably scheduled — single
    // ownership holds: the run task never wrote the verdict, so no
    // collect could observe the settle before this scheduling).
    assert_eq!(deleted.outcome, Some("deleted"));
    assert_eq!(deleted.subagent.status, "cancelled");
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "cancelled");
    // End the busy turn WITHOUT a user prompt: the deferred task delivers.
    rig.catalog
        .provider("glm-5.3")
        .push_text_turn("notice reply");
    rig.engine.session.agent().abort();
    let parent = Arc::clone(&rig.engine);
    let target_id = handle.rlm_child_id.clone();
    eventually("the cancelled notice row after the abort", move || {
        let parent = Arc::clone(&parent);
        let target_id = target_id.clone();
        async move {
            let rows = parent
                .session
                .shared_persistence()
                .lock()
                .await
                .get_entries();
            rows.iter().any(|entry| {
                matches!(
                    entry,
                    pa_types::session::FileEntry::CustomMessage { payload, .. }
                        if payload.custom_type
                            == crate::session_engine::rlm_notices::RLM_CHILD_TERMINAL_NOTICE_CUSTOM_TYPE
                            && payload.details.as_ref()
                                .and_then(|details| details.get("childId"))
                                .and_then(serde_json::Value::as_str)
                                == Some(target_id.as_str())
                )
            })
        }
    })
    .await;
}

#[tokio::test]
async fn an_error_run_settles_only_after_its_failure_notice() {
    let rig = TestRig::new().await;
    // The child's only scripted turn fails at the stream call: the prompt
    // admission itself errors and the run task takes the error verdict.
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_fail_start_turn("provider exploded");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("broken"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    rig.catalog
        .provider("glm-5.3")
        .push_text_turn("notice turn");
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    let result = one(&collected);
    assert_eq!(result.status, "error");
    assert!(result.settled);
    // The settled state exposed by the collect already implies the
    // failure notice was admitted (streaming) or its row persisted.
    let streaming = rig.engine.session.agent().state().await.is_streaming;
    let failure_admitted = rig.parent_rows().await.iter().any(|entry| {
        matches!(
            entry,
            pa_types::session::FileEntry::CustomMessage { payload, .. }
                if payload.custom_type
                    == crate::session_engine::rlm_notices::RLM_CHILD_FAILURE_CUSTOM_TYPE
        )
    });
    assert!(
        streaming || failure_admitted,
        "the failure notice admission precedes the error settle"
    );
    let parent = Arc::clone(&rig.engine);
    eventually("the failure notice row", move || {
        let parent = Arc::clone(&parent);
        async move {
            let rows = parent
                .session
                .shared_persistence()
                .lock()
                .await
                .get_entries();
            rows.iter().any(|entry| {
                matches!(
                    entry,
                    pa_types::session::FileEntry::CustomMessage { payload, .. }
                        if payload.custom_type
                            == crate::session_engine::rlm_notices::RLM_CHILD_FAILURE_CUSTOM_TYPE
                )
            })
        }
    })
    .await;
}

#[tokio::test]
async fn child_usage_attributes_into_the_parent_row() {
    let rig = TestRig::new().await;
    rig.run_parent_turn("parent's spawning turn").await;
    rig.catalog.provider("glm-5.3-turbo").push_text_turn("done");
    let handle = rig
        .host
        .spawn(spawn_request(None, Some("test-provider/glm-5.3-turbo")))
        .await
        .unwrap();
    // The kernel's `rlm.run` handler registers the spawn target on the
    // parent's producer; the direct host call does the same registration
    // here so the child's batches fold.
    rig.engine
        .rlm_usage
        .register_spawn(&handle.rlm_child_id)
        .await;
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "done");
    let attributions = rig
        .parent_rows()
        .await
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                pa_types::session::FileEntry::ChildUsageAttributed { .. }
            )
        })
        .count();
    assert_eq!(attributions, 1, "one per-origin attribution row");
}

#[tokio::test]
async fn progress_notes_surface_in_the_roster() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("partial");
    let handle = rig
        .host
        .spawn(spawn_request(None, Some("test-provider/glm-5.3-turbo")))
        .await
        .unwrap();
    // The child's own `rlm.progress.note` store feeds the roster row.
    let child = rig.first_child().await;
    let _ = child.engine.rlm.notes.note("halfway there", 1_000).await;
    let roster = rig.host.list_subagents().await.unwrap();
    assert_eq!(roster[0].progress_note.as_deref(), Some("halfway there"));
    let _ = rig.host.delete_subagent(handle.rlm_child_id).await;
}

#[tokio::test]
async fn collect_settles_only_after_the_notice_is_admitted() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_text_turn("child done");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("quiet"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    // The child settles without an agent-message reply; the settle
    // signal publishes only after the no-reply notice is admitted on the
    // parent, so at the collect return the notice turn is already
    // admitted (streaming) or its row already persisted.
    rig.catalog
        .provider("glm-5.3")
        .push_text_turn("notice reply");
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "done");
    let streaming = rig.engine.session.agent().state().await.is_streaming;
    let notice_admitted = rig.parent_rows().await.iter().any(|entry| {
        matches!(
            entry,
            pa_types::session::FileEntry::CustomMessage { payload, .. }
                if payload.custom_type
                    == crate::session_engine::rlm_notices::RLM_CHILD_TERMINAL_NOTICE_CUSTOM_TYPE
        )
    });
    assert!(
        streaming || notice_admitted,
        "the no-reply notice admission precedes the collect settle"
    );
    let parent = Arc::clone(&rig.engine);
    eventually("the no-reply notice row", move || {
        let parent = Arc::clone(&parent);
        async move {
            let rows = parent
                .session
                .shared_persistence()
                .lock()
                .await
                .get_entries();
            rows.iter().any(|entry| {
                matches!(
                    entry,
                    pa_types::session::FileEntry::CustomMessage { payload, .. }
                        if payload.custom_type
                            == crate::session_engine::rlm_notices::RLM_CHILD_TERMINAL_NOTICE_CUSTOM_TYPE
                )
            })
        }
    })
    .await;
}

#[tokio::test]
async fn a_child_reply_suppresses_the_notice() {
    let rig = TestRig::new().await;
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("working");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("chatty"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    // The child's family controller: the parent is its one family member.
    let child = rig.first_child().await;
    let controller = InProcessFamilyController::new(
        Arc::clone(&child.child_host),
        FamilySelf::Child {
            child_id: handle.rlm_child_id.clone(),
        },
    );
    let family = controller.family().await.unwrap();
    assert_eq!(family.len(), 1);
    assert_eq!(family[0].id, rig.engine.session.session_id().await);
    assert_eq!(family[0].relationship.as_str(), "parent");
    // The requested name is durable session state: the child's own file
    // carries the session-info row, and the parent binding reads it.
    let child_session_name = child
        .engine
        .session
        .shared_persistence()
        .lock()
        .await
        .get_session_name();
    assert_eq!(child_session_name.as_deref(), Some("chatty"));
    // The reply delivers as the parent's own turn; the parent's script
    // needs that turn queued.
    rig.catalog.provider("glm-5.3").push_text_turn("parent ack");
    let receipt = controller
        .send_agent_message(AgentMessageSendInput {
            target: family[0].id.clone(),
            message: "task done, shipping the report".to_string(),
            receiver_role: None,
        })
        .await
        .unwrap();
    assert_eq!(receipt.delivery_status.as_str(), "delivered");
    rig.engine.session.agent().wait_for_idle().await;
    // The reply landed on the parent as the agent-message row.
    let rows = rig.parent_rows().await;
    let reply_rows = rows
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                pa_types::session::FileEntry::CustomMessage { payload, .. }
                    if payload.custom_type
                        == crate::session_engine::agent_messaging::AGENT_MESSAGE_CUSTOM_TYPE
            )
        })
        .count();
    assert_eq!(reply_rows, 1);
    // The child's reply flipped the record's replied flag.
    assert!(child.state().await.replied_since_task);
    // The abort settles the stalled run; the no-reply notice is withheld.
    // Wait until the child's stalled run is actually streaming (an abort
    // before the run registers is a no-op).
    let streaming_child = Arc::clone(&child.engine);
    eventually("the child streams", move || {
        let engine = Arc::clone(&streaming_child);
        async move { engine.session.agent().state().await.is_streaming }
    })
    .await;
    child.engine.session.agent().abort();
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "done");
    rig.run_parent_turn("drain").await;
    let notice_rows = rig
        .parent_rows()
        .await
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                pa_types::session::FileEntry::CustomMessage { payload, .. }
                    if payload.custom_type
                        == crate::session_engine::rlm_notices::RLM_CHILD_TERMINAL_NOTICE_CUSTOM_TYPE
            )
        })
        .count();
    assert_eq!(notice_rows, 0, "the reply suppressed the notice");
}

#[tokio::test]
async fn the_parent_reaches_and_observes_its_children() {
    let rig = TestRig::new().await;
    rig.catalog.provider("glm-5.3-turbo").push_text_turn("done");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("solo"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "done");
    // The parent's controller sees the child as its family.
    let controller = InProcessFamilyController::new(Arc::clone(&rig.host), FamilySelf::Root);
    let family = controller.family().await.unwrap();
    assert_eq!(family.len(), 1);
    assert_eq!(family[0].id, handle.rlm_child_id);
    assert_eq!(family[0].name.as_deref(), Some("solo"));
    // The observe roster: the parent (current) plus the child.
    let agents = controller.list_agents().await.unwrap();
    assert_eq!(agents.len(), 2);
    assert!(agents.iter().any(|agent| agent.is_current));
    let parent_session_id = rig.engine.session.session_id().await;
    let child_row = agents
        .iter()
        .find(|agent| agent.session_id != parent_session_id)
        .expect("the child summary")
        .clone();
    assert_eq!(
        child_row.relationship.map(|role| role.as_str()),
        Some("child")
    );
    assert_eq!(child_row.runtime_kind.as_deref(), Some("subagent"));
    // The parent messages the child by name: delivered as the child's
    // own turn.
    let child = rig.first_child().await;
    let _ = child.engine.rlm.notes.note("unrelated", 1).await;
    rig.catalog.provider("glm-5.3-turbo").push_text_turn("ack");
    let receipt = controller
        .send_agent_message(AgentMessageSendInput {
            target: "solo".to_string(),
            message: "status?".to_string(),
            receiver_role: None,
        })
        .await
        .unwrap();
    assert_eq!(receipt.delivery_status.as_str(), "delivered");
    child.engine.session.agent().wait_for_idle().await;
    // The child's transcript carries the rendered prompt row.
    let child_entries = child
        .engine
        .session
        .shared_persistence()
        .lock()
        .await
        .get_entries();
    let has_row = child_entries.iter().any(|entry| {
        matches!(
            entry,
            pa_types::session::FileEntry::CustomMessage { payload, .. }
                if payload.custom_type
                    == crate::session_engine::agent_messaging::AGENT_MESSAGE_CUSTOM_TYPE
        )
    });
    assert!(has_row, "the delivered row persisted on the child");
    // The child's recent messages are observable from the parent.
    let recent = controller
        .recent_messages(&child_row.session_id, 10, 800)
        .await
        .unwrap();
    assert!(!recent.is_empty());
}

#[tokio::test]
async fn an_idle_target_send_returns_at_admission() {
    let rig = TestRig::new().await;
    // The child stalls mid-turn; the parent sends it a message while it
    // is busy: the receipt returns when the steering lane accepts the
    // row, without waiting the child's (never-ending) model turn.
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("partial");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("busy"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let child = rig.first_child().await;
    eventually("the child streams", move || {
        let engine = Arc::clone(&child.engine);
        async move { engine.session.agent().state().await.is_streaming }
    })
    .await;
    let controller = InProcessFamilyController::new(Arc::clone(&rig.host), FamilySelf::Root);
    let receipt = controller
        .send_agent_message(AgentMessageSendInput {
            target: handle.rlm_child_id.clone(),
            message: "status?".to_string(),
            receiver_role: None,
        })
        .await
        .unwrap();
    assert_eq!(receipt.delivery_status.as_str(), "queued");
    // An IDLE target admits the injected row and returns at admission:
    // the sender resumes while the target's model turn still runs.
    rig.catalog
        .provider("glm-5.3-turbo")
        .push_stalled_turn("reply");
    let child2 = rig.first_child().await;
    child2.engine.session.agent().abort();
    child2.engine.session.agent().wait_for_idle().await;
    let controller = InProcessFamilyController::new(
        Arc::clone(&child2.child_host),
        FamilySelf::Child {
            child_id: handle.rlm_child_id.clone(),
        },
    );
    let parent_id = rig.engine.session.session_id().await;
    rig.catalog
        .provider("glm-5.3")
        .push_stalled_turn("parent turn");
    let started = std::time::Instant::now();
    let receipt = controller
        .send_agent_message(AgentMessageSendInput {
            target: parent_id,
            message: "reply while the parent streams".to_string(),
            receiver_role: None,
        })
        .await
        .unwrap();
    // The receipt landed in well under a model turn, while the parent's
    // admitted turn is still streaming (admission, not run completion).
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(receipt.delivery_status.as_str(), "delivered");
    assert!(
        rig.engine.session.agent().state().await.is_streaming,
        "the send returned before the target's model turn finished"
    );
    rig.engine.session.agent().abort();
}

#[tokio::test]
async fn the_child_session_name_lands_in_the_session_file() {
    let rig = TestRig::new().await;
    rig.catalog.provider("glm-5.3-turbo").push_text_turn("done");
    let handle = rig
        .host
        .spawn(spawn_request(
            Some("named-worker"),
            Some("test-provider/glm-5.3-turbo"),
        ))
        .await
        .unwrap();
    let collected = rig
        .host
        .collect(vec![handle.rlm_child_id.clone()], 10_000)
        .await
        .unwrap();
    assert_eq!(one(&collected).status, "done");
    // The durable file carries the session-info row (a resumed child file
    // reads its own name), not just the registry metadata.
    let child = rig.first_child().await;
    let entries = child
        .engine
        .session
        .shared_persistence()
        .lock()
        .await
        .get_entries();
    let name_row = entries.iter().find_map(|entry| match entry {
        pa_types::session::FileEntry::SessionInfo { payload, .. } => payload.name.clone(),
        _ => None,
    });
    assert_eq!(name_row.as_deref(), Some("named-worker"));
    // The child's own observe row reports the durable name.
    let controller = InProcessFamilyController::new(
        Arc::clone(&child.child_host),
        FamilySelf::Child {
            child_id: handle.rlm_child_id.clone(),
        },
    );
    let agents = controller.list_agents().await.unwrap();
    let self_row = agents
        .iter()
        .find(|agent| agent.is_current)
        .expect("the child's own observe row");
    assert_eq!(self_row.session_name.as_deref(), Some("named-worker"));
}

#[tokio::test]
async fn a_composed_remote_family_routes_sends_beyond_the_local_graph() {
    struct RemoteSurface {
        members: Vec<crate::session_engine::agent_messaging::AgentFamilyMember>,
        sent: Mutex<Vec<String>>,
    }
    impl RemoteSurface {
        /// One remote observe row for the roster join (the seam's optional
        /// observe surface).
        fn remote_row() -> crate::session_engine::agent_messaging::AgentObserveSummary {
            crate::session_engine::agent_messaging::AgentObserveSummary {
                active_session_id: Some("cloud-parent".to_string()),
                session_id: "cloud-parent".to_string(),
                session_name: Some("cloud-parent".to_string()),
                relationship: Some(
                    crate::session_engine::agent_messaging::AgentFamilyRelationship::Parent,
                ),
                runtime_kind: Some("top-level".to_string()),
                status: crate::session_engine::agent_messaging::AgentFamilyStatus::Idle,
                activity: None,
                is_current: false,
                is_streaming: false,
                is_compacting: false,
                attached_clients: 0,
                queued_count: 0,
                is_session_active: true,
            }
        }
    }
    impl super::RlmRemoteFamily for RemoteSurface {
        fn members(
            &self,
        ) -> crate::session_engine::rlm_host::RlmHostFuture<
            Vec<crate::session_engine::agent_messaging::AgentFamilyMember>,
        > {
            let members = self.members.clone();
            Box::pin(async move { Ok(members) })
        }
        fn observe_summaries(
            &self,
        ) -> crate::session_engine::rlm_host::RlmHostFuture<
            Vec<crate::session_engine::agent_messaging::AgentObserveSummary>,
        > {
            Box::pin(async move { Ok(vec![Self::remote_row()]) })
        }
        fn send(
            &self,
            input: AgentMessageSendInput,
        ) -> crate::session_engine::rlm_host::RlmHostFuture<
            crate::session_engine::agent_messaging::AgentMessageReceipt,
        > {
            self.sent.lock().unwrap().push(input.message.clone());
            Box::pin(async move {
                Ok(crate::session_engine::agent_messaging::AgentMessageReceipt {
                    id: "remote".to_string(),
                    target: "cloud-parent".to_string(),
                    target_session_id: None,
                    target_session_name: Some("cloud-parent".to_string()),
                    target_runtime_kind: Some("top-level".to_string()),
                    message: input.message,
                    delivery_status:
                        crate::session_engine::agent_messaging::AgentMessageDeliveryStatus::Delivered,
                    delivery_mode: None,
                    receiver_role: input.receiver_role,
                    delivered_at: None,
                    queued_at: None,
                })
            })
        }
    }
    // The remote seam is a composition concern of the ROOT host: a host
    // built without it stays local-only.
    let rig = TestRig::new().await;
    let controller = InProcessFamilyController::new(Arc::clone(&rig.host), FamilySelf::Root);
    let error = controller
        .send_agent_message(AgentMessageSendInput {
            target: "cloud-parent".to_string(),
            message: "up?".to_string(),
            receiver_role: None,
        })
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("No agent message target matches"),
        "{error}"
    );
    // A composed remote family extends the roster and routes non-local
    // sends through the seam.
    let remote = Arc::new(RemoteSurface {
        members: vec![crate::session_engine::agent_messaging::AgentFamilyMember {
            relationship: crate::session_engine::agent_messaging::AgentFamilyRelationship::Parent,
            id: "cloud-parent".to_string(),
            name: Some("cloud-parent".to_string()),
            aliases: Vec::new(),
        }],
        sent: Mutex::new(Vec::new()),
    });
    let host = Arc::new(InProcessRlmHost::new(super::InProcessRlmHostConfig {
        agent_dir: rig.agent_dir.clone(),
        registry: Arc::clone(&rig.host.config().registry),
        stream_fn_factory: rig.catalog.factory(),
        rlm_depth: 0,
        rlm_max_depth: 0,
        default_thinking: None,
        remote_family: Some(remote.clone() as Arc<dyn super::RlmRemoteFamily>),
        root_runtime_kind: Some("subagent".to_string()),
    }));
    host.bind_parent(Arc::clone(&rig.engine)).await;
    let controller = InProcessFamilyController::new(host, FamilySelf::Root);
    let family = controller.family().await.unwrap();
    assert!(family.iter().any(|member| member.id == "cloud-parent"));
    let receipt = controller
        .send_agent_message(AgentMessageSendInput {
            target: "cloud-parent".to_string(),
            message: "up?".to_string(),
            receiver_role: None,
        })
        .await
        .unwrap();
    assert_eq!(receipt.id, "remote");
    assert_eq!(
        remote.sent.lock().unwrap().as_slice(),
        ["up?".to_string()].as_slice()
    );
    // The seam's observe rows ride the roster, and a remote row resolves
    // through get_agent.
    let agents = controller.list_agents().await.unwrap();
    let remote_row = agents
        .iter()
        .find(|agent| agent.session_id == "cloud-parent")
        .expect("the remote observe row");
    assert!(!remote_row.is_current);
    let fetched = controller.get_agent("cloud-parent").await.unwrap();
    assert!(fetched.is_some_and(|agent| agent.session_id == "cloud-parent"));
    // The composed root kind surfaces on the root's own observe row.
    let agents = controller.list_agents().await.unwrap();
    let self_row = agents
        .iter()
        .find(|agent| agent.is_current)
        .expect("the root observe row");
    assert_eq!(self_row.runtime_kind.as_deref(), Some("subagent"));
}
