//! Decision API input, deferred auth, and cancellation regressions.
use super::*;
use crate::auth_panel::{AuthPanelHandle, AuthPanelRequest};
use crate::client_auth::{
    AuthFuture, AuthReadinessFuture, ClientAuthCommands, ClientAuthCommandsHandle,
};
use crate::interactive::{InteractiveOptions, ModelSelection, SessionSelection};
use crate::theme::{ColorMode, Theme};
use crossterm::event::{KeyCode, KeyModifiers};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

struct ControlledAuth {
    checks: mpsc::UnboundedSender<oneshot::Sender<bool>>,
    key_answers: Mutex<Vec<bool>>,
}
impl ClientAuthCommands for ControlledAuth {
    fn login(&self, _: &str, _: AuthPanelHandle) -> AuthFuture {
        Box::pin(async { unreachable!("Decision API never uses MCP OAuth login") })
    }
    fn paste_token(&self, _: &str, _: AuthPanelHandle) -> AuthFuture {
        Box::pin(async { unreachable!("Decision API never uses MCP paste flow") })
    }
    fn logout(&self, _: &str) -> AuthFuture {
        Box::pin(async { unreachable!("Decision API never uses MCP logout") })
    }
    fn api_key(&self, _: &str, _: AuthPanelHandle) -> AuthFuture {
        Box::pin(async { Ok("Saved test key".to_string()) })
    }
    fn api_key_ready(&self, _: &str) -> AuthReadinessFuture {
        if let Some(answer) = self.key_answers.lock().unwrap().pop() {
            return Box::pin(async move { Ok(answer) });
        }
        let (tx, rx) = oneshot::channel();
        self.checks.send(tx).unwrap();
        Box::pin(async move { Ok(rx.await?) })
    }
}

fn options() -> InteractiveOptions {
    InteractiveOptions {
        models: None,
        socket_path: PathBuf::from("/tmp/unused.sock"),
        cwd: PathBuf::from("/tmp"),
        session_dir: None,
        script_path: None,
        model_selection: ModelSelection::default(),
        model_catalog: Vec::new(),
        model_configured_providers: HashSet::default(),
        model_recent_models: Vec::new(),
        default_thinking_level: None,
        no_session: false,
        session: SessionSelection::Attach("s1".to_string()),
        initial_message: None,
        show_images: true,
        fullscreen_mouse: true,
        theme: "prime".to_string(),
        code_block_indent: "  ".to_string(),
        tree_filter_mode: String::new(),
        branch_summary_skip_prompt: false,
        version: "0.0.0".to_string(),
        onboarding: None,
        telemetry_disabled: None,
        client_auth: None,
        traces: None,
        provider_auth: None,
        update_commands: None,
        telemetry: None,
        keybindings: crate::keybindings::KeybindingsManager::new(),
        session_rlm_depth: None,
        prompt_stash: std::sync::Arc::default(),
        session_has_children: false,
        restore_dock_focus: false,
        client_settings: None,
    }
}

async fn fixture() -> (
    SessionUi,
    AgentView,
    mpsc::UnboundedReceiver<AuthPanelRequest>,
    mpsc::UnboundedReceiver<super::super::prompt::PromptOrder>,
) {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("tui.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7},\"serverCapabilities\":[],\"clientId\":\"test\"}\n").await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let request: serde_json::Value = serde_json::from_str(&line).unwrap();
            let command = &request["command"]["type"];
            let data = if command == "attach" {
                serde_json::json!({
                    "activeSessionId": "s1",
                    "snapshot": {"activeSessionId": "s1", "summary": {"id": "s1", "cwd": "/tmp"},
                        "state": {"sessionId": "durable-1", "isStreaming": true, "decisionApi": "clef"}, "messages": []}
                })
            } else {
                serde_json::json!({})
            };
            let response = serde_json::json!({"type":"response", "id":request["id"], "command":command, "success":true, "data":data});
            if writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let (client, _events) = crate::daemon_client::DaemonClient::connect(&socket)
        .await
        .unwrap();
    let (auth_tx, auth_rx) = mpsc::unbounded_channel();
    let ui = SessionUi::open(
        client,
        &options(),
        mpsc::unbounded_channel().0,
        mpsc::unbounded_channel().0,
        mpsc::unbounded_channel().0,
        mpsc::unbounded_channel().0,
        mpsc::unbounded_channel().0,
        mpsc::unbounded_channel().0,
        mpsc::unbounded_channel().0,
        mpsc::unbounded_channel().0,
        auth_tx,
        super::super::ActivityUpdates {
            heartbeats: mpsc::unbounded_channel().0,
            bash: mpsc::unbounded_channel().0,
            factory: mpsc::unbounded_channel().0,
            commands: mpsc::unbounded_channel().0,
        },
    )
    .await
    .unwrap();
    let mut ui = ui;
    let (orders, orders_rx) = mpsc::unbounded_channel();
    ui.prompt_orders = orders;
    let view = AgentView::new(Theme::builtin("prime", ColorMode::TrueColor));
    (ui, view, auth_rx, orders_rx)
}

async fn next<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn decision_picker_preserves_follow_up_and_active_provider() {
    let (mut ui, mut view, _, mut orders) = fixture().await;
    ui.handle_decision_api_command(
        "",
        "/decision-api",
        super::super::SubmitBehavior::FollowUp,
        &mut view,
    )
    .unwrap();
    let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    ui.handle_key(key, &mut view, &mut true).await.unwrap();
    let order = next(&mut orders).await;
    assert_eq!(
        (order.text.as_str(), order.behavior),
        ("/decision-api clef", super::super::SubmitBehavior::FollowUp)
    );
}

#[tokio::test]
async fn decision_first_key_completion_preserves_follow_up() {
    let (mut ui, mut view, mut requests, mut orders) = fixture().await;
    let (checks, _rx) = mpsc::unbounded_channel();
    ui.client_auth = Some(ClientAuthCommandsHandle(Arc::new(ControlledAuth {
        checks,
        key_answers: Mutex::new(vec![true, false]),
    })));
    ui.handle_decision_api_command(
        "jev",
        "/decision-api jev",
        super::super::SubmitBehavior::FollowUp,
        &mut view,
    )
    .unwrap();
    ui.apply_auth_panel_request(next(&mut requests).await, &mut view)
        .await;
    assert!(ui.pending_mcp_auth.is_some());
    ui.run_mcp_auth(&mut view);
    ui.apply_auth_panel_request(next(&mut requests).await, &mut view)
        .await;
    ui.apply_auth_panel_request(next(&mut requests).await, &mut view)
        .await;
    let order = next(&mut orders).await;
    assert_eq!(
        (order.text.as_str(), order.behavior),
        ("/decision-api jev", super::super::SubmitBehavior::FollowUp)
    );
}

#[tokio::test]
async fn stalled_decision_credential_resolution_allows_paint_and_cancel_and_ignores_late_completion(
) {
    let (mut ui, mut view, mut requests, mut orders) = fixture().await;
    let (checks, mut check_rx) = mpsc::unbounded_channel();
    ui.client_auth = Some(ClientAuthCommandsHandle(Arc::new(ControlledAuth {
        checks,
        key_answers: Mutex::new(vec![]),
    })));
    ui.handle_decision_api_command(
        "jev",
        "/decision-api jev",
        super::super::SubmitBehavior::FollowUp,
        &mut view,
    )
    .unwrap();
    let release = next(&mut check_rx).await;
    let old_generation = ui.mcp_auth_generation;
    assert!(!view.render_frame(80, 24).is_empty());
    ui.handle_auth_panel_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), &mut view)
        .unwrap();
    assert!(view.auth_panel.is_none());
    release.send(true).unwrap();
    ui.handle_decision_api_command(
        "clef",
        "/decision-api clef",
        super::super::SubmitBehavior::Steer,
        &mut view,
    )
    .unwrap();
    let release_new = next(&mut check_rx).await;
    ui.apply_auth_panel_request(
        AuthPanelRequest::DecisionApiReady {
            generation: old_generation,
            result: Ok(true),
        },
        &mut view,
    )
    .await;
    ui.apply_auth_panel_request(
        AuthPanelRequest::McpSettled {
            generation: old_generation,
            note: "Late saved key".to_string(),
        },
        &mut view,
    )
    .await;
    assert!(view.auth_panel.is_some());
    assert!(orders.try_recv().is_err());
    release_new.send(true).unwrap();
    ui.apply_auth_panel_request(next(&mut requests).await, &mut view)
        .await;
    let order = next(&mut orders).await;
    assert_eq!(
        (order.text.as_str(), order.behavior),
        ("/decision-api clef", super::super::SubmitBehavior::Steer)
    );
}

#[tokio::test]
async fn an_unusable_saved_key_reports_failure_without_submitting() {
    let (mut ui, mut view, mut requests, mut orders) = fixture().await;
    let (checks, _rx) = mpsc::unbounded_channel();
    ui.client_auth = Some(ClientAuthCommandsHandle(Arc::new(ControlledAuth {
        checks,
        key_answers: Mutex::new(vec![false, false]),
    })));
    ui.handle_decision_api_command(
        "jev",
        "/decision-api jev",
        super::super::SubmitBehavior::FollowUp,
        &mut view,
    )
    .unwrap();
    ui.apply_auth_panel_request(next(&mut requests).await, &mut view)
        .await;
    ui.run_mcp_auth(&mut view);
    ui.apply_auth_panel_request(next(&mut requests).await, &mut view)
        .await;
    ui.apply_auth_panel_request(next(&mut requests).await, &mut view)
        .await;
    let frame = view
        .render_frame(120, 30)
        .into_iter()
        .flatten()
        .map(|span| span.content)
        .collect::<String>();
    assert!(frame.contains("did not resolve to a usable key"), "{frame}");
    assert!(orders.try_recv().is_err());
    assert!(ui.decision_api_pending.is_none());
    assert!(view.auth_panel.is_none());
}
