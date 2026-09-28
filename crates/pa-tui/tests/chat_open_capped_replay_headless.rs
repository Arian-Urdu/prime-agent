//! Headless e2e for the capped initial replay (TS
//! `INITIAL_TRANSCRIPT_RENDER_MESSAGE_LIMIT`): an attach whose snapshot
//! carries 460 messages must fold the newest 400-message window (plus
//! the orphan-toolcall stitch TS `initialRenderMessages` performs),
//! carry the dim `Showing latest N of M messages for faster open.`
//! notice at the transcript head, and never render the rows the window
//! dropped. The under-limit arm pins the frozen surface: a short
//! transcript attaches whole with no notice row anywhere.
//!
//! The notice sits above the fold at attach exactly like TS (the
//! viewport follows the tail), so the plan scrolls the transcript to
//! its head (`HeadlessStep::ScrollTop`, the `tui.viewport.top` path)
//! before asserting the notice bytes.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use pa_tui::interactive::{
    run_interactive, HeadlessPlan, HeadlessStep, InteractiveOptions, ModelSelection,
    SessionSelection, UiMode,
};
use serde_json::{json, Value};

/// The transcript the attach serves: the capped 460-message shape or a
/// short three-message one.
enum Transcript {
    Capped,
    Small,
}

struct MockSupervisor {
    listener: UnixListener,
}

impl MockSupervisor {
    fn bind(socket: &std::path::Path) -> Self {
        MockSupervisor {
            listener: UnixListener::bind(socket).expect("bind mock socket"),
        }
    }

    fn serve(self, transcript: Transcript) {
        let (stream, _) = self.listener.accept().expect("accept");
        let write_stream = stream.try_clone().expect("clone mock socket");
        let mut writer = write_stream;
        let mut reader = BufReader::new(stream);

        write_json(
            &mut writer,
            &json!({
                "type": "daemon_hello",
                "protocol": { "name": "prime-agent.daemon", "version": 7 },
                "serverCapabilities": ["kernel_bash_activity"],
                "clientId": "mock",
            }),
        );

        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let Ok(envelope) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            let id = envelope.get("id").and_then(Value::as_str).unwrap_or("");
            let command = envelope.get("command").cloned().unwrap_or(Value::Null);
            let command_type = command
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            match command_type.as_str() {
                "attach" => {
                    let data = match transcript {
                        Transcript::Capped => attach_data_capped(id),
                        Transcript::Small => attach_data_small(id),
                    };
                    write_json(&mut writer, &data);
                }
                "heartbeats_list" => {
                    write_json(
                        &mut writer,
                        &json!({
                            "type": "response",
                            "id": id,
                            "command": "heartbeats_list",
                            "success": true,
                            "data": { "heartbeats": [] },
                        }),
                    );
                }
                "list_kernel_bash" => {
                    write_json(
                        &mut writer,
                        &json!({
                            "type": "response",
                            "id": id,
                            "command": "list_kernel_bash",
                            "success": true,
                            "data": { "activities": [] },
                        }),
                    );
                }
                "get_session_stats" => {
                    write_json(
                        &mut writer,
                        &json!({
                            "type": "response",
                            "id": id,
                            "command": "get_session_stats",
                            "success": true,
                            "data": {
                                "contextUsage": { "tokens": 1200, "contextWindow": 200_000 },
                                "cost": 0.01,
                            },
                        }),
                    );
                }
                _ => {
                    write_json(
                        &mut writer,
                        &json!({
                            "type": "response",
                            "id": id,
                            "command": command_type,
                            "success": true,
                            "data": {},
                        }),
                    );
                }
            }
        }
    }
}

fn write_json(writer: &mut UnixStream, value: &Value) {
    let mut line = serde_json::to_string(value).expect("serialize mock frame");
    line.push('\n');
    writer.write_all(line.as_bytes()).expect("write mock frame");
    writer.flush().expect("flush mock frame");
}

/// The capped attach result: 460 messages with a stitched ancestor (a
/// call before the window whose result sits inside it, plus a second
/// uncompleted call that must drop from the stitched copy) and a
/// dangling tool result (a call id no assistant carries).
fn attach_data_capped(id: &str) -> Value {
    let mut messages: Vec<Value> = (0..460)
        .map(|index| {
            json!({
                "role": if index % 2 == 0 { "user" } else { "assistant" },
                "content": format!("row {index:03}"),
                "timestamp": index,
            })
        })
        .collect();
    messages[58] = json!({
        "role": "assistant",
        "content": [
            { "type": "text", "text": "ancestor body" },
            { "type": "toolCall", "id": "tc-required", "name": "ipython", "arguments": {"code": "1"} },
            { "type": "toolCall", "id": "tc-uncompleted", "name": "ipython", "arguments": {"code": "2"} },
        ],
        "provider": "faux", "model": "faux-1", "timestamp": 58,
    });
    messages[440] = json!({
        "role": "toolResult",
        "toolCallId": "tc-required",
        "toolName": "ipython",
        "content": [{ "type": "text", "text": "the kept result" }],
        "isError": false,
        "timestamp": 440,
    });
    messages[441] = json!({
        "role": "toolResult",
        "toolCallId": "tc-dangling",
        "toolName": "ipython",
        "content": [{ "type": "text", "text": "dangling output" }],
        "isError": false,
        "timestamp": 441,
    });
    attach_data(id, messages)
}

/// The short attach result: a three-message transcript under the cap.
fn attach_data_small(id: &str) -> Value {
    let messages = [
        json!({ "role": "user", "content": "small row 0", "timestamp": 1 }),
        json!({
            "role": "assistant",
            "content": "small row 1",
            "provider": "faux", "model": "faux-1", "timestamp": 2,
        }),
        json!({ "role": "user", "content": "small row 2", "timestamp": 3 }),
    ];
    attach_data(id, messages.to_vec())
}

fn attach_data(id: &str, messages: Vec<Value>) -> Value {
    json!({
        "type": "response",
        "id": id,
        "command": "attach",
        "success": true,
        "data": {
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "activeSessionId": "s1",
            "snapshot": {
                "activeSessionId": "s1",
                "summary": { "id": "s1", "cwd": "/tmp" },
                "state": {
                    "activeSessionId": "s1",
                    "cwd": "/tmp",
                    "sessionId": "sess-1",
                    "sessionName": "capped replay probe",
                    "model": "faux-1",
                    "isStreaming": false,
                    "isCompacting": false,
                    "sessionActions": { "queuedCount": 0, "steering": [], "followUps": [] },
                },
                "messages": messages,
                "lastEventSequence": 0,
                "lastEventCursor": null,
            },
            "client": { "id": "mock", "capabilities": [] },
            "lastEventSequence": 0,
            "lastEventCursor": null,
        },
    })
}

fn options(socket: PathBuf) -> InteractiveOptions {
    InteractiveOptions {
        socket_path: socket,
        cwd: PathBuf::from("/tmp"),
        session_dir: None,
        script_path: None,
        model_selection: ModelSelection::default(),
        model_catalog: Vec::new(),
        model_configured_providers: std::collections::HashSet::default(),
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
        keybindings: pa_tui::keybindings::KeybindingsManager::new(),
        session_rlm_depth: None,
        prompt_stash: std::sync::Arc::default(),
        session_has_children: false,
        restore_dock_focus: false,
        client_settings: None,
    }
}

fn run_open(transcript: Transcript, scroll_top: bool) -> Vec<String> {
    std::env::remove_var("TMUX");
    let tail_needle = match transcript {
        Transcript::Capped => "row 459",
        Transcript::Small => "small row 2",
    };
    let dir = tempfile::TempDir::new().expect("temp dir");
    let socket = dir.path().join("tui.sock");
    let supervisor = MockSupervisor::bind(&socket);
    let handle = std::thread::spawn(move || supervisor.serve(transcript));
    let mut steps = vec![
        // The attach's tail frame first: the newest transcript message.
        HeadlessStep::WaitRender {
            needle: tail_needle.to_string(),
            timeout_ms: 10_000,
        },
    ];
    if scroll_top {
        // The notice sits above the fold: scroll the transcript to its
        // head so the notice row enters the frame.
        steps.push(HeadlessStep::ScrollTop);
        steps.push(HeadlessStep::WaitRender {
            needle: "Showing latest 399 of 460 messages for faster open.".to_string(),
            timeout_ms: 10_000,
        });
    }
    steps.push(HeadlessStep::WaitMs(50));
    let plan = HeadlessPlan {
        steps,
        width: 100,
        height: 30,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let outcome = runtime
        .block_on(run_interactive(options(socket), UiMode::Headless(plan)))
        .expect("interactive run");
    handle.join().expect("mock supervisor finished");
    outcome.frames
}

#[test]
fn the_capped_replay_folds_the_window_and_carries_the_notice() {
    let frames = run_open(Transcript::Capped, true);
    assert!(
        frames
            .iter()
            .any(|frame| frame.contains("Showing latest 399 of 460 messages for faster open.")),
        "the cap notice renders at the transcript head after the scroll:\n{}",
        frames.join("\n---frame---\n")
    );
    // The stitched ancestor leads the windowed rows (its text and only
    // the required call render; the uncompleted call dropped).
    assert!(
        frames.iter().any(|frame| frame.contains("ancestor body")),
        "the stitched ancestor's text renders:\n{}",
        frames.join("\n---frame---\n")
    );
    assert!(
        frames.iter().any(|frame| frame.contains("row 061")),
        "the walked window's first message renders:\n{}",
        frames.join("\n---frame---\n")
    );
    // The walk moved the start past index 60: the boundary rows before
    // the window never render, and the dangling tool result (a call id
    // no assistant carries) drops exactly like TS.
    for absent in ["row 059", "row 060", "dangling output"] {
        assert!(
            frames.iter().all(|frame| !frame.contains(absent)),
            "{absent} never renders (frames carry it)",
        );
    }
}

#[test]
fn the_under_limit_replay_stays_whole_without_the_notice() {
    let frames = run_open(Transcript::Small, false);
    assert!(
        frames.iter().any(|frame| frame.contains("small row 2")),
        "the short transcript painted:\n{}",
        frames.join("\n---frame---\n")
    );
    for row in ["small row 0", "small row 1", "small row 2"] {
        assert!(
            frames.iter().any(|frame| frame.contains(row)),
            "{row} renders"
        );
    }
    assert!(
        frames
            .iter()
            .all(|frame| !frame.contains("messages for faster open")),
        "no cap notice renders under the limit"
    );
}
