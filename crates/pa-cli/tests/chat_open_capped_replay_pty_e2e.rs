//! Real-pty e2e for the capped initial replay (TS
//! `INITIAL_TRANSCRIPT_RENDER_MESSAGE_LIMIT`): a direct open into an
//! existing session whose snapshot carries 460 messages — the port
//! must fold the newest 400-message window (plus the orphan-toolcall
//! stitch TS `initialRenderMessages` performs) and print the exact
//! dim cap notice TS `renderSessionContext` prints above the windowed
//! rows. The under-limit arm pins the frozen surface: a short
//! transcript attaches with NO notice row and every row painted.
//!
//! Served-path assertions (anti-vacuity): the capped arm FAILS unless
//! the notice bytes appear (the cap path ran) AND the
//! walk-start-adjusted boundary row ("row 060") is absent (the
//! stitch genuinely moved the window start), and the under-limit arm
//! fails if the notice appears. The harness reuses the chat-open
//! first-frame e2e's structure (child in its own process group, mock
//! supervisor socket, non-blocking pty master).

#![cfg(unix)]

use std::io::{BufRead, Read, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::fcntl::{fcntl, OFlag, F_SETFL};
use nix::pty::{openpty, Winsize};
use nix::unistd::Pid;
use serde_json::{json, Value};

use pa_tui::interactive::{
    run_interactive, InteractiveOptions, ModelSelection, SessionSelection, UiMode,
};

/// The child-mode socket: set (with the socket path) only when this very
/// binary is re-executed as the product-under-test.
const CHILD_SOCKET_ENV: &str = "PA_CAPPED_REPLAY_CHILD_SOCKET";

/// The flavor the parent's mock supervisor serves: the capped 460-message
/// snapshot or the short 3-message one.
enum Flavor {
    Capped,
    Small,
}

/// The child half of the e2e: runs the real interactive loop in terminal
/// mode against the parent's mock supervisor. A plain `cargo test` run
/// (no `CHILD_SOCKET_ENV`) passes trivially — only the parent test drives
/// the real path.
#[test]
fn chat_open_capped_replay_child_mode() {
    let Ok(socket) = std::env::var(CHILD_SOCKET_ENV) else {
        return;
    };
    let options = child_options(PathBuf::from(socket));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let _ = runtime.block_on(run_interactive(options, UiMode::Terminal));
}

/// The pty harnesses serialize: each drives a raw pty; concurrent
/// byte-level waits flake on the shared sandbox CPUs.
static HARNESS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn the_capped_replay_paints_the_notice_and_the_windowed_tail() {
    if !session_runner() {
        return;
    }
    match nix::unistd::setsid() {
        Ok(_) => {}
        Err(error) => panic!("the harness could not start a fresh session: {error}"),
    }
    let _lock = match HARNESS_LOCK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut harness = CappedReplayHarness::start(Flavor::Capped);

    // The windowed tail paints first (the viewport follows the tail, so
    // the attach frame shows the newest rows).
    harness.wait_from_start("row 459", "the newest windowed message painted");
    // The notice sits at the transcript HEAD — above the fold at attach
    // exactly like TS — so scroll the viewport to the top and audit the
    // bytes there.
    harness.send_keys(b"\x1b[1;4A");
    harness.wait_from_start(
        "Showing latest 399 of 460 messages for faster open.",
        "the cap notice painted",
    );
    harness.wait_from_start("ancestor body", "the stitched ancestor painted");
    harness.wait_from_start("row 061", "the walked window's first message painted");

    // Let the surface settle so the audit covers every repaint the open
    // can produce, then read the whole byte stream.
    harness.drain_until_quiet(10);
    let collected = harness.output();
    let stream: &[u8] = &collected;

    // The notice paints exactly once (one content frame).
    let notice_paints = count_occurrences(
        stream,
        b"Showing latest 399 of 460 messages for faster open.",
    );
    assert_eq!(notice_paints, 1, "the cap notice paints exactly once");

    // The newest windowed message painted (in the attach frame, before
    // the scroll).
    assert!(
        find_subsequence(stream, b"row 458").is_some(),
        "the newest windowed message painted"
    );
    // The walk moved the start past index 60: the boundary rows before
    // the window never paint (the stitch genuinely consumed the slot).
    assert!(
        find_subsequence(stream, b"row 060").is_none(),
        "the walk-dropped boundary row never paints"
    );
    assert!(
        find_subsequence(stream, b"row 059").is_none(),
        "the pre-window rows never paint"
    );
    // The dangling tool result (a call id no assistant carries) drops
    // from the capped window exactly like TS `omitOrphanToolResults`.
    assert!(
        find_subsequence(stream, b"dangling output").is_none(),
        "the dangling tool result never paints in the capped window"
    );

    harness.finish();
}

#[test]
fn the_under_limit_replay_stays_uncapped() {
    if !session_runner() {
        return;
    }
    match nix::unistd::setsid() {
        Ok(_) => {}
        Err(error) => panic!("the harness could not start a fresh session: {error}"),
    }
    let _lock = match HARNESS_LOCK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut harness = CappedReplayHarness::start(Flavor::Small);

    harness.wait_from_start("small row 2", "the short transcript painted");
    harness.drain_until_quiet(10);
    let collected = harness.output();
    let stream: &[u8] = &collected;

    // The frozen surface: a short transcript attaches whole, with no cap
    // notice anywhere in the stream.
    assert!(
        find_subsequence(stream, b"messages for faster open").is_none(),
        "no cap notice paints under the limit"
    );
    for row in ["small row 0", "small row 1", "small row 2"] {
        assert!(
            find_subsequence(stream, row.as_bytes()).is_some(),
            "{row} painted"
        );
        assert_eq!(
            count_occurrences(stream, row.as_bytes()),
            1,
            "{row} paints exactly once"
        );
    }

    harness.finish();
}

/// Whether this runner is attached to a controlling-terminal session (the
/// child re-exec needs a session it can leave and re-enter safely).
fn session_runner() -> bool {
    // SAFETY: tcgetpgrp only queries the fd's foreground process group.
    let foreground = unsafe { libc::tcgetpgrp(0) };
    if foreground < 0 {
        eprintln!(
            "no controlling-terminal session on the runner (tcgetpgrp(fd 0) \
             failed); skipping the capped-replay e2e — it needs a \
             controlling-terminal session to drive the pty child"
        );
        return false;
    }
    true
}

/// One pty-backed product child plus the mock supervisor it attaches to.
struct CappedReplayHarness {
    child: Child,
    /// The mock-supervisor server thread's join handle (it exits with
    /// the child's connection).
    _server: std::thread::JoinHandle<()>,
    master: PtyReader,
}

impl CappedReplayHarness {
    fn start(flavor: Flavor) -> CappedReplayHarness {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let socket = dir.path().join("tui.sock");
        let supervisor = MockSupervisor::bind(&socket, flavor);
        let server = std::thread::spawn(move || supervisor.serve());

        let pty = openpty(
            Some(&Winsize {
                ws_row: 24,
                ws_col: 80,
                ws_xpixel: 0,
                ws_ypixel: 0,
            }),
            None,
        )
        .expect("open pty");

        let child = spawn_child(&socket, &pty.slave);
        // Leak the temp dir's socket path on purpose: the child needs the
        // socket for the lifetime of the test, and the whole tree dies
        // with the child at teardown.
        std::mem::forget(dir);
        CappedReplayHarness {
            child,
            _server: server,
            master: PtyReader::new(pty.master),
        }
    }

    fn wait_from_start(&mut self, needle: &str, what: &str) {
        self.master.wait_from(0, needle, what);
    }

    /// Send raw keys to the child's terminal (the viewport's scroll
    /// keys: the e2e scrolls the capped transcript up to its head so
    /// the notice row — above the fold at attach — paints and audits).
    fn send_keys(&mut self, keys: &[u8]) {
        self.master.send(keys);
    }

    fn drain_until_quiet(&mut self, quiet_polls: usize) {
        self.master.drain_until_quiet(quiet_polls);
    }

    fn output(&self) -> Vec<u8> {
        self.master.output.clone()
    }

    fn finish(mut self) {
        let _ = self.child.kill();
        // Reap the child so no zombie is left behind.
        let _ = self.child.wait();
    }
}

/// Non-blocking reader over the pty master, collecting the raw byte
/// stream the child writes.
struct PtyReader {
    file: std::fs::File,
    output: Vec<u8>,
}

impl PtyReader {
    fn send(&mut self, keys: &[u8]) {
        use std::io::Write as _;
        let mut file = self.file.try_clone().expect("clone pty master for writes");
        file.write_all(keys).expect("write keys to the pty");
        let _ = file.flush();
    }

    fn new(master: OwnedFd) -> PtyReader {
        let fd = master.as_raw_fd();
        fcntl(fd, F_SETFL(OFlag::O_NONBLOCK)).expect("pty master non-blocking");
        PtyReader {
            file: master.into(),
            output: Vec::new(),
        }
    }

    /// Drain the master until it goes quiet for `quiet_polls` consecutive
    /// polls: a settle window keeps every later byte (the pty driver
    /// drops writes that find its kernel-side buffer full).
    fn drain_until_quiet(&mut self, quiet_polls: usize) {
        let mut quiet = 0;
        while quiet < quiet_polls {
            let mut buffer = [0u8; 8192];
            match self.file.read(&mut buffer) {
                Ok(0) | Err(_) => quiet += 1,
                Ok(n) => {
                    self.output.extend_from_slice(&buffer[..n]);
                    quiet = 0;
                }
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    /// Drain the master until the needle appears in the output collected
    /// since the given mark, bounded by a generous harness deadline.
    fn wait_from(&mut self, mark: usize, needle: &str, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if find_subsequence(&self.output[mark..], needle.as_bytes()).is_some() {
                return;
            }
            let mut buffer = [0u8; 8192];
            let result = self.file.read(&mut buffer);
            match result {
                Ok(0) | Err(_) => {}
                Ok(n) => self.output.extend_from_slice(&buffer[..n]),
            }
            if Instant::now() > deadline {
                let text = String::from_utf8_lossy(&self.output[mark..]);
                panic!(
                    "timeout waiting for {what} (needle {needle:?}); pty tail since mark:\n{text}"
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
    let mut count = 0;
    let mut offset = 0;
    while let Some(found) = find_subsequence(&haystack[offset..], needle) {
        count += 1;
        offset += found + needle.len();
    }
    count
}

/// A child process group of this very binary, re-executed in child mode
/// with the pty slave as its terminal and no tmux (the tmux keyboard
/// check must stay out of the way).
fn spawn_child(socket: &std::path::Path, slave: &OwnedFd) -> Child {
    // Runs between fork and exec in the child: setpgid moves it into its
    // own process group, inside the runner's session.
    fn make_process_group() -> std::io::Result<()> {
        nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))?;
        Ok(())
    }
    let mut command = Command::new(std::env::current_exe().expect("test binary"));
    command
        .arg("--exact")
        .arg("chat_open_capped_replay_child_mode")
        .env(CHILD_SOCKET_ENV, socket)
        .env_remove("TMUX")
        .stdin(slave_as_stdio(slave))
        .stdout(slave_as_stdio(slave))
        .stderr(slave_as_stdio(slave));
    // SAFETY: the pre_exec hook is the supported std seam for
    // process-group setup; it runs post-fork pre-exec in the child only
    // and cannot disturb this process.
    unsafe { command.pre_exec(make_process_group) };
    command.spawn().expect("spawn child")
}

fn slave_as_stdio(slave: &OwnedFd) -> Stdio {
    slave.try_clone().expect("clone pty slave").into()
}

fn child_options(socket: PathBuf) -> InteractiveOptions {
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
        // A direct open into an existing session: the agents-view hand
        // path, not a fresh create.
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

/// One attached session behind a mock supervisor socket: the attach
/// snapshot carries the flavor's transcript.
struct MockSupervisor {
    listener: std::os::unix::net::UnixListener,
    flavor: Flavor,
}

impl MockSupervisor {
    fn bind(socket: &std::path::Path, flavor: Flavor) -> Self {
        MockSupervisor {
            listener: std::os::unix::net::UnixListener::bind(socket).expect("bind mock socket"),
            flavor,
        }
    }

    fn serve(self) {
        let Ok((stream, _)) = self.listener.accept() else {
            return;
        };
        let write_stream = stream.try_clone().expect("clone mock socket");
        let mut writer = write_stream;
        let mut reader = std::io::BufReader::new(stream);
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
                    let data = match self.flavor {
                        Flavor::Capped => attach_data_capped(id),
                        Flavor::Small => attach_data_small(id),
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

fn write_json(writer: &mut std::os::unix::net::UnixStream, value: &Value) {
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
