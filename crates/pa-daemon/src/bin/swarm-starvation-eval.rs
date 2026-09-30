//! Swarm starvation eval driver: the real-token harness run.
//!
//! Port of `packages/coding-agent/scripts/swarm-starvation-eval.ts` (#2353).
//! Drives a real daemon orchestrator session (children spawn through the
//! product `rlm.spawn` surface and reply over the agent-message path) across
//! crew sizes x message sizes x arrival patterns, then scores each trial
//! against the pre-registered defense lines in [`pa_core::swarm_eval`].
//!
//! This harness spends real model tokens and never runs in CI; the
//! deterministic pieces (defense lines, prompts, verification, reporting,
//! argument parsing) are unit-tested in `pa_core::swarm_eval`, and the
//! daemon-level flow is smoke-tested by
//! `tests/swarm_starvation_eval_e2e.rs`.
//!
//! Usage:
//!
//! ```text
//! swarm-starvation-eval --model provider/id [--sizes 2,5,10,20,40] \
//!   [--msg-size short|long] [--pattern spread|burst] [--trials N] \
//!   [--gap-seconds S] [--timeout-minutes M|inf] [--out DIR] [--seed N] \
//!   [--socket PATH]
//! ```
//!
//! The daemon must already be running (its default socket is used unless
//! `--socket` overrides it); each trial creates its own session under a
//! temporary cwd/session dir, so no user session state is touched.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pa_core::swarm_eval::transcript::snapshot_from_transcript;
use pa_core::swarm_eval::{
    build_orchestrator_prompt, parse_answer_line, render_markdown_report, seeded_secrets,
    trial_deadline, trial_result_from_snapshot, EvalArgsError, MessagingStatsSnapshot,
    SwarmEvalConfig, SwarmEvalTrialResult,
};
use pa_types::platform::transport::{connect_blocking, BlockingTransportStream};
use serde_json::{json, Value};

/// A blocking JSONL client for one daemon socket.
struct Client {
    reader: BufReader<Box<dyn BlockingTransportStream>>,
    writer: Box<dyn BlockingTransportStream>,
    request_id: u64,
}

impl Client {
    fn connect(socket: &Path) -> Result<Self, String> {
        let stream = connect_blocking(socket).map_err(|error| {
            format!(
                "failed to connect to the Prime Agent daemon at {}: {error}; start it with \
                 `prime-agent --mode daemon`",
                socket.display()
            )
        })?;
        let writer = stream
            .try_clone_box()
            .map_err(|error| format!("failed to clone the daemon socket: {error}"))?;
        let mut client = Self {
            reader: BufReader::new(stream),
            writer,
            request_id: 0,
        };
        // The daemon greets every client before it accepts commands.
        let hello = client.read_line(Duration::from_secs(15))?;
        if hello.get("type").and_then(Value::as_str) != Some("daemon_hello") {
            return Err(format!("unexpected daemon greeting: {hello}"));
        }
        Ok(client)
    }

    fn read_line(&mut self, timeout: Duration) -> Result<Value, String> {
        let deadline = Instant::now() + timeout;
        let mut line = String::new();
        loop {
            self.reader
                .get_ref()
                .set_read_timeout(Duration::from_millis(100))
                .map_err(|error| format!("failed to set the read timeout: {error}"))?;
            match self.reader.read_line(&mut line) {
                Ok(0) => return Err("the daemon closed the connection".to_string()),
                // A large frame can straddle the 100ms read windows: the
                // buffer resets only after a complete line is consumed, so
                // a read timeout keeps the partial bytes instead of
                // discarding them and failing the frame's parse.
                Ok(_) if line.trim().is_empty() => line.clear(),
                Ok(_) => {
                    return serde_json::from_str(line.trim())
                        .map_err(|error| format!("invalid daemon line: {error}"));
                }
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(format!("timed out reading from the daemon: {error}"));
                    }
                }
            }
        }
    }

    fn command(&mut self, command: &Value, timeout: Duration) -> Result<Value, String> {
        self.request_id += 1;
        let id = format!("swarm-eval-{}", self.request_id);
        let envelope = json!({
            "type": "command",
            "id": id,
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "command": command,
        });
        let mut line =
            serde_json::to_string(&envelope).map_err(|error| format!("serialize: {error}"))?;
        line.push('\n');
        self.writer
            .write_all(line.as_bytes())
            .map_err(|error| format!("failed to send command: {error}"))?;
        self.writer
            .flush()
            .map_err(|error| format!("failed to flush command: {error}"))?;
        // One fixed budget for the whole command: the daemon broadcasts
        // unsolicited frames (heartbeats_changed and friends) to attached
        // clients, and a full-timeout retry per frame would let a steady
        // broadcast stream delay the response — and the trial cleanup
        // behind it — indefinitely. Each read gets only what remains.
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("timed out waiting for the command response".to_string());
            }
            let response = self.read_line(remaining)?;
            if response.get("id").and_then(Value::as_str) == Some(id.as_str()) {
                return Ok(response);
            }
        }
    }
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let (socket, argv) = match socket_from_args(&argv) {
        Ok((socket, argv)) => (socket, argv),
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    };
    let config = match pa_core::swarm_eval::parse_eval_args(&argv) {
        Ok(config) => config,
        Err(EvalArgsError::Help) => {
            println!("See the header of swarm-starvation-eval.rs for usage.");
            return;
        }
        Err(EvalArgsError::Message(message)) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    };
    match run(&socket, &config) {
        Ok(()) => {}
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
}

/// The `--socket` flag overrides the daemon socket; it is extracted before
/// the shared parser because it addresses the driver itself, not the eval.
/// A trailing `--socket` without a value is an incomplete option, not an
/// omitted one: silently connecting to the default socket would spend real
/// tokens against the wrong daemon.
fn socket_from_args(argv: &[String]) -> Result<(PathBuf, Vec<String>), String> {
    let (socket, given, rest) = extract_flag(argv, "--socket");
    match (socket, given) {
        (Some(socket), _) => Ok((PathBuf::from(socket), rest)),
        (None, false) => Ok((pa_daemon::platform::default_daemon_socket_path(), rest)),
        (None, true) => Err("Missing value for --socket".to_string()),
    }
}

/// Pull one `--flag <value>` pair out of argv, leaving the rest. Returns
/// the value (absent when the flag is missing entirely or trails without
/// one) and whether the flag was present at all, so an incomplete option
/// can be told apart from an omitted one.
fn extract_flag(argv: &[String], flag: &str) -> (Option<String>, bool, Vec<String>) {
    let mut value = None;
    let mut given = false;
    let mut rest = Vec::new();
    let mut index = 0;
    while index < argv.len() {
        if argv[index] == flag {
            given = true;
            value = argv.get(index + 1).cloned();
            index += 2;
        } else {
            rest.push(argv[index].clone());
            index += 1;
        }
    }
    (value, given, rest)
}

/// The per-run scratch root: the process id plus the start time keeps two
/// harness processes launched in the same millisecond from sharing trial
/// directories and `swarm-eval-{size}-{trial}` daemon session names.
fn runs_root_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "swarm-eval-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ))
}

fn run(socket: &Path, config: &SwarmEvalConfig) -> Result<(), String> {
    let mut client = Client::connect(socket)?;
    let runs_root = runs_root_path();
    fs::create_dir_all(&runs_root).map_err(|error| format!("create runs root: {error}"))?;

    let mut results: Vec<SwarmEvalTrialResult> = Vec::new();
    for &size in &config.sizes {
        for trial in 1..=config.trials {
            println!(
                "running crew size {size} trial {trial}/{} on {}",
                config.trials, config.model
            );
            let started = Instant::now();
            match run_trial(&mut client, config, size, trial, &runs_root) {
                Ok(result) => results.push(result),
                Err(message) => {
                    eprintln!("trial failed: {message}");
                    // An errored trial stays in the sweep as its own row:
                    // zeroed counters, a failed task, and the error as the
                    // instant-fail reason. The report can then never cover a
                    // subset of the planned trials, and a never-completed
                    // crew size cannot hide behind the rows that did.
                    results.push(trial_result_from_snapshot(
                        config,
                        size,
                        trial,
                        &MessagingStatsSnapshot::default(),
                        false,
                        Some(format!("trial error: {message}")),
                        started.elapsed().as_secs_f64(),
                    ));
                }
            }
        }
    }
    if results.is_empty() {
        return Err("no trials completed".to_string());
    }

    let markdown = render_markdown_report(&results, config);
    let out_dir = PathBuf::from(&config.out_dir);
    fs::create_dir_all(&out_dir)
        .map_err(|error| format!("create {}: {error}", out_dir.display()))?;
    fs::write(out_dir.join("report.md"), &markdown)
        .map_err(|error| format!("write report.md: {error}"))?;
    let json_report = json!({ "config": config, "results": results });
    fs::write(
        out_dir.join("report.json"),
        serde_json::to_string_pretty(&json_report)
            .map_err(|error| format!("serialize report: {error}"))?,
    )
    .map_err(|error| format!("write report.json: {error}"))?;
    println!("{markdown}");
    println!("reports written to {}", out_dir.display());
    Ok(())
}

fn run_trial(
    client: &mut Client,
    config: &SwarmEvalConfig,
    size: usize,
    trial: usize,
    runs_root: &Path,
) -> Result<SwarmEvalTrialResult, String> {
    let started = Instant::now();
    let (provider, model_id) = config
        .model
        .split_once('/')
        .ok_or_else(|| format!("model must be provider/id, got {}", config.model))?;
    let trial_root = runs_root.join(format!("size-{size}-trial-{trial}"));
    let sessions_dir = trial_root.join("sessions");
    fs::create_dir_all(&sessions_dir).map_err(|error| format!("create trial dir: {error}"))?;

    // The derived trial seed is `seed + 31 * size + trial` (i64). The
    // argument parser rejects a sweep that cannot represent it, but a
    // hand-built config still reaches here: the checked arithmetic fails
    // this trial cleanly instead of panicking (or silently wrapping the
    // seed in release builds).
    let size_seed = i64::try_from(size)
        .map_err(|_| format!("crew size {size} is too large to derive a trial seed"))?;
    let trial_seed = i64::try_from(trial)
        .map_err(|_| format!("trial {trial} is too large to derive a trial seed"))?;
    let seed = 31_i64
        .checked_mul(size_seed)
        .and_then(|offset| config.seed.checked_add(offset))
        .and_then(|seed| seed.checked_add(trial_seed))
        .ok_or_else(|| {
            format!(
                "seed {} with size {size} and trial {trial} overflows the derived trial seed",
                config.seed
            )
        })?;
    let secrets = seeded_secrets(seed, size);
    let prompt = build_orchestrator_prompt(config, size, &secrets);

    let created = command_data(
        client,
        &json!({
            "type": "create",
            "name": format!("swarm-eval-{size}-{trial}"),
            "config": {
                "cwd": trial_root.to_string_lossy(),
                "sessionDir": sessions_dir.to_string_lossy(),
                "provider": provider,
                "model": model_id,
            },
        }),
        Duration::from_mins(2),
    )?;
    let session_id = created
        .get("activeSessionId")
        .or_else(|| created.get("id"))
        .and_then(Value::as_str);
    let Some(session_id) = session_id else {
        // No session id means nothing to kill, but the trial dir goes.
        let _ = fs::remove_dir_all(&trial_root);
        return Err(format!("create returned no session id: {created}"));
    };
    let session_id = session_id.to_string();

    let outcome = drive_trial(
        client,
        config,
        size,
        trial,
        &session_id,
        &secrets,
        &prompt,
        started,
    );
    // The TS harness disposes the orchestrator in a `finally` block on every
    // path; this is the port's equivalent: the session is killed and the
    // trial directory removed whether the trial scored or errored, so a
    // failed prompt, poll, or stats request can never leave a live
    // orchestrator issuing real model requests after the trial ends.
    let _ = client.command(
        &json!({ "type": "kill", "activeSessionId": session_id }),
        Duration::from_secs(30),
    );
    let _ = fs::remove_dir_all(&trial_root);
    outcome
}

/// The post-create trial body: prompt, poll, verify, and score. Cleanup is
/// owned by [`run_trial`], which kills the session on every outcome.
#[allow(clippy::too_many_arguments)]
fn drive_trial(
    client: &mut Client,
    config: &SwarmEvalConfig,
    size: usize,
    trial: usize,
    session_id: &str,
    secrets: &[u32],
    prompt: &str,
    started: Instant,
) -> Result<SwarmEvalTrialResult, String> {
    let _ = command_data(
        client,
        &json!({ "type": "prompt", "activeSessionId": session_id, "message": prompt }),
        Duration::from_mins(1),
    )?;

    // `--timeout-minutes inf` parses to an unbounded wait, and any
    // non-representable finite value degrades to unbounded instead of
    // panicking in `Duration::from_secs_f64`.
    let deadline = trial_deadline(config.timeout_minutes);
    let mut answer_text = None;
    loop {
        let text = command_data(
            client,
            &json!({ "type": "get_last_assistant_text", "activeSessionId": session_id }),
            Duration::from_secs(30),
        )?
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_string);
        let running = running_children(client, session_id)?;
        if running == 0 && parse_answer_line(text.as_deref()).is_some() {
            answer_text = text;
            break;
        }
        if deadline.is_some_and(|at| Instant::now() >= at) {
            break;
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    let messages = command_data(
        client,
        &json!({ "type": "get_messages", "activeSessionId": session_id }),
        Duration::from_mins(1),
    )?
    .get("messages")
    .and_then(Value::as_array)
    .cloned()
    .unwrap_or_default();
    let context_tokens = command_data(
        client,
        &json!({ "type": "get_session_stats", "activeSessionId": session_id }),
        Duration::from_secs(30),
    )?
    .get("contextUsage")
    .and_then(|usage| usage.get("tokens"))
    .and_then(Value::as_u64);

    let expected: Vec<u64> = secrets.iter().map(|secret| u64::from(*secret)).collect();
    let answer = parse_answer_line(answer_text.as_deref());
    let task_success = answer.as_deref() == Some(expected.as_slice());
    let instant_fail = rate_limit_failure(&messages);

    let snapshot = snapshot_from_transcript(&messages, context_tokens);
    Ok(trial_result_from_snapshot(
        config,
        size,
        trial,
        &snapshot,
        task_success,
        instant_fail,
        started.elapsed().as_secs_f64(),
    ))
}

fn command_data(client: &mut Client, command: &Value, timeout: Duration) -> Result<Value, String> {
    let response = client.command(command, timeout)?;
    if response.get("success").and_then(Value::as_bool) != Some(true) {
        return Err(format!("command failed: {response}"));
    }
    Ok(response.get("data").cloned().unwrap_or(Value::Null))
}

fn running_children(client: &mut Client, session_id: &str) -> Result<usize, String> {
    let data = command_data(
        client,
        &json!({ "type": "get_rlm_children", "activeSessionId": session_id }),
        Duration::from_secs(30),
    )?;
    Ok(data
        .get("children")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter(|row| row.get("status").and_then(Value::as_str) == Some("running"))
                .count()
        })
        .unwrap_or_default())
}

/// A rate-limit model error during the trial is an instant failure.
fn rate_limit_failure(messages: &[Value]) -> Option<String> {
    messages.iter().find_map(|message| {
        let stop_reason_error = message.get("stopReason").and_then(Value::as_str) == Some("error");
        let error = message
            .get("errorMessage")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let rate_limited = error.contains("429")
            || error.contains("rate limit")
            || error.contains("too many requests");
        (stop_reason_error && rate_limited).then(|| "rate-limit error during trial".to_string())
    })
}

#[cfg(all(test, unix))]
mod tests {
    //! Driver-loop regression tests over a scripted daemon socket: no real
    //! daemon, no tokens, just the driver's command sequence.

    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::Path;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::thread;
    use std::time::{Duration, Instant};

    use pa_core::swarm_eval::{seeded_secrets, ArrivalPattern, MessageSize, SwarmEvalConfig};
    use serde_json::{json, Value};

    use super::{run, run_trial, runs_root_path, socket_from_args, Client};

    /// A scripted daemon socket: greets the client, then answers each
    /// command by its `type` from `script`, recording every command in
    /// order. Commands without a scripted entry fail (the error path).
    struct FakeDaemon {
        socket: std::path::PathBuf,
        commands: Receiver<Value>,
        _dir: tempfile::TempDir,
    }

    fn fake_daemon(script: Vec<(&'static str, Value)>) -> FakeDaemon {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let socket = dir.path().join("daemon.sock");
        let listener = UnixListener::bind(&socket).expect("bind socket");
        let (tx, rx) = channel();
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            serve_client(stream, &script, &tx);
        });
        FakeDaemon {
            socket,
            commands: rx,
            _dir: dir,
        }
    }

    fn serve_client(stream: UnixStream, script: &[(&'static str, Value)], tx: &Sender<Value>) {
        let mut writer = stream.try_clone().expect("clone stream");
        let mut reader = BufReader::new(stream);
        let _ = writeln!(writer, r#"{{"type":"daemon_hello"}}"#);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => continue,
            }
            let Ok(envelope) = serde_json::from_str::<Value>(line.trim()) else {
                continue;
            };
            let id = envelope.get("id").cloned().unwrap_or(Value::Null);
            let command = envelope.get("command").cloned().unwrap_or(Value::Null);
            let _ = tx.send(command.clone());
            let kind = command.get("type").and_then(Value::as_str).unwrap_or("");
            let mut response = json!({ "id": id, "type": "response" });
            if let Some((_, data)) = script.iter().find(|(kind_, _)| *kind_ == kind) {
                response["success"] = json!(true);
                response["data"] = data.clone();
            } else {
                response["success"] = json!(false);
                response["error"] = json!(format!("no script for {kind}"));
            }
            let _ = writeln!(writer, "{response}");
        }
    }

    impl FakeDaemon {
        /// Wait (bounded) for the kill of `session_id` and return every
        /// command the driver sent in order.
        fn drain_until_killed(&self, session_id: &str) -> Vec<Value> {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut commands = Vec::new();
            loop {
                while let Ok(command) = self.commands.try_recv() {
                    commands.push(command);
                }
                if commands.iter().any(|command| {
                    command["type"] == "kill" && command["activeSessionId"] == session_id
                }) {
                    return commands;
                }
                // (The session id is not written into the assert message;
                // CodeQL's cleartext logger flags it by name.)
                assert!(
                    Instant::now() < deadline,
                    "the kill never arrived; commands so far: {commands:?}"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    fn test_config(out_dir: &Path, size: usize, timeout_minutes: f64) -> SwarmEvalConfig {
        SwarmEvalConfig {
            model: "scripted/faux".to_string(),
            sizes: vec![size],
            message_size: MessageSize::Short,
            pattern: ArrivalPattern::Spread,
            trials: 1,
            gap_seconds: 2.0,
            timeout_minutes,
            out_dir: out_dir.to_string_lossy().to_string(),
            seed: 1,
        }
    }

    #[test]
    fn a_failed_post_create_request_kills_the_session_and_is_reported() {
        // Only `create` and `kill` are scripted: the `prompt` right after
        // the create fails, which used to return from the trial without
        // killing the freshly created session.
        let daemon = fake_daemon(vec![
            ("create", json!({ "activeSessionId": "s-eval" })),
            ("kill", json!(null)),
        ]);
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let config = test_config(out_dir.path(), 2, 15.0);

        run(&daemon.socket, &config).expect("the errored trial is reported, not fatal");

        // The session was killed even though the trial errored.
        let commands = daemon.drain_until_killed("s-eval");
        let prompt_index = commands
            .iter()
            .position(|command| command["type"] == "prompt")
            .expect("the prompt was attempted");
        let kill_index = commands
            .iter()
            .position(|command| command["type"] == "kill")
            .expect("the kill follows the failed prompt");
        assert!(kill_index > prompt_index, "{commands:?}");

        // The errored trial is a reported row, not a dropped one: the
        // report names it as a failure and keeps the full sweep.
        let report = std::fs::read_to_string(out_dir.path().join("report.md")).expect("report.md");
        assert!(report.contains("trial error:"), "{report}");
        assert!(report.contains("1/1 trials failed"), "{report}");
        let raw = std::fs::read_to_string(out_dir.path().join("report.json")).expect("report.json");
        let json: Value = serde_json::from_str(&raw).expect("report.json parses");
        let rows = json["results"].as_array().expect("results array");
        assert_eq!(rows.len(), 1, "the errored trial is a row: {rows:?}");
        assert_eq!(rows[0]["verdict"], "fail", "{rows:?}");
        assert!(
            rows[0]["instant_fail"]
                .as_str()
                .expect("instant fail")
                .starts_with("trial error:"),
            "{rows:?}"
        );
    }

    #[test]
    fn an_unbounded_timeout_runs_the_trial_without_panicking() {
        // `--timeout-minutes inf` must reach an unbounded wait: the old
        // `Duration::from_secs_f64(timeout * 60.0)` panicked here before the
        // poll loop could ever kill the session.
        // The driver's seed math is config.seed + 31 * size + trial; for
        // this config (seed 1, size 1, trial 1) it selects seed 33.
        let (seed, size, trial) = (1, 1, 1);
        let secret = seeded_secrets(seed + 31 * size + trial, 1)[0];
        let daemon = fake_daemon(vec![
            ("create", json!({ "activeSessionId": "s-eval" })),
            ("prompt", json!({})),
            (
                "get_last_assistant_text",
                json!({ "text": format!("ANSWER: {secret}") }),
            ),
            ("get_rlm_children", json!({ "children": [] })),
            ("get_messages", json!({ "messages": [] })),
            (
                "get_session_stats",
                json!({ "contextUsage": { "tokens": 1_000 } }),
            ),
            ("kill", json!(null)),
        ]);
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let config = test_config(out_dir.path(), 1, f64::INFINITY);

        run(&daemon.socket, &config).expect("the unbounded trial completes");

        // The poll loop broke on the ANSWER and the session was still killed.
        let commands = daemon.drain_until_killed("s-eval");
        assert!(
            commands
                .iter()
                .any(|command| command["type"] == "get_last_assistant_text"),
            "the poll loop ran: {commands:?}"
        );
        let report = std::fs::read_to_string(out_dir.path().join("report.md")).expect("report.md");
        assert!(report.contains("| 1 | 1 |"), "{report}");
        assert!(report.contains("inconclusive"), "{report}");
    }

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn a_fragmented_frame_survives_read_timeouts() {
        // A large daemon frame (a full transcript, a wide children list)
        // can straddle the client's 100ms read windows; the partial bytes
        // must survive the timeout retry instead of being cleared away
        // and corrupting the line's parse.
        let dir = tempfile::TempDir::new().expect("temp dir");
        let socket = dir.path().join("frag.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut writer = stream.try_clone().expect("clone");
            let mut reader = BufReader::new(stream);
            let _ = writeln!(writer, r#"{{"type":"daemon_hello"}}"#);
            let mut line = String::new();
            let _ = reader.read_line(&mut line);
            // The response arrives in two pieces with a gap wider than the
            // client's 100ms read window.
            let response = r#"{"id":"swarm-eval-1","type":"response","success":true,"data":{"text":"ANSWER: 501"}}"#;
            writer
                .write_all(&response.as_bytes()[..20])
                .expect("first chunk");
            writer.flush().expect("flush chunk");
            thread::sleep(Duration::from_millis(300));
            writer.write_all(&response.as_bytes()[20..]).expect("rest");
            writer.write_all(b"\n").expect("newline");
            writer.flush().expect("flush rest");
        });
        let mut client = Client::connect(&socket).expect("connect");
        let response = client
            .command(
                &json!({ "type": "get_last_assistant_text", "activeSessionId": "s" }),
                Duration::from_secs(5),
            )
            .expect("the fragmented frame completes");
        assert_eq!(response["id"], "swarm-eval-1", "{response}");
        assert_eq!(response["data"]["text"], "ANSWER: 501", "{response}");
    }

    #[test]
    fn a_broadcast_stream_never_extends_the_command_timeout() {
        // The daemon broadcasts unsolicited frames (heartbeats_changed and
        // friends) to attached clients; a full-timeout retry per frame
        // would let a steady stream delay the command response — and the
        // trial cleanup behind it — indefinitely. The command budget is
        // one fixed deadline.
        let dir = tempfile::TempDir::new().expect("temp dir");
        let socket = dir.path().join("stream.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        let started = Instant::now();
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut writer = stream.try_clone().expect("clone");
            let _ = writeln!(writer, r#"{{"type":"daemon_hello"}}"#);
            // One unsolicited frame every 100ms for ~4s: far longer than
            // the command's 300ms budget, and never the awaited response.
            for _ in 0..40 {
                let _ = writeln!(writer, r#"{{"type":"heartbeats_changed"}}"#);
                thread::sleep(Duration::from_millis(100));
            }
        });
        let mut client = Client::connect(&socket).expect("connect");
        let error = client
            .command(
                &json!({ "type": "get_session_stats", "activeSessionId": "s" }),
                Duration::from_millis(300),
            )
            .expect_err("the command must hit its fixed budget");
        assert!(error.contains("timed out"), "{error}");
        // The budget fired long before the broadcast stream (~4s) ended.
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the command deadline must hold under a broadcast stream, waited {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn the_runs_root_is_process_unique() {
        // Two harness processes launched in the same millisecond must not
        // share a scratch root: their trial directories and daemon session
        // names would collide.
        let name = runs_root_path()
            .file_name()
            .expect("a name")
            .to_string_lossy()
            .to_string();
        assert!(
            name.starts_with(&format!("swarm-eval-{}-", std::process::id())),
            "{name}"
        );
    }

    #[test]
    fn a_trailing_socket_flag_without_a_value_is_rejected() {
        // A trailing `--socket` must not fall back to the default socket:
        // the driver would spend real tokens against the wrong daemon.
        assert_eq!(
            socket_from_args(&args(&["--socket"])),
            Err("Missing value for --socket".to_string())
        );
        // An omitted flag keeps the default, and the rest is untouched.
        let (socket, rest) =
            socket_from_args(&args(&["--model", "x/y"])).expect("an omitted flag is fine");
        assert_eq!(socket, pa_daemon::platform::default_daemon_socket_path());
        assert_eq!(rest, args(&["--model", "x/y"]));
        // A value is honored and peeled off for the shared parser.
        let (socket, rest) =
            socket_from_args(&args(&["--socket", "/tmp/eval.sock", "--model", "x/y"]))
                .expect("parses");
        assert_eq!(socket, std::path::PathBuf::from("/tmp/eval.sock"));
        assert_eq!(rest, args(&["--model", "x/y"]));
    }

    #[test]
    fn an_overflowing_derived_seed_fails_the_trial_cleanly() {
        // A hand-built config can bypass the argument parser's sweep
        // validation; the trial must return a configuration error instead
        // of panicking on `seed + 31 * size + trial`.
        let daemon = fake_daemon(Vec::new());
        let out_dir = tempfile::TempDir::new().expect("out dir");
        let config = test_config(out_dir.path(), 2, 15.0);
        let config = SwarmEvalConfig {
            seed: i64::MAX,
            ..config
        };
        let runs_root = out_dir.path().join("runs");
        let mut client = Client::connect(&daemon.socket).expect("connect");
        let error = run_trial(&mut client, &config, 2, 1, &runs_root)
            .expect_err("the overflowing seed must fail the trial");
        assert!(
            error.contains("overflows the derived trial seed"),
            "{error}"
        );
    }
}
