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
//!   [--gap-seconds S] [--timeout-minutes M] [--out DIR] [--seed N] \
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
    trial_result_from_snapshot, EvalArgsError, SwarmEvalConfig, SwarmEvalTrialResult,
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
            line.clear();
            self.reader
                .get_ref()
                .set_read_timeout(Duration::from_millis(100))
                .map_err(|error| format!("failed to set the read timeout: {error}"))?;
            match self.reader.read_line(&mut line) {
                Ok(0) => return Err("the daemon closed the connection".to_string()),
                Ok(_) if line.trim().is_empty() => {}
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
        loop {
            let response = self.read_line(timeout)?;
            if response.get("id").and_then(Value::as_str) == Some(id.as_str()) {
                return Ok(response);
            }
        }
    }
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let (socket, argv) = extract_flag(&argv, "--socket");
    let socket = socket.map_or_else(
        pa_daemon::platform::default_daemon_socket_path,
        PathBuf::from,
    );
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

/// Pull one `--flag <value>` pair out of argv, leaving the rest.
fn extract_flag(argv: &[String], flag: &str) -> (Option<String>, Vec<String>) {
    let mut value = None;
    let mut rest = Vec::new();
    let mut index = 0;
    while index < argv.len() {
        if argv[index] == flag {
            value = argv.get(index + 1).cloned();
            index += 2;
        } else {
            rest.push(argv[index].clone());
            index += 1;
        }
    }
    (value, rest)
}

fn run(socket: &Path, config: &SwarmEvalConfig) -> Result<(), String> {
    let mut client = Client::connect(socket)?;
    let runs_root = std::env::temp_dir().join(format!(
        "swarm-eval-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    ));
    fs::create_dir_all(&runs_root).map_err(|error| format!("create runs root: {error}"))?;

    let mut results: Vec<SwarmEvalTrialResult> = Vec::new();
    for &size in &config.sizes {
        for trial in 1..=config.trials {
            println!(
                "running crew size {size} trial {trial}/{} on {}",
                config.trials, config.model
            );
            match run_trial(&mut client, config, size, trial, &runs_root) {
                Ok(result) => results.push(result),
                Err(message) => eprintln!("trial failed: {message}"),
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

    let seed = config.seed
        + 31 * i64::try_from(size).unwrap_or(i64::MAX)
        + i64::try_from(trial).unwrap_or(i64::MAX);
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
        .and_then(Value::as_str)
        .ok_or_else(|| format!("create returned no session id: {created}"))?
        .to_string();

    let _ = command_data(
        client,
        &json!({ "type": "prompt", "activeSessionId": session_id, "message": prompt }),
        Duration::from_mins(1),
    )?;

    let deadline = Instant::now() + Duration::from_secs_f64(config.timeout_minutes * 60.0);
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
        let running = running_children(client, &session_id)?;
        if running == 0 && parse_answer_line(text.as_deref()).is_some() {
            answer_text = text;
            break;
        }
        if Instant::now() >= deadline {
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

    let _ = client.command(
        &json!({ "type": "kill", "activeSessionId": session_id }),
        Duration::from_secs(30),
    );
    let _ = fs::remove_dir_all(&trial_root);

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
