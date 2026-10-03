// The Tier-C/D ruling (fleet-uniform, 2026-09-28): stack-resident futures
// by design on hot paths; 64-bit targets - the narrowing sits at OS/protocol
// boundaries where the values are bounded (pid syscalls, milliseconds).
#![allow(
    clippy::large_futures,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

//! Real-pty e2e for the interactive startup's first paint (the operator's
//! 2026-10-03 herdr tab-open report: a new tab's client took visibly
//! longer than before to paint anything). The regression shape: the
//! startup telemetry block awaited the one-shot client's flush BEFORE the
//! TUI ran, and with the default analytics sink that flush is a network
//! round trip (up to the 1.5s request timeout), so the pane stayed blank
//! for the whole delivery.
//!
//! The pin: with telemetry on and the analytics endpoint pointed (via the
//! debug-only test seam) at a local stub that never answers, the client's
//! first paint must land well inside the stub's stall — the flush rides
//! in the background, never on the interactive critical path. An awaited
//! flush paints only after the full request timeout and fails the bound.
//!
//! The harness reuses the chat-open pty e2e's shape (child in its own
//! process group inside this runner's session, non-blocking pty master)
//! and the interactive-daemon e2e's real-supervisor spawn.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::fcntl::{fcntl, FcntlArg::F_SETFL, OFlag};
use nix::pty::{openpty, Winsize};
use nix::unistd::Pid;

/// The bound: the paint must land well inside the stub's stall. The
/// unfixed tree paints only after the analytics request timeout (1.5s)
/// plus the client boot; the fixed tree paints at the boot's own pace.
const PAINT_BOUND: Duration = Duration::from_millis(1000);

/// How long to wait for the stub to observe the analytics request after
/// the paint: the flush runs in the background on the fixed tree, so the
/// request may land just after the paint. Bounded well inside the stub's
/// own stall.
const STUB_REQUEST_BOUND: Duration = Duration::from_secs(3);

/// The pty harnesses serialize: each drives a raw pty; concurrent
/// byte-level waits flake on the shared sandbox CPUs.
static HARNESS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A running supervisor for the client to find, plus its socket path.
struct Supervisor {
    child: Child,
    socket: std::path::PathBuf,
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        // The shutdown ACK is written before the workers stop, so the
        // supervisor must actually exit here; a SIGKILL that lands first
        // would orphan its detached workers.
        let _ = graceful_shutdown(&self.socket);
        let worker_pids = child_pids_of(self.child.id());
        let deadline = Instant::now() + Duration::from_secs(10);
        while process_alive(self.child.id()) {
            if Instant::now() > deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        for pid in worker_pids {
            kill_worker(pid);
        }
        let _ = std::fs::remove_file(&self.socket);
    }
}

/// Pids whose parent is `ppid` (the supervisor's live worker children).
fn child_pids_of(ppid: u32) -> Vec<u32> {
    let mut pids = Vec::new();
    let entries = std::fs::read_dir("/proc").expect("read /proc");
    for entry in entries.flatten() {
        let Ok(entry_pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{entry_pid}/stat")) else {
            continue;
        };
        // `comm` can contain spaces and parens, so parse after the last ')'.
        let Some((_, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        fields.next(); // process state
        let Ok(parent) = fields.next().unwrap_or_default().parse::<u32>() else {
            continue;
        };
        if parent == ppid {
            pids.push(entry_pid);
        }
    }
    pids
}

/// Liveness that ignores zombies: an exited process nobody reaps keeps
/// its `/proc` entry, so path existence alone would call it alive.
fn process_alive(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let Some((_, rest)) = stat.rsplit_once(')') else {
        return false;
    };
    let state = rest.split_whitespace().next().unwrap_or_default();
    !state.starts_with('Z') && !state.starts_with('X')
}

/// Kill a leaked worker process (SIGKILL; the graceful path already
/// failed) and wait briefly for it to disappear.
fn kill_worker(pid: u32) {
    for round in 0..2 {
        // SAFETY: kill(2) on a pid we own; the reaping contract is the
        // supervisor's, this is the test's backstop.
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while process_alive(pid) {
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if !process_alive(pid) {
            return;
        }
        eprintln!("worker {pid} survived teardown kill round {round}; re-killing");
    }
}

#[test]
fn the_interactive_first_paint_never_awaits_the_analytics_flush() {
    if !cfg!(debug_assertions) {
        // The stall is the debug-only endpoint seam; a release client
        // keeps the one product endpoint, so a release run of this test
        // would neither see the stall nor stay off production analytics.
        eprintln!(
            "release build: skipping the startup first-paint e2e — its \
             analytics-stub seam is debug-only"
        );
        return;
    }
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

    let dir = tempfile::TempDir::new().expect("temp dir");
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).expect("agent dir");
    // A settled home: onboarding already shown, a default model, the
    // telemetry disclosure seen — the operator's steady state, so the
    // client goes straight to the session screen.
    std::fs::write(
        agent_dir.join("settings.json"),
        serde_json::json!({
            "onboardingShown": true,
            "defaultProvider": "prime-inference",
            "defaultModel": "internal/glm-5.3-fast",
            "telemetry": {"noticeShown": true},
        })
        .to_string(),
    )
    .expect("settings.json");

    // The analytics stub: accepts the request and never answers, so an
    // awaited flush stalls the full request timeout. The first request
    // byte latches the observation signal — the test proves its own
    // stimulus (a missing sink would leave it unset).
    let stub = TcpListener::bind("127.0.0.1:0").expect("bind the stub");
    let stub_port = stub.local_addr().expect("stub addr").port();
    let request_observed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal = std::sync::Arc::clone(&request_observed);
    std::thread::spawn(move || {
        while let Ok((mut stream, _)) = stub.accept() {
            let mut buffer = [0u8; 4096];
            loop {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        signal.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
        }
    });

    let supervisor = spawn_supervisor(dir.path());
    let pty = openpty(
        Some(&Winsize {
            ws_row: 40,
            ws_col: 120,
            ws_xpixel: 0,
            ws_ypixel: 0,
        }),
        None,
    )
    .expect("openpty");

    let started = Instant::now();
    let mut client = spawn_client(&supervisor.socket, dir.path(), &pty.slave, stub_port);
    let mut reader = PtyReader::new(pty.master);

    reader.wait_for_alt_screen(Duration::from_secs(30));
    let elapsed = started.elapsed();
    // The flush stimulus, proven: the client stays up until the stub has
    // seen the request (bounded, off the paint measurement) — a client
    // that somehow lost its sink would paint fast and fail here instead
    // of a false green.
    let stimulus_deadline = Instant::now() + STUB_REQUEST_BOUND;
    while !request_observed.load(std::sync::atomic::Ordering::SeqCst) {
        if Instant::now() > stimulus_deadline {
            break;
        }
        let mut buffer = [0u8; 8192];
        if let Ok(n) = reader.file.read(&mut buffer) {
            reader.output.extend_from_slice(&buffer[..n]);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let stimulus = request_observed.load(std::sync::atomic::Ordering::SeqCst);
    let _ = client.kill();
    let _ = client.wait();
    drop(supervisor);

    assert!(
        stimulus,
        "the analytics stub never saw the request — the test's stall was \
         never exercised; check the endpoint seam reaches the client's sink"
    );
    assert!(
        elapsed < PAINT_BOUND,
        "the first paint waited {elapsed:?} for the analytics flush — the \
         interactive start must never hold the pane blank behind telemetry \
         delivery (the stub's stall is 1.5s, the bound {PAINT_BOUND:?}): a \
         new herdr tab pays the whole network round trip before anything \
         paints. pty tail:\n{}",
        String::from_utf8_lossy(&reader.output),
    );
}

/// Non-blocking reader over the pty master, collecting the raw byte
/// stream the child writes.
struct PtyReader {
    file: std::fs::File,
    output: Vec<u8>,
}

impl PtyReader {
    fn new(master: OwnedFd) -> PtyReader {
        let fd = master.as_raw_fd();
        fcntl(fd, F_SETFL(OFlag::O_NONBLOCK)).expect("pty master non-blocking");
        PtyReader {
            file: master.into(),
            output: Vec::new(),
        }
    }

    /// Drain the master until the alt-screen enter (the first paint) or
    /// the deadline; returns when the paint landed.
    fn wait_for_alt_screen(&mut self, bound: Duration) -> Instant {
        let deadline = Instant::now() + bound;
        loop {
            if self
                .output
                .windows(8)
                .any(|window| window == b"\x1b[?1049h")
            {
                return Instant::now();
            }
            let mut buffer = [0u8; 8192];
            match self.file.read(&mut buffer) {
                Ok(0) | Err(_) => {}
                Ok(n) => self.output.extend_from_slice(&buffer[..n]),
            }
            if Instant::now() > deadline {
                return Instant::now();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// The environment every spawned product process gets: the sandbox home
/// and agent dir, the worker-role vars of this runner stripped (a CLI
/// running inside a daemon worker must not leak them), and no ambient
/// credentials.
fn sandbox_env(command: &mut Command, dir: &Path, extra: &[(&str, String)]) {
    for var in [
        pa_daemon::worker::WORKER_ROLE_ENV,
        pa_daemon::worker::WORKER_TOKEN_ENV,
        pa_daemon::worker::WORKER_ACTIVE_SESSION_ID_ENV,
        pa_daemon::worker::WORKER_RECOVERY_JOURNAL_ENV,
        pa_daemon::worker::WORKER_SUPERVISOR_SOCKET_ENV,
        pa_daemon::worker::WORKER_SOCKET_ENV,
        pa_daemon::worker::WORKER_INSTANCE_ID_ENV,
        pa_daemon::worker::WORKER_SCRIPT_ENV,
        "PRIME_AGENT_INTERNAL_SESSION_LEASE_OWNER_ID",
    ] {
        command.env_remove(var);
    }
    for provider in pa_ai::models_generated::get_providers() {
        if let Some(vars) = pa_ai::env_api_keys::get_api_key_env_vars(provider) {
            for var in vars {
                command.env_remove(var);
            }
        }
    }
    command
        .env_remove("PRIME_TEAM_ID")
        .env("HOME", dir.join("home"))
        .env("PRIME_AGENT_CODING_AGENT_DIR", dir.join("agent"))
        .env_remove("TMUX");
    for (key, value) in extra {
        command.env(key, value);
    }
}

/// Spawn the real supervisor on the sandbox socket (the interactive
/// daemon e2e's spawn shape: worker env stripped, offline catalog,
/// die-with-the-test-binary).
#[allow(clippy::zombie_processes)]
fn spawn_supervisor(dir: &Path) -> Supervisor {
    std::fs::create_dir_all(dir.join("home")).expect("home dir");
    let socket = dir.join("daemon.sock");
    let mut command = Command::new(env!("CARGO_BIN_EXE_prime-agent"));
    command
        .args(["--mode", "daemon", "--daemon-socket"])
        .arg(&socket)
        .env("PI_OFFLINE", "1")
        .env_remove("DO_NOT_TRACK")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    sandbox_env(&mut command, dir, &[]);
    // PDEATHSIG is a Linux hook; the Drop guard's kill path covers the
    // other unix targets.
    #[cfg(target_os = "linux")]
    unsafe {
        command.pre_exec(move || {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            Ok(())
        });
    }
    let child = command.spawn().expect("spawn prime-agent --mode daemon");
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if socket.exists() {
            return Supervisor { child, socket };
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("supervisor socket never appeared");
}

/// The interactive client on the pty: telemetry on (the default), the
/// analytics endpoint pointed at the never-answering stub through the
/// debug-only seam, and the herdr pane identity the operator's tabs run
/// with.
fn spawn_client(socket: &Path, dir: &Path, slave: &OwnedFd, stub_port: u16) -> Child {
    fn make_process_group() -> std::io::Result<()> {
        nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))?;
        Ok(())
    }
    let slave_stdio = || {
        let clone = slave.try_clone().expect("clone pty slave");
        Stdio::from(clone)
    };
    std::fs::create_dir_all(dir.join("work")).expect("work dir");
    let mut command = Command::new(env!("CARGO_BIN_EXE_prime-agent"));
    command
        .arg("--daemon-socket")
        .arg(socket)
        .env("TERM", "xterm-256color")
        .env_remove("PI_OFFLINE")
        .env_remove("DO_NOT_TRACK")
        .env_remove("PRIME_AGENT_TELEMETRY")
        .env(
            pa_core::session_engine::telemetry::TEST_ANALYTICS_ENDPOINT_ENV,
            format!("http://127.0.0.1:{stub_port}"),
        )
        .env("HERDR_ENV", "1")
        .env("HERDR_PANE_ID", "pane-e2e")
        .env("HERDR_SOCKET_PATH", dir.join("herdr.sock"))
        .current_dir(dir.join("work"))
        .stdin(slave_stdio())
        .stdout(slave_stdio())
        .stderr(slave_stdio());
    sandbox_env(&mut command, dir, &[]);
    unsafe { command.pre_exec(make_process_group) };
    command.spawn().expect("spawn the interactive client")
}

fn session_runner() -> bool {
    // SAFETY: tcgetpgrp only queries the fd's foreground process group.
    let foreground = unsafe { libc::tcgetpgrp(0) };
    if foreground < 0 {
        eprintln!(
            "no controlling-terminal session on the runner (tcgetpgrp(fd 0) \
             failed); skipping the startup first-paint e2e — it needs a \
             controlling-terminal session to drive the pty child"
        );
        return false;
    }
    true
}

/// Stop the supervisor by protocol so it can shut its workers down; the
/// caller kills the child as the backstop.
fn graceful_shutdown(socket: &Path) -> Option<u32> {
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixStream;

    let Ok(stream) = UnixStream::connect(socket) else {
        return None;
    };
    // A daemon that accepts but stops answering must not park the
    // teardown: every read is bounded, the kill backstop stays reachable.
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let Ok(write_half) = stream.try_clone() else {
        return None;
    };
    let mut reader = BufReader::new(stream);
    let mut writer = write_half;
    let mut hello = String::new();
    let _ = reader.read_line(&mut hello); // daemon_hello
    let supervisor_pid = serde_json::from_str::<serde_json::Value>(hello.trim())
        .ok()
        .and_then(|hello| hello["supervisorPid"].as_u64())
        .map(|pid| pid as u32);

    let command = serde_json::json!({
        "type": "command",
        "id": "test-shutdown",
        "protocol": { "name": "prime-agent.daemon", "version": 7 },
        "command": { "type": "shutdown" },
    });
    let mut line = serde_json::to_string(&command).expect("serialize shutdown");
    line.push('\n');
    if writer.write_all(line.as_bytes()).is_err() {
        return None;
    }
    let _ = writer.flush();
    let mut response = String::new();
    let _ = reader.read_line(&mut response);
    supervisor_pid
}
