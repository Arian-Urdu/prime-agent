//! `prime-agent tailscale`: the Tailscale detection core (TS
//! `packages/coding-agent/src/cli/tailscale.ts`, PR #2512, trimmed from the
//! detection half of #2451).
//!
//! [`probe_tailscale`] is the detection seam the tailnet agent mesh builds on
//! (peer discovery, remote sessions labeled with their tailscale connection,
//! cross-machine messaging/spawn). This module ships the detection itself: the
//! `tailscale` command group and the `doctor` fact. The mesh layers ship
//! separately.
//!
//! The `tailscale` binary is spawned with a bounded timeout, so a hung
//! `tailscaled` cannot wedge the CLI. The TS port capped `spawnSync` output at
//! 32 MiB because Node's 1 MiB default fails ENOBUFS on a large tailnet
//! (`status --json` carries every peer, ~1 KiB of JSON each); Rust's
//! `Command::output` has no such default cap, so this port reads the whole
//! payload and pins the large-tailnet case with a regression test.

use std::ffi::OsStr;
use std::io::Write;
use std::time::Duration;

use serde_json::Value;

pub(crate) mod format;

use format::{bold, green, red, yellow};

/// The CLI this module wraps (the TS `cliPath` value).
pub(crate) const TAILSCALE_BINARY: &str = "tailscale";

/// Bound on every probe call (TS `timeout: 15000`).
const TAILSCALE_TIMEOUT: Duration = Duration::from_secs(15);

/// Bound on the interactive serve/funnel call (TS `timeout: 60000`).
const SERVE_TIMEOUT: Duration = Duration::from_mins(1);

/// This node's tailnet state (TS `TailscaleProbe`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TailscaleProbe {
    /// `Some("tailscale")` when the CLI was found and ran, `None` when absent.
    pub(crate) cli_path: Option<String>,
    /// True when this node is up on a tailnet right now.
    pub(crate) on_tailnet: bool,
    /// The tailnet's `MagicDNS` suffix (e.g. `tailnet-name.ts.net.`), or `None`.
    pub(crate) magic_dns_suffix: Option<String>,
    /// This node's tailnet hostname (without the suffix), or `None`.
    pub(crate) hostname: Option<String>,
    /// True when the backend is Running but the node is not online right now.
    pub(crate) offline_but_up: bool,
    /// The raw error line when the CLI exists but reports a failure.
    pub(crate) error: Option<String>,
}

/// The parsed `prime-agent tailscale ...` invocation (TS `TailscaleArgs`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TailscaleArgs {
    Serve { port: f64, funnel: bool },
    Status { json: bool },
    Error(String),
}

/// One `tailscale` invocation's result (TS `runTailscale`'s return).
#[derive(Debug)]
struct TailscaleRun {
    /// The exit code, or -1 when the CLI could not be spawned or timed out.
    code: i32,
    stdout: String,
    stderr: String,
    /// The spawn failure kind (TS `result.error.code`); `NotFound` is ENOENT.
    spawn_error: Option<std::io::ErrorKind>,
}

/// Run the tailscale CLI once, returning stdout/stderr or an error descriptor.
async fn run_tailscale(program: &OsStr, args: &[&str]) -> TailscaleRun {
    let output = tokio::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(TAILSCALE_TIMEOUT, output).await {
        Ok(Ok(output)) => TailscaleRun {
            code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            spawn_error: None,
        },
        Ok(Err(error)) => TailscaleRun {
            code: -1,
            stdout: String::new(),
            stderr: error.to_string(),
            spawn_error: Some(error.kind()),
        },
        Err(_) => TailscaleRun {
            code: -1,
            stdout: String::new(),
            stderr: format!(
                "tailscale {args:?} timed out after {}s",
                TAILSCALE_TIMEOUT.as_secs()
            ),
            spawn_error: None,
        },
    }
}

/// Detect the CLI and, when present, this node's tailnet state. The detection
/// seam the mesh builds on.
pub(crate) async fn probe_tailscale(program: &OsStr) -> TailscaleProbe {
    // Rust's own ENOENT detection - no `which` binary needed.
    let version = run_tailscale(program, &["version"]).await;
    if let Some(kind) = version.spawn_error {
        // ENOENT = genuinely absent; anything else (EACCES, a hung CLI) is an
        // installed-but-unusable CLI and must say so instead of "not found".
        if kind == std::io::ErrorKind::NotFound {
            return TailscaleProbe::default();
        }
        return TailscaleProbe {
            cli_path: Some(TAILSCALE_BINARY.to_string()),
            error: Some(format!("tailscale CLI could not be run ({kind:?})")),
            ..TailscaleProbe::default()
        };
    }
    let cli_path = Some(TAILSCALE_BINARY.to_string());
    let status = run_tailscale(program, &["status", "--json"]).await;
    if status.code != 0 {
        let first_line = status.stderr.split('\n').next().unwrap_or_default().trim();
        let error = if first_line.is_empty() {
            "tailscale status failed with no diagnostic".to_string()
        } else {
            first_line.to_string()
        };
        return TailscaleProbe {
            cli_path,
            error: Some(error),
            ..TailscaleProbe::default()
        };
    }
    match serde_json::from_str::<Value>(&status.stdout) {
        Ok(parsed) => {
            // BackendState distinguishes a stopped/logged-out daemon from a node
            // that is up but temporarily unreachable; only Running serves.
            let backend = parsed
                .get("BackendState")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let self_field = parsed.get("Self");
            let online = self_field
                .and_then(|value| value.get("Online"))
                .and_then(Value::as_bool);
            let dns_name = self_field
                .and_then(|value| value.get("DNSName"))
                .and_then(Value::as_str)
                .or_else(|| {
                    self_field
                        .and_then(|value| value.get("HostName"))
                        .and_then(Value::as_str)
                });
            // Top-level MagicDNSSuffix is deprecated upstream; prefer
            // CurrentTailnet's.
            let suffix = parsed
                .get("CurrentTailnet")
                .and_then(|value| value.get("MagicDNSSuffix"))
                .and_then(Value::as_str)
                .or_else(|| parsed.get("MagicDNSSuffix").and_then(Value::as_str))
                .map(str::to_string);
            let hostname = dns_name.filter(|name| !name.is_empty()).map(|name| {
                let mut host = trim_trailing_dots(name).to_string();
                if let Some(suffix) = &suffix {
                    let suffix = trim_trailing_dots(suffix);
                    if let Some(stripped) = host.strip_suffix(&format!(".{suffix}")) {
                        host = stripped.to_string();
                    }
                }
                host
            });
            TailscaleProbe {
                cli_path,
                on_tailnet: online == Some(true) || backend == "Running",
                magic_dns_suffix: suffix,
                hostname,
                offline_but_up: backend == "Running" && online == Some(false),
                error: None,
            }
        }
        Err(error) => TailscaleProbe {
            cli_path,
            error: Some(format!("unparseable status output: {error}")),
            ..TailscaleProbe::default()
        },
    }
}

/// Parse `tailscale serve status --json`. A tailnet with nothing served answers
/// a bare `null` (valid JSON and the common empty-config encoding), so
/// normalize a null payload to an empty serve config before any property
/// access; otherwise the status command and post-serve verification read a
/// valid empty config as unparseable output.
fn parse_serve_status(stdout: &str) -> Result<Value, serde_json::Error> {
    let parsed: Value = serde_json::from_str(stdout)?;
    Ok(match parsed {
        Value::Null => serde_json::json!({}),
        other => other,
    })
}

/// Parse `prime-agent tailscale ...` argv into a mode, port, funnel, and json
/// flag (TS `parseTailscaleArgs`).
pub(crate) fn parse_tailscale_args(args: &[String]) -> TailscaleArgs {
    let json = args.iter().any(|arg| arg == "--json");
    let rest: Vec<&str> = args
        .iter()
        .filter(|arg| arg.as_str() != "--json")
        .map(String::as_str)
        .collect();
    if rest.is_empty() || (rest.len() == 1 && rest[0] == "status") {
        return TailscaleArgs::Status { json };
    }
    // --json was stripped above, so a serve request carrying it would silently
    // run serve with human output; machine-readable output is status-only.
    if json
        && (rest.contains(&"serve")
            || rest.contains(&"--funnel")
            || rest
                .iter()
                .any(|arg| *arg == "--port" || arg.starts_with("--port=")))
    {
        return error_args("tailscale: --json is only supported for status");
    }
    let mut port: Option<f64> = None;
    let mut funnel = false;
    let mut saw_serve = false;
    let mut saw_status = false;
    let mut index = 0;
    while index < rest.len() {
        let token = rest[index];
        match token {
            "serve" => {
                if saw_serve || saw_status {
                    return error_args(format!("tailscale: {token} appears more than once"));
                }
                saw_serve = true;
            }
            "status" => {
                if saw_status || saw_serve {
                    return error_args(format!("tailscale: {token} appears more than once"));
                }
                saw_status = true;
            }
            "--funnel" => {
                if funnel {
                    return error_args("tailscale: --funnel appears more than once");
                }
                funnel = true;
            }
            _ if token == "--port" || token.starts_with("--port=") => {
                if port.is_some() {
                    return error_args("tailscale: --port appears more than once");
                }
                let value = if let Some(inline) = token.strip_prefix("--port=") {
                    Some(inline)
                } else {
                    index += 1;
                    rest.get(index).copied()
                };
                let Some(parsed) = value.and_then(js_number) else {
                    return error_args("--port requires a numeric value (1-65535)");
                };
                port = Some(parsed);
            }
            _ if token.starts_with('-') => {
                return error_args(format!("tailscale: unrecognized option {token}"));
            }
            _ => return error_args(format!("tailscale: unexpected argument {token}")),
        }
        index += 1;
    }
    if saw_status {
        if port.is_some() || funnel {
            return error_args("tailscale: status takes no serve flags");
        }
        return TailscaleArgs::Status { json };
    }
    if saw_serve || port.is_some() || funnel {
        let Some(port) = port else {
            return error_args(
                "tailscale serve requires --port <n> (the LOCAL port to expose); \
                 refusing to guess a default",
            );
        };
        return TailscaleArgs::Serve { port, funnel };
    }
    error_args(format!("tailscale: unknown subcommand {}", rest[0]))
}

fn error_args(message: impl Into<String>) -> TailscaleArgs {
    TailscaleArgs::Error(message.into())
}

/// JS `Number(value)`: `None` stands for `NaN` (the parse-rejection case).
fn js_number(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Some(0.0);
    }
    trimmed
        .parse::<f64>()
        .ok()
        .filter(|parsed| !parsed.is_nan())
}

/// Print the tailnet overview (human or `--json`; TS `runTailscaleStatus`).
pub(crate) async fn run_status(program: &OsStr, json: bool) -> i32 {
    let mut stdout = std::io::stdout();
    run_status_to(&mut stdout, program, json).await
}

async fn run_status_to<W: Write>(out: &mut W, program: &OsStr, json: bool) -> i32 {
    let probe = probe_tailscale(program).await;
    if json {
        let _ = writeln!(out, "{}", status_json(&probe));
        return i32::from(probe.cli_path.is_none() || !probe.on_tailnet || probe.error.is_some());
    }
    if let Some(error) = &probe.error {
        let _ = writeln!(
            out,
            "{}",
            red(&format!("tailscale reported a problem: {error}"))
        );
        return 1;
    }
    if probe.cli_path.is_none() {
        let _ = writeln!(out, "{}", yellow("tailscale CLI not found on PATH"));
        let _ = writeln!(out, "Install Tailscale: https://tailscale.com/download");
        return 1;
    }
    if !probe.on_tailnet {
        // offlineButUp implies onTailnet, so an offline node reaches the success
        // path (which prints its state); this branch is genuinely not up.
        let _ = writeln!(
            out,
            "{}",
            yellow("Tailscale is installed but this machine is not up on a tailnet")
        );
        let _ = writeln!(out, "Run `tailscale up` first (or log in), then retry.");
        return 1;
    }
    let _ = writeln!(
        out,
        "{}: {}",
        bold("Tailnet"),
        if probe.offline_but_up {
            "up (currently offline)"
        } else {
            "on (this machine is online)"
        }
    );
    let _ = writeln!(
        out,
        "{}: {}",
        bold("MagicDNS suffix"),
        probe.magic_dns_suffix.as_deref().unwrap_or("unknown")
    );
    let _ = writeln!(
        out,
        "{}: {}",
        bold("This node"),
        probe.hostname.as_deref().unwrap_or("unknown")
    );
    let serve = run_tailscale(program, &["serve", "status", "--json"]).await;
    if serve.code != 0 {
        let _ = writeln!(
            out,
            "{}",
            yellow(&format!(
                "tailscale serve status failed (exit {}); served-local status is unavailable",
                serve.code
            ))
        );
        // The probe already confirmed this node is on a tailnet; a missing serve
        // capability is a warning, not a hard failure (matches --json behavior).
        return 0;
    }
    if let Ok(parsed) = parse_serve_status(&serve.stdout) {
        let rows = serve_rows(&parsed);
        if rows.is_empty() {
            let _ = writeln!(
                out,
                "{}: nothing (see `prime-agent help tailscale`)",
                bold("Served locally")
            );
        } else {
            let _ = writeln!(out, "{}:", bold("Served locally"));
            for row in rows {
                let _ = writeln!(out, "{row}");
            }
        }
    } else {
        let _ = writeln!(
            out,
            "{}",
            yellow(
                "tailscale serve status output was unparseable; served-local status is unavailable"
            )
        );
        // The probe already confirmed this node is on a tailnet; unparseable
        // serve output is a warning, not a hard failure.
    }
    0
}

/// Machine-readable status for `--json` (TS `tailscaleStatusJson`).
fn status_json(probe: &TailscaleProbe) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "cli": &probe.cli_path,
        "onTailnet": probe.on_tailnet,
        "offlineButUp": probe.offline_but_up,
        "magicDnsSuffix": &probe.magic_dns_suffix,
        "hostname": &probe.hostname,
        "error": &probe.error,
    }))
    .unwrap_or_default()
}

/// The `listen -> target` rows of a serve status payload: only a real
/// `TCPForward` is a raw TCP forward (an HTTPS/HTTP listener lands in `TCP`
/// with no `TCPForward`; its target is the `Web` row printed beside it), and
/// each `Web` handler prints as `listen/path -> target`.
fn serve_rows(parsed: &Value) -> Vec<String> {
    let mut rows = Vec::new();
    if let Some(tcp) = parsed.get("TCP").and_then(Value::as_object) {
        for (listen, entry) in tcp {
            if let Some(forward) = entry.get("TCPForward").and_then(Value::as_str) {
                rows.push(format!("  {listen} -> {forward}"));
            }
        }
    }
    if let Some(web) = parsed.get("Web").and_then(Value::as_object) {
        for (listen, server) in web {
            if let Some(handlers) = server.get("Handlers").and_then(Value::as_object) {
                for (path, handler) in handlers {
                    let target = handler
                        .get("Proxy")
                        .and_then(Value::as_str)
                        .or_else(|| handler.get("Path").and_then(Value::as_str))
                        .or_else(|| handler.get("Text").and_then(Value::as_str))
                        .unwrap_or("static");
                    rows.push(format!("  {listen}{path} -> {target}"));
                }
            }
        }
    }
    rows
}

/// Expose a local port on the tailnet (TS `runTailscaleServe`).
pub(crate) async fn run_serve(program: &OsStr, port: f64, funnel: bool) -> i32 {
    let mut stdout = std::io::stdout();
    run_serve_to(&mut stdout, program, port, funnel).await
}

async fn run_serve_to<W: Write>(out: &mut W, program: &OsStr, port: f64, funnel: bool) -> i32 {
    if !valid_port(port) {
        let _ = writeln!(
            out,
            "{}",
            red(&format!("--port must be 1-65535, got {port}"))
        );
        return 1;
    }
    // Validated: an integer in 1..=65535.
    let port = port as u16;
    let probe = probe_tailscale(program).await;
    if let Some(error) = &probe.error {
        let _ = writeln!(out, "{}", red(&format!("tailscale status failed: {error}")));
        return 1;
    }
    if probe.cli_path.is_none() {
        let _ = writeln!(out, "{}", yellow("tailscale CLI not found on PATH"));
        let _ = writeln!(out, "Install Tailscale: https://tailscale.com/download");
        return 1;
    }
    if !probe.on_tailnet {
        let _ = writeln!(
            out,
            "{}",
            yellow("This machine is not up on a tailnet (run `tailscale up` first)")
        );
        return 1;
    }
    if probe.offline_but_up {
        let _ = writeln!(
            out,
            "{}",
            yellow(
                "This node is up on a tailnet but currently offline - restore connectivity \
                 before serving"
            )
        );
        return 1;
    }
    let target = format!("localhost:{port}");
    let args: Vec<&str> = if funnel {
        vec!["funnel", "--bg", target.as_str()]
    } else {
        vec!["serve", "--bg", target.as_str()]
    };
    let _ = writeln!(out, "Running tailscale {} ...", args.join(" "));
    let code = spawn_serve(program, &args).await;
    if code != 0 {
        let _ = writeln!(
            out,
            "{}",
            red("tailscale did not accept the serve/funnel command (see its output above)")
        );
        return code;
    }
    // tailscale can exit 0 after only printing an interactive enable URL
    // (enableFeatureInteractive) without configuring anything; verify the target
    // is really served, matching the local endpoint EXACTLY (port 80 must not
    // match localhost:8000).
    let verify = run_tailscale(program, &["serve", "status", "--json"]).await;
    if verify.code != 0 {
        let _ = writeln!(
            out,
            "{}",
            red(&format!(
                "post-serve verification failed: tailscale serve status exited {}",
                verify.code
            ))
        );
        return 1;
    }
    let (served_exactly, funnel_enabled) = if let Ok(parsed) = parse_serve_status(&verify.stdout) {
        verify_served(&parsed, port)
    } else {
        // Serve-status output was unparseable: report the parse failure
        // (matching the status command), not a pending-enable flow.
        let _ = writeln!(
            out,
            "{}",
            red("post-serve verification failed: tailscale serve status output was unparseable")
        );
        return 1;
    };
    if !served_exactly {
        let _ = writeln!(
            out,
            "{}",
            yellow(
                "tailscale exited 0 but the target does not appear in `tailscale serve status` \
                 - an interactive enable flow (URL printed above) may still be pending; re-run \
                 this command after enabling."
            )
        );
        return 1;
    }
    if funnel && !funnel_enabled {
        let _ = writeln!(
            out,
            "{}",
            yellow(
                "the local target is served, but `tailscale serve status` reports the endpoint \
                 as NOT funnel-enabled - check your tailnet funnel ACL and the enable URL \
                 printed above, then re-run."
            )
        );
        return 1;
    }
    let _ = writeln!(out);
    if let Some(hostname) = &probe.hostname {
        let suffix = probe
            .magic_dns_suffix
            .as_deref()
            .map_or("ts.net", trim_trailing_dots);
        // Exact domain-suffix match: the hostname must end with ".<suffix>" (or
        // equal it).
        let host = if hostname.ends_with(&format!(".{suffix}")) || hostname == suffix {
            hostname.clone()
        } else {
            format!("{hostname}.{suffix}")
        };
        let _ = writeln!(
            out,
            "{}",
            green(&format!("Now reachable on your tailnet as {host}"))
        );
        if funnel {
            let _ = writeln!(out, "Public URL: https://{host}/");
        }
    }
    let _ = writeln!(
        out,
        "Stop with: tailscale serve status, then tailscale serve off (or funnel off)"
    );
    0
}

/// Whether a port is an integer in 1-65535 (TS `Number.isInteger(port) || ...`).
fn valid_port(port: f64) -> bool {
    port.is_finite() && port.fract().abs() < f64::EPSILON && (1.0..=65535.0).contains(&port)
}

/// Run the interactive serve/funnel call (TS `stdio: "inherit"`: stdin, stdout,
/// and stderr are inherited so funnel's first-enable prompt works). Returns the
/// exit code, or 1 when the spawn fails or the call times out.
async fn spawn_serve(program: &OsStr, args: &[&str]) -> i32 {
    let status = tokio::process::Command::new(program)
        .args(args)
        .kill_on_drop(true)
        .status();
    match tokio::time::timeout(SERVE_TIMEOUT, status).await {
        Ok(Ok(status)) => status.code().unwrap_or(1),
        Ok(Err(_)) | Err(_) => 1,
    }
}

/// Whether the target appears in a serve status payload (exact match), and
/// whether the matched endpoint is funnel-enabled.
fn verify_served(parsed: &Value, port: u16) -> (bool, bool) {
    let mut served_exactly = false;
    let mut funnel_enabled = false;
    let tcp_port = format!("{port}");
    if let Some(tcp) = parsed.get("TCP").and_then(Value::as_object) {
        for entry in tcp.values() {
            if let Some(forward) = entry.get("TCPForward").and_then(Value::as_str) {
                if forward == format!("127.0.0.1:{tcp_port}")
                    || forward == format!("localhost:{tcp_port}")
                {
                    served_exactly = true;
                }
            }
        }
    }
    if let Some(web) = parsed.get("Web").and_then(Value::as_object) {
        let allow_funnel = parsed.get("AllowFunnel").and_then(Value::as_object);
        for (listen, server) in web {
            let Some(handlers) = server.get("Handlers").and_then(Value::as_object) else {
                continue;
            };
            for handler in handlers.values() {
                let Some(proxy) = handler.get("Proxy").and_then(Value::as_str) else {
                    continue;
                };
                let Some(target) = proxy_target(proxy) else {
                    continue;
                };
                if target.port == port && target.loopback {
                    served_exactly = true;
                    if allow_funnel
                        .and_then(|map| map.get(listen))
                        .and_then(Value::as_bool)
                        == Some(true)
                    {
                        funnel_enabled = true;
                    }
                }
            }
        }
    }
    (served_exactly, funnel_enabled)
}

/// A parsed serve proxy target.
struct ProxyTarget {
    port: u16,
    loopback: bool,
}

/// The loopback host and port of a serve proxy target (TS `new URL(proxy)`,
/// for the `scheme://host[:port]` targets tailscale writes). `None` when the
/// target has no scheme or a non-numeric/explicit port above 65535.
fn proxy_target(proxy: &str) -> Option<ProxyTarget> {
    let (scheme, rest) = proxy.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let (host, explicit_port) = split_authority(authority);
    let port = match explicit_port {
        Some(text) => text.parse::<u16>().ok()?,
        // `URL.port` is "" for default ports (80 for http:, 443 for https:).
        None => {
            if scheme.eq_ignore_ascii_case("https") {
                443
            } else {
                80
            }
        }
    };
    Some(ProxyTarget {
        port,
        loopback: is_loopback_host(host),
    })
}

/// Split a URL authority into host and explicit port, honoring IPv6 brackets.
fn split_authority(authority: &str) -> (&str, Option<&str>) {
    if let Some(rest) = authority.strip_prefix('[') {
        return match rest.split_once(']') {
            Some((host, tail)) => (host, tail.strip_prefix(':').filter(|port| !port.is_empty())),
            None => (authority, None),
        };
    }
    match authority.rsplit_once(':') {
        Some((host, port))
            if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            (host, Some(port))
        }
        _ => (authority, None),
    }
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn trim_trailing_dots(value: &str) -> &str {
    value.trim_end_matches('.')
}

/// One-line doctor facts for `prime-agent doctor` (TS `tailscaleDoctorFacts`).
pub(crate) async fn doctor_facts(program: &OsStr) -> Vec<String> {
    let probe = probe_tailscale(program).await;
    if let Some(error) = &probe.error {
        return vec![format!("tailscale: CLI present but erroring ({error})")];
    }
    if probe.cli_path.is_none() {
        return vec![
            "tailscale: CLI not found (optional; install from https://tailscale.com/download)"
                .to_string(),
        ];
    }
    if !probe.on_tailnet {
        return vec!["tailscale: installed but not up on a tailnet (tailscale up)".to_string()];
    }
    let offline = if probe.offline_but_up {
        " (currently offline)"
    } else {
        ""
    };
    vec![format!(
        "tailscale: on tailnet (node {}, MagicDNS {}{offline})",
        probe.hostname.as_deref().unwrap_or("unknown"),
        probe.magic_dns_suffix.as_deref().unwrap_or("unknown")
    )]
}

/// Drive the async detection core from the synchronous public-command path
/// (the CLI's usual current-thread-runtime bridge, mirroring `provider_login`).
fn block_on<F: std::future::Future>(future: F) -> Option<F::Output> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()
        .map(|runtime| runtime.block_on(future))
}

/// `prime-agent tailscale` status (exit code): the sync bridge.
pub(crate) fn run_tailscale_status(json: bool) -> i32 {
    block_on(run_status(OsStr::new(TAILSCALE_BINARY), json)).unwrap_or(1)
}

/// `prime-agent tailscale serve` (exit code): the sync bridge.
pub(crate) fn run_tailscale_serve(port: f64, funnel: bool) -> i32 {
    block_on(run_serve(OsStr::new(TAILSCALE_BINARY), port, funnel)).unwrap_or(1)
}

/// The `doctor` fact line: the sync bridge.
pub(crate) fn tailscale_doctor_facts() -> Vec<String> {
    block_on(doctor_facts(OsStr::new(TAILSCALE_BINARY))).unwrap_or_default()
}

#[cfg(all(test, unix))]
#[path = "tests.rs"]
mod tests;
