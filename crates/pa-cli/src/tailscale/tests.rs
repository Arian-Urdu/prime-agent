//! Unit tests for the tailscale detection core, mirroring the TS suite
//! (`test/tailscale.test.ts`): a fake `tailscale` binary on disk exercises the
//! real spawn/capture path (large-tailnet payloads included), and the pure
//! parser/verifier functions are tested directly.

use super::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

/// A realistic online status payload.
const ONLINE: &str = concat!(
    r#"{"BackendState":"Running","Self":{"Online":true,"HostName":"milk","#,
    r#""DNSName":"milk.tailnet.ts.net."},"MagicDNSSuffix":"tailnet.ts.net."}"#,
);

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write shim");
    let mut permissions = fs::metadata(path).expect("stat shim").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod shim");
}

/// A fake `tailscale` binary: answers `version` (exit 0), `status` with the
/// given payload, and `serve status` with the given payload; appends every
/// argv to `argv.log`.
struct Shim {
    dir: TempDir,
}

impl Shim {
    fn write(status_json: &str, serve_json: &str) -> Shim {
        let dir = tempfile::tempdir().expect("temp dir");
        fs::write(dir.path().join("status.json"), status_json).expect("status payload");
        fs::write(dir.path().join("serve.json"), serve_json).expect("serve payload");
        let script = format!(
            "#!/bin/sh\n\
             printf '%s\\n' \"$*\" >> '{log}'\n\
             [ \"$1\" = version ] && exit 0\n\
             if [ \"$1\" = status ]; then cat '{status}'; exit 0; fi\n\
             if [ \"$1\" = serve ] && [ \"$2\" = status ]; then cat '{serve}'; exit 0; fi\n\
             case \"$1\" in serve|funnel) exit 0;; esac\n\
             exit 1\n",
            log = dir.path().join("argv.log").display(),
            status = dir.path().join("status.json").display(),
            serve = dir.path().join("serve.json").display(),
        );
        write_executable(&dir.path().join("tailscale"), &script);
        Shim { dir }
    }

    /// A shim whose whole behavior is the given script.
    fn raw(script: &str) -> Shim {
        let dir = tempfile::tempdir().expect("temp dir");
        write_executable(&dir.path().join("tailscale"), script);
        Shim { dir }
    }

    fn path(&self) -> PathBuf {
        self.dir.path().join("tailscale")
    }

    fn argvs(&self) -> Vec<Vec<String>> {
        fs::read_to_string(self.dir.path().join("argv.log"))
            .map(|log| {
                log.lines()
                    .map(|line| line.split_whitespace().map(str::to_string).collect())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A serve-status payload whose Web map proxies the given local port.
fn serve_status_for(port: u32) -> String {
    serde_json::json!({
        "Web": {
            "milk.tailnet.ts.net:443": {
                "Handlers": { "/": { "Proxy": format!("http://127.0.0.1:{port}") } }
            }
        },
        "AllowFunnel": { "milk.tailnet.ts.net:443": true },
    })
    .to_string()
}

/// A realistic `status --json` payload for a `peer_count`-node tailnet: the real
/// one carries every peer (~1 KiB of JSON each), which is the shape that
/// overruns Node's spawnSync 1 MiB default buffer.
fn large_tailnet_status(peer_count: usize) -> String {
    let mut peers = serde_json::Map::new();
    for index in 0..peer_count {
        let ip = format!("100.64.{}.{}", index % 250, (index * 7) % 250);
        let public_key = format!("nodekey:{}{index:04x}", "a1b2c3d4e5f60718".repeat(3));
        peers.insert(
            public_key.clone(),
            serde_json::json!({
                "ID": format!("n{index}CNTRL"),
                "PublicKey": public_key,
                "HostName": format!("build-{index}"),
                "DNSName": format!("build-{index}.tailnet.ts.net."),
                "OS": if index % 3 == 0 { "linux" } else { "macOS" },
                "UserID": 123_456_789_u64,
                "TailscaleIPs": [ip, "fd7a:115c:a1e0:ab12:4843:cd96:6258:1c2d"],
                "AllowedIPs": [
                    format!("{ip}/32"),
                    "fd7a:115c:a1e0:ab12:4843:cd96:6258:1c2d/128"
                ],
                "Addrs": [format!("192.168.{}:41641", index % 250)],
                "CurAddr": format!("192.168.{}:41641", (index * 3) % 250),
                "Relay": "nyc",
                "RxBytes": 123_456_789_u64 + index as u64,
                "TxBytes": 98_765_432_u64 + index as u64,
                "Created": "2024-01-05T12:34:56.789012345Z",
                "LastSeen": "2026-09-28T19:00:00Z",
                "LastHandshake": "2026-09-28T18:59:00Z",
                "Online": true,
                "ExitNode": false,
                "ExitNodeOption": false,
                "Active": false,
                "PeerAPIURL": [format!("http://{ip}:43210")],
            }),
        );
    }
    serde_json::json!({
        "BackendState": "Running",
        "Self": { "Online": true, "HostName": "milk", "DNSName": "milk.tailnet.ts.net." },
        "MagicDNSSuffix": "tailnet.ts.net.",
        "CurrentTailnet": { "Name": "example.com", "MagicDNSSuffix": "tailnet.ts.net." },
        "Peer": serde_json::Value::Object(peers),
    })
    .to_string()
}

fn missing_binary() -> TempDir {
    tempfile::tempdir().expect("temp dir")
}

#[tokio::test]
async fn reports_a_null_cli_when_tailscale_is_not_on_path() {
    let dir = missing_binary();
    let probe = probe_tailscale(dir.path().join("tailscale").as_os_str()).await;
    assert!(probe.cli_path.is_none());
    assert!(!probe.on_tailnet);
    assert!(probe.error.is_none(), "{:?}", probe.error);
}

#[tokio::test]
async fn parses_tailnet_facts_from_a_working_status() {
    let shim = Shim::write(ONLINE, "{}");
    let probe = probe_tailscale(shim.path().as_os_str()).await;
    assert_eq!(probe.cli_path.as_deref(), Some("tailscale"));
    assert!(probe.on_tailnet);
    assert_eq!(probe.magic_dns_suffix.as_deref(), Some("tailnet.ts.net."));
    assert_eq!(probe.hostname.as_deref(), Some("milk"));
    assert!(!probe.offline_but_up);
}

#[tokio::test]
async fn surfaces_a_cli_error_line_when_status_fails() {
    let shim = Shim::raw("#!/bin/sh\necho \"shim failure\" >&2\nexit 1\n");
    let probe = probe_tailscale(shim.path().as_os_str()).await;
    assert_eq!(probe.cli_path.as_deref(), Some("tailscale"));
    assert!(!probe.on_tailnet);
    assert!(
        probe
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("shim failure"),
        "{:?}",
        probe.error
    );
}

#[tokio::test]
async fn parses_a_status_payload_larger_than_the_node_default_buffer() {
    let payload = large_tailnet_status(3000);
    assert!(
        payload.len() > 1024 * 1024,
        "payload is {} bytes",
        payload.len()
    );
    let shim = Shim::write(&payload, "{}");
    let probe = probe_tailscale(shim.path().as_os_str()).await;
    assert!(probe.error.is_none(), "{:?}", probe.error);
    assert!(probe.on_tailnet);
    assert_eq!(probe.magic_dns_suffix.as_deref(), Some("tailnet.ts.net."));
    assert_eq!(probe.hostname.as_deref(), Some("milk"));
}

#[tokio::test]
async fn prefers_current_tailnet_magic_dns_suffix_and_trims_the_hostname() {
    // Top-level MagicDNSSuffix is deprecated upstream; CurrentTailnet's value
    // wins, and the node's DNSName loses the suffix.
    let payload = serde_json::json!({
        "BackendState": "Running",
        "Self": { "Online": true, "DNSName": "milk.new.ts.net." },
        "MagicDNSSuffix": "deprecated.ts.net.",
        "CurrentTailnet": { "MagicDNSSuffix": "new.ts.net." },
    })
    .to_string();
    let shim = Shim::write(&payload, "{}");
    let probe = probe_tailscale(shim.path().as_os_str()).await;
    assert_eq!(probe.magic_dns_suffix.as_deref(), Some("new.ts.net."));
    assert_eq!(probe.hostname.as_deref(), Some("milk"));
}

#[tokio::test]
async fn falls_back_to_hostname_when_the_dns_name_is_absent() {
    let payload = serde_json::json!({
        "BackendState": "Running",
        "Self": { "Online": true, "HostName": "milk" },
        "CurrentTailnet": { "MagicDNSSuffix": "tailnet.ts.net." },
    })
    .to_string();
    let shim = Shim::write(&payload, "{}");
    let probe = probe_tailscale(shim.path().as_os_str()).await;
    // The bare HostName carries no suffix to trim.
    assert_eq!(probe.hostname.as_deref(), Some("milk"));
}

#[tokio::test]
async fn does_not_mark_a_healthy_online_node_as_offline() {
    let shim = Shim::write(ONLINE, &serve_status_for(3000));
    let program = shim.path();
    let probe = probe_tailscale(program.as_os_str()).await;
    assert!(probe.on_tailnet);
    assert!(!probe.offline_but_up);
    assert_eq!(run_status(program.as_os_str(), false).await, 0);
}

#[tokio::test]
async fn distinguishes_a_stopped_backend_from_up_but_offline() {
    let stopped = serde_json::json!({
        "BackendState": "Stopped",
        "Self": { "Online": false },
        "MagicDNSSuffix": "tailnet.ts.net.",
    })
    .to_string();
    let offline = serde_json::json!({
        "BackendState": "Running",
        "Self": { "Online": false, "HostName": "milk" },
        "MagicDNSSuffix": "tailnet.ts.net.",
    })
    .to_string();
    let stopped_shim = Shim::write(&stopped, "{}");
    let probe = probe_tailscale(stopped_shim.path().as_os_str()).await;
    assert!(!probe.on_tailnet);
    let offline_shim = Shim::write(&offline, "{}");
    let probe = probe_tailscale(offline_shim.path().as_os_str()).await;
    assert!(probe.on_tailnet);
    assert!(probe.offline_but_up);
}

#[tokio::test]
async fn reports_unparseable_status_output_as_a_parse_failure() {
    let shim = Shim::write("this is not JSON", "{}");
    let program = shim.path();
    let probe = probe_tailscale(program.as_os_str()).await;
    assert_eq!(probe.cli_path.as_deref(), Some("tailscale"));
    assert!(
        probe
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("unparseable status output"),
        "{:?}",
        probe.error
    );
    assert_eq!(run_status(program.as_os_str(), false).await, 1);
    assert_eq!(run_status(program.as_os_str(), true).await, 1);
}

#[tokio::test]
async fn reports_an_error_for_a_hard_status_failure_with_empty_stderr() {
    let shim = Shim::raw("#!/bin/sh\nexit 1\n");
    let program = shim.path();
    let probe = probe_tailscale(program.as_os_str()).await;
    assert_eq!(probe.cli_path.as_deref(), Some("tailscale"));
    assert!(
        probe
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("no diagnostic"),
        "{:?}",
        probe.error
    );
    assert_eq!(run_status(program.as_os_str(), false).await, 1);
}

#[tokio::test]
async fn status_exits_one_with_the_install_hint_when_the_cli_is_missing() {
    let dir = missing_binary();
    let missing = dir.path().join("tailscale");
    assert_eq!(run_status(missing.as_os_str(), false).await, 1);
    assert_eq!(run_status(missing.as_os_str(), true).await, 1);
}

#[tokio::test]
async fn status_exits_one_when_the_backend_is_stopped() {
    let stopped = serde_json::json!({
        "BackendState": "Stopped",
        "Self": { "Online": false },
    })
    .to_string();
    let shim = Shim::write(&stopped, "{}");
    let program = shim.path();
    assert_eq!(run_status(program.as_os_str(), false).await, 1);
    assert_eq!(run_status(program.as_os_str(), true).await, 1);
}

#[tokio::test]
async fn status_exits_zero_and_prints_forwards_and_web_rows() {
    let serve_status = serde_json::json!({
        "TCP": {
            "10000": { "TCPForward": "127.0.0.1:9000" },
            "443": { "HTTPS": true },
        },
        "Web": {
            "milk.tailnet.ts.net:443": {
                "Handlers": { "/": { "Proxy": "http://127.0.0.1:3000" } }
            }
        },
        "AllowFunnel": {},
    })
    .to_string();
    let shim = Shim::write(ONLINE, &serve_status);
    let program = shim.path();
    let mut json_buf = Vec::new();
    assert_eq!(
        run_status_to(&mut json_buf, program.as_os_str(), true).await,
        0
    );
    let mut buf = Vec::new();
    assert_eq!(run_status_to(&mut buf, program.as_os_str(), false).await, 0);
    let text = String::from_utf8(buf).expect("utf8");
    assert!(text.contains("127.0.0.1:9000"), "{text}");
    assert!(
        text.contains("milk.tailnet.ts.net:443/ -> http://127.0.0.1:3000"),
        "{text}"
    );
    // The HTTPS listener must not read as a TCP forward.
    assert!(!text.contains("-> tcp"), "{text}");
}

#[tokio::test]
async fn status_reads_a_null_serve_status_as_nothing_served() {
    // `tailscale serve status --json` answers a bare `null` when nothing is
    // served.
    let shim = Shim::write(ONLINE, "null");
    let program = shim.path();
    let mut buf = Vec::new();
    assert_eq!(run_status_to(&mut buf, program.as_os_str(), false).await, 0);
    let text = String::from_utf8(buf).expect("utf8");
    assert!(text.contains("nothing"), "{text}");
    assert!(!text.contains("unparseable"), "{text}");
}

#[tokio::test]
async fn status_returns_zero_when_serve_status_is_unavailable_after_a_successful_probe() {
    let shim = Shim::raw(&format!(
        "#!/bin/sh\n\
         [ \"$1\" = version ] && exit 0\n\
         if [ \"$1\" = status ]; then printf '%s' '{ONLINE}'; exit 0; fi\n\
         echo err >&2\nexit 1\n"
    ));
    assert_eq!(run_status(shim.path().as_os_str(), false).await, 0);
}

#[tokio::test]
async fn serve_refuses_ports_outside_the_valid_range_before_anything_else() {
    let shim = Shim::write(ONLINE, &serve_status_for(3000));
    let program = shim.path();
    assert_eq!(run_serve(program.as_os_str(), 0.0, false).await, 1);
    assert_eq!(run_serve(program.as_os_str(), 65536.0, false).await, 1);
    assert_eq!(run_serve(program.as_os_str(), 3000.5, false).await, 1);
    assert!(
        shim.argvs().is_empty(),
        "an invalid port must not spawn: {:?}",
        shim.argvs()
    );
}

#[tokio::test]
async fn serve_refuses_when_the_cli_is_missing() {
    let dir = missing_binary();
    let missing = dir.path().join("tailscale");
    assert_eq!(run_serve(missing.as_os_str(), 3000.0, false).await, 1);
}

#[tokio::test]
async fn serve_builds_serve_and_funnel_background_argv() {
    let shim = Shim::write(ONLINE, &serve_status_for(3000));
    let program = shim.path();
    assert_eq!(run_serve(program.as_os_str(), 3000.0, false).await, 0);
    assert_eq!(run_serve(program.as_os_str(), 3000.0, true).await, 0);
    let spawned: Vec<Vec<String>> = shim
        .argvs()
        .into_iter()
        .filter(|argv| {
            matches!(argv.first().map(String::as_str), Some("serve" | "funnel"))
                && argv.get(1).map(String::as_str) != Some("status")
        })
        .collect();
    assert_eq!(
        spawned,
        vec![
            vec![
                "serve".to_string(),
                "--bg".to_string(),
                "localhost:3000".to_string()
            ],
            vec![
                "funnel".to_string(),
                "--bg".to_string(),
                "localhost:3000".to_string()
            ],
        ]
    );
    // funnel requested, endpoint not funnel-enabled -> failure.
    let not_public = Shim::write(
        ONLINE,
        &serve_status_for(3000).replace(
            "\"milk.tailnet.ts.net:443\":true",
            "\"milk.tailnet.ts.net:443\":false",
        ),
    );
    assert_eq!(
        run_serve(not_public.path().as_os_str(), 3000.0, true).await,
        1
    );
}

#[tokio::test]
async fn post_serve_verification_does_not_match_a_longer_port_via_substring() {
    let longer = Shim::write(
        ONLINE,
        &serde_json::json!({
            "Web": {
                "milk.ts.net:443": {
                    "Handlers": { "/": { "Proxy": "http://localhost:8000" } }
                }
            }
        })
        .to_string(),
    );
    assert_eq!(run_serve(longer.path().as_os_str(), 80.0, false).await, 1);
    let exact_tcp = Shim::write(
        ONLINE,
        &serde_json::json!({ "TCP": { "443": { "TCPForward": "127.0.0.1:80" } } }).to_string(),
    );
    assert_eq!(
        run_serve(exact_tcp.path().as_os_str(), 80.0, false).await,
        0
    );
    let default_port = Shim::write(
        ONLINE,
        &serve_status_for(80).replace("http://127.0.0.1:80", "http://127.0.0.1"),
    );
    assert_eq!(
        run_serve(default_port.path().as_os_str(), 80.0, false).await,
        0
    );
}

#[tokio::test]
async fn post_serve_verification_treats_a_bare_https_proxy_target_as_port_443() {
    let shim = Shim::write(
        ONLINE,
        &serde_json::json!({
            "Web": {
                "milk.ts.net:443": {
                    "Handlers": { "/": { "Proxy": "https://127.0.0.1" } }
                }
            }
        })
        .to_string(),
    );
    assert_eq!(run_serve(shim.path().as_os_str(), 443.0, false).await, 0);
}

#[tokio::test]
async fn serve_fails_when_tailscale_exits_zero_without_serving_the_target() {
    let pending = Shim::write(ONLINE, "{}");
    assert_eq!(
        run_serve(pending.path().as_os_str(), 3000.0, false).await,
        1
    );
    let served = Shim::write(ONLINE, &serve_status_for(3000));
    assert_eq!(run_serve(served.path().as_os_str(), 3000.0, false).await, 0);
}

#[tokio::test]
async fn serve_treats_a_null_serve_status_as_nothing_served_not_a_parse_failure() {
    let shim = Shim::write(ONLINE, "null");
    let program = shim.path();
    let mut buf = Vec::new();
    assert_eq!(
        run_serve_to(&mut buf, program.as_os_str(), 3000.0, false).await,
        1
    );
    let text = String::from_utf8(buf).expect("utf8");
    assert!(text.contains("interactive enable flow"), "{text}");
    assert!(!text.contains("unparseable"), "{text}");
}

#[test]
fn parse_requires_a_port_for_serve_and_never_guesses_a_default() {
    assert!(matches!(
        parse_tailscale_args(&args(&["serve"])),
        TailscaleArgs::Error(_)
    ));
    assert!(matches!(
        parse_tailscale_args(&args(&["--funnel"])),
        TailscaleArgs::Error(_)
    ));
}

#[test]
fn parse_accepts_port_forms_and_funnel_together() {
    assert_eq!(
        parse_tailscale_args(&args(&["serve", "--port", "3000"])),
        TailscaleArgs::Serve {
            port: 3000.0,
            funnel: false
        }
    );
    assert_eq!(
        parse_tailscale_args(&args(&["serve", "--port=3000"])),
        TailscaleArgs::Serve {
            port: 3000.0,
            funnel: false
        }
    );
    assert_eq!(
        parse_tailscale_args(&args(&["--port", "3000", "--funnel"])),
        TailscaleArgs::Serve {
            port: 3000.0,
            funnel: true
        }
    );
    assert!(matches!(
        parse_tailscale_args(&args(&["--port", "abc"])),
        TailscaleArgs::Error(_)
    ));
}

#[test]
fn parse_treats_no_args_as_status_honors_json_and_rejects_unknown_subcommands() {
    assert_eq!(
        parse_tailscale_args(&args(&[])),
        TailscaleArgs::Status { json: false }
    );
    assert_eq!(
        parse_tailscale_args(&args(&["status", "--json"])),
        TailscaleArgs::Status { json: true }
    );
    assert!(matches!(
        parse_tailscale_args(&args(&["bogus"])),
        TailscaleArgs::Error(_)
    ));
}

#[test]
fn parse_rejects_json_with_serve_instead_of_silently_serving() {
    match parse_tailscale_args(&args(&["serve", "--port", "3000", "--json"])) {
        TailscaleArgs::Error(message) => {
            assert!(
                message.contains("--json is only supported for status"),
                "{message}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        parse_tailscale_args(&args(&["--json", "--port", "3000"])),
        TailscaleArgs::Error(_)
    ));
}

#[test]
fn parse_rejects_unconsumed_repeated_and_conflicting_arguments() {
    // "--funnel false" must NOT be parsed as funnel: true (public exposure!).
    assert!(matches!(
        parse_tailscale_args(&args(&["serve", "--port", "3000", "--funnel", "false"])),
        TailscaleArgs::Error(_)
    ));
    assert!(matches!(
        parse_tailscale_args(&args(&["serve", "--port", "3000", "--port", "4000"])),
        TailscaleArgs::Error(_)
    ));
    assert!(matches!(
        parse_tailscale_args(&args(&["serve", "--funel", "--port", "3000"])),
        TailscaleArgs::Error(_)
    ));
    assert!(matches!(
        parse_tailscale_args(&args(&["status", "--port", "3000"])),
        TailscaleArgs::Error(_)
    ));
    match parse_tailscale_args(&args(&["serve"])) {
        TailscaleArgs::Error(message) => assert!(message.contains("requires --port"), "{message}"),
        other => panic!("{other:?}"),
    }
}
