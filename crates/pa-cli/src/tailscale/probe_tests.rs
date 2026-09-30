//! Probe and trusted-resolution tests: the tailnet facts parsed from a real
//! spawned CLI (including a large-tailnet payload), the absent/erroring
//! states, and the fail-closed `PATH` resolution the public commands spawn
//! through - a `tailscale` planted in the current directory must never
//! execute.

use std::ffi::OsStr;
use std::path::Path;

use super::super::probe::resolve_tailscale_binary;
use super::super::*;
use super::*;

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

/// The resolution takes the first trusted entry holding an executable CLI.
#[test]
fn resolution_takes_the_first_trusted_absolute_entry() {
    let first = tempfile::tempdir().expect("temp dir");
    write_executable(&first.path().join("tailscale"), "#!/bin/sh\nexit 0\n");
    let second = tempfile::tempdir().expect("temp dir");
    write_executable(&second.path().join("tailscale"), "#!/bin/sh\nexit 0\n");
    let cwd = tempfile::tempdir().expect("temp dir");
    let path = std::env::join_paths([first.path(), second.path()]).expect("join paths");
    assert_eq!(
        resolve_tailscale_binary(path.as_os_str(), cwd.path(), "tailscale"),
        Some(first.path().join("tailscale"))
    );
}

/// A relative `PATH` entry (including the empty entry) resolves to the
/// current directory, and an entry naming the current directory is just as
/// untrusted: none may supply the CLI. The test runs *from* the working
/// directory so the relative entry really points at the planted binary.
#[test]
fn resolution_skips_relative_entries_and_the_current_directory() {
    let _env = crate::config::env_lock();
    // The process cwd is global: capture it in a drop guard, so a failed
    // assert unwinds through the restore instead of skipping it.
    let _cwd = CwdGuard::capture();
    let workspace = tempfile::tempdir().expect("temp dir");
    write_executable(&workspace.path().join("tailscale"), "#!/bin/sh\nexit 0\n");
    let real = tempfile::tempdir().expect("temp dir");
    write_executable(&real.path().join("tailscale"), "#!/bin/sh\nexit 0\n");
    std::env::set_current_dir(workspace.path()).expect("chdir workspace");
    let path =
        std::env::join_paths([Path::new("."), workspace.path(), real.path()]).expect("join paths");
    assert_eq!(
        resolve_tailscale_binary(path.as_os_str(), workspace.path(), "tailscale"),
        Some(real.path().join("tailscale"))
    );
    // The empty entry is the current directory in shell `PATH` semantics.
    assert_eq!(
        resolve_tailscale_binary(OsStr::new(""), workspace.path(), "tailscale"),
        None
    );
}

/// When only untrusted entries hold the CLI, the CLI is absent. Runs from
/// the working directory so the relative entries really point at the
/// planted binary.
#[test]
fn resolution_reports_no_cli_when_only_untrusted_entries_hold_it() {
    let _env = crate::config::env_lock();
    // Same guard: the cwd restores on every exit path, failed assert or not.
    let _cwd = CwdGuard::capture();
    let workspace = tempfile::tempdir().expect("temp dir");
    write_executable(&workspace.path().join("tailscale"), "#!/bin/sh\nexit 0\n");
    std::env::set_current_dir(workspace.path()).expect("chdir workspace");
    let path = std::env::join_paths([Path::new("."), Path::new(""), workspace.path()])
        .expect("join paths");
    assert_eq!(
        resolve_tailscale_binary(path.as_os_str(), workspace.path(), "tailscale"),
        None
    );
}

/// A failed assertion unwinds through the cwd guard: the process cwd is
/// restored even though the chdir'd temp directory has already been
/// removed, so the panic cannot leave other tests running from a deleted
/// directory.
#[test]
fn a_failed_assertion_restores_the_process_cwd() {
    let _env = crate::config::env_lock();
    let before = std::env::current_dir().expect("cwd");
    let unwind = std::panic::catch_unwind(|| {
        // Creation order mirrors the resolution tests: the guard first,
        // then the chdir'd directory, so the unwind drops the directory
        // before the guard restores the cwd.
        let _cwd = CwdGuard::capture();
        let workspace = tempfile::tempdir().expect("temp dir");
        std::env::set_current_dir(workspace.path()).expect("chdir workspace");
        panic!("simulate a failed assertion while chdir'd into the temp dir");
    });
    assert!(unwind.is_err(), "the simulated assertion must unwind");
    assert_eq!(
        std::env::current_dir().expect("cwd"),
        before,
        "the guard must restore the process cwd after the unwind"
    );
}

/// A non-executable `tailscale` in an entry does not resolve.
#[test]
fn resolution_skips_a_non_executable_candidate() {
    let plain = tempfile::tempdir().expect("temp dir");
    std::fs::write(plain.path().join("tailscale"), "not executable").expect("write");
    let executable = tempfile::tempdir().expect("temp dir");
    write_executable(&executable.path().join("tailscale"), "#!/bin/sh\nexit 0\n");
    let cwd = tempfile::tempdir().expect("temp dir");
    let path = std::env::join_paths([plain.path(), executable.path()]).expect("join paths");
    assert_eq!(
        resolve_tailscale_binary(path.as_os_str(), cwd.path(), "tailscale"),
        Some(executable.path().join("tailscale"))
    );
}
/// Windows installs `tailscale.exe`; the resolution must accept the
/// platform's binary name (`tailscale.exe` on Windows, selected by
/// [`tailscale_program_name`]).
#[test]
fn resolution_finds_the_windows_binary_name() {
    let trusted = tempfile::tempdir().expect("temp dir");
    write_executable(&trusted.path().join("tailscale.exe"), "#!/bin/sh\nexit 0\n");
    let cwd = tempfile::tempdir().expect("temp dir");
    let path = std::env::join_paths([trusted.path()]).expect("join paths");
    assert_eq!(
        resolve_tailscale_binary(path.as_os_str(), cwd.path(), "tailscale.exe"),
        Some(trusted.path().join("tailscale.exe"))
    );
    // The probe name is what the resolution joins: the unix name must not
    // find the exe-named install.
    assert_eq!(
        resolve_tailscale_binary(path.as_os_str(), cwd.path(), "tailscale"),
        None
    );
}

/// The regression the trusted resolution exists for: the public commands
/// must not execute a `tailscale` planted in the current directory,
/// reachable through a relative `PATH` entry or an entry naming the
/// directory itself - the spawned CLI runs with this process's credentials
/// and environment.
#[test]
fn the_public_commands_never_execute_a_tailscale_shadowing_the_cwd() {
    let _env = crate::config::env_lock();
    let workspace = tempfile::tempdir().expect("workspace");
    let marker = workspace.path().join("shadow-ran");
    // The shadow announces its execution.
    write_executable(
        &workspace.path().join("tailscale"),
        &format!(
            "#!/bin/sh\necho ran > '{marker}'\nexit 0\n",
            marker = marker.display()
        ),
    );
    let real = Shim::write(ONLINE, "{}");
    // The cwd guard covers the unwind paths (a panic inside a bridge call
    // would otherwise leak the chdir'd directory).
    let _cwd = CwdGuard::capture();
    let previous_path = std::env::var_os("PATH");
    std::env::set_current_dir(workspace.path()).expect("chdir workspace");
    // The poisoned entries come first; the original `PATH` rides last so
    // the shims' own shell tools (and any concurrently running test's
    // `PATH` binaries) keep resolving. The trusted resolution takes the
    // first trusted entry holding the CLI, which is the real shim.
    std::env::set_var(
        "PATH",
        format!(
            ".:{}:{}:{}",
            workspace.path().display(),
            real.dir().display(),
            previous_path
                .as_deref()
                .map(std::ffi::OsStr::to_string_lossy)
                .unwrap_or_default(),
        ),
    );
    // Run every public bridge under the poisoned env, then restore the
    // poisoned `PATH` before asserting so an assertion failure cannot
    // leak it; the cwd guard restores the directory on every exit path.
    let facts = tailscale_doctor_facts();
    let status_code = run_tailscale_status(false);
    let serve_code = run_tailscale_serve(3000.0, false);
    let serve_went_to_real = real
        .argvs()
        .iter()
        .any(|argv| argv == &["serve", "--bg", "localhost:3000"]);
    match previous_path {
        Some(path) => std::env::set_var("PATH", path),
        None => std::env::remove_var("PATH"),
    }
    assert!(
        !marker.exists(),
        "the shadowing ./tailscale executed; facts: {facts:?}"
    );
    // Only the trusted entry's CLI ran: doctor sees its working status, and
    // the serve spawn went to it.
    assert_eq!(status_code, 0, "the trusted CLI must answer status");
    assert_eq!(serve_code, 1);
    assert!(
        facts.iter().any(|fact| fact.contains("on tailnet")),
        "the trusted CLI must answer: {facts:?}"
    );
    assert!(
        serve_went_to_real,
        "the trusted CLI must receive the serve spawn: {:?}",
        real.argvs()
    );
}

/// The process working directory on scope exit (including panics): a drop
/// guard, so a failed assert cannot leak the chdir'd temp directory to the
/// binary's other tests - the `CwdGuard` convention from
/// `pa-types/src/platform/transport.rs`.
struct CwdGuard(std::path::PathBuf);

impl CwdGuard {
    fn capture() -> Self {
        Self(std::env::current_dir().expect("current dir"))
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}
