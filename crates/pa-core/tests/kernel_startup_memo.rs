// The Tier-C/D ruling (fleet-uniform, 2026-09-28) - this target's own
// crate root: the same bounded-boundary disposition as src/lib.rs
// (large_futures/too_many_lines/the cast family; details there).
#![allow(
    clippy::large_futures,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

//! Verifier integration tests for the startup memo's generation contract
//! and the provisioner's mid-boot lifecycle edges (all on the
//! single-threaded `#[tokio::test]` runtime, whose scheduler runs a whole
//! wake chain - park, publish, clear, taker - in one drain; the fixtures
//! therefore observe state at drain boundaries and never rely on sleeping):
//!
//! - a settled boot's publisher clears only the memo generation it
//!   installed (TS `ensure()`'s `managerPromise === startup` guard): a
//!   boot doomed by a mid-flight `kill()` settles against a NEWER memo
//!   armed underneath it and must leave that memo alone;
//! - `kill()` before publication leaves no resident kernel (TS `kill()`
//!   clears `managerPromise`; the doomed boot's own settle kills its
//!   kernel instead of parking it);
//! - `dispose()` during a boot owns that boot: it waits the in-flight
//!   startup, and the disposed boot settles without parking a kernel
//!   (the atomic settle/publish's invariant).
//!
//! The kernel Python is ambient product state (the auto-bootstrapped kernel
//! venv); like `kernel_stop_revive.rs`, these tests skip (with a note) on
//! machines without a live install so the suite stays hermetic elsewhere.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pa_core::kernel::provisioner::{IpythonKernelProvisioner, IpythonKernelProvisionerOptions};
use pa_core::kernel::shared::{ExecuteOptions, ExecuteStatus};
use std::os::unix::fs::PermissionsExt;

/// The kernel Python with prime-agent-runtime installed (see
/// `kernel_stop_revive.rs`); skipped with a note when absent.
fn kernel_python() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("PA_CORE_KERNEL_PYTHON") {
        let explicit = PathBuf::from(explicit);
        assert!(
            explicit.exists(),
            "PA_CORE_KERNEL_PYTHON {} not found",
            explicit.display()
        );
        return Some(explicit);
    }
    let candidate = PathBuf::from(std::env::var("HOME").map_or_else(
        |_| "/home/ubuntu/.prime/agent/kernel-venv/bin/python".to_string(),
        |home| format!("{home}/.prime/agent/kernel-venv/bin/python"),
    ));
    if candidate.exists() {
        return Some(candidate);
    }
    eprintln!(
        "kernel python {} not found; skipping live startup-memo test",
        candidate.display()
    );
    None
}

/// A kernel interpreter wrapper that counts spawns into `count` before
/// exec'ing the real kernel Python, so a test can pin exactly how many
/// kernels were armed. Lives in the session dir, which outlives the
/// kernel spawns the test observes.
fn counting_kernel(
    dir: &std::path::Path,
    python: &std::path::Path,
    count: &std::path::Path,
) -> PathBuf {
    let wrapper = dir.join("counting-python");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nprintf x >> '{}'\nexec '{}' \"$@\"\n",
            count.display(),
            python.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    wrapper
}

/// Number of interpreter spawns recorded so far.
fn starts(count: &std::path::Path) -> u64 {
    count.metadata().map_or(0, |m| m.len())
}

/// A settled boot's publisher clears only the memo generation it installed
/// (TS `ensure()`'s `managerPromise === startup` guard). `kill()` clears the
/// memo mid-boot (the generation invalidation), a NEWER memo is armed before
/// the doomed boot settles, and the doomed settle's clear must leave that
/// newer memo alone: the late joiner below must land on the newer boot, and
/// exactly two interpreters may spawn.
#[tokio::test]
async fn stale_publisher_cannot_clear_a_newer_startup_memo() {
    let Some(python) = kernel_python() else {
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let count = dir.path().join("starts");
    let wrapped = counting_kernel(dir.path(), &python, &count);
    let provisioner = IpythonKernelProvisioner::new(
        dir.path(),
        IpythonKernelProvisionerOptions {
            python: Some(wrapped),
            ..Default::default()
        },
    );
    // Boot one, doomed by a kill before publication.
    let doomed = tokio::spawn({
        let provisioner = provisioner.clone();
        async move { provisioner.ensure(None, None).await }
    });
    // The interpreter has spawned; the boot is still mid-handshake.
    tokio::time::timeout(Duration::from_secs(30), async {
        while starts(&count) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the fixture interpreter spawned");
    provisioner.kill();
    // Arm the NEWER generation while the doomed boot is still settling.
    let newer = tokio::spawn({
        let provisioner = provisioner.clone();
        async move { provisioner.ensure(None, None).await }
    });
    // The doomed boot's settle (its memo clear) runs before this resolves.
    let settled = tokio::time::timeout(Duration::from_secs(30), doomed)
        .await
        .expect("the doomed boot settled")
        .unwrap();
    assert!(
        settled.is_err(),
        "a kill before publication must not deliver a kernel"
    );
    // The newer boot survived the doomed settle; the late ask joins it.
    let late = tokio::time::timeout(Duration::from_secs(30), provisioner.ensure(None, None))
        .await
        .expect("the late ask settled")
        .unwrap();
    let newer = tokio::time::timeout(Duration::from_secs(30), newer)
        .await
        .expect("the newer ask settled")
        .unwrap()
        .unwrap();
    assert_eq!(
        starts(&count),
        2,
        "the doomed boot and the newer one; a stale clear must not arm a duplicate"
    );
    for manager in [&late, &newer] {
        let result = manager
            .execute("1 + 1", ExecuteOptions::default())
            .await
            .unwrap();
        assert_eq!(result.status, ExecuteStatus::Ok);
    }
}

/// `kill()` before publication must not leave a resident kernel: the kill
/// clears the startup memo (TS `kill()` clears `managerPromise`), so the
/// doomed boot's own settle kills its kernel instead of parking it, and
/// the next `ensure()` boots fresh.
#[tokio::test]
async fn kill_during_boot_leaves_no_resident_kernel() {
    let Some(python) = kernel_python() else {
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let count = dir.path().join("starts");
    let wrapped = counting_kernel(dir.path(), &python, &count);
    let provisioner = IpythonKernelProvisioner::new(
        dir.path(),
        IpythonKernelProvisionerOptions {
            python: Some(wrapped),
            ..Default::default()
        },
    );
    let boot = tokio::spawn({
        let provisioner = provisioner.clone();
        async move { provisioner.ensure(None, None).await }
    });
    // The interpreter has spawned but the boot has not published yet.
    tokio::time::timeout(Duration::from_secs(30), async {
        while starts(&count) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the fixture interpreter spawned");
    provisioner.kill();
    let settled = tokio::time::timeout(Duration::from_secs(30), boot)
        .await
        .expect("the doomed boot settled")
        .unwrap();
    assert!(
        settled.is_err(),
        "a kill before publication must not deliver a kernel"
    );
    assert!(
        provisioner.manager().is_none(),
        "no kernel may park after a kill"
    );
    assert!(!provisioner.has_running_kernel());
    // TS kill() clears the memo: the next ensure() starts a fresh boot.
    let fresh = tokio::time::timeout(Duration::from_secs(30), provisioner.ensure(None, None))
        .await
        .expect("the next ensure booted fresh")
        .unwrap();
    let result = fresh
        .execute("1 + 1", ExecuteOptions::default())
        .await
        .unwrap();
    assert_eq!(result.status, ExecuteStatus::Ok);
    assert_eq!(
        starts(&count),
        2,
        "the doomed boot and the fresh boot, nothing else"
    );
}

/// `dispose()` during a boot owns that boot: it waits the in-flight
/// startup, and the disposed boot settles as a failure without ever
/// parking a kernel - the atomic settle/publish's invariant. A boot that
/// published between separate check and publish scopes could park a live
/// kernel into a provisioner that had already reported itself torn down.
#[tokio::test]
async fn dispose_during_boot_settles_it_without_parking() {
    let Some(python) = kernel_python() else {
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let count = dir.path().join("starts");
    let wrapped = counting_kernel(dir.path(), &python, &count);
    let provisioner = IpythonKernelProvisioner::new(
        dir.path(),
        IpythonKernelProvisionerOptions {
            python: Some(wrapped),
            ..Default::default()
        },
    );
    let boot = tokio::spawn({
        let provisioner = provisioner.clone();
        async move { provisioner.ensure(None, None).await }
    });
    // The interpreter has spawned; the boot is still mid-handshake.
    tokio::time::timeout(Duration::from_secs(30), async {
        while starts(&count) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the fixture interpreter spawned");
    provisioner.dispose(None).await;
    let settled = tokio::time::timeout(Duration::from_secs(30), boot)
        .await
        .expect("the disposed boot settled")
        .unwrap();
    assert!(
        settled.is_err(),
        "the disposed provisioner must reject the boot"
    );
    assert!(
        provisioner.manager().is_none(),
        "no kernel may park into a disposed provisioner"
    );
    assert!(!provisioner.has_running_kernel());
    // And nothing may park afterwards either: the provisioner stays
    // disposed, and no further interpreter spawns.
    let error = provisioner
        .ensure(None, None)
        .await
        .expect_err("a disposed provisioner rejects new boots");
    assert!(error.to_string().contains("disposed"));
    assert_eq!(
        starts(&count),
        1,
        "exactly one interpreter: the disposed boot, no duplicates"
    );
}

/// The kill-vs-boot contract composes with #3257's panic-recovery: a
/// panicking boot settles and clears its memo, the re-armed boot is a
/// fresh generation, and `kill()` mid-re-armed-boot still leaves no
/// resident kernel - the doomed re-armed boot is killed by its own settle.
#[tokio::test]
async fn kill_during_the_re_armed_boot_leaves_no_resident_kernel() {
    let Some(python) = kernel_python() else {
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let count = dir.path().join("starts");
    let wrapped = counting_kernel(dir.path(), &python, &count);
    let provisioner = IpythonKernelProvisioner::new(
        dir.path(),
        IpythonKernelProvisionerOptions {
            python: Some(wrapped),
            ..Default::default()
        },
    );
    // F1's self-heal: a panicking progress callback kills the boot before
    // the interpreter spawns; the memo settles and the next ensure boots
    // fresh (the panic-cleanup ordering #3257 added).
    let progress: pa_core::kernel::bootstrap::KernelBootstrapProgressHandler =
        Arc::new(|_| panic!("progress callback panic"));
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        provisioner.ensure(Some(progress), None),
    )
    .await
    .expect("panicked startup settled")
    .expect_err("panicked callback must not report success");
    assert!(format!("{error:#}").contains("kernel startup task failed"));
    assert_eq!(
        starts(&count),
        0,
        "the panicked boot spawned no interpreter"
    );
    // The re-armed boot (a fresh generation): kill it before publication.
    let re_armed = tokio::spawn({
        let provisioner = provisioner.clone();
        async move { provisioner.ensure(None, None).await }
    });
    tokio::time::timeout(Duration::from_secs(30), async {
        while starts(&count) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the re-armed interpreter spawned");
    provisioner.kill();
    let settled = tokio::time::timeout(Duration::from_secs(30), re_armed)
        .await
        .expect("the doomed re-armed boot settled")
        .unwrap();
    assert!(
        settled.is_err(),
        "a kill before publication must not deliver a kernel"
    );
    assert!(provisioner.manager().is_none());
    assert!(!provisioner.has_running_kernel());
    // The provisioner stays consistent: the next ensure boots fresh.
    let fresh = tokio::time::timeout(Duration::from_secs(30), provisioner.ensure(None, None))
        .await
        .expect("the next ensure booted fresh")
        .unwrap();
    let result = fresh
        .execute("1 + 1", ExecuteOptions::default())
        .await
        .unwrap();
    assert_eq!(result.status, ExecuteStatus::Ok);
    assert_eq!(
        starts(&count),
        2,
        "the doomed re-armed boot and the fresh boot, nothing else"
    );
}
