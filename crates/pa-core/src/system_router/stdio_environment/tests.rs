//! The stdio adapter protocol battery: real subprocesses speaking the
//! JSON-lines contract.

use std::time::Duration;

use super::*;

/// Write a small python adapter to a temp dir and return (dir, command).
fn adapter(script: &str) -> (tempfile::TempDir, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("adapter.py");
    std::fs::write(&path, script).unwrap();
    (
        dir,
        vec![
            "python3".to_string(),
            "-u".to_string(),
            path.to_string_lossy().to_string(),
        ],
    )
}

const ECHO_ADAPTER: &str = r#"
import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    request = json.loads(line)
    kind = request.get("type")
    if kind == "close":
        break
    reply = {"id": request.get("id"), "ok": True}
    if kind == "init":
        reply["environment"] = {"actions": {"wait": {"description": "Wait one tick."}}}
    elif kind == "observe":
        reply["observation"] = {"text": "the screen", "fields": {"hp": 3}, "terminal": False}
    elif kind == "execute":
        reply["text"] = "pressed " + request.get("action", "")
        reply["terminal"] = False
    print(json.dumps(reply), flush=True)
"#;

#[tokio::test]
async fn the_adapter_round_trips_every_request() {
    let (_dir, command) = adapter(ECHO_ADAPTER);
    let env =
        StdioRouterEnvironment::new(command, None, 5_000, Some(serde_json::json!({"rom": "x"})));
    let info = env.init().await.unwrap().expect("init environment info");
    assert!(info.get("actions").is_some());
    env.reset("reach the overworld").await.unwrap();
    let observation = env.observe().await.unwrap();
    assert_eq!(observation.text, "the screen");
    assert_eq!(observation.fields["hp"], serde_json::json!(3));
    assert_eq!(observation.image, None);
    assert!(!observation.terminal);
    let execution = env
        .execute(
            "press",
            &std::collections::BTreeMap::from([("button".to_string(), "a".to_string())]),
        )
        .await
        .unwrap();
    assert_eq!(execution.text, "pressed press");
    env.close(RouterCloseOptions {
        budget_ms: Some(500),
    })
    .await;
    // Close is idempotent.
    env.close(RouterCloseOptions {
        budget_ms: Some(500),
    })
    .await;
}

#[tokio::test]
async fn an_adapter_error_reply_propagates() {
    let (_dir, command) = adapter(
        r#"
import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    request = json.loads(line)
    if request.get("type") == "close":
        break
    print(json.dumps({"id": request.get("id"), "ok": False, "error": "no rom loaded"}), flush=True)
"#,
    );
    let env = StdioRouterEnvironment::new(command, None, 5_000, None);
    let error = env.init().await.unwrap_err();
    assert_eq!(error.to_string(), "no rom loaded");
    env.close(RouterCloseOptions {
        budget_ms: Some(500),
    })
    .await;
}

#[tokio::test]
async fn an_adapter_that_exits_early_fails_the_pending_request() {
    let env = StdioRouterEnvironment::new(
        vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
        None,
        5_000,
        None,
    );
    let error = env.init().await.unwrap_err().to_string();
    assert!(
        error.contains("exited early") || error.contains("stdin failed"),
        "unexpected early-exit error: {error}"
    );
    env.close(RouterCloseOptions {
        budget_ms: Some(500),
    })
    .await;
}

#[tokio::test]
async fn a_silent_adapter_times_out_per_request() {
    let env = StdioRouterEnvironment::new(
        vec!["sh".to_string(), "-c".to_string(), "sleep 30".to_string()],
        None,
        150,
        None,
    );
    let started = std::time::Instant::now();
    let error = env.init().await.unwrap_err().to_string();
    assert_eq!(error, "environment adapter init timed out after 150ms");
    assert!(started.elapsed() < Duration::from_secs(5));
    // The close budget bounds the teardown even though the adapter ignores it.
    let started = std::time::Instant::now();
    env.close(RouterCloseOptions {
        budget_ms: Some(300),
    })
    .await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "cleanup stays inside its budget"
    );
}

#[tokio::test]
async fn an_oversized_unterminated_line_is_a_protocol_violation() {
    let env = StdioRouterEnvironment::new(
        vec![
            "python3".to_string(),
            "-c".to_string(),
            "import sys,time; sys.stdout.write('x'*1500000); sys.stdout.flush(); time.sleep(30)"
                .to_string(),
        ],
        None,
        10_000,
        None,
    );
    let error = env.init().await.unwrap_err().to_string();
    assert!(
        error.contains("unterminated reply line over 1000000 chars"),
        "unexpected overflow error: {error}"
    );
    env.close(RouterCloseOptions {
        budget_ms: Some(300),
    })
    .await;
}

#[tokio::test]
async fn an_invalid_utf8_line_is_a_protocol_violation() {
    let env = StdioRouterEnvironment::new(
        vec![
            "python3".to_string(),
            "-c".to_string(),
            "import sys,time; sys.stdout.buffer.write(b'\\xff\\n'); sys.stdout.flush(); time.sleep(30)"
                .to_string(),
        ],
        None,
        10_000,
        None,
    );
    let error = env.init().await.unwrap_err().to_string();
    assert!(
        error.contains("not valid UTF-8"),
        "unexpected utf8 error: {error}"
    );
    env.close(RouterCloseOptions {
        budget_ms: Some(300),
    })
    .await;
}

#[tokio::test]
async fn closing_an_unstarted_adapter_is_a_no_op() {
    let env = StdioRouterEnvironment::new(vec!["python3".to_string()], None, 1_000, None);
    env.close(RouterCloseOptions {
        budget_ms: Some(100),
    })
    .await;
}

#[tokio::test]
async fn an_unknown_reply_id_is_ignored() {
    let (_dir, command) = adapter(
        r#"
import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    request = json.loads(line)
    if request.get("type") == "close":
        break
    # A stray reply for an id nobody is waiting on, then the real one.
    print(json.dumps({"id": 999, "ok": True}), flush=True)
    print(json.dumps({"id": request.get("id"), "ok": True, "environment": {}}), flush=True)
"#,
    );
    let env = StdioRouterEnvironment::new(command, None, 5_000, None);
    let info = env.init().await.unwrap();
    assert_eq!(info, Some(serde_json::json!({})));
    env.close(RouterCloseOptions {
        budget_ms: Some(500),
    })
    .await;
}
