//! Credential readiness uses the host resolver and leaves rendering runnable.
use super::*;
use pa_core::auth::{AuthStorageBackend, FileAuthStorageBackend};
use pa_tui::theme::{ColorMode, Theme};
use pa_tui::view::AgentView;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn decision_key_readiness_matches_resolved_runtime_credentials() -> Result<()> {
    let empty_env = format!("PA_DECISION_EMPTY_{}", uuid::Uuid::new_v4().simple());
    std::env::set_var(&empty_env, "");
    let cases = [
        (json!({"type":"api_key", "key":""}), false),
        (json!({"type":"api_key", "key":"   "}), false),
        (json!({"type":"api_key", "key":"fixture-key"}), true),
        (json!({"type":"api_key", "key":"PATH"}), true),
        (json!({"type":"api_key", "key":empty_env}), false),
        (json!({"type":"api_key", "key":"!printf fixture-key"}), true),
        (json!({"type":"api_key", "key":"!printf ''"}), false),
        (json!({"type":"api_key", "key":"!exit 1"}), false),
        (
            json!({"type":"oauth", "access":"", "expires":i64::MAX}),
            false,
        ),
        (
            json!({"type":"oauth", "access":"fixture-token", "expires":i64::MAX}),
            true,
        ),
        (
            json!({"type":"oauth", "access":"expired-token", "expires":0}),
            false,
        ),
    ];
    let mut outcomes = Vec::new();
    for (credential, expected) in cases {
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("auth.json"),
            json!({"typesafe":credential}).to_string(),
        )?;
        let hook = TerminalMcpAuth::new(dir.path(), dir.path());
        outcomes.push((hook.api_key_ready("typesafe").await?, expected));
    }
    std::env::remove_var(empty_env);
    assert_eq!(
        outcomes
            .iter()
            .map(|(actual, _)| *actual)
            .collect::<Vec<_>>(),
        outcomes
            .into_iter()
            .map(|(_, expected)| expected)
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stalled_auth_store_cannot_block_a_tui_frame() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let auth_path = dir.path().join("auth.json");
    std::fs::write(
        &auth_path,
        json!({"typesafe":{"type":"api_key", "key":"fixture-key"}}).to_string(),
    )?;
    let backend = FileAuthStorageBackend::new(auth_path);
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let mut locked_tx = Some(locked_tx);
        backend
            .with_lock(&mut |_| {
                locked_tx.take().unwrap().send(()).unwrap();
                // This bound only cleans up a broken blocking implementation.
                // Passing requires rendering before this lock is released.
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
                Ok(((), None))
            })
            .unwrap();
    });
    locked_rx.await?;
    let hook = TerminalMcpAuth::new(dir.path(), dir.path());
    let mut ready = hook.api_key_ready("typesafe");
    let mut view = AgentView::new(Theme::builtin("prime", ColorMode::TrueColor));
    let paint = tokio::time::timeout(Duration::from_millis(250), async {
        tokio::select! {
            biased;
            result = &mut ready => panic!("resolver completed while its auth document was locked: {result:?}"),
            () = tokio::task::yield_now() => view.render_frame(80, 24),
        }
    }).await;
    let _ = release_tx.send(());
    holder.join().unwrap();
    assert!(!paint
        .expect("paint must complete while the credential lookup is stalled")
        .is_empty());
    assert!(ready.await?);
    Ok(())
}

#[tokio::test]
async fn corrupt_auth_document_reports_an_error_instead_of_missing_key() -> Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("auth.json"), "{broken-json")?;
    let hook = TerminalMcpAuth::new(dir.path(), dir.path());
    assert!(hook.api_key_ready("typesafe").await.is_err());
    Ok(())
}
