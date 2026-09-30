//! The `system_router.run` host-request battery: action-model precedence,
//! auth preflight, the allowlist pin, and one registry round trip.

use std::path::Path;

use serde_json::json;

use crate::kernel::shared::HostRequestPayload;

use super::*;

const MODELS_JSON: &str = r#"{
  "providers": {
    "testprov": {
      "baseUrl": "http://localhost:9",
      "apiKey": "router-key",
      "api": "openai-completions",
      "models": [
        { "id": "session-model", "name": "Session Model", "contextWindow": 128000 },
        { "id": "subagent-model", "name": "Subagent Model", "contextWindow": 128000 },
        { "id": "action-model", "name": "Action Model", "contextWindow": 128000 }
      ]
    }
  }
}"#;

fn write_catalog(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("models.json"), MODELS_JSON).unwrap();
}

fn session_model() -> Model {
    serde_json::from_value(json!({
        "id": "session-model", "name": "Session Model", "api": "openai-completions",
        "provider": "testprov", "baseUrl": "http://localhost:9", "reasoning": false,
        "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
        "contextWindow": 128_000, "maxTokens": 4096
    }))
    .unwrap()
}

fn host_config(
    dir: &Path,
    subagent_default_model: Option<&str>,
    allowed_models: Option<Vec<String>>,
) -> SystemRouterHostConfig {
    SystemRouterHostConfig {
        agent_dir: dir.to_path_buf(),
        cwd: dir.to_path_buf(),
        session_model: session_model(),
        session_id: "session-1".to_string(),
        subagent_default_model: subagent_default_model.map(str::to_string),
        allowed_models,
        policy: crate::session_engine::provider_retry::DEFAULT_PROVIDER_RETRY_POLICY,
    }
}

/// The action-model precedence: spec selector, then the configured subagent
/// default, then the session model.
#[test]
fn the_action_model_falls_back_in_the_documented_order() {
    let dir = tempfile::tempdir().unwrap();
    write_catalog(dir.path());
    let config = host_config(dir.path(), Some("testprov/subagent-model"), None);
    let cases = [
        (Some("testprov/action-model"), "action-model"),
        (Some("  action-model  "), "action-model"),
        (None, "subagent-model"),
    ];
    for (reference, expected) in cases {
        let (model, api_key, _headers) = resolve_action_model(&config, reference).unwrap();
        assert_eq!(model.id, expected, "reference {reference:?}");
        assert_eq!(api_key.as_deref(), Some("router-key"));
    }
    // No spec model and no subagent default: the session model.
    let config = host_config(dir.path(), None, None);
    let (model, _, _) = resolve_action_model(&config, None).unwrap();
    assert_eq!(model.id, "session-model");
    // A subagent default equal to the session selector keeps the session model.
    let config = host_config(dir.path(), Some("testprov/session-model"), None);
    let (model, _, _) = resolve_action_model(&config, None).unwrap();
    assert_eq!(model.id, "session-model");
}

#[test]
fn an_unresolvable_action_model_is_refused_loudly() {
    let dir = tempfile::tempdir().unwrap();
    write_catalog(dir.path());
    let config = host_config(dir.path(), None, None);
    let error = resolve_action_model(&config, Some("testprov/missing")).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Requested system-router model \"testprov/missing\" is unavailable, unauthenticated, or expired; selectors use the form \"provider/model-id\" (e.g. \"prime-inference/internal/glm-5.3-fast\")"
    );
}

#[test]
fn the_allowlist_pin_refuses_a_resolved_action_model() {
    let dir = tempfile::tempdir().unwrap();
    write_catalog(dir.path());
    let config = host_config(dir.path(), None, Some(vec!["other/*".to_string()]));
    let error = resolve_action_model(&config, Some("testprov/action-model")).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Requested system-router model \"testprov/action-model\" is blocked by the model allowlist"
    );
    // A selector inside the pin resolves.
    let config = host_config(dir.path(), None, Some(vec!["testprov/*".to_string()]));
    let (model, _, _) = resolve_action_model(&config, Some("testprov/action-model")).unwrap();
    assert_eq!(model.id, "action-model");
}

/// A provider with no credential is not searchable: the resolution reports
/// the model as unavailable, unauthenticated, or expired.
#[test]
fn a_provider_without_a_credential_is_not_searchable() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path()).unwrap();
    std::fs::write(
        dir.path().join("models.json"),
        r#"{
          "providers": {
            "testprov": {
              "baseUrl": "http://localhost:9",
              "api": "openai-completions",
              "models": [ { "id": "session-model", "name": "Session Model", "contextWindow": 128000 } ]
            }
          }
        }"#,
    )
    .unwrap();
    let config = host_config(dir.path(), None, None);
    let error = resolve_action_model(&config, None).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("is unavailable, unauthenticated, or expired"),
        "unexpected error: {error}"
    );
}

/// One registry round trip: the handler parses the spec, resolves the action
/// model and auth, and reaches the segment (whose adapter cannot start).
#[tokio::test]
async fn the_registered_handler_reaches_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    write_catalog(dir.path());
    let mut handlers = HostRequestHandlers::default();
    register_system_router_handlers(
        &mut handlers,
        host_config(dir.path(), None, Some(vec!["testprov/*".to_string()])),
    );
    let handler = handlers
        .get("system_router.run")
        .expect("the system_router.run handler is registered")
        .clone();
    let payload = HostRequestPayload {
        data: json!({
            "type": "system_router.run",
            "goal": "reach the overworld",
            "model": "testprov/action-model",
            "timeoutMs": 150,
            "actions": { "look": { "description": "Look at the screen." } },
            "environment": { "stdio": { "command": ["sh", "-c", "exit 0"] } }
        }),
        cell_source_code: None,
    };
    let error = handler(payload).await.unwrap_err().to_string();
    assert!(
        error.contains("environment adapter init failed")
            || error.contains("environment adapter failed to start"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn the_registered_handler_rejects_a_malformed_spec() {
    let dir = tempfile::tempdir().unwrap();
    write_catalog(dir.path());
    let mut handlers = HostRequestHandlers::default();
    register_system_router_handlers(&mut handlers, host_config(dir.path(), None, None));
    let handler = handlers.get("system_router.run").unwrap().clone();
    let payload = HostRequestPayload {
        data: json!({ "type": "system_router.run" }),
        cell_source_code: None,
    };
    let error = handler(payload).await.unwrap_err().to_string();
    assert!(
        error.contains("system_router.run goal must be a non-empty string"),
        "{error}"
    );
}
