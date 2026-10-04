//! The experimental Decision API: off by default and switched per session by
//! `/decision-api`, which picks the decision model the session uses. While a
//! session's switch is off, its prompt omits the decision-api skill, the
//! kernel does not pre-import `decision_api`, and the host refuses
//! `decision_api.decide`. The newest durable `decision_api_status` row in the
//! selected session branch is the session's state, so a rebuilt or resumed session
//! adopts it again.
//!
//! The host side of the skill: the kernel sends a decision request body
//! (`model`, `state`, `questions`, and for Clef `images`) and the host routes
//! it to the session's provider. Keys resolve through [`AuthStorage`] here
//! (literal, env var, or `!command`), so they never enter the kernel process.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{anyhow, bail};
use pa_types::slash_commands::DecisionApiProvider;
use serde_json::Value;

use super::engine::SessionEngine;
use crate::auth::AuthStorage;
use crate::kernel::shared::{host_handler, HostRequestHandlers};

/// The bundled skill the switch gates.
pub const DECISION_API_SKILL_NAME: &str = "decision-api";

const JEV_DEFAULT_MODEL: &str = "jev-latest";
const CLEF_MODEL: &str = "clef";
/// One session's Decision API switch: the chosen provider, off by default.
/// Clones share the state: the host handler, the kernel's pre-import filter,
/// and the prompt selection read one switch.
#[derive(Debug, Clone, Default)]
pub(crate) struct DecisionApiSwitch(Arc<Mutex<Option<DecisionApiProvider>>>);

impl DecisionApiSwitch {
    pub(crate) fn new(provider: Option<DecisionApiProvider>) -> Self {
        Self(Arc::new(Mutex::new(provider)))
    }

    #[must_use]
    pub(crate) fn provider(&self) -> Option<DecisionApiProvider> {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[must_use]
    pub(crate) fn is_enabled(&self) -> bool {
        self.provider().is_some()
    }

    /// Returns the previous provider.
    fn replace(&self, provider: Option<DecisionApiProvider>) -> Option<DecisionApiProvider> {
        std::mem::replace(
            &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner),
            provider,
        )
    }
}

/// A session's Decision API wiring: the switch plus the two system prompts
/// it selects between.
pub(crate) struct DecisionApiSession {
    pub(crate) switch: DecisionApiSwitch,
    pub(crate) agent_dir: PathBuf,
    pub(crate) prompt_on: String,
    pub(crate) prompt_off: String,
}

/// The status row's content: what the model learns about the switch.
pub(crate) fn status_note(provider: Option<DecisionApiProvider>) -> String {
    let Some(provider) = provider else {
        return "[decision-api: off] The user turned the Decision API off for this session: the \
                decision-api skill and the `decision_api` module are no longer available."
            .to_string();
    };
    let images = match provider {
        DecisionApiProvider::Jev => "It is text-only: decisions cannot carry images.",
        DecisionApiProvider::Clef => "It is vision-capable: decisions can carry up to 4 images.",
    };
    format!(
        "[decision-api: {id}] The user enabled the Decision API for this session with {label}: \
         the decision-api skill is in your skills list and `decision_api` is pre-imported in the \
         Python kernel. {images}",
        id = provider.id(),
        label = provider.label(),
    )
}

impl SessionEngine {
    /// The system prompt the session runs with now.
    #[must_use]
    pub fn system_prompt(&self) -> &str {
        if self.decision_api.switch.is_enabled() {
            &self.decision_api.prompt_on
        } else {
            &self.decision_api.prompt_off
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn decision_api_switch(&self) -> DecisionApiSwitch {
        self.decision_api.switch.clone()
    }

    /// Whether `/decision-api <provider>` can succeed: the user stored its key.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn decision_api_has_key(
        &self,
        provider: DecisionApiProvider,
    ) -> anyhow::Result<bool> {
        Ok(
            AuthStorage::resolve_api_key(&self.decision_api.agent_dir, provider.credential())
                .await?
                .is_some(),
        )
    }

    /// Pick the session's decision model, or turn the Decision API off.
    /// Turning it on or off swaps the system prompt and restarts the kernel
    /// (its namespace revives from the stop's snapshot), so the new kernel
    /// pre-imports `decision_api` only while it is on.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn set_decision_api(&self, provider: Option<DecisionApiProvider>) {
        let previous = self.decision_api.switch.replace(provider);
        if previous.is_some() == provider.is_some() {
            return;
        }
        self.session
            .agent()
            .set_system_prompt(self.system_prompt())
            .await;
        self.stop_kernel_snapshot().await;
        self.provisioner.prewarm();
    }

    /// Adopt the selected branch's newest durable `/decision-api` state,
    /// including configuration before its compacted transcript. Hosts call
    /// this after moving a session between branches.
    #[tracing::instrument(skip(self))]
    pub async fn sync_decision_api_from_session(&self) {
        let provider = {
            let session = self.session.session_handle().clone();
            let session = session.lock().await;
            session.decision_api_provider()
        };
        self.set_decision_api(provider).await;
    }
}

/// Register `decision_api.decide`, gated by `switch`, against
/// the auth store under `agent_dir`.
pub(crate) fn register_decision_api_handler(
    handlers: &mut HostRequestHandlers,
    switch: DecisionApiSwitch,
    agent_dir: PathBuf,
) {
    let client = pa_ai::DecisionApiClient::default();
    handlers.register(
        "decision_api.decide",
        host_handler(move |payload| {
            let client = client.clone();
            let switch = switch.clone();
            let agent_dir = agent_dir.clone();
            Box::pin(async move {
                let Some(provider) = switch.provider() else {
                    bail!(
                        "The Decision API is off for this session. Ask the user to run \
                         /decision-api."
                    );
                };
                let Some(Value::Object(mut request)) = payload.data.get("request").cloned() else {
                    bail!("decision_api.decide needs a request object");
                };
                let vendor = provider.vendor();
                let no_images = request.get("images").is_none_or(|images| {
                    images.is_null() || images.as_array().is_some_and(Vec::is_empty)
                });
                if no_images {
                    request.remove("images");
                } else if provider == DecisionApiProvider::Jev {
                    bail!(
                        "{} from {vendor} is text-only, so decisions cannot carry images. Drop \
                         the images, or ask the user to run /decision-api and pick Clef, which \
                         is vision-capable.",
                        provider.model()
                    );
                }
                let default_model = match provider {
                    DecisionApiProvider::Jev => JEV_DEFAULT_MODEL,
                    DecisionApiProvider::Clef => CLEF_MODEL,
                };
                let model = match request.get("model") {
                    None | Some(Value::Null) => default_model.to_string(),
                    Some(Value::String(model)) => model.clone(),
                    Some(other) => bail!("The decision model must be a string, not {other}."),
                };
                if (provider == DecisionApiProvider::Clef) != (model == CLEF_MODEL) {
                    let served = match provider {
                        DecisionApiProvider::Jev => "Jev models",
                        DecisionApiProvider::Clef => CLEF_MODEL,
                    };
                    bail!(
                        "{} from {vendor} serves {served}, not {model}. Leave the model unset to \
                         use {default_model}, or ask the user to run /decision-api and pick \
                         another model.",
                        provider.model()
                    );
                }
                request.insert("model".to_string(), Value::from(model.as_str()));
                let key = AuthStorage::resolve_api_key(&agent_dir, provider.credential())
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "No {vendor} API key is stored. Ask the user to run /decision-api and \
                         paste their {vendor} API key."
                        )
                    })?;
                client.decide(provider, &key, &Value::Object(request)).await
            })
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthCredential;
    use crate::kernel::shared::{HostHandlerFuture, HostRequestPayload};
    use std::path::Path;

    /// The registered `decision_api.decide`, called with a payload's data.
    fn decider(
        switch: &DecisionApiSwitch,
        agent_dir: &Path,
    ) -> impl Fn(Value) -> HostHandlerFuture {
        let mut handlers = HostRequestHandlers::default();
        register_decision_api_handler(&mut handlers, switch.clone(), agent_dir.to_path_buf());
        let decide = handlers
            .get("decision_api.decide")
            .expect("registered")
            .clone();
        move |data| {
            decide(HostRequestPayload {
                data,
                cell_source_code: None,
            })
        }
    }

    #[tokio::test]
    async fn an_empty_stored_key_is_not_ready_for_the_host() {
        let agent_dir = tempfile::tempdir().unwrap();
        store_key(agent_dir.path(), "typesafe", "  ");
        assert_eq!(
            AuthStorage::resolve_api_key(agent_dir.path(), "typesafe")
                .await
                .unwrap(),
            None
        );
        store_key(agent_dir.path(), "typesafe", "fixture-key");
        assert_eq!(
            AuthStorage::resolve_api_key(agent_dir.path(), "typesafe")
                .await
                .unwrap(),
            Some("fixture-key".to_string())
        );
    }

    fn store_key(agent_dir: &Path, credential: &str, key: &str) {
        AuthStorage::create(agent_dir).set(
            credential,
            AuthCredential::ApiKey {
                key: key.to_string(),
                prime_team: None,
            },
        );
    }

    #[tokio::test]
    async fn decide_refuses_before_any_network_call() {
        let agent_dir = tempfile::tempdir().expect("tempdir");
        let switch = DecisionApiSwitch::default();
        let call = decider(&switch, agent_dir.path());
        let error = |data| async { call(data).await.unwrap_err().to_string() };
        let request = serde_json::json!({ "request": { "state": {}, "images": [] } });
        let with_image = serde_json::json!({
            "request": { "state": {}, "images": ["data:image/png;base64,AA=="] }
        });
        assert_eq!(
            error(request.clone()).await,
            "The Decision API is off for this session. Ask the user to run /decision-api."
        );
        switch.replace(Some(DecisionApiProvider::Jev));
        assert_eq!(
            error(serde_json::json!({})).await,
            "decision_api.decide needs a request object"
        );
        assert_eq!(
            error(with_image.clone()).await,
            "Jev from TypeSafe is text-only, so decisions cannot carry images. Drop \
             the images, or ask the user to run /decision-api and pick Clef, which is \
             vision-capable."
        );
        let with_model =
            |model: &str| serde_json::json!({ "request": { "state": {}, "model": model } });
        assert_eq!(
            error(with_model("clef")).await,
            "Jev from TypeSafe serves Jev models, not clef. Leave the model unset to use \
             jev-latest, or ask the user to run /decision-api and pick another model."
        );
        assert_eq!(
            error(request).await,
            "No TypeSafe API key is stored. Ask the user to run /decision-api and paste their \
             TypeSafe API key."
        );
        switch.replace(Some(DecisionApiProvider::Clef));
        assert_eq!(
            error(with_model("jev-latest")).await,
            "Clef from Cloudflare serves clef, not jev-latest. Leave the model unset to use \
             clef, or ask the user to run /decision-api and pick another model."
        );
        assert_eq!(
            error(with_image).await,
            "No Cloudflare API key is stored. Ask the user to run /decision-api and paste their \
             Cloudflare API key."
        );
    }

    #[test]
    fn catch_startup_and_artifacts_follow_the_environment_contract() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/catch");
        let output = std::process::Command::new("python3")
            .args(["-m", "unittest", "discover", "-s"])
            .arg(&root)
            .args(["-p", "test_*.py", "-v"])
            .output()
            .expect("python3 runs the Catch environment tests");
        assert!(
            output.status.success(),
            "Catch tests failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// The skill's `Loop` and `decide` (image strings, step control, System 2's
    /// action gate) run as the package's own unittest.
    #[test]
    fn the_decision_api_python_loop_follows_its_contract() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../skills/decision-api");
        let output = std::process::Command::new("python3")
            .args(["-m", "unittest", "discover", "-s"])
            .arg(root.join("tests"))
            .arg("-v")
            .env("PYTHONPATH", root.join("src"))
            .output()
            .expect("python3 runs the decision-api loop tests");
        assert!(
            output.status.success(),
            "decision-api loop tests failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
