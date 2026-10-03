//! The experimental Decision API: off by default and switched per session by
//! `/decision-api`, which picks the decision model the session uses. While a
//! session's switch is off, its prompt omits the decision-api skill, the
//! kernel does not pre-import `decision_api`, and the host refuses
//! `decision_api.decide`. The newest durable `decision_api_status` row in the
//! session context is the session's state, so a rebuilt or resumed session
//! adopts it again.
//!
//! The host side of the skill: the kernel sends a decision request body
//! (`model`, `state`, `questions`, and for Clef `images`) and the host routes
//! it to the session's provider. Keys resolve through [`AuthStorage`] here
//! (literal, env var, or `!command`), so they never enter the kernel process.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use pa_types::session::AgentMessage;
use pa_types::slash_commands::DecisionApiProvider;
pub use pa_types::slash_commands::DECISION_API_STATUS_CUSTOM_TYPE;
use serde_json::Value;

use super::engine::SessionEngine;
use crate::auth::AuthStorage;
use crate::kernel::shared::{host_handler, HostRequestHandlers};

/// The bundled skill the switch gates.
pub const DECISION_API_SKILL_NAME: &str = "decision-api";

const SYSTEMONE_URL: &str = "https://api.typesafe.ai/v1/systemone";
const JEV_DEFAULT_MODEL: &str = "jev-latest";
const CLOUDFLARE_API: &str = "https://api.cloudflare.com/client/v4";
const CLEF_MODEL: &str = "clef";
const CLEF_FLASH_MODEL: &str = "clef-flash";
/// Each Clef model runs at its own `@cf/cloudflare/<model>` endpoint.
const CLEF_MODELS: [&str; 2] = [CLEF_MODEL, CLEF_FLASH_MODEL];
/// Picks the Cloudflare account when the token can reach more than one.
const CLOUDFLARE_ACCOUNT_ENV: &str = "CLOUDFLARE_ACCOUNT_ID";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const ERROR_BODY_CHARS: usize = 500;

/// One session's Decision API switch: the chosen provider, off by default.
/// Clones share the state: the host handler, the kernel's pre-import filter,
/// the prompt selection, and the embedding that reports it to clients read
/// one switch.
#[derive(Debug, Clone, Default)]
pub struct DecisionApiSwitch(Arc<Mutex<Option<DecisionApiProvider>>>);

impl DecisionApiSwitch {
    #[must_use]
    pub fn provider(&self) -> Option<DecisionApiProvider> {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
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

async fn stored_key(
    agent_dir: &Path,
    provider: DecisionApiProvider,
) -> anyhow::Result<Option<String>> {
    let agent_dir = agent_dir.to_path_buf();
    Ok(tokio::task::spawn_blocking(move || {
        AuthStorage::create(&agent_dir).get_api_key(provider.credential())
    })
    .await?)
}

/// The provider the newest status row in `messages` records (`None` when it
/// turned the Decision API off or there is no row).
fn latest_state(messages: &[AgentMessage]) -> Option<DecisionApiProvider> {
    messages
        .iter()
        .rev()
        .find_map(|message| {
            let AgentMessage::Custom(custom) = message else {
                return None;
            };
            if custom.custom_type != DECISION_API_STATUS_CUSTOM_TYPE {
                return None;
            }
            let provider = custom.details.as_ref()?.get("provider")?;
            Some(provider.as_str().and_then(DecisionApiProvider::from_id))
        })
        .flatten()
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
        DecisionApiProvider::Clef | DecisionApiProvider::ClefFlash => {
            "It is vision-capable: decisions can carry up to 4 images."
        }
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

    /// The session's Decision API switch, for embeddings that report it.
    #[must_use]
    pub fn decision_api_switch(&self) -> DecisionApiSwitch {
        self.decision_api.switch.clone()
    }

    /// Whether `/decision-api <provider>` can succeed: the user stored its key.
    pub(crate) async fn decision_api_has_key(
        &self,
        provider: DecisionApiProvider,
    ) -> anyhow::Result<bool> {
        Ok(stored_key(&self.decision_api.agent_dir, provider)
            .await?
            .is_some())
    }

    /// Pick the session's decision model, or turn the Decision API off.
    /// Turning it on or off swaps the system prompt and restarts the kernel
    /// (its namespace revives from the stop's snapshot), so the new kernel
    /// pre-imports `decision_api` only while it is on.
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

    /// Adopt the newest `/decision-api` state in the session context (off
    /// when it records none). Hosts call this after restoring or moving the
    /// session's context, so a rebuilt or resumed session keeps the user's
    /// choice and a fresh one starts off.
    pub async fn sync_decision_api_from_context(&self) {
        let provider = {
            let session = self.session.session_handle().clone();
            let session = session.lock().await;
            latest_state(&session.active_context().messages)
        };
        self.set_decision_api(provider).await;
    }
}

/// Send one provider request and parse its JSON body, surfacing HTTP errors
/// with the start of the body.
async fn send_json(request: reqwest::RequestBuilder, vendor: &str) -> anyhow::Result<Value> {
    let response = request
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("the {vendor} request failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("reading the {vendor} response failed"))?;
    if !status.is_success() {
        let detail: String = body.chars().take(ERROR_BODY_CHARS).collect();
        bail!("{vendor} returned HTTP {status}: {detail}");
    }
    serde_json::from_str(&body).with_context(|| format!("{vendor} returned invalid JSON"))
}

/// Unwrap a Cloudflare API envelope (`success`, `errors`, `result`).
fn cloudflare_result(mut envelope: Value) -> anyhow::Result<Value> {
    if envelope.get("success").and_then(Value::as_bool) == Some(true) {
        if let Some(result) = envelope.get_mut("result") {
            return Ok(result.take());
        }
    }
    let errors = envelope.get("errors").cloned().unwrap_or(Value::Null);
    bail!("Cloudflare returned errors: {errors}")
}

/// Register `decision_api.decide`, routed by `switch`, against the auth store
/// under `agent_dir`.
pub(crate) fn register_decision_api_handler(
    handlers: &mut HostRequestHandlers,
    switch: DecisionApiSwitch,
    agent_dir: PathBuf,
) {
    let client = reqwest::Client::new();
    // The Cloudflare account resolved for a key: one lookup per key, not per decision.
    let clef_account: Arc<tokio::sync::Mutex<Option<(String, String)>>> = Arc::default();
    handlers.register(
        "decision_api.decide",
        host_handler(move |payload| {
            let client = client.clone();
            let switch = switch.clone();
            let agent_dir = agent_dir.clone();
            let clef_account = clef_account.clone();
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
                         the images, or ask the user to run /decision-api and pick Clef or Clef \
                         Flash, which are vision-capable.",
                        provider.model()
                    );
                }
                let default_model = match provider {
                    DecisionApiProvider::Jev => JEV_DEFAULT_MODEL,
                    DecisionApiProvider::Clef => CLEF_MODEL,
                    DecisionApiProvider::ClefFlash => CLEF_FLASH_MODEL,
                };
                let model = match request.get("model") {
                    None | Some(Value::Null) => default_model.to_string(),
                    Some(Value::String(model)) => model.clone(),
                    Some(other) => bail!("The decision model must be a string, not {other}."),
                };
                let is_clef_model = CLEF_MODELS.contains(&model.as_str());
                if (provider == DecisionApiProvider::Jev) == is_clef_model {
                    let served = match provider {
                        DecisionApiProvider::Jev => "Jev models".to_string(),
                        DecisionApiProvider::Clef | DecisionApiProvider::ClefFlash => {
                            CLEF_MODELS.join(" and ")
                        }
                    };
                    bail!(
                        "{} from {vendor} serves {served}, not {model}. Leave the model unset to \
                         use {default_model}, or ask the user to run /decision-api and pick \
                         another model.",
                        provider.model()
                    );
                }
                request.insert("model".to_string(), Value::from(model.as_str()));
                let key = stored_key(&agent_dir, provider).await?.ok_or_else(|| {
                    anyhow!(
                        "No {vendor} API key is stored. Ask the user to run /decision-api and \
                         paste their {vendor} API key."
                    )
                })?;
                match provider {
                    DecisionApiProvider::Jev => {
                        send_json(
                            client.post(SYSTEMONE_URL).bearer_auth(key).json(&request),
                            vendor,
                        )
                        .await
                    }
                    DecisionApiProvider::Clef | DecisionApiProvider::ClefFlash => {
                        // `CLOUDFLARE_ACCOUNT_ID`, else the single account the
                        // token can reach (looked up once per key).
                        let mut cached = clef_account.lock().await;
                        let account = match (std::env::var(CLOUDFLARE_ACCOUNT_ENV), cached.as_ref())
                        {
                            (Ok(account), _) if !account.trim().is_empty() => {
                                account.trim().to_string()
                            }
                            (_, Some((cached_key, account))) if *cached_key == key => {
                                account.clone()
                            }
                            (Ok(_) | Err(_), Some(_) | None) => {
                                let accounts = cloudflare_result(
                                    send_json(
                                        client
                                            .get(format!("{CLOUDFLARE_API}/accounts"))
                                            .bearer_auth(&key),
                                        vendor,
                                    )
                                    .await?,
                                )?;
                                let ids: Vec<&str> = accounts
                                    .as_array()
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|account| account.get("id")?.as_str())
                                    .collect();
                                let account = match ids.as_slice() {
                                    [id] => (*id).to_string(),
                                    [] => bail!(
                                        "The Cloudflare API token cannot read any account. Give \
                                         it Account > Workers AI access, or set \
                                         {CLOUDFLARE_ACCOUNT_ENV}."
                                    ),
                                    [_, _, ..] => bail!(
                                        "The Cloudflare API token can reach {} accounts. Set \
                                         {CLOUDFLARE_ACCOUNT_ENV} to the one that runs Clef.",
                                        ids.len()
                                    ),
                                };
                                *cached = Some((key.clone(), account.clone()));
                                account
                            }
                        };
                        drop(cached);
                        let url = format!(
                            "{CLOUDFLARE_API}/accounts/{account}/ai/run/@cf/cloudflare/{model}"
                        );
                        cloudflare_result(
                            send_json(client.post(url).bearer_auth(key).json(&request), vendor)
                                .await?,
                        )
                    }
                }
            })
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::shared::HostRequestPayload;

    #[tokio::test]
    async fn decide_refuses_before_any_network_call() {
        let agent_dir = tempfile::tempdir().expect("tempdir");
        let switch = DecisionApiSwitch::default();
        let mut handlers = HostRequestHandlers::default();
        register_decision_api_handler(
            &mut handlers,
            switch.clone(),
            agent_dir.path().to_path_buf(),
        );
        let decide = handlers
            .get("decision_api.decide")
            .expect("registered")
            .clone();
        let call = |data| {
            decide(HostRequestPayload {
                data,
                cell_source_code: None,
            })
        };
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
             the images, or ask the user to run /decision-api and pick Clef or Clef Flash, \
             which are vision-capable."
        );
        let with_model =
            |model: &str| serde_json::json!({ "request": { "state": {}, "model": model } });
        assert_eq!(
            error(with_model("clef-flash")).await,
            "Jev from TypeSafe serves Jev models, not clef-flash. Leave the model unset to use \
             jev-latest, or ask the user to run /decision-api and pick another model."
        );
        assert_eq!(
            error(request).await,
            "No TypeSafe API key is stored. Ask the user to run /decision-api and paste their \
             TypeSafe API key."
        );
        switch.replace(Some(DecisionApiProvider::ClefFlash));
        assert_eq!(
            error(with_model("jev-latest")).await,
            "Clef Flash from Cloudflare serves clef and clef-flash, not jev-latest. Leave the \
             model unset to use clef-flash, or ask the user to run /decision-api and pick \
             another model."
        );
        assert_eq!(
            error(with_image).await,
            "No Cloudflare API key is stored. Ask the user to run /decision-api and paste their \
             Cloudflare API key."
        );
    }

    #[test]
    fn a_cloudflare_envelope_yields_its_result_or_its_errors() {
        let ok =
            serde_json::json!({ "success": true, "errors": [], "result": { "model": "clef" } });
        assert_eq!(
            cloudflare_result(ok).unwrap(),
            serde_json::json!({ "model": "clef" })
        );
        let failed = serde_json::json!({ "success": false, "errors": [{ "message": "bad" }] });
        assert_eq!(
            cloudflare_result(failed).unwrap_err().to_string(),
            r#"Cloudflare returned errors: [{"message":"bad"}]"#
        );
    }

    #[test]
    fn the_newest_status_row_is_the_state() {
        let row = |provider: Option<DecisionApiProvider>| {
            AgentMessage::Custom(pa_types::session::CustomMessage {
                custom_type: DECISION_API_STATUS_CUSTOM_TYPE.to_string(),
                content: pa_types::ai::UserContent::Text(status_note(provider)),
                display: false,
                details: Some(
                    serde_json::json!({ "provider": provider.map(DecisionApiProvider::id) }),
                ),
                timestamp: 0,
                rest: serde_json::Map::default(),
            })
        };
        let clef = Some(DecisionApiProvider::Clef);
        assert_eq!(latest_state(&[]), None);
        assert_eq!(latest_state(&[row(clef)]), clef);
        assert_eq!(latest_state(&[row(clef), row(None)]), None);
    }
}
