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

/// Where decisions go. The default is the production APIs, with the Clef
/// account from `CLOUDFLARE_ACCOUNT_ID` when it is set.
#[derive(Debug, Clone)]
pub(crate) struct DecisionApiEndpoints {
    systemone_url: String,
    cloudflare_api: String,
    cloudflare_account: Option<String>,
}

impl Default for DecisionApiEndpoints {
    fn default() -> Self {
        Self {
            systemone_url: SYSTEMONE_URL.to_string(),
            cloudflare_api: CLOUDFLARE_API.to_string(),
            cloudflare_account: std::env::var(CLOUDFLARE_ACCOUNT_ENV)
                .ok()
                .map(|account| account.trim().to_string())
                .filter(|account| !account.is_empty()),
        }
    }
}

/// Register `decision_api.decide`, routed by `switch` to `endpoints`, against
/// the auth store under `agent_dir`.
pub(crate) fn register_decision_api_handler(
    handlers: &mut HostRequestHandlers,
    switch: DecisionApiSwitch,
    agent_dir: PathBuf,
    endpoints: DecisionApiEndpoints,
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
            let endpoints = endpoints.clone();
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
                let key = stored_key(&agent_dir, provider).await?.ok_or_else(|| {
                    anyhow!(
                        "No {vendor} API key is stored. Ask the user to run /decision-api and \
                         paste their {vendor} API key."
                    )
                })?;
                match provider {
                    DecisionApiProvider::Jev => {
                        send_json(
                            client
                                .post(&endpoints.systemone_url)
                                .bearer_auth(key)
                                .json(&request),
                            vendor,
                        )
                        .await
                    }
                    DecisionApiProvider::Clef => {
                        // The configured account, else the single account the
                        // token can reach (looked up once per key).
                        let cloudflare_api = &endpoints.cloudflare_api;
                        let mut cached = clef_account.lock().await;
                        let account = match (&endpoints.cloudflare_account, cached.as_ref()) {
                            (Some(account), _) => account.clone(),
                            (None, Some((cached_key, account))) if *cached_key == key => {
                                account.clone()
                            }
                            (None, Some(_) | None) => {
                                let accounts = cloudflare_result(
                                    send_json(
                                        client
                                            .get(format!("{cloudflare_api}/accounts"))
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
                            "{cloudflare_api}/accounts/{account}/ai/run/@cf/cloudflare/{CLEF_MODEL}"
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
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::auth::AuthCredential;
    use crate::kernel::shared::{HostHandlerFuture, HostRequestPayload};

    /// The registered `decision_api.decide`, called with a payload's data.
    fn decider(
        switch: &DecisionApiSwitch,
        agent_dir: &Path,
        endpoints: DecisionApiEndpoints,
    ) -> impl Fn(Value) -> HostHandlerFuture {
        let mut handlers = HostRequestHandlers::default();
        register_decision_api_handler(
            &mut handlers,
            switch.clone(),
            agent_dir.to_path_buf(),
            endpoints,
        );
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

    fn store_key(agent_dir: &Path, credential: &str, key: &str) {
        AuthStorage::create(agent_dir).set(
            credential,
            AuthCredential::ApiKey {
                key: key.to_string(),
                prime_team: None,
            },
        );
    }

    /// One request the loopback provider received.
    #[derive(Debug, Clone, PartialEq)]
    struct Seen {
        method: String,
        path: String,
        authorization: String,
        body: Value,
    }

    impl Seen {
        fn new(method: &str, path: &str, key: &str, body: Value) -> Self {
            Self {
                method: method.to_string(),
                path: path.to_string(),
                authorization: format!("Bearer {key}"),
                body,
            }
        }
    }

    /// A loopback provider answering each path in `routes` with its status
    /// and JSON body (404 elsewhere), serving both APIs from one base URL.
    /// Returns the endpoints aimed at it and the requests it received.
    async fn serve(
        routes: Vec<(&'static str, u16, Value)>,
    ) -> (DecisionApiEndpoints, Arc<Mutex<Vec<Seen>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let log = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut raw = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head, body) = loop {
                    let read = socket.read(&mut chunk).await.expect("read request");
                    assert_ne!(read, 0, "the client closed mid-request");
                    raw.extend_from_slice(&chunk[..read]);
                    let text = String::from_utf8_lossy(&raw).to_string();
                    let Some((head, body)) = text.split_once("\r\n\r\n") else {
                        continue;
                    };
                    let length = head
                        .lines()
                        .find_map(|line| {
                            let line = line.to_ascii_lowercase();
                            Some(line.strip_prefix("content-length:")?.trim().parse().ok()?)
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        break (head.to_string(), body.to_string());
                    }
                };
                let mut request_line = head.lines().next().unwrap_or_default().split(' ');
                let method = request_line.next().unwrap_or_default().to_string();
                let path = request_line.next().unwrap_or_default().to_string();
                let authorization = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("authorization")
                            .then(|| value.trim().to_string())
                    })
                    .unwrap_or_default();
                let body = serde_json::from_str(&body).unwrap_or(Value::Null);
                let (status, reply) = routes
                    .iter()
                    .find(|(route, ..)| *route == path)
                    .map_or((404, Value::Null), |(_, status, reply)| {
                        (*status, reply.clone())
                    });
                log.lock().expect("log").push(Seen {
                    method,
                    path,
                    authorization,
                    body,
                });
                let reply = reply.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Status\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
        });
        let endpoints = DecisionApiEndpoints {
            systemone_url: format!("{base}/v1/systemone"),
            cloudflare_api: base,
            cloudflare_account: None,
        };
        (endpoints, seen)
    }

    #[tokio::test]
    async fn jev_posts_the_request_with_the_stored_key_and_the_default_model() {
        let agent_dir = tempfile::tempdir().expect("tempdir");
        store_key(agent_dir.path(), "typesafe", "ts-key");
        let answer =
            json!({ "model": "jev-latest", "answers": { "action": { "choice": "left" } } });
        let (endpoints, seen) = serve(vec![("/v1/systemone", 200, answer.clone())]).await;
        let switch = DecisionApiSwitch::default();
        switch.replace(Some(DecisionApiProvider::Jev));
        let decide = decider(&switch, agent_dir.path(), endpoints);
        let request = json!({ "state": { "x": 1 }, "questions": {}, "images": [] });
        assert_eq!(decide(json!({ "request": request })).await.unwrap(), answer);
        assert_eq!(
            *seen.lock().unwrap(),
            [Seen::new(
                "POST",
                "/v1/systemone",
                "ts-key",
                json!({ "state": { "x": 1 }, "questions": {}, "model": "jev-latest" })
            )]
        );
    }

    #[tokio::test]
    async fn clef_looks_up_the_account_once_per_key_and_unwraps_the_envelope() {
        let agent_dir = tempfile::tempdir().expect("tempdir");
        store_key(agent_dir.path(), "cloudflare", "cf-key");
        let result = json!({ "model": "clef", "answers": { "action": { "choice": "up" } } });
        let run = "/accounts/acct-1/ai/run/@cf/cloudflare/clef";
        let (endpoints, seen) = serve(vec![
            (
                "/accounts",
                200,
                json!({ "success": true, "errors": [], "result": [{ "id": "acct-1" }] }),
            ),
            (
                run,
                200,
                json!({ "success": true, "errors": [], "result": result }),
            ),
        ])
        .await;
        let switch = DecisionApiSwitch::default();
        switch.replace(Some(DecisionApiProvider::Clef));
        let decide = decider(&switch, agent_dir.path(), endpoints);
        let request = json!({ "state": {}, "images": ["data:image/png;base64,AA=="] });
        for _ in 0..2 {
            assert_eq!(decide(json!({ "request": request })).await.unwrap(), result);
        }
        let sent = Seen::new(
            "POST",
            run,
            "cf-key",
            json!({ "state": {}, "images": ["data:image/png;base64,AA=="], "model": "clef" }),
        );
        assert_eq!(
            *seen.lock().unwrap(),
            [
                Seen::new("GET", "/accounts", "cf-key", Value::Null),
                sent.clone(),
                sent
            ]
        );
    }

    #[tokio::test]
    async fn provider_failures_reach_the_kernel_with_their_detail() {
        let agent_dir = tempfile::tempdir().expect("tempdir");
        store_key(agent_dir.path(), "typesafe", "ts-key");
        store_key(agent_dir.path(), "cloudflare", "cf-key");
        let (endpoints, _) = serve(vec![
            ("/v1/systemone", 500, json!({ "error": "overloaded" })),
            (
                "/accounts",
                200,
                json!({ "success": true, "errors": [], "result": [{ "id": "a" }, { "id": "b" }] }),
            ),
        ])
        .await;
        let switch = DecisionApiSwitch::default();
        let decide = decider(&switch, agent_dir.path(), endpoints);
        let error = async |provider| {
            switch.replace(Some(provider));
            decide(json!({ "request": { "state": {} } }))
                .await
                .unwrap_err()
                .to_string()
        };
        assert_eq!(
            error(DecisionApiProvider::Jev).await,
            r#"TypeSafe returned HTTP 500 Internal Server Error: {"error":"overloaded"}"#
        );
        assert_eq!(
            error(DecisionApiProvider::Clef).await,
            "The Cloudflare API token can reach 2 accounts. Set CLOUDFLARE_ACCOUNT_ID to the one \
             that runs Clef."
        );
    }

    #[tokio::test]
    async fn decide_refuses_before_any_network_call() {
        let agent_dir = tempfile::tempdir().expect("tempdir");
        let switch = DecisionApiSwitch::default();
        let call = decider(&switch, agent_dir.path(), DecisionApiEndpoints::default());
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
