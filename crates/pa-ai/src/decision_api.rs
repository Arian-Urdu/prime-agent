//! Non-streaming Decision API provider transport. Session authorization stays in pa-core.
use anyhow::{bail, Context};
use pa_types::slash_commands::DecisionApiProvider;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

const SYSTEMONE_URL: &str = "https://api.typesafe.ai/v1/systemone";
const CLOUDFLARE_API: &str = "https://api.cloudflare.com/client/v4";
const CLEF_MODEL: &str = "clef";
/// Picks the Cloudflare account when the token can reach more than one.
const CLOUDFLARE_ACCOUNT_ENV: &str = "CLOUDFLARE_ACCOUNT_ID";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const ERROR_BODY_CHARS: usize = 500;
const ACCOUNT_DISCOVERY_CONTEXT: &str =
    "Cloudflare account discovery failed. Set CLOUDFLARE_ACCOUNT_ID from the Workers AI dashboard and verify the token's Workers AI access.";

/// Send one provider request and parse its JSON body, surfacing HTTP errors
/// with the start of the body.
#[tracing::instrument(skip(request))]
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
struct DecisionApiEndpoints {
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

/// Provider transport with a per-client Cloudflare account cache. Keys are supplied
/// by the host for each call; the client never reads session or auth storage.
#[derive(Clone)]
pub struct DecisionApiClient {
    client: reqwest::Client,
    endpoints: DecisionApiEndpoints,
    clef_account: Arc<tokio::sync::Mutex<Option<(String, String)>>>,
}

impl Default for DecisionApiClient {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoints: DecisionApiEndpoints::default(),
            clef_account: Arc::default(),
        }
    }
}

impl DecisionApiClient {
    /// Send a validated request to the chosen provider. Cloudflare replies are
    /// unwrapped; HTTP, JSON, and account-discovery failures reach the caller.
    ///
    /// # Errors
    /// Returns an error on transport failures, rejected HTTP responses, invalid
    /// JSON/envelopes, or ambiguous or unavailable Cloudflare accounts.
    #[tracing::instrument(skip(self, key, request))]
    pub async fn decide(
        &self,
        provider: DecisionApiProvider,
        key: &str,
        request: &Value,
    ) -> anyhow::Result<Value> {
        let vendor = provider.vendor();
        match provider {
            DecisionApiProvider::Jev => {
                send_json(
                    self.client
                        .post(&self.endpoints.systemone_url)
                        .bearer_auth(key)
                        .json(request),
                    vendor,
                )
                .await
            }
            DecisionApiProvider::Clef => {
                // The configured account, else the single account the
                // token can reach (looked up once per key).
                let cloudflare_api = &self.endpoints.cloudflare_api;
                let mut cached = self.clef_account.lock().await;
                let account = match (&self.endpoints.cloudflare_account, cached.as_ref()) {
                    (Some(account), _) => account.clone(),
                    (None, Some((cached_key, account))) if cached_key == key => account.clone(),
                    (None, Some(_) | None) => {
                        let accounts = cloudflare_result(
                            send_json(
                                self.client
                                    .get(format!("{cloudflare_api}/accounts"))
                                    .bearer_auth(key),
                                vendor,
                            )
                            .await
                            .context(ACCOUNT_DISCOVERY_CONTEXT)?,
                        )
                        .context(ACCOUNT_DISCOVERY_CONTEXT)?;
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
                         it Workers AI Read and Edit access, and set \
                         {CLOUDFLARE_ACCOUNT_ENV} from the Workers AI dashboard."
                            ),
                            [_, _, ..] => bail!(
                                "The Cloudflare API token can reach {} accounts. Set \
                         {CLOUDFLARE_ACCOUNT_ENV} to the one that runs Clef.",
                                ids.len()
                            ),
                        };
                        *cached = Some((key.to_string(), account.clone()));
                        account
                    }
                };
                drop(cached);
                let url = format!(
                    "{cloudflare_api}/accounts/{account}/ai/run/@cf/cloudflare/{CLEF_MODEL}"
                );
                cloudflare_result(
                    send_json(self.client.post(url).bearer_auth(key).json(request), vendor).await?,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
                            line.strip_prefix("content-length:")?.trim().parse().ok()
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
    async fn jev_posts_the_validated_request_with_the_supplied_key() {
        let answer =
            json!({ "model": "jev-latest", "answers": { "action": { "choice": "left" } } });
        let (endpoints, seen) = serve(vec![("/v1/systemone", 200, answer.clone())]).await;
        let client = DecisionApiClient {
            client: reqwest::Client::new(),
            endpoints,
            clef_account: Arc::default(),
        };
        let request = json!({ "state": { "x": 1 }, "questions": {}, "model": "jev-latest" });
        assert_eq!(
            client
                .decide(DecisionApiProvider::Jev, "ts-key", &request)
                .await
                .unwrap(),
            answer
        );
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
        let client = DecisionApiClient {
            client: reqwest::Client::new(),
            endpoints,
            clef_account: Arc::default(),
        };
        let request =
            json!({ "state": {}, "images": ["data:image/png;base64,AA=="], "model": "clef" });
        for _ in 0..2 {
            assert_eq!(
                client
                    .decide(DecisionApiProvider::Clef, "cf-key", &request)
                    .await
                    .unwrap(),
                result
            );
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
    async fn provider_failures_reach_the_caller_with_their_detail() {
        let (endpoints, _) = serve(vec![
            ("/v1/systemone", 500, json!({ "error": "overloaded" })),
            (
                "/accounts",
                200,
                json!({ "success": true, "errors": [], "result": [{ "id": "a" }, { "id": "b" }] }),
            ),
        ])
        .await;
        let client = DecisionApiClient {
            client: reqwest::Client::new(),
            endpoints,
            clef_account: Arc::default(),
        };
        let error = async |provider| {
            client
                .decide(provider, "test-key", &json!({ "state": {} }))
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
    async fn denied_account_discovery_keeps_the_http_error_and_explains_explicit_configuration() {
        let (endpoints, seen) = serve(vec![(
            "/accounts",
            403,
            json!({ "errors": [{ "message": "forbidden" }] }),
        )])
        .await;
        let client = DecisionApiClient {
            client: reqwest::Client::new(),
            endpoints,
            clef_account: Arc::default(),
        };
        let error = client
            .decide(
                DecisionApiProvider::Clef,
                "fixture-token",
                &json!({"model":"clef"}),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Set CLOUDFLARE_ACCOUNT_ID"));
        assert!(error
            .root_cause()
            .to_string()
            .contains("HTTP 403 Forbidden"));
        assert!(error.root_cause().to_string().contains("forbidden"));
        assert_eq!(
            *seen.lock().unwrap(),
            [Seen::new("GET", "/accounts", "fixture-token", Value::Null)]
        );
    }

    #[tokio::test]
    async fn explicit_account_bypasses_discovery_even_for_a_restricted_token() {
        let run = "/accounts/configured/ai/run/@cf/cloudflare/clef";
        let answer = json!({"model":"clef", "answers":{}});
        let (mut endpoints, seen) =
            serve(vec![(run, 200, json!({"success":true, "result":answer}))]).await;
        endpoints.cloudflare_account = Some("configured".to_string());
        let client = DecisionApiClient {
            client: reqwest::Client::new(),
            endpoints,
            clef_account: Arc::default(),
        };
        let request = json!({"model":"clef", "state":{}});
        assert_eq!(
            client
                .decide(DecisionApiProvider::Clef, "restricted-token", &request)
                .await
                .unwrap(),
            answer
        );
        assert_eq!(
            *seen.lock().unwrap(),
            [Seen::new("POST", run, "restricted-token", request)]
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
}
