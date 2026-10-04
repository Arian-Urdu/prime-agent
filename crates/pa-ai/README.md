# pa-ai

Provider APIs and model registry.

## Scope
Provider trait + per-provider streaming clients (anthropic, openai-completions/responses, google, bedrock, mistral, azure, prime-inference), model registry/resolution, usage accounting, stream-failure retry, provider-error shapes (per-SDK user-facing texts, diagnostic error names, connection-error profiles), bedrock transport selection (h2c prior-knowledge HTTP/2 cleartext, h2-preferred TLS ALPN, the http1 AWS_BEDROCK_FORCE_HTTP1/proxy mode), overflow handling, JSON repair parsing, faux provider for tests.

Decision API transport: authenticated TypeSafe System One requests and
Cloudflare Clef requests, Cloudflare account discovery/cache, response
envelopes, HTTP timeout and error decoding. Session activation, auth-store
resolution, prompt selection, and kernel policy remain in pa-core.

## Non-goals
No agent loop, no tool execution, no session state, no UI. Receives/returns `pa-types` messages.

## Public API
`Provider` trait, `ProviderRegistry`, model lookup/resolution, faux provider. Per-provider internals are `pub(crate)`.

`DecisionApiClient::{default, decide}` is the minimal Decision API transport
facade. The caller supplies the resolved credential and provider from
pa-types; construction captures `CLOUDFLARE_ACCOUNT_ID` with production endpoints.
The client does not load credentials or mutate session state. Transport
endpoints, helpers, and provider envelope/account logic are private. Definition-level
tracing skips credentials and request payloads.

## Depends on
pa-types (one-way).

Uses the existing workspace `tracing` dependency for Decision API transport
instrumentation. This adds no workspace dependency edge; the transport uses
the HTTP/JSON dependencies already owned by this crate.
