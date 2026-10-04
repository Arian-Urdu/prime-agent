# Decision API setup and verification

The Decision API is experimental and off by default. Run `/decision-api` in
Prime Agent to select Jev from TypeSafe (text-only) or Clef from Cloudflare
(supports images). The first selection asks for that provider's key in the
terminal auth panel. `/decision-api off` disables it for the session. To replace
a stored key, use its credential row in `/mcp`.

The selected provider persists with the session's branch, including across
compaction and worker restart. Forks inherit the configuration at their branch
point; switching branches restores that branch's selection. Enabling the API
adds the bundled `decision-api` skill to the system prompt and pre-imports its
Python module when the kernel boots. Changing between on and off restarts the
kernel with its saved namespace; stop an active control loop before switching.

Keys resolve in the host from Prime Agent's auth store, including configured
environment references and `!command` references. The decision request sent by
the Python module contains no API key. Paste keys into the auth panel rather
than a model prompt or Python cell. For referenced environment variables, the
relevant host process must inherit them at startup.

## TypeSafe Jev

Create a key in the [TypeSafe console](https://console.typesafe.ai/), following
the [official quick start](https://docs.typesafe.ai/introduction/quickstart).
Select Jev with `/decision-api` and paste the key when prompted. Its default
model is `jev-latest`; Jev requests cannot contain images.

## Cloudflare Clef

Follow Cloudflare's [Workers AI REST API setup](https://developers.cloudflare.com/workers-ai/get-started/rest-api/):
in the selected account's Workers AI dashboard, open **Use REST API**, create
a Workers AI API token, and copy the account ID. A custom token needs both
**Workers AI Read** and **Workers AI Edit** on the account running Clef.
Select Clef with `/decision-api` and paste this token when prompted.

Set the account explicitly before launching Prime Agent (recommended, including for restricted tokens):

```bash
export CLOUDFLARE_ACCOUNT_ID='<your Cloudflare account ID>'
prime-agent
```

This account ID selects the [Clef model endpoint](https://developers.cloudflare.com/workers-ai/models/clef/)
`/accounts/<account-id>/ai/run/@cf/cloudflare/clef`. It is configuration, not
the token. Setting it bypasses account discovery. If it is unset or blank,
the host calls Cloudflare's [List Accounts endpoint](https://developers.cloudflare.com/api/resources/accounts/methods/list/)
using the token, and proceeds only if exactly one account is returned.
Discovery can fail for restricted tokens; the List Accounts documentation does
not establish which Bearer-token permission enables it. If lookup is denied
or returns no account, set the ID explicitly and verify the token's Workers AI
access. If it returns
multiple accounts, explicitly select the intended one with
`CLOUDFLARE_ACCOUNT_ID`; Prime Agent does not choose the first account.

## Daemon and worker environment

The daemon inherits the environment of the process that starts it. Each
session worker inherits the daemon's environment; a worker captures
`CLOUDFLARE_ACCOUNT_ID` in its provider endpoint configuration when constructing
the session engine. A direct headless engine captures it from its own process.
The account is not a per-session setting saved in the transcript.

Exporting a new value in an attached terminal, reattaching, running `/reload`,
or toggling the Decision API does not update an existing worker's endpoints.
To change the account, finish active work, run `prime-agent shutdown` (this
stops all agents and background services in the current Prime Agent state
root), then relaunch from a shell with the intended environment and resume
the session. If a service manager launches the daemon, update that service's
environment and restart it and its workers. Restarting only a worker under
an existing daemon reuses the daemon's original environment. The provider
selection survives the restart; the account comes from the new startup
environment.

## Opt-in live smoke checks

These checks make billed provider requests. Run them only when intentionally
verifying live access with your own keys. Normal Rust and Python tests use
synthetic fixtures and do not establish current production availability or
latency. No live call is required merely to open the picker or store a key.

In a Prime Agent session, select Jev and wait for a Python cell to run in the
persistent REPL. Execute one text decision:

```python
result = await decision_api.decide(
    {"target_direction": "left"},
    {"left": "Move toward a target on the left", "right": "Move toward a target on the right"},
    goal="Move toward the target",
)
assert result["action"] in {"left", "right"}, result
assert 0 <= result["confidence"] <= 1, result
print(result)
```

Select Clef and repeat the same cell. This exercises the Cloudflare account,
Bearer token, request, response envelope, and module decoding. To check Clef's
image path, additionally pass `images=[png_data_url]` using a real, small PNG,
JPEG, or WebP data URL relevant to the observation. Use up to four images;
avoid degenerate single-pixel images. Keep the request synthetic and record
the returned `model`, action, confidence, and `latency_ms` without credentials.
Do not demand an exact probability or latency from a nondeterministic live
service.

Finally run `/decision-api off`. Confirm the skill disappears from the system
prompt (`/system-prompt`) and the new kernel does not pre-import
`decision_api`. If you explicitly import it, `decide` must return the host's
off-state error before contacting either provider. Re-enable your chosen
provider, resume after a restart, and confirm the selection is retained.

The [skill guide](../skills/decision-api/SKILL.md) covers loop design and
System 2 operation. A provider smoke check verifies a single host call;
it does not prove a live System 2 child, a full control loop, or environment
cleanup. Record those checks separately with the runtime and environment
used.
