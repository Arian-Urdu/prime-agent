# Decision API setup and verification

The Decision API is experimental and off by default. It runs the
`decision-api` skill's System 1 / System 2 control loop, with System 1
serving one fast decision per observation.

Configure the decision model in settings.json:

```json
{ "decisionApi": { "systemOneModel": "prime-inference/clef" } }
```

The value is a registry model reference (`"provider/model-id"` or a bare id)
resolved exactly like `imageModel`: it must match an available, authenticated
model in the model registry. Requests then ride the model's normal provider
transport (the Anthropic / OpenAI-compatible request paths) with the
credentials the registry resolves for it — the Decision API has no transport
or credential store of its own. A vision-capable model there also serves
decision images.

Run `/decision-api on` to enable the feature for a session and
`/decision-api off` to disable it. Turning it on requires the configured
reference to resolve; the failure names the setting and the fix. The switch
persists with the session's branch, including across compaction and worker
restart. Forks inherit the configuration at their branch point; switching
branches restores that branch's selection. Enabling the API adds the bundled
`decision-api` skill to the system prompt and pre-imports its Python module
when the kernel boots. Changing between on and off restarts the kernel with
its saved namespace; stop an active control loop before switching.

## Opt-in live smoke checks

These checks make billed provider requests. Run them only when intentionally
verifying live access with your own credentials. Normal Rust and Python tests
use synthetic fixtures and do not establish current production availability or
latency. No live call is required merely to store the setting or switch the
session on.

In a Prime Agent session, configure a decision model, run `/decision-api on`,
and wait for a Python cell to run in the persistent REPL. Execute one text
decision:

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

With a vision-capable model, additionally pass
`images=[png_data_url]` using a real, small PNG, JPEG, or WebP data URL
relevant to the observation. Use up to four images; avoid degenerate
single-pixel images. Keep the request synthetic and record the returned
`model`, action, confidence, and `latency_ms` without credentials. Do not
demand an exact probability or latency from a nondeterministic live service.

Finally run `/decision-api off`. Confirm the skill disappears from the system
prompt (`/system-prompt`) and the new kernel does not pre-import
`decision_api`. If you explicitly import it, `decide` must return the host's
off-state error before contacting any provider. Re-enable the switch, resume
after a restart, and confirm the state is retained.

The [skill guide](../skills/decision-api/SKILL.md) covers loop design and
System 2 operation. A provider smoke check verifies a single host call; it
does not prove a live System 2 child, a full control loop, or environment
cleanup. Record those checks separately with the runtime and environment
used.
