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

While the setting is unset, the feature stays off: the system prompt omits
the `decision-api` skill and every decision request refuses with a message
naming the setting. Setting it to a resolvable reference turns the feature
on for new sessions and kernels (the prompt inclusion and the kernel's
pre-import of the `decision_api` module are gated at session and kernel
start; an existing kernel pre-imports it only after a restart). Stopping an
active control loop before changing the setting avoids half-learned state.

The loop runs System 1 as a spawned decision child
(`rlm.spawn(..., kind="decision")`): the child's model is the setting's
reference, its every message is one decision request served by one model
call, and it lives exactly as long as the loop. Goals flow to it as tagged
`decision_api.goal` agent messages (an older goal never overwrites a newer
one), and its answers return to the loop as tagged `decision_api.decision`
messages.

## Opt-in live smoke checks

These checks make billed provider requests. Run them only when intentionally
verifying live access with your own credentials. Normal Rust and Python tests
use synthetic fixtures and do not establish current production availability or
latency. No live call is required merely to store the setting.

In a Prime Agent session with the setting configured, wait for a Python cell
to run in the persistent REPL (restart the kernel if it predates the
configuration change). Execute one text decision:

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

Clearing the setting hides the skill again on the next session build. If you
explicitly import it in an unconfigured session, `decide` must refuse with
the host's message naming the setting before contacting any provider.

The [skill guide](../skills/decision-api/SKILL.md) covers loop design and
System 2 operation. A provider smoke check verifies a single host call; it
does not prove a live System 2 child, a full control loop, or environment
cleanup. Record those checks separately with the runtime and environment
used.
