---
name: decision-api
description: Experimental System 1 / System 2 loop for real-time, low-latency tasks. System 1 is the model named by the decisionApi.systemOneModel setting, choosing every action from observations; the optional System 2 is a subagent for slower, longer-horizon goals. You design, measure, and optimize the whole loop. Requires /decision-api.
---

# Decision API (System 1 / System 2)

Use this for real-time control tasks where every step is one choice from an
action set (a game, a device, a UI) and a full agent turn per step is too slow.

- **System 1** is the decision model the user configured: the
  `decisionApi.systemOneModel` setting names it (a registry model reference
  like `prime-inference/clef`), and `/decision-api on` switches the session
  on. Each observation becomes one call that returns the chosen action and
  its confidence. It is an API call, not a subagent: it knows only what one
  request carries (`state`, `images` with a vision-capable model,
  `instructions`, the action descriptions), so that is how you tune it. The
  session's status note says which model is active;
  `loop.status()["system1_model"]` reports it too.
- **System 2** (optional, on by default) is one subagent (`rlm.spawn`, your
  model unless set) for longer-horizon decisions. It gets the newest
  observation, System 1's recent actions, and the objective, and writes a new
  goal when System 1 is stuck or needs guidance or a goal. It never gives
  direct actions, and the loop never waits for it.

## Your role: optimize the whole loop

You own the loop's design and performance:
- what System 1 sees (`state`, and `images` with a vision-capable model);
- how its question reads (`instructions` and the action descriptions);
- its `history_size` and `tick`;
- whether System 2 runs at all, and how.

Writing the task's strategy into System 1's instructions and action
descriptions is loop design, not acting as System 2.

Diagnose from the outcome before changing anything. The task's own result
(score, success rate) is the measure; `loop.history` confidences and
`loop.goal_updates` explain it.
- **System 1 problems:** low confidence, actions flipping between options,
  or failures while the goal stays the same. Give `state` the information
  the decision needs, sharpen `instructions`, or make the action descriptions
  easier to tell apart.
- **System 2 problems:** goals that are wrong, churn, or lag the situation.
  Fix its `prompt`, `model`, `interval`, or `message`, or the `objective`.
- **No System 2 needed:** if the task has no longer-horizon decisions (pure
  reflexes, or one fixed strategy), turn System 2 off (`loop.system2 = None`).
  Otherwise it only adds cost and goal churn.

Change one thing at a time and compare runs under the same conditions (same
seed or scenario, same duration), keeping what measurably helps. Make changes
through the loop's settings or the task's own code, never by editing this
skill's package. Do the analysis yourself rather than delegating it to extra
subagents.

You never steer the live loop by hand:
- Do not choose actions or goals from observations yourself.
- Do not send goal replies yourself or message System 2. `loop.goal` is
  read-only for this reason.
- If System 2 sends an ordinary chat message, do not act on or forward it; fix its prompt
  instead.

System 2 never sends actions: by default the loop ignores any action it
writes (see `loop.errors`).

## Setup

The Decision API is off by default and switched per session: the user sets
`decisionApi.systemOneModel` in settings.json to a registry model reference
(`"provider/model-id"` or a bare id), then runs `/decision-api on`;
`/decision-api off` turns it off. The host resolves the model and its
credentials through the model registry — the same path any other model call
takes — and makes every provider call. If a call reports that the Decision
API is off or the setting is not configured, ask the user to fix the setting
and run `/decision-api`. Do not ask for keys yourself.

## Usage

Write `observe` and `act` for the environment (sync or async), start the loop
in the background, then watch and adjust it from later cells:

```python
actions = {
    "left": "Move left when the target is to the left",
    "right": "Move right when the target is to the right",
    "wait": "Do nothing when already aligned",
}
loop = decision_api.Loop(observe, act, actions, objective="Keep the paddle under the ball", tick=0.3)
loop.start()

status = await loop.wait(timeout=20)   # returns early if the loop ends
print(status, loop.history[-5:], loop.goal_updates[-3:], loop.errors[-3:])
```

Every attribute is read again each step, so assignments take effect live:

```python
loop.actions["fire"] = "Fire when an enemy is straight ahead"   # or a function (observation) -> dict
loop.instructions = "..."; loop.tick = 0.2
loop.state = lambda observation, goal, history: {...}           # exactly what System 1 sees
loop.images = lambda observation: [png_data_url]                # vision models: up to 4 images per step
loop.on_error = "skip"            # "stop" (default), "skip", or (error, observation) -> action
loop.on_step = lambda record, observation: ...                  # log or render; return "stop" to end
loop.objective = "..."            # System 2 sees it in its next message
loop.system2.prompt = "..."       # schedules replacement; keep the tagged parent-message reply format
loop.system2.model = "..."        # respawns too
loop.system2.interval = 2.0       # send every 2 s instead of once System 2 answered
loop.system2.timeout = 30.0       # bound spawn/message transport; failures retry in the background
loop.system2.can_act = True       # only if the user asks: lets System 2 override one action (respawns)
loop.system2 = None               # System 1 only (goal falls back to the objective)
loop.system2 = decision_api.System2(prompt=...)   # back on, fresh System 2
loop.pause(); loop.resume()
final = await loop.stop()         # also removes System 2
```

Construction accepts the same settings as keywords (`system2=None`,
`max_steps=500`, `history_size=10`, ...); `help(decision_api.Loop)` lists all
of them. `await loop.run()` runs to completion in one cell instead.

System 2 transport keeps one newest pending observation, so a slow child
does not create an unbounded message queue or block System 1. Startup and
delivery failures appear in `loop.errors` and retry with backoff capped at
five seconds. Changing the child settings schedules retirement and replacement;
an in-flight operation is bounded by `timeout`. Stopping cancels transport and
attempts child cleanup with a five-second bound. System 2 sends JSON parent
messages with `type="decision_api.goal"`, `seq`, and optional `goal`;
these route into the loop without starting a parent turn. Stale or duplicate replies cannot update the
current goal. Python 3.11 or newer is required.

Stay in your turn while a loop runs: poll with `loop.wait(timeout=...)`, read
`history`, `goal_updates`, and `errors`, adjust, and stop it when done. The
loop keeps running between cells, but nothing wakes you once your turn ends.

For a single System 1 decision, `await decision_api.decide(observation,
actions, goal="...", images=None)` returns `action`, `confidence`,
`probabilities`, `latency_ms`, and `model`.

Images (vision-capable models only): at most 4 per decision, each a data URL
string such as `f"data:image/png;base64,{base64.b64encode(png_bytes).decode()}"`
(not bytes, paths, or dicts); PNG, JPEG, or WebP, up to 4 MiB and 16
megapixels each and 8 MiB in total. Small, cropped images keep latency low; degenerate ones (a 1x1 pixel)
fail with a server error.
