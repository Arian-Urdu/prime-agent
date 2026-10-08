# Catch

Catch is a small Gym-style environment for the Decision API skill. Install
[uv](https://docs.astral.sh/uv/) and use Python 3.11 or newer. `uv run` installs
the game's declared pygame dependency. Install ffmpeg on PATH to record MP4;
use `record=False` when recording is unnecessary. `headless=True` runs without
a display. Each run writes into its own `examples/catch/runs/catch-*` directory.

In Prime Agent, configure a `decisionApi.systemOneModel` registry model
(vision-capable for image-based observations) and run `/decision-api on`. Run this in the persistent
Python REPL with the skill enabled, substituting the repository path:

```python
import sys
sys.path.insert(0, "<repo>/examples/catch")
import env

catch = await env.CatchEnv.make(seconds=60, seed=1, record=False, headless=True)
loop = None
try:
    observation, info = await catch.reset()

    async def observe():
        return None if observation.get("done") or observation.get("truncated") else observation

    async def act(action):
        global observation
        observation, reward, terminated, truncated, info = await catch.step(action)
        if terminated or truncated:
            observation = {**observation, "done": True}

    loop = decision_api.Loop(observe, act, catch.actions, objective="Catch the falling circles", tick=0.1)
    loop.start()
    while (await loop.wait(timeout=10))["running"]:
        print(loop.status(), loop.errors[-3:])
    print(await catch.close())
finally:
    if loop is not None:
        await loop.stop()
    await catch.close()
```

The first `reset()` starts the episode clock. `step(action)` returns
`(observation, reward, terminated, truncated, info)`; reward is catches minus
drops since the previous step. `close()` returns the final score and optional
recording path and is safe to call again. The session agent designs the loop;
the environment supplies observations and applies actions.

Run startup/artifact regression tests with
`python3 -m unittest discover -s examples/catch -p 'test_*.py' -v`.
