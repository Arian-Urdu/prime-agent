"""Prime Agent decision-api skill: an experimental System 1 / System 2 loop.

System 1 is the session's decision model, picked by the user with
/decision-api: TypeSafe's Jev (text-only) or Cloudflare's Clef
(vision-capable). One call per observation picks the next action. System 2 is
a Prime Agent subagent that
reads the newest observation plus System 1's action history and writes the
sub-goal System 1 follows. The calling agent operates the loop: it starts it,
watches it, and adjusts any part of it live, but never acts as System 2.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import agent_message
import rlm

# None uses the picked provider's model (jev-latest or clef).
DEFAULT_MODEL: str | None = None
DEFAULT_INSTRUCTIONS = "Choose the next action that best advances the goal given the observation."
DEFAULT_SYSTEM2_PROMPT = """You are System 2 in a real-time control loop.

System 1, a fast decision model, picks one action per observation from the
current action set while following the goal you set. You receive JSON messages
with `seq`, the operator's `objective`, the newest `observation`, the current
`actions`, System 1's `recent_actions` (with confidences), and the
`current_goal`.

For every message, judge whether System 1 is making progress toward the
objective and answer in one Python cell by writing GOAL_FILE:

    import json, pathlib
    pathlib.Path("GOAL_FILE").write_text(json.dumps({"seq": <seq>, "goal": "..."}))

Use the `seq` of the message you answered. Write a new `goal` only when
System 1 needs a decision from you: it has no goal yet, it is stuck or
repeating itself, it drifts from the objective, or the situation changed and
needs a new direction. Otherwise leave `goal` out (`{"seq": <seq>}`) and
System 1 keeps its current goal.

You make high-level decisions only and never give direct actions: a goal is
one or two sentences of strategy or guidance, never an action name, a key, a
move, or a step-by-step command, and never an `action` field. The file is your
only channel: never message any agent (including your parent) and keep replies
empty. Keep turns short; the loop keeps acting while you think. Act only on
JSON messages that carry a `seq`; if these instructions arrive without one, do
nothing.
"""
_CAN_ACT_NOTE = (
    'Exception: the operator allows one direct action at a time. When System 1 is stuck, you may add '
    '"action": "<name>" to the JSON to make System 1 take that action once.\n'
)
_OBSERVATION_CHARS = 6000
_MAX_ERRORS = 50


def _default_state(observation: Any, goal: str, history: list[dict[str, Any]]) -> dict[str, Any]:
    """What System 1 sees by default: the observation, the goal, and recent actions."""
    state: dict[str, Any] = {"observation": observation}
    if goal:
        state["goal"] = goal
    if history:
        state["recent_actions"] = history
    return state


def _default_message(
    observation: Any, history: list[dict[str, Any]], goal: str, objective: str, actions: dict[str, str]
) -> dict[str, Any]:
    """What System 2 receives by default (the loop adds `seq`)."""
    return {
        "objective": objective,
        "observation": json.dumps(observation, default=str)[:_OBSERVATION_CHARS],
        "actions": list(actions),
        "recent_actions": history,
        "current_goal": goal,
    }


async def _ask_system1(
    state: Any,
    actions: dict[str, str],
    *,
    instructions: str = DEFAULT_INSTRUCTIONS,
    model: str | None = DEFAULT_MODEL,
    images: list[str] | None = None,
) -> dict[str, Any]:
    """One System 1 decision for an arbitrary `state`. The host calls the
    session's provider with the key the user saved via /decision-api; the key
    never enters this kernel. `images` (Clef only, at most 4) are data
    URL strings (`data:image/png;base64,...`).
    Returns `action`, `confidence`, `probabilities`, `latency_ms`, and `model`."""
    question = {"type": "choice", "instructions": instructions, "criteria": actions}
    request: dict[str, Any] = {"state": state, "questions": {"action": question}}
    if model is not None:
        request["model"] = model
    if images:
        bad = [
            repr(image)[:40] for image in images if not (isinstance(image, str) and image.startswith("data:image/"))
        ]
        if bad:
            raise TypeError(f"images must be data URL strings like 'data:image/png;base64,...', got {bad}")
        request["images"] = list(images)
    started = time.perf_counter()
    body = await rlm.host_request("decision_api.decide", {"request": request})
    answer = body["answers"]["action"]
    return {
        "action": answer["choice"],
        "confidence": answer["confidence"],
        "probabilities": answer["probabilities"],
        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        "model": body.get("model", model),
    }


async def decide(
    observation: Any,
    actions: dict[str, str],
    *,
    goal: str = "",
    history: list[dict[str, Any]] | None = None,
    instructions: str = DEFAULT_INSTRUCTIONS,
    model: str | None = DEFAULT_MODEL,
    images: list[str] | None = None,
) -> dict[str, Any]:
    """A single System 1 decision with the default state."""
    return await _ask_system1(
        _default_state(observation, goal, history or []),
        actions,
        instructions=instructions,
        model=model,
        images=images,
    )


async def _call(fn: Callable[..., Any], *args: Any) -> Any:
    result = fn(*args)
    return await result if inspect.isawaitable(result) else result


@dataclass
class System2:
    """How System 2 operates. Changing `prompt` or `model` on a running loop
    respawns System 2 at the next step; `interval` and `message` apply at once.

    - `prompt`: System 2's instructions; `GOAL_FILE` is replaced with the goal
      file path. They are sent with the first message.
    - `model`: subagent model (None inherits the calling agent's model).
    - `interval`: None sends the newest observation once System 2 answered its
      previous message; a number of seconds sends on that fixed interval instead.
    - `message`: `(observation, history, goal, objective, actions) -> dict`.
    - `can_act`: False (default) ignores any action from System 2, including a
      goal that only names an action; True lets an "action" in the goal file
      replace System 1's next decision once.
    """

    prompt: str = DEFAULT_SYSTEM2_PROMPT
    model: str | None = None
    interval: float | None = None
    message: Callable[..., dict[str, Any]] = _default_message
    can_act: bool = False


class Loop:
    """A live System 1 / System 2 control loop.

    Every attribute is read afresh each step, so assigning one on a running
    loop changes the next step. `goal` is read-only: System 2 owns it (it is
    the `objective` when System 2 is off).

    - `observe()` returns an observation, or None to end the loop.
    - `act(action)` applies an action. Both may be sync or async.
    - `actions`: action name -> when it applies, or `(observation) -> dict`.
    - `objective`: the overall task, sent to System 2 with every message.
    - `state`: `(observation, goal, history) -> Any`, what System 1 sees.
    - `images`: None, or `(observation) -> list` of data URL strings System 1 sees
      next to `state` (Clef only; see `decide`).
    - `instructions`, `model`: System 1's question instructions and model
      (None picks the provider's default).
    - `system1`: None for the session's decision model, or `(observation,
      actions, goal, history) -> action name or dict with "action"` to
      replace System 1.
    - `system2`: a `System2`, or None for System 1 only.
    - `tick`: minimum seconds per step (None runs as fast as decisions come).
    - `max_steps`: stop after this many steps (None runs until observe ends it).
    - `history_size`: recent actions shown to System 1 and System 2.
    - `on_step`: `(record, observation)` after each step; returning "stop" ends the loop.
    - `on_error`: when System 1 fails, "stop", "skip", or `(error, observation)
      -> action name or None`.
    """

    def __init__(
        self,
        observe: Callable[[], Any],
        act: Callable[[str], Any],
        actions: dict[str, str] | Callable[[Any], dict[str, str]],
        *,
        objective: str,
        **settings: Any,
    ) -> None:
        self.observe = observe
        self.act = act
        self.actions = actions
        self.objective = objective
        self.system2: System2 | None = System2()
        self.state: Callable[..., Any] = _default_state
        self.images: Callable[[Any], list[str] | None] | None = None
        self.instructions = DEFAULT_INSTRUCTIONS
        self.model: str | None = DEFAULT_MODEL
        self.system1: Callable[..., Any] | None = None
        self.tick: float | None = None
        self.max_steps: int | None = None
        self.history_size = 20
        self.on_step: Callable[..., Any] | None = None
        self.on_error: str | Callable[..., Any] = "stop"
        for name, value in settings.items():
            if not hasattr(self, name):
                raise TypeError(f"Loop has no setting {name!r}")
            setattr(self, name, value)
        self.history: list[dict[str, Any]] = []
        self.goal_updates: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []
        self.error: str | None = None
        self.step = 0
        self._goal_file = Path(tempfile.mkdtemp(prefix="decision-api-")) / "goal.json"
        self._goal: str | None = None
        self._goal_text: str | None = None
        self._system2_action: str | None = None
        self._answered = -1
        self._live: tuple[System2, str | None, tuple[str, str | None, bool]] | None = None
        self._spawn_seq = 0
        self._sent = -1
        self._sent_at = 0.0
        self._spawns = 0
        self._system1_model: str | None = None
        self._system2_model: str | None = None
        self._resume = asyncio.Event()
        self._resume.set()
        self._stopping = False
        self._task: asyncio.Task[None] | None = None

    @property
    def goal(self) -> str:
        if self.system2 is None or self._goal is None:
            return self.objective
        return self._goal

    def start(self) -> Loop:
        """Run the loop as a background task and return immediately."""
        if self._task is not None:
            raise RuntimeError("This loop was already started")
        self._task = asyncio.get_running_loop().create_task(self._run())
        return self

    def pause(self) -> None:
        self._resume.clear()

    def resume(self) -> None:
        self._resume.set()

    async def wait(self, timeout: float | None = None) -> dict[str, Any]:
        """Wait up to `timeout` seconds for the loop to end; return its status."""
        if self._task is None:
            raise RuntimeError("Start the loop first")
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout)
        except TimeoutError:
            pass
        except asyncio.CancelledError:
            if not self._task.cancelled():
                raise
        return self.status()

    async def stop(self) -> dict[str, Any]:
        """End the loop after the current step, remove System 2, return the status."""
        self._stopping = True
        self._resume.set()
        return await self.wait()

    async def run(self) -> dict[str, Any]:
        """Run to completion in the current cell."""
        return await self.start().wait()

    def status(self) -> dict[str, Any]:
        recent = self.history[-self.history_size :]
        latencies = [r["latency_ms"] for r in recent if r.get("latency_ms") is not None]
        return {
            "running": self._task is not None and not self._task.done(),
            "paused": not self._resume.is_set(),
            "step": self.step,
            "objective": self.objective,
            "goal": self.goal,
            "system1_model": self._system1_model,
            "system2": self._live[1] if self._live else None,
            "system2_model": self._system2_model if self._live else None,
            "last": self.history[-1] if self.history else None,
            "mean_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
            "errors": len(self.errors),
            "error": self.error,
        }

    def _record_error(self, where: str, error: BaseException) -> None:
        self.errors.append({"step": self.step, "where": where, "error": f"{type(error).__name__}: {error}"})
        del self.errors[:-_MAX_ERRORS]

    async def _run(self) -> None:
        try:
            while not self._stopping and (self.max_steps is None or self.step < self.max_steps):
                await self._resume.wait()
                if self._stopping:
                    break
                started = time.perf_counter()
                if await self._step() == "stop":
                    break
                if self.tick is not None:
                    await asyncio.sleep(max(0.0, self.tick - (time.perf_counter() - started)))
        except asyncio.CancelledError:
            self.error = "cancelled"
            raise
        except Exception as error:
            self.error = f"{type(error).__name__}: {error}"
        finally:
            await self._retire_system2()

    async def _step(self) -> str | None:
        observation = await _call(self.observe)
        if observation is None:
            return "stop"
        actions = self.actions(observation) if callable(self.actions) else self.actions
        self._read_goal(actions)
        history = self.history[-self.history_size :]
        try:
            if self._system2_action is not None:
                decision = {"action": self._system2_action, "source": "system2"}
                self._system2_action = None
            else:
                decision = await self._decide(observation, actions, history)
        except Exception as error:
            self._record_error("system1", error)
            if self.on_error == "stop":
                raise
            fallback = None if self.on_error == "skip" else await _call(self.on_error, error, observation)
            if fallback is None:
                self.step += 1
                return None
            decision = {"action": fallback, "confidence": None, "latency_ms": None, "fallback": True}
        if decision.get("model"):
            self._system1_model = decision["model"]
        await _call(self.act, decision["action"])
        record = {
            "step": self.step,
            "action": decision["action"],
            "confidence": decision.get("confidence"),
            "latency_ms": decision.get("latency_ms"),
        }
        if decision.get("fallback"):
            record["fallback"] = True
        if decision.get("source"):
            record["source"] = decision["source"]
        self.history.append(record)
        await self._drive_system2(observation, actions)
        self.step += 1
        if self.on_step is not None:
            return await _call(self.on_step, record, observation)
        return None

    async def _decide(
        self, observation: Any, actions: dict[str, str], history: list[dict[str, Any]]
    ) -> dict[str, Any]:
        if self.system1 is None:
            state = self.state(observation, self.goal, history)
            images = None if self.images is None else await _call(self.images, observation)
            return await _ask_system1(
                state, actions, instructions=self.instructions, model=self.model, images=images
            )
        started = time.perf_counter()
        choice = await _call(self.system1, observation, actions, self.goal, history)
        decision = dict(choice) if isinstance(choice, dict) else {"action": choice}
        decision.setdefault("latency_ms", round((time.perf_counter() - started) * 1000, 1))
        return decision

    def _read_goal(self, actions: dict[str, str]) -> None:
        try:
            text = self._goal_file.read_text()
        except OSError:
            return
        if text == self._goal_text:
            return
        self._goal_text = text
        try:
            data = json.loads(text)
            seq = int(data["seq"])
        except (ValueError, KeyError, TypeError) as error:
            self._record_error("system2", ValueError(f"unreadable goal file: {error!r}"))
            return
        self._answered = seq
        can_act = self.system2 is not None and self.system2.can_act
        action = data.get("action")
        if action is not None:
            if can_act and action in actions:
                self._system2_action = action
            else:
                self._record_error("system2", ValueError(f"ignored action {action!r} from System 2"))
        goal = data.get("goal")
        if not isinstance(goal, str) or not goal.strip():
            return
        if not can_act and goal.strip().strip(".").lower() in {name.lower() for name in actions}:
            self._record_error("system2", ValueError(f"ignored goal {goal!r}: it only names an action"))
            return
        self._goal = goal
        self.goal_updates.append({"step": self.step, "seq": seq, "goal": goal})

    async def _drive_system2(self, observation: Any, actions: dict[str, str]) -> None:
        wanted = self.system2
        config = None if wanted is None else (wanted.prompt, wanted.model, wanted.can_act)
        if self._live is not None and (self._live[0] is not wanted or self._live[2] != config):
            await self._retire_system2()
        if wanted is None or config is None:
            return
        if self._live is not None:
            # Nothing more reaches System 2 before it answered the message carrying its instructions.
            if self._live[1] is None or self._answered < self._spawn_seq:
                return
            if wanted.interval is None:
                due = self._answered >= self._sent
            else:
                due = time.monotonic() - self._sent_at >= wanted.interval
            if not due:
                return
        history = self.history[-self.history_size :]
        message = {"seq": self.step, **wanted.message(observation, history, self.goal, self.objective, actions)}
        text = json.dumps(message, default=str)
        if self._live is None:
            # The daemon holds a spawn prompt until the caller's turn ends, but
            # agent messages arrive at once: the instructions ride in the first
            # message, and the late spawn prompt (without a message) is a no-op.
            self._spawns += 1
            name = f"system-2-{self._goal_file.parent.name[-8:]}-{self._spawns}"
            instructions = wanted.prompt.replace("GOAL_FILE", str(self._goal_file))
            if wanted.can_act:
                instructions += _CAN_ACT_NOTE
            try:
                handle = await rlm.spawn(instructions, name=name, model=wanted.model)
            except Exception as error:
                self._record_error("system2", error)
                self._live = (wanted, None, config)
                return
            self._live = (wanted, name, config)
            self._system2_model = handle.model
            self._spawn_seq = self.step
            text = f"{instructions}\nFirst message:\n{text}"
        try:
            await agent_message.send(text, receiver_role="child", receiver_name=self._live[1])
        except Exception as error:
            self._record_error("system2", error)
            return
        self._sent, self._sent_at = self.step, time.monotonic()

    async def _retire_system2(self) -> None:
        if self._live is None:
            return
        name = self._live[1]
        self._live = None
        if name is not None:
            try:
                await rlm.delete_subagent(name)
            except Exception as error:
                self._record_error("system2", error)
