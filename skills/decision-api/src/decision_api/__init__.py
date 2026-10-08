"""Prime Agent decision-api skill: an experimental System 1 / System 2 loop.

System 1 is the session's decision model: the model the user named in the
decisionApi.systemOneModel setting. One call per observation picks the next
action. System 2 is
a Prime Agent subagent that
reads the newest observation plus System 1's action history and writes the
sub-goal System 1 follows. The calling agent operates the loop: it starts it,
watches it, and adjusts any part of it live, but never acts as System 2.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import agent_message
import rlm

DEFAULT_INSTRUCTIONS = "Choose the next action that best advances the goal given the observation."
DEFAULT_SYSTEM2_PROMPT = """You are System 2 in a real-time control loop.

System 1, a fast decision model, picks one action per observation from the
current action set while following the goal you set. You receive JSON messages
with `seq`, the operator's `objective`, the newest `observation`, the current
`actions`, System 1's `recent_actions` (with confidences), and the
`current_goal`.

For every message, judge whether System 1 is making progress toward the
objective and answer in one Python cell by messaging your parent:

    import json, agent_message
    await agent_message.send(json.dumps({"type": "decision_api.goal", "seq": <seq>, "goal": "..."}), receiver_role="parent")

Use the `seq` of the message you answered. Write a new `goal` only when
System 1 needs a decision from you: it has no goal yet, it is stuck or
repeating itself, it drifts from the objective, or the situation changed and
needs a new direction. Otherwise leave `goal` out
(`{"type": "decision_api.goal", "seq": <seq>}`) and System 1 keeps its current goal.

You make high-level decisions only and never give direct actions: a goal is
one or two sentences of strategy or guidance, never an action name, a key, a
move, or a step-by-step command, and never an `action` field. Parent messages
route directly into the loop; keep final replies
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
_SYSTEM2_CLEANUP_TIMEOUT = 5.0


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
    images: list[str] | None = None,
) -> dict[str, Any]:
    """One System 1 decision for an arbitrary `state`. The host serves the
    request with the decisionApi.systemOneModel model through the normal
    provider transports; no key ever enters this kernel. `images` (vision
    models only, at most 4) are data URL strings
    (`data:image/png;base64,...`).
    Returns `action`, `confidence`, `probabilities`, `latency_ms`, and `model`."""
    question = {"type": "choice", "instructions": instructions, "criteria": actions}
    request: dict[str, Any] = {"state": state, "questions": {"action": question}}
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
        "model": body.get("model"),
    }


async def decide(
    observation: Any,
    actions: dict[str, str],
    *,
    goal: str = "",
    history: list[dict[str, Any]] | None = None,
    instructions: str = DEFAULT_INSTRUCTIONS,
    images: list[str] | None = None,
) -> dict[str, Any]:
    """A single System 1 decision with the default state."""
    return await _ask_system1(
        _default_state(observation, goal, history or []),
        actions,
        instructions=instructions,
        images=images,
    )


async def _call(fn: Callable[..., Any], *args: Any) -> Any:
    result = fn(*args)
    return await result if inspect.isawaitable(result) else result


@dataclass
class System2:
    """How System 2 operates. Changing `prompt` or `model` on a running loop
    schedules a replacement System 2; `interval` and `message` apply at once.
    Transport runs in the background with one newest pending observation.

    - `prompt`: System 2's instructions, sent with the first message.
      Replies use tagged JSON parent messages as in DEFAULT_SYSTEM2_PROMPT.
    - `model`: subagent model (None inherits the calling agent's model).
    - `interval`: None sends the newest observation once System 2 answered its
      previous message; a number of seconds sends on that fixed interval instead.
    - `message`: `(observation, history, goal, objective, actions) -> dict`.
    - `can_act`: False (default) ignores any action from System 2, including a
      goal that only names an action; True lets an "action" in the goal message
      replace System 1's next decision once.
    - `timeout`: maximum seconds for child creation or message delivery.
      Transient failures retry with capped exponential backoff while System 1
      keeps acting. Child cleanup has a separate five-second bound.
    """

    prompt: str = DEFAULT_SYSTEM2_PROMPT
    model: str | None = None
    interval: float | None = None
    message: Callable[..., dict[str, Any]] = _default_message
    can_act: bool = False
    timeout: float = 30.0


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
      next to `state` (vision-capable models only; see `decide`).
    - `instructions`: System 1's question instructions (the model comes from
      the decisionApi.systemOneModel setting).
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
        self._loop_id = uuid.uuid4().hex
        self._goal: str | None = None
        self._system2_action: str | None = None
        self._answered = -1
        self._live: tuple[System2, str | None, tuple[str, str | None, bool]] | None = None
        self._spawn_seq = 0
        self._sent = -1
        self._sent_at = 0.0
        self._spawns = 0
        self._spawning_name: str | None = None
        self._retiring_name: str | None = None
        self._instructions = ""
        self._pending_observation: tuple[int, Any, dict[str, str], list[dict[str, Any]]] | None = None
        self._system2_ready = asyncio.Event()
        self._system2_task: asyncio.Task[None] | None = None
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
        self._system2_task = asyncio.get_running_loop().create_task(self._drive_system2())
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
                else:
                    # Synchronous callbacks must not starve transport or other kernel tasks.
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            self.error = "cancelled"
            raise
        except Exception as error:
            self.error = f"{type(error).__name__}: {error}"
        finally:
            self._stopping = True
            if self._system2_task is not None:
                self._system2_task.cancel()
                try:
                    await self._system2_task
                except asyncio.CancelledError:
                    pass

    async def _step(self) -> str | None:
        observation = await _call(self.observe)
        if observation is None:
            return "stop"
        actions = self.actions(observation) if callable(self.actions) else self.actions
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
        self._pending_observation = (self.step, observation, dict(actions), list(self.history[-self.history_size :]))
        self._system2_ready.set()
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
                state, actions, instructions=self.instructions, images=images
            )
        started = time.perf_counter()
        choice = await _call(self.system1, observation, actions, self.goal, history)
        decision = dict(choice) if isinstance(choice, dict) else {"action": choice}
        decision.setdefault("latency_ms", round((time.perf_counter() - started) * 1000, 1))
        return decision

    def _read_goal(self, data: dict[str, Any], actions: dict[str, str]) -> None:
        if self._live is None or self.system2 is not self._live[0]:
            return
        if (self.system2.prompt, self.system2.model, self.system2.can_act) != self._live[2]:
            return
        try:
            seq = data["seq"]
            if type(seq) is not int:
                raise ValueError("seq must be an integer")
        except (ValueError, KeyError, TypeError) as error:
            self._record_error("system2", ValueError(f"invalid goal message: {error!r}"))
            return
        # Only the live child's latest delivered observation can update guidance.
        if seq != self._sent or seq <= self._answered or seq < self._spawn_seq:
            return
        self._answered = seq
        can_act = self.system2 is not None and self.system2.can_act
        action = data.get("action")
        if action is not None:
            if can_act and isinstance(action, str) and action in actions:
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

    async def _drive_system2(self) -> None:
        failures = 0
        try:
            while not self._stopping:
                await self._system2_ready.wait()
                self._system2_ready.clear()
                wanted = self.system2
                config = None if wanted is None else (wanted.prompt, wanted.model, wanted.can_act)
                if self._live is not None and (self._live[0] is not wanted or self._live[2] != config):
                    await self._retire_system2()
                    failures = 0
                if wanted is None or config is None or self._pending_observation is None:
                    continue
                try:
                    if wanted.timeout <= 0:
                        raise ValueError("System2.timeout must be positive")
                    if self._live is not None:
                        data = await asyncio.wait_for(
                            rlm.host_request("decision_api.goal", {"name": self._live[1]}), wanted.timeout
                        )
                        if data is not None:
                            self._read_goal(data, self._pending_observation[2])
                        if self._sent >= self._spawn_seq:
                            # Instructions must be acknowledged before further observations arrive.
                            if self._answered < self._spawn_seq:
                                continue
                            due = (
                                self._answered >= self._sent if wanted.interval is None
                                else time.monotonic() - self._sent_at >= wanted.interval
                            )
                            if not due:
                                continue
                    if self._live is None:
                        self._spawns += 1
                        self._answered = self._sent = -1
                        self._system2_action = None
                        name = f"system-2-{self._loop_id}-{self._spawns}"
                        self._instructions = wanted.prompt
                        if wanted.can_act:
                            self._instructions += _CAN_ACT_NOTE
                        # Track the name before awaiting so cancellation also attempts cleanup.
                        self._spawning_name = name
                        await asyncio.wait_for(rlm.host_request("decision_api.goal", {"name": name}), wanted.timeout)
                        handle = await asyncio.wait_for(
                            rlm.spawn(self._instructions, name=name, model=wanted.model), wanted.timeout
                        )
                        self._live = (wanted, name, config)
                        self._spawning_name = None
                        self._system2_model = handle.model
                        self._spawn_seq = self._pending_observation[0]
                    seq, observation, actions, history = self._pending_observation
                    message = {
                        **wanted.message(observation, history, self.goal, self.objective, actions), "seq": seq
                    }
                    text = json.dumps(message, default=str)
                    if self._sent < self._spawn_seq:
                        # Instructions ride in the first message, including a retried first send.
                        text = f"{self._instructions}\nFirst message:\n{text}"
                    await asyncio.wait_for(
                        agent_message.send(text, receiver_role="child", receiver_name=self._live[1]), wanted.timeout
                    )
                    self._sent, self._sent_at = seq, time.monotonic()
                    failures = 0
                except Exception as error:
                    self._record_error("system2", error)
                    if self._spawning_name is not None:
                        await self._retire_system2()
                    failures += 1
                    await asyncio.sleep(min(5.0, 0.25 * 2 ** min(failures - 1, 5)))
                    self._system2_ready.set()
        finally:
            await self._retire_system2()

    async def _retire_system2(self) -> None:
        if self._live is None and self._spawning_name is None and self._retiring_name is None:
            return
        name = self._live[1] if self._live is not None else self._spawning_name or self._retiring_name
        self._live = None
        self._spawning_name = None
        self._retiring_name = name
        self._system2_action = None
        if name is not None:
            try:
                await asyncio.wait_for(
                    rlm.host_request("decision_api.goal", {"name": name, "close": True}), _SYSTEM2_CLEANUP_TIMEOUT
                )
            except Exception as error:
                self._record_error("system2", error)
            try:
                await asyncio.wait_for(rlm.delete_subagent(name), _SYSTEM2_CLEANUP_TIMEOUT)
            except Exception as error:
                self._record_error("system2", error)
        self._retiring_name = None
