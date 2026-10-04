"""The decision-api loop against a stand-in host.

Covers image strings, step control, a live settings change, and System 2's
action gate. `rlm` and `agent_message` are installed before the skill imports.
"""

import asyncio
import json
import sys
import types
import unittest

host_calls = []
spawned = []


async def host_request(kind, payload):
    host_calls.append((kind, payload))
    return {
        "answers": {
            "action": {
                "choice": "left",
                "confidence": 0.9,
                "probabilities": {"left": 0.9},
            }
        },
        "model": "jev-latest",
    }


class _Handle:
    model = "sub"


async def spawn(prompt, *, name, model):
    spawned.append({"prompt": prompt, "name": name, "model": model})
    return _Handle()


async def send(text, *, receiver_role, receiver_name):
    spawned.append({"sent": text, "role": receiver_role, "name": receiver_name})


async def delete_subagent(name):
    spawned.append({"deleted": name})


def _install_stubs():
    rlm = types.ModuleType("rlm")
    rlm.host_request = host_request
    rlm.spawn = spawn
    rlm.delete_subagent = delete_subagent
    agent_message = types.ModuleType("agent_message")
    agent_message.send = send
    sys.modules["rlm"] = rlm
    sys.modules["agent_message"] = agent_message


_install_stubs()

from decision_api import DEFAULT_INSTRUCTIONS, Loop, System2, decide  # noqa: E402


class DecideTests(unittest.TestCase):
    def setUp(self):
        host_calls.clear()

    def test_images_must_be_data_url_strings(self):
        async def check():
            for images in (
                [{"content_type": "image/png", "base64": "AA=="}],
                [b"png"],
                ["frame.png"],
            ):
                with self.assertRaises(TypeError) as raised:
                    await decide({"x": 1}, {"left": "go left"}, images=images)
                self.assertIn("data:image/png;base64", str(raised.exception))
            self.assertEqual(host_calls, [])

            result = await decide(
                {"x": 1},
                {"left": "go left"},
                images=["data:image/png;base64,AA=="],
            )
            self.assertEqual(result["action"], "left")
            self.assertEqual(result["confidence"], 0.9)
            self.assertEqual(result["probabilities"], {"left": 0.9})
            self.assertEqual(result["model"], "jev-latest")
            self.assertIsInstance(result["latency_ms"], float)

            kind, payload = host_calls[0]
            request = payload["request"]
            self.assertEqual(kind, "decision_api.decide")
            self.assertEqual(request["state"], {"observation": {"x": 1}})
            self.assertEqual(request["images"], ["data:image/png;base64,AA=="])
            self.assertNotIn("model", request)
            self.assertEqual(
                request["questions"]["action"],
                {
                    "type": "choice",
                    "instructions": DEFAULT_INSTRUCTIONS,
                    "criteria": {"left": "go left"},
                },
            )

        asyncio.run(check())


class LoopTests(unittest.TestCase):
    def setUp(self):
        host_calls.clear()
        spawned.clear()

    def test_steps_skip_a_system1_error_and_stop_when_observe_ends(self):
        seen = []

        def observe():
            observe.n += 1
            if observe.n > 3:
                return None
            return {"n": observe.n}

        observe.n = 0

        def system1(_observation, _actions, _goal, _history):
            if observe.n == 2:
                raise RuntimeError("blip")
            return "left"

        async def run():
            loop = Loop(
                observe,
                seen.append,
                {"left": "go left"},
                objective="stay under it",
                system1=system1,
                system2=None,
                on_error="skip",
            )
            status = await loop.run()
            self.assertEqual(seen, ["left", "left"])
            self.assertEqual(loop.step, 3)
            self.assertEqual(len(loop.errors), 1)
            self.assertIn("blip", loop.errors[0]["error"])
            self.assertEqual(host_calls, [])
            self.assertEqual(
                {
                    key: status[key]
                    for key in (
                        "running",
                        "paused",
                        "step",
                        "objective",
                        "goal",
                        "errors",
                        "error",
                    )
                },
                {
                    "running": False,
                    "paused": False,
                    "step": 3,
                    "objective": "stay under it",
                    "goal": "stay under it",
                    "errors": 1,
                    "error": None,
                },
            )

        asyncio.run(run())

    def test_images_reach_the_host_and_a_bad_image_stops_the_next_step(self):
        taken = []

        def observe():
            return {}

        async def run():
            loop = Loop(
                observe,
                taken.append,
                {"left": "go left"},
                objective="catch",
                system2=None,
                images=lambda _observation: ["data:image/png;base64,AA=="],
            )

            async def on_step(record, _observation):
                if record["step"] == 0:
                    loop.images = lambda _observation: ["not-a-data-url"]

            loop.on_step = on_step
            status = await loop.run()
            self.assertEqual(taken, ["left"])
            self.assertTrue(status["error"].startswith("TypeError:"))
            self.assertEqual(len(host_calls), 1)
            self.assertEqual(
                host_calls[0][1]["request"]["images"],
                ["data:image/png;base64,AA=="],
            )

        asyncio.run(run())

    def test_the_next_step_reads_actions_assigned_on_a_running_loop(self):
        taken = []

        def observe():
            observe.n += 1
            return {} if observe.n <= 2 else None

        observe.n = 0

        def system1(_observation, actions, _goal, _history):
            return next(iter(actions))

        async def run():
            loop = Loop(
                observe,
                taken.append,
                {"left": "go left"},
                objective="catch",
                system1=system1,
                system2=None,
            )

            def on_step(record, _observation):
                if record["step"] == 0:
                    loop.actions = {"up": "go up"}

            loop.on_step = on_step
            await loop.run()
            self.assertEqual(taken, ["left", "up"])
            self.assertEqual(host_calls, [])

        asyncio.run(run())



if __name__ == "__main__":
    unittest.main()
