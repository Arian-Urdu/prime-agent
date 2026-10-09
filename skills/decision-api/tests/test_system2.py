"""Fault injection for background System 2 delivery and response freshness."""

import asyncio
import json
import unittest
from unittest.mock import patch

# Install the same host stand-ins before importing the package.
from test_loop import _Handle, _install_stubs

_install_stubs()

import decision_api


async def until(predicate):
    async def ready():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(ready(), 2)


class System2Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.observations = asyncio.Queue()
        self.completed = asyncio.Queue()
        self.sent = []
        self.children = []
        self.thinking = []
        self.deleted = []
        self.goals = {}
        self.loop = decision_api.Loop(
            self.observations.get,
            lambda _action: None,
            {"left": "go left", "right": "go right"},
            objective="catch",
            system1=lambda *_args: "left",
            on_step=lambda record, _observation: self.completed.put_nowait(record),
        )

        async def spawn(_prompt, *, name, model, thinking=None):
            self.children.append(name)
            self.thinking.append(thinking)
            return _Handle()

        async def send(text, **_kwargs):
            self.sent.append(text)

        async def receive(_kind, payload):
            name = payload["name"]
            if payload.get("close"):
                self.goals.pop(name, None)
                return None
            reply = self.goals.get(name)
            self.goals[name] = None
            return reply

        async def delete(name):
            self.deleted.append(name)

        for owner, name, replacement in (
            (decision_api.rlm, "spawn", spawn),
            (decision_api.rlm, "host_request", receive),
            (decision_api.agent_message, "send", send),
            (decision_api.rlm, "delete_subagent", delete),
        ):
            patcher = patch.object(owner, name, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.loop.start()

    async def asyncTearDown(self):
        if not self.loop._task.done():
            self.loop._task.cancel()
        await asyncio.wait_for(self.loop.wait(), 2)

    async def step(self):
        self.observations.put_nowait({"n": self.loop.step})
        return await asyncio.wait_for(self.completed.get(), 2)

    async def reply(self, data):
        self.goals[self.children[-1]] = data
        self.loop._system2_ready.set()
        await until(lambda: self.goals.get(self.children[-1]) is None)

    async def test_thinking_reaches_the_child_and_a_change_respawns_it(self):
        self.loop.system2.thinking = "low"
        await self.step()
        await until(lambda: self.children)
        self.loop.system2.thinking = "high"
        await self.step()
        await until(lambda: len(self.children) == 2)
        self.assertEqual(self.thinking, ["low", "high"])
        self.assertEqual(self.deleted, self.children[:1])

    async def test_hanging_send_does_not_block_actions_and_is_cancelled_on_stop(self):
        entered = asyncio.Event()
        cancelled = asyncio.Event()

        async def send(_text, **_kwargs):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        with patch.object(decision_api.agent_message, "send", send):
            await self.step()
            await asyncio.wait_for(entered.wait(), 2)
            for _ in range(20):
                await self.step()
            self.assertEqual(self.loop.step, 21)
            self.assertEqual(self.loop._pending_observation[0], 20)
            self.observations.put_nowait(None)
            result = await asyncio.wait_for(self.loop.wait(), 2)
        self.assertFalse(result["running"])
        self.assertTrue(cancelled.is_set())
        self.assertEqual(self.deleted, self.children)
        self.assertEqual(self.goals, {})

    async def test_hanging_goal_transport_does_not_block_actions(self):
        entered = asyncio.Event()

        async def receive(_kind, payload):
            if not payload.get("close"):
                entered.set()
                await asyncio.Event().wait()

        with patch.object(decision_api.rlm, "host_request", receive):
            await self.step()
            await asyncio.wait_for(entered.wait(), 2)
            for _ in range(20):
                await self.step()
            self.assertEqual(self.loop.step, 21)
            self.observations.put_nowait(None)
            await asyncio.wait_for(self.loop.wait(), 2)

    async def test_hanging_spawn_is_cancelled_and_its_named_child_is_cleaned(self):
        entered = asyncio.Event()

        async def spawn(_prompt, *, name, model, thinking=None):
            self.children.append(name)
            entered.set()
            await asyncio.Event().wait()

        with patch.object(decision_api.rlm, "spawn", spawn):
            await self.step()
            await asyncio.wait_for(entered.wait(), 2)
            await self.step()
            self.observations.put_nowait(None)
            await asyncio.wait_for(self.loop.wait(), 2)
        self.assertEqual(self.deleted, self.children)

    async def test_transient_spawn_failure_retries_without_a_new_observation(self):
        attempts = 0

        async def spawn(_prompt, *, name, model, thinking=None):
            nonlocal attempts
            attempts += 1
            self.children.append(name)
            if attempts == 1:
                raise RuntimeError("temporary spawn failure")
            return _Handle()

        with patch.object(decision_api.rlm, "spawn", spawn):
            await self.step()
            await until(lambda: self.loop._sent == 0)
        self.assertEqual(attempts, 2)
        self.assertIn("temporary spawn failure", self.loop.errors[0]["error"])
        self.assertIn("First message:", self.sent[0])

    async def test_first_send_retry_keeps_instructions_and_uses_latest_observation(self):
        first = asyncio.Event()

        async def send(text, **_kwargs):
            self.sent.append(text)
            if len(self.sent) == 1:
                first.set()
                raise RuntimeError("temporary send failure")

        with patch.object(decision_api.agent_message, "send", send):
            await self.step()
            await asyncio.wait_for(first.wait(), 2)
            await self.step()
            await until(lambda: self.loop._sent == 1)
        self.assertEqual(len(self.children), 1)
        self.assertTrue(all("First message:" in text for text in self.sent))
        self.assertEqual(json.loads(self.sent[-1].split("First message:\n")[1])["seq"], 1)

    async def test_stale_and_duplicate_responses_cannot_roll_guidance_back(self):
        await self.step()
        await until(lambda: self.loop._sent == 0)
        await self.reply({"seq": 0, "goal": "new strategy"})
        await self.step()
        await until(lambda: self.loop._sent == 1)
        await self.reply({"seq": 0, "goal": "stale strategy"})
        await self.step()
        self.assertEqual(self.loop.goal, "new strategy")
        await self.reply({"seq": 1, "goal": "newest strategy"})
        await self.step()
        await self.reply({"seq": 1, "goal": "duplicate strategy"})
        await self.step()
        self.assertEqual([update["goal"] for update in self.loop.goal_updates], ["new strategy", "newest strategy"])

    async def test_replaced_child_cannot_update_the_new_goal(self):
        await self.step()
        await until(lambda: self.loop._sent == 0)
        old_name = self.children[-1]
        self.loop.system2 = decision_api.System2(model="different")
        await self.step()
        await until(lambda: self.loop._sent == 1)
        self.goals[old_name] = {"seq": 1, "goal": "retired child strategy"}
        await self.step()
        self.assertEqual(self.loop.goal, "catch")
        self.assertNotEqual(old_name, self.children[-1])
        self.assertIn(old_name, self.deleted)

    async def test_system2_action_is_consumed_once_and_named_action_goal_is_rejected(self):
        await self.step()
        await until(lambda: self.loop._sent == 0)
        await self.reply({"seq": 0, "goal": "left"})
        self.assertEqual((await self.step())["action"], "left")
        self.assertTrue(any("ignored goal" in error["error"] for error in self.loop.errors))
        await until(lambda: self.loop._sent == 1)
        self.loop.system2 = decision_api.System2(can_act=True)
        await self.step()
        await until(lambda: self.loop._sent == 2)
        await self.reply({"seq": 2, "goal": "hold the line", "action": "right"})
        record = await self.step()
        self.assertEqual((record["action"], record["source"]), ("right", "system2"))
        self.assertEqual((await self.step())["action"], "left")
        self.assertEqual(self.loop.goal, "hold the line")

    async def test_cancel_during_retirement_retries_cleanup(self):
        entered = asyncio.Event()
        attempts = []

        async def delete(name):
            attempts.append(name)
            if len(attempts) == 1:
                entered.set()
                await asyncio.Event().wait()

        with patch.object(decision_api.rlm, "delete_subagent", delete):
            await self.step()
            await until(lambda: self.loop._sent == 0)
            self.loop.system2 = None
            await self.step()
            await asyncio.wait_for(entered.wait(), 2)
            self.observations.put_nowait(None)
            await asyncio.wait_for(self.loop.wait(), 2)
        self.assertEqual(attempts, [self.children[0], self.children[0]])

    async def test_malformed_guidance_cannot_stop_system1(self):
        self.loop.system2 = decision_api.System2(can_act=True)
        await self.step()
        await until(lambda: self.loop._sent == 0)
        await self.reply({"seq": "invalid"})
        self.assertEqual((await self.step())["action"], "left")
        await self.reply({"seq": 0, "action": ["right"]})
        self.assertEqual((await self.step())["action"], "left")
        self.assertEqual(self.loop.step, 3)
        self.assertTrue(any("invalid goal" in error["error"] for error in self.loop.errors))
        self.assertTrue(any("ignored action" in error["error"] for error in self.loop.errors))

    async def test_hanging_cleanup_is_bounded_and_reported(self):
        async def delete(_name):
            await asyncio.Event().wait()

        with (
            patch.object(decision_api.rlm, "delete_subagent", delete),
            patch.object(decision_api, "_SYSTEM2_CLEANUP_TIMEOUT", 0.01),
        ):
            await self.step()
            await until(lambda: self.loop._sent == 0)
            self.observations.put_nowait(None)
            result = await asyncio.wait_for(self.loop.wait(), 2)
        self.assertFalse(result["running"])
        self.assertTrue(any("TimeoutError" in error["error"] for error in self.loop.errors))


class DefaultMessageTests(unittest.TestCase):
    def test_text_observation_is_encoded_once_with_the_message(self):
        text = "HUD\n#@#"
        message = decision_api._default_message(text, [], "", "objective", {"move": "keys"})
        self.assertEqual(json.loads(json.dumps(message))["observation"], text)
        self.assertEqual(decision_api._default_message({"x": 1}, [], "", "", {})["observation"], '{"x": 1}')

if __name__ == "__main__":
    unittest.main()
