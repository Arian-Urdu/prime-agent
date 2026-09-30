from __future__ import annotations

import asyncio
import os
import sys
import time
import unittest
from typing import Any
from unittest.mock import AsyncMock, patch

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import rlm  # noqa: E402


class FakeJobHandle:
    """Fake bash handle: output grows per poll, then the job stops."""

    def __init__(self, pid: int, chunks: list[str]) -> None:
        self.pid = pid
        self.command = "fake long-running job"
        self._chunks = chunks
        self._index = 0
        self.running = True

    def output(self) -> str:
        consumed = "".join(self._chunks[: self._index])
        return consumed

    def _grow(self) -> None:
        if self._index < len(self._chunks):
            self._index += 1
        if self._index >= len(self._chunks):
            self.running = False


class RlmWatchJobTest(unittest.TestCase):
    def tearDown(self) -> None:
        rlm._JOB_WATCHES.clear()

    def test_job_watch_emits_byte_ranges_and_stops_when_the_job_ends(self) -> None:
        seen: list[tuple[int, int]] = []
        handle = FakeJobHandle(4242, ["a" * 100, "b" * 200, "c" * 300])

        async def fake_host_request(request_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
            if request_type == "bash.progress":
                seen.append((payload["fromBytes"], payload["toBytes"]))
            return {"status": "ok"}

        async def scenario() -> None:
            with patch.object(rlm, "host_request", AsyncMock(side_effect=fake_host_request)):
                rlm._JOB_WATCHES.clear()
                result = await rlm.rlm.watch.job(handle, interval_seconds=0.01)
                self.assertEqual(result, {"pid": 4242, "watching": True})
                self.assertEqual(rlm.rlm.watch.job_list(), [{"pid": 4242, "interval": 0.01}])

                await asyncio.sleep(0.005)  # let the poller capture its 0-byte baseline
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    handle._grow()
                    await asyncio.sleep(0.005)
                    if not handle.running:
                        break
                # Let the poller observe the final chunk and exit its loop.
                for _ in range(20):
                    await asyncio.sleep(0.01)
                    if seen and seen[-1][1] == 600:
                        break

                self.assertEqual(rlm.rlm.watch.job_cancel(4242), True)
                self.assertEqual(rlm.rlm.watch.job_cancel(4242), False)

        asyncio.run(scenario())
        self.assertTrue(len(seen) >= 1)
        self.assertEqual(seen[0][0], 0)
        self.assertEqual(seen[-1][1], 600)

    def test_job_watch_rejects_non_handles_and_invalid_intervals(self) -> None:
        async def scenario() -> None:
            rlm._JOB_WATCHES.clear()
            with self.assertRaises(TypeError):
                await rlm.rlm.watch.job(object())
            with self.assertRaises(ValueError):
                await rlm.rlm.watch.job(FakeJobHandle(1, ["x"]), interval_seconds=0)

        asyncio.run(scenario())


class RlmWatchAgentTest(unittest.TestCase):
    def test_agent_watch_forwards_to_host_handlers(self) -> None:
        host_request = AsyncMock(return_value={"id": "watch-agent-sub-1", "childName": "worker", "messages": 3})
        with patch.object(rlm, "host_request", host_request):
            result = asyncio.run(rlm.rlm.watch.agent("worker"))
        self.assertEqual(result["childName"], "worker")
        host_request.assert_awaited_once_with("rlm.watch.agent", {"target": "worker"})

        host_request.reset_mock()
        host_request.return_value = {"watches": []}
        with patch.object(rlm, "host_request", host_request):
            asyncio.run(rlm.rlm.watch.agent_list())
        host_request.assert_awaited_once_with("rlm.watch.agent_list")

        host_request.reset_mock()
        host_request.return_value = {"cancelled": True}
        with patch.object(rlm, "host_request", host_request):
            asyncio.run(rlm.rlm.watch.agent_cancel("watch-agent-sub-1"))
        host_request.assert_awaited_once_with("rlm.watch.agent_cancel", {"id": "watch-agent-sub-1"})

    def test_agent_watch_rejects_empty_targets_before_the_host_request(self) -> None:
        host_request = AsyncMock(return_value={})
        with patch.object(rlm, "host_request", host_request):
            with self.assertRaises(ValueError):
                asyncio.run(rlm.rlm.watch.agent("   "))
        host_request.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
