"""Catch startup owns its subprocess, including failed and cancelled startup."""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import env


class Process:
    def __init__(self, line=b"PORT 12345\n"):
        self.stdout = type("Stdout", (), {"readline": AsyncMock(return_value=line)})()
        self.returncode = None
        self.reaped = False

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9

    async def wait(self):
        self.reaped = True
        return self.returncode


class CatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.here = patch.object(env, "HERE", Path(self.directory.name))
        self.here.start()
        self.addCleanup(self.here.stop)

    async def test_concurrent_runs_have_independent_artifacts(self):
        processes = [Process(), Process()]
        writer = type("Writer", (), {"close": lambda _self: None, "wait_closed": AsyncMock()})()
        with (
            patch.object(asyncio, "create_subprocess_exec", AsyncMock(side_effect=processes)),
            patch.object(asyncio, "open_connection", AsyncMock(return_value=(None, writer))),
        ):
            first, second = await asyncio.gather(env.CatchEnv.make(), env.CatchEnv.make())
            self.assertNotEqual(first.summary_path, second.summary_path)
            self.assertNotEqual(first.recording, second.recording)
            self.assertNotEqual(first.summary_path.parent, second.summary_path.parent)
            await asyncio.gather(first.close(), second.close())
        self.assertTrue(all(process.reaped for process in processes))

    async def test_failed_startup_terminates_and_reaps_process(self):
        for line, failure in ((b"bad startup\n", RuntimeError), (b"PORT invalid\n", ValueError)):
            process = Process(line)
            with patch.object(asyncio, "create_subprocess_exec", AsyncMock(return_value=process)):
                with self.assertRaises(failure):
                    await env.CatchEnv.make(record=False)
            self.assertEqual(process.returncode, -15)
            self.assertTrue(process.reaped)

    async def test_connection_failure_terminates_and_reaps_process(self):
        process = Process()
        with (
            patch.object(asyncio, "create_subprocess_exec", AsyncMock(return_value=process)),
            patch.object(asyncio, "open_connection", AsyncMock(side_effect=ConnectionRefusedError)),
        ):
            with self.assertRaises(ConnectionRefusedError):
                await env.CatchEnv.make(record=False)
        self.assertEqual(process.returncode, -15)
        self.assertTrue(process.reaped)

    async def test_cancelled_startup_terminates_and_reaps_process(self):
        process = Process()
        process.stdout.readline.side_effect = asyncio.CancelledError
        with patch.object(asyncio, "create_subprocess_exec", AsyncMock(return_value=process)):
            with self.assertRaises(asyncio.CancelledError):
                await env.CatchEnv.make(record=False)
        self.assertEqual(process.returncode, -15)
        self.assertTrue(process.reaped)


if __name__ == "__main__":
    unittest.main()
