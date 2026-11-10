from __future__ import annotations

import asyncio
import importlib
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tests.live.support.actions import ActionGate
from tests.live.support.config import LiveAccount, load_accounts, parse_accounts, select_account
from tests.live.support.context import LiveContext
from tests.live.support.errors import CleanupError, SafetyError
from tests.live.support.journal import write_private_json


class LiveCredentialTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = self.root / ".config"
        self.config.mkdir(mode=0o700)
        if os.name == "posix":
            self.config.chmod(0o700)
        self.path = self.config / "credentials.json"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write(self, data: str, mode: int = 0o600) -> None:
        self.path.write_text(data, encoding="utf-8")
        if os.name == "posix":
            self.path.chmod(mode)

    def test_loads_one_alias_without_exposing_secret_in_repr(self) -> None:
        self._write(
            json.dumps(
                {
                    "version": 1,
                    "accounts": [
                        {
                            "id": "personal",
                            "provider": "chatgpt",
                            "credentials": {"cookies": {"session": "unit-test-secret"}},
                        }
                    ],
                }
            )
        )
        account = select_account(
            load_accounts(self.path, require_ignored=False), "chatgpt", "personal"
        )
        self.assertNotIn("unit-test-secret", repr(account))
        self.assertEqual(account.provider, "chatgpt")

    def test_rejects_permissive_secret_file(self) -> None:
        if os.name != "posix":
            self.skipTest("POSIX mode-bit check")
        self._write('{"version":1,"accounts":[]}', 0o644)
        with self.assertRaisesRegex(SafetyError, "mode 0600"):
            load_accounts(self.path, require_ignored=False)

    def test_invalid_credentials_never_echo_values(self) -> None:
        secret = "never-echo-this-cookie"
        data = {
            "version": 1,
            "accounts": [
                {
                    "id": "personal",
                    "provider": "chatgpt",
                    "credentials": {"cookie": secret},
                }
            ],
        }
        with self.assertRaises(SafetyError) as caught:
            parse_accounts({"version": 2, "secret": secret})
        self.assertNotIn(secret, str(caught.exception))
        account = parse_accounts(data)[0]
        self.assertNotIn(secret, repr(account))

    def test_refuses_symlinked_credentials(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("Symlink support")
        target = self.config / "actual.json"
        target.write_text('{"version":1,"accounts":[]}', encoding="utf-8")
        if os.name == "posix":
            target.chmod(0o600)
        self.path.symlink_to(target)
        with self.assertRaisesRegex(SafetyError, "symlinks"):
            load_accounts(self.path, require_ignored=False)

    def test_cleanup_journal_rejects_nested_credentials(self) -> None:
        with self.assertRaisesRegex(SafetyError, "must never contain credentials"):
            write_private_json(self.path, {"resources": [{"credentials": {"token": "secret"}}]})


class ActionGateTests(unittest.TestCase):
    def test_scenario_modules_compose_into_a_unittest_case(self) -> None:
        from tests.live.test_api import TestLiveProviderAPI

        self.assertTrue(issubclass(TestLiveProviderAPI, unittest.TestCase))

    def test_serializes_actions_and_stops_at_fixed_budget(self) -> None:
        async def scenario() -> list[float]:
            gate = ActionGate(interval=0.02, timeout=1, limit=2)
            started: list[float] = []

            async def action() -> None:
                started.append(asyncio.get_running_loop().time())

            await gate.call_async(action)
            await gate.call_async(action)
            with self.assertRaisesRegex(SafetyError, "action limit"):
                await gate.call_async(action)
            return started

        starts = asyncio.run(scenario())
        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(starts[1] - starts[0], 0.018)

    def test_sync_api_actions_share_the_cooldown_and_budget(self) -> None:
        gate = ActionGate(interval=0.02, limit=2)
        starts: list[float] = []

        def action() -> None:
            starts.append(time.monotonic())

        gate.call_sync(action)
        gate.call_sync(action)
        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(starts[1] - starts[0], 0.018)
        with self.assertRaisesRegex(SafetyError, "action limit"):
            gate.call_sync(action)

    def test_live_api_suite_is_disabled_without_explicit_opt_in(self) -> None:
        from tests.live import test_api

        with patch.dict(os.environ, {"WEBAI_LIVE_TESTS": "0"}):
            module = importlib.reload(test_api)
        self.assertTrue(module.TestLiveProviderAPI.__unittest_skip__)


class CleanupSafetyTests(unittest.TestCase):
    def test_existing_unmarked_remote_object_is_never_deleted(self) -> None:
        marker = "webai-live-0123456789abcdef0123456789abcdef-chat"
        remote = SimpleNamespace(remote_id="existing-chat", title="A pre-existing conversation")
        session = SimpleNamespace(
            chats=SimpleNamespace(refresh=Mock()),
            get_chat=Mock(return_value=remote),
            delete_chat=Mock(),
        )
        context = LiveContext(session, LiveAccount("personal", "chatgpt", {}), "sync", None)
        entry = {
            "kind": "chat",
            "markers": [marker],
            "remote_id": remote.remote_id,
            "attempted": True,
        }

        with self.assertRaisesRegex(CleanupError, "unique test marker"):
            context.cleanup_entry(entry)
        session.delete_chat.assert_not_called()
