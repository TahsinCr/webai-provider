from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.live.runner import (
    ActionGate,
    CleanupError,
    LiveAccount,
    LiveSuite,
    SafetyError,
    _write_private_json,
    load_accounts,
    parse_accounts,
    select_account,
)


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
            _write_private_json(self.path, {"resources": [{"credentials": {"token": "secret"}}]})


class ActionGateTests(unittest.TestCase):
    def test_serializes_actions_and_stops_at_fixed_budget(self) -> None:
        async def scenario() -> list[float]:
            gate = ActionGate(interval=0.02, timeout=1, limit=2)
            started: list[float] = []

            async def action() -> None:
                started.append(asyncio.get_running_loop().time())

            await gate.call(action)
            await gate.call(action)
            with self.assertRaisesRegex(SafetyError, "action limit"):
                await gate.call(action)
            return started

        starts = asyncio.run(scenario())
        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(starts[1] - starts[0], 0.018)

    def test_supports_sync_api_actions_with_the_same_cooldown(self) -> None:
        async def scenario() -> list[float]:
            gate = ActionGate(interval=0.02, limit=2)
            started: list[float] = []

            def action() -> None:
                started.append(time.monotonic())

            await gate.call(action, asynchronous=False)
            await gate.call(action, asynchronous=False)
            return started

        starts = asyncio.run(scenario())
        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(starts[1] - starts[0], 0.018)

    def test_default_cli_plan_does_not_load_credentials_or_start_network(self) -> None:
        from tests.live import runner

        with patch.object(
            runner, "load_accounts", side_effect=AssertionError("must not read secrets")
        ):
            self.assertEqual(runner.main([]), 0)


class CleanupSafetyTests(unittest.TestCase):
    def test_existing_unmarked_remote_object_is_never_deleted(self) -> None:
        marker = "webai-live-0123456789abcdef0123456789abcdef-chat"
        remote = SimpleNamespace(remote_id="existing-chat", title="A pre-existing conversation")
        session = SimpleNamespace(
            chats=SimpleNamespace(refresh=AsyncMock()),
            get_chat=AsyncMock(return_value=remote),
            delete_chat=AsyncMock(),
        )
        suite = LiveSuite(session, LiveAccount("personal", "chatgpt", {}))
        entry = {
            "kind": "chat",
            "markers": [marker],
            "remote_id": remote.remote_id,
            "attempted": True,
        }

        async def scenario() -> None:
            from webaiprovider.errors import RemoteNotFound

            with self.assertRaisesRegex(CleanupError, "unique test marker"):
                await suite._cleanup_one(entry, RemoteNotFound)

        asyncio.run(scenario())
        session.delete_chat.assert_not_awaited()
