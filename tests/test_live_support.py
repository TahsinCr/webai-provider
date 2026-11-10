from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from tests.live.support.actions import ActionGate
from tests.live.support.case import LiveAsyncTestCase, LiveSession, LiveSyncTestCase
from tests.live.support.config import (
    LiveAccount,
    load_accounts,
    load_or_import_account,
    parse_accounts,
    select_account,
)
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

    def test_imports_missing_provider_from_browser_and_preserves_other_accounts(self) -> None:
        from webaiprovider.accounts.account import Account
        from webaiprovider.accounts.filters import REQUIRED_PROVIDER_COOKIES

        self._write(
            json.dumps(
                {
                    "version": 1,
                    "accounts": [
                        {
                            "id": "claude-personal",
                            "provider": "claude",
                            "credentials": {"cookies": {"session": "existing-secret"}},
                        }
                    ],
                }
            )
        )
        chatgpt_auth_cookie = REQUIRED_PROVIDER_COOKIES["https://chatgpt.com"]
        browser_account = Account.from_cookies(
            "chatgpt", {chatgpt_auth_cookie: "browser-secret"}, id="default-browser"
        )
        with patch(
            "webaiprovider.accounts.account.Account.from_browser",
            return_value=browser_account,
        ) as from_browser:
            account = load_or_import_account("chatgpt", path=self.path, require_ignored=False)

        from_browser.assert_called_once_with(
            "chatgpt", browser=None, id="default-browser", name="Chatgpt default browser"
        )
        self.assertEqual((account.provider, account.alias), ("chatgpt", "default-browser"))
        accounts = load_accounts(self.path, require_ignored=False)
        self.assertEqual({item.provider for item in accounts}, {"chatgpt", "claude"})
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_existing_browser_import_is_not_refreshed_or_rewritten(self) -> None:
        self._write(
            json.dumps(
                {
                    "version": 1,
                    "accounts": [
                        {
                            "id": "personal",
                            "provider": "grok",
                            "credentials": {"cookies": {"session": "saved-secret"}},
                        }
                    ],
                }
            )
        )
        original = self.path.read_bytes()
        with patch("webaiprovider.accounts.account.Account.from_browser") as from_browser:
            account = load_or_import_account(
                "grok", "personal", path=self.path, require_ignored=False
            )
        from_browser.assert_not_called()
        self.assertEqual(account.alias, "personal")
        self.assertEqual(self.path.read_bytes(), original)

    def test_browser_import_without_required_auth_cookie_is_not_saved(self) -> None:
        from webaiprovider.accounts.account import Account

        browser_account = Account.from_cookies(
            "chatgpt", {"non_auth_cookie": "must-not-be-saved"}, id="default-browser"
        )
        with patch(
            "webaiprovider.accounts.account.Account.from_browser",
            return_value=browser_account,
        ):
            with self.assertRaises(SafetyError) as caught:
                load_or_import_account("chatgpt", path=self.path, require_ignored=False)
        self.assertNotIn("must-not-be-saved", str(caught.exception))
        self.assertFalse(self.path.exists())

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
    def test_feature_scenarios_use_standard_unittest_cases(self) -> None:
        from tests.live.test_chats import AsyncChatApiTests, ChatApiTests
        from tests.live.test_messages import AsyncMessageApiTests, MessageApiTests
        from tests.live.test_models import AsyncModelApiTests, ModelApiTests
        from tests.live.test_projects import AsyncProjectApiTests, ProjectApiTests
        from tests.live.test_sessions import AsyncSessionApiTests, SessionApiTests

        synchronous = (
            ChatApiTests,
            MessageApiTests,
            ModelApiTests,
            ProjectApiTests,
            SessionApiTests,
        )
        asynchronous = (
            AsyncChatApiTests,
            AsyncMessageApiTests,
            AsyncModelApiTests,
            AsyncProjectApiTests,
            AsyncSessionApiTests,
        )
        self.assertTrue(all(issubclass(case, unittest.TestCase) for case in synchronous))
        self.assertTrue(
            all(issubclass(case, unittest.IsolatedAsyncioTestCase) for case in asynchronous)
        )
        self.assertTrue(all(case.__bases__ == (LiveSyncTestCase,) for case in synchronous))
        self.assertTrue(all(case.__bases__ == (LiveAsyncTestCase,) for case in asynchronous))

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
        from tests.live.test_sessions import SessionApiTests

        with patch.dict(os.environ, {"WEBAI_LIVE_TESTS": "0"}):
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(SessionApiTests)
            result = unittest.TestResult()
            suite.run(result)
        self.assertEqual(len(result.skipped), 1)
        self.assertFalse(result.errors)
        self.assertFalse(result.failures)


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

    def test_provider_error_after_delete_is_verified_against_collection(self) -> None:
        from webaiprovider.errors import ProviderError

        marker = "webai-live-0123456789abcdef0123456789abcdef-project"
        remote = SimpleNamespace(remote_id="temporary-project", name=marker, title=marker)
        session = SimpleNamespace(
            projects=SimpleNamespace(refresh=Mock(return_value=[])),
            get_project=Mock(side_effect=[remote, ProviderError("not found")]),
            delete_project=Mock(),
        )
        context = LiveContext(session, LiveAccount("default-browser", "gemini", {}), "sync", None)
        context.gate = ActionGate(interval=0)
        entry = {
            "kind": "project",
            "markers": [marker],
            "remote_id": remote.remote_id,
            "attempted": True,
        }
        context.state["resources"].append(entry)

        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "cleanup.json"
            with patch("tests.live.support.context.CLEANUP_FILE", journal):
                context.cleanup_entry(entry)

        session.delete_project.assert_called_once_with(remote.remote_id)
        session.projects.refresh.assert_called_once_with()
        self.assertEqual(context.state["resources"], [])
        self.assertFalse(journal.exists())


class LiveSessionLifecycleTests(unittest.TestCase):
    def test_sync_session_closes_even_when_remote_cleanup_cannot_be_confirmed(self) -> None:
        live = LiveSession("sync")
        context = Mock()
        context.state = {"resources": [{"kind": "project"}]}
        context.cleanup_pending.side_effect = CleanupError("pending")
        lock = MagicMock()
        live.context = context
        live.lock = lock

        with self.assertRaisesRegex(CleanupError, "pending"):
            live.close_sync()

        context.close.assert_called_once_with()
        lock.__exit__.assert_called_once_with(None, None, None)


class AsyncLiveSessionLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_session_closes_even_when_remote_cleanup_cannot_be_confirmed(self) -> None:
        live = LiveSession("async")
        context = Mock()
        context.state = {"resources": [{"kind": "project"}]}
        context.cleanup_pending_async = AsyncMock(side_effect=CleanupError("pending"))
        context.aclose = AsyncMock()
        lock = MagicMock()
        live.context = context
        live.lock = lock

        with self.assertRaisesRegex(CleanupError, "pending"):
            await live.close_async()

        context.aclose.assert_awaited_once_with()
        lock.__exit__.assert_called_once_with(None, None, None)
