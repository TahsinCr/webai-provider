from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

from tests.live.support.actions import ActionGate
from tests.live.support.case import (
    _FAILED_SESSION_OPENS,
    LiveAsyncTestCase,
    LiveSession,
    LiveSyncTestCase,
    _skip_after_session_open_failure,
)
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
from webaiprovider import Account, Provider
from webaiprovider.collections import Page
from webaiprovider.errors import ProviderError, RemoteNotFound
from webaiprovider.resources.chat.types import Conversation
from webaiprovider.resources.common import Origin
from webaiprovider.resources.message.types import Content, MessageData
from webaiprovider.resources.project.types import ProjectData


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
        browser_cookies = {
            f"{chatgpt_auth_cookie}.0": "browser-secret-0",
            f"{chatgpt_auth_cookie}.1": "browser-secret-1",
        }
        browser_account = Account.from_cookies("chatgpt", browser_cookies, id="default-browser")
        with patch(
            "webaiprovider.accounts.account.Account.from_browser",
            return_value=browser_account,
        ) as from_browser:
            account = load_or_import_account("chatgpt", path=self.path, require_ignored=False)

        from_browser.assert_called_once_with(
            "chatgpt", browser=None, id="default-browser", name="Chatgpt default browser"
        )
        self.assertEqual((account.provider, account.alias), ("chatgpt", "default-browser"))
        self.assertEqual(account.credentials, browser_cookies)
        accounts = load_accounts(self.path, require_ignored=False)
        self.assertEqual({item.provider for item in accounts}, {"chatgpt", "claude"})
        self.assertEqual(
            select_account(accounts, "chatgpt", "default-browser").credentials,
            browser_cookies,
        )
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

    def test_refreshes_invalid_browser_cookies_without_replacing_other_accounts(self) -> None:
        from webaiprovider.accounts.account import Account
        from webaiprovider.accounts.filters import REQUIRED_PROVIDER_COOKIES

        self._write(
            json.dumps(
                {
                    "version": 1,
                    "accounts": [
                        {
                            "id": "default-browser",
                            "provider": "chatgpt",
                            "credentials": {"cookies": {"session": "expired"}},
                        },
                        {
                            "id": "personal",
                            "provider": "claude",
                            "credentials": {"cookies": {"session": "preserved"}},
                        },
                    ],
                }
            )
        )
        auth_cookie = REQUIRED_PROVIDER_COOKIES["https://chatgpt.com"]
        browser_account = Account.from_cookies(
            "chatgpt", {auth_cookie: "fresh"}, id="default-browser"
        )
        with patch(
            "webaiprovider.accounts.account.Account.from_browser",
            return_value=browser_account,
        ) as from_browser:
            account = load_or_import_account(
                "chatgpt",
                "default-browser",
                refresh=True,
                path=self.path,
                require_ignored=False,
            )

        from_browser.assert_called_once()
        self.assertEqual(account.credentials[auth_cookie], "fresh")
        saved = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(len(saved["accounts"]), 2)
        self.assertEqual(saved["accounts"][0]["credentials"], account.credentials)
        self.assertEqual(saved["accounts"][1]["credentials"], {"cookies": {"session": "preserved"}})
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_automatic_cookie_refresh_does_not_replace_named_accounts(self) -> None:
        self._write(
            json.dumps(
                {
                    "version": 1,
                    "accounts": [
                        {
                            "id": "personal",
                            "provider": "grok",
                            "credentials": {"cookies": {"session": "saved"}},
                        }
                    ],
                }
            )
        )
        with (
            patch("webaiprovider.accounts.account.Account.from_browser") as from_browser,
            self.assertRaisesRegex(SafetyError, "default-browser"),
        ):
            load_or_import_account(
                "grok", "personal", refresh=True, path=self.path, require_ignored=False
            )
        from_browser.assert_not_called()
        self.assertEqual(load_accounts(self.path, require_ignored=False)[0].alias, "personal")

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

    def test_cleanup_journal_accepts_live_marker_formats(self) -> None:
        from tests.live.support.journal import read_cleanup_state

        run_id = "webai-live-" + "0" * 32
        state = {
            "version": 1,
            "provider": "chatgpt",
            "account": "default-browser",
            "run_id": run_id,
            "resources": [
                {
                    "kind": "chat",
                    "markers": [f"{run_id}-chat", f"{run_id}-chat-renamed"],
                    "remote_id": "chat-id",
                    "attempted": True,
                },
                {
                    "kind": "project",
                    "markers": [f"{run_id}-p", f"{run_id}-r"],
                    "remote_id": "project-id",
                    "attempted": True,
                },
            ],
        }
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "live-cleanup.json"
            journal.touch()
            with (
                patch("tests.live.support.journal.CLEANUP_FILE", journal),
                patch("tests.live.support.journal.read_private_json", return_value=state),
            ):
                self.assertEqual(read_cleanup_state(), state)


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


class LiveContextErrorTests(unittest.TestCase):
    def test_chatgpt_http_error_reports_status_without_request_details(self) -> None:
        from tests.live.support.context import _safe_error_label
        from webaiprovider.providers.chatgpt.errors import ChatGPTError

        error = ChatGPTError("ChatGPT returned HTTP 400 for PATCH /backend-api/projects/private-id")
        summary = _safe_error_label(error)

        self.assertEqual(summary, "ChatGPTError (HTTP 400)")
        self.assertNotIn("private-id", summary)


class CleanupSafetyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.journal = Path(temporary.name) / "cleanup.json"
        for module in ("context", "journal"):
            patcher = patch(f"tests.live.support.{module}.CLEANUP_FILE", self.journal)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.provider = SimpleNamespace()
        self.context = LiveContext(
            self.provider,
            LiveAccount("personal", "chatgpt", {}),
            "async",
            gate=ActionGate(interval=0),
        )
        self.origin = Origin("chatgpt", "personal", "temporary-id")

    def track(self, kind="project"):
        marker = f"{self.context.marker}-p" if kind == "project" else f"{self.context.marker}-chat"
        remote = (
            ProjectData(self.origin, marker)
            if kind == "project"
            else Conversation(self.origin, marker)
        )
        entry = self.context.new_remote(kind, marker)
        entry.update(attempted=True, remote_id=self.origin.remote_id)
        self.context.save()
        return entry, remote

    async def test_unrelated_marker_or_remote_id_prevents_deletion(self) -> None:
        entry, tracked = self.track()
        self.provider.delete_project = AsyncMock()
        for remote in (
            ProjectData(self.origin, "Unrelated"),
            ProjectData(Origin("chatgpt", "personal", "another-id"), tracked.name),
        ):
            with self.subTest(remote=remote):
                self.provider.get_project = AsyncMock(return_value=remote)
                with self.assertRaisesRegex(CleanupError, "unique test marker"):
                    await self.context.cleanup_entry_async(entry)
                self.provider.delete_project.assert_not_awaited()
                self.assertEqual(self.context.state["resources"], [entry])
                self.assertTrue(self.journal.exists())

    async def test_uncertain_delete_is_confirmed_without_replaying_the_mutation(self) -> None:
        entry, remote = self.track()
        self.provider.get_project = AsyncMock(side_effect=(remote, ProviderError("uncertain")))
        self.provider.delete_project = AsyncMock(side_effect=ProviderError("uncertain"))
        self.provider.list_projects = AsyncMock(return_value=Page(()))
        await self.context.cleanup_entry_async(entry)
        self.provider.delete_project.assert_awaited_once_with(self.origin.remote_id)
        self.assertEqual(self.context.state["resources"], [])
        self.assertFalse(self.journal.exists())

    async def test_project_recovery_paginates_and_uses_native_origin_id(self) -> None:
        entry, remote = self.track()
        entry["remote_id"] = None
        self.provider.list_projects = AsyncMock(
            side_effect=(Page((), "next-page"), Page((remote,)))
        )
        self.provider.get_project = AsyncMock(side_effect=(remote, RemoteNotFound("missing")))
        self.provider.delete_project = AsyncMock()
        await self.context.cleanup_entry_async(entry)
        self.assertEqual(
            self.provider.list_projects.await_args_list, [call(None), call("next-page")]
        )
        self.provider.delete_project.assert_awaited_once_with(self.origin.remote_id)
        self.assertFalse(self.journal.exists())

    async def test_later_pages_prevent_false_deletion_confirmation(self) -> None:
        for kind in ("chat", "project"):
            with self.subTest(kind=kind):
                self.context.state["resources"].clear()
                entry, remote = self.track(kind)
                delete = AsyncMock()
                listing = AsyncMock(side_effect=(Page((), "next-page"), Page((remote,))))
                setattr(
                    self.provider,
                    f"get_{kind}",
                    AsyncMock(side_effect=(remote, ProviderError("uncertain"))),
                )
                setattr(self.provider, f"delete_{kind}", delete)
                setattr(self.provider, "list_chats" if kind == "chat" else "list_projects", listing)
                with self.assertRaisesRegex(CleanupError, "did not confirm deletion"):
                    await self.context.cleanup_entry_async(entry)
                delete.assert_awaited_once_with(self.origin.remote_id)
                self.assertEqual(listing.await_args_list, [call(None), call("next-page")])
                self.assertEqual(self.context.state["resources"], [entry])
                self.assertTrue(self.journal.exists())

    async def test_repeated_cursor_stops_recovery_without_deleting(self) -> None:
        entry, _ = self.track()
        entry["remote_id"] = None
        self.provider.list_projects = AsyncMock(return_value=Page((), "same-page"))
        self.provider.get_project = AsyncMock()
        self.provider.delete_project = AsyncMock()
        with self.assertRaisesRegex(CleanupError, "repeated cursor"):
            await self.context.cleanup_entry_async(entry)
        self.provider.delete_project.assert_not_awaited()
        self.provider.get_project.assert_not_awaited()
        self.assertEqual(self.provider.list_projects.await_count, 2)

    async def test_chat_marker_in_history_allows_exact_cleanup(self) -> None:
        entry, remote = self.track("chat")
        remote = Conversation(
            self.origin,
            "Automatic title",
            (MessageData("user-id", "user", (Content("text", entry["markers"][0]),)),),
        )
        self.provider.get_chat = AsyncMock(side_effect=(remote, RemoteNotFound("missing")))
        self.provider.delete_chat = AsyncMock()
        await self.context.cleanup_entry_async(entry)
        self.provider.delete_chat.assert_awaited_once_with(self.origin.remote_id)
        self.assertFalse(self.journal.exists())


class SyncCleanupTests(unittest.TestCase):
    def test_cleanup_runs_shared_core_on_the_existing_provider_portal(self) -> None:
        provider = Provider(Account(id="personal", provider="chatgpt", credentials={}))
        self.addCleanup(provider.close)
        native = provider._provider
        context = LiveContext(
            provider, LiveAccount("personal", "chatgpt", {}), "sync", gate=ActionGate(interval=0)
        )
        marker = f"{context.marker}-p"
        origin = Origin("chatgpt", "personal", "temporary-id")
        native.get_project = AsyncMock(
            side_effect=(ProjectData(origin, marker), RemoteNotFound("missing"))
        )
        native.delete_project = AsyncMock()
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "cleanup.json"
            with (
                patch("tests.live.support.context.CLEANUP_FILE", journal),
                patch("tests.live.support.journal.CLEANUP_FILE", journal),
            ):
                entry = context.new_remote("project", marker)
                entry.update(attempted=True, remote_id=origin.remote_id)
                context.cleanup_entry(entry)
                context.cleanup_pending()
                self.assertFalse(journal.exists())
        native.delete_project.assert_awaited_once_with(origin.remote_id)
        self.assertEqual(native.get_project.await_count, 2)
        self.assertEqual(context.state["resources"], [])


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


class LiveSessionSetupCacheTests(unittest.TestCase):
    def test_failed_setup_skips_only_the_same_provider_api_pair(self) -> None:
        key = ("gemini", "async")
        with patch.dict(os.environ, {"WEBAI_LIVE_PROVIDER": key[0]}):
            _FAILED_SESSION_OPENS.add(key)
            try:
                with self.assertRaisesRegex(unittest.SkipTest, "no further provider requests"):
                    _skip_after_session_open_failure(key[1])
                _skip_after_session_open_failure("sync")
            finally:
                _FAILED_SESSION_OPENS.discard(key)


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


class LiveSessionCookieRefreshTests(unittest.TestCase):
    def test_sync_auth_failure_refreshes_default_browser_cookies_once(self) -> None:
        from contextlib import redirect_stdout
        from io import StringIO

        from webaiprovider.errors import AccountPaused

        account = LiveAccount("default-browser", "gemini", {"cookies": {"sid": "old"}})
        refreshed_account = LiveAccount("default-browser", "gemini", {"cookies": {"sid": "new"}})
        old_provider = Mock()
        old_provider.open.side_effect = AccountPaused("expired")
        new_provider = Mock()
        old_context = LiveContext(old_provider, account, "sync", gate=ActionGate(interval=0))
        live = LiveSession("sync")
        live.context, live.session = old_context, old_provider
        gate = ActionGate(interval=0, limit=4)

        with (
            patch("tests.live.support.case._ACTION_GATE", gate),
            patch.object(live, "_prepare", return_value=old_context),
            patch.object(live, "_create_provider_session", return_value=new_provider),
            patch(
                "tests.live.support.case.load_or_import_account", return_value=refreshed_account
            ) as refresh,
            redirect_stdout(StringIO()),
        ):
            context = live.open_sync()
            refresh.assert_called_once()
            self.assertEqual(refresh.call_args.kwargs["refresh"], True)
            self.assertEqual(old_context.gate.calls + gate.calls, 2)
            self.assertIs(context.session, new_provider)
            live.close_sync()

    def test_sync_network_failure_does_not_refresh_cookies(self) -> None:
        from webaiprovider.errors import NetworkError

        account = LiveAccount("default-browser", "gemini", {"cookies": {"sid": "saved"}})
        provider = Mock()
        provider.open.side_effect = NetworkError("unavailable")
        context = LiveContext(provider, account, "sync", gate=ActionGate(interval=0))
        live = LiveSession("sync")
        live.context, live.session = context, provider

        from tests.live.support.errors import LiveApiError

        with (
            patch("tests.live.support.case._ACTION_GATE", ActionGate(interval=0)),
            patch.object(live, "_prepare", return_value=context),
            patch("tests.live.support.case.load_or_import_account") as refresh,
            self.assertRaises(LiveApiError),
        ):
            live.open_sync()
        refresh.assert_not_called()
        self.assertIsNone(live.context)


class AsyncLiveSessionCookieRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_auth_failure_refreshes_default_browser_cookies_once(self) -> None:
        from contextlib import redirect_stdout
        from io import StringIO

        from webaiprovider.errors import AccountPaused

        account = LiveAccount("default-browser", "gemini", {"cookies": {"sid": "old"}})
        refreshed_account = LiveAccount("default-browser", "gemini", {"cookies": {"sid": "new"}})
        old_provider = Mock()
        old_provider.open = AsyncMock(side_effect=AccountPaused("expired"))
        old_provider.close = AsyncMock()
        new_provider = Mock()
        new_provider.open = AsyncMock(return_value=None)
        new_provider.close = AsyncMock()
        old_context = LiveContext(old_provider, account, "async", gate=ActionGate(interval=0))
        live = LiveSession("async")
        live.context, live.session = old_context, old_provider
        gate = ActionGate(interval=0, limit=4)

        with (
            patch("tests.live.support.case._ACTION_GATE", gate),
            patch.object(live, "_prepare", return_value=old_context),
            patch.object(live, "_create_provider_session", return_value=new_provider),
            patch(
                "tests.live.support.case.load_or_import_account", return_value=refreshed_account
            ) as refresh,
            redirect_stdout(StringIO()),
        ):
            context = await live.open_async()
            refresh.assert_called_once()
            self.assertEqual(refresh.call_args.kwargs["refresh"], True)
            self.assertEqual(old_context.gate.calls + gate.calls, 2)
            self.assertIs(context.session, new_provider)
            await live.close_async()
