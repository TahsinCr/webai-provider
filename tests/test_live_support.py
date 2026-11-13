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
        with (
            patch(
                "webaiprovider.accounts.account.Account.from_browser",
                return_value=browser_account,
            ),
            self.assertRaises(SafetyError) as caught,
        ):
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
        from tests.live.support.journal import read_cleanup_journal

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
                self.assertEqual(read_cleanup_journal()[0], state)


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
    def test_chat_id_is_persisted_only_when_it_changes(self) -> None:
        context = LiveContext.__new__(LiveContext)
        context.save = Mock()
        entry = {"remote_id": None}
        chat = SimpleNamespace(remote_id=None)
        context.record_chat_id(entry, chat)
        context.save.assert_not_called()
        chat.remote_id = "first"
        context.record_chat_id(entry, chat)
        context.record_chat_id(entry, chat)
        self.assertEqual(entry["remote_id"], "first")
        context.save.assert_called_once_with()
        chat.remote_id = "second"
        context.record_chat_id(entry, chat)
        self.assertEqual(entry["remote_id"], "second")
        self.assertEqual(context.save.call_count, 2)

    def test_timeout_reports_kind_without_provider_details(self) -> None:
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        from tests.live.support.context import _safe_error_label

        self.assertEqual(
            _safe_error_label(CurlTimeout("private request")), "provider network timeout"
        )
        self.assertEqual(
            _safe_error_label(CurlTimeout("curl: (28) Resolving timed out after 10000 ms")),
            "provider DNS resolution timeout",
        )
        self.assertEqual(
            _safe_error_label(CurlTimeout("curl: (28) Connection timed out after 10000 ms")),
            "provider connection timeout",
        )
        self.assertEqual(_safe_error_label(TimeoutError("private request")), "operation timeout")

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

    async def test_invalid_cursor_stops_recovery_without_deleting(self) -> None:
        entry, _ = self.track()
        entry["remote_id"] = None
        self.provider.list_projects = AsyncMock(return_value=Page((), ""))
        self.provider.get_project = AsyncMock()
        self.provider.delete_project = AsyncMock()
        with self.assertRaisesRegex(CleanupError, "invalid.*cursor"):
            await self.context.cleanup_entry_async(entry)
        self.provider.get_project.assert_not_awaited()
        self.provider.delete_project.assert_not_awaited()
        self.assertEqual(self.provider.list_projects.await_count, 1)

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


class LiveSessionTimeoutTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def timeout_error(label="session.open"):
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        from tests.live.support.errors import LiveApiError

        try:
            raise CurlTimeout("private provider details")
        except CurlTimeout:
            try:
                raise LiveApiError(
                    f"{label} failed after 1000 ms: provider network timeout"
                ) from None
            except LiveApiError as error:
                return error

    def test_sync_retries_only_once_for_open_timeout(self) -> None:
        from tests.live.support.errors import LiveApiError

        live = LiveSession("sync")
        context = object()
        with (
            patch.object(
                live, "_open_sync_once", side_effect=[self.timeout_error(), context]
            ) as run,
            patch("builtins.print"),
        ):
            self.assertIs(live.open_sync(), context)
            self.assertEqual(run.call_count, 2)

        with (
            patch.object(live, "_open_sync_once", side_effect=self.timeout_error()) as run,
            patch("builtins.print"),
            self.assertRaises(LiveApiError),
        ):
            live.open_sync()
        self.assertEqual(run.call_count, 2)

    async def test_cancelled_cookie_refresh_finishes_before_lock_release(self) -> None:
        import threading

        from webaiprovider.errors import AuthenticationError

        started, finish = threading.Event(), threading.Event()
        account = LiveAccount("default", "gemini", {})
        provider = SimpleNamespace(open=AsyncMock(side_effect=AuthenticationError("expired")))
        context = LiveContext(provider, account, "async", gate=ActionGate(interval=0))
        live = LiveSession("async")
        live.context, live.session = context, provider

        def refresh(*args, **kwargs):
            started.set()
            finish.wait(5)
            return account

        with (
            patch.object(live, "_prepare", return_value=context),
            patch.object(live, "_close_async", AsyncMock()),
            patch.object(live, "_release_lock") as release,
            patch("tests.live.support.case.load_or_import_account", side_effect=refresh),
        ):
            task = asyncio.create_task(live.open_async())
            await asyncio.to_thread(started.wait, 5)
            self.assertTrue(started.is_set())
            task.cancel()
            await asyncio.sleep(0)
            release.assert_not_called()
            finish.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            release.assert_called_once()

    async def test_async_retries_only_open_timeout(self) -> None:
        from tests.live.support.errors import LiveApiError

        live = LiveSession("async")
        context = object()
        with (
            patch.object(
                live, "_open_async_once", AsyncMock(side_effect=[self.timeout_error(), context])
            ) as run,
            patch("builtins.print"),
        ):
            self.assertIs(await live.open_async(), context)
            self.assertEqual(run.await_count, 2)

        error = self.timeout_error("session.open.after_cookie_refresh")
        with patch.object(live, "_open_async_once", AsyncMock(side_effect=error)) as run:
            with self.assertRaises(LiveApiError):
                await live.open_async()
            run.assert_awaited_once()

    async def test_async_retries_only_transient_provider_open_errors(self) -> None:
        from tests.live.support.errors import LiveApiError
        from webaiprovider.errors import ProviderError, RateLimited

        def wrapped(exc: Exception) -> LiveApiError:
            try:
                raise exc
            except Exception:
                try:
                    raise LiveApiError(
                        "session.open failed after 1000 ms: provider error"
                    ) from None
                except LiveApiError as error:
                    return error

        live = LiveSession("async")
        context = object()
        transient = wrapped(ProviderError("Provider request failed (HTTP 502)"))
        with (
            patch.object(
                live, "_open_async_once", AsyncMock(side_effect=[transient, context])
            ) as run,
            patch("builtins.print"),
        ):
            self.assertIs(await live.open_async(), context)
            self.assertEqual(run.await_count, 2)

        for non_retryable in (
            wrapped(
                ProviderError("Provider denied access (HTTP 403); the request was not retried")
            ),
            wrapped(RateLimited(5)),
        ):
            with patch.object(
                live, "_open_async_once", AsyncMock(side_effect=non_retryable)
            ) as run:
                with self.assertRaises(LiveApiError):
                    await live.open_async()
                run.assert_awaited_once()


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

    async def test_cancelling_async_prepare_waits_for_worker_and_releases_lock(self) -> None:
        import threading

        started = threading.Event()
        finish = threading.Event()
        released = False
        live = LiveSession("async")

        def slow_prepare():
            started.set()
            finish.wait(1)
            live.lock = SimpleNamespace(release=lambda: None)
            live.context = SimpleNamespace(aclose=AsyncMock())
            return live.context

        def track_release():
            nonlocal released
            released = True

        with (
            patch.object(live, "_prepare", side_effect=slow_prepare),
            patch.object(live, "_release_lock", side_effect=track_release),
        ):
            task = asyncio.create_task(live.open_async())
            await asyncio.to_thread(started.wait, 1)
            task.cancel()
            await asyncio.sleep(0.02)
            finish.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        self.assertTrue(released)


class PerProviderIsolationTests(unittest.TestCase):
    def test_cleanup_and_lock_files_are_isolated_per_provider(self) -> None:
        from tests.live.support.journal import cleanup_file, lock_file

        self.assertNotEqual(cleanup_file("chatgpt"), cleanup_file("grok"))
        self.assertNotEqual(lock_file("chatgpt"), lock_file("grok"))
        self.assertEqual(cleanup_file("chatgpt").name, "live-cleanup-chatgpt.json")
        self.assertEqual(lock_file("grok").name, "live-run-grok.lock")


class BranchRecoverySafetyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.journal = Path(temporary.name) / "live-cleanup-gemini.json"
        for module in ("context", "journal"):
            patcher = patch(f"tests.live.support.{module}.CLEANUP_FILE", self.journal)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.provider = SimpleNamespace()
        self.context = LiveContext(
            self.provider,
            LiveAccount("personal", "gemini", {}),
            "async",
            gate=ActionGate(interval=0),
        )

    async def test_lost_branch_response_recovers_by_marker_without_deleting_parent(self) -> None:
        parent_origin = Origin("gemini", "personal", "c_parent")
        branch_origin = Origin("gemini", "personal", "c_branch")
        marker = f"{self.context.marker}-branch"
        parent_chat = Conversation(parent_origin, f"{self.context.marker}-src")
        branch_chat = Conversation(branch_origin, f"{self.context.marker}-src")

        entry = self.context.new_remote("chat", marker, exclude_ids=("c_parent",))
        entry["markers"].append(f"{self.context.marker}-src")
        entry["attempted"] = True
        self.context.save()

        self.provider.list_chats = AsyncMock(return_value=Page((parent_chat, branch_chat), None))
        self.provider.get_chat = AsyncMock(side_effect=(branch_chat, RemoteNotFound("missing")))
        self.provider.delete_chat = AsyncMock()

        await self.context.cleanup_entry_async(entry)

        self.provider.delete_chat.assert_awaited_once_with("c_branch")
        self.assertFalse(self.journal.exists())

    async def test_lost_branch_response_refuses_guessing_when_zero_or_multiple_matches(
        self,
    ) -> None:
        parent_chat = Conversation(
            Origin("gemini", "personal", "c_parent"), f"{self.context.marker}-src"
        )
        b1 = Conversation(Origin("gemini", "personal", "c_b1"), f"{self.context.marker}-src")
        b2 = Conversation(Origin("gemini", "personal", "c_b2"), f"{self.context.marker}-src")

        for listed in ((parent_chat,), (parent_chat, b1, b2)):
            with self.subTest(listed_count=len(listed)):
                self.context.state["resources"].clear()
                entry = self.context.new_remote(
                    "chat", f"{self.context.marker}-src", exclude_ids=("c_parent",)
                )
                entry["attempted"] = True
                self.context.save()
                self.provider.list_chats = AsyncMock(return_value=Page(listed, None))
                self.provider.get_chat = AsyncMock()
                self.provider.delete_chat = AsyncMock()

                with self.assertRaisesRegex(CleanupError, "uniquely located"):
                    await self.context.cleanup_entry_async(entry)
                self.provider.delete_chat.assert_not_awaited()
                self.assertEqual(len(self.context.state["resources"]), 1)
                self.assertTrue(self.journal.exists())

    async def test_branch_entry_never_deletes_excluded_parent_id(self) -> None:
        parent_chat = Conversation(
            Origin("gemini", "personal", "c_parent"), f"{self.context.marker}-src"
        )
        entry = self.context.new_remote(
            "chat", f"{self.context.marker}-src", exclude_ids=("c_parent",)
        )
        entry["attempted"] = True
        entry["remote_id"] = "c_parent"
        self.context.save()
        self.provider.get_chat = AsyncMock(return_value=parent_chat)
        self.provider.delete_chat = AsyncMock()

        with self.assertRaisesRegex(CleanupError, "excluded"):
            await self.context.cleanup_entry_async(entry)
        self.provider.delete_chat.assert_not_awaited()
        self.assertTrue(self.journal.exists())


class RateLimitAndUncertainMutationTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_rate_limit_without_remote_id_preserves_attempted_and_journal(self) -> None:
        from webaiprovider.errors import RateLimited

        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "live-cleanup-chatgpt.json"
            with (
                patch("tests.live.support.context.CLEANUP_FILE", journal),
                patch("tests.live.support.journal.CLEANUP_FILE", journal),
            ):
                case = LiveAsyncTestCase("runTest")
                provider = SimpleNamespace()
                case.context = LiveContext(
                    provider,
                    LiveAccount("personal", "chatgpt", {}),
                    "async",
                    gate=ActionGate(interval=0),
                )
                entry = case.context.new_remote("chat", f"{case.context.marker}-chat")
                entry["attempted"] = True
                case.context.save()
                chat = SimpleNamespace(remote_id=None)

                async def rate_limited_send():
                    raise RateLimited(5.0)

                with self.assertRaises(unittest.SkipTest):
                    await case.call_or_skip_rate_limit(
                        "messages.create.stream", rate_limited_send, entry=entry, chat=chat
                    )

                self.assertTrue(entry["attempted"])
                self.assertTrue(journal.exists())
                saved = json.loads(journal.read_text(encoding="utf-8"))
                self.assertTrue(saved["resources"][0]["attempted"])

    def test_sync_rate_limit_without_remote_id_preserves_attempted_and_journal(self) -> None:
        from webaiprovider.errors import RateLimited

        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "live-cleanup-chatgpt.json"
            with (
                patch("tests.live.support.context.CLEANUP_FILE", journal),
                patch("tests.live.support.journal.CLEANUP_FILE", journal),
            ):
                case = LiveSyncTestCase("runTest")
                provider = SimpleNamespace()
                case.context = LiveContext(
                    provider,
                    LiveAccount("personal", "chatgpt", {}),
                    "sync",
                    gate=ActionGate(interval=0),
                )
                entry = case.context.new_remote("chat", f"{case.context.marker}-chat")
                entry["attempted"] = True
                case.context.save()
                chat = SimpleNamespace(remote_id="chat-late-id")

                def rate_limited_send():
                    raise RateLimited(5.0)

                with self.assertRaises(unittest.SkipTest):
                    case.call_or_skip_rate_limit(
                        "messages.create.stream", rate_limited_send, entry=entry, chat=chat
                    )

                self.assertTrue(entry["attempted"])
                self.assertEqual(entry["remote_id"], "chat-late-id")
                saved = json.loads(journal.read_text(encoding="utf-8"))
                self.assertTrue(saved["resources"][0]["attempted"])
                self.assertEqual(saved["resources"][0]["remote_id"], "chat-late-id")

    async def test_timeout_or_cancel_without_remote_id_keeps_attempted_and_triggers_recovery(
        self,
    ) -> None:
        from tests.live.support.errors import LiveApiError

        for failure, expected in (
            (TimeoutError("timed out"), LiveApiError),
            (asyncio.CancelledError(), asyncio.CancelledError),
        ):
            with (
                self.subTest(failure=type(failure).__name__),
                tempfile.TemporaryDirectory() as directory,
            ):
                journal = Path(directory) / "live-cleanup-gemini.json"
                with (
                    patch("tests.live.support.context.CLEANUP_FILE", journal),
                    patch("tests.live.support.journal.CLEANUP_FILE", journal),
                ):
                    provider = SimpleNamespace()
                    context = LiveContext(
                        provider,
                        LiveAccount("personal", "gemini", {}),
                        "async",
                        gate=ActionGate(interval=0),
                    )
                    entry = context.new_remote("chat", f"{context.marker}-chat")
                    entry["attempted"] = True
                    context.save()

                    async def failing_send(exc=failure):
                        raise exc

                    with self.assertRaises(expected):
                        await context.call_async("messages.send", failing_send)

                    self.assertTrue(entry["attempted"])
                    self.assertTrue(journal.exists())
                    remote = Conversation(
                        Origin("gemini", "personal", "c_orphan"), f"{context.marker}-chat"
                    )
                    provider.list_chats = AsyncMock(return_value=Page((remote,), None))
                    provider.get_chat = AsyncMock(side_effect=(remote, RemoteNotFound("missing")))
                    provider.delete_chat = AsyncMock()
                    await context.cleanup_pending_async()
                    provider.delete_chat.assert_awaited_once_with("c_orphan")
                    self.assertFalse(journal.exists())


class LegacyAndPerProviderRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_journal_routes_to_owning_provider_without_touching_others(self) -> None:
        from tests.live.support.journal import pending_cleanup_files, read_cleanup_journal

        with tempfile.TemporaryDirectory() as directory:
            cfg = Path(directory) / ".config"
            cfg.mkdir(mode=0o700)
            legacy = cfg / "live-cleanup.json"
            claude_journal = cfg / "live-cleanup-claude.json"
            gemini_run = "webai-live-" + "a" * 32
            claude_run = "webai-live-" + "b" * 32
            gemini_state = {
                "version": 1,
                "provider": "gemini",
                "account": "personal",
                "run_id": gemini_run,
                "resources": [
                    {
                        "kind": "chat",
                        "markers": [f"{gemini_run}-chat"],
                        "remote_id": "c_gem",
                        "attempted": True,
                    }
                ],
            }
            claude_state = {
                "version": 1,
                "provider": "claude",
                "account": "personal",
                "run_id": claude_run,
                "resources": [
                    {
                        "kind": "chat",
                        "markers": [f"{claude_run}-chat"],
                        "remote_id": "c_cl",
                        "attempted": True,
                    }
                ],
            }
            write_private_json(legacy, gemini_state)
            write_private_json(claude_journal, claude_state)

            with (
                patch("tests.live.support.journal.CONFIG_DIR", cfg),
                patch("tests.live.support.journal.CLEANUP_FILE", legacy),
                patch("tests.live.support.journal._DEFAULT_CLEANUP_FILE", legacy),
                patch("tests.live.support.config._require_ignored"),
                patch("tests.live.support.context.CLEANUP_FILE", legacy),
                patch("tests.live.support.context._DEFAULT_CLEANUP_FILE", legacy),
            ):
                self.assertEqual(set(pending_cleanup_files()), {legacy, claude_journal})
                self.assertEqual(read_cleanup_journal("chatgpt"), (None, None))
                loaded_state, loaded_path = read_cleanup_journal("gemini")
                self.assertEqual(loaded_path, legacy)
                self.assertEqual(loaded_state, gemini_state)

                provider = SimpleNamespace(
                    get_chat=AsyncMock(
                        side_effect=(
                            Conversation(
                                Origin("gemini", "personal", "c_gem"),
                                f"{gemini_run}-chat",
                            ),
                            RemoteNotFound("missing"),
                        )
                    ),
                    delete_chat=AsyncMock(),
                )
                context = LiveContext(
                    provider,
                    LiveAccount("personal", "gemini", {}),
                    "async",
                    state=loaded_state,
                    journal_path=loaded_path,
                    gate=ActionGate(interval=0),
                )
                await context.cleanup_pending_async()
                self.assertFalse(legacy.exists())
                self.assertTrue(claude_journal.exists())

    def test_legacy_journal_mismatch_or_corruption_refuses_silent_deletion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cfg = Path(directory) / ".config"
            cfg.mkdir(mode=0o700)
            legacy = cfg / "live-cleanup.json"
            state = {
                "version": 1,
                "provider": "gemini",
                "account": "work",
                "run_id": "webai-live-" + "c" * 32,
                "resources": [],
            }
            write_private_json(legacy, state)

            live = LiveSession("async")
            with (
                patch.dict(
                    os.environ,
                    {
                        "WEBAI_LIVE_PROVIDER": "gemini",
                        "WEBAI_LIVE_ACCOUNT": "personal",
                        "WEBAI_LIVE_RECOVER": "1",
                    },
                ),
                patch("tests.live.support.journal.CONFIG_DIR", cfg),
                patch("tests.live.support.journal.CLEANUP_FILE", legacy),
                patch("tests.live.support.journal._DEFAULT_CLEANUP_FILE", legacy),
                patch("tests.live.support.config._require_ignored"),
                patch(
                    "tests.live.support.case.load_or_import_account",
                    return_value=LiveAccount("personal", "gemini", {"cookies": {"sid": "x"}}),
                ),
                patch("tests.live.support.case.RunLock", return_value=MagicMock()),
                self.assertRaisesRegex(SafetyError, "different account"),
            ):
                live._prepare()
            self.assertTrue(legacy.exists())

            legacy.write_text("{corrupt", encoding="utf-8")
            with (
                patch.dict(os.environ, {"WEBAI_LIVE_PROVIDER": "gemini"}),
                patch("tests.live.support.journal.CONFIG_DIR", cfg),
                patch("tests.live.support.journal.CLEANUP_FILE", legacy),
                patch("tests.live.support.journal._DEFAULT_CLEANUP_FILE", legacy),
                patch("tests.live.support.config._require_ignored"),
            ):
                from tests.live.support.journal import read_cleanup_journal

                with self.assertRaises(SafetyError):
                    read_cleanup_journal("gemini")
            self.assertTrue(legacy.exists())


class CredentialLockAndConcurrencyTests(unittest.TestCase):
    def test_concurrent_account_imports_preserve_all_providers(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        from webaiprovider.accounts.account import Account
        from webaiprovider.accounts.filters import REQUIRED_PROVIDER_COOKIES

        with tempfile.TemporaryDirectory() as directory:
            cfg = Path(directory) / ".config"
            cfg.mkdir(mode=0o700)
            creds = cfg / "credentials.json"

            def fake_from_browser(provider, *, browser=None, id="", name=""):
                origin_url = {
                    "chatgpt": "https://chatgpt.com",
                    "claude": "https://claude.ai",
                    "gemini": "https://gemini.google.com",
                    "grok": "https://grok.com",
                }[provider]
                cookie_name = REQUIRED_PROVIDER_COOKIES[origin_url]
                return Account.from_cookies(
                    provider, {cookie_name: f"secret-{provider}"}, id=id, name=name
                )

            with patch(
                "webaiprovider.accounts.account.Account.from_browser",
                side_effect=fake_from_browser,
            ):
                providers = ("chatgpt", "claude", "gemini", "grok")
                with ThreadPoolExecutor(max_workers=4) as pool:
                    results = list(
                        pool.map(
                            lambda p: load_or_import_account(
                                p, "default-browser", path=creds, require_ignored=False
                            ),
                            providers,
                        )
                    )
                self.assertEqual(len(results), 4)
                loaded = load_accounts(creds, require_ignored=False)
                self.assertEqual({item.provider for item in loaded}, set(providers))

    def test_windows_msvcrt_lock_and_lockless_fallback(self) -> None:
        import sys

        from tests.live.support.config import _credentials_lock, supports_process_credentials_lock

        calls: list[tuple[int, int]] = []
        attempts = {"count": 0}

        class FakeMsvcrt:
            LK_NBLCK = 2
            LK_UNLCK = 0

            @staticmethod
            def locking(fd: int, mode: int, nbytes: int) -> None:
                if mode == 2 and attempts["count"] == 0:
                    attempts["count"] += 1
                    raise OSError("locked")
                calls.append((mode, nbytes))

        with tempfile.TemporaryDirectory() as directory:
            creds = Path(directory) / ".config" / "credentials.json"
            with (
                patch("tests.live.support.config.os.name", "nt"),
                patch.dict(sys.modules, {"msvcrt": FakeMsvcrt}),
                patch("tests.live.support.config.time.sleep"),
            ):
                self.assertTrue(supports_process_credentials_lock())
                with _credentials_lock(creds):
                    self.assertTrue((creds.parent / ".credentials.lock").exists())
                self.assertEqual(calls, [(FakeMsvcrt.LK_NBLCK, 1), (FakeMsvcrt.LK_UNLCK, 1)])

            with (
                patch("tests.live.support.config.os.name", "java"),
                patch("tests.live.support.config.time.sleep"),
            ):
                self.assertFalse(supports_process_credentials_lock())
                lock_path = creds.parent / ".credentials.lock"
                lock_path.unlink(missing_ok=True)
                with _credentials_lock(creds):
                    self.assertTrue(lock_path.exists())
                self.assertFalse(lock_path.exists())


class ActionGateSharedBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_gate_state_enforces_spacing_and_combined_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gate_file = Path(directory) / "gate-chatgpt.json"
            gate_one = ActionGate(interval=1.0, limit=3, state_file=gate_file)
            gate_two = ActionGate(interval=1.0, limit=3, state_file=gate_file)
            now = {"mono": 100.0, "epoch": 1000.0}
            slept: list[float] = []

            def fake_sleep(delay: float) -> None:
                slept.append(delay)
                now["mono"] += delay
                now["epoch"] += delay

            async def fake_async_sleep(delay: float) -> None:
                slept.append(delay)
                now["mono"] += delay
                now["epoch"] += delay

            with (
                patch("tests.live.support.actions.time.monotonic", side_effect=lambda: now["mono"]),
                patch("tests.live.support.actions.time.time", side_effect=lambda: now["epoch"]),
                patch("tests.live.support.actions.time.sleep", side_effect=fake_sleep),
                patch("tests.live.support.actions.asyncio.sleep", side_effect=fake_async_sleep),
            ):
                self.assertEqual(gate_one.call_sync(lambda: "one"), "one")
                now["mono"] += 0.2
                now["epoch"] += 0.2
                self.assertEqual(gate_two.call_sync(lambda: "two"), "two")
                self.assertEqual(len(slept), 1)
                self.assertAlmostEqual(slept[0], 0.8, places=2)

                await gate_two.call_async(AsyncMock(return_value="three"))
                with self.assertRaisesRegex(SafetyError, "API-action limit"):
                    gate_one.call_sync(lambda: "four")

    async def test_temporary_chat_cleanup_routes_delete_through_action_gate(self) -> None:
        from tests.live.test_messages import AsyncMessageApiTests
        from webaiprovider.base import Capability

        gate = ActionGate(interval=0, limit=10)
        deleted = AsyncMock()
        chat = SimpleNamespace(
            temporary=True,
            remote_id="tmp-1",
            messages=SimpleNamespace(
                create=lambda *_a, **_kw: SimpleNamespace(
                    start=AsyncMock(
                        return_value=SimpleNamespace(message=SimpleNamespace(content="OK"))
                    )
                )
            ),
            delete=deleted,
        )
        session = SimpleNamespace(
            capabilities={Capability.CHAT, Capability.TEMPORARY},
            chats=SimpleNamespace(
                create=AsyncMock(return_value=chat),
                refresh=AsyncMock(return_value=[]),
            ),
        )
        test = AsyncMessageApiTests("test_incognito_temporary_chat")
        test.context = LiveContext(
            session, LiveAccount("personal", "chatgpt", {}), "async", gate=gate
        )
        await test.test_incognito_temporary_chat()
        deleted.assert_awaited_once()
        self.assertEqual(gate.calls, 3)

    def test_live_case_records_passed_skipped_rate_limited_and_setup_failed_outcomes(
        self,
    ) -> None:
        import tests.live.support.case as case_module
        from webaiprovider.errors import RateLimited

        class SampleAsyncCase(LiveAsyncTestCase):
            async def test_pass(self) -> None:
                self.context.skip("optional: unsupported")

            async def test_unsupported(self) -> None:
                self.skipTest("unsupported")

            async def test_rate_limited(self) -> None:
                async def boom():
                    raise RateLimited(1.0)

                await self.call_or_skip_rate_limit("sample.op", boom)

        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "report.json"

            def fake_live_session(_api: str):
                ctx = LiveContext(
                    SimpleNamespace(),
                    LiveAccount("personal", "grok", {}),
                    "async",
                    gate=ActionGate(interval=0),
                )
                return SimpleNamespace(
                    open_async=AsyncMock(return_value=ctx),
                    close_async=AsyncMock(),
                )

            def failing_live_session(_api: str):
                return SimpleNamespace(
                    open_async=AsyncMock(side_effect=RuntimeError("session open boom")),
                    close_async=AsyncMock(),
                )

            with (
                patch.dict(
                    os.environ,
                    {
                        "WEBAI_LIVE_TESTS": "1",
                        "WEBAI_LIVE_PROVIDER": "grok",
                        "WEBAI_LIVE_API": "async",
                        "WEBAI_LIVE_REPORT_FILE": str(report_path),
                    },
                    clear=True,
                ),
                patch.object(case_module, "_FAILED_SESSION_OPENS", set()),
            ):
                result = unittest.TestResult()
                suite = unittest.TestSuite(
                    [
                        SampleAsyncCase("test_pass"),
                        SampleAsyncCase("test_unsupported"),
                        SampleAsyncCase("test_rate_limited"),
                    ]
                )
                with patch("tests.live.support.case.LiveSession", side_effect=fake_live_session):
                    suite.run(result)

                setup_suite = unittest.TestSuite(
                    [
                        SampleAsyncCase("test_pass"),
                        SampleAsyncCase("test_unsupported"),
                    ]
                )
                with patch("tests.live.support.case.LiveSession", side_effect=failing_live_session):
                    setup_suite.run(result)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(
                [(item["status"], item["feature_skips"]) for item in report["tests"]],
                [
                    ("passed", ["optional: unsupported"]),
                    ("skipped_unsupported", []),
                    ("skipped_rate_limited", ["sample.op: rate_limited"]),
                    ("failed", []),
                    ("skipped_setup_failed", []),
                ],
            )
