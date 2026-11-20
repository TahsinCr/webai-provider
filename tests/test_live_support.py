from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

from tests.live.support.actions import ActionGate
from tests.live.support.case import (
    _FAILED_SESSION_OPENS,
    LiveAsyncTestCase,
    LiveSession,
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
from webaiprovider.collections import Page
from webaiprovider.errors import ProviderError, RateLimited, RemoteNotFound
from webaiprovider.resources.chat.types import Conversation
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
        if os.name == "posix":
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
        if os.name == "posix":
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
        from tests.live.test_chats import AsyncChatApiTests
        from tests.live.test_messages import AsyncMessageApiTests
        from tests.live.test_models import AsyncModelApiTests
        from tests.live.test_projects import AsyncProjectApiTests
        from tests.live.test_sessions import AsyncSessionApiTests

        asynchronous = (
            AsyncChatApiTests,
            AsyncMessageApiTests,
            AsyncModelApiTests,
            AsyncProjectApiTests,
            AsyncSessionApiTests,
        )
        self.assertTrue(
            all(issubclass(case, unittest.IsolatedAsyncioTestCase) for case in asynchronous)
        )
        self.assertTrue(all(case.__bases__ == (LiveAsyncTestCase,) for case in asynchronous))

    def test_select_live_models_keeps_only_latest_claude_5x_per_family(self) -> None:
        from tests.live.test_models import _select_live_models
        from webaiprovider.resources.model.model import Model

        claude_models = [
            Model("claude-fable-5-2", "Claude Fable 5.2"),
            Model("claude-opus-5-5", "Claude Opus 5.5"),
            Model("claude-sonnet-5-5", "Claude Sonnet 5.5"),
            Model("claude-haiku-5-5", "Claude Haiku 5.5"),
            Model("claude-fable-5-1", "Claude Fable 5.1"),
            Model("claude-fable-5", "Claude Fable 5"),
            Model("claude-opus-5", "Claude Opus 5"),
            Model("claude-opus-4-8", "Claude Opus 4.8"),
            Model("claude-3-opus-20240229", "Claude Opus 3"),
            Model("claude-sonnet-5", "Claude Sonnet 5"),
            Model("claude-sonnet-4-6", "Claude Sonnet 4.6"),
        ]
        selected = _select_live_models("claude", claude_models)
        self.assertEqual(
            [item.id for item in selected],
            [
                "claude-fable-5-2",
                "claude-opus-5-5",
                "claude-sonnet-5-5",
                "claude-haiku-5-5",
            ],
        )

        free_tier_models = [
            Model("claude-sonnet-5-5", "Claude Sonnet 5.5"),
            Model("claude-haiku-5-5", "Claude Haiku 5.5"),
            Model("claude-sonnet-5", "Claude Sonnet 5"),
            Model("claude-sonnet-4-6", "Claude Sonnet 4.6"),
        ]
        self.assertEqual(
            [item.id for item in _select_live_models("claude", free_tier_models)],
            ["claude-sonnet-5-5", "claude-haiku-5-5"],
        )

        chatgpt_models = [
            Model("gpt-6", "GPT-6"),
            Model("gpt-6-instant", "GPT-6 Instant"),
            Model("gpt-6-thinking", "GPT-6 Thinking"),
            Model("gpt-6-t-mini", "GPT-6 Thinking Mini"),
            Model("gpt-5-4", "GPT-5.4"),
        ]
        self.assertEqual(
            [item.id for item in _select_live_models("chatgpt", chatgpt_models)],
            ["gpt-6-instant", "gpt-6-thinking"],
        )

        gemini_models = [
            Model("gemini-otomatik", "Otomatik"),
            Model("gemini-hydrogen", "Hydrogen"),
            Model("gemini-neon", "Neon"),
            Model("gemini-carbon", "Carbon"),
        ]
        self.assertEqual(
            [item.id for item in _select_live_models("gemini", gemini_models)],
            ["gemini-hydrogen", "gemini-neon"],
        )

    def test_live_api_suite_is_disabled_without_explicit_opt_in(self) -> None:
        from tests.live.test_sessions import AsyncSessionApiTests

        with patch.dict(os.environ, {"WEBAI_LIVE_TESTS": "0"}):
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(AsyncSessionApiTests)
            result = unittest.TestResult()
            suite.run(result)
        self.assertEqual(len(result.skipped), 1)
        self.assertFalse(result.errors)
        self.assertFalse(result.failures)


class LiveContextErrorTests(unittest.TestCase):
    def test_edit_readback_uses_the_new_prompt_and_requires_a_remote_answer(self) -> None:
        from tests.live.test_messages import _has_edited_turn
        from webaiprovider import AsyncMessage, Response

        response = Response(
            "Edited answer",
            id="persisted-assistant:1",
            message=AsyncMessage("Edited prompt"),
            metadata={"from_history": True},
        )
        self.assertTrue(_has_edited_turn("Edited prompt", [response]))
        self.assertFalse(_has_edited_turn("Original prompt", [response]))
        response.metadata.clear()
        self.assertFalse(_has_edited_turn("Edited prompt", [response]))

    def test_setup_diagnostics_show_only_typed_status_or_known_workspace_error(self) -> None:
        from tests.live.support.context import _safe_error_label

        cases = {
            "Provider denied access (HTTP 403); the request was not retried": "ProviderError (HTTP 403)",
            "Provider request failed (HTTP 503)": "ProviderError (HTTP 503)",
            "Claude workspace selection is invalid; reverify the account": "Claude workspace selection could not be verified",
            "Provider request failed (HTTP 503) private route": "ProviderError",
        }
        for message, expected in cases.items():
            self.assertEqual(_safe_error_label(ProviderError(message)), expected)

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
        self.assertEqual(
            _safe_error_label(ChatGPTError("ChatGPT submission returned HTTP 422")),
            "ChatGPTError (HTTP 422)",
        )
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
            gate=ActionGate(interval=0),
        )
        self.remote_id = "temporary-id"

    def track(self, kind="project"):
        marker = f"{self.context.marker}-p" if kind == "project" else f"{self.context.marker}-chat"
        remote = (
            ProjectData(self.remote_id, marker)
            if kind == "project"
            else Conversation(self.remote_id, marker)
        )
        entry = self.context.new_remote(kind, marker)
        entry.update(attempted=True, remote_id=self.remote_id)
        self.context.save()
        return entry, remote

    async def test_unrelated_marker_or_remote_id_prevents_deletion(self) -> None:
        entry, tracked = self.track()
        self.provider.delete_project = AsyncMock()
        for remote in (
            ProjectData(self.remote_id, "Unrelated"),
            ProjectData("another-id", tracked.name),
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
        self.provider.delete_project.assert_awaited_once_with(self.remote_id)
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
        self.provider.delete_project.assert_awaited_once_with(self.remote_id)
        self.assertFalse(self.journal.exists())

    async def test_later_pages_prevent_false_deletion_confirmation(self) -> None:
        for kind in ("chat", "project"):
            with self.subTest(kind=kind):
                self.context.state["resources"].clear()
                entry, remote = self.track(kind)
                delete = AsyncMock(side_effect=ProviderError("uncertain"))
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
                delete.assert_awaited_once_with(self.remote_id)
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

    async def test_rate_limited_cleanup_keeps_journal_and_does_not_scan_or_retry(self) -> None:
        entry, remote = self.track()
        self.provider.get_project = AsyncMock(return_value=remote)
        self.provider.delete_project = AsyncMock(side_effect=RateLimited(30))
        self.provider.list_projects = AsyncMock()
        with self.assertRaisesRegex(CleanupError, "rate limited cleanup"):
            await self.context.cleanup_entry_async(entry)
        self.provider.get_project.assert_awaited_once_with(self.remote_id)
        self.provider.delete_project.assert_awaited_once_with(self.remote_id)
        self.provider.list_projects.assert_not_awaited()
        self.assertTrue(self.journal.exists())

    async def test_chat_marker_in_history_allows_exact_cleanup(self) -> None:
        self.context.account = LiveAccount(
            alias=self.context.account.alias, provider="grok", credentials={}
        )
        entry, _ = self.track("chat")
        remote = Conversation(
            self.remote_id,
            "Automatic title",
            (MessageData("user-id", "user", (Content("text", entry["markers"][0]),)),),
        )
        self.provider._get_chat_metadata = AsyncMock(
            side_effect=(Conversation(self.remote_id, "Automatic title"), RemoteNotFound("missing"))
        )
        self.provider.get_chat = AsyncMock(return_value=remote)
        self.provider.delete_chat = AsyncMock()
        await self.context.cleanup_entry_async(entry)
        self.provider.get_chat.assert_awaited_once_with(self.remote_id)
        self.assertEqual(self.provider._get_chat_metadata.await_count, 1)
        self.provider.delete_chat.assert_awaited_once_with(self.remote_id)
        self.assertFalse(self.journal.exists())


class LiveSessionSetupCacheTests(unittest.TestCase):
    def test_live_cases_use_the_runner_available_on_this_python(self) -> None:
        from tests.live.support import case as support

        loops = []

        class Scenario(LiveAsyncTestCase):
            async def runTest(self) -> None:
                loops.append(asyncio.get_running_loop())

        with (
            patch.dict(os.environ, {"WEBAI_LIVE_TESTS": "1"}, clear=True),
            patch.object(support, "_SHARED_ASYNC_RUNNER", None),
            patch.object(support, "_SHARED_ASYNC_SESSION", None),
            patch.object(support, "_SHARED_CLEANUP_REGISTERED", True),
            patch.object(LiveSession, "open_async", new_callable=AsyncMock) as open_session,
            patch.object(LiveSession, "close_async", new_callable=AsyncMock) as close_session,
        ):
            try:
                result = unittest.TestResult()
                unittest.TestSuite([Scenario(), Scenario()]).run(result)
                self.assertTrue(result.wasSuccessful(), result.errors)
                self.assertEqual(len(loops), 2)
            finally:
                support._close_shared_async_session()

            shared = hasattr(asyncio, "Runner")
            self.assertEqual(loops[0] is loops[1], shared)
            self.assertEqual(open_session.await_count, 1 if shared else 2)
            self.assertEqual(close_session.await_count, 1 if shared else 2)

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
        live = LiveSession()
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
                raise LiveApiError(f"{label} failed: provider network timeout") from None
            except LiveApiError as error:
                return error

    async def test_cancelled_cookie_refresh_finishes_before_lock_release(self) -> None:
        import threading

        from webaiprovider.errors import AuthenticationError

        started, finish = threading.Event(), threading.Event()
        account = LiveAccount("default", "gemini", {})
        provider = SimpleNamespace(open=AsyncMock(side_effect=AuthenticationError("expired")))
        context = LiveContext(provider, account, gate=ActionGate(interval=0))
        live = LiveSession()
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

        live = LiveSession()
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
                    raise LiveApiError("session.open failed: provider error") from None
                except LiveApiError as error:
                    return error

        live = LiveSession()
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
            wrapped(ProviderError("Provider request failed (HTTP 400)")),
            wrapped(ProviderError("Claude workspace selection is invalid; reverify the account")),
        ):
            with patch.object(
                live, "_open_async_once", AsyncMock(side_effect=non_retryable)
            ) as run:
                with self.assertRaises(LiveApiError):
                    await live.open_async()
                run.assert_awaited_once()


class AsyncLiveSessionCookieRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_network_failure_does_not_refresh_cookies(self) -> None:
        from webaiprovider.errors import RequestNotSentError

        account = LiveAccount("default-browser", "gemini", {"cookies": {"sid": "saved"}})
        provider = SimpleNamespace(
            open=AsyncMock(side_effect=RequestNotSentError("unavailable")), close=AsyncMock()
        )
        context = LiveContext(provider, account, gate=ActionGate(interval=0))
        live = LiveSession()
        live.context, live.session = context, provider

        from tests.live.support.errors import LiveApiError

        with (
            patch("tests.live.support.case._ACTION_GATE", ActionGate(interval=0)),
            patch.object(live, "_prepare", return_value=context),
            patch("tests.live.support.case.load_or_import_account") as refresh,
            self.assertRaises(LiveApiError),
        ):
            await live.open_async()
        refresh.assert_not_called()
        self.assertIsNone(live.context)

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
        old_context = LiveContext(old_provider, account, gate=ActionGate(interval=0))
        live = LiveSession()
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
        live = LiveSession()

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
            gate=ActionGate(interval=0),
        )

    async def test_lost_branch_response_recovers_by_marker_without_deleting_parent(self) -> None:
        marker = f"{self.context.marker}-branch"
        parent_chat = Conversation("c_parent", f"{self.context.marker}-src")
        branch_chat = Conversation("c_branch", f"{self.context.marker}-src")

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
        parent_chat = Conversation("c_parent", f"{self.context.marker}-src")
        b1 = Conversation("c_b1", f"{self.context.marker}-src")
        b2 = Conversation("c_b2", f"{self.context.marker}-src")

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
        parent_chat = Conversation("c_parent", f"{self.context.marker}-src")
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
    async def test_cancelled_queued_action_does_not_require_remote_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "cleanup.json"
            provider = SimpleNamespace(get_chat=AsyncMock(), delete_chat=AsyncMock())
            gate = ActionGate(interval=0)
            case = LiveAsyncTestCase("runTest")
            case.context = LiveContext(
                provider, LiveAccount("personal", "chatgpt", {}), gate=gate, journal_path=journal
            )
            entry = case.context.new_remote("chat", f"{case.context.marker}-chat")
            started, release = asyncio.Event(), asyncio.Event()

            async def hold_slot():
                started.set()
                await release.wait()

            holder = asyncio.create_task(gate.call_async(hold_slot))
            try:
                await started.wait()
                mutation = AsyncMock()
                task = asyncio.create_task(
                    case.call_or_skip_rate_limit("chats.send", mutation, entry=entry)
                )
                await asyncio.sleep(0)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                release.set()
                await holder

            mutation.assert_not_awaited()
            self.assertFalse(entry["attempted"])
            await case.context.cleanup_entry_async(entry)
            provider.get_chat.assert_not_awaited()
            provider.delete_chat.assert_not_awaited()
            self.assertFalse(journal.exists())

    async def test_live_action_logs_elapsed_milliseconds(self) -> None:
        context = LiveContext(
            SimpleNamespace(), LiveAccount("personal", "chatgpt", {}), gate=ActionGate(interval=0)
        )
        clock = SimpleNamespace(monotonic=Mock(side_effect=(10.0, 10.125)))
        with (
            patch("tests.live.support.context.time", clock),
            patch("builtins.print") as output,
        ):
            self.assertEqual(
                await context.call_async("fixture", AsyncMock(return_value="ok")), "ok"
            )
        output.assert_called_once_with("ACTION OK  fixture  125 ms", flush=True)

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
                    gate=ActionGate(interval=0),
                )
                entry = case.context.new_remote("chat", f"{case.context.marker}-chat")
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
                    remote = Conversation("c_orphan", f"{context.marker}-chat")
                    provider.list_chats = AsyncMock(return_value=Page((remote,), None))
                    provider.get_chat = AsyncMock(side_effect=(remote, RemoteNotFound("missing")))
                    provider.delete_chat = AsyncMock()
                    await context.cleanup_pending_async()
                    provider.delete_chat.assert_awaited_once_with("c_orphan")
                    self.assertFalse(journal.exists())

    async def test_pre_send_rate_limit_clears_attempted_unless_remote_id_bound(self) -> None:
        async def rejected_send():
            raise RateLimited(1.0, message_submitted=False)

        for remote_id in (None, "c_bound"):
            with self.subTest(remote_id=remote_id), tempfile.TemporaryDirectory() as directory:
                journal = Path(directory) / "live-cleanup-chatgpt.json"
                provider = SimpleNamespace(delete_chat=AsyncMock())
                context = LiveContext(
                    provider,
                    LiveAccount("personal", "chatgpt", {}),
                    gate=ActionGate(interval=0),
                    journal_path=journal,
                )
                case = LiveAsyncTestCase()
                case.context = context
                entry = context.new_remote("chat", f"{context.marker}-chat")
                entry["attempted"] = True
                if remote_id:
                    context.record_chat_id(entry, SimpleNamespace(remote_id=remote_id))
                else:
                    context.save()
                with self.assertRaises(unittest.SkipTest):
                    await case.call_or_skip_rate_limit("messages.send", rejected_send, entry=entry)
                self.assertEqual(entry["attempted"], remote_id is not None)
                if remote_id is None:
                    await context.cleanup_pending_async()
                self.assertEqual(journal.exists(), remote_id is not None)
                provider.delete_chat.assert_not_awaited()


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
                            Conversation("c_gem", f"{gemini_run}-chat"),
                            RemoteNotFound("missing"),
                        )
                    ),
                    delete_chat=AsyncMock(),
                )
                context = LiveContext(
                    provider,
                    LiveAccount("personal", "gemini", {}),
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

            live = LiveSession()
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
            now = 100.0
            slept: list[float] = []

            async def advance(delay: float) -> None:
                nonlocal now
                slept.append(delay)
                now += delay

            with (
                patch(
                    "tests.live.support.actions.time",
                    SimpleNamespace(monotonic=lambda: now, time=lambda: now + 900.0),
                ),
                patch("tests.live.support.actions.asyncio", wraps=asyncio, sleep=advance),
            ):
                action = AsyncMock(return_value="done")
                local_gate = ActionGate(interval=1.0, limit=2)
                await local_gate.call_async(action)
                await local_gate.call_async(action)
                self.assertEqual(slept, [1.0])
                with self.assertRaisesRegex(SafetyError, "API-action limit"):
                    await local_gate.call_async(action)
                self.assertEqual(action.await_count, 2)

                slept.clear()
                action.reset_mock()
                self.assertEqual(await gate_one.call_async(action), "done")
                now += 0.25
                self.assertEqual(await gate_two.call_async(action), "done")
                self.assertEqual(slept, [0.75])
                await gate_two.call_async(action)
                self.assertEqual(slept, [0.75, 1.0])
                with self.assertRaisesRegex(SafetyError, "API-action limit"):
                    await gate_one.call_async(action)
                self.assertEqual(action.await_count, 3)

    async def test_temporary_chat_cleanup_routes_delete_through_action_gate(self) -> None:
        from tests.live.test_messages import AsyncMessageApiTests
        from webaiprovider.base import Capability

        gate = ActionGate(interval=0, limit=10)
        deleted = AsyncMock()
        chat = SimpleNamespace(
            temporary=True,
            remote_id="tmp-1",
            responses=SimpleNamespace(
                create=lambda *_a, **_kw: SimpleNamespace(
                    start=AsyncMock(
                        return_value=SimpleNamespace(
                            text="OK", message=SimpleNamespace(content="OK")
                        )
                    )
                )
            ),
            delete=deleted,
        )
        session = SimpleNamespace(
            capabilities={Capability.CHAT, Capability.TEMPORARY},
            chats=SimpleNamespace(
                create=AsyncMock(return_value=chat),
                list=AsyncMock(return_value=[]),
            ),
        )
        test = AsyncMessageApiTests("test_incognito_temporary_chat")
        test.context = LiveContext(session, LiveAccount("personal", "chatgpt", {}), gate=gate)
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

            def fake_live_session():
                ctx = LiveContext(
                    SimpleNamespace(),
                    LiveAccount("personal", "grok", {}),
                    gate=ActionGate(interval=0),
                )
                return SimpleNamespace(
                    open_async=AsyncMock(return_value=ctx),
                    close_async=AsyncMock(),
                )

            def failing_live_session():
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
                    ("partial", ["optional: unsupported"]),
                    ("skipped_unsupported", []),
                    ("skipped_rate_limited", []),
                    ("failed", []),
                    ("skipped_setup_failed", []),
                ],
            )


class ChatGPTBranchLiveFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_chatgpt_branch_deferred_rename_preview_exclusion_and_rate_limit(self) -> None:
        from tests.live.test_messages import AsyncMessageApiTests
        from webaiprovider.base import Capability
        from webaiprovider.providers.chatgpt.resources import AsyncChatGPTChat
        from webaiprovider.resources.message.operation import OutputDelta, OutputEnd, TerminalStatus

        for rate_limit_branch in (False, True):
            with (
                self.subTest(rate_limit_branch=rate_limit_branch),
                tempfile.TemporaryDirectory() as directory,
            ):
                gate = ActionGate(interval=0, limit=30)
                now = 100.0
                waits = []

                async def advance(delay: float) -> None:
                    nonlocal now
                    waits.append(delay)
                    now += delay

                journal = Path(directory) / "live-cleanup-chatgpt.json"
                events_order = []
                created_entries = []

                def make_op(chat_obj, chat_id, text, *, fail_rate=False):
                    class StreamOp:
                        def __init__(self):
                            self.chat = chat_obj
                            self.result = SimpleNamespace(
                                chat_id=chat_id,
                                chat=chat_obj,
                                id=f"m-{chat_id}",
                                text=text,
                                content=text,
                                sources=(),
                                artifacts=(),
                                message=MessageData(
                                    f"m-{chat_id}",
                                    "assistant",
                                    (Content("text", text=text),),
                                    actions=("retry", "branch"),
                                ),
                            )

                        async def start(self):
                            if fail_rate:
                                raise RateLimited(1.0, message_submitted=False)
                            events_order.append(("branched_send", chat_id))
                            chat_obj.id = chat_id
                            chat_obj.remote_id = chat_id
                            chat_obj.metadata.pop("pending_branch", None)
                            return self.result

                        async def wait(self):
                            return self.result

                        async def stream(self):
                            events_order.append(("stream_bound", chat_id))
                            chat_obj.id = chat_id
                            chat_obj.remote_id = chat_id
                            yield OutputDelta(text)
                            yield OutputEnd(text)
                            yield TerminalStatus("completed")

                    return StreamOp()

                async def fake_rename_branched(title):
                    events_order.append(("rename_branched", branched.id, title))
                    branched.title = title

                root_chat: AsyncChatGPTChat | None = None
                branched: AsyncChatGPTChat | None = None
                deleted_ids = set()

                async def fake_delete_chat(cid):
                    deleted_ids.add(cid)

                async def fake_get_chat(cid):
                    if cid in deleted_ids or rate_limit_branch:
                        if cid != "c_root" or cid in deleted_ids:
                            raise RemoteNotFound("gone")
                    title = branched.title if cid == "c_branch_real" else root_chat.title
                    return Conversation(cid, title)

                from webaiprovider import AsyncMessage, Response, TimingMetrics

                root_user = AsyncMessage("u1", role="user", id="u-1", remote_id="u-1")
                branch_user = AsyncMessage("u1", role="user", id="u-1", remote_id="u-1")
                root_msgs_data = [
                    MessageData(
                        "u-1",
                        "user",
                        (Content("text", text="u1"),),
                        actions=("edit",),
                    ),
                    MessageData(
                        "m-c_root",
                        "assistant",
                        (Content("text", text="ROOT"),),
                        actions=("retry", "branch"),
                    ),
                ]
                branched_msgs_data = [
                    MessageData(
                        "u-1",
                        "user",
                        (Content("text", text="u1"),),
                        actions=("edit",),
                    ),
                    MessageData(
                        "m-c_branch_real",
                        "assistant",
                        (Content("text", text="BR"),),
                        actions=("retry", "branch"),
                    ),
                ]

                provider_impl = SimpleNamespace(
                    branch_chat=AsyncMock(
                        side_effect=lambda cid, mid: (
                            events_order.append(("branch", "preview-1")),
                            Conversation(
                                "preview-1",
                                "Branched preview",
                                metadata={
                                    "pending_branch": {
                                        "source_conversation_id": cid,
                                        "message_id": mid,
                                    }
                                },
                            ),
                        )[1]
                    ),
                    rename_chat=AsyncMock(),
                    delete_chat=AsyncMock(side_effect=fake_delete_chat),
                )

                def bind_chat(cid, title, *, messages_data, rename_hook=None):
                    chat_obj = AsyncChatGPTChat(id=cid, title=title, provider=session)
                    resp_obj = Response(
                        "BR" if cid == "preview-1" else "ROOT",
                        id=f"m-{cid}",
                        remote_id=f"m-{cid}",
                        message=branch_user if cid == "preview-1" else root_user,
                        chat=chat_obj,
                        metadata={"actions": ("retry", "branch"), "from_history": True},
                        timing=TimingMetrics(duration_ms=50),
                    )
                    chat_obj.responses = SimpleNamespace(
                        create=lambda *_a, **_kw: make_op(
                            chat_obj,
                            "c_branch_real" if cid == "preview-1" else "c_root",
                            "BR" if cid == "preview-1" else "ROOT",
                            fail_rate=rate_limit_branch if cid == "preview-1" else False,
                        ),
                        list=AsyncMock(return_value=[resp_obj]),
                    )
                    if rename_hook is not None:
                        chat_obj.rename = AsyncMock(side_effect=rename_hook)
                    return chat_obj

                from webaiprovider import Account, AsyncChatGPTProvider
                from webaiprovider.core.async_hooks import AsyncOperationHooks

                backing = AsyncChatGPTProvider(Account.from_cookies("chatgpt", {}, id="personal"))
                session = SimpleNamespace(
                    capabilities={
                        Capability.MODELS,
                        Capability.CHAT,
                        Capability.DELETE,
                        Capability.HISTORY,
                        Capability.RENAME,
                        Capability.CHAT_BRANCH,
                    },
                    hooks=AsyncOperationHooks(),
                    new_context=backing.new_context,
                    send_lock=backing.send_lock,
                    activity=backing.activity,
                    emit=backing.emit,
                    emit_safe=backing.emit_safe,
                    has_event_subscribers=False,
                    _require_provider=lambda: provider_impl,
                    _branch_chat_data=provider_impl.branch_chat,
                    _get_chat_data=fake_get_chat,
                    _delete_chat_data=fake_delete_chat,
                    models=SimpleNamespace(refresh=AsyncMock(return_value=[])),
                    chats=SimpleNamespace(
                        hooks=AsyncOperationHooks(),
                        create=AsyncMock(),
                        get=AsyncMock(
                            side_effect=lambda cid: Conversation(
                                cid,
                                branched.title if cid == "c_branch_real" else root_chat.title,
                                messages=tuple(
                                    root_msgs_data if cid == "c_root" else branched_msgs_data
                                ),
                            )
                        ),
                        list=AsyncMock(
                            side_effect=lambda: [
                                Conversation(cid, "Title")
                                for cid in (
                                    ["c_root"] + ([] if rate_limit_branch else ["c_branch_real"])
                                )
                            ]
                        ),
                    ),
                    list_chats=AsyncMock(return_value=Page((), None)),
                    get_chat=AsyncMock(side_effect=fake_get_chat),
                    delete_chat=AsyncMock(side_effect=fake_delete_chat),
                )
                root_chat = bind_chat("c_root", "Root", messages_data=root_msgs_data)
                root_chat.rename = AsyncMock(
                    side_effect=lambda title: setattr(root_chat, "title", title)
                )
                orig_branch = AsyncChatGPTChat.branch

                async def capture_branch(self, message_id, *, title=None):
                    nonlocal branched
                    real = await orig_branch(self, message_id, title=title)
                    real.id = "preview-1"
                    real.rename = AsyncMock(side_effect=fake_rename_branched)
                    real_resp = Response(
                        "BR",
                        id="m-c_branch_real",
                        remote_id="m-c_branch_real",
                        message=branch_user,
                        chat=real,
                        metadata={"actions": ("retry", "branch"), "from_history": True},
                    )
                    real.responses = SimpleNamespace(
                        create=lambda *_a, **_kw: make_op(
                            real, "c_branch_real", "BR", fail_rate=rate_limit_branch
                        ),
                        list=AsyncMock(return_value=[real_resp]),
                    )
                    branched = real
                    return real

                root_chat.branch = capture_branch.__get__(root_chat, AsyncChatGPTChat)
                session.chats.create.return_value = root_chat
                with (
                    patch("tests.live.support.context.CLEANUP_FILE", journal),
                    patch("tests.live.support.journal.CLEANUP_FILE", journal),
                    patch(
                        "tests.live.support.actions.time", SimpleNamespace(monotonic=lambda: now)
                    ),
                    patch("tests.live.support.actions.asyncio", wraps=asyncio, sleep=advance),
                ):
                    test = AsyncMessageApiTests("test_temporary_message_stream_history_and_cleanup")
                    test.context = LiveContext(
                        session, LiveAccount("personal", "chatgpt", {}), gate=gate
                    )
                    orig_new_remote = test.context.new_remote

                    def capture_new_remote(*args, **kwargs):
                        created = orig_new_remote(*args, **kwargs)
                        created_entries.append(created)
                        return created

                    test.context.new_remote = capture_new_remote
                    if rate_limit_branch:
                        with self.assertRaises(unittest.SkipTest):
                            await test.test_temporary_message_stream_history_and_cleanup()
                        branch_entries = [
                            e for e in created_entries if "c_root" in e.get("exclude_ids", [])
                        ]
                        self.assertEqual(len(branch_entries), 1)
                        self.assertFalse(branch_entries[0]["attempted"])
                        self.assertIn("preview-1", branch_entries[0]["exclude_ids"])
                        self.assertEqual(test.context.state["resources"], [])
                        self.assertFalse(journal.exists())
                        branched.rename.assert_not_awaited()
                        self.assertEqual(deleted_ids, {"c_root"})
                        self.assertEqual(waits, [1.0, 0.5])
                    else:
                        await test.test_temporary_message_stream_history_and_cleanup()
                        branch_idx = events_order.index(("branch", "preview-1"))
                        send_idx = events_order.index(("branched_send", "c_branch_real"))
                        rename_idx = events_order.index(
                            ("rename_branched", "c_branch_real", branched.title)
                        )
                        self.assertLess(branch_idx, send_idx)
                        self.assertLess(send_idx, rename_idx)
                        branch_entries = [
                            e for e in created_entries if "c_root" in e.get("exclude_ids", [])
                        ]
                        self.assertEqual(branch_entries[0]["remote_id"], "c_branch_real")
                        self.assertIn("preview-1", branch_entries[0]["exclude_ids"])
                        self.assertEqual(deleted_ids, {"c_root", "c_branch_real"})
                        self.assertEqual(waits, [])
                        self.assertEqual(test.context.state["resources"], [])
                        self.assertFalse(journal.exists())

    async def test_branch_finally_cleans_parent_even_if_branch_cleanup_fails(self) -> None:
        from tests.live.test_messages import AsyncMessageApiTests
        from webaiprovider.base import Capability
        from webaiprovider.providers.chatgpt.resources import AsyncChatGPTChat
        from webaiprovider.resources.message.operation import OutputDelta

        gate = ActionGate(interval=0, limit=30)
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "live-cleanup-chatgpt.json"
            deleted_ids = set()

            async def fake_delete_chat(cid):
                deleted_ids.add(cid)

            async def fake_get_chat(cid):
                if cid in deleted_ids or cid == "c_root":
                    if cid in deleted_ids:
                        raise RemoteNotFound("gone")
                    return Conversation(cid, root_chat.title)
                raise RemoteNotFound("gone")

            session = SimpleNamespace(
                capabilities={
                    Capability.MODELS,
                    Capability.CHAT,
                    Capability.DELETE,
                    Capability.HISTORY,
                    Capability.RENAME,
                    Capability.CHAT_BRANCH,
                },
                models=SimpleNamespace(refresh=AsyncMock(return_value=[])),
                chats=SimpleNamespace(create=AsyncMock(), list=AsyncMock(return_value=[])),
                list_chats=AsyncMock(return_value=Page((), None)),
                get_chat=AsyncMock(side_effect=fake_get_chat),
                delete_chat=AsyncMock(side_effect=fake_delete_chat),
            )
            root_chat = AsyncChatGPTChat(id="c_root", title="Root", provider=session)

            def make_fake_op(target_chat, cid: str, msg_id: str, text: str):
                result = SimpleNamespace(
                    id=msg_id,
                    text=text,
                    content=text,
                    message=MessageData(
                        msg_id,
                        "assistant",
                        (Content("text", text),),
                    ),
                    sources=(),
                )

                class _FakeOp:
                    async def start(self):
                        target_chat.remote_id = cid
                        return result

                    async def wait(self):
                        return result

                    async def stream(self):
                        target_chat.remote_id = cid
                        yield OutputDelta(text)

                return _FakeOp()

            from webaiprovider import AsyncMessage, Response, TimingMetrics

            root_turn = Response(
                "ROOT",
                id="m-c_root",
                remote_id="m-c_root",
                message=AsyncMessage("u1", role="user", id="u-1", remote_id="u-1"),
                chat=root_chat,
                metadata={"actions": ("branch",), "from_history": True},
                timing=TimingMetrics(duration_ms=50),
            )
            root_chat.responses = SimpleNamespace(
                create=lambda *_a, **_kw: make_fake_op(root_chat, "c_root", "m-c_root", "ROOT"),
                list=AsyncMock(return_value=[root_turn]),
            )
            root_chat.rename = AsyncMock(
                side_effect=lambda title: setattr(root_chat, "title", title)
            )

            async def bad_branch(_mid):
                branched_chat = AsyncChatGPTChat(
                    id="preview-1",
                    remote_id="preview-1",
                    title="Broken branch",
                    metadata={"pending_branch": {"conversation_id": "c_root"}},
                    provider=session,
                )
                branched_chat.responses = SimpleNamespace(
                    create=lambda *_a, **_kw: make_fake_op(
                        branched_chat, "c_branch_real", "m-c_branch_real", "BR"
                    ),
                )
                branched_chat.rename = AsyncMock(
                    side_effect=lambda title: setattr(branched_chat, "title", title)
                )
                return branched_chat

            root_chat.branch = bad_branch
            session.chats.create.return_value = root_chat
            with (
                patch("tests.live.support.context.CLEANUP_FILE", journal),
                patch("tests.live.support.journal.CLEANUP_FILE", journal),
            ):
                test = AsyncMessageApiTests("test_temporary_message_stream_history_and_cleanup")
                test.context = LiveContext(
                    session, LiveAccount("personal", "chatgpt", {}), gate=gate
                )
                orig_cleanup = test.context.cleanup_entry_async
                seen_cleanups = []

                async def wrapped_cleanup(entry):
                    seen_cleanups.append(entry["markers"][0])
                    if len(seen_cleanups) == 1:
                        raise CleanupError("branch locate failed")
                    return await orig_cleanup(entry)

                test.context.cleanup_entry_async = wrapped_cleanup
                with self.assertRaisesRegex(CleanupError, "branch locate failed"):
                    await test.test_temporary_message_stream_history_and_cleanup()
                self.assertEqual(len(seen_cleanups), 2)
                self.assertEqual(deleted_ids, {"c_root"})
