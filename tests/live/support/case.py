from __future__ import annotations

import logging
import os
import unittest
from typing import Any

from .actions import ActionGate
from .config import LiveAccount, load_or_import_account
from .context import LiveContext
from .errors import SafetyError
from .journal import RunLock, read_cleanup_state

_ACTION_GATE = ActionGate(limit=32)


class LiveSession:
    """Own one real provider session and its guarded cleanup lifecycle."""

    def __init__(self, api: str) -> None:
        self.api = api
        self.lock: RunLock | None = None
        self.context: LiveContext | None = None
        self.session: Any = None

    def _prepare(self) -> LiveContext:
        provider = os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold()
        alias = os.environ.get("WEBAI_LIVE_ACCOUNT", "")
        if self.api not in {"async", "sync"}:
            raise SafetyError("WEBAI_LIVE_API must be sync or async")
        self.lock = RunLock()
        self.lock.__enter__()
        try:
            account = load_or_import_account(
                provider,
                alias,
                browser=os.environ.get("WEBAI_LIVE_BROWSER"),
            )
            state = read_cleanup_state()
            recover = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
            if state and (state["provider"] != provider or state["account"] != account.alias):
                raise SafetyError("Pending cleanup belongs to a different account")
            if state and not recover:
                raise SafetyError("Recover pending test data before starting another live run")
            if recover and state is None:
                raise SafetyError("No pending cleanup journal exists")

            for logger_name in ("httpx", "httpcore", "curl_cffi", "websockets", "urllib3"):
                logging.getLogger(logger_name).setLevel(logging.WARNING)
            self.session = self._create_provider_session(account)
            self.context = LiveContext(
                self.session,
                account,
                self.api,
                state=state,
                gate=_ACTION_GATE,
            )
            return self.context
        except BaseException:
            self._release_lock()
            raise

    def _create_provider_session(self, account: LiveAccount) -> Any:
        from webaiprovider.accounts.account import Account

        account_object = Account(
            id=account.alias,
            provider=account.provider,
            credentials=account.credentials,
            name="Live API test",
        )
        if self.api == "async":
            from webaiprovider.async_provider import AsyncProvider

            return AsyncProvider(account_object)
        from webaiprovider.provider import Provider

        return Provider(account_object)

    def open_sync(self) -> LiveContext:
        context = self._prepare()
        try:
            context.call("session.open", self.session.open)
        except BaseException:
            try:
                context.close()
            except Exception:
                pass
            self._release_lock()
            raise
        return context

    async def open_async(self) -> LiveContext:
        context = self._prepare()
        try:
            await context.call_async("session.open", self.session.open)
        except BaseException:
            try:
                await context.aclose()
            except Exception:
                pass
            self._release_lock()
            raise
        return context

    def close_sync(self) -> None:
        error: Exception | None = None
        try:
            if self.context is not None:
                if self.context.state["resources"]:
                    try:
                        self.context.cleanup_pending()
                    except Exception as exc:
                        error = exc
                try:
                    self.context.print_report()
                except Exception as exc:
                    error = error or exc
                try:
                    self.context.close()
                except Exception as exc:
                    error = error or exc
        finally:
            self._release_lock()
        if error is not None:
            raise error

    async def close_async(self) -> None:
        error: Exception | None = None
        try:
            if self.context is not None:
                if self.context.state["resources"]:
                    try:
                        await self.context.cleanup_pending_async()
                    except Exception as exc:
                        error = exc
                try:
                    self.context.print_report()
                except Exception as exc:
                    error = error or exc
                try:
                    await self.context.aclose()
                except Exception as exc:
                    error = error or exc
        finally:
            self._release_lock()
        if error is not None:
            raise error

    def _release_lock(self) -> None:
        if self.lock is not None:
            lock, self.lock = self.lock, None
            lock.__exit__(None, None, None)


class LiveSyncTestCase(unittest.TestCase):
    """Base for one synchronous live feature scenario."""

    recovery_only = False

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if os.environ.get("WEBAI_LIVE_TESTS") != "1":
            raise unittest.SkipTest("Set WEBAI_LIVE_TESTS=1 to opt in to real provider requests")
        if os.environ.get("WEBAI_LIVE_API", "async").casefold() != "sync":
            raise unittest.SkipTest("This run selected the asynchronous API")
        recovering = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
        if recovering != cls.recovery_only:
            raise unittest.SkipTest("Recovery mode runs only cleanup recovery tests")
        cls.live = LiveSession("sync")
        cls.context = cls.live.open_sync()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if hasattr(cls, "live"):
                cls.live.close_sync()
        finally:
            super().tearDownClass()

    def call_api(self, label: str, action, *, expected_errors: tuple[type[Exception], ...] = ()):
        return self.context.call(label, action, expected_errors=expected_errors)


class LiveAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """Base for one asynchronous live feature scenario."""

    recovery_only = False

    async def asyncSetUp(self) -> None:
        if os.environ.get("WEBAI_LIVE_TESTS") != "1":
            self.skipTest("Set WEBAI_LIVE_TESTS=1 to opt in to real provider requests")
        if os.environ.get("WEBAI_LIVE_API", "async").casefold() != "async":
            self.skipTest("This run selected the synchronous API")
        recovering = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
        if recovering != self.recovery_only:
            self.skipTest("Recovery mode runs only cleanup recovery tests")
        self.live = LiveSession("async")
        self.context = await self.live.open_async()

    async def asyncTearDown(self) -> None:
        if hasattr(self, "live"):
            await self.live.close_async()

    async def call_api(
        self, label: str, action, *, expected_errors: tuple[type[Exception], ...] = ()
    ):
        return await self.context.call_async(label, action, expected_errors=expected_errors)


class LiveSyncRecoveryTestCase(LiveSyncTestCase):
    """Recovery-only live test base for the synchronous API."""

    recovery_only = True


class LiveAsyncRecoveryTestCase(LiveAsyncTestCase):
    """Recovery-only live test base for the asynchronous API."""

    recovery_only = True
