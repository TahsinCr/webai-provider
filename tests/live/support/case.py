from __future__ import annotations

import asyncio
import logging
import os
import unittest

from .config import LiveAccount, load_accounts, select_account
from .context import LiveContext
from .errors import SafetyError
from .journal import RunLock, read_cleanup_state


class LiveProviderCase:
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.lock = RunLock()
        cls.lock.__enter__()
        cls.loop = None
        cls.context = None
        cls.recover_only = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
        try:
            provider = os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold()
            alias = os.environ.get("WEBAI_LIVE_ACCOUNT", "")
            api = os.environ.get("WEBAI_LIVE_API", "async").casefold()
            if api not in {"async", "sync"}:
                raise SafetyError("WEBAI_LIVE_API must be sync or async")
            account = select_account(load_accounts(), provider, alias)
            state = read_cleanup_state()
            if state and (state["provider"] != provider or state["account"] != alias):
                raise SafetyError("Pending cleanup belongs to a different account")
            if state and not cls.recover_only:
                raise SafetyError("Recover pending test data first with WEBAI_LIVE_RECOVER=1")
            if cls.recover_only and state is None:
                raise SafetyError("No pending cleanup journal exists")
            cls.loop = asyncio.new_event_loop() if api == "async" else None
            cls.context = cls._open_session(account, api, state)
            if cls.recover_only:
                cls.context.cleanup_pending()
                cls.context.print_report()
                cls._close_session()
                raise unittest.SkipTest("Pending test data was cleaned; rerun the API suite")
        except BaseException:
            cls._close_session()
            raise

    @classmethod
    def _open_session(cls, account: LiveAccount, api: str, state):
        from webaiprovider.accounts.account import Account

        for logger_name in ("httpx", "httpcore", "curl_cffi", "websockets", "urllib3"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)
        account_object = Account(
            id=account.alias,
            provider=account.provider,
            credentials=account.credentials,
            name="Live API test",
        )
        if api == "async":
            from webaiprovider.async_provider import AsyncProvider

            session = AsyncProvider(account_object)
        else:
            from webaiprovider.provider import Provider

            session = Provider(account_object)
        context = LiveContext(session, account, api, cls.loop, state)
        cls.context = context
        context.call("session.open", session.open)
        return context

    @classmethod
    def _close_session(cls) -> None:
        try:
            if cls.context is not None:
                cls.context.close()
        finally:
            if cls.loop is not None and not cls.loop.is_closed():
                cls.loop.close()
            cls.lock.__exit__(None, None, None)

    @classmethod
    def tearDownClass(cls) -> None:
        error = None
        try:
            if cls.context is not None and cls.context.state["resources"]:
                cls.context.cleanup_pending()
            if cls.context is not None:
                cls.context.print_report()
        except Exception as exc:
            error = exc
        finally:
            cls._close_session()
            super().tearDownClass()
        if error is not None:
            raise error

    def call_api(self, label, action):
        return self.context.call(label, action)
