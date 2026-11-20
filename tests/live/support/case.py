from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import unittest
from pathlib import Path
from typing import Any

from .actions import (
    DEFAULT_ACTION_INTERVAL,
    DEFAULT_COMBINED_RUN_LIMIT,
    DEFAULT_SINGLE_API_LIMIT,
    ActionGate,
)
from .config import (
    LiveAccount,
    load_or_import_account,
    lock_file_guard,
    save_verified_browser_refresh,
)
from .context import LiveContext, _provider_http_status, _safe_error_label
from .errors import LiveApiError, SafetyError
from .journal import RunLock, read_cleanup_journal

_DEFAULT_ACTION_GATE = ActionGate(interval=DEFAULT_ACTION_INTERVAL, limit=DEFAULT_SINGLE_API_LIMIT)
_ACTION_GATE = _DEFAULT_ACTION_GATE
_SHARED_ACTION_GATE: tuple[tuple[str, int], ActionGate] | None = None
_FAILED_SESSION_OPENS: set[tuple[str, str]] = set()
_SHARED_ASYNC_RUNNER: asyncio.Runner | None = None
_SHARED_ASYNC_SESSION: tuple[Any, LiveContext] | None = None
_SHARED_CLEANUP_REGISTERED = False


def _close_shared_async_session() -> None:
    global _SHARED_ASYNC_RUNNER, _SHARED_ASYNC_SESSION
    session_pair, _SHARED_ASYNC_SESSION = _SHARED_ASYNC_SESSION, None
    runner, _SHARED_ASYNC_RUNNER = _SHARED_ASYNC_RUNNER, None
    if session_pair is not None and runner is not None:
        try:
            runner.run(session_pair[0].close_async())
        except Exception:
            pass
    if runner is not None:
        try:
            runner.close()
        except Exception:
            pass


def _get_shared_async_runner(loop_factory: Any = None) -> asyncio.Runner:
    global _SHARED_ASYNC_RUNNER, _SHARED_CLEANUP_REGISTERED
    if _SHARED_ASYNC_RUNNER is None:
        _SHARED_ASYNC_RUNNER = asyncio.Runner(debug=False, loop_factory=loop_factory)
        if not _SHARED_CLEANUP_REGISTERED:
            atexit.register(_close_shared_async_session)
            _SHARED_CLEANUP_REGISTERED = True
    return _SHARED_ASYNC_RUNNER


def _resolve_action_gate() -> ActionGate:
    global _SHARED_ACTION_GATE
    if _ACTION_GATE is not _DEFAULT_ACTION_GATE:
        return _ACTION_GATE
    state_raw = os.environ.get("WEBAI_LIVE_GATE_FILE", "").strip()
    limit_raw = os.environ.get("WEBAI_LIVE_GATE_LIMIT", "").strip()
    if not state_raw and not limit_raw:
        return _ACTION_GATE
    limit = DEFAULT_SINGLE_API_LIMIT
    if limit_raw:
        try:
            limit = max(1, min(int(limit_raw), DEFAULT_COMBINED_RUN_LIMIT))
        except ValueError:
            raise SafetyError("WEBAI_LIVE_GATE_LIMIT must be a positive integer") from None
    key = (state_raw, limit)
    if _SHARED_ACTION_GATE is None or _SHARED_ACTION_GATE[0] != key:
        state_file = Path(state_raw) if state_raw else None
        _SHARED_ACTION_GATE = (
            key,
            ActionGate(interval=DEFAULT_ACTION_INTERVAL, limit=limit, state_file=state_file),
        )
    return _SHARED_ACTION_GATE[1]


def _append_report_entry(api: str, test_id: str, status: str, feature_skips: list[str]) -> None:
    report_raw = os.environ.get("WEBAI_LIVE_REPORT_FILE", "").strip()
    if not report_raw:
        return
    report_path = Path(report_raw)
    if report_path.is_symlink():
        raise SafetyError("Live report path must not be a symlink")
    lock_path = report_path.with_name(f".{report_path.name}.lock")
    with lock_file_guard(lock_path):
        try:
            data = json.loads(report_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            data = {
                "provider": os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold(),
                "api": api,
                "tests": [],
            }
        if not isinstance(data, dict) or not isinstance(data.get("tests"), list):
            data = {
                "provider": os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold(),
                "api": api,
                "tests": [],
            }
        data["tests"].append(
            {
                "id": test_id,
                "status": status,
                "feature_skips": list(feature_skips),
            }
        )
        tmp = report_path.with_suffix(".tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(tmp, flags, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, report_path)


def _session_key(api: str) -> tuple[str, str]:
    return os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold(), api


def _skip_after_session_open_failure(api: str) -> None:
    key = _session_key(api)
    if key in _FAILED_SESSION_OPENS:
        raise unittest.SkipTest(
            f"{key[0]} session setup already failed; no further provider requests were sent"
        )


def _record_session_open_failure(api: str) -> None:
    _FAILED_SESSION_OPENS.add(_session_key(api))


async def _run_blocking(action, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(action, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except BaseException:
        await asyncio.gather(task, return_exceptions=True)
        raise


class LiveSession:
    """Own one real provider session and its guarded cleanup lifecycle."""

    def __init__(self) -> None:
        self.lock: RunLock | None = None
        self.context: LiveContext | None = None
        self.session: Any = None

    def _prepare(self) -> LiveContext:
        provider = os.environ.get("WEBAI_LIVE_PROVIDER", "").casefold()
        alias = os.environ.get("WEBAI_LIVE_ACCOUNT", "")
        self.lock = RunLock(provider)
        self.lock.__enter__()
        try:
            account = load_or_import_account(
                provider,
                alias,
                browser=os.environ.get("WEBAI_LIVE_BROWSER"),
            )
            state, journal_path = read_cleanup_journal(provider)
            recover = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
            if state and (state["provider"] != provider or state["account"] != account.alias):
                raise SafetyError("Pending cleanup belongs to a different account")
            if state and not recover:
                raise SafetyError("Recover pending test data before starting another live run")
            if recover and state is None:
                raise SafetyError("No pending cleanup journal exists")

            for logger_name in ("curl_cffi", "websockets", "urllib3"):
                logging.getLogger(logger_name).setLevel(logging.WARNING)
            return self._new_context(account, state, journal_path=journal_path)
        except BaseException:
            self._release_lock()
            raise

    def _new_context(
        self, account: LiveAccount, state=None, *, journal_path: Path | None = None
    ) -> LiveContext:
        self.session = self._create_provider_session(account)
        self.context = LiveContext(
            self.session,
            account,
            state=state,
            gate=_resolve_action_gate(),
            journal_path=journal_path,
        )
        return self.context

    def _create_provider_session(self, account: LiveAccount) -> Any:
        from webaiprovider.accounts.account import Account
        from webaiprovider.async_provider import AsyncProvider

        account_object = Account(
            id=account.alias,
            provider=account.provider,
            credentials=account.credentials,
            name="Live API test",
        )
        return AsyncProvider(account_object)

    @staticmethod
    def _retryable_open_error(error: LiveApiError) -> bool:
        from curl_cffi.requests.exceptions import Timeout as CurlTimeout

        if not str(error).startswith("session.open failed: "):
            return False
        cause = error.__context__
        if isinstance(cause, (CurlTimeout, TimeoutError)):
            return True
        return isinstance(cause, Exception) and _provider_http_status(cause) in {
            "500",
            "502",
            "503",
            "504",
        }

    async def open_async(self) -> LiveContext:
        try:
            return await self._open_async_once()
        except LiveApiError as error:
            if not self._retryable_open_error(error):
                raise
            print("Retrying session.open once after a transient failure", flush=True)
            return await self._open_async_once()

    async def _open_async_once(self) -> LiveContext:
        from webaiprovider.errors import AuthenticationError

        try:
            context = await _run_blocking(self._prepare)
        except BaseException:
            await self._close_async(suppress=True)
            self._release_lock()
            raise
        try:
            try:
                await context.call_async(
                    "session.open",
                    self.session.open,
                    expected_errors=(AuthenticationError,),
                )
            except AuthenticationError as error:
                reason = _safe_error_label(error)
                account, state = context.account, context.state
                if account.alias != "default-browser":
                    raise SafetyError(
                        f"{account.provider} rejected the configured account credentials ({reason})"
                    ) from None
                await self._close_async()
                refreshed = await _run_blocking(
                    load_or_import_account,
                    account.provider,
                    account.alias,
                    browser=os.environ.get("WEBAI_LIVE_BROWSER"),
                    refresh=True,
                )
                old_cookies = account.credentials.get("cookies", account.credentials)
                new_cookies = refreshed.credentials.get("cookies", refreshed.credentials)
                if old_cookies == new_cookies:
                    raise SafetyError(
                        f"{account.provider} rejected the browser credentials ({reason}) and the refresh returned the same cookies"
                    ) from None
                context = self._new_context(refreshed, state, journal_path=context._journal_path)
                try:
                    await context.call_async(
                        "session.open.after_cookie_refresh",
                        self.session.open,
                        expected_errors=(AuthenticationError,),
                    )
                except AuthenticationError as error:
                    raise SafetyError(
                        f"{account.provider} browser cookies remain invalid after one refresh "
                        f"({_safe_error_label(error)})"
                    ) from None
                await _run_blocking(save_verified_browser_refresh, account, refreshed)
        except BaseException:
            await self._close_async(suppress=True)
            self._release_lock()
            raise
        return context

    async def _close_async(self, *, suppress: bool = False) -> None:
        context, self.context = self.context, None
        self.session = None
        if context is not None:
            try:
                await context.aclose()
            except Exception:
                if not suppress:
                    raise

    async def close_async(self) -> None:
        error: Exception | None = None
        try:
            if self.context is not None:
                resources = self.context.state["resources"]
                attempted = getattr(self.context, "_cleanup_attempted", None)
                should_cleanup = (
                    any(id(entry) not in attempted for entry in resources)
                    if isinstance(attempted, (set, frozenset))
                    else bool(resources)
                )
                if should_cleanup:
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


_ORIGINAL_LIVE_SESSION = LiveSession


class _LiveReportingMixin(unittest.TestCase):
    """Capture non-secret per-scenario outcomes for the live runner summary."""

    _live_api: str = ""
    recovery_only = False

    def run(self, result: unittest.TestResult | None = None):
        if result is None or not os.environ.get("WEBAI_LIVE_REPORT_FILE"):
            return super().run(result)
        self._live_ignore_skip = False
        self._live_skip_kind: str | None = None
        self._live_feature_skips_start: int | None = None
        ctx_before = getattr(self, "context", None)
        before_feature_skips = (
            len(getattr(ctx_before, "skipped", ())) if ctx_before is not None else 0
        )
        before_bad = len(result.failures) + len(result.errors)
        before_skipped = len(result.skipped)
        returned = super().run(result)
        active_api = os.environ.get("WEBAI_LIVE_API", "async").casefold()
        recovering = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
        enabled = os.environ.get("WEBAI_LIVE_TESTS") == "1"
        if (
            self._live_ignore_skip
            or not enabled
            or active_api != self._live_api
            or recovering != self.recovery_only
        ):
            return returned
        after_bad = len(result.failures) + len(result.errors)
        after_skipped = len(result.skipped)
        if after_bad > before_bad:
            status = "failed"
        elif after_skipped > before_skipped:
            status = self._live_skip_kind or "skipped_unsupported"
        else:
            status = "passed"
        ctx = getattr(self, "context", None)
        if ctx is None:
            feature_skips: list[str] = []
        elif self._live_feature_skips_start is not None:
            feature_skips = list(getattr(ctx, "skipped", ())[self._live_feature_skips_start :])
        elif ctx is ctx_before:
            feature_skips = list(getattr(ctx, "skipped", ())[before_feature_skips:])
        else:
            feature_skips = list(getattr(ctx, "skipped", ()))
        if status == "passed" and feature_skips:
            status = "partial"
        if status == "skipped_rate_limited":
            feature_skips = [
                label for label in feature_skips if not label.endswith(": rate_limited")
            ]
        _append_report_entry(self._live_api, self.id(), status, feature_skips)
        return returned

    def _skip_on_rate_limit(
        self,
        label: str,
        exc: Exception,
        entry: dict[str, Any] | None,
        chat: Any,
    ) -> None:
        if entry is not None:
            if chat is not None:
                self.context.record_chat_id(entry, chat)
            if getattr(exc, "message_submitted", None) is False and not entry.get("remote_id"):
                entry["attempted"] = False
                self.context.save()
        self.context.skip(f"{label}: rate_limited")
        self._live_skip_kind = "skipped_rate_limited"
        self.skipTest("Provider rate limit reached")


class LiveAsyncTestCase(_LiveReportingMixin, unittest.IsolatedAsyncioTestCase):
    """Base for one asynchronous live feature scenario."""

    _live_api = "async"
    recovery_only = False
    use_shared_session = True

    def _uses_shared_session(self) -> bool:
        return (
            hasattr(asyncio, "Runner")
            and self.use_shared_session
            and not self.recovery_only
            and os.environ.get("WEBAI_LIVE_TESTS") == "1"
            and os.environ.get("WEBAI_LIVE_API", "async").casefold() == "async"
            and os.environ.get("WEBAI_LIVE_SHARED_SESSION", "1") == "1"
            and LiveSession is _ORIGINAL_LIVE_SESSION
        )

    def _setupAsyncioRunner(self) -> None:
        if self._uses_shared_session():
            self._asyncioRunner = _get_shared_async_runner(getattr(self, "loop_factory", None))
            self._owns_shared_runner = False
            return
        super()._setupAsyncioRunner()
        self._owns_shared_runner = True

    def _tearDownAsyncioRunner(self) -> None:
        if getattr(self, "_owns_shared_runner", True):
            super()._tearDownAsyncioRunner()

    async def asyncSetUp(self) -> None:
        global _SHARED_ASYNC_SESSION
        if os.environ.get("WEBAI_LIVE_TESTS") != "1":
            self._live_ignore_skip = True
            self.skipTest("Set WEBAI_LIVE_TESTS=1 to opt in to real provider requests")
        if os.environ.get("WEBAI_LIVE_API", "async").casefold() != "async":
            self._live_ignore_skip = True
            self.skipTest("This run selected the synchronous API")
        recovering = os.environ.get("WEBAI_LIVE_RECOVER") == "1"
        if recovering != self.recovery_only:
            self._live_ignore_skip = True
            self.skipTest("Recovery mode runs only cleanup recovery tests")
        try:
            _skip_after_session_open_failure("async")
        except unittest.SkipTest:
            self._live_skip_kind = "skipped_setup_failed"
            raise
        if self._uses_shared_session():
            if _SHARED_ASYNC_SESSION is None:
                live = LiveSession()
                try:
                    context = await live.open_async()
                except Exception:
                    _record_session_open_failure("async")
                    raise
                _SHARED_ASYNC_SESSION = (live, context)
            self.live, self.context = _SHARED_ASYNC_SESSION
            self._owns_live_session = False
            self._live_feature_skips_start = len(getattr(self.context, "skipped", ()))
            return
        self.live = LiveSession()
        self._owns_live_session = True
        try:
            self.context = await self.live.open_async()
        except Exception:
            _record_session_open_failure("async")
            raise
        self._live_feature_skips_start = len(getattr(self.context, "skipped", ()))

    async def asyncTearDown(self) -> None:
        if getattr(self, "_owns_live_session", True) and hasattr(self, "live"):
            await self.live.close_async()

    async def call_api(
        self, label: str, action, *, expected_errors: tuple[type[Exception], ...] = ()
    ):
        return await self.context.call_async(label, action, expected_errors=expected_errors)

    async def call_or_skip_rate_limit(
        self,
        label: str,
        action,
        *,
        entry: dict[str, Any] | None = None,
        chat: Any = None,
    ):
        from webaiprovider.errors import RateLimited

        async def attempt():
            if not entry["attempted"]:
                entry["attempted"] = True
                self.context.save()
            try:
                return await action()
            finally:
                if chat is not None:
                    self.context.record_chat_id(entry, chat)

        try:
            return await self.call_api(
                label, attempt if entry is not None else action, expected_errors=(RateLimited,)
            )
        except RateLimited as exc:
            self._skip_on_rate_limit(label, exc, entry, chat)


class LiveAsyncRecoveryTestCase(LiveAsyncTestCase):
    """Recovery-only live test base for the asynchronous API."""

    recovery_only = True
