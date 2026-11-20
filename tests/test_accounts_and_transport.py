import os
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import patch

from tests.fakes import FakeHTTPResponse
from webaiprovider import Provider
from webaiprovider.accounts import (
    Account,
    FileCredentialStore,
    MemoryCredentialStore,
    SessionState,
    SessionStatus,
    read_session,
    save_session,
    session_exists,
)
from webaiprovider.accounts.filters import (
    ALLOWED_PROVIDER_ORIGINS,
    ProviderCookieFilter,
    filter_provider_cookies,
)
from webaiprovider.accounts.importer import (
    CookieReader,
    import_browser_session,
)
from webaiprovider.base import Capability
from webaiprovider.errors import (
    AccountPaused,
    ProviderError,
    RateLimited,
    RemoteNotFound,
)
from webaiprovider.transport import (
    AsyncHttpSession,
    parse_retry_after,
)


class AccountsTests(unittest.TestCase):
    def test_account_creation_and_store_resolution(self):
        mem_acc = Account(
            id="acc-1",
            provider="chatgpt",
            credentials={"session": "token123"},
            capabilities=(Capability.CHAT,),
            name="Primary",
        )
        self.assertEqual(mem_acc.id, "acc-1")
        self.assertEqual(mem_acc.provider, "chatgpt")
        self.assertEqual(mem_acc.name, "Primary")
        self.assertTrue(mem_acc.is_active)
        store = mem_acc.store()
        self.assertIsInstance(store, MemoryCredentialStore)
        self.assertEqual(store.load(), {"session": "token123"})

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "creds.json"
            file_acc = Account(
                id="acc-2",
                provider="claude",
                credentials=path,
            )
            file_store = file_acc.store()
            self.assertIsInstance(file_store, FileCredentialStore)
            file_store.save({"sessionKey": "sk-123"})
            self.assertTrue(file_store.exists())
            self.assertEqual(file_store.load(), {"sessionKey": "sk-123"})

    def test_file_credential_store_lifecycle_and_permissions(self):
        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "sub" / "store.json"
            store = FileCredentialStore(p)
            self.assertFalse(store.exists())
            self.assertFalse(session_exists(store))

            with self.assertRaises(AccountPaused):
                store.load()

            payload = {"auth": "secret_abc"}
            store.save(payload)
            self.assertTrue(store.exists())
            self.assertTrue(session_exists(p))
            self.assertEqual(store.load(), payload)
            self.assertEqual(read_session(store), payload)
            self.assertEqual(read_session(p), payload)

            if os.name != "nt":
                self.assertEqual(p.stat().st_mode & 0o777, 0o600)

            p.write_text("invalid json")
            with self.assertRaises(AccountPaused):
                store.load()

            p.write_bytes(b"a" * (1_048_577))
            with self.assertRaises(AccountPaused):
                store.load()

            store.delete()
            self.assertFalse(store.exists())
            self.assertFalse(session_exists(p))

    def test_memory_credential_store_lifecycle(self):
        store = MemoryCredentialStore()
        self.assertFalse(store.exists())
        self.assertFalse(session_exists(store))

        with self.assertRaises(AccountPaused):
            store.load()

        store.save({"key": "value"})
        self.assertTrue(store.exists())
        self.assertEqual(store.load(), {"key": "value"})

        store.delete()
        self.assertFalse(store.exists())
        with self.assertRaises(AccountPaused):
            store.load()

    def test_session_status(self):
        status = SessionStatus(state=SessionState.AUTHENTICATED, saved_snapshot=True)
        self.assertEqual(status.state, SessionState.AUTHENTICATED)
        self.assertTrue(status.saved_snapshot)
        self.assertIsNone(status.reason)

    def test_blocking_provider_reads_session_status_on_caller_without_starting_loop(self):
        caller = threading.get_ident()
        threads = []

        def exists(_store):
            threads.append(threading.get_ident())
            return True

        with patch.object(MemoryCredentialStore, "exists", exists):
            for kind in ("chatgpt", "claude", "gemini", "grok"):
                with self.subTest(provider=kind):
                    provider = Provider(Account.from_cookies(kind, {"fixture": "saved"}))
                    try:
                        with patch.object(
                            provider,
                            "_call",
                            side_effect=AssertionError("Status must not start the loop"),
                        ):
                            status = provider.session_status()
                        self.assertEqual(status.state, SessionState.UNKNOWN)
                        self.assertTrue(status.saved_snapshot)
                    finally:
                        provider.close()
                    with self.assertRaisesRegex(RuntimeError, "provider is closed"):
                        provider.session_status()
        self.assertEqual(threads, [caller] * 4)


class AuthTests(unittest.TestCase):
    def test_browser_selection_and_import_share_provider_authentication_policy(self):
        from webaiprovider.accounts.cookies import select_cookies
        from webaiprovider.errors import ProfileSessionNotFoundError

        for provider, host, name in (
            ("chatgpt", "chatgpt.com", "__Secure-authjs.session-token"),
            ("grok", "grok.com", "sso-rw"),
        ):
            with self.subTest(provider=provider):
                cookie = {"name": name, "value": "fixture", "domain": host, "secure": True}
                selected = select_cookies([cookie], provider)
                with patch.object(CookieReader, "read_provider_cookies", return_value=selected):
                    self.assertEqual(import_browser_session(f"https://{host}"), selected)
                with self.assertRaises(ProfileSessionNotFoundError):
                    select_cookies([dict(cookie, domain="example.test")], provider)
                if provider == "grok":
                    with self.assertRaises(ProfileSessionNotFoundError):
                        select_cookies([dict(cookie, secure=False)], provider)

    def test_origin_validation(self):
        for origin in ALLOWED_PROVIDER_ORIGINS:
            self.assertEqual(ProviderCookieFilter.validate_origin(origin), origin)

        for invalid in ("https://untrusted.com", "http://chatgpt.com", "ftp://example.com"):
            with self.assertRaises(ValueError):
                ProviderCookieFilter.validate_origin(invalid)

    def test_cookie_filtering_and_sanitation(self):
        from webaiprovider.accounts.filters import REQUIRED_PROVIDER_COOKIES

        valid = {"sessionKey": "sk_valid_123", "extra": "val"}
        filtered = ProviderCookieFilter.filter_cookies("https://claude.ai", valid)
        self.assertEqual(filtered, valid)

        chatgpt_cookie = REQUIRED_PROVIDER_COOKIES["https://chatgpt.com"]
        chunked = {f"{chatgpt_cookie}.0": "chunk-zero", f"{chatgpt_cookie}.1": "chunk-one"}
        self.assertEqual(
            ProviderCookieFilter.filter_cookies("https://chatgpt.com", chunked), chunked
        )
        incomplete = {f"{chatgpt_cookie}.0": "chunk-zero", f"{chatgpt_cookie}.2": "chunk-two"}
        with self.assertRaises(AccountPaused):
            ProviderCookieFilter.filter_cookies("https://chatgpt.com", incomplete)
        with self.assertRaises(AccountPaused):
            ProviderCookieFilter.filter_cookies("https://chatgpt.com", {1: "invalid"})

        with self.assertRaises(AccountPaused):
            ProviderCookieFilter.filter_cookies("https://claude.ai", {"other": "token"})

        with self.assertRaises(AccountPaused):
            ProviderCookieFilter.filter_cookies("https://claude.ai", {})

        with self.assertRaises(AccountPaused):
            ProviderCookieFilter.filter_cookies(
                "https://claude.ai", {"sessionKey": "invalid\r\ntoken"}
            )

        with self.assertRaises(AccountPaused):
            ProviderCookieFilter.filter_cookies(
                "https://claude.ai", {"bad\nkey": "val", "sessionKey": "ok"}
            )

        gemini_valid = {"__Secure-1PSID": "sid_123"}
        self.assertEqual(
            filter_provider_cookies("https://gemini.google.com", gemini_valid), gemini_valid
        )

    def test_domain_matches(self):
        self.assertTrue(ProviderCookieFilter.domain_matches("claude.ai", "claude.ai"))
        self.assertTrue(ProviderCookieFilter.domain_matches("claude.ai", ".claude.ai"))
        self.assertTrue(ProviderCookieFilter.domain_matches("sub.claude.ai", ".claude.ai"))
        self.assertFalse(ProviderCookieFilter.domain_matches("evilclaude.ai", "claude.ai"))
        self.assertFalse(ProviderCookieFilter.domain_matches("claude.ai.attacker.com", "claude.ai"))

    def test_cookie_reader_provider_for_origin(self):
        self.assertEqual(CookieReader.provider_for_origin("https://chatgpt.com"), "chatgpt")
        self.assertEqual(CookieReader.provider_for_origin("https://claude.ai/chat"), "claude")
        self.assertEqual(CookieReader.provider_for_origin("gemini"), "gemini")
        with self.assertRaises(ValueError):
            CookieReader.provider_for_origin("https://unknown.com")

    def test_import_browser_session_with_mock_reader(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "imported.json"
            store = FileCredentialStore(path)

            mock_cookies = {"sso": "grok_sso_cookie"}
            with patch.object(CookieReader, "read_provider_cookies", return_value=mock_cookies):
                imported = import_browser_session("https://grok.com", target=store)
                self.assertEqual(imported, mock_cookies)
                self.assertTrue(store.exists())
                self.assertEqual(store.load(), mock_cookies)


class TransportTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_retry_after(self):
        self.assertIsNone(parse_retry_after(None))
        self.assertIsNone(parse_retry_after(""))
        self.assertIsNone(parse_retry_after("   "))
        self.assertIsNone(parse_retry_after("not-a-number-or-date"))
        self.assertEqual(parse_retry_after(45), 45.0)
        self.assertEqual(parse_retry_after("120"), 120.0)
        self.assertEqual(parse_retry_after("15.5"), 15.5)

        base_time = 1700000000.0
        dt = datetime.fromtimestamp(base_time + 40, tz=timezone.utc)
        header_val = format_datetime(dt, usegmt=True)
        self.assertEqual(parse_retry_after(header_val, now=base_time), 40.0)

        dt_past = datetime.fromtimestamp(base_time - 10, tz=timezone.utc)
        past_val = format_datetime(dt_past, usegmt=True)
        self.assertEqual(parse_retry_after(past_val, now=base_time), 0.0)

    async def test_http_session_origin_restriction_and_status_checks(self):
        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "claude.json"
            save_session(p, {"sessionKey": "sk-test"})
            session = AsyncHttpSession("https://claude.ai", p, "sessionKey")
            self.assertEqual(session.url("/api/user"), "https://claude.ai/api/user")

            for invalid in (
                "https://other.com/api",
                "//other.com/api",
                "/../outside",
                "relative/path",
            ):
                with self.assertRaises(ValueError):
                    session.url(invalid)

            with self.assertRaises(AccountPaused):
                session.check(FakeHTTPResponse(401))

            with self.assertRaises(AccountPaused):
                session.check(FakeHTTPResponse(302))

            with self.assertRaises(ProviderError):
                session.check(FakeHTTPResponse(403))

            with self.assertRaises(RemoteNotFound):
                session.check(FakeHTTPResponse(404))

            with self.assertRaises(RateLimited) as ctx:
                session.check(FakeHTTPResponse(429, headers={"retry-after": "15"}))
            self.assertEqual(ctx.exception.retry_after, 15.0)

    def test_session_write_lock_entries_reclaimed_by_gc(self):
        import gc
        import tempfile

        from webaiprovider.accounts.credentials import _SESSION_WRITE_LOCKS, _session_write_lock

        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "ephemeral.json"
            lock = _session_write_lock(target)
            with lock:
                self.assertGreater(len(_SESSION_WRITE_LOCKS), 0)
            del lock
            gc.collect()
            self.assertNotIn(str(target.resolve()), _SESSION_WRITE_LOCKS)
            self.assertEqual(len(_SESSION_WRITE_LOCKS), 0)
