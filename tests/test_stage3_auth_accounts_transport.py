import os
import tempfile
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from unittest.mock import patch

import httpx

from webai_provider.accounts import (
    Account,
    FeatureState,
    FileCredentialStore,
    MemoryCredentialStore,
    SessionState,
    SessionStatus,
    read_session,
    save_session,
    session_exists,
)
from webai_provider.api import Capability
from webai_provider.auth import (
    ALLOWED_PROVIDER_ORIGINS,
    CookieReader,
    ProviderCookieFilter,
    filter_provider_cookies,
    import_browser_session,
)
from webai_provider.errors import (
    AccountPaused,
    ProviderError,
    RateLimited,
    RemoteNotFound,
)
from webai_provider.transport import (
    BoundedWorkPool,
    HttpSession,
    RetryPolicy,
    calculate_backoff,
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

            data = file_acc.to_dict()
            self.assertEqual(data["id"], "acc-2")
            self.assertEqual(data["credentials"], str(path))

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

    def test_session_status_and_features(self):
        status = SessionStatus(state=SessionState.AUTHENTICATED, saved_snapshot=True)
        self.assertEqual(status.state, SessionState.AUTHENTICATED)
        self.assertTrue(status.saved_snapshot)
        self.assertIsNone(status.reason)
        self.assertEqual(FeatureState.AVAILABLE.value, "available")


class AuthTests(unittest.TestCase):
    def test_origin_validation(self):
        for origin in ALLOWED_PROVIDER_ORIGINS:
            self.assertEqual(ProviderCookieFilter.validate_origin(origin), origin)

        for invalid in ("https://untrusted.com", "http://chatgpt.com", "ftp://example.com"):
            with self.assertRaises(ValueError):
                ProviderCookieFilter.validate_origin(invalid)

    def test_cookie_filtering_and_sanitation(self):
        valid = {"sessionKey": "sk_valid_123", "extra": "val"}
        filtered = ProviderCookieFilter.filter_cookies("https://claude.ai", valid)
        self.assertEqual(filtered, valid)

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

    def test_calculate_backoff_and_retry_policy(self):
        policy = RetryPolicy(max_attempts=3, base_backoff=1.0, max_backoff=10.0)
        self.assertTrue(policy.should_retry(429, attempt=0, method="POST"))
        self.assertTrue(policy.should_retry(502, attempt=1, method="GET"))
        self.assertFalse(policy.should_retry(502, attempt=1, method="POST"))
        self.assertFalse(policy.should_retry(500, attempt=0, method="GET"))
        self.assertFalse(policy.should_retry(429, attempt=3, method="GET"))

        backoff = policy.backoff_for(0, retry_after=5.0)
        self.assertEqual(backoff, 5.0)

        b0 = calculate_backoff(0, base=2.0, jitter=False)
        self.assertEqual(b0, 2.0)
        b1 = calculate_backoff(1, base=2.0, jitter=False)
        self.assertEqual(b1, 4.0)

    async def test_bounded_work_pool_json_offloading(self):
        pool = BoundedWorkPool(max_workers=2)
        try:
            small_data = '{"small": true}'
            parsed_small = await pool.parse_json(small_data, threshold=100)
            self.assertEqual(parsed_small, {"small": True})

            large_dict = {"large": "x" * 200}
            large_data = httpx.Response(200, json=large_dict).text
            parsed_large = await pool.parse_json(large_data, threshold=50)
            self.assertEqual(parsed_large, large_dict)
        finally:
            pool.shutdown(wait=True)

    async def test_http_session_origin_restriction_and_status_checks(self):
        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "claude.json"
            save_session(p, {"sessionKey": "sk-test"})
            session = HttpSession("https://claude.ai", p, "sessionKey")
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
                session.check(httpx.Response(401))

            with self.assertRaises(AccountPaused):
                session.check(httpx.Response(302))

            with self.assertRaises(ProviderError):
                session.check(httpx.Response(403))

            with self.assertRaises(RemoteNotFound):
                session.check(httpx.Response(404))

            with self.assertRaises(RateLimited) as ctx:
                session.check(httpx.Response(429, headers={"retry-after": "15"}))
            self.assertEqual(ctx.exception.retry_after, 15.0)
