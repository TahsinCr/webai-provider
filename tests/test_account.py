"""Account, credential store, and browser cookie import tests."""

import gc
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from webaiprovider import Provider
from webaiprovider.accounts import (
    Account,
    FileCredentialStore,
    MemoryCredentialStore,
    SessionState,
    SessionStatus,
    read_session,
    session_exists,
)
from webaiprovider.accounts.cookie_policy import ALLOWED_PROVIDER_ORIGINS
from webaiprovider.accounts.credentials import _SESSION_WRITE_LOCKS, _session_write_lock
from webaiprovider.accounts.filters import (
    ProviderCookieFilter,
    filter_provider_cookies,
)
from webaiprovider.accounts.importer import (
    CookieReader,
    import_browser_session,
)
from webaiprovider.base import Capability
from webaiprovider.errors import AccountPaused, ProviderError


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

            p.write_bytes(b"a" * 1_048_577)
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
                        self.assertFalse(hasattr(provider, "_loop"))
                        self.assertFalse(hasattr(provider, "_provider"))
                        status = provider.session_status()
                        self.assertEqual(status.state, SessionState.UNKNOWN)
                        self.assertTrue(status.saved_snapshot)
                    finally:
                        provider.close()
                    with self.assertRaisesRegex(ProviderError, "closing or is closed"):
                        provider.session_status()
        self.assertEqual(threads, [caller] * 4)

    def test_session_write_lock_entries_reclaimed_by_gc(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "ephemeral.json"
            lock = _session_write_lock(target)
            with lock:
                self.assertGreater(len(_SESSION_WRITE_LOCKS), 0)
            del lock
            gc.collect()
            self.assertNotIn(str(target.resolve()), _SESSION_WRITE_LOCKS)
            self.assertEqual(len(_SESSION_WRITE_LOCKS), 0)

    def test_verified_session_reuses_snapshot_until_credentials_change_or_forget(self):
        from unittest.mock import Mock

        from webaiprovider.resources.model import Model, ModelCatalog, ModelCatalogState

        account = Account.from_cookies("grok", {"sso": "token-1"})
        catalog = ModelCatalog(ModelCatalogState.AVAILABLE, (Model("fast", "Grok"),))

        first = Provider(account)
        first.client.authenticate = Mock()
        first._load_catalog = Mock(return_value=catalog)
        first.open()
        first.close()
        first.client.authenticate.assert_called_once()
        first._load_catalog.assert_called_once()

        second = Provider(account)
        second.client.authenticate = Mock()
        second._load_catalog = Mock(side_effect=AssertionError("expected cached catalog"))
        second.open()
        self.assertEqual([item.id for item in second.models.list()], ["fast"])
        second.client.authenticate.assert_not_called()

        # Credential change invalidates the verified snapshot automatically.
        account.store().save({"sso": "token-2"})
        third = Provider(account)
        third.client.authenticate = Mock()
        third._load_catalog = Mock(return_value=catalog)
        third.open()
        third.client.authenticate.assert_called_once()
        third._load_catalog.assert_called_once()

        # Explicit forget_session clears the cached verified snapshot.
        third.forget_session()
        third.close()
        second.close()
        self.assertIsNone(account._verified_session)

    def test_gemini_verified_bootstrap_is_reused_and_cleared_on_account_paused(self):
        from webaiprovider.providers.gemini.client import GeminiClient
        from webaiprovider.resources.model import Model, ModelCatalog, ModelCatalogState

        account = Account.from_cookies(
            "gemini", {"__Secure-1PSID": "sid-1", "OSID": "o-1", "__Secure-OSID": "so-1"}
        )
        catalog = ModelCatalog(ModelCatalogState.AVAILABLE, (Model("gemini-hydrogen", "Hydrogen"),))
        open_calls: list[dict[str, str] | None] = []

        def fake_open(client, *, verified_bootstrap=None):
            open_calls.append(verified_bootstrap)
            if verified_bootstrap is not None:
                client._apply_verified_bootstrap(verified_bootstrap)
            else:
                client._apply_bootstrap_html(
                    '"SNlM0e":"tok-1","cfb2h":"bld-1","FdrFJe":"fsid-1","TuX5cc":"en"'
                )
            return client

        with (
            patch.object(GeminiClient, "open", fake_open),
            patch.object(GeminiClient, "discover_models", return_value=catalog.models) as discover,
        ):
            first = Provider(account)
            first.open()
            first.close()

            second = Provider(account)
            second.open()
            self.assertEqual([item.id for item in second.models.list()], ["gemini-hydrogen"])
            self.assertEqual(second._client.token, "tok-1")
            with self.assertRaises(AccountPaused):
                with second._activity():
                    raise AccountPaused("expired")
            second.close()

        self.assertEqual(discover.call_count, 1)
        self.assertEqual(len(open_calls), 2)
        self.assertIsNone(open_calls[0])
        self.assertEqual(open_calls[1]["token"], "tok-1")
        self.assertIsNone(account._verified_session)


class AuthTests(unittest.TestCase):
    def test_browser_selection_and_import_share_provider_authentication_policy(self):
        from webaiprovider.accounts.cookies import select_cookies
        from webaiprovider.errors import ProfileSessionNotFoundError

        for provider, host, names in (
            ("chatgpt", "chatgpt.com", ("__Secure-authjs.session-token",)),
            ("grok", "grok.com", ("sso-rw",)),
            (
                "gemini",
                "gemini.google.com",
                (
                    "__Secure-1PSID",
                    "__Secure-1PSIDCC",
                    "__Secure-3PSIDCC",
                    "SIDCC",
                    "OSID",
                    "__Secure-OSID",
                ),
            ),
        ):
            with self.subTest(provider=provider):
                cookies = [
                    {
                        "name": name,
                        "value": "fixture",
                        "domain": (
                            host
                            if provider != "gemini" or name in {"OSID", "__Secure-OSID"}
                            else "google.com"
                        ),
                        "secure": True,
                    }
                    for name in names
                ]
                selected = select_cookies(cookies, provider)
                self.assertEqual(selected, dict.fromkeys(names, "fixture"))
                with patch.object(CookieReader, "read_provider_cookies", return_value=selected):
                    self.assertEqual(import_browser_session(f"https://{host}"), selected)
                with self.assertRaises(ProfileSessionNotFoundError):
                    select_cookies(
                        [dict(cookie, domain="example.test") for cookie in cookies], provider
                    )
                if provider == "grok":
                    with self.assertRaises(ProfileSessionNotFoundError):
                        select_cookies([dict(cookies[0], secure=False)], provider)

    def test_origin_validation(self):
        for origin in ALLOWED_PROVIDER_ORIGINS:
            self.assertEqual(ProviderCookieFilter.validate_origin(origin), origin)

        for invalid in ("https://untrusted.com", "http://chatgpt.com", "ftp://example.com"):
            with self.assertRaises(ValueError):
                ProviderCookieFilter.validate_origin(invalid)

    def test_cookie_filtering_and_sanitation(self):
        from webaiprovider.accounts import REQUIRED_PROVIDER_COOKIES

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
