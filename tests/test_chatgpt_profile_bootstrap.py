"""Profile import, account-session and provider bootstrap behavior."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .fakes import cookie


class ChatGPTProfileBootstrapTests(unittest.TestCase):
    def test_missing_saved_session_uses_the_common_authentication_error(self):
        from webaiprovider.errors import AuthenticationError
        from webaiprovider.providers.chatgpt import BrowserSession, SessionPaths

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            session = BrowserSession(SessionPaths(root / "missing.json", root / "profile"))
            with self.assertRaises(AuthenticationError):
                session.load()

    def test_bootstrap_saves_provider_token_and_browser_headers_without_cdp(self):
        from unittest.mock import MagicMock

        from webaiprovider.providers.chatgpt import BrowserSession, SessionPaths

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            session = BrowserSession(SessionPaths(root / "session.json", root / "profile"))
            http = MagicMock()
            http.get.return_value.status_code = 200
            http.get.return_value.json.return_value = {"accessToken": "verified-token"}
            http.cookies.jar = [
                cookie("__cf_bm", "fresh", domain=".chatgpt.com"),
                cookie("foreign", "ignore", domain=".evil.test"),
            ]
            factory = MagicMock()
            factory.return_value.__enter__.return_value = http
            with (
                patch("curl_cffi.requests.Session", factory),
                patch.object(session, "is_ready", side_effect=AssertionError("CDP")),
            ):
                session.import_cookies(
                    {"__Secure-next-auth.session-token": "login", "oai-did": "device"}
                )
            url, headers = session.load()
            self.assertEqual(url, "https://chatgpt.com/backend-api/conversations")
            self.assertEqual(headers["authorization"], "Bearer verified-token")
            self.assertIn("Chrome/", headers["user-agent"])
            self.assertEqual(headers["oai-device-id"], "device")
            self.assertIn("__cf_bm=fresh", headers["cookie"])
            self.assertNotIn("foreign", headers["cookie"])
            self.assertFalse(http.get.call_args.kwargs["allow_redirects"])
            self.assertEqual(http.get.call_args.args[0], "https://chatgpt.com/api/auth/session")

    def test_provider_open_prepares_cookie_credentials_before_catalog(self):
        import asyncio
        from unittest.mock import AsyncMock, patch

        from webaiprovider.accounts.account import Account
        from webaiprovider.accounts.filters import REQUIRED_PROVIDER_COOKIES
        from webaiprovider.providers.chatgpt.provider import AsyncChatGPTProvider

        cookie_name = REQUIRED_PROVIDER_COOKIES["https://chatgpt.com"]
        cookies = {f"{cookie_name}.0": "part-zero", f"{cookie_name}.1": "part-one"}
        provider = AsyncChatGPTProvider(Account.from_cookies("chatgpt", cookies))

        with (
            patch.object(provider.session, "import_cookies") as import_cookies,
            patch.object(provider, "_load_model_catalog", new_callable=AsyncMock) as load_catalog,
        ):
            asyncio.run(provider.prepare())

        import_cookies.assert_called_once_with(cookies)
        load_catalog.assert_awaited_once_with()

    def test_bootstrap_rejection_retains_previous_snapshot_and_does_not_follow_redirect(self):
        from unittest.mock import MagicMock

        from webaiprovider.errors import ProviderError
        from webaiprovider.providers.chatgpt import BrowserSession, SessionPaths

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            session = BrowserSession(SessionPaths(root / "session.json", root / "profile"))
            session.save(
                "https://chatgpt.com/backend-api/conversations", {"authorization": "previous"}
            )
            before = session.paths.session_file.read_bytes()
            http = MagicMock()
            http.get.return_value.status_code = 302
            factory = MagicMock()
            factory.return_value.__enter__.return_value = http
            with patch("curl_cffi.requests.Session", factory), self.assertRaises(ProviderError):
                session.import_cookies({"__Secure-next-auth.session-token": "login"})
            self.assertEqual(session.paths.session_file.read_bytes(), before)
            http.get.assert_called_once()
