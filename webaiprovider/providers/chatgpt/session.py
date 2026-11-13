#!/usr/bin/env python3
"""Browser authentication, Chrome lifecycle and caller-owned session snapshots."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from ...accounts import CredentialStore, read_session, save_session
from ...errors import AccountPaused
from ...utils.cdp import DevToolsError, capture_chatgpt_auth, is_devtools_available
from .constants import DEFAULT_CDP_URL, FORBIDDEN_REPLAY_HEADERS
from .errors import ChatGPTAuthenticationError, ChatGPTError

CHATGPT_ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class SessionPaths:
    session_file: Path | CredentialStore
    profile: Path = CHATGPT_ROOT / "login-profile"


def _private(path: Path, mode: int) -> None:
    if os.name != "nt":
        os.chmod(path, mode)


class BrowserSession:
    """Browser authentication with caller-supplied paths; no application state."""

    def __init__(self, paths: SessionPaths, cdp_url: str = DEFAULT_CDP_URL):
        self.paths = paths
        self.cdp_url = cdp_url

    def save(self, url: str, headers: dict[str, str]) -> None:
        path = self.paths.session_file
        if not isinstance(path, CredentialStore):
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _private(path.parent, 0o700)
        save_session(
            path, {"version": 1, "url": url, "headers": headers}, ensure_ascii=True, newline=True
        )
        if not isinstance(path, CredentialStore):
            _private(path, 0o600)

    def import_cookies(self, cookies: dict[str, str]) -> None:
        """Bootstrap a profile's web session without CDP or a running browser."""
        import re

        from curl_cffi.requests import Session
        from curl_cffi.requests.impersonate import DEFAULT_CHROME

        # Follow the maintained HTTP transport's browser version, not a frozen
        # application-specific Chrome version or the Python HTTP client UA.
        version = re.search(r"\d+", DEFAULT_CHROME).group()
        platform = (
            "Windows NT 10.0; Win64; x64"
            if sys.platform == "win32"
            else "Macintosh; Intel Mac OS X 10_15_7"
            if sys.platform == "darwin"
            else "X11; Linux x86_64"
        )
        headers = {
            "accept": "application/json",
            "origin": "https://chatgpt.com",
            "referer": "https://chatgpt.com/",
            "user-agent": f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{version}.0.0.0 Safari/537.36",
        }
        if cookies.get("oai-did"):
            headers["oai-device-id"] = cookies["oai-did"]
        with Session(impersonate="chrome", cookies=cookies, timeout=30) as client:
            from ...transport.http import (
                _invalidate_host_dns,
                _is_curl_timeout,
                _prepare_curl_session_sync,
            )

            _prepare_curl_session_sync(client, "chatgpt.com", 443)
            try:
                response = client.get(
                    "https://chatgpt.com/api/auth/session", headers=headers, allow_redirects=False
                )
            except Exception as exc:
                if _is_curl_timeout(exc):
                    _invalidate_host_dns("chatgpt.com", 443, client)
                raise
            if response.status_code != 200:
                raise ChatGPTError(
                    f"ChatGPT profile verification returned HTTP {response.status_code}"
                )
            try:
                token = response.json().get("accessToken")
            except (ValueError, AttributeError):
                token = None
            if not isinstance(token, str) or not token:
                raise ChatGPTError("Session could not be verified")
            values = dict(cookies)
            for cookie in client.cookies.jar:
                domain = (cookie.domain or "").lstrip(".")
                if domain == "chatgpt.com" and cookie.path == "/":
                    values[cookie.name] = cookie.value
        headers["authorization"] = "Bearer " + token
        headers["cookie"] = "; ".join(f"{name}={value}" for name, value in values.items())
        self.save("https://chatgpt.com/backend-api/conversations", headers)

    def load(self) -> tuple[str, dict[str, str]]:
        path = self.paths.session_file
        try:
            value = read_session(path)
            url = value["url"]
            headers = value["headers"]
        except (FileNotFoundError, AccountPaused) as error:
            raise ChatGPTAuthenticationError(
                "No saved ChatGPT session; authenticate the account"
            ) from error
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError, OSError) as error:
            raise ChatGPTAuthenticationError(
                "Saved ChatGPT session is invalid; verify the account"
            ) from error
        if not isinstance(url, str):
            raise ChatGPTAuthenticationError(
                "Saved ChatGPT session has an invalid origin; verify the account"
            )
        try:
            parsed = urlsplit(url)
        except ValueError as error:
            raise ChatGPTAuthenticationError(
                "Saved ChatGPT session has an invalid origin; verify the account"
            ) from error
        if parsed.scheme != "https" or parsed.netloc != "chatgpt.com":
            raise ChatGPTAuthenticationError(
                "Saved ChatGPT session has an invalid origin; verify the account"
            )
        if not isinstance(headers, dict) or not all(
            isinstance(key, str) and isinstance(item, str) for key, item in headers.items()
        ):
            raise ChatGPTAuthenticationError(
                "Saved ChatGPT session has invalid headers; verify the account"
            )
        return url, {key.lower(): value for key, value in headers.items()}

    def capture_auth(self) -> tuple[str, dict[str, str]]:
        try:
            url, captured_headers = capture_chatgpt_auth(self.cdp_url)
        except DevToolsError as error:
            raise ChatGPTError(str(error)) from error
        headers = {
            key.lower(): value
            for key, value in captured_headers.items()
            if isinstance(key, str)
            and isinstance(value, str)
            and key.lower() not in FORBIDDEN_REPLAY_HEADERS
        }
        return url, headers

    def is_ready(self) -> bool:
        return is_devtools_available(self.cdp_url)
