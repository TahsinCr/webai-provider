"""Browser authentication, and caller-owned session snapshots."""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Iterable
from http.cookiejar import Cookie
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from ...accounts.cookie_policy import CookiePolicy
from ...accounts.credentials import (
    CredentialStore,
    read_session,
    read_session_async,
    save_session,
    save_session_async,
)
from ...errors import AccountPaused
from .constants import DEFAULT_CDP_URL, FORBIDDEN_REPLAY_HEADERS
from .errors import ChatGPTAuthenticationError, ChatGPTError

if TYPE_CHECKING:
    from curl_cffi.requests import Response


COOKIE_POLICY = CookiePolicy(
    origin="https://chatgpt.com",
    required="__Secure-next-auth.session-token",
    names=frozenset(
        {
            "__Host-next-auth.csrf-token",
            "__Secure-next-auth.session-token",
            "__Secure-authjs.session-token",
            "__cf_bm",
            "cf_clearance",
            "oai-did",
        }
    ),
    allow_chunks=True,
    alternatives=("__Secure-authjs.session-token",),
    critical_names=("__cf_bm", "cf_clearance"),
)


def _private(path: Path, mode: int) -> None:
    if os.name != "nt":
        os.chmod(path, mode)


class BrowserSession:
    """Browser authentication with caller-supplied paths; no application state."""

    def __init__(self, session_file: Path | CredentialStore, cdp_url: str = DEFAULT_CDP_URL):
        self.session_file = session_file
        self.cdp_url = cdp_url

    def _prepare_save_target(self) -> CredentialStore | Path:
        path = self.session_file
        if not isinstance(path, CredentialStore):
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _private(path.parent, 0o700)
        return path

    def _finalize_save_target(self, path: CredentialStore | Path) -> None:
        if not isinstance(path, CredentialStore):
            _private(path, 0o600)

    def save(self, url: str, headers: dict[str, str]) -> None:
        path = self._prepare_save_target()
        save_session(
            path, {"version": 1, "url": url, "headers": headers}, ensure_ascii=True, newline=True
        )
        self._finalize_save_target(path)

    async def save_async(self, url: str, headers: dict[str, str]) -> None:
        path = self._prepare_save_target()
        await save_session_async(
            path, {"version": 1, "url": url, "headers": headers}, ensure_ascii=True, newline=True
        )
        self._finalize_save_target(path)

    @staticmethod
    def _bootstrap_headers(cookies: dict[str, str]) -> dict[str, str]:
        import re

        from curl_cffi.requests.impersonate import DEFAULT_CHROME

        # Follow the maintained HTTP transport's browser version, not a frozen
        # application-specific Chrome version or the Python HTTP client UA.
        match = re.search(r"\d+", DEFAULT_CHROME)
        if match is None:
            raise ChatGPTError("The transport did not provide a Chrome version")
        version = match.group()
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
        return headers

    @staticmethod
    def _finalize_imported_headers(
        headers: dict[str, str], cookies: dict[str, str], response: Response, jar: Iterable[Cookie]
    ) -> dict[str, str]:
        if response.status_code != 200:
            raise ChatGPTError(f"ChatGPT profile verification returned HTTP {response.status_code}")
        try:
            token = response.json().get("accessToken")
        except (ValueError, AttributeError):
            token = None
        if not isinstance(token, str) or not token:
            raise ChatGPTError("Session could not be verified")
        values = dict(cookies)
        for cookie in jar:
            domain = (cookie.domain or "").lstrip(".")
            if domain == "chatgpt.com" and cookie.path == "/" and cookie.value is not None:
                values[cookie.name] = cookie.value
        headers["authorization"] = "Bearer " + token
        headers["cookie"] = "; ".join(f"{name}={value}" for name, value in values.items())
        return headers

    def import_cookies(self, cookies: dict[str, str]) -> None:
        """Bootstrap a profile's web session without CDP or a running browser."""
        from curl_cffi.requests import Session

        from ...transport.http import (
            _invalidate_host_dns,
            _is_curl_timeout,
            _prepare_curl_session_sync,
            curl_cookie_jar,
        )

        headers = self._bootstrap_headers(cookies)
        with Session(
            impersonate="chrome",
            cookies=curl_cookie_jar(cookies),
            timeout=30,
            trust_env=False,
        ) as client:
            _prepare_curl_session_sync(client, "chatgpt.com", 443)
            try:
                response = client.get(
                    "https://chatgpt.com/api/auth/session", headers=headers, allow_redirects=False
                )
            except Exception as exc:
                if _is_curl_timeout(exc):
                    _invalidate_host_dns("chatgpt.com", 443, client)
                raise
            final_headers = self._finalize_imported_headers(
                headers, cookies, response, client.cookies.jar
            )
        self.save("https://chatgpt.com/backend-api/conversations", final_headers)

    async def import_cookies_async(self, cookies: dict[str, str]) -> None:
        """Bootstrap a profile's web session natively on the active event loop."""
        from curl_cffi.requests import AsyncSession

        from ...transport.async_http import _prepare_curl_session_async
        from ...transport.http import (
            _invalidate_host_dns,
            _is_curl_timeout,
            curl_cookie_jar,
        )

        headers = self._bootstrap_headers(cookies)
        async with AsyncSession(
            impersonate="chrome",
            cookies=curl_cookie_jar(cookies),
            timeout=(10.0, 30.0),
            trust_env=False,
        ) as client:
            await _prepare_curl_session_async(client, "chatgpt.com", 443, timeout=10.0)
            try:
                response = await client.get(
                    "https://chatgpt.com/api/auth/session", headers=headers, allow_redirects=False
                )
            except Exception as exc:
                if _is_curl_timeout(exc):
                    _invalidate_host_dns("chatgpt.com", 443, client)
                raise
            final_headers = self._finalize_imported_headers(
                headers, cookies, response, client.cookies.jar
            )
        await self.save_async("https://chatgpt.com/backend-api/conversations", final_headers)

    @staticmethod
    def _validate_loaded_record(value: dict) -> tuple[str, dict[str, str]]:
        try:
            url = value["url"]
            headers = value["headers"]
        except (KeyError, TypeError) as error:
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
        return url, {key.lower(): item for key, item in headers.items()}

    def load(self) -> tuple[str, dict[str, str]]:
        path = self.session_file
        try:
            value = read_session(path)
        except (FileNotFoundError, AccountPaused) as error:
            raise ChatGPTAuthenticationError(
                "No saved ChatGPT session; authenticate the account"
            ) from error
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError, OSError) as error:
            raise ChatGPTAuthenticationError(
                "Saved ChatGPT session is invalid; verify the account"
            ) from error
        return self._validate_loaded_record(value)

    async def load_async(self) -> tuple[str, dict[str, str]]:
        path = self.session_file
        try:
            value = await read_session_async(path)
        except (FileNotFoundError, AccountPaused) as error:
            raise ChatGPTAuthenticationError(
                "No saved ChatGPT session; authenticate the account"
            ) from error
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError, OSError) as error:
            raise ChatGPTAuthenticationError(
                "Saved ChatGPT session is invalid; verify the account"
            ) from error
        return self._validate_loaded_record(value)

    def capture_auth(self) -> tuple[str, dict[str, str]]:
        from ...accounts.cdp import DevToolsError

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
        from ...accounts.cdp import is_devtools_available

        return is_devtools_available(self.cdp_url)


def capture_chatgpt_auth(cdp_url: str, timeout: float = 15.0) -> tuple[str, dict]:
    """Capture the authenticated ChatGPT /me request from the connected page."""
    from ...accounts import cdp

    base = cdp._local_http_base(cdp_url)
    origin = "https://chatgpt.com"
    page = cdp._page_for_origin(base, origin)
    requests = {}
    extra_headers = {}
    responses = {}

    def bound(mapping, key, value):
        mapping[key] = value
        while len(mapping) > 256:
            mapping.pop(next(iter(mapping)))

    def result_for(request_id):
        if (
            request_id not in requests
            or request_id not in extra_headers
            or request_id not in responses
        ):
            return None
        status = responses.pop(request_id)
        request = requests.pop(request_id)
        headers = extra_headers.pop(request_id)
        if not 200 <= status < 300:
            return None
        merged_headers = request.get("headers", {})
        if not isinstance(merged_headers, dict):
            merged_headers = {}
        merged_headers = {**merged_headers, **headers}
        return request["url"], merged_headers

    with cdp.DevToolsConnection(page["webSocketDebuggerUrl"]) as connection:
        connection.call("Network.enable")
        connection.call("Page.navigate", {"url": f"{origin}/"})
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise cdp.DevToolsError(
                    "Timed out waiting for an authenticated ChatGPT account request"
                )
            event = connection.next_event(remaining)
            params = event.get("params", {})
            request_id = params.get("requestId")
            if not isinstance(request_id, str):
                continue
            method = event.get("method")
            if method == "Network.requestWillBeSent":
                request = params.get("request", {})
                if not isinstance(request, dict) or request.get("method") != "GET":
                    continue
                try:
                    request_url = urlsplit(request.get("url", ""))
                except ValueError:
                    continue
                if (
                    request_url.scheme != "https"
                    or request_url.hostname != "chatgpt.com"
                    or request_url.path != "/backend-api/me"
                ):
                    continue
                bound(requests, request_id, request)
            elif method == "Network.requestWillBeSentExtraInfo":
                headers = params.get("headers", {})
                bound(
                    extra_headers,
                    request_id,
                    headers if isinstance(headers, dict) else {},
                )
            elif method == "Network.responseReceived" and request_id in requests:
                response = params.get("response", {})
                status = response.get("status") if isinstance(response, dict) else None
                if isinstance(status, (int, float)):
                    bound(responses, request_id, int(status))
            captured = result_for(request_id)
            if captured is not None:
                return captured
