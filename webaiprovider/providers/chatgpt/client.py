#!/usr/bin/env python3
"""Standalone synchronous ChatGPT web client with injected browser session."""

from __future__ import annotations

import json
import os
import stat
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager, suppress
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from curl_cffi.requests import Session as CurlSession
from curl_cffi.requests.exceptions import RequestException as CurlRequestException

from ...core import MultiDimensionBoundedCache
from ...core.timing import SendTiming, current_send_timing
from ...errors import RateLimited
from ...transport import HTTPTransport, Transport, TransportConfig
from ...transport.frames import SSEFrameDecoder
from ...transport.http import _is_ip_literal
from ...transport.retry import parse_retry_after
from .catalogs import parse_model_catalog
from .constants import CHATGPT_ORIGIN, DEFAULT_PAGE_SIZE
from .discovery import (
    page as parse_page,
)
from .discovery import (
    project as parse_project,
)
from .discovery import (
    project_id as extract_project_id,
)
from .errors import ChatGPTAuthenticationError, ChatGPTError, SentinelCompatibilityError
from .formats import message_parts, text_from_parts
from .sentinel import SentinelRuntime
from .session import BrowserSession

_CONTINUATION_CACHE_TTL_SECONDS = 60.0
_CONTINUATION_CACHE_MAX_ITEMS = 8
_CONTINUATION_CACHE_MAX_MESSAGES = 2_000
_CONTINUATION_CACHE_MAX_TEXT_CHARS = 1_000_000


@lru_cache(maxsize=8)
def _cached_system_timezone(tz_env: str) -> str | None:
    candidates = [tz_env.removeprefix(":")]
    try:
        target = str(Path("/etc/localtime").resolve())
        if "/zoneinfo/" in target:
            candidates.append(target.split("/zoneinfo/", 1)[1])
        path = Path("/etc/timezone")
        if path.is_file():
            candidates.append(path.read_text().strip())
    except OSError:
        pass
    for value in candidates:
        try:
            if value:
                return ZoneInfo(value).key
        except (ValueError, ZoneInfoNotFoundError):
            pass
    return None


def system_timezone():
    """Use an actual IANA identifier, omitting the optional field if unknown."""
    return _cached_system_timezone(os.environ.get("TZ", ""))


class ChatGPTClient:
    def __init__(
        self,
        session: BrowserSession,
        *,
        authenticate: bool = True,
        transport: Transport | None = None,
    ) -> None:
        self.session = session
        self.cdp_url = session.cdp_url
        self.list_url = ""
        self.headers: dict[str, str] = {}
        self._conversation_endpoint = "conversations"
        self._conversation_endpoint_verified = False
        self.temporary_conversations: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.pending_branches: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._owned_temporary_conversations = self.temporary_conversations
        self._owned_pending_branches = self.pending_branches
        self._continuation_cache = MultiDimensionBoundedCache(
            max_entries=_CONTINUATION_CACHE_MAX_ITEMS,
            max_bytes=_CONTINUATION_CACHE_MAX_TEXT_CHARS,
            default_ttl=_CONTINUATION_CACHE_TTL_SECONDS,
            getsizeof=lambda key, value: len(key) + value[2],
        )
        self._sentinel_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._refresh_lock = threading.Lock()
        self._sentinel_runtime: SentinelRuntime | None = None
        self._sentinel_runtime_key: str | None = None
        self._prewarmed_sentinel: tuple[float, str, dict[str, str]] | None = None
        self._prewarming = False
        self._sentinel_inflight: threading.Event | None = None
        self._upload_http: CurlSession | None = None
        self._active_streams: dict[int, Any] = {}
        self._closed = False
        timing = current_send_timing()
        http_started = time.perf_counter_ns() if timing is not None else None
        if transport is None:
            self.http = CurlSession(
                impersonate="chrome",
                trust_env=False,
                timeout=30.0,
                discard_cookies=True,
            )
            self.transport = HTTPTransport(
                CHATGPT_ORIGIN,
                client_getter=lambda: self.http,
                config=TransportConfig(timeout=30),
                headers=self._transport_headers,
            )
            if http_started is not None:
                timing.record("http_client_construct_ms", time.perf_counter_ns() - http_started)
        else:
            self.http = None
            self.transport = transport
        if authenticate:
            try:
                auth_started = time.perf_counter_ns() if timing is not None else None
                self.refresh_auth()
                if auth_started is not None:
                    timing.record("provider_auth_refresh_ms", time.perf_counter_ns() - auth_started)
            except BaseException:
                if self.http is not None:
                    self.http.close()
                raise

    def close(self) -> None:
        inflight: threading.Event | None = None
        runtime_to_close: SentinelRuntime | None = None
        with self._sentinel_lock:
            self._closed = True
            self._prewarmed_sentinel = None
            self._prewarming = False
            inflight = getattr(self, "_sentinel_inflight", None)
            if inflight is None or inflight.is_set():
                runtime_to_close = self._sentinel_runtime
            self._sentinel_runtime = None
            self._sentinel_runtime_key = None
        if inflight is not None:
            inflight.set()
        if runtime_to_close is not None:
            runtime_to_close.close()
        with self._state_guard:
            self._continuation_cache.clear()
            owned_temps = getattr(
                self, "_owned_temporary_conversations", self.temporary_conversations
            )
            if self.temporary_conversations is owned_temps:
                self.temporary_conversations.clear()
            else:
                self.temporary_conversations = OrderedDict()
            pending = getattr(self, "pending_branches", None)
            owned_pending = getattr(self, "_owned_pending_branches", pending)
            if isinstance(pending, dict):
                if pending is owned_pending:
                    pending.clear()
                else:
                    self.pending_branches = OrderedDict()
            upload_http = getattr(self, "_upload_http", None)
            self._upload_http = None
            raw_streams = getattr(self, "_active_streams", None)
            active_streams = (
                list(raw_streams.values())
                if isinstance(raw_streams, dict)
                else list(raw_streams or ())
            )
            http_client = getattr(self, "http", None)
        for stream in active_streams:
            with suppress(Exception):
                stream.close()
        if upload_http is not None:
            with suppress(Exception):
                upload_http.close()
        if http_client is not None:
            with suppress(Exception):
                http_client.close()

    @staticmethod
    def _conversation_text_size(conversation: dict[str, Any]) -> int:
        messages = conversation.get("messages")
        if not isinstance(messages, list):
            return 0
        total = 0
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            parts = content.get("parts") if isinstance(content, dict) else None
            if isinstance(parts, list):
                for part in parts:
                    if isinstance(part, str):
                        total += len(part)
                        if total > _CONTINUATION_CACHE_MAX_TEXT_CHARS:
                            return total
        return total

    @property
    def _state_guard(self) -> threading.RLock:
        lock = getattr(self, "_state_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._state_lock = lock
        return lock

    @property
    def _refresh_guard(self) -> threading.Lock:
        lock = getattr(self, "_refresh_lock", None)
        if lock is None:
            lock = threading.Lock()
            self._refresh_lock = lock
        return lock

    def _is_closed(self) -> bool:
        http_client = getattr(self, "http", None)
        return (
            getattr(self, "_closed", False) is True
            or getattr(http_client, "_closed", False) is True
            or getattr(http_client, "is_closed", False) is True
        )

    @contextmanager
    def _tracked_stream(self, method: str, path: str, **options):
        if (
            options.get("inherit_headers", True)
            and "headers" not in options
            and getattr(self, "headers", None) is not None
        ):
            options["headers"] = self._transport_headers(method, path)
        with self.transport.stream(method, path, **options) as stream:
            stream_key = id(stream)
            with self._state_guard:
                active = getattr(self, "_active_streams", None)
                if not isinstance(active, dict):
                    active = {}
                    self._active_streams = active
                active[stream_key] = stream
            try:
                if self._is_closed():
                    with suppress(Exception):
                        stream.close()
                yield stream
            finally:
                with self._state_guard:
                    active = getattr(self, "_active_streams", None)
                    if isinstance(active, dict):
                        active.pop(stream_key, None)

    def _sentinel_user_agent(self) -> str:
        with self._state_guard:
            return self.headers.get("user-agent") or "Mozilla/5.0"

    def _record_rate_limit(self, delay: float | None) -> None:
        record = getattr(self.transport, "record_rate_limit", None)
        if callable(record):
            record(delay)

    def _remember_conversation(self, conversation: dict[str, Any]) -> None:
        conversation_id = conversation.get("conversation_id") or conversation.get("id")
        messages = conversation.get("messages")
        if (
            not isinstance(conversation_id, str)
            or not conversation_id
            or "current_node" not in conversation
            or not isinstance(conversation.get("current_node"), (str, type(None)))
            or not isinstance(messages, list)
            or len(messages) > _CONTINUATION_CACHE_MAX_MESSAGES
        ):
            if isinstance(conversation_id, str) and conversation_id:
                self._forget_conversation(conversation_id)
            return
        text_size = self._conversation_text_size(conversation)
        if text_size > _CONTINUATION_CACHE_MAX_TEXT_CHARS:
            self._forget_conversation(conversation_id)
            return
        with self._state_guard:
            self._continuation_cache.put(
                conversation_id, (time.monotonic(), conversation, text_size)
            )

    def _cached_conversation(self, conversation_id: str) -> dict[str, Any] | None:
        with self._state_guard:
            cached = self._continuation_cache.get(conversation_id)
            if cached is None:
                return None
            checked_at, conversation, _text_size = cached
            if time.monotonic() - checked_at >= _CONTINUATION_CACHE_TTL_SECONDS:
                self._forget_conversation(conversation_id)
                return None
            return conversation

    def _forget_conversation(self, conversation_id: str) -> None:
        with self._state_guard:
            pending = getattr(self, "pending_branches", None)
            if isinstance(pending, dict):
                pending.pop(conversation_id, None)
            self._continuation_cache.pop(conversation_id, None)

    def __enter__(self) -> ChatGPTClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def refresh_auth(self):
        try:
            list_url, headers = self.session.load()
            with self._state_guard:
                self.list_url, self.headers = list_url, headers
        except ChatGPTError:
            raise ChatGPTError(
                "No saved session; verify the account in the Accounts section"
            ) from None

    def _transport_headers(self, method, path):
        with self._state_guard:
            headers = dict(self.headers)
        headers["accept-encoding"] = "gzip, deflate"
        target = urlsplit(path).path
        headers["x-openai-target-path"] = target
        headers["x-openai-target-route"] = target
        return headers

    def refresh_session(self) -> None:
        with self._state_guard:
            headers = self.headers.copy()
        headers.pop("authorization", None)
        headers["accept-encoding"] = "gzip, deflate"
        response = self.transport.send(
            "GET",
            "/api/auth/session",
            check_status=False,
            inherit_headers=False,
            headers=headers,
            timeout=30,
        )
        if response.status_code == 429:
            self._record_rate_limit(
                parse_retry_after(response.headers.get("retry-after"), now=time.time())
            )
        if response.status_code != 200:
            raise ChatGPTError("Saved ChatGPT session expired; re-authenticate the account")
        try:
            value = response.json()
            token = value.get("accessToken")
        except (ValueError, AttributeError) as error:
            raise ChatGPTError("ChatGPT session refresh returned invalid data") from error
        if not isinstance(token, str) or not token:
            raise ChatGPTError("Saved ChatGPT session expired; re-authenticate the account")
        with self._state_guard:
            self.headers["authorization"] = f"Bearer {token}"
            cookie_map: dict[str, str] = {}
            for pair in self.headers.get("cookie", "").split(";"):
                name, sep, item = pair.strip().partition("=")
                name = name.strip()
                if sep and name:
                    cookie_map[name] = item.strip()
            for cookie in response.cookies.jar:
                domain = (cookie.domain or "").lstrip(".")
                if cookie.name and (not domain or domain == "chatgpt.com"):
                    cookie_map[cookie.name] = cookie.value or ""
            self.headers["cookie"] = "; ".join(f"{name}={val}" for name, val in cookie_map.items())
            list_url = self.list_url
            snapshot = dict(self.headers)
        self.session.save(list_url, snapshot)

    def request(self, method: str, url: str, *, expect_json: bool = True, **kwargs: Any) -> Any:
        target = urlsplit(url)
        if target.scheme != "https" or target.netloc != "chatgpt.com":
            raise ChatGPTError("authenticated requests must stay on the ChatGPT origin")
        for attempt in range(2):
            request_headers = self._transport_headers(method, target.path)
            path = target.path + ("?" + target.query if target.query else "")
            response = self.transport.send(
                method, path, headers=request_headers, check_status=False, **kwargs
            )
            if (
                response.status_code not in (401, 403)
                or attempt == 1
                or method.upper() not in ("GET", "HEAD")
            ):
                break
            failed_auth = request_headers.get("authorization")
            with self._refresh_guard:
                with self._state_guard:
                    current_auth = self.headers.get("authorization")
                if not current_auth or current_auth == failed_auth:
                    try:
                        self.refresh_session()
                    except ChatGPTError:
                        raise ChatGPTAuthenticationError(
                            "The session could not be refreshed; verify it again in the Accounts section."
                        ) from None
        if response.status_code == 429:
            delay = parse_retry_after(response.headers.get("retry-after"), now=time.time())
            self._record_rate_limit(delay)
            raise RateLimited(delay, message_submitted=False)
        if response.status_code == 404:
            from ...errors import RemoteNotFound

            raise RemoteNotFound("ChatGPT conversation no longer exists")
        if response.status_code in (401, 403):
            raise ChatGPTAuthenticationError(
                "ChatGPT session expired or was rejected; verify the account"
            )
        if response.status_code >= 400:
            raw_url = getattr(response, "url", None)
            resp_path = (
                urlsplit(raw_url).path
                if isinstance(raw_url, str)
                else getattr(raw_url, "path", None)
            ) or target.path
            raise ChatGPTError(
                f"ChatGPT returned HTTP {response.status_code} for {method} {resp_path}"
            )
        content_type = response.headers.get("content-type", "")
        if (
            not expect_json
            and response.status_code in (200, 201, 202, 204)
            and not response.content
        ):
            return response
        if "application/json" not in content_type:
            raise ChatGPTError(
                f"ChatGPT returned unexpected content type {content_type or '<missing>'}"
            )
        return response

    def verify(self) -> dict[str, Any]:
        return self.request(
            "GET",
            urljoin(CHATGPT_ORIGIN, "/backend-api/conversations"),
            params={
                "offset": 0,
                "limit": DEFAULT_PAGE_SIZE,
                "order": "updated",
                "is_archived": "false",
                "is_starred": "false",
            },
        ).json()

    def model_catalog(self, *, refresh: bool = False):
        with self._state_guard:
            cached = getattr(self, "_model_catalog", None)
            if cached is not None and not refresh and time.monotonic() - cached[0] < 300:
                return cached[1]
        raw = self.request(
            "GET",
            urljoin(CHATGPT_ORIGIN, "/backend-api/models"),
            params={
                "iim": "false",
                "is_gizmo": "false",
                "supports_model_picker_upgrade_presets": "true",
            },
        ).json()
        catalog = parse_model_catalog(raw)
        with self._state_guard:
            self._model_catalog = (time.monotonic(), catalog)
        return catalog

    def list_projects(self, cursor: str | None = None) -> dict[str, Any]:
        params = {"conversations_per_gizmo": 0, "limit": 20, "owned_only": "false"}
        if cursor is not None:
            params["cursor"] = cursor
        raw = self.request(
            "GET", urljoin(CHATGPT_ORIGIN, "/backend-api/gizmos/snorlax/sidebar"), params=params
        ).json()
        items, next_cursor = parse_page(raw)
        projects = [value for item in items if (value := parse_project(item)) is not None]
        return {"items": projects, "cursor": next_cursor}

    def create_project(self, name, instructions="", *, memory_share=True):
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 50:
            raise ValueError("project name must contain 1–50 characters")
        if type(memory_share) is not bool:
            raise TypeError("memory_share must be a bool")
        raw = self.request(
            "POST",
            urljoin(CHATGPT_ORIGIN, "/backend-api/projects"),
            json={
                "name": name.strip(),
                "instructions": instructions,
                "memory_scope": "global" if memory_share else "project_v2",
            },
        ).json()
        if raw.get("error") or not isinstance(raw.get("resource"), dict):
            raise ChatGPTError("Project creation response changed; request was not replayed")
        project = parse_project(raw["resource"])
        if project is None:
            raise ChatGPTError("Project creation returned no project")
        return self.get_project(project["id"])

    def get_project(self, project_id: str) -> dict[str, Any]:
        raw = self.request(
            "GET", urljoin(CHATGPT_ORIGIN, f"/backend-api/gizmos/{quote(project_id, safe='')}")
        ).json()
        value = parse_project(raw)
        if value is None:
            raise ChatGPTError("Requested gizmo is not a project")
        return value

    def list_project_chats(self, project_id: str, cursor: str | None = None) -> dict[str, Any]:
        raw = self.request(
            "GET",
            urljoin(
                CHATGPT_ORIGIN, f"/backend-api/gizmos/{quote(project_id, safe='')}/conversations"
            ),
            params={"cursor": cursor or "0", "limit": 30, "owned_only": "false"},
        ).json()
        items, next_cursor = parse_page(raw)
        return {
            "items": [dict(item, project_id=project_id) for item in items],
            "cursor": next_cursor,
        }

    def list_page(self, offset: int = 0, limit: int = DEFAULT_PAGE_SIZE) -> dict[str, Any]:
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid conversation page")
        return self.request(
            "GET",
            urljoin(CHATGPT_ORIGIN, "/backend-api/conversations"),
            params={
                "offset": offset,
                "limit": limit,
                "order": "updated",
                "is_archived": "false",
                "is_starred": "false",
            },
        ).json()

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        from ...errors import RemoteNotFound

        with self._state_guard:
            endpoint = self._conversation_endpoint
            verified = self._conversation_endpoint_verified
        alternate = "conversation" if endpoint == "conversations" else "conversations"
        # ChatGPT accounts currently expose either route; learn the working
        # form after the first 404 so later history reads need only one request.
        endpoints = (endpoint,) if verified else (endpoint, alternate)
        for index, candidate in enumerate(endpoints):
            url = urljoin(
                CHATGPT_ORIGIN,
                f"/backend-api/{candidate}/{quote(conversation_id, safe='')}",
            )
            try:
                response = self.request("GET", url)
            except RemoteNotFound:
                if index == 0 and not verified:
                    continue
                raise
            with self._state_guard:
                self._conversation_endpoint = candidate
                self._conversation_endpoint_verified = True
            result = response.json()
            break
        if not isinstance(result, dict):
            raise ChatGPTError("conversation response was not an object")
        self._remember_conversation(result)
        return result

    def rename_conversation(self, conversation_id: str, title: str) -> None:
        self.request(
            "PATCH",
            urljoin(CHATGPT_ORIGIN, f"/backend-api/conversation/{quote(conversation_id, safe='')}"),
            json={"title": title},
            expect_json=False,
        )

    def delete_conversation(self, conversation_id: str) -> bool:
        if not isinstance(conversation_id, str) or not conversation_id:
            raise ValueError("conversation ID is required")
        response = self.request(
            "PATCH",
            urljoin(CHATGPT_ORIGIN, f"/backend-api/conversation/{quote(conversation_id, safe='')}"),
            json={"is_visible": False},
            expect_json=False,
        )
        payload = response.json() if response.content else None
        if isinstance(payload, dict) and payload.get("success") is False:
            raise ChatGPTError("ChatGPT refused to delete the conversation.")
        self._forget_conversation(conversation_id)
        if getattr(response, "status_code", None) == 204:
            return True
        return isinstance(payload, dict) and (
            payload.get("success") is True or payload.get("is_visible") is False
        )

    def move_conversation(self, conversation_id: str, project_id: str | None) -> dict[str, Any]:
        """Update native project membership without an extra conversation readback GET."""
        if not isinstance(conversation_id, str) or not conversation_id:
            raise ValueError("conversation ID is required")
        if project_id is not None and (
            not isinstance(project_id, str) or not project_id.startswith("g-p-")
        ):
            raise ValueError("invalid project ID")
        response = self.request(
            "PATCH",
            urljoin(CHATGPT_ORIGIN, f"/backend-api/conversation/{quote(conversation_id, safe='')}"),
            json={"gizmo_id": project_id},
            expect_json=False,
        )
        payload = response.json() if response.content else None
        if isinstance(payload, dict):
            if payload.get("success") is False:
                raise ChatGPTError(
                    "Server did not confirm project membership; refresh this conversation"
                )
            if (
                any(
                    key in payload for key in ("gizmo_id", "project_id", "conversation_template_id")
                )
                and extract_project_id(payload) != project_id
            ):
                raise ChatGPTError(
                    "Server did not confirm project membership; refresh this conversation"
                )
        with self._state_guard:
            cached = self._cached_conversation(conversation_id)
            if isinstance(cached, dict):
                updated_snapshot = dict(cached)
                updated_snapshot["gizmo_id"] = project_id
                updated_snapshot["project_id"] = project_id
                self._remember_conversation(updated_snapshot)
        result = dict(payload) if isinstance(payload, dict) else {}
        result.setdefault("conversation_id", conversation_id)
        result["project_id"] = project_id
        result["gizmo_id"] = project_id
        return result

    def _resolve_send_previous(
        self,
        conversation_id: str | None,
        project_id: str | None,
        temporary: bool,
        action: Any,
    ) -> tuple[dict[str, Any] | None, set[str], bool]:
        previous_ids: set[str] = set()
        previous: dict[str, Any] | None = None
        is_pending_branch = False
        if conversation_id:
            if temporary:
                with self._state_guard:
                    previous = self.temporary_conversations.get(conversation_id)
                if previous is None:
                    raise ChatGPTError(
                        "temporary conversation expired locally; start a new temporary chat"
                    )
            else:
                with self._state_guard:
                    pending = getattr(self, "pending_branches", None)
                    pending_entry = (
                        pending.get(conversation_id) if isinstance(pending, dict) else None
                    )
                if pending_entry is not None:
                    previous = pending_entry
                    is_pending_branch = isinstance(previous.get("pending_branch"), dict)
                else:
                    timing = current_send_timing()
                    may_reuse_snapshot = action is None
                    cache_lookup_started = (
                        time.perf_counter_ns()
                        if timing is not None and may_reuse_snapshot
                        else None
                    )
                    cached = (
                        self._cached_conversation(conversation_id) if may_reuse_snapshot else None
                    )
                    if cache_lookup_started is not None:
                        timing.record(
                            "pre_send_conversation_cache_lookup_ms",
                            time.perf_counter_ns() - cache_lookup_started,
                        )
                    if cached is not None:
                        previous = cached
                        if timing is not None:
                            timing.increment("pre_send_conversation_cache_hit")
                    else:
                        started = time.perf_counter_ns() if timing is not None else None
                        previous = self.get_conversation(conversation_id)
                        if started is not None:
                            timing.record(
                                "pre_send_get_conversation_ms", time.perf_counter_ns() - started
                            )
                        if timing is not None and may_reuse_snapshot:
                            timing.increment("pre_send_conversation_cache_miss")
            previous_messages = previous.get("messages", [])
            if isinstance(previous_messages, list):
                previous_ids = {
                    item["id"]
                    for item in previous_messages
                    if isinstance(item, dict) and item.get("id")
                }

        if project_id:
            if previous and extract_project_id(previous) not in (None, project_id):
                raise ChatGPTError("conversation belongs to another project")
            previous = dict(previous or {}, project_id=project_id)
            if action is not None and conversation_id:
                self._forget_conversation(conversation_id)
        return previous, previous_ids, is_pending_branch

    def _store_temporary_conversation(
        self,
        resolved_id: str,
        model: str,
        prompt: str,
        previous: dict[str, Any] | None,
        submitted_message_id: str,
        submitted_message: dict[str, Any] | None,
        streamed_messages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        submitted_messages = (
            [submitted_message]
            if isinstance(submitted_message, dict)
            else [
                {
                    "id": submitted_message_id,
                    "author": {"role": "user"},
                    "content": {"content_type": "text", "parts": [prompt]},
                }
            ]
        )
        conversation = {
            "id": resolved_id,
            "conversation_id": resolved_id,
            "title": "Geçici sohbet",
            "default_model_slug": model,
            "current_node": streamed_messages[-1].get("id") if streamed_messages else None,
            "messages": list((previous or {}).get("messages", []))
            + submitted_messages
            + streamed_messages,
            "temporary": True,
        }
        if not conversation["current_node"]:
            raise ChatGPTError("temporary response has no continuation cursor")
        size = sum(
            len(text_from_parts(message_parts(item.get("content"))))
            for item in conversation["messages"]
        )
        if size > 2_000_000:
            raise ChatGPTError(
                "temporary conversation reached the local memory limit; start a new one"
            )
        with self._state_guard:
            self.temporary_conversations[resolved_id] = conversation
            self.temporary_conversations.move_to_end(resolved_id)
            while len(self.temporary_conversations) > 32:
                self.temporary_conversations.popitem(last=False)
        return conversation

    def send_prompt(
        self,
        conversation_id: str | None,
        prompt: str,
        attachments: list[dict[str, Any]] | None = None,
        on_event: Any = None,
        model: str | None = None,
        effort: str | None = None,
        project_id: str | None = None,
        temporary: bool = False,
        action=None,
        message_id: str | None = None,
    ) -> tuple[str, dict[str, Any], set[str]]:
        if type(temporary) is not bool or (
            project_id is not None
            and (not isinstance(project_id, str) or not project_id.startswith("g-p-"))
        ):
            raise ChatGPTError("invalid conversation options")
        if temporary and project_id:
            raise ChatGPTError("temporary conversations cannot belong to a project")
        previous, previous_ids, is_pending_branch = self._resolve_send_previous(
            conversation_id, project_id, temporary, action
        )
        timing = current_send_timing()
        started = time.perf_counter_ns() if timing is not None else None
        catalog = self.model_catalog()
        if started is not None:
            timing.record("model_catalog_ms", time.perf_counter_ns() - started)
        model = (
            model
            or (previous.get("default_model_slug") if previous else None)
            or catalog.default_model
        )
        selected = next((item for item in catalog.models if item.id == model), None)
        if selected is None:
            raise ChatGPTError("Select a model from the current account catalog before sending")
        if effort is not None and effort not in {item.id for item in selected.efforts}:
            raise ChatGPTError(f"Effort '{effort}' is not offered for model '{model}'")

        try:
            resolved_id, submitted_message_id, submitted_message, streamed_messages = (
                self.send_prompt_direct(
                    previous,
                    prompt,
                    attachments or [],
                    on_event,
                    model,
                    effort,
                    **({"temporary": True} if temporary else {}),
                    **({"action": action, "message_id": message_id} if action else {}),
                )
            )
        except Exception:
            if conversation_id and not temporary and not is_pending_branch:
                self._forget_conversation(conversation_id)
            raise
        if is_pending_branch and conversation_id:
            self._forget_conversation(conversation_id)
        if temporary:
            conversation = self._store_temporary_conversation(
                resolved_id,
                model,
                prompt,
                previous,
                submitted_message_id,
                submitted_message,
                streamed_messages,
            )
        else:
            previous_messages = (previous or {}).get("messages")
            last_message = streamed_messages[-1] if streamed_messages else None
            first_message = streamed_messages[0] if streamed_messages else None
            streamed_parent = (
                first_message.get("parent") or first_message.get("parent_message_id")
                if isinstance(first_message, dict)
                else None
            )
            current_node = last_message.get("id") if isinstance(last_message, dict) else None
            can_build_from_stream = (
                not is_pending_branch
                and (conversation_id is None or resolved_id == conversation_id)
                and action is None
                and (conversation_id is None or isinstance(previous_messages, list))
                and isinstance(submitted_message, dict)
                and submitted_message.get("id") == submitted_message_id
                and (streamed_parent is None or streamed_parent == submitted_message_id)
                and isinstance(current_node, str)
                and bool(current_node)
            )
            if can_build_from_stream:
                base_messages = list(previous_messages or ())
                conversation = dict(previous or {})
                conversation.update(
                    {
                        "id": resolved_id,
                        "conversation_id": resolved_id,
                        "title": (previous or {}).get("title") or prompt[:60],
                        "default_model_slug": model,
                        "current_node": current_node,
                        "messages": base_messages + [submitted_message] + streamed_messages,
                    }
                )
            else:
                timing = current_send_timing()
                started = time.perf_counter_ns() if timing is not None else None
                try:
                    conversation = self.get_conversation(resolved_id)
                except Exception:
                    self._forget_conversation(resolved_id)
                    raise
                finally:
                    if started is not None:
                        timing.record(
                            "post_send_get_conversation_ms", time.perf_counter_ns() - started
                        )
        messages = conversation.get("messages", [])
        attached = isinstance(messages, list) and any(
            isinstance(message, dict) and message.get("id") == submitted_message_id
            for message in messages
        )
        if not attached:
            if not temporary:
                self._forget_conversation(resolved_id)
            raise ChatGPTError("ChatGPT did not attach the submitted message to the conversation")
        if is_pending_branch:
            previous_ids = set()
            for item in messages:
                if not isinstance(item, dict):
                    continue
                if item.get("id") == submitted_message_id:
                    break
                if item.get("id"):
                    previous_ids.add(item["id"])
        if not temporary:
            self._remember_conversation(conversation)
        self.trigger_sentinel_prewarm()
        return resolved_id, conversation, previous_ids

    def _edit_parent_id(
        self,
        conversation: dict[str, Any] | None,
        conversation_id: str | None,
        items: list[dict[str, Any]],
        target_index: int,
        target: dict[str, Any],
        message_id: str | None,
    ) -> str:
        from ...errors import RemoteNotFound

        for key in ("parent", "parent_message_id"):
            direct = target.get(key)
            if isinstance(direct, str) and direct:
                return direct
        mapping = conversation.get("mapping") if isinstance(conversation, dict) else None
        if isinstance(mapping, dict) and message_id:
            node = mapping.get(message_id)
            parent = node.get("parent") if isinstance(node, dict) else None
            if isinstance(parent, str) and parent:
                return parent
        if target_index > 0:
            prev_id = items[target_index - 1].get("id")
            if isinstance(prev_id, str) and prev_id:
                return prev_id
        # Flat `/conversations/{id}` payloads and in-memory streams can omit the
        # synthetic root node so turn 1 lands at index 0; fetch the singular
        # mapping view only then to recover the server's real parent UUID.
        if conversation_id and message_id:
            url = urljoin(
                CHATGPT_ORIGIN,
                f"/backend-api/conversation/{quote(conversation_id, safe='')}",
            )
            try:
                response = self.request("GET", url)
            except RemoteNotFound:
                raise ChatGPTError("The original message parent is unavailable") from None
            try:
                data = response.json()
            except ValueError:
                raise ChatGPTError("The original message parent is unavailable") from None
            if isinstance(data, dict):
                node = (data.get("mapping") or {}).get(message_id)
                parent = node.get("parent") if isinstance(node, dict) else None
                if isinstance(parent, str) and parent:
                    return parent
        raise ChatGPTError("The original message parent is unavailable")

    def _apply_message_action(
        self,
        body: dict[str, Any],
        conversation: dict[str, Any] | None,
        conversation_id: str | None,
        action: Any,
        message_id: str | None,
        prompt: str,
    ) -> str:
        from ...resources.message.types import MessageAction

        items = (conversation or {}).get("messages", [])
        target_index = next(
            (i for i, item in enumerate(items) if item.get("id") == message_id), None
        )
        if target_index is None or (conversation or {}).get("is_read_only"):
            raise ChatGPTError("The selected message is no longer editable")
        target = items[target_index]
        expected_role = "user" if action == MessageAction.EDIT else "assistant"
        if target.get("channel") == "analysis" or target.get("recipient") not in (None, "all"):
            raise ChatGPTError("Only visible messages can be changed")
        if (target.get("author") or {}).get("role") != expected_role:
            raise ChatGPTError("The message role does not match the requested action")
        if action == MessageAction.RETRY:
            user = next(
                (
                    item
                    for item in reversed(items[:target_index])
                    if (item.get("author") or {}).get("role") == "user"
                ),
                None,
            )
            if user is None:
                raise ChatGPTError("The original prompt is unavailable")
            body.update(action="variant", messages=[], parent_message_id=user["id"])
            return user["id"]
        if action == MessageAction.EDIT:
            body["parent_message_id"] = self._edit_parent_id(
                conversation,
                conversation_id,
                items,
                target_index,
                target,
                message_id,
            )
            original = target.get("content") or {}
            resource_parts = [
                part for part in original.get("parts", []) if not isinstance(part, str)
            ]
            body["messages"][0]["content"] = {
                "content_type": original.get("content_type") or "text",
                "parts": resource_parts + [prompt],
            }
            body["messages"][0]["metadata"]["attachments"] = (target.get("metadata") or {}).get(
                "attachments", []
            )
            return body["messages"][0]["id"]
        raise ChatGPTError("Unsupported message action")

    def _consume_chat_stream(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
        conversation_id: str | None,
        on_event: Any,
    ) -> tuple[str, list[dict[str, Any]]]:
        resolved_id: str | None = conversation_id
        bound_id: str | None = None
        rendered_by_channel: dict[str, str] = {}
        terminal_seen = False
        completion_notified = False
        final_seen = False
        streamed_messages: OrderedDict[str, dict[str, Any]] = OrderedDict()
        timing = current_send_timing()
        sse_connect_started = time.perf_counter_ns() if timing is not None else None
        first_sse_event = False
        first_assistant_text = False
        with self._tracked_stream(
            method,
            url.removeprefix(CHATGPT_ORIGIN),
            check_status=False,
            headers=headers,
            json=body,
            timeout=(30.0, 300.0),
        ) as response:
            response_opened = time.perf_counter_ns() if timing is not None else None
            if response_opened is not None:
                timing.record("sse_connect_ms", response_opened - sse_connect_started)
            if response.status_code == 429:
                delay = parse_retry_after(response.headers.get("retry-after"), now=time.time())
                self._record_rate_limit(delay)
                raise RateLimited(delay, message_submitted=False)
            if response.status_code >= 400:
                raise ChatGPTError(f"ChatGPT submission returned HTTP {response.status_code}")
            content_type = response.headers.get("content-type", "")
            if "text/event-stream" not in content_type:
                raise ChatGPTError(
                    f"ChatGPT returned unexpected content type {content_type or '<missing>'}"
                )
            for frame in SSEFrameDecoder().decode(response.iter_bytes()):
                payload = frame.data
                event_at = time.perf_counter_ns() if timing is not None else None
                if timing is not None and not first_sse_event:
                    first_sse_event = True
                    timing.record("sse_first_event_ms", event_at - response_opened)
                    timing.record("button_to_first_sse_event_ms", event_at - timing.started_ns)
                if payload == "[DONE]":
                    terminal_seen = True
                    if final_seen and not completion_notified and on_event is not None:
                        on_event({"type": "response_complete"})
                        completion_notified = True
                    if timing is not None:
                        timing.record("sse_last_event_ms", event_at - response_opened)
                        timing.record("button_to_last_sse_event_ms", event_at - timing.started_ns)
                    break
                try:
                    event = json.loads(payload)
                except json.JSONDecodeError:
                    raise ChatGPTError(
                        "ChatGPT stream protocol changed; request not retried"
                    ) from None
                if not isinstance(event, dict):
                    continue
                if event.get("error"):
                    raise ChatGPTError("ChatGPT stream failed; request not retried")
                event_id = event.get("conversation_id")
                if isinstance(event_id, str) and event_id:
                    resolved_id = event_id
                    if event_id != bound_id:
                        bound_id = event_id
                        if on_event is not None and getattr(on_event, "_accepts_bound", False):
                            on_event({"type": "bound", "conversation_id": event_id})
                message = event.get("message")
                if not isinstance(message, dict):
                    continue
                author = message.get("author")
                if not isinstance(author, dict) or author.get("role") != "assistant":
                    continue
                identity = str(message.get("id") or message.get("channel") or "assistant")
                streamed_messages[identity] = message
                content = message.get("content") or {}
                channel = message.get("channel") or (
                    "analysis"
                    if content.get("content_type")
                    in ("thoughts", "reasoning_recap", "reasoning_summary")
                    else "final"
                )
                if (message.get("metadata") or {}).get("is_visually_hidden_from_conversation"):
                    continue
                if message.get("recipient") not in (None, "all"):
                    continue
                if channel != "analysis":
                    final_seen = True
                text = text_from_parts(message_parts(message.get("content")))
                rendered = rendered_by_channel.get(identity, "")
                if text and text != rendered:
                    if (
                        timing is not None
                        and channel != "analysis"
                        and not first_assistant_text
                        and text.strip()
                    ):
                        first_assistant_text = True
                        timing.record(
                            "sse_first_assistant_text_ms", time.perf_counter_ns() - response_opened
                        )
                        timing.record(
                            "button_to_first_assistant_text_ms", event_at - timing.started_ns
                        )
                    if on_event is not None:
                        on_event(
                            {
                                "type": "delta" if text.startswith(rendered) else "replace",
                                "text": text[len(rendered) :]
                                if text.startswith(rendered)
                                else text,
                                "channel": channel,
                                "message_id": identity,
                            }
                        )
                    rendered_by_channel[identity] = text
                if (
                    channel != "analysis"
                    and message.get("status") == "finished_successfully"
                    and not completion_notified
                    and on_event is not None
                ):
                    on_event({"type": "response_complete"})
                    completion_notified = True
        if not terminal_seen or not final_seen:
            raise ChatGPTError(
                "ChatGPT response incomplete; remote outcome unknown; request not retried"
            )
        if not isinstance(resolved_id, str) or not resolved_id:
            raise ChatGPTError("ChatGPT did not return a conversation ID")
        return resolved_id, list(streamed_messages.values())

    def send_prompt_direct(
        self,
        conversation: dict[str, Any] | None,
        prompt: str,
        attachments: list[dict[str, Any]],
        on_event: Any = None,
        model: str | None = None,
        effort: str | None = None,
        temporary: bool = False,
        action=None,
        message_id: str | None = None,
    ) -> tuple[str, str, dict[str, Any] | None, list[dict[str, Any]]]:
        conversation_id = None
        if conversation:
            candidate = conversation.get("conversation_id") or conversation.get("id")
            if isinstance(candidate, str):
                conversation_id = candidate
        method = "POST"
        url = urljoin(CHATGPT_ORIGIN, "/backend-api/f/conversation")
        headers = self.headers.copy()
        body = self.create_conversation_body(conversation, prompt, attachments)
        pending_branch = (
            conversation.get("pending_branch") if isinstance(conversation, dict) else None
        )
        if isinstance(pending_branch, dict):
            branch_chat_id = pending_branch.get("conversation_id")
            branch_msg_id = pending_branch.get("message_id")
            if not isinstance(branch_chat_id, str) or not isinstance(branch_msg_id, str):
                raise ChatGPTError("invalid pending branch continuation context")
            body.pop("conversation_id", None)
            conversation_id = None
            body.update(
                branching_from_conversation_id=branch_chat_id,
                branching_from_message_id=branch_msg_id,
                parent_message_id=branch_msg_id,
            )
        submitted_message_id = body["messages"][0]["id"]
        if action:
            submitted_message_id = self._apply_message_action(
                body, conversation, conversation_id, action, message_id, prompt
            )
        # Apply the provider-owned choices to this request; never retain an
        # effort preference captured from a different model.
        if model:
            body["model"] = model
            # A captured recipe can carry the old model's effort preference.
            body.pop("thinking_effort", None)
        if effort:
            body["thinking_effort"] = effort

        if conversation and extract_project_id(conversation):
            body["gizmo_id"] = extract_project_id(conversation)
        body["history_and_training_disabled"] = temporary
        headers.pop("x-openai-web-sse-compression", None)
        headers["x-openai-target-path"] = "/backend-api/f/conversation"
        headers["x-openai-target-route"] = "/backend-api/f/conversation"
        body["supported_encodings"] = []
        body["supports_buffering"] = False
        if not isinstance(submitted_message_id, str) or not submitted_message_id:
            raise ChatGPTError("conversation request has no user message ID")
        submitted_message = (
            body["messages"][0]
            if body.get("messages") and isinstance(body["messages"][0], dict)
            else None
        )
        headers.update(self.create_sentinel_headers())
        resolved_id, streamed_messages = self._consume_chat_stream(
            method, url, headers, body, conversation_id, on_event
        )
        return (
            resolved_id,
            submitted_message_id,
            submitted_message,
            streamed_messages,
        )

    def _get_or_create_sentinel_runtime(self, user_agent: str) -> SentinelRuntime:
        runtime = self._sentinel_runtime
        if runtime is None or self._sentinel_runtime_key != user_agent:
            replacement = SentinelRuntime(user_agent=user_agent)
            previous_runtime = runtime
            self._sentinel_runtime = replacement
            self._sentinel_runtime_key = user_agent
            runtime = replacement
            if previous_runtime is not None:
                previous_runtime.close()
        return runtime

    def _create_sentinel_headers_direct(
        self, runtime: SentinelRuntime, timing: SendTiming | None
    ) -> dict[str, str]:
        local_started = time.perf_counter_ns() if timing is not None else None
        initial = runtime.create_initial_proof()
        if local_started is not None:
            timing.record("local_validation_prepare_ms", time.perf_counter_ns() - local_started)
        prepare_started = time.perf_counter_ns() if timing is not None else None
        try:
            prepare_response = self.request(
                "POST",
                urljoin(CHATGPT_ORIGIN, "/backend-api/sentinel/chat-requirements/prepare"),
                json={"p": initial},
            )
            requirements = prepare_response.json()
        except SentinelCompatibilityError:
            raise
        except ChatGPTError as error:
            message = str(error)
            if any(code in message for code in ("HTTP 400", "HTTP 403", "HTTP 422")):
                raise SentinelCompatibilityError(
                    "ChatGPT rejected the Sentinel prepare request"
                ) from error
            raise
        except ValueError as error:
            raise SentinelCompatibilityError("Sentinel prepare returned invalid JSON") from error
        if prepare_started is not None:
            timing.record("sentinel_prepare_http_ms", time.perf_counter_ns() - prepare_started)
        if not isinstance(requirements, dict):
            raise SentinelCompatibilityError("Sentinel prepare returned an invalid response")
        prepare_token = requirements.get("prepare_token")
        if not isinstance(prepare_token, str) or not prepare_token.strip():
            raise SentinelCompatibilityError("Sentinel did not return required enforcement tokens")

        enforcement_started = time.perf_counter_ns() if timing is not None else None
        _, proof, turnstile = runtime.create_enforcement_tokens(requirements, initial_proof=initial)
        if enforcement_started is not None:
            timing.record(
                "local_validation_enforcement_ms",
                time.perf_counter_ns() - enforcement_started,
            )

        if not isinstance(proof, str) or not proof:
            raise SentinelCompatibilityError("Sentinel did not return required enforcement tokens")
        result = {
            "openai-sentinel-chat-requirements-prepare-token": prepare_token,
            "openai-sentinel-proof-token": proof,
        }
        if isinstance(turnstile, str) and turnstile:
            result["openai-sentinel-turnstile-token"] = turnstile
        so_requirements = requirements.get("so")
        if so_requirements is None:
            so_token = None
        elif not isinstance(so_requirements, dict):
            raise SentinelCompatibilityError("Sentinel session observer field was invalid")
        elif so_requirements.get("required") is True:
            so_token = runtime.create_session_observer_token(so_requirements, initial_proof=initial)
            if not isinstance(so_token, str) or not so_token:
                raise SentinelCompatibilityError(
                    "Sentinel session observer challenge returned no token"
                )
        elif so_requirements.get("required") is False:
            so_token = None
        else:
            raise SentinelCompatibilityError("Sentinel session observer field was invalid")
        if so_token is not None:
            result["openai-sentinel-so-token"] = so_token
        return result

    def create_sentinel_headers(self) -> dict[str, str]:
        timing = current_send_timing()
        user_agent = self._sentinel_user_agent()
        while True:
            wait_event: threading.Event | None = None
            runtime: SentinelRuntime | None = None
            inflight: threading.Event | None = None
            with self._sentinel_lock:
                if self._is_closed():
                    raise ChatGPTError("ChatGPT client is closed")
                prewarmed = getattr(self, "_prewarmed_sentinel", None)
                if prewarmed is not None:
                    created_at, prewarm_key, headers = prewarmed
                    self._prewarmed_sentinel = None
                    if prewarm_key == user_agent and (time.monotonic() - created_at) < 540.0:
                        if timing is not None:
                            timing.record("sentinel_prewarmed_hit_ms", 0)
                        return headers
                current = getattr(self, "_sentinel_inflight", None)
                if current is not None and not current.is_set():
                    wait_event = current
                else:
                    inflight = threading.Event()
                    self._sentinel_inflight = inflight
                    runtime = self._get_or_create_sentinel_runtime(user_agent)
            if wait_event is not None:
                wait_event.wait()
                continue
            try:
                assert runtime is not None and inflight is not None
                return self._create_sentinel_headers_direct(runtime, timing)
            finally:
                with self._sentinel_lock:
                    if getattr(self, "_sentinel_inflight", None) is inflight:
                        self._sentinel_inflight = None
                    if inflight is not None:
                        inflight.set()
                    if (
                        runtime is not None
                        and self._is_closed()
                        and runtime is not getattr(self, "_sentinel_runtime", None)
                    ):
                        runtime.close()

    def trigger_sentinel_prewarm(self) -> None:
        if (
            self._is_closed()
            or getattr(self.create_sentinel_headers, "__func__", None)
            is not ChatGPTClient.create_sentinel_headers
        ):
            return
        user_agent = self._sentinel_user_agent()
        with self._sentinel_lock:
            current = getattr(self, "_sentinel_inflight", None)
            if (
                self._is_closed()
                or getattr(self, "_prewarming", False)
                or (current is not None and not current.is_set())
            ):
                return
            prewarmed = getattr(self, "_prewarmed_sentinel", None)
            if prewarmed is not None:
                created_at, prewarm_key, _ = prewarmed
                if prewarm_key == user_agent and (time.monotonic() - created_at) < 540.0:
                    return
            self._prewarming = True
            inflight = threading.Event()
            self._sentinel_inflight = inflight

        def _worker() -> None:
            runtime: SentinelRuntime | None = None
            try:
                with self._sentinel_lock:
                    if self._is_closed():
                        return
                    ua = self._sentinel_user_agent()
                    runtime = self._get_or_create_sentinel_runtime(ua)
                # Serialize runtime proof/enforcement outside _sentinel_lock via
                # _sentinel_inflight so foreground close() never stalls on network I/O.
                headers = self._create_sentinel_headers_direct(runtime, None)
                with self._sentinel_lock:
                    if not self._is_closed():
                        self._prewarmed_sentinel = (time.monotonic(), ua, headers)
            except Exception:
                pass
            finally:
                with self._sentinel_lock:
                    self._prewarming = False
                    if getattr(self, "_sentinel_inflight", None) is inflight:
                        self._sentinel_inflight = None
                    inflight.set()
                    if (
                        runtime is not None
                        and self._is_closed()
                        and runtime is not getattr(self, "_sentinel_runtime", None)
                    ):
                        runtime.close()

        try:
            threading.Thread(target=_worker, daemon=True).start()
        except Exception:
            with self._sentinel_lock:
                self._prewarming = False
                if getattr(self, "_sentinel_inflight", None) is inflight:
                    self._sentinel_inflight = None
                inflight.set()

    def create_conversation_body(
        self,
        conversation: dict[str, Any] | None,
        prompt: str,
        attachments: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        parent = conversation.get("current_node") if conversation else None
        if not isinstance(parent, str) or not parent:
            parent = str(uuid.uuid4())
        zone = system_timezone()
        now = datetime.now().astimezone()
        offset = now.utcoffset()
        offset_minutes = -int(offset.total_seconds() / 60) if offset else 0
        model = None
        if conversation:
            candidate = conversation.get("default_model_slug")
            if isinstance(candidate, str) and candidate:
                model = candidate
        attachments = attachments or []
        content_parts: list[Any] = [prompt]
        attachment_metadata: list[dict[str, Any]] = []
        if attachments:
            content_parts = []
            for attachment in attachments:
                file_id = attachment.get("id")
                if not isinstance(file_id, str) or not file_id.startswith(("file-", "file_")):
                    raise ChatGPTError("invalid uploaded image ID")
                size = attachment.get("size_bytes")
                if attachment.get("mime_type") != "image/png" and "width" not in attachment:
                    if type(size) is not int or size <= 0:
                        raise ChatGPTError("invalid file size")
                    attachment_metadata.append(
                        {
                            "id": file_id,
                            "name": attachment.get("name"),
                            "mime_type": attachment.get("mime_type"),
                            "size": size,
                        }
                    )
                    continue
                width = attachment.get("width")
                height = attachment.get("height")
                if not all(isinstance(value, int) and value > 0 for value in (size, width, height)):
                    raise ChatGPTError("invalid uploaded image dimensions")
                content_parts.append(
                    {
                        "content_type": "image_asset_pointer",
                        "asset_pointer": f"file-service://{file_id}",
                        "size_bytes": size,
                        "width": width,
                        "height": height,
                    }
                )
                attachment_metadata.append(
                    {
                        "id": file_id,
                        "name": attachment.get("name") or "clipboard.png",
                        "mime_type": attachment.get("mime_type") or "image/png",
                        "size": size,
                        "width": width,
                        "height": height,
                    }
                )
            content_parts.append(prompt)
        body = {
            "action": "next",
            "messages": [
                {
                    "id": str(uuid.uuid4()),
                    "author": {"role": "user"},
                    "create_time": time.time(),
                    "content": {
                        "content_type": "multimodal_text"
                        if any(isinstance(part, dict) for part in content_parts)
                        else "text",
                        "parts": content_parts,
                    },
                    "metadata": {
                        "attachments": attachment_metadata,
                        "selected_sources": [],
                        "serialization_metadata": {"custom_symbol_offsets": []},
                        "submission_mode": "manual_send",
                    },
                }
            ],
            "parent_message_id": parent,
            "client_prepare_state": "sent",
            "timezone_offset_min": offset_minutes,
            **({"timezone": zone} if zone else {}),
            "conversation_mode": {"kind": "primary_assistant"},
            "enable_message_followups": True,
            "system_hints": [],
            "supports_buffering": False,
            "supported_encodings": [],
            "paragen_cot_summary_display_override": "allow",
            "force_parallel_switch": "auto",
            "local_function_names": ["local.continue_in_work"],
        }
        if model:
            body["model"] = model
        if conversation:
            project_id = extract_project_id(conversation)
            if project_id:
                body["gizmo_id"] = project_id
            conversation_id = conversation.get("conversation_id") or conversation.get("id")
            if isinstance(conversation_id, str) and conversation_id:
                body["conversation_id"] = conversation_id
        return body

    def _get_upload_http(self) -> CurlSession:
        with self._state_guard:
            existing = getattr(self, "_upload_http", None)
            if (
                existing is not None
                and not getattr(existing, "_closed", False)
                and not getattr(existing, "is_closed", False)
            ):
                return existing
            if self._is_closed():
                raise ChatGPTError("ChatGPT client is closed")
            client = CurlSession(trust_env=False, timeout=30.0, discard_cookies=True)
            self._upload_http = client
            return client

    def _upload_request(self, url, **options):
        target = urlsplit(url)
        if (
            target.scheme != "https"
            or not target.hostname
            or _is_ip_literal(target.hostname)
            or target.username
            or target.password
        ):
            raise ChatGPTError("Upload URL must be an HTTPS origin without credentials")
        path = (target.path or "/") + ("?" + target.query if target.query else "")
        transport = HTTPTransport(f"https://{target.netloc}", client_getter=self._get_upload_http)
        try:
            response = transport.send("PUT", path, check_status=False, **options)
        except ChatGPTError:
            raise
        except CurlRequestException:
            # Never propagate raw transport exceptions carrying signed SAS upload query tokens.
            raise ChatGPTError("ChatGPT file upload request failed") from None
        if response.status_code >= 400:
            raise ChatGPTError(f"ChatGPT upload returned HTTP {response.status_code}")
        return response

    @staticmethod
    def _consume_project_upload_event(line: bytes | bytearray) -> bool:
        if len(line) > 1024 * 1024:
            raise ChatGPTError("ChatGPT returned an oversized project source event")
        if not line.strip():
            return False
        try:
            event = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ChatGPTError("ChatGPT returned an invalid project source event") from None
        if not isinstance(event, dict):
            raise ChatGPTError("ChatGPT returned an invalid project source event")
        event_type = event.get("type") or event.get("event")
        if event_type in ("file.processing.error", "file.processing.failed"):
            raise ChatGPTError("ChatGPT could not process the project source")
        return event_type == "file.processing.completed"

    def _process_project_upload_stream(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        project_id: str,
        ensure_active,
    ) -> None:
        payload = {
            "entry_surface": "project_sources",
            "file_id": file_id,
            "file_name": file_name,
            "gizmo_id": project_id,
            "index_for_retrieval": True,
            "mime_type": mime_type,
            "use_case": "gizmo",
        }
        with self._tracked_stream(
            "POST",
            "/backend-api/files/process_upload_stream",
            check_status=False,
            json=payload,
            timeout=(30.0, 300.0),
        ) as processing:
            if processing.status_code == 429:
                delay = parse_retry_after(processing.headers.get("retry-after"), now=time.time())
                self._record_rate_limit(delay)
                raise RateLimited(delay, message_submitted=False)
            if processing.status_code in (401, 403):
                raise ChatGPTAuthenticationError(
                    "ChatGPT session expired or was rejected; verify the account"
                )
            if processing.status_code >= 400:
                raise ChatGPTError(
                    f"ChatGPT project source processing returned HTTP {processing.status_code}"
                )
            if "text/event-stream" not in processing.headers.get("content-type", ""):
                raise ChatGPTError("ChatGPT returned an invalid project source processing stream")

            pending = bytearray()
            completed = False
            for chunk in processing.iter_bytes():
                ensure_active()
                pending.extend(chunk)
                offset = 0
                while (end := pending.find(b"\n", offset)) >= 0:
                    completed = self._consume_project_upload_event(pending[offset:end])
                    offset = end + 1
                    if completed:
                        break
                if completed:
                    break
                del pending[:offset]
                if len(pending) > 1024 * 1024:
                    raise ChatGPTError("ChatGPT returned an oversized project source event")
            if not completed:
                completed = self._consume_project_upload_event(pending)
            if not completed:
                raise ChatGPTError("ChatGPT ended project source processing without completion")

    def upload_file(
        self, file_name: str, mime_type: str, data, *, project_id=None, cancel_event=None
    ) -> dict[str, Any]:
        path = data if isinstance(data, Path) else None
        stream = path.open("rb") if path is not None else None
        try:
            before = os.fstat(stream.fileno()) if stream is not None else None
            if before is not None and not stat.S_ISREG(before.st_mode):
                raise ChatGPTError("upload source must be a regular file")
            size = before.st_size if before is not None else len(data)
            max_bytes = 512 * 1024 * 1024
            if mime_type.startswith("image/"):
                max_bytes = min(max_bytes, 20 * 1024 * 1024)
            if Path(file_name).suffix.lower() in {".csv", ".tsv", ".xls", ".xlsx"}:
                max_bytes = min(max_bytes, 50 * 1024 * 1024)
            if not 0 < size <= max_bytes:
                raise ChatGPTError("file exceeds the ChatGPT web upload size limit")

            deadline = time.monotonic() + max(60, min(3600, size / (512 * 1024)))

            def ensure_active():
                if cancel_event is not None and cancel_event.is_set():
                    raise ChatGPTError("file upload cancelled")
                if time.monotonic() >= deadline:
                    raise ChatGPTError("file upload timed out")

            def stream_chunks():
                assert stream is not None
                while True:
                    ensure_active()
                    block = stream.read(256 * 1024)
                    if not block:
                        return
                    yield block

            def verify_unchanged():
                if stream is None:
                    return
                after = os.fstat(stream.fileno())
                if (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                    before.st_ctime_ns,
                ) != (
                    after.st_dev,
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                    after.st_ctime_ns,
                ):
                    raise ChatGPTError("file changed while it was being uploaded")

            ensure_active()
            if mime_type == "image/png" and project_id is None:
                header = stream.read(24) if stream is not None else data[:24]
                if stream is not None:
                    stream.seek(0)
                ensure_active()
                if not header.startswith(b"\x89PNG\r\n\x1a\n") or len(header) < 24:
                    raise ChatGPTError("invalid PNG file")
                result = self.upload_image(
                    file_name,
                    mime_type,
                    stream_chunks() if stream is not None else data,
                    int.from_bytes(header[16:20], "big"),
                    int.from_bytes(header[20:24], "big"),
                    size=size,
                    ensure_active=ensure_active,
                )
                ensure_active()
                verify_unchanged()
                return result

            ensure_active()
            slot = self.request(
                "POST",
                urljoin(CHATGPT_ORIGIN, "/backend-api/files"),
                json={
                    "file_name": file_name,
                    "file_size": size,
                    "use_case": "gizmo" if project_id else "ace_upload",
                    **({"gizmo_id": project_id, "mime_type": mime_type} if project_id else {}),
                },
            ).json()
            ensure_active()
            file_id, upload_url = slot.get("file_id"), slot.get("upload_url")
            if not isinstance(file_id, str) or not isinstance(upload_url, str):
                raise ChatGPTError("invalid file upload slot")
            headers = {"content-type": mime_type, "x-ms-blob-type": "BlockBlob"}
            if stream is not None:
                headers["content-length"] = str(size)
                content = stream_chunks()
            else:
                content = data

            self._upload_request(
                upload_url, content=content, headers=headers, timeout=(30.0, 270.0)
            )
            ensure_active()
            verify_unchanged()
            if project_id:
                self._process_project_upload_stream(
                    file_id, file_name, mime_type, project_id, ensure_active
                )
            else:
                result = self.request(
                    "POST",
                    urljoin(
                        CHATGPT_ORIGIN, f"/backend-api/files/{quote(file_id, safe='')}/uploaded"
                    ),
                    json={},
                ).json()
                if result.get("status") != "success":
                    raise ChatGPTError("provider did not finalize the file upload")
            return {"id": file_id, "name": file_name, "mime_type": mime_type, "size_bytes": size}
        finally:
            if stream is not None:
                stream.close()

    def upload_image(
        self,
        file_name: str,
        mime_type: str,
        data,
        width: int,
        height: int,
        *,
        size: int | None = None,
        ensure_active=None,
    ) -> dict[str, Any]:
        is_bytes = isinstance(data, (bytes, bytearray, memoryview))
        byte_size = len(data) if is_bytes else size
        if mime_type != "image/png" or byte_size is None or not 0 < byte_size <= 20 * 1024 * 1024:
            if mime_type != "image/png":
                raise ChatGPTError("only PNG clipboard images are currently supported")
            raise ChatGPTError("clipboard image must be smaller than 20 MiB")
        if is_bytes and bytes(data[:8]) != b"\x89PNG\r\n\x1a\n":
            raise ChatGPTError("only PNG clipboard images are currently supported")
        slot = self.request(
            "POST",
            urljoin(CHATGPT_ORIGIN, "/backend-api/files"),
            json={
                "file_name": file_name,
                "file_size": byte_size,
                "use_case": "multimodal",
            },
        ).json()
        if ensure_active is not None:
            ensure_active()
        file_id = slot.get("file_id")
        upload_url = slot.get("upload_url")
        if not isinstance(file_id, str) or not isinstance(upload_url, str):
            raise ChatGPTError("ChatGPT returned an invalid image upload slot")
        headers = {"content-type": mime_type, "x-ms-blob-type": "BlockBlob"}
        if not is_bytes:
            headers["content-length"] = str(byte_size)
        self._upload_request(
            upload_url,
            content=data,
            headers=headers,
            timeout=(30.0, 120.0),
        )
        if ensure_active is not None:
            ensure_active()
        finalized = self.request(
            "POST",
            urljoin(CHATGPT_ORIGIN, f"/backend-api/files/{quote(file_id, safe='')}/uploaded"),
            json={},
        ).json()
        if finalized.get("status") != "success":
            raise ChatGPTError("ChatGPT did not finalize the image upload")
        return {
            "id": file_id,
            "name": file_name,
            "mime_type": mime_type,
            "size_bytes": byte_size,
            "width": width,
            "height": height,
        }
