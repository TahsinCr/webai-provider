from __future__ import annotations

import json
import math
import mimetypes
import os
import re
import time
import uuid
from collections import OrderedDict
from dataclasses import replace
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ...accounts import session_exists
from ...accounts.status import SessionState, SessionStatus
from ...base import BaseWebAIProvider, Capability, WebAIProviderType
from ...collections import Page
from ...core.cache import ContinuationCache
from ...core.json import json_dumps, json_loads
from ...core.timing import SendTiming
from ...errors import RateLimited, RemoteNotFound, WebAIProviderError
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.message.protocol import (
    ConversationBound,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    ResponseVerifying,
    TextDelta,
    TextReplacement,
)
from ...resources.message.types import Content, MessageAction, MessageData, actions_for
from ...resources.project.types import ProjectData, ProjectOption, ProjectOptions, source_file
from ...transport.downloader import DownloadSource
from ...transport.network import _is_ip_literal
from ...transport.retry import parse_retry_after
from .constants import CHATGPT_ORIGIN
from .deep_research import apply_deep_research_payload
from .discovery import page as parse_page
from .discovery import project as parse_project
from .discovery import project_id as extract_project_id
from .errors import ChatGPTAuthenticationError, ChatGPTError, SentinelCompatibilityError
from .formats import message_files, message_parts, text_from_parts
from .image import apply_image_tool_payload
from .sentinel import SentinelRuntime

_CONTINUATION_CACHE_TTL_SECONDS = 60.0


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


class _ChatGPTSSEProcessor:
    """Shared incremental ChatGPT SSE frame parser for sync and async streams."""

    def __init__(self, conversation_id: str | None, timing: SendTiming | None) -> None:
        self.resolved_id: str | None = conversation_id
        self.bound_id: str | None = None
        self.rendered_by_channel: dict[str, str] = {}
        self.terminal_seen = False
        self.completion_notified = False
        self.final_seen = False
        self.streamed_messages: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.timing = timing
        self.response_opened: int | None = None
        self.first_sse_event = False
        self.first_assistant_text = False

    def feed_frame(
        self, raw_data: str, *, accepts_bound: bool = False
    ) -> tuple[bool, list[dict[str, Any]]]:
        payload = raw_data.strip()
        if not payload:
            return False, []
        event_at = time.perf_counter_ns() if self.timing is not None else None
        if self.timing is not None and not self.first_sse_event and event_at is not None:
            self.first_sse_event = True
            if self.response_opened is not None:
                self.timing.record("sse_first_event_ms", event_at - self.response_opened)
            self.timing.record("button_to_first_sse_event_ms", event_at - self.timing.started_ns)
        emitted: list[dict[str, Any]] = []
        if payload == "[DONE]":
            self.terminal_seen = True
            if self.final_seen and not self.completion_notified:
                emitted.append({"type": "response_complete"})
                self.completion_notified = True
            if self.timing is not None and event_at is not None:
                if self.response_opened is not None:
                    self.timing.record("sse_last_event_ms", event_at - self.response_opened)
                self.timing.record("button_to_last_sse_event_ms", event_at - self.timing.started_ns)
            return True, emitted
        try:
            event = json_loads(payload)
        except json.JSONDecodeError:
            raise ChatGPTError("ChatGPT stream protocol changed; request not retried") from None
        if not isinstance(event, dict):
            return False, emitted
        if event.get("error"):
            raise ChatGPTError("ChatGPT stream failed; request not retried")
        event_id = event.get("conversation_id")
        if isinstance(event_id, str) and event_id:
            self.resolved_id = event_id
            if event_id != self.bound_id:
                self.bound_id = event_id
                if accepts_bound:
                    emitted.append({"type": "bound", "conversation_id": event_id})
        message = event.get("message")
        if not isinstance(message, dict):
            return False, emitted
        author = message.get("author")
        role = author.get("role") if isinstance(author, dict) else None
        content = message.get("content") or {}
        meta = message.get("metadata") or {}
        is_tool_media = role == "tool" and content.get("content_type") == "multimodal_text"
        is_tool_sdk = role == "tool" and isinstance(meta.get("chatgpt_sdk"), dict)
        if role != "assistant" and not is_tool_media and not is_tool_sdk:
            return False, emitted
        identity = str(message.get("id") or message.get("channel") or "assistant")
        self.streamed_messages[identity] = message
        channel = message.get("channel") or (
            "analysis"
            if content.get("content_type") in ("thoughts", "reasoning_recap", "reasoning_summary")
            else "final"
        )
        if (message.get("metadata") or {}).get("is_visually_hidden_from_conversation"):
            return False, emitted
        if message.get("recipient") not in (None, "all"):
            return False, emitted
        if channel != "analysis":
            self.final_seen = True
        text = text_from_parts(message_parts(message.get("content")))
        rendered = self.rendered_by_channel.get(identity, "")
        if text and text != rendered:
            if (
                self.timing is not None
                and channel != "analysis"
                and not self.first_assistant_text
                and text.strip()
            ):
                self.first_assistant_text = True
                if self.response_opened is not None:
                    self.timing.record(
                        "sse_first_assistant_text_ms",
                        time.perf_counter_ns() - self.response_opened,
                    )
                if event_at is not None:
                    self.timing.record(
                        "button_to_first_assistant_text_ms", event_at - self.timing.started_ns
                    )
            emitted.append(
                {
                    "type": "delta" if text.startswith(rendered) else "replace",
                    "text": text[len(rendered) :] if text.startswith(rendered) else text,
                    "channel": channel,
                    "message_id": identity,
                }
            )
            self.rendered_by_channel[identity] = text
        return False, emitted

    def finish(self) -> tuple[str, list[dict[str, Any]]]:
        if not self.terminal_seen or not self.final_seen:
            raise ChatGPTError(
                "ChatGPT response incomplete; remote outcome unknown; request not retried"
            )
        if not isinstance(self.resolved_id, str) or not self.resolved_id:
            raise ChatGPTError("ChatGPT did not return a conversation ID")
        return self.resolved_id, list(self.streamed_messages.values())


class BaseChatGPTClient:
    """Shared non-I/O protocol, cache, Sentinel, and payload logic for ChatGPT clients."""

    session: Any
    transport: Any
    headers: dict[str, str]
    list_url: str

    def _init_client_state(self, session: Any, *, authenticate: bool = True) -> None:
        self.session = session
        self.cdp_url = session.cdp_url
        self.list_url = ""
        self.headers = {}
        self._authenticated = False
        self._needs_initial_auth = authenticate
        self._conversation_endpoint = "conversations"
        self._conversation_endpoint_verified = False
        self.temporary_conversations: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.pending_branches: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._owned_temporary_conversations = self.temporary_conversations
        self._owned_pending_branches = self.pending_branches
        self._continuation_cache = ContinuationCache[dict[str, str | None]](
            ttl=_CONTINUATION_CACHE_TTL_SECONDS
        )
        self._sentinel_runtime: SentinelRuntime | None = None
        self._sentinel_runtime_key: str | None = None
        self._prewarmed_sentinel: tuple[float, str, dict[str, str]] | None = None
        self._upload_http: Any = None
        self._active_streams: dict[int, Any] = {}
        self._closed = False

    def _clear_local_state(self) -> None:
        self._closed = True
        self._prewarmed_sentinel = None
        runtime_to_close = getattr(self, "_sentinel_runtime", None)
        self._sentinel_runtime = None
        self._sentinel_runtime_key = None
        if runtime_to_close is not None:
            runtime_to_close.close()
        self._continuation_cache.clear()
        owned_temps = getattr(
            self, "_owned_temporary_conversations", getattr(self, "temporary_conversations", None)
        )
        temp_map = getattr(self, "temporary_conversations", None)
        if isinstance(temp_map, dict):
            if temp_map is owned_temps:
                temp_map.clear()
            else:
                self.temporary_conversations = OrderedDict()
        pending = getattr(self, "pending_branches", None)
        owned_pending = getattr(self, "_owned_pending_branches", pending)
        if isinstance(pending, dict):
            if pending is owned_pending:
                pending.clear()
            else:
                self.pending_branches = OrderedDict()

    def _is_closed(self) -> bool:
        return getattr(self, "_closed", False) is True

    def _sentinel_user_agent(self) -> str:
        return (getattr(self, "headers", None) or {}).get("user-agent") or "Mozilla/5.0"

    def _record_rate_limit(self, delay: float | None) -> None:
        record = getattr(self.transport, "record_rate_limit", None)
        if callable(record):
            record(delay)

    def _remember_conversation(self, conversation: dict[str, Any]) -> None:
        conversation_id = conversation.get("conversation_id") or conversation.get("id")
        if not isinstance(conversation_id, str) or not conversation_id:
            return
        current_node = conversation.get("current_node")
        if (
            "current_node" not in conversation
            or not isinstance(current_node, (str, type(None)))
            or not isinstance(conversation.get("messages"), list)
        ):
            self._forget_conversation(conversation_id)
            return
        model = conversation.get("default_model_slug")
        self._continuation_cache.put(
            conversation_id,
            {
                "id": conversation_id,
                "current_node": current_node,
                "default_model_slug": model if isinstance(model, str) else None,
                "gizmo_id": extract_project_id(conversation),
            },
        )

    def _cached_continuation(self, conversation_id: str) -> dict[str, str | None] | None:
        return self._continuation_cache.get(conversation_id)

    def _forget_conversation(self, conversation_id: str) -> None:
        pending = getattr(self, "pending_branches", None)
        if isinstance(pending, dict):
            pending.pop(conversation_id, None)
        self._continuation_cache.remove(conversation_id)

    def _transport_headers(self, method: str, path: str) -> dict[str, str]:
        del method
        headers = dict(self.headers)
        headers["accept-encoding"] = "gzip, deflate"
        target = urlsplit(path).path
        headers["x-openai-target-path"] = target
        headers["x-openai-target-route"] = target
        return headers

    def _apply_refreshed_session(self, response: Any) -> dict[str, str]:
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
        self.headers["authorization"] = f"Bearer {token}"
        cookie_map: dict[str, str] = {}
        for pair in self.headers.get("cookie", "").split(";"):
            name, sep, item = pair.strip().partition("=")
            name = name.strip()
            if sep and name:
                cookie_map[name] = item.strip()
        cookies_attr = getattr(response, "cookies", None)
        jar = getattr(cookies_attr, "jar", None)
        if jar is not None:
            for cookie in jar:
                domain = (cookie.domain or "").lstrip(".")
                if cookie.name and (not domain or domain == "chatgpt.com"):
                    cookie_map[cookie.name] = cookie.value or ""
        self.headers["cookie"] = "; ".join(f"{name}={val}" for name, val in cookie_map.items())
        return dict(self.headers)

    def _check_api_response(
        self, method: str, target_path: str, response: Any, *, expect_json: bool = True
    ) -> Any:
        if response.status_code == 429:
            delay = parse_retry_after(response.headers.get("retry-after"), now=time.time())
            self._record_rate_limit(delay)
            raise RateLimited(delay, message_submitted=False)
        if response.status_code == 404:
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
            ) or target_path
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

    @staticmethod
    def _parse_created_project(
        raw: Any, name: str, instructions: str, memory_scope: str
    ) -> tuple[dict[str, Any], bool]:
        if raw.get("error") or not isinstance(raw.get("resource"), dict):
            raise ChatGPTError("Project creation response changed; request was not replayed")
        project = parse_project(raw["resource"])
        if project is None:
            raise ChatGPTError("Project creation returned no project")
        native = project["metadata"]
        for _ in range(3):
            if not isinstance(native.get("gizmo"), dict):
                break
            native = native["gizmo"]
        display = native.get("display") or {}
        permission = native.get("current_user_permission")
        complete = (
            display.get("name") == name
            and project["instructions"] == instructions
            and (not instructions or isinstance(native.get("instructions"), str))
            and native.get("memory_scope") == memory_scope
            and isinstance(permission, dict)
            and isinstance(permission.get("can_write"), bool)
        )
        return project, not complete

    def _finalize_delete_conversation(self, response: Any, conversation_id: str) -> bool:
        payload = response.json() if getattr(response, "content", None) else None
        if isinstance(payload, dict) and payload.get("success") is False:
            raise ChatGPTError("ChatGPT refused to delete the conversation.")
        self._forget_conversation(conversation_id)
        if getattr(response, "status_code", None) in (200, 202, 204):
            return True
        return isinstance(payload, dict) and (
            payload.get("success") is True or payload.get("is_visible") is False
        )

    def _finalize_move_conversation(
        self, response: Any, conversation_id: str, project_id: str | None
    ) -> dict[str, Any]:
        payload = response.json() if getattr(response, "content", None) else None
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
        cached = self._cached_continuation(conversation_id)
        if cached is not None:
            self._continuation_cache.put(conversation_id, dict(cached, gizmo_id=project_id))
        result = dict(payload) if isinstance(payload, dict) else {}
        result.setdefault("conversation_id", conversation_id)
        result["project_id"] = project_id
        result["gizmo_id"] = project_id
        return result

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
            "title": "Temporary Chat",
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
        self.temporary_conversations[resolved_id] = conversation
        self.temporary_conversations.move_to_end(resolved_id)
        while len(self.temporary_conversations) > 32:
            self.temporary_conversations.popitem(last=False)
        return conversation

    @staticmethod
    def _resolve_model_for_send(
        catalog: Any, previous: dict[str, Any] | None, model: str | None, effort: str | None
    ) -> str:
        resolved = (
            model
            or (previous.get("default_model_slug") if previous else None)
            or catalog.default_model
        )
        selected = next((item for item in catalog.models if item.id == resolved), None)
        if selected is None:
            raise ChatGPTError("Select a model from the current account catalog before sending")
        if effort is not None and effort not in {item.id for item in selected.efforts}:
            raise ChatGPTError(f"Effort '{effort}' is not offered for model '{resolved}'")
        return resolved

    @staticmethod
    def _try_build_streamed_conversation(
        *,
        is_pending_branch: bool,
        conversation_id: str | None,
        resolved_id: str,
        action: Any,
        previous: dict[str, Any] | None,
        prompt: str,
        model: str,
        submitted_message_id: str,
        submitted_message: dict[str, Any] | None,
        streamed_messages: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        last_message = streamed_messages[-1] if streamed_messages else None
        first_message = streamed_messages[0] if streamed_messages else None
        streamed_parent = (
            first_message.get("parent") or first_message.get("parent_message_id")
            if isinstance(first_message, dict)
            else None
        )
        current_node = last_message.get("id") if isinstance(last_message, dict) else None
        can_build = (
            not is_pending_branch
            and (conversation_id is None or resolved_id == conversation_id)
            and action is None
            and (
                conversation_id is None
                or (
                    previous is not None
                    and (previous.get("conversation_id") or previous.get("id")) == conversation_id
                    and "current_node" in previous
                )
            )
            and isinstance(submitted_message, dict)
            and submitted_message.get("id") == submitted_message_id
            and (streamed_parent is None or streamed_parent == submitted_message_id)
            and isinstance(current_node, str)
            and bool(current_node)
        )
        if not can_build:
            return None
        return {
            "id": resolved_id,
            "conversation_id": resolved_id,
            "title": (previous or {}).get("title") or prompt[:60],
            "default_model_slug": model,
            "current_node": current_node,
            "messages": [submitted_message, *streamed_messages],
            "gizmo_id": extract_project_id(previous) if previous else None,
        }

    def _finalize_sent_conversation(
        self,
        *,
        conversation: dict[str, Any],
        resolved_id: str,
        submitted_message_id: str,
        temporary: bool,
        is_pending_branch: bool,
        previous_ids: set[str],
    ) -> tuple[str, dict[str, Any], set[str]]:
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
        return resolved_id, conversation, previous_ids

    def _edit_parent_lookup(
        self, conversation, conversation_id, items, target_index, target, message_id
    ) -> tuple[str | None, str | None]:
        parent = self._find_local_edit_parent(conversation, items, target_index, target, message_id)
        if parent is not None:
            return parent, None
        if not conversation_id or not message_id:
            raise ChatGPTError("The original message parent is unavailable")
        return None, f"{CHATGPT_ORIGIN}/backend-api/conversation/{quote(conversation_id, safe='')}"

    @staticmethod
    def _find_local_edit_parent(
        conversation: dict[str, Any] | None,
        items: list[dict[str, Any]],
        target_index: int,
        target: dict[str, Any],
        message_id: str | None,
    ) -> str | None:
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
        return None

    @staticmethod
    def _validate_message_action_target(
        conversation: dict[str, Any] | None, action: Any, message_id: str | None
    ) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
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
        return items, target_index, target

    @staticmethod
    def _apply_edit_body(
        body: dict[str, Any], target: dict[str, Any], parent_id: str, prompt: str
    ) -> str:
        body["parent_message_id"] = parent_id
        original = target.get("content") or {}
        resource_parts = [part for part in original.get("parts", []) if not isinstance(part, str)]
        body["messages"][0]["content"] = {
            "content_type": original.get("content_type") or "text",
            "parts": resource_parts + [prompt],
        }
        body["messages"][0]["metadata"]["attachments"] = (target.get("metadata") or {}).get(
            "attachments", []
        )
        return body["messages"][0]["id"]

    @staticmethod
    def _apply_retry_body(
        body: dict[str, Any], items: list[dict[str, Any]], target_index: int
    ) -> str:
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

    def _get_or_create_sentinel_runtime(self, user_agent: str) -> SentinelRuntime:
        runtime = getattr(self, "_sentinel_runtime", None)
        if runtime is None or getattr(self, "_sentinel_runtime_key", None) != user_agent:
            replacement = SentinelRuntime(user_agent=user_agent)
            previous_runtime = runtime
            self._sentinel_runtime = replacement
            self._sentinel_runtime_key = user_agent
            runtime = replacement
            if previous_runtime is not None:
                previous_runtime.close()
        return runtime

    @staticmethod
    def _finalize_sentinel_headers(
        runtime: SentinelRuntime,
        initial: str,
        requirements: Any,
        timing: SendTiming | None,
    ) -> dict[str, str]:
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
            **({"model": model} if model else {}),
            "timezone_offset_min": offset_minutes,
            **({"timezone": zone} if zone else {}),
            "conversation_mode": {"kind": "primary_assistant"},
            "force_paragen": False,
            "force_rate_limit": False,
            "reset_rate_limits": False,
            "supported_encodings": [],
            "supports_buffering": False,
        }
        if conversation:
            project_id = extract_project_id(conversation)
            if project_id:
                body["gizmo_id"] = project_id
            conversation_id = conversation.get("conversation_id") or conversation.get("id")
            if isinstance(conversation_id, str) and conversation_id:
                body["conversation_id"] = conversation_id
        return body

    @staticmethod
    def _validate_upload_url(url: str) -> tuple[str, str]:
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
        return f"https://{target.netloc}", path

    @staticmethod
    def _consume_project_upload_event(line: bytes | bytearray) -> bool:
        if len(line) > 1024 * 1024:
            raise ChatGPTError("ChatGPT returned an oversized project source event")
        if not line.strip():
            return False
        try:
            event = json_loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ChatGPTError("ChatGPT returned an invalid project source event") from None
        if not isinstance(event, dict):
            raise ChatGPTError("ChatGPT returned an invalid project source event")
        event_type = event.get("type") or event.get("event")
        if event_type in ("file.processing.error", "file.processing.failed"):
            raise ChatGPTError("ChatGPT could not process the project source")
        return event_type == "file.processing.completed"

    @staticmethod
    def _validate_upload_size(file_name: str, mime_type: str, size: int) -> None:
        max_bytes = 512 * 1024 * 1024
        if mime_type.startswith("image/"):
            max_bytes = min(max_bytes, 20 * 1024 * 1024)
        if Path(file_name).suffix.lower() in {".csv", ".tsv", ".xls", ".xlsx"}:
            max_bytes = min(max_bytes, 50 * 1024 * 1024)
        if not 0 < size <= max_bytes:
            raise ChatGPTError("file exceeds the ChatGPT web upload size limit")

    def _check_stream_response(self, response: Any) -> None:
        if response.status_code == 429:
            delay = parse_retry_after(response.headers.get("retry-after"), now=time.time())
            self._record_rate_limit(delay)
            raise RateLimited(delay, message_submitted=False)
        if response.status_code >= 400:
            raise ChatGPTError(f"ChatGPT submission returned HTTP {response.status_code}")
        content_type = response.headers.get("content-type", "")
        if "text/event-stream" not in content_type:
            raise ChatGPTError(
                f"ChatGPT stream returned unexpected content type {content_type or '<missing>'}"
            )

    @staticmethod
    def _validate_request_url(url: str) -> tuple[str, str]:
        target = urlsplit(url)
        if target.scheme != "https" or target.netloc != "chatgpt.com":
            raise ChatGPTError("authenticated requests must stay on the ChatGPT origin")
        full_path = target.path + ("?" + target.query if target.query else "")
        return target.path, full_path

    @staticmethod
    def _parse_projects_page(raw: Any) -> dict[str, Any]:
        items, next_cursor = parse_page(raw)
        projects = [value for item in items if (value := parse_project(item)) is not None]
        return {"items": projects, "cursor": next_cursor}

    @staticmethod
    def _prepare_create_project(
        name: Any, instructions: str, memory_share: Any
    ) -> tuple[str, str, dict[str, Any]]:
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 50:
            raise ValueError("project name must contain 1–50 characters")
        if type(memory_share) is not bool:
            raise TypeError("memory_share must be a bool")
        cleaned = name.strip()
        memory_scope = "global" if memory_share else "project_v2"
        payload = {"name": cleaned, "instructions": instructions, "memory_scope": memory_scope}
        return cleaned, memory_scope, payload

    @staticmethod
    def _parse_project_or_raise(raw: Any) -> dict[str, Any]:
        value = parse_project(raw)
        if value is None:
            raise ChatGPTError("Requested gizmo is not a project")
        return value

    @staticmethod
    def _parse_project_chats_page(raw: Any, project_id: str) -> dict[str, Any]:
        items, next_cursor = parse_page(raw)
        return {
            "items": [dict(item, project_id=project_id) for item in items],
            "cursor": next_cursor,
        }

    @staticmethod
    def _validate_list_page(offset: int, limit: int) -> dict[str, Any]:
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid conversation page")
        return {
            "offset": offset,
            "limit": limit,
            "order": "updated",
            "is_archived": "false",
            "is_starred": "false",
        }

    def _conversation_endpoints(self) -> tuple[str, ...]:
        endpoint = self._conversation_endpoint
        if self._conversation_endpoint_verified:
            return (endpoint,)
        alternate = "conversation" if endpoint == "conversations" else "conversations"
        return (endpoint, alternate)

    def _record_conversation_result(self, candidate: str, result: Any) -> dict[str, Any]:
        self._conversation_endpoint = candidate
        self._conversation_endpoint_verified = True
        if not isinstance(result, dict):
            raise ChatGPTError("conversation response was not an object")
        self._remember_conversation(result)
        return result

    @staticmethod
    def _validate_conversation_id(conversation_id: Any) -> None:
        if not isinstance(conversation_id, str) or not conversation_id:
            raise ValueError("conversation ID is required")

    @staticmethod
    def _validate_move_args(conversation_id: Any, project_id: Any) -> None:
        BaseChatGPTClient._validate_conversation_id(conversation_id)
        if project_id is not None and (
            not isinstance(project_id, str) or not project_id.startswith("g-p-")
        ):
            raise ValueError("invalid project ID")

    @staticmethod
    def _is_retryable_gateway_error(error: ChatGPTError) -> bool:
        text = str(error)
        return any(f"HTTP {code} " in text for code in (502, 503, 504))

    @staticmethod
    def _validate_send_options(temporary: Any, project_id: Any) -> None:
        if type(temporary) is not bool or (
            project_id is not None
            and (not isinstance(project_id, str) or not project_id.startswith("g-p-"))
        ):
            raise ChatGPTError("invalid conversation options")
        if temporary and project_id:
            raise ChatGPTError("temporary conversations cannot belong to a project")

    def _lookup_local_send_previous(
        self,
        conversation_id: str | None,
        temporary: bool,
        action: Any,
        timing: SendTiming | None,
    ) -> tuple[bool, dict[str, Any] | None, bool]:
        if not conversation_id:
            return True, None, False
        if temporary:
            previous = self.temporary_conversations.get(conversation_id)
            if previous is None:
                raise ChatGPTError(
                    "temporary conversation expired locally; start a new temporary chat"
                )
            return True, previous, False
        pending = getattr(self, "pending_branches", None)
        pending_entry = pending.get(conversation_id) if isinstance(pending, dict) else None
        if pending_entry is not None:
            return True, pending_entry, isinstance(pending_entry.get("pending_branch"), dict)
        may_reuse = action is None
        started = time.perf_counter_ns() if timing is not None and may_reuse else None
        cached = self._cached_continuation(conversation_id) if may_reuse else None
        if started is not None:
            timing.record("pre_send_conversation_cache_lookup_ms", time.perf_counter_ns() - started)
        if cached is not None:
            if timing is not None:
                timing.increment("pre_send_conversation_cache_hit")
            return True, cached, False
        return False, None, False

    def _finalize_send_previous(
        self,
        conversation_id: str | None,
        project_id: str | None,
        action: Any,
        previous: dict[str, Any] | None,
        is_pending_branch: bool,
    ) -> tuple[dict[str, Any] | None, set[str], bool]:
        previous_ids: set[str] = set()
        if conversation_id and previous is not None:
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

    @staticmethod
    def _extract_edit_parent_from_mapping(data: Any, message_id: str) -> str:
        if isinstance(data, dict):
            node = (data.get("mapping") or {}).get(message_id)
            parent = node.get("parent") if isinstance(node, dict) else None
            if isinstance(parent, str) and parent:
                return parent
        raise ChatGPTError("The original message parent is unavailable")

    def _prepare_send_payload(
        self,
        previous: dict[str, Any] | None,
        prompt: str,
        attachments: list[dict[str, Any]],
    ) -> tuple[str | None, dict[str, str], dict[str, Any], str]:
        conv_id = None
        if previous:
            candidate = previous.get("conversation_id") or previous.get("id")
            if isinstance(candidate, str):
                conv_id = candidate
        headers = self.headers.copy()
        body = self.create_conversation_body(previous, prompt, attachments)
        pending_branch = previous.get("pending_branch") if isinstance(previous, dict) else None
        if isinstance(pending_branch, dict):
            branch_chat_id = pending_branch.get("conversation_id")
            branch_msg_id = pending_branch.get("message_id")
            if not isinstance(branch_chat_id, str) or not isinstance(branch_msg_id, str):
                raise ChatGPTError("invalid pending branch continuation context")
            body.pop("conversation_id", None)
            conv_id = None
            body.update(
                branching_from_conversation_id=branch_chat_id,
                branching_from_message_id=branch_msg_id,
                parent_message_id=branch_msg_id,
            )
        return conv_id, headers, body, body["messages"][0]["id"]

    @staticmethod
    def _finalize_send_payload(
        headers: dict[str, str],
        body: dict[str, Any],
        previous: dict[str, Any] | None,
        submitted_message_id: Any,
        *,
        model: str | None,
        effort: str | None,
        temporary: bool,
        tool: Any = None,
        prompt: str = "",
    ) -> dict[str, Any] | None:
        from ...resources.tools.types import DeepResearchTool, ImageGenerationTool, WebSearchTool

        if model:
            body["model"] = model
            body.pop("thinking_effort", None)
        if effort:
            body["thinking_effort"] = effort
        if previous and extract_project_id(previous):
            body["gizmo_id"] = extract_project_id(previous)
        body["history_and_training_disabled"] = temporary
        if isinstance(tool, WebSearchTool):
            body["system_hints"] = ["search"]
            if body.get("messages") and isinstance(body["messages"][0], dict):
                body["messages"][0].setdefault("metadata", {})["system_hints"] = ["search"]
        elif isinstance(tool, ImageGenerationTool):
            apply_image_tool_payload(body)
        elif isinstance(tool, DeepResearchTool):
            apply_deep_research_payload(body, prompt)
        headers.pop("x-openai-web-sse-compression", None)
        headers["x-openai-target-path"] = "/backend-api/f/conversation"
        headers["x-openai-target-route"] = "/backend-api/f/conversation"
        body["supported_encodings"] = []
        body["supports_buffering"] = False
        if not isinstance(submitted_message_id, str) or not submitted_message_id:
            raise ChatGPTError("conversation request has no user message ID")
        return (
            body["messages"][0]
            if body.get("messages") and isinstance(body["messages"][0], dict)
            else None
        )

    @staticmethod
    def _classify_sentinel_prepare_error(error: Exception) -> None:
        if isinstance(error, SentinelCompatibilityError):
            raise error
        if isinstance(error, ChatGPTError):
            if any(code in str(error) for code in ("HTTP 400", "HTTP 403", "HTTP 422")):
                raise SentinelCompatibilityError(
                    "ChatGPT rejected the Sentinel prepare request"
                ) from error
            raise error
        if isinstance(error, ValueError):
            raise SentinelCompatibilityError("Sentinel prepare returned invalid JSON") from error
        raise error

    @staticmethod
    def _project_upload_payload(
        file_id: str, file_name: str, mime_type: str, project_id: str
    ) -> dict[str, Any]:
        return {
            "entry_surface": "project_sources",
            "file_id": file_id,
            "file_name": file_name,
            "gizmo_id": project_id,
            "index_for_retrieval": True,
            "mime_type": mime_type,
            "use_case": "gizmo",
        }

    def _check_project_upload_stream_response(self, processing: Any) -> None:
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

    def _feed_project_upload_chunk(self, pending: bytearray, chunk: bytes) -> bool:
        pending.extend(chunk)
        offset = 0
        while (end := pending.find(b"\n", offset)) >= 0:
            if self._consume_project_upload_event(pending[offset:end]):
                return True
            offset = end + 1
        del pending[:offset]
        if len(pending) > 1024 * 1024:
            raise ChatGPTError("ChatGPT returned an oversized project source event")
        return False

    def _finish_project_upload_buffer(self, pending: bytearray, completed: bool) -> None:
        if not completed:
            completed = self._consume_project_upload_event(pending)
        if not completed:
            raise ChatGPTError("ChatGPT ended project source processing without completion")

    @staticmethod
    def _upload_deadline(size: int) -> float:
        return time.monotonic() + max(60, min(3600, size / (512 * 1024)))

    @staticmethod
    def _make_upload_guard(cancel_event: Any, deadline: float):
        def ensure_active() -> None:
            if cancel_event is not None and cancel_event.is_set():
                raise ChatGPTError("file upload cancelled")
            if time.monotonic() >= deadline:
                raise ChatGPTError("file upload timed out")

        return ensure_active

    @staticmethod
    def _validate_png_header(header: bytes) -> tuple[int, int]:
        if not header.startswith(b"\x89PNG\r\n\x1a\n") or len(header) < 24:
            raise ChatGPTError("invalid PNG file")
        return int.from_bytes(header[16:20], "big"), int.from_bytes(header[20:24], "big")

    @staticmethod
    def _file_slot_payload(
        file_name: str, mime_type: str, size: int, project_id: str | None
    ) -> dict[str, Any]:
        return {
            "file_name": file_name,
            "file_size": size,
            "use_case": "gizmo" if project_id else "ace_upload",
            **({"gizmo_id": project_id, "mime_type": mime_type} if project_id else {}),
        }

    @staticmethod
    def _validate_upload_slot(slot: Any, error_message: str) -> tuple[str, str]:
        file_id, upload_url = slot.get("file_id"), slot.get("upload_url")
        if not isinstance(file_id, str) or not isinstance(upload_url, str):
            raise ChatGPTError(error_message)
        return file_id, upload_url

    @staticmethod
    def _upload_blob_headers(mime_type: str, size: int, *, streamed: bool) -> dict[str, str]:
        headers = {"content-type": mime_type, "x-ms-blob-type": "BlockBlob"}
        if streamed:
            headers["content-length"] = str(size)
        return headers

    @staticmethod
    def _validate_image_upload(mime_type: str, data: Any, size: int | None) -> tuple[bool, int]:
        is_bytes = isinstance(data, (bytes, bytearray, memoryview))
        byte_size = len(data) if is_bytes else size
        if mime_type != "image/png" or byte_size is None or not 0 < byte_size <= 20 * 1024 * 1024:
            if mime_type != "image/png":
                raise ChatGPTError("only PNG clipboard images are currently supported")
            raise ChatGPTError("clipboard image must be smaller than 20 MiB")
        if is_bytes and bytes(data[:8]) != b"\x89PNG\r\n\x1a\n":
            raise ChatGPTError("only PNG clipboard images are currently supported")
        return is_bytes, byte_size


class BaseChatGPTProjectEditor:
    """Shared non-I/O project options, update payloads, and upload confirmation."""

    supports_batch_add = True

    @staticmethod
    def _create_options() -> ProjectOptions:
        return ProjectOptions(
            fields=("name",),
            visibility=(ProjectOption("private", "Private"),),
            memory=(
                ProjectOption("default", "Default"),
                ProjectOption("project_only", "Project only"),
            ),
        )

    @staticmethod
    def _edit_options_from_metadata(raw: dict[str, Any]) -> ProjectOptions:
        for _ in range(3):
            if not isinstance(raw.get("gizmo"), dict):
                break
            raw = raw["gizmo"]
        if raw.get("current_user_permission", {}).get("can_write") is False:
            return ProjectOptions(source_error="Proje salt okunur")
        accepted = next(
            (
                tool.get("accepted_mime_types")
                for tool in raw.get("tools", [])
                if tool.get("type") == "retrieval"
            ),
            None,
        )
        return ProjectOptions(
            fields=("name", "instructions"),
            sources=(
                {
                    "id": "file",
                    "title": "File",
                    "accept": ",".join(accepted) if accepted else "*/*",
                },
                {"id": "text", "title": "Text"},
            ),
            remove_sources=True,
        )

    @staticmethod
    def _update_payload(
        native: dict[str, Any], current_name: Any, current_instructions: Any, values: dict[str, Any]
    ) -> dict[str, Any]:
        for _ in range(3):
            if not isinstance(native.get("gizmo"), dict):
                break
            native = native["gizmo"]
        display = native.get("display") or {}
        return {
            "name": values.get("name", current_name),
            "instructions": values.get("instructions", current_instructions),
            "emoji": display.get("emoji"),
            "theme": display.get("theme"),
            "memory_scope": native.get("memory_scope"),
        }

    @staticmethod
    def _confirmed_files(rows: Any, expected_ids: set[str]) -> dict[str, dict[str, Any]]:
        confirmed: dict[str, dict[str, Any]] = {}
        if not isinstance(rows, list):
            return confirmed
        for row in rows:
            if not isinstance(row, dict):
                continue
            file_id = row.get("file_id") or row.get("id")
            if isinstance(file_id, str) and file_id in expected_ids:
                if file_id in confirmed:
                    return {}
                confirmed[file_id] = row
        return confirmed

    @staticmethod
    def _build_attached_results(
        uploads: list[dict[str, Any]], confirmed: dict[str, dict[str, Any]]
    ) -> list[Attachment]:
        results = []
        for upload in uploads:
            file_id = upload["file_id"]
            remote = confirmed[file_id]
            results.append(
                Attachment(
                    file_id,
                    str(remote.get("name") or upload["name"]),
                    remote.get("type") or upload["type"],
                    metadata=dict(remote),
                )
            )
        return results

    @staticmethod
    def _project_url(project_id: str) -> str:
        return "https://chatgpt.com/backend-api/projects/" + quote(project_id, safe="")

    @staticmethod
    def _extract_update_state(
        provided: Any, fetched: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], Any, Any] | None:
        if (
            provided is not None
            and isinstance(getattr(provided, "metadata", None), dict)
            and provided.metadata
        ):
            return dict(provided.metadata), provided.name, provided.instructions
        if fetched is not None:
            return fetched["metadata"], fetched["name"], fetched["instructions"]
        return None

    @staticmethod
    def _updated_project_result(
        provided: Any, values: dict[str, Any], current_name: Any, current_instructions: Any
    ) -> ProjectData | None:
        if isinstance(provided, ProjectData):
            return replace(
                provided,
                name=values.get("name", current_name),
                instructions=values.get("instructions", current_instructions),
            )
        return None

    @staticmethod
    def _prepare_text_source(values: dict[str, Any]) -> tuple[str, bytes, int, int, str]:
        name, data, mime = values["name"], values["text"].encode(), "text/plain"
        if not name.endswith(".txt"):
            name += ".txt"
        return name, data, len(data), int(time.time() * 1000), mime

    @staticmethod
    def _prepare_file_source(path: Any) -> tuple[str, Path, int, int, str]:
        file = source_file(path)
        metadata = file.stat()
        return (
            file.name,
            file,
            metadata.st_size,
            metadata.st_mtime_ns // 1_000_000,
            mimetypes.guess_type(file.name)[0] or "application/octet-stream",
        )

    @staticmethod
    def _build_uploaded_source_entry(
        uploaded: dict[str, Any], name: str, mime: str, size: int, last_modified: int
    ) -> dict[str, Any]:
        return {
            "file_id": uploaded["id"],
            "last_modified": last_modified,
            "name": name,
            "size": uploaded.get("size_bytes", size),
            "type": mime,
            "location": "fs",
        }

    @staticmethod
    def _validate_upload_ids(uploads: list[dict[str, Any]]) -> set[str]:
        ids = {item["file_id"] for item in uploads}
        if len(ids) != len(uploads):
            raise WebAIProviderError("ChatGPT returned duplicate project upload IDs")
        return ids

    @staticmethod
    def _extract_attached_rows(attached: Any) -> Any:
        if callable(getattr(attached, "json", None)) and getattr(attached, "content", None):
            attached = attached.json()
        return attached.get("files", [attached]) if isinstance(attached, dict) else attached

    @classmethod
    def _finalize_attached_sources(
        cls,
        uploads: list[dict[str, Any]],
        confirmed: dict[str, dict[str, Any]],
        ids: set[str],
    ) -> list[Attachment]:
        if confirmed.keys() != ids:
            raise WebAIProviderError(
                "ChatGPT did not confirm all project attachments; reload the project before retrying"
            )
        return cls._build_attached_results(uploads, confirmed)

    @staticmethod
    def _removed_project_result(current_project: Any, rid: str) -> ProjectData | None:
        if isinstance(current_project, ProjectData):
            return replace(
                current_project,
                resources=tuple(r for r in current_project.resources if r.id != rid),
            )
        return None


class BaseChatGPTWebAIProvider(BaseWebAIProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = WebAIProviderType.CHATGPT

    _capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.DELETE,
            Capability.PROJECT_DELETE,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.HISTORY,
            Capability.RENAME,
            Capability.AUTH,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_MEMBERSHIP,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
            Capability.WEB_SEARCH,
            Capability.IMAGE_GENERATION,
            Capability.DEEP_RESEARCH,
        }
    )

    def _project(self, raw: dict[str, Any]) -> ProjectData:
        return ProjectData(
            raw["id"],
            raw["name"],
            raw.get("instructions"),
            tuple(
                Attachment(
                    str(item.get("file_id") or item.get("id") or ""),
                    str(item.get("name") or ""),
                    item.get("type"),
                    metadata=dict(item),
                )
                for item in raw.get("resources", [])
            ),
            dict(raw.get("metadata") or {}),
        )

    def _message(self, item: dict[str, Any], *, allow_actions: bool) -> MessageData:
        content = item.get("content") or {}
        parts = message_parts(content)
        author = item.get("author")
        role = author.get("role", "unknown") if isinstance(author, dict) else "unknown"
        channel = item.get("channel")
        public_recipient = item.get("recipient") in (None, "all")
        summary = isinstance(content, dict) and content.get("content_type") in (
            "thoughts",
            "reasoning_recap",
            "reasoning_summary",
        )
        if summary or (role == "assistant" and not public_recipient):
            channel = "analysis"
        if len(parts) == 1 and isinstance(parts[0], str):
            blocks = (Content("text", text=parts[0]),)
        else:
            blocks = tuple(
                Content("text", text=part)
                if isinstance(part, str)
                else Content("text", text=part["text"], metadata={"native": part})
                if isinstance(part, dict) and isinstance(part.get("text"), str)
                else Content("provider", metadata={"value": part})
                for part in parts
            )
        message_id = item.get("id", "")
        return MessageData(
            str(message_id),
            role,
            blocks,
            channel,
            dict(item),
            actions=actions_for(role, branch=True)
            if allow_actions and message_id and channel != "analysis" and public_recipient
            else (),
        )

    @staticmethod
    def _active_messages(raw: dict[str, Any]) -> list[dict[str, Any]]:
        mapping = raw.get("mapping")
        if not isinstance(mapping, dict) or not mapping:
            messages = raw.get("messages")
            return messages if isinstance(messages, list) else []
        node_id = raw.get("current_node")
        if not isinstance(node_id, str) or node_id not in mapping:
            raise ValueError("ChatGPT conversation has no active message path")
        reversed_messages: list[dict[str, Any]] = []
        visited: set[str] = set()
        while node_id is not None:
            if node_id in visited:
                raise ValueError("ChatGPT conversation message path contains a cycle")
            visited.add(node_id)
            node = mapping.get(node_id)
            if not isinstance(node, dict):
                raise ValueError("ChatGPT conversation message path is incomplete")
            message = node.get("message")
            if isinstance(message, dict):
                reversed_messages.append(message)
            parent_id = node.get("parent")
            if parent_id is None:
                break
            if not isinstance(parent_id, str) or parent_id not in mapping:
                raise ValueError("ChatGPT conversation message path is incomplete")
            node_id = parent_id
        reversed_messages.reverse()
        return reversed_messages

    def _conversation(self, raw: dict[str, Any]) -> Conversation:
        remote_id = raw.get("conversation_id") or raw.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise ValueError("provider conversation has no remote ID")
        allow_actions = not (
            raw.get("pending_branch") or raw.get("is_read_only") or raw.get("is_temporary_chat")
        )
        messages, turn = [], []
        for item in self._active_messages(raw):
            if not isinstance(item, dict):
                continue
            message = self._message(item, allow_actions=allow_actions)
            if message.role == "user":
                turn.clear()
            else:
                turn.append(item)
            if (
                message.role == "assistant"
                and message.channel != "analysis"
                and item.get("recipient") in (None, "all")
            ):
                message.metadata["files"] = message_files(*turn)
                turn.clear()
            messages.append(message)
        metadata = {key: value for key, value in raw.items() if key != "mapping"}
        latest_assistant = next(
            (message for message in reversed(messages) if message.role == "assistant"), None
        )
        if latest_assistant and latest_assistant.metadata.get("status") == "in_progress":
            generation: dict[str, Any] = {"active": True}
            started_at = latest_assistant.metadata.get("create_time")
            if (
                isinstance(started_at, (int, float))
                and not isinstance(started_at, bool)
                and 0 < started_at < 4_102_444_800
                and math.isfinite(started_at)
            ):
                generation["started_at"] = started_at
            metadata["generation"] = generation
        pid = extract_project_id(raw)
        return Conversation(
            remote_id,
            raw.get("title") or "Untitled",
            tuple(messages),
            project=ProjectData(pid, "") if pid else None,
            metadata=metadata,
        )

    @staticmethod
    def _parse_branch_preview(
        result: Any, conversation_id: str, message_id: str
    ) -> tuple[str, dict[str, Any]]:
        preview = result.get("conversation") if isinstance(result, dict) else None
        if not isinstance(preview, dict):
            raise ValueError("ChatGPT did not confirm a new conversation")
        remote = preview.get("conversation_id")
        if not isinstance(remote, str) or not remote or remote == conversation_id:
            raise ValueError("ChatGPT did not confirm a new conversation")
        if quote(remote, safe="") != remote:
            raise ValueError("Invalid branch conversation identity")
        mapping, node = preview.get("mapping"), preview.get("current_node")
        if not isinstance(mapping, dict) or node not in mapping:
            raise ValueError("ChatGPT branch preview has no selected message path")
        selected, seen = [], set()
        while node in mapping and node not in seen:
            seen.add(node)
            item = mapping[node]
            if item.get("message"):
                selected.append(item["message"])
            node = item.get("parent")
        normalized = dict(
            preview,
            messages=list(reversed(selected)),
            pending_branch={"conversation_id": conversation_id, "message_id": message_id},
        )
        normalized.pop("mapping", None)
        return remote, normalized

    @staticmethod
    def _validate_file_service_id(parsed: Any, conversation_id: str | None) -> str:
        file_id = parsed.netloc
        if (
            not re.fullmatch(r"file[-_][A-Za-z0-9_-]+", file_id)
            or parsed.path
            or not conversation_id
        ):
            raise ValueError("File must belong to the selected conversation")
        return file_id

    @staticmethod
    def _select_sandbox_message(
        chat_payload: dict[str, Any], sandbox_path: str, message_id: str | None
    ) -> dict[str, Any] | None:
        candidates = chat_payload.get("messages", [])
        if not candidates and isinstance(chat_payload.get("mapping"), dict):
            candidates = [
                node.get("message")
                for node in chat_payload["mapping"].values()
                if isinstance(node, dict)
            ]
        matched = [
            item
            for item in candidates
            if isinstance(item, dict) and sandbox_path in json_dumps(item.get("content", {}))
        ]
        selected = next(
            (item for item in matched if item.get("id") == message_id),
            matched[0] if len(matched) == 1 else None,
        )
        return selected if isinstance(selected, dict) and selected.get("id") else None

    @staticmethod
    def _build_download_source(result: Any, client: Any, filename: str | None) -> DownloadSource:
        target = result.get("download_url") if isinstance(result, dict) else None
        if not target or urlsplit(target).scheme != "https":
            raise ValueError("Provider did not return a file download URL")
        headers = dict(client.headers) if urlsplit(target).netloc == "chatgpt.com" else {}
        return DownloadSource(target, filename, True, headers)

    @property
    def session(self) -> Any:
        return self.session_state

    def _active_transport(self) -> Any:
        return self._effective_transport

    def _transport_headers(self, method: str, path: str) -> dict[str, str]:
        client = self._client
        if client is not None and callable(getattr(client, "_transport_headers", None)):
            return client._transport_headers(method, path)
        return {}

    def _session_status(self) -> SessionStatus:
        saved = session_exists(self.session_state.session_file)
        return SessionStatus(
            SessionState.UNKNOWN if saved else SessionState.LOGIN_REQUIRED,
            saved,
            "snapshot_not_verified" if saved else "no_saved_session",
        )

    @staticmethod
    def _parse_list_chats_args(cursor: str | None, limit: int) -> int:
        offset = int(cursor or "0")
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid conversation page")
        return offset

    @staticmethod
    def _compute_list_backoff(error: RateLimited) -> float:
        retry_after = error.retry_after
        if (
            not isinstance(retry_after, (int, float))
            or not math.isfinite(retry_after)
            or retry_after <= 0
        ):
            retry_after = 2.0
        return max(0.75, retry_after)

    def _build_list_chats_page(self, raw: Any, offset: int, limit: int) -> Page[Conversation]:
        items = raw.get("items", [])
        if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
            raise ValueError("invalid conversation page")
        return Page(
            tuple(self._conversation(item) for item in items),
            str(offset + len(items)) if len(items) == limit else None,
        )

    def _register_pending_branch(self, client: Any, remote: str, preview: dict[str, Any]) -> None:
        client.pending_branches = self._pending_branches
        client.pending_branches[remote] = preview
        client.pending_branches.move_to_end(remote)
        while len(client.pending_branches) > 32:
            client.pending_branches.popitem(last=False)
        remember = getattr(client, "_remember_conversation", None)
        if callable(remember):
            remember(preview)

    @staticmethod
    def _check_delete_project_response(response: Any) -> bool:
        payload = (
            response.json() if response is not None and getattr(response, "content", None) else None
        )
        if isinstance(payload, dict) and payload.get("success") is False:
            raise ChatGPTError("ChatGPT refused to delete the project.")
        return getattr(response, "status_code", None) in (200, 202, 204)

    @staticmethod
    def _build_send_kwargs(request: ProviderRequest, attachments: list[Any]) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if attachments:
            kwargs["attachments"] = attachments
        if request.model is not None:
            kwargs["model"] = request.model
        if request.effort is not None:
            kwargs["effort"] = request.effort
        if request.project_id:
            kwargs["project_id"] = request.project_id
        if request.temporary:
            kwargs["temporary"] = True
        if request.action:
            kwargs["action"] = request.action
            kwargs["message_id"] = request.message_id
        return kwargs

    @staticmethod
    def _translate_raw_event(
        value: dict[str, Any], bound_emitted: str | None
    ) -> tuple[str | None, ProviderStreamEvent | None]:
        kind = value.get("type")
        if kind == "bound":
            cid = value.get("conversation_id")
            if isinstance(cid, str) and cid and cid != bound_emitted:
                return cid, ConversationBound(cid)
            return bound_emitted, None
        if kind == "delta":
            return bound_emitted, TextDelta(
                value.get("text", ""), value.get("channel"), value.get("message_id")
            )
        if kind == "replace":
            return bound_emitted, TextReplacement(
                value.get("text", ""), value.get("channel"), value.get("message_id")
            )
        if kind == "response_complete":
            return bound_emitted, ResponseComplete()
        if kind == "response_verifying":
            return bound_emitted, ResponseVerifying()
        return bound_emitted, None

    @staticmethod
    def _has_file_service_url(chat_payload: dict[str, Any], url: str) -> bool:
        return any(
            file.get("url") == url
            for item in chat_payload.get("messages", ())
            if isinstance(item, dict)
            for file in message_files(item)
        )

    @staticmethod
    def _parse_estuary_file_id(parsed: Any, conversation_id: str | None) -> str | None:
        if (
            parsed.scheme == "https"
            and parsed.netloc == "chatgpt.com"
            and parsed.path == "/backend-api/estuary/content"
        ):
            file_id = parse_qs(parsed.query).get("id", [None])[0]
            if file_id and file_id.startswith(("file-", "file_")) and conversation_id:
                return file_id
            return ""
        return None

    @staticmethod
    def _validate_sandbox_path(parsed: Any, conversation_id: str | None) -> str:
        if not conversation_id:
            raise ValueError("Conversation ID is not available for this file yet")
        sandbox_path = unquote(parsed.path)
        if not sandbox_path.startswith("/mnt/data/"):
            raise ValueError("Unsupported conversation file path")
        return sandbox_path
