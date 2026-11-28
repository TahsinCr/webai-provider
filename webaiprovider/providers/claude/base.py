from __future__ import annotations

import mimetypes
import os
import stat
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit

from ...accounts import delete_session, session_exists
from ...accounts.status import SessionState, SessionStatus
from ...base import BaseWebAIProvider, Capability, WebAIProviderType
from ...collections import Page
from ...core.cache import ContinuationCache
from ...core.json import json_loads
from ...core.timing import SendTiming
from ...errors import AccountPaused, FeatureUnavailable, WebAIProviderError
from ...resources.chat.types import Conversation
from ...resources.message.attachment import CLAUDE_ATTACHMENTS, Attachment
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    PartClosed,
    ProviderRequest,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.message.types import MessageAction
from ...resources.project.types import ProjectData, ProjectOptions
from ...resources.response.artifact import MAX_FILE_REFERENCES, files_from
from ...resources.response.source import sources_from
from ...transport.downloader import DownloadSource, NativeDownloadClient, cookie_header
from .catalogs import parse_model_catalog
from .errors import ClaudeCompatibilityError, ClaudeRequestNotSent, ClaudeStreamError
from .records import CLAUDE_FILE_RULE, action_target, conversation, presented_files, project

ORIGIN = "https://claude.ai"
_WEB_SEARCH_TOOL = {"type": "web_search_v0", "name": "web_search"}
_ALLOWED_UPLOAD_SUFFIXES = frozenset(CLAUDE_ATTACHMENTS.allowed_extensions)


def validate_upload_candidate(path=None, *, content=None, name=None, mime_type=None):
    """Validate and resolve a local path or byte payload against Claude upload limits."""
    if content is not None:
        filename = name or (Path(path).name if path is not None else "attachment")
        suffix = Path(filename).suffix.lower()
        size = len(content)
        local_path = None
    elif path is not None:
        resolved = Path(path).expanduser().resolve(strict=True)
        try:
            with resolved.open("rb") as handle:
                info = os.fstat(handle.fileno())
        except OSError:
            raise ValueError("Claude files must not exceed 30 MB") from None
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("Claude files must not exceed 30 MB")
        filename = name or resolved.name
        suffix = resolved.suffix.lower()
        size = info.st_size
        local_path = str(resolved)
    else:
        raise ValueError("Claude upload requires a file path or byte content")

    if suffix not in _ALLOWED_UPLOAD_SUFFIXES or not 0 < size <= CLAUDE_ATTACHMENTS.max_bytes:
        raise ValueError("Claude files must not exceed 30 MB")
    mime = mime_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return filename, mime, size, content, local_path


def remote_id(value: Any) -> str:
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Invalid Claude identity") from None


def organization_from_bootstrap(data: Any, selected: Any) -> str:
    account = data.get("account")
    if not isinstance(account, dict):
        raise AccountPaused("Claude session could not be verified")
    organizations = [x.get("organization", {}) for x in account.get("memberships", [])]
    available = [x for x in organizations if "chat" in x.get("capabilities", [])]
    org = next((x for x in available if x.get("uuid") == selected), None)
    if org is None:
        if not selected and len(available) == 1:
            org = available[0]
        else:
            raise WebAIProviderError("Claude workspace selection is invalid; reverify the account")
    return org["uuid"]


class _ClaudeSSEProcessor:
    """Shared incremental Claude SSE state machine for both sync and async streams."""

    def __init__(self, *, timing: Any, response_opened: int | None, action: Any) -> None:
        self.timing = timing
        self.response_opened = response_opened
        self.action = action
        self.completed = False
        self.completion_notified = False
        self.blocks: dict[int, Any] = {}
        self.response_id: str | None = None
        self.parent_id: str | None = None
        self.final_index = 0
        self.response_model: str | None = None
        self.presented_chunks: dict[int, tuple[list[str], int]] = {}
        self.raw_source_roots: list[dict[str, Any]] = []
        self.raw_file_roots: list[dict[str, Any]] = []
        self.first_event = False
        self.first_assistant_text = False
        self.last_event_at: int | None = None

    def _record_first_assistant_text(self) -> None:
        if self.timing is None or self.first_assistant_text:
            return
        self.first_assistant_text = True
        now = time.perf_counter_ns()
        if self.response_opened is not None:
            self.timing.record("sse_first_assistant_text_ms", now - self.response_opened)
        self.timing.record("button_to_first_assistant_text_ms", now - self.timing.started_ns)

    def _part_id(self, index: int) -> str:
        return f"{self.response_id}:{index}" if self.response_id else str(index)

    def feed_raw(self, raw: str):
        if raw == "[DONE]":
            return
        try:
            event = json_loads(raw)
        except ValueError as error:
            raise ClaudeCompatibilityError("Claude returned an invalid SSE event") from error
        if not isinstance(event, dict):
            raise ClaudeCompatibilityError("Claude returned an invalid SSE event")
        event_at = time.perf_counter_ns() if self.timing is not None else None
        if self.timing is not None:
            self.last_event_at = event_at
            if not self.first_event and self.response_opened is not None and event_at is not None:
                self.first_event = True
                self.timing.record("sse_first_event_ms", event_at - self.response_opened)
                self.timing.record(
                    "button_to_first_sse_event_ms", event_at - self.timing.started_ns
                )
        kind, delta = event.get("type"), event.get("delta", {})
        if kind == "message_start":
            native = event.get("message") or {}
            self.response_id = native.get("uuid") or native.get("id")
            self.parent_id = native.get("parent_uuid")
            self.response_model = native.get("model")
        if kind == "error" or event.get("error"):
            raise ClaudeStreamError("Claude stream failed; the request was not retried")
        if kind == "content_block_start":
            block = event.get("content_block", {})
            block_type = block.get("type")
            block_index = event.get("index", 0)
            if block:
                self.raw_file_roots.append(block)
            if block_type == "tool_use" and block.get("name") == "present_files":
                presented = presented_files(block)
                if presented:
                    self.raw_file_roots.append({"files": presented})
                self.presented_chunks[block_index] = ([], 0)
            self.blocks[block_index] = block_type
            if block_type == "web_search_tool_result" or (
                block_type == "tool_result" and block.get("name") == "web_search"
            ):
                results = block.get("content")
                if isinstance(results, list) and results:
                    self.raw_source_roots.append({"sources": results})
            elif block_type == "text" and block:
                self.raw_source_roots.append(block)
            if block_type == "text" and block.get("text"):
                self.final_index = block_index
                if block["text"].strip():
                    self._record_first_assistant_text()
                yield {
                    "type": "delta",
                    "text": block["text"],
                    "part_id": self._part_id(block_index),
                }
            elif block_type == "thinking" and block.get("thinking"):
                yield {
                    "type": "delta",
                    "channel": "analysis",
                    "text": block["thinking"],
                    "part_id": self._part_id(block_index),
                }
        if kind == "content_block_delta":
            index = event.get("index", 0)
            delta_type = delta.get("type")
            if delta_type == "input_json_delta" and index in self.presented_chunks:
                piece = delta.get("partial_json") or ""
                chunks_list, total_len = self.presented_chunks[index]
                new_len = total_len + len(piece)
                if new_len <= 65536:
                    chunks_list.append(piece)
                    self.presented_chunks[index] = (chunks_list, new_len)
                else:
                    self.presented_chunks.pop(index)
            if delta_type in ("citations_delta", "citation_start_delta"):
                citation = delta.get("citation")
                if citation is not None:
                    if (
                        self.raw_source_roots
                        and len(self.raw_source_roots[-1]) == 1
                        and "citations" in self.raw_source_roots[-1]
                        and len(self.raw_source_roots[-1]["citations"]) < 128
                    ):
                        self.raw_source_roots[-1]["citations"].append(citation)
                    else:
                        self.raw_source_roots.append({"citations": [citation]})
            channel = self.blocks.get(index)
            if delta_type == "thinking_delta" and channel == "thinking":
                yield {
                    "type": "delta",
                    "channel": "analysis",
                    "text": delta.get("thinking", ""),
                    "part_id": self._part_id(index),
                }
            elif delta_type == "text_delta" and channel == "text":
                text = delta.get("text", "")
                if text:
                    self.final_index = index
                if text.strip():
                    self._record_first_assistant_text()
                yield {"type": "delta", "text": text, "part_id": self._part_id(index)}
        if kind == "content_block_stop":
            index = event.get("index", 0)
            block_type = self.blocks.pop(index, None)
            if block_type in ("thinking", "text"):
                yield {
                    "type": "part_closed",
                    "part_id": self._part_id(index),
                    "channel": "analysis" if block_type == "thinking" else "final",
                }
            entry = self.presented_chunks.pop(event.get("index", 0), None)
            if entry is not None:
                presented = BaseClaudeClient._presented_file_entries(entry[0])
                if presented:
                    self.raw_file_roots.append({"files": presented})
        elif "completion" in event:
            text = event.get("completion") or ""
            if text.strip():
                self._record_first_assistant_text()
            yield {"type": "delta", "text": text}
            self.completed = bool(event.get("stop_reason"))
        if kind == "message_stop" or (kind == "message_delta" and delta.get("stop_reason")):
            self.completed = True
        if self.completed and not self.completion_notified:
            yield {"type": "response_complete"}
            self.completion_notified = True

    def finish(self, cid: str, is_temporary: bool, continuations: Any):
        if (
            self.last_event_at is not None
            and self.response_opened is not None
            and self.timing is not None
        ):
            self.timing.record("sse_last_event_ms", self.last_event_at - self.response_opened)
            self.timing.record(
                "button_to_last_sse_event_ms", self.last_event_at - self.timing.started_ns
            )
        if not self.completed:
            continuations.remove(cid)
            raise ClaudeStreamError("Claude response ended before completion; it was not resent")
        if self.response_id:
            continuations.put(cid, (self.response_id, is_temporary))
            sources = BaseClaudeClient._normalize_sources(self.raw_source_roots)
            files = BaseClaudeClient._normalize_files(self.raw_file_roots)
            yield {
                "type": "metadata",
                "message_id": self.response_id + ":" + str(self.final_index),
                "user_message_id": self.parent_id + ":0"
                if isinstance(self.parent_id, str) and self.action != MessageAction.RETRY
                else None,
                "sources": sources,
                "files": files,
                "model": self.response_model,
                "actions": ["retry"],
            }
        yield {"type": "done"}


def _translate_claude_event(event: dict[str, Any], request: ProviderRequest):
    kind = event["type"]
    if kind == "bound":
        return ConversationBound(event["id"])
    if kind in ("replace", "delta"):
        event_type = TextDelta if kind == "delta" else TextReplacement
        return event_type(event["text"], event.get("channel", "final"), event.get("part_id"))
    if kind == "part_closed":
        return PartClosed(event["part_id"], event["channel"])
    if kind == "response_complete":
        return ResponseComplete()
    if kind == "metadata":
        return MessageMetadata(
            message_id=event.get("message_id"),
            user_message_id=event.get("user_message_id"),
            model=event.get("model") or request.model,
            effort=request.effort,
            sources=tuple(event.get("sources", ())),
            files=tuple(event.get("files", ())),
        )
    return None


class BaseClaudeClient:
    """Shared non-I/O protocol logic for ClaudeClient and AsyncClaudeClient."""

    ORIGIN = ORIGIN
    session: Any
    http: Any
    catalog: Any
    _continuations: Any

    def _validate_transport_origin(self, transport: Any) -> None:
        if transport is not None and self.http.origin != self.ORIGIN:
            raise ValueError("Provider transport origin does not match its account")

    @staticmethod
    def _resolve_organization(data: Any, values: dict[str, Any]) -> str:
        last_org = values.get("cookies", values).get("lastActiveOrg")
        try:
            organization = organization_from_bootstrap(data, last_org)
        except AccountPaused:
            raise
        except WebAIProviderError:
            organization = organization_from_bootstrap(data, None)
        return remote_id(organization)

    @staticmethod
    def _parse_upload_result(result: Any, filename: str, mime: str, size: int) -> dict[str, Any]:
        if not isinstance(result, dict):
            raise ClaudeCompatibilityError("Claude file upload response changed")
        file_id = result.get("file_uuid") or result.get("uuid") or result.get("id")
        if isinstance(file_id, str) and file_id:
            return {
                "id": file_id,
                "name": filename,
                "mime_type": mime,
                "size_bytes": size,
                "provider": "claude",
            }
        extracted = result.get("extracted_content")
        if isinstance(extracted, str):
            doc_name = str(result.get("file_name") or filename)
            raw_size = result.get("file_size")
            doc_size = (
                raw_size if isinstance(raw_size, int) and not isinstance(raw_size, bool) else size
            )
            doc = {
                "file_name": doc_name,
                "file_type": str(result.get("file_type") or ""),
                "file_size": doc_size,
                "extracted_content": extracted,
            }
            return {
                "id": doc_name,
                "name": doc_name,
                "mime_type": mime,
                "size_bytes": doc_size,
                "provider": "claude",
                "claude_document": doc,
            }
        raise ClaudeCompatibilityError("Claude file upload returned no file identity")

    @staticmethod
    def _apply_history_action(history: dict[str, Any], v: dict[str, Any]):
        target = action_target(history, v["message_id"], v["action"])
        if v["action"].value not in target.actions:
            raise WebAIProviderError("This Claude message action is unavailable")
        original = target.metadata["native_message"]
        parent = original.get("parent_message_uuid")
        updated_v = dict(
            v,
            attachments=[
                dict(item, id=item.get("uuid") or item.get("id"))
                for item in original.get("files", [])
            ],
            original_attachments=original.get("attachments", []),
        )
        if updated_v["action"] == MessageAction.RETRY:
            parent = original["uuid"]
        return parent, updated_v

    @staticmethod
    def _build_completion_body(model, choice, parent, v):
        inline_docs = [
            item["claude_document"]
            for item in v.get("attachments", [])
            if isinstance(item, dict) and isinstance(item.get("claude_document"), dict)
        ]
        body = {
            "prompt": "" if v.get("action") == MessageAction.RETRY else v["prompt"],
            "model": model,
            "timezone": "UTC",
            "attachments": [*v.get("original_attachments", []), *inline_docs],
            "files": [
                item["id"]
                for item in v.get("attachments", [])
                if isinstance(item, dict) and item.get("id") and not item.get("claude_document")
            ],
            "rendering_mode": "messages",
            "tools": [_WEB_SEARCH_TOOL.copy()],
        }
        if parent:
            body["parent_message_uuid"] = parent
        effort = v.get("effort")
        if effort:
            if "selections" in choice.metadata:
                body.update(choice.metadata["selections"][effort])
            else:
                body["thinking_mode"] = effort
        return body

    @staticmethod
    def _presented_file_entries(chunks):
        if not chunks:
            return []
        try:
            inputs = json_loads("".join(chunks))
        except ValueError:
            inputs = {}
        if not isinstance(inputs, dict):
            return []
        return presented_files({"type": "tool_use", "name": "present_files", "input": inputs})

    @staticmethod
    def _normalize_sources(raw_roots):
        if not raw_roots:
            return ()
        if len(raw_roots) <= 64:
            return sources_from(*raw_roots)
        sources = ()
        for index in range(0, len(raw_roots), 64):
            sources = sources_from({"sources": list(sources)}, *raw_roots[index : index + 64])
        return sources

    @staticmethod
    def _normalize_files(raw_roots):
        if not raw_roots:
            return ()
        if len(raw_roots) <= 64:
            return files_from(*raw_roots, rule=CLAUDE_FILE_RULE)
        files = []
        seen: set[str] = set()
        for index in range(0, len(raw_roots), 64):
            files.extend(
                files_from(*raw_roots[index : index + 64], rule=CLAUDE_FILE_RULE, seen=seen)
            )
            if len(seen) >= MAX_FILE_REFERENCES:
                break
        return tuple(files)

    def _select_model(self, v: dict[str, Any]):
        model = v.get("model") or self.catalog.default_model
        choice = next((x for x in self.catalog.models if x.id == model), None)
        if choice is None or (
            v.get("effort") and not any(x.id == v["effort"] for x in choice.efforts)
        ):
            raise ValueError("Select a model/thinking option from this account")
        return model, choice

    def _init_client_state(self, session: Any, transport: Any) -> None:
        self.session = session
        self.http = transport or session
        self._validate_transport_origin(transport)
        self.path, self.organization, self.catalog = self.session.path, None, None
        self.authenticated = False
        self._continuations = ContinuationCache[tuple[str | None, bool]](ttl=300.0)
        self._discovered_at = 0.0

    def _is_catalog_fresh(self, force: bool) -> bool:
        from ...resources.model.collections import MODEL_CATALOG_TTL

        return bool(
            not force
            and self.organization
            and self.catalog is not None
            and time.monotonic() - self._discovered_at < MODEL_CATALOG_TTL
        )

    def _record_discovered(self, organization: str, selected: Any) -> Any:
        catalog = parse_model_catalog(selected)
        self.organization = organization
        self.catalog = catalog
        self.authenticated = True
        self._discovered_at = time.monotonic()
        return catalog

    def _reset_client_state(self) -> None:
        self._continuations.clear()
        self.organization = None
        self.catalog = None
        self.authenticated = False

    def _plan_single_request(
        self, base: str, mode: str, v: dict[str, Any]
    ) -> tuple[str, str, dict[str, Any], str | None]:
        cid = remote_id(v["conversation_id"]) if v.get("conversation_id") else None
        pid = remote_id(v["project_id"]) if v.get("project_id") else None
        if mode == "chats":
            offset, limit = int(v.get("cursor") or 0), int(v.get("limit") or 50)
            if offset < 0 or not 1 <= limit <= 100:
                raise ValueError("Invalid Claude page")
            return "GET", base + f"/chat_conversations?limit={limit}&offset={offset}", {}, cid
        if mode == "history":
            if cid is None:
                raise ValueError("Conversation required")
            return (
                "GET",
                base
                + "/chat_conversations/"
                + cid
                + "?tree=True&rendering_mode=messages&render_all_tools=true",
                {},
                cid,
            )
        if mode == "projects":
            return "GET", base + "/projects", {}, cid
        if mode == "create_project":
            return (
                "POST",
                base + "/projects",
                {
                    "json": {
                        "name": v["name"],
                        "description": "",
                        "prompt_template": v.get("instructions") or "",
                        "is_private": True,
                    }
                },
                cid,
            )
        if mode in ("project_chats", "delete_project"):
            if pid is None:
                raise ValueError("Project required")
            path = base + "/projects/" + pid
            if mode == "project_chats":
                return "GET", path + "/conversations", {}, cid
            return "DELETE", path, {}, cid
        if cid is None:
            raise ValueError("Conversation required")
        path = base + "/chat_conversations/" + cid
        if mode == "continuation":
            return "GET", path, {}, cid
        if mode == "delete_chat":
            self._continuations.remove(cid)
            return "DELETE", path, {}, cid
        if mode == "rename_chat":
            return "PUT", path, {"json": {"name": v["title"]}}, cid
        if mode == "move_chat":
            return (
                "POST",
                base + "/chat_conversations/move_many",
                {"json": {"conversation_uuids": [cid], "project_uuid": pid}},
                cid,
            )
        raise ValueError("Unsupported Claude operation")

    def _finalize_single_request(
        self, mode: str, cid: str | None, v: dict[str, Any], result: Any
    ) -> Any:
        if mode in ("history", "continuation") and cid is not None:
            if mode == "continuation":
                if not isinstance(result, dict):
                    raise ClaudeCompatibilityError("Claude continuation response changed")
                self._finalize_history_continuation(cid, result, v)
            if isinstance(result, dict) and "current_leaf_message_uuid" in result:
                self._continuations.put(
                    cid,
                    (result.get("current_leaf_message_uuid"), result.get("is_temporary") is True),
                )
            return result
        if mode == "rename_chat":
            if isinstance(result, dict) and "name" in result and result.get("name") != v["title"]:
                raise WebAIProviderError("Claude rename readback failed")
            return None
        if mode == "move_chat":
            return None
        return result

    @staticmethod
    def _combine_project_detail(detail: Any, docs: Any, files: Any) -> dict[str, Any]:
        if not isinstance(docs, list) or not isinstance(files, list):
            raise ClaudeCompatibilityError("Claude project resource response changed")
        return dict(detail, docs=docs, files=files)

    @staticmethod
    def _build_create_chat_payload(cid: str, v: dict[str, Any]) -> dict[str, Any]:
        title = v.get("draft_title")
        title = title.strip() if isinstance(title, str) else ""
        payload: dict[str, Any] = {"uuid": cid, "name": title if len(title) <= 200 else ""}
        if v.get("temporary"):
            payload["is_temporary"] = True
        return payload

    @staticmethod
    def _created_chat_state(record):
        if not isinstance(record, dict):
            return None, False
        return record.get("current_leaf_message_uuid"), record.get("is_temporary") is True

    def _lookup_cached_continuation(
        self, cid: str, v: dict[str, Any], timing: SendTiming | None
    ) -> tuple[bool, str | None, bool]:
        if not v.get("action"):
            cached = self._continuations.get(cid)
            if cached is not None:
                cached_parent, cached_temp = cached
                if not v.get("temporary") or cached_temp:
                    if timing is not None:
                        timing.record("pre_send_get_conversation_ms", 0)
                    return True, cached_parent, cached_temp
        return False, None, False

    def _finalize_history_continuation(
        self, cid: str, history: dict[str, Any], v: dict[str, Any]
    ) -> tuple[str | None, dict[str, Any], bool]:
        if v.get("temporary") and history.get("is_temporary") is not True:
            self._continuations.remove(cid)
            raise ClaudeRequestNotSent("Claude temporary conversation could not be verified")
        is_temp = history.get("is_temporary") is True
        parent = history.get("current_leaf_message_uuid")
        if v.get("action"):
            self._continuations.remove(cid)
            parent, v = self._apply_history_action(history, v)
        return parent, v, is_temp

    @staticmethod
    def _collect_pending_uploads(
        request: ProviderRequest,
    ) -> tuple[list[Any], list[tuple[int, dict[str, Any]]]]:
        if len(request.attachments) > CLAUDE_ATTACHMENTS.max_files:
            raise ValueError(f"Claude accepts at most {CLAUDE_ATTACHMENTS.max_files} attachments")
        uploaded = list(request.attachments)
        pending = [
            (index, item)
            for index, item in enumerate(uploaded)
            if isinstance(item, dict) and ("bytes" in item or "path" in item)
        ]
        return uploaded, pending

    @staticmethod
    def _send_event_kwargs(
        request: ProviderRequest, attachments: tuple[Any, ...]
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "prompt": request.prompt,
            "conversation_id": request.conversation_id,
            "draft_title": request.draft_title,
            "project_id": request.project_id,
            "model": request.model,
            "effort": request.effort,
            "temporary": request.temporary,
            "attachments": attachments,
        }
        if request.action:
            kwargs["action"] = request.action
            kwargs["message_id"] = request.message_id
        return kwargs


class BaseClaudeProjectEditor:
    """Shared non-I/O project validation and parsing for Claude project editors."""

    supports_batch_remove = True

    @staticmethod
    def _options(project_id: str | None = None) -> ProjectOptions:
        return ProjectOptions(
            fields=("name", "instructions") if project_id else ("name",),
            sources=(
                {"id": "text", "title": "Text"},
                {"id": "file", "title": "File", "accept": "*/*"},
            )
            if project_id
            else (),
            remove_sources=bool(project_id),
        )

    @staticmethod
    def _update_fields(v: dict[str, Any]) -> dict[str, Any]:
        fields = {"name": v["name"]} if "name" in v else {}
        if "instructions" in v:
            fields["prompt_template"] = v["instructions"]
        return fields

    @staticmethod
    def _update_result(project_id: str, fields: dict[str, Any], result: Any, v: dict[str, Any]):
        current = v.get("current")
        if (
            isinstance(current, ProjectData)
            and isinstance(result, dict)
            and (result.get("uuid") or project_id) == project_id
            and all(key in result and result[key] == value for key, value in fields.items())
        ):
            return replace(current, **{k: v[k] for k in ("name", "instructions") if k in v})
        return result

    @staticmethod
    def _read_metadata_result(project_id: str, current: ProjectData, data: Any) -> ProjectData:
        if not isinstance(data, dict) or (data.get("uuid") or data.get("id")) != project_id:
            raise WebAIProviderError("Claude project response contract changed")
        return replace(
            current,
            name=data.get("name", current.name),
            instructions=data.get("prompt_template", current.instructions),
        )

    @staticmethod
    def _text_source_result(result: Any, name: str):
        if isinstance(result, dict) and (result.get("uuid") or result.get("id")):
            return Attachment(
                str(result.get("uuid") or result.get("id") or ""),
                result.get("file_name") or result.get("name") or name,
                metadata={"native": result},
            )
        return result

    @staticmethod
    def _file_source_result(result: Any, filename: str):
        if (
            isinstance(result, dict)
            and (result.get("uuid") or result.get("id"))
            and result.get("file_uuid")
        ):
            return Attachment(
                str(result.get("uuid") or result.get("id") or ""),
                result.get("file_name") or result.get("name") or filename,
                metadata={"native": result},
            )
        return result

    @staticmethod
    def _native_resource_info(resource: Any) -> tuple[Any, Any] | None:
        native = (
            resource.metadata.get("native")
            if resource is not None and isinstance(resource.metadata, dict)
            else None
        )
        if not isinstance(native, dict):
            return None
        return native.get("uuid") or native.get("id"), native.get("file_uuid")

    @classmethod
    def _resolve_native_remove_call(
        cls, path: str, rid: str, resource: Any
    ) -> tuple[str, str, dict[str, Any]] | None:
        info = cls._native_resource_info(resource)
        if info is None:
            return None
        native_id, file_uuid = info
        if file_uuid and (file_uuid == rid or native_id == rid):
            return "POST", path + "/files/delete_many", {"json": {"file_uuids": [file_uuid]}}
        if (
            native_id == rid
            and file_uuid is None
            and "file_uuid" not in resource.metadata["native"]
        ):
            return "DELETE", path + "/docs/" + quote(rid, safe=""), {}
        return None

    @staticmethod
    def _resolve_remote_remove_call(
        path: str, rid: str, current_remote: dict[str, Any]
    ) -> tuple[str, str, dict[str, Any]]:
        if any((x.get("uuid") or x.get("id")) == rid for x in current_remote["docs"]):
            return "DELETE", path + "/docs/" + quote(rid, safe=""), {}
        file = next(
            (x for x in current_remote["files"] if (x.get("uuid") or x.get("id")) == rid),
            None,
        )
        if file is None or not file.get("file_uuid"):
            raise ValueError("Resource does not belong to this project or its file ID is missing")
        return "POST", path + "/files/delete_many", {"json": {"file_uuids": [file["file_uuid"]]}}

    @classmethod
    def _partition_batch_removals(
        cls, path: str, items: list[dict[str, Any]]
    ) -> tuple[list[str], list[str], list[dict[str, Any]], set[str]]:
        doc_paths: list[str] = []
        file_uuids: list[str] = []
        fallback_items: list[dict[str, Any]] = []
        removed_ids: set[str] = set()
        for entry in items:
            rid = str(entry["resource_id"])
            resource = entry.get("resource")
            info = cls._native_resource_info(resource)
            if info is not None:
                native_id, file_uuid = info
                if file_uuid and (file_uuid == rid or native_id == rid):
                    file_uuids.append(str(file_uuid))
                    removed_ids.add(rid)
                    continue
                if native_id == rid and "file_uuid" not in resource.metadata["native"]:
                    doc_paths.append(path + "/docs/" + quote(rid, safe=""))
                    removed_ids.add(rid)
                    continue
            fallback_items.append(entry)
        return doc_paths, list(dict.fromkeys(file_uuids)), fallback_items, removed_ids

    @staticmethod
    def _removed_project_result(current: Any, removed_ids: set[str], fallback: Any = None) -> Any:
        if isinstance(current, ProjectData):
            return replace(
                current,
                resources=tuple(r for r in current.resources if r.id not in removed_ids),
            )
        return fallback


class BaseClaudeWebAIProvider(BaseWebAIProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = WebAIProviderType.CLAUDE

    _capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
            Capability.HISTORY,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_DELETE,
            Capability.PROJECT_MEMBERSHIP,
            Capability.DELETE,
            Capability.RENAME,
            Capability.ATTACHMENTS,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.WEB_SEARCH,
        }
    )

    @staticmethod
    def _download_path(url: str) -> str | None:
        parsed = urlsplit(url)
        path = unquote(parsed.path)
        direct = (
            parsed.scheme == "https"
            and parsed.netloc == "claude.ai"
            and parsed.path.endswith("/wiggle/download-file")
        )
        if direct:
            path = parse_qs(parsed.query).get("path", [""])[0]
        elif parsed.scheme not in ("", "sandbox", "computer") or not path.startswith(
            "/mnt/user-data/"
        ):
            return None
        if (parsed.netloc and not direct) or any(part in (".", "..") for part in path.split("/")):
            raise ValueError("Unsupported conversation file path")
        return path

    @staticmethod
    def _parse_chats_page(values: Any, cursor: str | None, limit: int) -> Page[Conversation]:
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude conversation list response changed")
        return Page(
            tuple(conversation(x) for x in values),
            str(int(cursor or 0) + limit) if len(values) == limit else None,
        )

    @staticmethod
    def _parse_projects_page(values: Any) -> Page[ProjectData]:
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude project list response changed")
        return Page(tuple(project(x) for x in values))

    @staticmethod
    def _parse_project_chats_page(project_id: str, values: Any) -> Page[Conversation]:
        if not isinstance(values, list):
            raise ClaudeCompatibilityError("Claude project conversation response changed")
        return Page(tuple(conversation(dict(x, project_uuid=project_id)) for x in values))

    @staticmethod
    def _build_download_source(
        endpoint: str, path: str, listing: Any, saved: dict[str, Any]
    ) -> DownloadSource:
        files = listing.get("files_metadata", []) if isinstance(listing, dict) else []
        if not any(isinstance(item, dict) and item.get("path") == path for item in files):
            raise ValueError("File was not found in the conversation record or has expired")
        cookies = saved.get("cookies", saved)
        values = {key: cookies.get(key) for key in ("sessionKey", "lastActiveOrg")}
        if not values["sessionKey"]:
            raise AccountPaused("Claude session could not be verified")
        headers = {
            "Cookie": cookie_header(values, ("sessionKey", "lastActiveOrg")),
            "Referer": "https://claude.ai/",
        }
        return DownloadSource(
            "https://claude.ai"
            + endpoint
            + "/download-file?"
            + urlencode({"path": path}, quote_via=quote),
            path.rsplit("/", 1)[-1],
            True,
            headers,
            NativeDownloadClient,
        )

    def _active_transport(self) -> Any:
        return self.client.http

    def _purge_credentials_unlocked(self) -> SessionStatus:
        delete_session(self.credential_file)
        self.client.authenticated = False
        self.account._clear_verified_session()
        return SessionStatus(SessionState.LOGIN_REQUIRED)

    def _session_status(self) -> SessionStatus:
        saved = session_exists(self.credential_file)
        if self.client.authenticated:
            state = SessionState.AUTHENTICATED
        elif saved:
            state = SessionState.UNKNOWN
        else:
            state = SessionState.LOGIN_REQUIRED
        return SessionStatus(state, saved)

    @staticmethod
    def _validate_chat_title(title: Any) -> str:
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
            raise ValueError("Chat title must contain 1–200 characters")
        return title.strip()

    @staticmethod
    def _validate_project_name(name: Any) -> str:
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
            raise ValueError("Project name must contain 1–200 characters")
        return name.strip()

    def _validate_stream_preconditions(self, request: ProviderRequest) -> None:
        if request.temporary and request.project_id:
            raise FeatureUnavailable(Capability.TEMPORARY, "projects_not_supported")
        if not session_exists(self.credential_file) and not self.client.authenticated:
            raise AccountPaused("Verify the Claude account in the Accounts section")
