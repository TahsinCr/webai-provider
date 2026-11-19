"""Account-scoped Claude HTTPS operations and completion streams."""

import asyncio
import mimetypes
import os
import stat
import time
import uuid
from pathlib import Path

from ...accounts import read_session_async
from ...core import ConcurrencyCoordinator, MultiDimensionBoundedCache
from ...core.files import same_file_version
from ...core.json import json_loads
from ...core.timing import current_send_timing
from ...errors import AccountPaused, ProviderError
from ...resources.message.attachment import CLAUDE_ATTACHMENTS
from ...resources.message.types import MessageAction
from ...resources.response.artifact import files_from
from ...resources.response.source import sources_from
from ...transport import HttpSession
from ...transport.frames import SSEFrameDecoder
from .catalogs import parse_model_catalog
from .errors import ClaudeCompatibilityError, ClaudeRequestNotSent, ClaudeStreamError
from .records import action_target, presented_files

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


def remote_id(value):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Invalid Claude identity") from None


def organization_from_bootstrap(data, selected):
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
            raise ProviderError("Claude workspace selection is invalid; reverify the account")
    return org["uuid"]


class ClaudeClient:
    def __init__(self, credential_file, *, session=None, transport=None):
        self.session = session or HttpSession("https://claude.ai", credential_file, "sessionKey")
        self.http = transport or self.session
        if transport is not None and self.http.origin != "https://claude.ai":
            raise ValueError("Provider transport origin does not match its account")
        self.path, self.organization, self.catalog = self.session.path, None, None
        self.authenticated = False
        self._coordinator = ConcurrencyCoordinator()
        self._continuations: MultiDimensionBoundedCache[str, tuple[str | None, bool]] = (
            MultiDimensionBoundedCache(max_entries=128, max_bytes=32768, default_ttl=300.0)
        )
        self._discovered_at = 0.0

    async def _discover(self):
        data, values = await asyncio.gather(
            self.http.request("GET", "/api/bootstrap"),
            read_session_async(self.path),
        )
        last_org = values.get("cookies", values).get("lastActiveOrg")
        try:
            organization = organization_from_bootstrap(data, last_org)
        except AccountPaused:
            raise
        except ProviderError:
            organization = organization_from_bootstrap(data, None)
        organization = remote_id(organization)
        selected = await self.http.request(
            "GET", "/api/organizations/" + organization + "/model_selector/chat"
        )
        catalog = parse_model_catalog(selected)
        self.organization = organization
        self.catalog = catalog
        self.authenticated = True
        self._discovered_at = time.monotonic()
        return catalog

    async def discover(self, *, force=False):
        timing = current_send_timing()
        started = time.perf_counter_ns() if timing is not None else None
        if (
            not force
            and self.organization
            and self.catalog is not None
            and time.monotonic() - self._discovered_at < 300
        ):
            if started is not None:
                timing.record("model_catalog_ms", time.perf_counter_ns() - started)
            return self.catalog
        try:
            return await self._coordinator.deduplicate("discover", self._discover)
        finally:
            if started is not None:
                timing.record("model_catalog_ms", time.perf_counter_ns() - started)

    async def prepare(self):
        """Resolve the account workspace and its selectable model catalog once."""
        return await self.discover()

    async def base(self):
        if not self.organization:
            await self.discover()
        return "/api/organizations/" + remote_id(self.organization)

    async def close(self):
        await self._coordinator.aclose()
        self._continuations.clear()
        self.organization = None
        self.catalog = None
        self.authenticated = False
        try:
            await self.http.close()
        finally:
            if self.http is not self.session:
                await self.session.close()

    async def upload_file(self, path=None, *, content=None, name=None, mime_type=None):
        """Upload a local file or byte payload through Claude's web document conversion endpoint."""
        from curl_cffi import CurlMime

        if content is None and path is not None:

            def _prepare_local():
                spec = validate_upload_candidate(
                    path, content=content, name=name, mime_type=mime_type
                )
                local = spec[4]
                return spec, (os.stat(local) if local is not None else None)

            (filename, mime, size, content, local_path), before = await asyncio.to_thread(
                _prepare_local
            )
        else:
            filename, mime, size, content, local_path = validate_upload_candidate(
                path, content=content, name=name, mime_type=mime_type
            )
            before = None
        body = CurlMime()
        try:
            body.addpart(
                name="file",
                filename=filename,
                content_type=mime,
                **({"data": content} if content is not None else {"local_path": local_path}),
            )
            result = await self.http.request(
                "POST", await self.base() + "/convert_document", multipart=body
            )
        finally:
            body.close()
        if local_path is not None and before is not None:
            after = await asyncio.to_thread(os.stat, local_path)
            if not same_file_version(before, after):
                raise ProviderError("File changed while it was being uploaded")
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

    async def request(self, mode, **v):
        if mode in ("status", "models"):
            data = await self.discover(force=mode == "status" or bool(v.get("force", False)))
            return data if mode == "models" else {"authenticated": True}
        base = await self.base()
        cid = remote_id(v["conversation_id"]) if v.get("conversation_id") else None
        pid = remote_id(v["project_id"]) if v.get("project_id") else None
        call = self.http.request
        if mode == "chats":
            offset, limit = int(v.get("cursor") or 0), int(v.get("limit") or 50)
            if offset < 0 or not 1 <= limit <= 100:
                raise ValueError("Invalid Claude page")
            return await call("GET", base + f"/chat_conversations?limit={limit}&offset={offset}")
        if mode == "history":
            if cid is None:
                raise ValueError("Conversation required")
            history = await call(
                "GET",
                base
                + "/chat_conversations/"
                + cid
                + "?tree=True&rendering_mode=messages&render_all_tools=true",
            )
            if isinstance(history, dict) and "current_leaf_message_uuid" in history:
                self._continuations.put(
                    cid,
                    (history.get("current_leaf_message_uuid"), history.get("is_temporary") is True),
                )
            return history
        if mode == "projects":
            return await call("GET", base + "/projects")
        if mode == "create_project":
            return await call(
                "POST",
                base + "/projects",
                json={
                    "name": v["name"],
                    "description": "",
                    "prompt_template": v.get("instructions") or "",
                    "is_private": True,
                },
            )
        if mode in ("project", "project_chats", "delete_project"):
            if pid is None:
                raise ValueError("Project required")
            path = base + "/projects/" + pid
            if mode == "project_chats":
                return await call("GET", path + "/conversations")
            if mode == "delete_project":
                return await call("DELETE", path)
            detail, docs, files = await asyncio.gather(
                call("GET", path),
                call("GET", path + "/docs"),
                call("GET", path + "/files"),
            )
            if not isinstance(docs, list) or not isinstance(files, list):
                raise ClaudeCompatibilityError("Claude project resource response changed")
            return dict(detail, docs=docs, files=files)
        if cid is None:
            raise ValueError("Conversation required")
        path = base + "/chat_conversations/" + cid
        if mode == "delete_chat":
            self._continuations.remove(cid)
            return await call("DELETE", path)
        if mode == "rename_chat":
            updated = await call("PUT", path, json={"name": v["title"]})
            if (
                isinstance(updated, dict)
                and "name" in updated
                and updated.get("name") != v["title"]
            ):
                raise ProviderError("Claude rename readback failed")
            return None
        if mode == "move_chat":
            await call(
                "POST",
                base + "/chat_conversations/move_many",
                json={"conversation_uuids": [cid], "project_uuid": pid},
            )
            return None
        raise ValueError("Unsupported Claude operation")

    async def _prepare_stream_conversation(self, cid, pid, path, v, timing):
        if not v.get("conversation_id"):
            create_payload = {"uuid": cid, "name": ""}
            if v.get("temporary"):
                create_payload["is_temporary"] = True
            created = await self.http.request(
                "POST",
                path.rsplit("/chat_conversations/", 1)[0] + "/chat_conversations",
                json=create_payload,
            )
            if v.get("temporary") and (
                not isinstance(created, dict) or created.get("is_temporary") is not True
            ):
                try:
                    created = await self.http.request("GET", path)
                except Exception:
                    created = None
                if not isinstance(created, dict) or created.get("is_temporary") is not True:
                    try:
                        await self.request("delete_chat", conversation_id=cid)
                    except Exception:
                        raise ClaudeRequestNotSent(
                            "Claude could not verify the temporary conversation; the message was not sent, and "
                            "cleanup of the empty conversation could not be confirmed"
                        ) from None
                    raise ClaudeRequestNotSent(
                        "Claude temporary conversation could not be verified"
                    )
            parent = created.get("current_leaf_message_uuid") if isinstance(created, dict) else None
            is_temp = bool(isinstance(created, dict) and created.get("is_temporary") is True)
            return parent, v, is_temp

        if not v.get("action"):
            cached = self._continuations.get(cid)
            if cached is not None:
                cached_parent, cached_temp = cached
                if not v.get("temporary") or cached_temp:
                    if timing is not None:
                        timing.record("pre_send_get_conversation_ms", 0)
                    return cached_parent, v, cached_temp

        history_started = time.perf_counter_ns() if timing is not None else None
        try:
            history = (
                await self.request("history", conversation_id=cid)
                if v.get("action")
                else await self.http.request("GET", path)
            )
        finally:
            if history_started is not None:
                timing.record(
                    "pre_send_get_conversation_ms",
                    time.perf_counter_ns() - history_started,
                )
        if v.get("temporary") and history.get("is_temporary") is not True:
            self._continuations.remove(cid)
            raise ClaudeRequestNotSent("Claude temporary conversation could not be verified")
        is_temp = history.get("is_temporary") is True
        parent = history.get("current_leaf_message_uuid")
        if v.get("action"):
            self._continuations.remove(cid)
            target = action_target(history, v["message_id"], v["action"])
            if v["action"].value not in target.actions:
                raise ProviderError("This Claude message action is unavailable")
            original = target.metadata["native_message"]
            parent = original.get("parent_message_uuid")
            v = dict(
                v,
                attachments=[
                    dict(item, id=item.get("uuid") or item.get("id"))
                    for item in original.get("files", [])
                ],
                original_attachments=original.get("attachments", []),
            )
            if v["action"] == MessageAction.RETRY:
                parent = original["uuid"]
        return parent, v, is_temp

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
            return files_from(*raw_roots)
        files = ()
        for index in range(0, len(raw_roots), 64):
            files = files_from({"files": list(files)}, *raw_roots[index : index + 64])
        return files

    async def events(self, mode, **v):
        if mode != "send":
            raise ValueError("Unsupported stream operation")
        timing = current_send_timing()
        client_started = time.perf_counter_ns() if timing is not None else None
        base = await self.base()
        if client_started is not None:
            timing.record("provider_client_ready_ms", time.perf_counter_ns() - client_started)
        model = v.get("model") or self.catalog.default_model
        choice = next((x for x in self.catalog.models if x.id == model), None)
        if choice is None or (
            v.get("effort") and not any(x.id == v["effort"] for x in choice.efforts)
        ):
            raise ValueError("Select a model/thinking option from this account")
        pid = remote_id(v["project_id"]) if v.get("project_id") else None
        creating = not v.get("conversation_id")
        cid = remote_id(v["conversation_id"]) if not creating else str(uuid.uuid4())
        path = base + "/chat_conversations/" + cid
        parent, v, is_temporary = await self._prepare_stream_conversation(cid, pid, path, v, timing)
        yield {"type": "bound", "id": cid}
        if creating and pid:
            await self.request("move_chat", conversation_id=cid, project_id=pid)
        body = self._build_completion_body(model, choice, parent, v)

        completed, completion_notified, blocks = False, False, {}
        response_id, parent_id, final_index = None, None, 0
        response_model = None
        presented_chunks: dict[int, tuple[list[str], int]] = {}
        raw_source_roots: list[dict] = []
        raw_file_roots: list[dict] = []
        first_event = False
        first_assistant_text = False
        last_event_at = None
        sse_started = time.perf_counter_ns() if timing is not None else None
        response_opened = None

        def record_first_assistant_text():
            nonlocal first_assistant_text
            if timing is None or first_assistant_text:
                return
            first_assistant_text = True
            now = time.perf_counter_ns()
            if response_opened is not None:
                timing.record("sse_first_assistant_text_ms", now - response_opened)
            timing.record("button_to_first_assistant_text_ms", now - timing.started_ns)

        suffix = "/retry_completion" if v.get("action") == MessageAction.RETRY else "/completion"
        try:
            async with self.http.stream(
                "POST",
                path + suffix,
                json=body,
                headers={"accept": "text/event-stream"},
                allow_redirects=False,
                timeout=600,
            ) as response:
                response_opened = time.perf_counter_ns() if timing is not None else None
                if sse_started is not None:
                    timing.record("sse_connect_ms", response_opened - sse_started)
                decoder = SSEFrameDecoder(max_event_bytes=1_048_576, utf8_errors="strict")
                async for frame in decoder.decode_async(response.aiter_bytes()):
                    raw = frame.data
                    if raw == "[DONE]":
                        continue
                    try:
                        event = json_loads(raw)
                    except ValueError as error:
                        raise ClaudeCompatibilityError(
                            "Claude returned an invalid SSE event"
                        ) from error
                    if not isinstance(event, dict):
                        raise ClaudeCompatibilityError("Claude returned an invalid SSE event")
                    event_at = time.perf_counter_ns() if timing is not None else None
                    if timing is not None:
                        last_event_at = event_at
                        if not first_event:
                            first_event = True
                            timing.record("sse_first_event_ms", event_at - response_opened)
                            timing.record(
                                "button_to_first_sse_event_ms", event_at - timing.started_ns
                            )
                    kind, delta = event.get("type"), event.get("delta", {})
                    if kind == "message_start":
                        native = event.get("message") or {}
                        response_id = native.get("uuid") or native.get("id")
                        parent_id = native.get("parent_uuid")
                        response_model = native.get("model")
                    if kind == "error" or event.get("error"):
                        raise ClaudeStreamError("Claude stream failed; the request was not retried")
                    if kind == "content_block_start":
                        block = event.get("content_block", {})
                        block_type = block.get("type")
                        block_index = event.get("index", 0)
                        if block:
                            raw_file_roots.append(block)
                        if block_type == "tool_use" and block.get("name") == "present_files":
                            presented = presented_files(block)
                            if presented:
                                raw_file_roots.append({"files": presented})
                            presented_chunks[block_index] = ([], 0)
                        blocks[block_index] = block_type
                        if block_type == "web_search_tool_result":
                            results = block.get("content")
                            if isinstance(results, list) and results:
                                raw_source_roots.append({"sources": results})
                        elif block_type == "text":
                            if block:
                                raw_source_roots.append(block)
                        if block_type == "text" and block.get("text"):
                            final_index = block_index
                            if block["text"].strip():
                                record_first_assistant_text()
                            yield {"type": "delta", "text": block["text"]}
                        elif block_type == "thinking" and block.get("thinking"):
                            yield {
                                "type": "delta",
                                "channel": "analysis",
                                "text": block["thinking"],
                            }
                    if kind == "content_block_delta":
                        index = event.get("index", 0)
                        delta_type = delta.get("type")
                        if delta_type == "input_json_delta" and index in presented_chunks:
                            piece = delta.get("partial_json") or ""
                            chunks_list, total_len = presented_chunks[index]
                            new_len = total_len + len(piece)
                            if new_len <= 65536:
                                chunks_list.append(piece)
                                presented_chunks[index] = (chunks_list, new_len)
                            else:
                                presented_chunks.pop(index)
                        if delta_type == "citations_delta":
                            citation = delta.get("citation")
                            if citation is not None:
                                if (
                                    raw_source_roots
                                    and len(raw_source_roots[-1]) == 1
                                    and "citations" in raw_source_roots[-1]
                                    and len(raw_source_roots[-1]["citations"]) < 128
                                ):
                                    raw_source_roots[-1]["citations"].append(citation)
                                else:
                                    raw_source_roots.append({"citations": [citation]})
                        channel = blocks.get(index)
                        if delta_type == "thinking_delta" and channel == "thinking":
                            yield {
                                "type": "delta",
                                "channel": "analysis",
                                "text": delta.get("thinking", ""),
                            }
                        elif delta_type == "text_delta" and channel == "text":
                            text = delta.get("text", "")
                            if text:
                                final_index = index
                            if text.strip():
                                record_first_assistant_text()
                            yield {"type": "delta", "text": text}
                    if kind == "content_block_stop":
                        entry = presented_chunks.pop(event.get("index", 0), None)
                        if entry is not None:
                            presented = self._presented_file_entries(entry[0])
                            if presented:
                                raw_file_roots.append({"files": presented})
                    elif "completion" in event:
                        text = event.get("completion") or ""
                        if text.strip():
                            record_first_assistant_text()
                        yield {"type": "delta", "text": text}
                        completed = bool(event.get("stop_reason"))
                    if kind == "message_stop" or (
                        kind == "message_delta" and delta.get("stop_reason")
                    ):
                        completed = True
                    if completed and not completion_notified:
                        yield {"type": "response_complete"}
                        completion_notified = True
        except BaseException:
            if not completed:
                self._continuations.remove(cid)
            raise
        if last_event_at is not None and response_opened is not None:
            timing.record("sse_last_event_ms", last_event_at - response_opened)
            timing.record("button_to_last_sse_event_ms", last_event_at - timing.started_ns)
        if not completed:
            self._continuations.remove(cid)
            raise ClaudeStreamError("Claude response ended before completion; it was not resent")
        if response_id:
            self._continuations.put(cid, (response_id, is_temporary))
            sources = self._normalize_sources(raw_source_roots)
            files = self._normalize_files(raw_file_roots)
            yield {
                "type": "metadata",
                "message_id": response_id + ":" + str(final_index),
                "user_message_id": parent_id + ":0"
                if isinstance(parent_id, str) and v.get("action") != MessageAction.RETRY
                else None,
                "sources": sources,
                "files": files,
                "model": response_model,
                "actions": ["retry"],
            }
        yield {"type": "done"}
