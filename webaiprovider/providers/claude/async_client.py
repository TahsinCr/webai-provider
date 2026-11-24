"""Account-scoped asynchronous Claude HTTPS client, streams, and project editor."""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import AsyncGenerator

from flowcoord import AsyncSingleFlight

from ...accounts import async_read_session
from ...core.concurrency import gather_bounded
from ...core.file_stat import same_file_version
from ...core.timing import current_send_timing
from ...errors import WebAIProviderError
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.message.types import MessageAction
from ...resources.project.types import source_file
from ...transport import AsyncHttpSession
from ...transport.frames import SSEFrameDecoder
from .base import (
    ORIGIN,
    BaseClaudeClient,
    BaseClaudeProjectEditor,
    _ClaudeSSEProcessor,
    _translate_claude_event,
    remote_id,
    validate_upload_candidate,
)
from .errors import ClaudeRequestNotSent, ClaudeStreamError

__all__ = [
    "AsyncClaudeClient",
    "AsyncClaudeProjectEditor",
    "stream_response",
]


class AsyncClaudeClient(BaseClaudeClient):
    """Asynchronous Claude HTTPS client backed by AsyncHttpSession."""

    def __init__(self, credential_file, *, session=None, transport=None):
        self._init_client_state(
            session or AsyncHttpSession(ORIGIN, credential_file, "sessionKey"), transport
        )
        self._discovery = AsyncSingleFlight()

    async def _discover(self):
        organization = self.organization
        if not organization:
            data, values = await asyncio.gather(
                self.http.request("GET", "/api/bootstrap"),
                async_read_session(self.path),
            )
            organization = self._resolve_organization(data, values)
        selected = await self.http.request(
            "GET", "/api/organizations/" + organization + "/model_selector/chat"
        )
        return self._record_discovered(organization, selected)

    async def discover(self, *, force: bool = False):
        timing = current_send_timing()
        started = time.perf_counter_ns() if timing is not None else None
        if self._is_catalog_fresh(force):
            if started is not None:
                timing.record("model_catalog_ms", time.perf_counter_ns() - started)
            return self.catalog
        try:
            return await self._discovery.run("discover", self._discover)
        finally:
            if started is not None:
                timing.record("model_catalog_ms", time.perf_counter_ns() - started)

    async def prepare(self):
        return await self.discover()

    async def base(self) -> str:
        if not self.organization:
            await self.discover()
        return "/api/organizations/" + remote_id(self.organization)

    async def close(self) -> None:
        await self._discovery.aclose()
        self._reset_client_state()
        try:
            await self.http.close()
        finally:
            if self.http is not self.session:
                await self.session.close()

    async def upload_file(self, path=None, *, content=None, name=None, mime_type=None):
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
                raise WebAIProviderError("File changed while it was being uploaded")
        return self._parse_upload_result(result, filename, mime, size)

    async def request(self, mode: str, **v):
        if mode in ("status", "models"):
            data = await self.discover(force=mode == "status" or bool(v.get("force", False)))
            return data if mode == "models" else {"authenticated": True}
        base = await self.base()
        if mode == "project":
            pid = remote_id(v["project_id"]) if v.get("project_id") else None
            if pid is None:
                raise ValueError("Project required")
            path = base + "/projects/" + pid
            detail, docs, files = await asyncio.gather(
                self.http.request("GET", path),
                self.http.request("GET", path + "/docs"),
                self.http.request("GET", path + "/files"),
            )
            return self._combine_project_detail(detail, docs, files)
        method, path, kwargs, cid = self._plan_single_request(base, mode, v)
        result = await self.http.request(method, path, **kwargs)
        return self._finalize_single_request(mode, cid, v, result)

    async def _prepare_stream_conversation(self, cid, path, v, timing):
        if not v.get("conversation_id"):
            created = await self.http.request(
                "POST",
                path.rsplit("/chat_conversations/", 1)[0] + "/chat_conversations",
                json=self._build_create_chat_payload(cid, v),
            )
            parent, is_temp = self._created_chat_state(created)
            if v.get("temporary") and not is_temp:
                try:
                    created = await self.http.request("GET", path)
                except Exception:
                    created = None
                parent, is_temp = self._created_chat_state(created)
                if not is_temp:
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
            return parent, v, is_temp

        hit, cached_parent, cached_temp = self._lookup_cached_continuation(cid, v, timing)
        if hit:
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
        return self._finalize_history_continuation(cid, history, v)

    async def events(self, mode: str, **v):
        if mode != "send":
            raise ValueError("Unsupported stream operation")
        timing = current_send_timing()
        client_started = time.perf_counter_ns() if timing is not None else None
        base = await self.base()
        if client_started is not None:
            timing.record("provider_client_ready_ms", time.perf_counter_ns() - client_started)
        model, choice = self._select_model(v)
        pid = remote_id(v["project_id"]) if v.get("project_id") else None
        creating = not v.get("conversation_id")
        cid = remote_id(v["conversation_id"]) if not creating else str(uuid.uuid4())
        path = base + "/chat_conversations/" + cid
        parent, v, is_temporary = await self._prepare_stream_conversation(cid, path, v, timing)
        yield {"type": "bound", "id": cid}
        if creating and pid:
            await self.request("move_chat", conversation_id=cid, project_id=pid)
        body = self._build_completion_body(model, choice, parent, v)

        sse_started = time.perf_counter_ns() if timing is not None else None
        processor = _ClaudeSSEProcessor(timing=timing, response_opened=None, action=v.get("action"))
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
                processor.response_opened = time.perf_counter_ns() if timing is not None else None
                if sse_started is not None and processor.response_opened is not None:
                    timing.record("sse_connect_ms", processor.response_opened - sse_started)
                decoder = SSEFrameDecoder(max_event_bytes=1_048_576, utf8_errors="strict")
                async for frame in decoder.decode_async(response.aiter_bytes()):
                    for event in processor.feed_raw(frame.data):
                        yield event
        except BaseException:
            if not processor.completed:
                self._continuations.remove(cid)
            raise
        for event in processor.finish(cid, is_temporary, self._continuations):
            yield event


class AsyncClaudeProjectEditor(BaseClaudeProjectEditor):
    """Asynchronous Claude project mutations."""

    def __init__(self, transport: AsyncClaudeClient):
        self.client = transport

    async def options(self, project_id=None):
        return self._options(project_id)

    async def update(self, project_id, **v):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        fields = self._update_fields(v)
        result = await self.client.http.request("PUT", path, json=fields)
        return self._update_result(project_id, fields, result, v)

    async def read_metadata(self, project_id, current):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        data = await self.client.http.request("GET", path)
        return self._read_metadata_result(project_id, current, data)

    async def add_source(self, project_id, **v):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        if v["kind"] == "text":
            result = await self.client.http.request(
                "POST", path + "/docs", json={"file_name": v["name"], "content": v["text"]}
            )
            return self._text_source_result(result, v["name"])
        if v["kind"] != "file":
            raise ValueError("Unsupported source type")
        from curl_cffi import CurlMime

        file = source_file(v["path"], error="Source must be at most 20 MiB")
        mime = CurlMime()
        try:
            mime.addpart(name="file", filename=file.name, local_path=str(file))
            result = await self.client.http.request("POST", path + "/upload", multipart=mime)
        finally:
            mime.close()
        return self._file_source_result(result, file.name)

    async def remove_source(self, project_id, **v):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        rid = str(v["resource_id"])
        planned = self._resolve_native_remove_call(path, rid, v.get("resource"))
        if planned is None:
            current_remote = await self.client.request("project", project_id=project_id)
            planned = self._resolve_remote_remove_call(path, rid, current_remote)
        method, target_path, kwargs = planned
        result = await self.client.http.request(method, target_path, **kwargs)
        return self._removed_project_result(v.get("current"), {rid}, result)

    async def remove_sources(self, project_id, items, *, current=None):
        if not items:
            return current
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        doc_paths, file_uuids, fallback_items, removed_ids = self._partition_batch_removals(
            path, items
        )
        for doc_path in doc_paths:
            await self.client.http.request("DELETE", doc_path)
        if file_uuids:
            await self.client.http.request(
                "POST", path + "/files/delete_many", json={"file_uuids": file_uuids}
            )
        for entry in fallback_items:
            await self.remove_source(project_id, **entry)
            removed_ids.add(str(entry["resource_id"]))
        return self._removed_project_result(current, removed_ids)


async def stream_response(
    client: AsyncClaudeClient, request: ProviderRequest
) -> AsyncGenerator[ProviderStreamEvent, None]:
    uploaded, pending = BaseClaudeClient._collect_pending_uploads(request)
    timing = current_send_timing()
    if timing is not None:
        timing.record("provider_send_gate_wait_ms", 0)

    def validate_uploads() -> None:
        for _, item in pending:
            validate_upload_candidate(
                item.get("path"),
                content=item.get("bytes"),
                name=item.get("name"),
                mime_type=item.get("mime_type"),
            )

    if any(item.get("path") is not None and item.get("bytes") is None for _, item in pending):
        await asyncio.to_thread(validate_uploads)
    else:
        validate_uploads()
    if pending:
        results = await gather_bounded(
            pending,
            lambda entry: client.upload_file(
                entry[1].get("path"),
                content=entry[1].get("bytes"),
                name=entry[1].get("name"),
                mime_type=entry[1].get("mime_type"),
            ),
            limit=2,
        )
        for (index, _), attachment in zip(pending, results, strict=True):
            uploaded[index] = attachment
    completed = False
    async for event in client.events(
        "send", **BaseClaudeClient._send_event_kwargs(request, tuple(uploaded))
    ):
        if event["type"] == "done":
            completed = True
        else:
            translated = _translate_claude_event(event, request)
            if translated is not None:
                yield translated
    if not completed:
        raise ClaudeStreamError("Claude response ended before completion; it was not resent")
