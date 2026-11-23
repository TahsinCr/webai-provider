"""Origin-scoped asynchronous Grok web client and workspace editor."""

from __future__ import annotations

import asyncio
import mimetypes
from collections.abc import Awaitable, Callable
from typing import Any

from ...errors import AccountPaused, WebAIProviderError
from ...resources.project.types import ProjectData
from ...transport import AsyncHttpSession
from ...transport.frames import SSEFrameDecoder, aiter_bounded_lines
from .base import (
    _MAX_LINE_BYTES,
    _MAX_STREAM_BYTES,
    ORIGIN,
    BaseGrokClient,
    BaseGrokProjectEditor,
    _b64_ascii,
    remote_id,
)
from .records import conversation

__all__ = ["AsyncGrokClient", "AsyncGrokProjectEditor"]


def _iter_bounded_lines(response):
    return aiter_bounded_lines(
        response.aiter_bytes(),
        max_line_bytes=_MAX_LINE_BYTES,
        max_stream_bytes=_MAX_STREAM_BYTES,
        error_label="Grok stream",
    )


class AsyncGrokClient(BaseGrokClient):
    """Asynchronous Grok HTTP client backed by AsyncHttpSession."""

    def __init__(self, credential_file, *, client_factory=None, transport=None):
        self.session = AsyncHttpSession(
            ORIGIN, credential_file, None, client_factory=client_factory
        )
        self.http = transport or self.session
        self._validate_transport_origin(transport)
        self._auth_lock = asyncio.Lock()

    async def authenticate(self):
        async with self._auth_lock:
            return await self._check_authentication()

    async def _ensure_authenticated(self):
        if not self.session.authenticated:
            async with self._auth_lock:
                if not self.session.authenticated:
                    await self._check_authentication()

    async def _check_authentication(self):
        try:
            status = await self.http.request("GET", "/rest/user-settings")
            verified = self._validate_auth_status(status)
        except AccountPaused:
            self.session.authenticated = False
            raise
        self.session.authenticated = True
        return verified

    async def request(self, method: str, path: str, **options: Any) -> Any:
        await self._ensure_authenticated()
        try:
            return await self.http.request(method, path, **options)
        except AccountPaused:
            self.session.authenticated = False
            raise

    async def upload_file(self, path=None, *, content=None, name=None, mime_type=None):
        filename, resolved_mime, payload_size, encoded = await asyncio.to_thread(
            self._prepare_chat_upload, path, content=content, name=name, mime_type=mime_type
        )
        result = await self.request(
            "POST",
            "/rest/app-chat/upload-file-v2/complete",
            json=self._chat_upload_body(filename, resolved_mime, encoded),
        )
        return self._chat_upload_result(result, filename, resolved_mime, payload_size)

    async def events(self, path: str, body: dict[str, Any]):
        await self._ensure_authenticated()
        try:
            async with self.http.stream(
                "POST",
                path,
                json=body,
                headers={"accept": "application/x-ndjson, text/event-stream"},
                allow_redirects=False,
                timeout=600,
            ) as response:
                decoder = SSEFrameDecoder(max_event_bytes=_MAX_LINE_BYTES, utf8_errors="strict")
                async for line in _iter_bounded_lines(response):
                    if line.startswith(("data:", "event:", "id:", "retry:", ":")) or not line:
                        for frame in decoder.feed_line(line):
                            if frame.data != "[DONE]":
                                yield self._parse_stream_json(frame.data)
                    elif line.strip() != "[DONE]":
                        yield self._parse_stream_json(line)
                for frame in decoder.feed(b"", final=True):
                    if frame.data != "[DONE]":
                        yield self._parse_stream_json(frame.data)
        except AccountPaused:
            self.session.authenticated = False
            raise

    async def close(self):
        try:
            await self.http.close()
        finally:
            if self.http is not self.session:
                await self.session.close()
        self.session.authenticated = False


class AsyncGrokProjectEditor(BaseGrokProjectEditor):
    """Asynchronous Grok workspace mutations."""

    def __init__(self, transport: AsyncGrokClient):
        self.client = transport

    async def options(self, project_id: str | None = None):
        return self._options(project_id)

    async def update(self, project_id: str, **values: Any):
        body = self._update_body(values)
        result = await self.client.request(
            "PUT", "/rest/workspaces/" + remote_id(project_id), json=body
        )
        return self._finalize_update(project_id, body, result, values.get("current"))

    async def read_metadata(self, project_id: str, current: ProjectData) -> ProjectData:
        data = await self.client.request("GET", "/rest/workspaces/" + remote_id(project_id))
        return self._validate_metadata_response(project_id, current, data)

    async def move_chat(
        self,
        conversation_id: str,
        project_id: str | None,
        *,
        source_project_id: str | None,
        fetch_metadata: Callable[[str], Awaitable[Any]],
    ) -> None:
        if source_project_id == project_id:
            return
        conversation_path = "/conversations/" + remote_id(conversation_id)
        if project_id:
            await self.client.request(
                "POST",
                "/rest/workspaces/" + remote_id(project_id) + "/conversations",
                json={"conversationId": conversation_id},
            )
        if source_project_id:
            try:
                await self.client.request(
                    "DELETE",
                    "/rest/workspaces/" + remote_id(source_project_id) + conversation_path,
                )
            except Exception:
                if project_id:
                    try:
                        await self.client.request(
                            "DELETE",
                            "/rest/workspaces/" + remote_id(project_id) + conversation_path,
                        )
                    except Exception:
                        raise WebAIProviderError(
                            "The Grok conversation move did not complete; the conversation may remain in multiple workspaces."
                        ) from None
                raise
        for _ in range(5):
            updated = conversation(await fetch_metadata(conversation_id))
            if (updated.project.id if updated.project is not None else None) == project_id:
                return
            await asyncio.sleep(0.1)
        raise WebAIProviderError(
            "Grok conversation project membership could not be confirmed by the server"
        )

    async def add_source(self, project_id: str, **values: Any):
        if values.get("kind") not in ("text", "file"):
            raise ValueError("Grok supports only text and file resources")
        if values["kind"] == "text":
            name, raw = self._prepare_text_source(values)
            mime_type = "text/plain"
            encoded = await asyncio.to_thread(_b64_ascii, raw)
        else:
            name, mime_type, encoded = await asyncio.to_thread(
                self._prepare_workspace_file,
                values["path"],
                guess_type=mimetypes.guess_type,
            )
        name = self._validate_workspace_name(name, values.get("existing_resources", ()))
        result = await self.client.request(
            "POST",
            "/rest/workspaces/" + remote_id(project_id) + "/files/upload",
            json={"fileName": name, "mimeType": mime_type, "content": encoded},
        )
        return self._source_result(result, name, mime_type)

    async def remove_source(self, project_id: str, **values: Any):
        method, path, kwargs = self._resolve_remove_call(
            "/rest/workspaces/" + remote_id(project_id), values
        )
        return await self.client.request(method, path, **kwargs)
