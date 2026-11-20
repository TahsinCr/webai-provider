"""Origin-scoped Grok web transport and workspace editor."""

import asyncio
import base64
import os
import stat
from urllib.parse import quote

from ...core.json import json_loads
from ...errors import AccountPaused, ProviderError
from ...transport import AsyncHttpSession
from ...transport.frames import SSEFrameDecoder, aiter_bounded_lines
from .base import validate_upload_candidate

ORIGIN = "https://grok.com"
_MAX_LINE_BYTES = 1_048_576
_MAX_STREAM_BYTES = 64 * 1024 * 1024
_MAX_CHAT_FILE_BYTES = 3 * 1024 * 1024
_MAX_WORKSPACE_FILE_BYTES = 20 * 1024 * 1024


def remote_id(value):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError("Grok record ID is invalid")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Grok record ID is invalid")
    return quote(value, safe="")


def _read_limited(
    path,
    *,
    max_bytes=_MAX_WORKSPACE_FILE_BYTES,
    error="Grok file is empty or exceeds the size limit",
):
    with path.open("rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= max_bytes:
            raise ValueError(error)
        data = stream.read(info.st_size + 1)
        if len(data) > info.st_size:
            data += stream.read(max_bytes + 1 - len(data))
    if not 0 < len(data) <= max_bytes:
        raise ValueError(error)
    return data


def _b64_ascii(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _iter_bounded_lines(response):
    return aiter_bounded_lines(
        response.aiter_bytes(),
        max_line_bytes=_MAX_LINE_BYTES,
        max_stream_bytes=_MAX_STREAM_BYTES,
        error_label="Grok stream",
    )


class GrokClient:
    def __init__(self, credential_file, *, client_factory=None, transport=None):
        self.session = AsyncHttpSession(
            ORIGIN, credential_file, None, client_factory=client_factory
        )
        self.http = transport or self.session
        if transport is not None and self.http.origin != "https://grok.com":
            raise ValueError("Provider transport origin does not match its account")
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
        except AccountPaused:
            self.session.authenticated = False
            raise
        if not isinstance(status, dict):
            self.session.authenticated = False
            raise AccountPaused(
                "The Grok session could not be verified; verify it again in the Accounts section."
            )
        self.session.authenticated = True
        return status

    async def request(self, method, path, **options):
        await self._ensure_authenticated()
        try:
            return await self.http.request(method, path, **options)
        except AccountPaused:
            self.session.authenticated = False
            raise

    async def upload_file(self, path=None, *, content=None, name=None, mime_type=None):
        def _prepare():
            filename, resolved_mime, _, file = validate_upload_candidate(
                path, content=content, name=name, mime_type=mime_type
            )
            payload = (
                content
                if content is not None
                else _read_limited(
                    file,
                    max_bytes=_MAX_CHAT_FILE_BYTES,
                    error="Grok chat file is empty or exceeds the 3 MiB limit",
                )
            )
            return filename, resolved_mime, len(payload), _b64_ascii(payload)

        filename, resolved_mime, payload_size, encoded = await asyncio.to_thread(_prepare)
        result = await self.request(
            "POST",
            "/rest/app-chat/upload-file-v2/complete",
            json={
                "directUpload": {
                    "fileData": encoded,
                    "fileName": filename,
                    "fileMimeType": resolved_mime,
                    "fileSource": "SELF_UPLOAD_FILE_SOURCE",
                }
            },
        )
        metadata = result.get("fileMetadata") if isinstance(result, dict) else None
        asset_id = (
            (metadata.get("assetId") or metadata.get("fileMetadataId"))
            if isinstance(metadata, dict)
            else None
        )
        if (
            not isinstance(result, dict)
            or not isinstance(metadata, dict)
            or result.get("terminalError")
            or not isinstance(asset_id, str)
            or not asset_id
        ):
            raise ProviderError("Grok chat file upload failed")
        return {
            "id": asset_id,
            "name": metadata.get("name") or metadata.get("fileName") or filename,
            "mime_type": metadata.get("mimeType") or metadata.get("fileMimeType") or resolved_mime,
            "size_bytes": payload_size,
            "grok_file": metadata,
        }

    async def events(self, path, body):
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

                def parse(raw):
                    try:
                        event = json_loads(raw)
                    except ValueError:
                        raise ProviderError("Grok stream response format has changed") from None
                    if not isinstance(event, dict):
                        raise ProviderError("Grok stream response format has changed")
                    return event

                async for line in _iter_bounded_lines(response):
                    if line.startswith(("data:", "event:", "id:", "retry:", ":")) or not line:
                        for frame in decoder.feed_line(line):
                            if frame.data != "[DONE]":
                                yield parse(frame.data)
                    elif line.strip() != "[DONE]":
                        yield parse(line)
                for frame in decoder.feed(b"", final=True):
                    if frame.data != "[DONE]":
                        yield parse(frame.data)
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
