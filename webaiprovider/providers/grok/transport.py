"""Origin-scoped Grok web transport and workspace editor."""

import asyncio
import base64
import json
import mimetypes
from urllib.parse import quote

from ...errors import AccountPaused, ProviderError
from ...models import ProjectOptions, source_file
from ...transport import WebSession
from ...utils.attachments import GROK_ATTACHMENTS

ORIGIN = "https://grok.com"
_MAX_LINE_BYTES = 1_048_576
_MAX_STREAM_BYTES = 64 * 1024 * 1024
_MAX_WORKSPACE_FILE_BYTES = 3 * 1024 * 1024


def remote_id(value):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError("Grok record ID is invalid")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Grok record ID is invalid")
    return quote(value, safe="")


def _read_limited(path):
    with path.open("rb") as stream:
        return stream.read(_MAX_WORKSPACE_FILE_BYTES + 1)


async def _iter_bounded_lines(response):
    buffer = bytearray()
    total = 0
    async for chunk in response.aiter_content():
        total += len(chunk)
        if total > _MAX_STREAM_BYTES:
            raise ProviderError("Grok stream response exceeds the size limit")
        start = 0
        while (end := chunk.find(b"\n", start)) >= 0:
            if len(buffer) + end - start > _MAX_LINE_BYTES:
                raise ProviderError("Grok stream response exceeds the size limit")
            buffer.extend(chunk[start:end])
            line = bytes(buffer)
            buffer.clear()
            if line.endswith(b"\r"):
                line = line[:-1]
            try:
                yield line.decode("utf-8")
            except UnicodeDecodeError:
                raise ProviderError("Grok stream response format has changed") from None
            start = end + 1
        if len(buffer) + len(chunk) - start > _MAX_LINE_BYTES:
            raise ProviderError("Grok stream response exceeds the size limit")
        buffer.extend(chunk[start:])
    if buffer:
        try:
            yield bytes(buffer).decode("utf-8")
        except UnicodeDecodeError:
            raise ProviderError("Grok stream response format has changed") from None


class GrokTransport:
    def __init__(self, credential_file, *, client_factory=None):
        self.session = WebSession(ORIGIN, credential_file, None, client_factory=client_factory)
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
            status = await self.session.request("GET", "/rest/user-settings")
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
            return await self.session.request(method, path, **options)
        except AccountPaused:
            self.session.authenticated = False
            raise

    async def upload_file(self, path):
        file = source_file(path, error="Grok chat dosyası en fazla 3 MiB olabilir")
        if file.suffix.lower() not in GROK_ATTACHMENTS.allowed_extensions:
            raise ValueError("Grok chat file type is not supported")
        if not 0 < file.stat().st_size <= GROK_ATTACHMENTS.max_bytes:
            raise ValueError("Grok chat file is empty or exceeds the 3 MiB limit")
        content = await asyncio.to_thread(_read_limited, file)
        if not content or len(content) > GROK_ATTACHMENTS.max_bytes:
            raise ValueError("Grok chat file is empty or exceeds the 3 MiB limit")

        mime_type = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
        result = await self.request(
            "POST",
            "/rest/app-chat/upload-file-v2/complete",
            json={
                "directUpload": {
                    "fileData": base64.b64encode(content).decode("ascii"),
                    "fileName": file.name,
                    "fileMimeType": mime_type,
                    "fileSource": "SELF_UPLOAD_FILE_SOURCE",
                }
            },
        )
        metadata = result.get("fileMetadata") if isinstance(result, dict) else None
        asset_id = metadata.get("assetId") if isinstance(metadata, dict) else None
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
            "name": metadata.get("name") or file.name,
            "mime_type": metadata.get("mimeType") or mime_type,
            "size_bytes": len(content),
            "grok_file": metadata,
        }

    async def events(self, path, body):
        await self._ensure_authenticated()
        try:
            client = self.session.connect()
            async with client.stream(
                "POST",
                self.session.url(path),
                json=body,
                headers={"accept": "application/x-ndjson, text/event-stream"},
                allow_redirects=False,
                timeout=600,
            ) as response:
                self.session.check(response)
                payload = []
                async for line in _iter_bounded_lines(response):
                    line = line.strip()
                    if not line:
                        if payload:
                            raw = "\n".join(payload)
                            payload.clear()
                            if raw == "[DONE]":
                                continue
                            try:
                                event = json.loads(raw)
                            except ValueError:
                                raise ProviderError(
                                    "Grok stream response format has changed"
                                ) from None
                            if not isinstance(event, dict):
                                raise ProviderError("Grok stream response format has changed")
                            yield event
                        continue
                    if line.startswith(":"):
                        continue
                    if line == "[DONE]":
                        continue
                    if line.startswith("data:"):
                        line = line[5:].lstrip()
                        if not line:
                            continue
                        if line == "[DONE]":
                            continue
                        payload.append(line)
                        continue
                    if line.startswith(("event:", "id:", "retry:")):
                        continue
                    try:
                        event = json.loads(line)
                    except ValueError:
                        raise ProviderError("Grok stream response format has changed") from None
                    if not isinstance(event, dict):
                        raise ProviderError("Grok stream response format has changed")
                    yield event
                if payload:
                    raw = "\n".join(payload)
                    if raw != "[DONE]":
                        try:
                            event = json.loads(raw)
                        except ValueError:
                            raise ProviderError("Grok stream response format has changed") from None
                        if not isinstance(event, dict):
                            raise ProviderError("Grok stream response format has changed")
                        yield event
        except AccountPaused:
            self.session.authenticated = False
            raise
        finally:
            await self.session.persist()

    async def close(self):
        await self.session.close()
        self.session.authenticated = False


class GrokProjectEditor:
    def __init__(self, transport):
        self.transport = transport

    async def options(self, project_id=None):
        return ProjectOptions(
            fields=("name", "instructions"),
            sources=(
                {"id": "text", "title": "Metin"},
                {"id": "file", "title": "Dosya", "accept": "*/*"},
            )
            if project_id
            else (),
            remove_sources=bool(project_id),
        )

    async def write(self, operation, project_id=None, **values):
        project_path = "/rest/workspaces/" + remote_id(project_id)
        if operation == "update":
            body = {}
            if "name" in values:
                name = values["name"]
                if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
                    raise ValueError("Project name must be between 1 and 200 characters")
                body["name"] = name.strip()
            if "instructions" in values:
                if not isinstance(values["instructions"], str):
                    raise ValueError("Project instructions must be text")
                body["customPersonality"] = values["instructions"]
            if not body:
                raise ValueError("A Grok project change is required")
            return await self.transport.request("PUT", project_path, json=body)
        if operation == "add_source":
            if values.get("kind") not in ("text", "file"):
                raise ValueError("Grok supports only text and file resources")
            if values["kind"] == "text":
                name, content = values.get("name"), values.get("text")
                if not isinstance(name, str) or not isinstance(content, str) or not content:
                    raise ValueError("Resource name and text content are required")
                mime_type = "text/plain"
                content = content.encode("utf-8")
            else:
                file = source_file(
                    values["path"], error="Grok workspace files must not exceed 3 MiB"
                )
                if file.stat().st_size > _MAX_WORKSPACE_FILE_BYTES:
                    raise ValueError("Grok workspace files must not exceed 3 MiB")
                content = await asyncio.to_thread(_read_limited, file)
                name = file.name
                mime_type = (
                    "text/markdown"
                    if file.suffix.casefold() in {".md", ".markdown"}
                    else mimetypes.guess_type(file.name)[0] or "text/plain"
                )
            if not 0 < len(content) <= _MAX_WORKSPACE_FILE_BYTES:
                raise ValueError("Grok workspace files must not exceed 3 MiB")
            name = name.strip()
            if not name or len(name) > 255 or any(char in name for char in "/\\\r\n\x00"):
                raise ValueError("Grok resource name is invalid")
            if any(
                (source.metadata.get("path") or source.name).rsplit("/", 1)[-1] == name
                for source in values.get("existing_resources", ())
            ):
                raise ValueError("A Grok project resource with this name already exists")
            return await self.transport.request(
                "POST",
                project_path + "/files/upload",
                json={
                    "fileName": name,
                    "mimeType": mime_type,
                    "content": base64.b64encode(content).decode("ascii"),
                },
            )
        if operation == "remove_source":
            source = values["resource"]
            if source.id != values["resource_id"]:
                raise ValueError("Grok project resource was not found")
            item = source.metadata
            if item.get("path"):
                return await self.transport.request(
                    "DELETE",
                    project_path + "/files",
                    params={"path": item["path"], "recursive": "false"},
                )
            if item.get("assetId"):
                return await self.transport.request(
                    "DELETE", project_path + "/assets/" + remote_id(item["assetId"])
                )
            raise ValueError("Grok project resource was not found")
        raise ValueError("Grok project operation is not supported")
