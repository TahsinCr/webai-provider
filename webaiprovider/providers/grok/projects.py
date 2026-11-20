import asyncio
import mimetypes
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from ...errors import ProviderError
from ...resources.message.attachment import Attachment
from ...resources.project.types import (
    ProjectData,
    ProjectOptions,
    source_file,
)
from .client import _MAX_WORKSPACE_FILE_BYTES, _b64_ascii, _read_limited, remote_id
from .records import conversation


class GrokProjectEditor:
    def __init__(self, transport):
        self.client = transport

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

    async def update(self, project_id, **values):
        project_path = "/rest/workspaces/" + remote_id(project_id)
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
        result = await self.client.request("PUT", project_path, json=body)
        current = values.get("current")
        if (
            isinstance(current, ProjectData)
            and isinstance(result, dict)
            and (result.get("workspaceId") or result.get("id") or project_id) == project_id
            and all(result.get(key) == value for key, value in body.items())
        ):
            return self._apply_metadata(current, result)
        return result

    @staticmethod
    def _apply_metadata(current, data):
        metadata = dict(current.metadata)
        for key, native in (("preferred_model", "preferredModel"), ("kind", "kind")):
            if native in data:
                metadata[key] = data[native]
        return replace(
            current,
            name=data.get("name", current.name),
            instructions=data.get("customPersonality", current.instructions),
            metadata=metadata,
        )

    async def read_metadata(self, project_id, current):
        data = await self.client.request("GET", "/rest/workspaces/" + remote_id(project_id))
        if not isinstance(data, dict) or (data.get("workspaceId") or data.get("id")) != project_id:
            raise ProviderError("Grok workspace response contract changed")
        return self._apply_metadata(current, data)

    async def move_chat(
        self,
        conversation_id: str,
        project_id: str | None,
        *,
        fetch_metadata: Callable[[str], Awaitable[Any]],
    ) -> None:
        metadata = await fetch_metadata(conversation_id)
        chat = conversation(metadata)
        old_project = chat.project.id if chat.project is not None else None
        if old_project == project_id:
            return
        conversation_path = "/conversations/" + remote_id(conversation_id)
        if project_id:
            await self.client.request(
                "POST",
                "/rest/workspaces/" + remote_id(project_id) + "/conversations",
                json={"conversationId": conversation_id},
            )
        if old_project:
            try:
                await self.client.request(
                    "DELETE",
                    "/rest/workspaces/" + remote_id(old_project) + conversation_path,
                )
            except Exception:
                if project_id:
                    try:
                        await self.client.request(
                            "DELETE",
                            "/rest/workspaces/" + remote_id(project_id) + conversation_path,
                        )
                    except Exception:
                        raise ProviderError(
                            "The Grok conversation move did not complete; the conversation may remain in multiple workspaces."
                        ) from None
                raise
        for _ in range(5):
            metadata = await fetch_metadata(conversation_id)
            updated = conversation(metadata)
            updated_project = updated.project.id if updated.project is not None else None
            if updated_project == project_id:
                return
            await asyncio.sleep(0.1)
        raise ProviderError(
            "Grok conversation project membership could not be confirmed by the server"
        )

    async def add_source(self, project_id, **values):
        project_path = "/rest/workspaces/" + remote_id(project_id)
        if values.get("kind") not in ("text", "file"):
            raise ValueError("Grok supports only text and file resources")
        if values["kind"] == "text":
            name, content = values.get("name"), values.get("text")
            if not isinstance(name, str) or not isinstance(content, str) or not content:
                raise ValueError("Resource name and text content are required")
            mime_type = "text/plain"
            raw = content.encode("utf-8")
            if not 0 < len(raw) <= _MAX_WORKSPACE_FILE_BYTES:
                raise ValueError("Grok workspace files must not exceed 20 MiB")
            encoded = await asyncio.to_thread(_b64_ascii, raw)
        else:

            def _prepare_workspace_file():
                file = source_file(
                    values["path"], error="Grok workspace files must not exceed 20 MiB"
                )
                raw_bytes = _read_limited(
                    file,
                    max_bytes=_MAX_WORKSPACE_FILE_BYTES,
                    error="Grok workspace files must not exceed 20 MiB",
                )
                resolved_mime = (
                    "text/markdown"
                    if file.suffix.casefold() in {".md", ".markdown"}
                    else mimetypes.guess_type(file.name)[0] or "text/plain"
                )
                return file.name, resolved_mime, _b64_ascii(raw_bytes)

            name, mime_type, encoded = await asyncio.to_thread(_prepare_workspace_file)
        name = name.strip()
        if not name or len(name) > 255 or any(char in name for char in "/\\\r\n\x00"):
            raise ValueError("Grok resource name is invalid")
        if any(
            (source.metadata.get("path") or source.name).rsplit("/", 1)[-1] == name
            for source in values.get("existing_resources", ())
        ):
            raise ValueError("A Grok project resource with this name already exists")
        result = await self.client.request(
            "POST",
            project_path + "/files/upload",
            json={
                "fileName": name,
                "mimeType": mime_type,
                "content": encoded,
            },
        )
        item = (
            result["file"]
            if isinstance(result, dict) and isinstance(result.get("file"), dict)
            else result
        )
        if isinstance(item, dict) and (item.get("path") or item.get("assetId")):
            return Attachment(
                str(item.get("path") or item.get("assetId")),
                str(item.get("name") or name),
                item.get("mimeType") or mime_type,
                metadata={
                    key: item[key]
                    for key in ("path", "assetId", "size", "createdAt", "modifiedAt")
                    if key in item
                },
            )
        return result

    async def remove_source(self, project_id, **values):
        project_path = "/rest/workspaces/" + remote_id(project_id)
        source = values["resource"]
        if source.id != values["resource_id"]:
            raise ValueError("Grok project resource was not found")
        item = source.metadata
        if item.get("path"):
            return await self.client.request(
                "DELETE",
                project_path + "/files",
                params={"path": item["path"], "recursive": "false"},
            )
        if item.get("assetId"):
            return await self.client.request(
                "DELETE", project_path + "/assets/" + remote_id(item["assetId"])
            )
        raise ValueError("Grok project resource was not found")
