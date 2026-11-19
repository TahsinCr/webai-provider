"""ChatGPT project controls through the native async HTTP client."""

from __future__ import annotations

import asyncio
import mimetypes
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from urllib.parse import quote

from ...core.concurrency import maybe_await
from ...resources.message.attachment import Attachment
from ...resources.project.types import (
    ProjectData,
    ProjectOption,
    ProjectOptions,
    source_file,
)


class ChatGPTProjectEditor:
    def __init__(self, session, factory, *, client_scope=None):
        self.session, self.factory = session, factory
        self.client_scope = client_scope

    @asynccontextmanager
    async def _use_client(self):
        if self.client_scope is not None:
            client = await maybe_await(self.client_scope())
            yield client
            return
        client = self.factory(self.session)
        try:
            yield client
        finally:
            aclose = getattr(client, "close", None)
            if callable(aclose):
                await maybe_await(aclose())

    async def options(self, project_id=None):
        if not project_id:
            return ProjectOptions(
                fields=("name",),
                visibility=(ProjectOption("private", "Özel"),),
                memory=(
                    ProjectOption("default", "Varsayılan"),
                    ProjectOption("project_only", "Yalnızca proje"),
                ),
            )
        async with self._use_client() as client:
            raw = (await maybe_await(client.get_project(project_id)))["metadata"]
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
                    "title": "Dosya",
                    "accept": ",".join(accepted) if accepted else "*/*",
                },
                {"id": "text", "title": "Metin"},
            ),
            remove_sources=True,
        )

    async def update(self, project_id, **values):
        async with self._use_client() as client:
            path = "https://chatgpt.com/backend-api/projects/" + quote(project_id, safe="")
            provided = values.get("current")
            if (
                provided is not None
                and isinstance(getattr(provided, "metadata", None), dict)
                and provided.metadata
            ):
                native = dict(provided.metadata)
                current_name = provided.name
                current_instructions = provided.instructions
            else:
                fetched = await maybe_await(client.get_project(project_id))
                native = fetched["metadata"]
                current_name = fetched["name"]
                current_instructions = fetched["instructions"]
            for _ in range(3):
                if not isinstance(native.get("gizmo"), dict):
                    break
                native = native["gizmo"]
            display = native.get("display") or {}
            await maybe_await(
                client.request(
                    "PATCH",
                    path,
                    expect_json=False,
                    json={
                        "name": values.get("name", current_name),
                        "instructions": values.get("instructions", current_instructions),
                        "emoji": display.get("emoji"),
                        "theme": display.get("theme"),
                        "memory_scope": native.get("memory_scope"),
                    },
                )
            )
            if isinstance(provided, ProjectData):
                return replace(
                    provided,
                    name=values.get("name", current_name),
                    instructions=values.get("instructions", current_instructions),
                )

    supports_batch_add = True

    @staticmethod
    async def _upload_source(client, project_id, values):
        last_modified = int(time.time() * 1000)
        if values["kind"] == "text":
            name, data, mime = values["name"], values["text"].encode(), "text/plain"
            if not name.endswith(".txt"):
                name += ".txt"
            size = len(data)
        elif values["kind"] == "file":

            def _prepare_file():
                file = source_file(values["path"])
                metadata = file.stat()
                return (
                    file.name,
                    file,
                    metadata.st_size,
                    metadata.st_mtime_ns // 1_000_000,
                    mimetypes.guess_type(file.name)[0] or "application/octet-stream",
                )

            name, data, size, last_modified, mime = await asyncio.to_thread(_prepare_file)
        else:
            raise ValueError("Unsupported project source type")
        uploaded = await maybe_await(client.upload_file(name, mime, data, project_id=project_id))
        file_payload = {
            "file_id": uploaded["id"],
            "last_modified": last_modified,
            "name": name,
            "size": uploaded.get("size_bytes", size),
            "type": mime,
            "location": "fs",
        }
        return uploaded, name, mime, file_payload

    @staticmethod
    def _build_attached(attached_item, uploaded, name, mime, file_payload):
        if attached_item is None:
            attached_item = {
                "file_id": uploaded["id"],
                "id": uploaded["id"],
                "name": name,
                "type": mime,
                **file_payload,
            }
        return Attachment(
            str(attached_item.get("file_id") or attached_item.get("id") or uploaded["id"]),
            str(attached_item.get("name") or name),
            attached_item.get("type") or mime,
            metadata=dict(attached_item),
        )

    async def add_source(self, project_id, **values):
        (created,) = await self.add_sources(project_id, [values])
        return created

    async def add_sources(self, project_id, items):
        if not items:
            return []
        async with self._use_client() as client:
            path = "https://chatgpt.com/backend-api/projects/" + quote(project_id, safe="")
            slots = asyncio.Semaphore(2)

            async def upload_one(values):
                async with slots:
                    return await self._upload_source(client, project_id, values)

            uploaded_entries = await asyncio.gather(*(upload_one(item) for item in items))
            attached = await maybe_await(
                client.request(
                    "POST",
                    path + "/files",
                    json={"files": [entry[3] for entry in uploaded_entries]},
                )
            )
            if callable(getattr(attached, "json", None)) and getattr(attached, "content", None):
                attached = attached.json()
            attached_files = (
                attached["files"]
                if isinstance(attached, dict) and isinstance(attached.get("files"), list)
                else [attached]
                if isinstance(attached, dict) and (attached.get("file_id") or attached.get("id"))
                else []
            )
            by_id = {
                str(item.get("file_id") or item.get("id")): item
                for item in attached_files
                if isinstance(item, dict) and (item.get("file_id") or item.get("id"))
            }
            results = []
            for idx, (uploaded, name, mime, file_payload) in enumerate(uploaded_entries):
                fid = str(uploaded["id"])
                attached_item = by_id.get(fid)
                if attached_item is None and idx < len(attached_files):
                    candidate = attached_files[idx]
                    if isinstance(candidate, dict):
                        attached_item = candidate
                results.append(
                    self._build_attached(attached_item, uploaded, name, mime, file_payload)
                )
            return results

    async def remove_sources(self, project_id, items, *, current=None):
        updated = current
        for entry in items:
            result = await self.remove_source(project_id, current=updated, **entry)
            if isinstance(result, ProjectData):
                updated = result
        return updated

    async def remove_source(self, project_id, **values):
        async with self._use_client() as client:
            path = "https://chatgpt.com/backend-api/projects/" + quote(project_id, safe="")
            rid = values["resource_id"]
            resource = values.get("resource")
            if resource is None or resource.id != rid:
                current = await maybe_await(client.get_project(project_id))
                if not any((x.get("file_id") or x.get("id")) == rid for x in current["resources"]):
                    raise ValueError("Resource does not belong to this project")
            await maybe_await(
                client.request("DELETE", path + "/files/" + quote(rid, safe=""), expect_json=False)
            )
            current_project = values.get("current")
            if isinstance(current_project, ProjectData):
                return replace(
                    current_project,
                    resources=tuple(r for r in current_project.resources if r.id != rid),
                )
