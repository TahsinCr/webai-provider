"""ChatGPT project controls through the authenticated HTTP client."""

import asyncio
import mimetypes
import time
from urllib.parse import quote

from ...resources.common import Choice
from ...resources.project.types import (
    ProjectOptions,
    source_file,
)


class ChatGPTProjectEditor:
    def __init__(self, session, factory, *, client_scope=None):
        self.session, self.factory = session, factory
        self.client_scope = client_scope or (lambda: self.factory(self.session))

    async def close(self):
        pass

    async def options(self, project_id=None):
        def read():
            if not project_id:
                return ProjectOptions(
                    fields=("name",),
                    visibility=(Choice("private", "Özel"),),
                    memory=(
                        Choice("default", "Varsayılan"),
                        Choice("project_only", "Yalnızca proje"),
                    ),
                )
            with self.client_scope() as client:
                raw = client.get_project(project_id)["metadata"]
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

        return await asyncio.to_thread(read)

    async def write(self, operation, project_id=None, **values):
        def send():
            scope = self.factory(self.session) if operation == "add_source" else self.client_scope()
            with scope as client:
                path = "https://chatgpt.com/backend-api/projects/" + quote(project_id, safe="")
                if operation == "update":
                    current = client.get_project(project_id)
                    native = current["metadata"]
                    for _ in range(3):
                        if not isinstance(native.get("gizmo"), dict):
                            break
                        native = native["gizmo"]
                    display = native.get("display") or {}
                    client.request(
                        "PATCH",
                        path,
                        expect_json=False,
                        json={
                            "name": values.get("name", current["name"]),
                            "instructions": values.get("instructions", current["instructions"]),
                            "emoji": display.get("emoji"),
                            "theme": display.get("theme"),
                            "memory_scope": native.get("memory_scope"),
                        },
                    )
                elif operation == "add_source":
                    last_modified = int(time.time() * 1000)
                    if values["kind"] == "text":
                        name, data, mime = values["name"], values["text"].encode(), "text/plain"
                        if not name.endswith(".txt"):
                            name += ".txt"
                        size = len(data)
                    elif values["kind"] == "file":
                        file = source_file(values["path"])
                        name, data = file.name, file
                        metadata = file.stat()
                        size = metadata.st_size
                        last_modified = metadata.st_mtime_ns // 1_000_000
                        mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
                    else:
                        raise ValueError("Unsupported project source type")
                    uploaded = client.upload_file(name, mime, data, project_id=project_id)
                    client.request(
                        "POST",
                        path + "/files",
                        json={
                            "files": [
                                {
                                    "file_id": uploaded["id"],
                                    "last_modified": last_modified,
                                    "name": name,
                                    "size": uploaded.get("size_bytes", size),
                                    "type": mime,
                                    "location": "fs",
                                }
                            ]
                        },
                    )
                elif operation == "remove_source":
                    current = client.get_project(project_id)
                    rid = values["resource_id"]
                    if not any(
                        (x.get("file_id") or x.get("id")) == rid for x in current["resources"]
                    ):
                        raise ValueError("Resource does not belong to this project")
                    client.request(
                        "DELETE", path + "/files/" + quote(rid, safe=""), expect_json=False
                    )
                else:
                    raise ValueError("Unknown project operation")

        return await asyncio.to_thread(send)
