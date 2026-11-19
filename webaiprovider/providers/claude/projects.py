from dataclasses import replace
from urllib.parse import quote

from ...errors import ProviderError
from ...resources.message.attachment import Attachment
from ...resources.project.types import (
    ProjectData,
    ProjectOptions,
    source_file,
)
from .client import remote_id


class ClaudeProjectEditor:
    def __init__(self, transport):
        self.client = transport

    async def options(self, project_id=None):
        return ProjectOptions(
            fields=("name", "instructions") if project_id else ("name",),
            sources=(
                {"id": "text", "title": "Metin"},
                {"id": "file", "title": "Dosya", "accept": "*/*"},
            )
            if project_id
            else (),
            remove_sources=bool(project_id),
        )

    async def update(self, project_id, **v):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        call = self.client.http.request
        fields = {"name": v["name"]} if "name" in v else {}
        if "instructions" in v:
            fields["prompt_template"] = v["instructions"]
        result = await call("PUT", path, json=fields)
        current = v.get("current")
        if (
            isinstance(current, ProjectData)
            and isinstance(result, dict)
            and (result.get("uuid") or project_id) == project_id
            and all(key in result and result[key] == value for key, value in fields.items())
        ):
            return replace(
                current,
                **{k: v[k] for k in ("name", "instructions") if k in v},
            )
        return result

    async def read_metadata(self, project_id, current):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        data = await self.client.http.request("GET", path)
        if not isinstance(data, dict) or (data.get("uuid") or data.get("id")) != project_id:
            raise ProviderError("Claude project response contract changed")
        return replace(
            current,
            name=data.get("name", current.name),
            instructions=data.get("prompt_template", current.instructions),
        )

    async def add_source(self, project_id, **v):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        call = self.client.http.request
        if v["kind"] == "text":
            result = await call(
                "POST", path + "/docs", json={"file_name": v["name"], "content": v["text"]}
            )
            if isinstance(result, dict) and (result.get("uuid") or result.get("id")):
                return Attachment(
                    str(result.get("uuid") or result.get("id") or ""),
                    result.get("file_name") or result.get("name") or v["name"],
                    metadata={"native": result},
                )
            return result
        if v["kind"] != "file":
            raise ValueError("Unsupported source type")
        from curl_cffi import CurlMime

        file = source_file(v["path"], error="Source must be at most 20 MiB")
        mime = CurlMime()
        try:
            mime.addpart(name="file", filename=file.name, local_path=str(file))
            result = await call("POST", path + "/upload", multipart=mime)
        finally:
            mime.close()
        if (
            isinstance(result, dict)
            and (result.get("uuid") or result.get("id"))
            and result.get("file_uuid")
        ):
            return Attachment(
                str(result.get("uuid") or result.get("id") or ""),
                result.get("file_name") or result.get("name") or file.name,
                metadata={"native": result},
            )
        return result

    async def remove_source(self, project_id, **v):
        path = await self.client.base() + "/projects/" + remote_id(project_id)
        call = self.client.http.request
        rid = quote(v["resource_id"], safe="")
        resource = v.get("resource")
        native = (
            resource.metadata.get("native")
            if resource is not None and isinstance(resource.metadata, dict)
            else None
        )
        result = None
        handled = False
        if isinstance(native, dict):
            native_id = native.get("uuid") or native.get("id")
            file_uuid = native.get("file_uuid")
            if file_uuid and (file_uuid == v["resource_id"] or native_id == v["resource_id"]):
                result = await call(
                    "POST", path + "/files/delete_many", json={"file_uuids": [file_uuid]}
                )
                handled = True
            elif native_id == v["resource_id"] and "file_uuid" not in native:
                result = await call("DELETE", path + "/docs/" + rid)
                handled = True
        if not handled:
            current_remote = await self.client.request("project", project_id=project_id)
            doc = next(
                (
                    x
                    for x in current_remote["docs"]
                    if (x.get("uuid") or x.get("id")) == v["resource_id"]
                ),
                None,
            )
            if doc is not None:
                result = await call("DELETE", path + "/docs/" + rid)
            else:
                file = next(
                    (
                        x
                        for x in current_remote["files"]
                        if (x.get("uuid") or x.get("id")) == v["resource_id"]
                    ),
                    None,
                )
                if file is None or not file.get("file_uuid"):
                    raise ValueError(
                        "Resource does not belong to this project or its file ID is missing"
                    )
                result = await call(
                    "POST",
                    path + "/files/delete_many",
                    json={"file_uuids": [file["file_uuid"]]},
                )
        current = v.get("current")
        if isinstance(current, ProjectData):
            return replace(
                current,
                resources=tuple(r for r in current.resources if r.id != v["resource_id"]),
            )
        return result
