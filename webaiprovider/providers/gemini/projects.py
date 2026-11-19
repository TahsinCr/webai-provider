from dataclasses import replace

from ...errors import ProviderError
from ...resources.message.attachment import Attachment
from ...resources.project.types import ProjectData
from .generation import upload_file_references
from .notebooks import field, notebook_id


class GeminiProjectEditor:
    def __init__(self, rpc, notebooks):
        self.rpc, self.notebooks = rpc, notebooks

    async def options(self, project_id=None):
        from ...resources.project.types import ProjectOptions

        return ProjectOptions(
            fields=("name", "instructions") if project_id else ("name",),
            sources=(
                ({"id": "text", "title": "Metin"}, {"id": "file", "title": "Dosya"})
                if project_id
                else ()
            ),
            remove_sources=bool(project_id),
        )

    async def create(self, **values):
        detail = [
            values["name"],
            "",
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            None,
            1,
            None,
            None,
            None,
            None,
            None,
            [2, None, None, None, 1],
        ]
        parts = await self.rpc.call("oMH3Zd", [detail])
        matches = []

        def collect(item):
            if not isinstance(item, list):
                return
            if len(item) == 2 and isinstance(item[0], str) and item[0].startswith("notebooks/"):
                matches.append(notebook_id(item[0]))
            else:
                for child in item:
                    collect(child)

        collect(parts)
        if len(set(matches)) != 1:
            raise ProviderError(
                "Notebook creation could not be confirmed; the operation was not retried"
            )
        return matches[0]

    async def delete(self, project_id):
        remote = notebook_id(project_id)
        path = "/notebook/" + remote.split("/")[1]
        return await self.rpc.call("Nwkn9", [remote], path, allow_empty=True)

    async def update(self, project_id, **values):
        remote = notebook_id(project_id)
        path = "/notebook/" + remote.split("/")[1]
        current = values.get("current")
        if current is None or not isinstance((current.metadata or {}).get("native"), (list, tuple)):
            current = await self.notebooks.get(remote)
        native = current.metadata["native"][1]
        if len(native) < 13 or not isinstance(native[12], list):
            raise ProviderError("Gemini notebook settings contract changed")
        detail = [
            values.get("name", current.name),
            current.metadata.get("description") or "",
            values.get("instructions", current.instructions or ""),
            None,
            None,
            None,
            None,
            None,
            0,
            0,
            None,
            None,
            [None, None, 0, 1, 0],
            None,
            None,
            0,
            native[12],
            None,
            None,
        ]
        result = await self.rpc.call("kHv0Vd", [remote, detail], path, allow_empty=True)
        if isinstance(values.get("current"), ProjectData):
            return replace(
                current,
                name=values.get("name", current.name),
                instructions=values.get("instructions", current.instructions or ""),
            )
        return result

    async def add_source(self, project_id, **values):
        remote = notebook_id(project_id)
        path = "/notebook/" + remote.split("/")[1]
        kind = values["kind"]
        if kind == "text":
            name, mime_type = values["name"], "text/plain"
            uploaded, text = None, [values["text"], 1]
        elif kind == "file":
            ((reference, name, mime_type),) = await upload_file_references(
                await self.rpc.connect(), [values["path"]]
            )
            if not reference.startswith("/"):
                raise ProviderError("Gemini returned an invalid file upload reference")
            uploaded, text = [reference, mime_type], None
        else:
            raise ValueError("Unsupported notebook source type")
        detail = [None, name, None, None, uploaded, text, None, None, mime_type]
        result = await self.rpc.call("ko3zcd", [remote, detail, [1, 3]], path)
        existing = {resource.id for resource in values.get("existing_resources", ())}
        matches = {}
        for part in result:
            if not isinstance(part, list):
                continue
            for row in part:
                if field(row, 0) != remote:
                    continue
                bundle = field(field(row, 1), 10)
                sources = field(bundle, 0)
                if not isinstance(sources, list):
                    continue
                for source in sources:
                    source_id = field(source, 5)
                    if (
                        isinstance(source_id, str)
                        and source_id.startswith(remote + "/sources/")
                        and source_id not in existing
                        and field(source, 2) == name
                    ):
                        matches[source_id] = source
        if len(matches) == 1:
            source_id, source = matches.popitem()
            return Attachment(
                source_id,
                name,
                mime_type=mime_type,
                metadata={"kind": kind, "native": source, "source_type": field(source, 1)},
            )
        if kind == "file":
            raise ProviderError("Gemini did not confirm the added file; sync before retrying")
        return result

    async def remove_source(self, project_id, **values):
        remote = notebook_id(project_id)
        path = "/notebook/" + remote.split("/")[1]
        resource = values["resource_id"]
        if not resource.startswith(remote + "/sources/"):
            raise ValueError("Source belongs to another notebook")
        result = await self.rpc.call("AptDmf", [resource, [1, 3]], path, allow_empty=True)
        current = values.get("current")
        if isinstance(current, ProjectData):
            return replace(
                current,
                resources=tuple(r for r in current.resources if r.id != resource),
            )
        return result
