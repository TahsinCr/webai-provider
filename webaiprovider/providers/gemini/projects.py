from dataclasses import replace

from ...errors import ProviderError
from ...resources.project.types import ProjectData
from .notebooks import notebook_id


class GeminiProjectEditor:
    def __init__(self, rpc, notebooks):
        self.rpc, self.notebooks = rpc, notebooks

    async def options(self, project_id=None):
        from ...resources.project.types import ProjectOptions

        return ProjectOptions(
            fields=("name", "instructions") if project_id else ("name",),
            sources=({"id": "text", "title": "Metin"},) if project_id else (),
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
        if values["kind"] != "text":
            raise ValueError("Unsupported notebook source type")
        detail = [
            None,
            values["name"],
            None,
            None,
            None,
            [values["text"], 1],
            None,
            None,
            "text/plain",
        ]
        return await self.rpc.call("ko3zcd", [remote, detail, [1, 3]], path)

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
