"""Gemini notebook mappings over the account-owned HTTP RPC reader."""

import re
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlparse

from ...collections import Page
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.project.types import ProjectData
from .errors import GeminiCompatibilityError, GeminiError, GeminiNotFoundError
from .history import timestamp


def notebook_id(value):
    """Accept native IDs and the earlier Gemini-only browser URL identities."""
    if isinstance(value, str) and value.startswith("https://"):
        url = urlparse(value)
        if url.netloc == "gemini.google.com" and re.fullmatch(
            r"/notebook/[A-Za-z0-9_-]{6,}/?", url.path
        ):
            value = "notebooks/" + url.path.rstrip("/").rsplit("/", 1)[1]
    if not isinstance(value, str) or not re.fullmatch(r"notebooks/[A-Za-z0-9_-]{6,}", value):
        raise ValueError("unsupported Gemini notebook identity")
    return value


def field(value, index, default=None):
    return value[index] if isinstance(value, list) and len(value) > index else default


def project_from_native(row, account_id):
    remote, detail = field(row, 0), field(row, 1)
    if (
        not isinstance(remote, str)
        or not remote.startswith("notebooks/")
        or not isinstance(detail, list)
        or not isinstance(field(detail, 0), str)
    ):
        raise GeminiCompatibilityError(
            "The Gemini notebook contract changed; the record mapping must be updated."
        )
    bundle = field(detail, 10, []) or []
    resources = []
    for source in field(bundle, 0, []) or []:
        resource_id = field(source, 5)
        name = field(source, 2)
        if isinstance(resource_id, str) and isinstance(name, str):
            resources.append(
                Attachment(
                    resource_id, name, metadata={"native": source, "source_type": field(source, 1)}
                )
            )
    settings = field(detail, 12, []) or []
    instructions = field(field(row, 2, []), 0)
    return ProjectData(
        remote,
        detail[0],
        instructions if isinstance(instructions, str) else None,
        tuple(resources),
        {
            "kind": "notebook",
            "updated_at": timestamp(field(field(detail, 14), 1)),
            "url": "https://gemini.google.com/notebook/" + remote.removeprefix("notebooks/"),
            "native": row,
            "notebook_type": field(settings, 4),
            "description": field(detail, 1),
            "read_only": False,
            "supports_chat": True,
        },
    )


class Notebooks:
    def __init__(self, account_id):
        self.account_id = account_id
        self.reader: Callable[..., Awaitable[list[Any]]] | None = None

    async def read(self, remote=None, mode=None, cursor=None):
        if self.reader is not None:
            return await self.reader(
                "notebooks",
                remote=notebook_id(remote) if remote else None,
                operation=mode or "list",
                cursor=cursor,
            )
        raise GeminiError("Gemini notebook HTTP reader is not configured")

    async def list(self, cursor=None):
        if cursor:
            raise GeminiError("Gemini notebook pagination is not supported by this version")
        parts = await self.read()
        rows = [row for part in parts for row in field(part, 2, []) or []]
        return Page(tuple(project_from_native(row, self.account_id) for row in rows))

    async def chats(self, remote, cursor=None):
        remote = notebook_id(remote)
        parts = await self.read(remote, mode="chats", cursor=cursor)
        rows = [row for part in parts for row in field(part, 2, []) or []]
        conversations = []
        for row in rows:
            if not isinstance(field(row, 0), str) or not isinstance(field(row, 1), str):
                raise GeminiCompatibilityError("Gemini notebook conversation mapping has changed")
            conversations.append(
                Conversation(
                    row[0],
                    row[1],
                    project=ProjectData(remote, ""),
                    metadata={"native": row},
                )
            )
        cursor_value = field(parts[-1], 1)
        return Page(tuple(conversations), cursor_value if isinstance(cursor_value, str) else None)

    async def get(self, remote):
        remote = notebook_id(remote)
        parts = await self.read(remote)
        rows = [row for part in parts for row in part if field(row, 0) == remote]
        if not rows:
            raise GeminiNotFoundError("Gemini notebook details were not found")
        if len(rows) != 1:
            raise GeminiError("Gemini notebook details were not found")
        return project_from_native(rows[0], self.account_id)
