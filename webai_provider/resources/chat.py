from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..collections.messages import MessagesCollection
    from ..resources.message import Message
    from .session import ProviderSession


class Chat:
    def __init__(
        self,
        id: str | None = None,
        title: str = "",
        *,
        session: ProviderSession | None = None,
        remote_id: str | None = None,
        project_id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.title = title
        self.session = session
        self.remote_id = remote_id
        self.project_id = project_id

        from ..collections.messages import MessagesCollection

        self.messages: MessagesCollection = MessagesCollection(self)

    def get(self, message_id: str) -> Message:
        return self.messages.get(message_id)

    async def download(self, target: Any) -> bytes:
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            return await self.session._bridge.download(target, conversation_id=self.remote_id)
        return b""

    async def rename(self, title: str) -> Chat:
        self.title = title
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.rename_chat(self, title)
        return self

    async def delete(self) -> None:
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            await self.session._bridge.delete_chat(self)

    async def branch(self, message_id: str, *, title: str | None = None) -> Chat:
        if self.session is not None and hasattr(self.session, "_bridge") and self.session._bridge:
            return await self.session._bridge.branch_chat(self, message_id, title=title)
        return Chat(
            title=title or f"{self.title} (Branch)",
            session=self.session,
            project_id=self.project_id,
        )

    def __repr__(self) -> str:
        return f"<Chat id={self.id!r} title={self.title!r} remote_id={self.remote_id!r}>"
