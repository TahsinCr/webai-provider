from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ...collections import AsyncManageableCollection, ManageableCollection, _CollectionFacade
from ...errors import ProviderError
from ...models import Conversation, Page
from ..project.project import AsyncProject, Project
from .chat import AsyncChat, Chat

if TYPE_CHECKING:
    from ...async_provider import AsyncProvider
    from ...provider import Provider


def _chat_from_conversation(item: Conversation, session: AsyncProvider) -> AsyncChat:
    return session._chat_type(
        title=item.title,
        remote_id=item.origin.remote_id,
        project_id=item.project_id,
        session=session,
    )


class AsyncChatsCollection(AsyncManageableCollection[AsyncChat]):
    def __init__(self, session: AsyncProvider) -> None:
        super().__init__()
        self.session = session

    async def create(
        self,
        title: str = "Yeni Sohbet",
        *,
        project: AsyncProject | str | None = None,
    ) -> AsyncChat:
        project_id = (
            (project.remote_id or project.id)
            if isinstance(project, AsyncProject)
            else str(project)
            if project is not None
            else None
        )
        chat = self.session._chat_type(
            title=title,
            session=self.session,
            project_id=project_id,
        )
        self._items.insert(0, chat)
        return chat

    async def refresh(self) -> list[AsyncChat]:
        page = await self.session._require_provider().list_chats()
        if not isinstance(page, Page) or any(
            not isinstance(item, Conversation) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid conversation page")
        self._items = [_chat_from_conversation(item, self.session) for item in page.items]
        return list(self._items)

    async def update(
        self,
        chat: AsyncChat | str,
        *,
        title: str | None = None,
    ) -> AsyncChat:
        target = chat if isinstance(chat, AsyncChat) else await self.get(chat)
        if title is not None and title != target.title:
            if target.remote_id:
                await self.session._require_provider().rename_chat(target.remote_id, title)
            target.title = title
        return target

    async def delete(self, chat: AsyncChat | str) -> None:
        target = chat if isinstance(chat, AsyncChat) else await self.get(chat)
        await target.delete()
        self._items = [c for c in self._items if c.id != target.id]


class ChatsCollection(_CollectionFacade[AsyncChat, Chat], ManageableCollection[Chat]):
    def __init__(self, owner: Provider) -> None:
        super().__init__(owner, owner._provider.chats)

    def _wrap(self, item: AsyncChat) -> Chat:
        return self._owner._wrap_chat(item)

    def refresh(self) -> list[Chat]:
        return [self._wrap(item) for item in self._owner._call(self._collection.refresh())]

    def create(
        self,
        title: str = "New chat",
        *,
        project: Project | AsyncProject | str | None = None,
    ) -> Chat:
        target = project._resource if isinstance(project, Project) else project
        chat = self._owner._call(self._collection.create(title, project=target))
        return self._wrap(chat)

    def update(
        self,
        chat: Chat | AsyncChat | str,
        *,
        title: str | None = None,
        **fields: Any,
    ) -> Chat:
        if fields:
            raise TypeError(f"Unsupported chat fields: {', '.join(fields)}")
        target = chat._resource if isinstance(chat, Chat) else chat
        return self._wrap(self._owner._call(self._collection.update(target, title=title)))

    def delete(self, chat: Chat | AsyncChat | str) -> None:
        target = chat._resource if isinstance(chat, Chat) else chat
        self._owner._call(self._collection.delete(target))
