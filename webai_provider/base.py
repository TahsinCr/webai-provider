from __future__ import annotations

import asyncio
from typing import Any

from .api import Capability, Provider
from .errors import FeatureUnavailable, ProviderError, RemoteNotFound
from .models import (
    Conversation,
    Feature,
    FeatureState,
    ModelCatalog,
    Origin,
    Page,
    Project,
    ProviderDescription,
    SessionStatus,
)
from .projects import ProjectOptions


class ProviderAdapter(Provider):
    account_id: str

    async def close(self) -> None:
        pass

    def attachment_policy(self) -> dict | None:
        from .utils.attachments import policy_for

        policy = policy_for(getattr(self, "id", ""))
        return policy.as_dict() if policy else None

    def _origin(self, remote) -> Origin:
        return Origin(getattr(self, "id", ""), getattr(self, "account_id", "default"), remote)

    async def describe(self) -> ProviderDescription:
        caps = getattr(self, "capabilities", frozenset())
        return ProviderDescription(
            getattr(self, "id", ""),
            tuple(
                Feature(
                    c,
                    FeatureState.AVAILABLE if c in caps else FeatureState.UNSUPPORTED,
                )
                for c in Capability
            ),
        )

    async def login(self) -> SessionStatus:
        return await self.authenticate()

    async def authenticate(self) -> SessionStatus:
        return SessionStatus(
            getattr(self, "id", ""),
            getattr(self, "account_id", "default"),
            authenticated=True,
        )

    async def session_status(self) -> SessionStatus:
        return await self.authenticate()

    async def forget_session(self) -> SessionStatus:
        return SessionStatus(
            getattr(self, "id", ""),
            getattr(self, "account_id", "default"),
            authenticated=False,
        )

    async def models(self, *, force: bool = False) -> ModelCatalog:
        raise FeatureUnavailable(Capability.MODELS, "not_implemented")

    async def list_chats(self, cursor: str | None = None, limit: int = 28) -> Page[Conversation]:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def get_chat(self, conversation_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def branch_chat(self, conversation_id: str, message_id: str) -> Conversation:
        raise FeatureUnavailable(Capability.CHAT_BRANCH, "not_implemented")

    async def conversation_attachment_count(self, conversation_id: str) -> int | None:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    async def rename_chat(self, conversation_id: str, title: str) -> None:
        raise FeatureUnavailable(Capability.CHAT, "not_implemented")

    async def delete_chat(self, conversation_id: str) -> None:
        raise FeatureUnavailable(Capability.DELETE, "not_implemented")

    async def upload_file(self, path: Any) -> dict:
        raise FeatureUnavailable(Capability.ATTACHMENTS, "not_implemented")

    async def list_projects(self, cursor: str | None = None) -> Page[Project]:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def get_project(self, project_id: str) -> Project:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def list_project_chats(
        self, project_id: str, cursor: str | None = None
    ) -> Page[Conversation]:
        raise FeatureUnavailable(Capability.PROJECTS, "not_implemented")

    async def create_project(
        self,
        name: str,
        instructions: str = "",
        *,
        visibility: str = "private",
        memory: str = "default",
    ) -> Project:
        raise FeatureUnavailable(Capability.PROJECT_CREATE, "not_implemented")

    async def update_project(self, project: Project) -> Project:
        raise FeatureUnavailable(Capability.PROJECT_UPDATE, "not_implemented")

    async def project_options(self, project_id: str | None = None) -> ProjectOptions:
        return ProjectOptions()

    async def add_project_resource(self, project_id: str, kind: str, **values: Any) -> Project:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    async def remove_project_resource(self, project_id: str, resource_id: str) -> Project:
        raise FeatureUnavailable(Capability.RESOURCES, "not_implemented")

    async def delete_project(self, project_id: str) -> None:
        raise FeatureUnavailable(Capability.PROJECT_DELETE, "not_implemented")

    async def move_chat(self, conversation_id: str, project_id: str | None) -> None:
        raise FeatureUnavailable(Capability.PROJECT_MEMBERSHIP, "not_implemented")

    async def resolve_download(
        self, url: str, conversation_id: str | None = None, message_id: str | None = None
    ) -> Any | None:
        return None


async def confirm_deleted(read: Any, missing: Any = lambda value: False) -> None:
    for delay in (0, 0.2, 0.5, 1):
        if delay:
            await asyncio.sleep(delay)
        try:
            value = await read()
        except RemoteNotFound:
            return
        if missing(value):
            return
    raise ProviderError("Silme sunucuda doğrulanamadı; yerel kayıt korundu. Eşitleyin.")
