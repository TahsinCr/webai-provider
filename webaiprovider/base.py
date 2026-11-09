from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from .accounts.account import Account
    from .provider import ProviderType


class Capability(str, Enum):
    CHAT = "chat"
    TEMPORARY = "temporary"
    HISTORY = "history"
    RENAME = "rename"
    DELETE = "delete"
    AUTH = "auth"
    ATTACHMENTS = "attachments"
    RESOURCES = "resources"
    PROJECTS = "projects"
    PROJECT_CREATE = "project_create"
    PROJECT_UPDATE = "project_update"
    PROJECT_DELETE = "project_delete"
    PROJECT_MEMBERSHIP = "project_membership"
    MODELS = "models"
    EFFORT = "effort"
    MESSAGE_EDIT = "message_edit"
    MESSAGE_RETRY = "message_retry"
    CHAT_BRANCH = "chat_branch"


class BaseProvider:
    """Shared account and provider identity for sync and async clients."""

    provider_type: ClassVar[ProviderType | None] = None

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
    ) -> None:
        from .provider import ProviderType

        account_type = ProviderType.parse(account.provider)
        expected_type = self.provider_type
        requested_type = ProviderType.parse(type) if type is not None else expected_type
        selected_type = requested_type or account_type
        if expected_type is not None and selected_type is not expected_type:
            raise ValueError(
                f"{self.__class__.__name__} requires provider type {expected_type.value!r}"
            )
        if selected_type is not account_type:
            raise ValueError("Provider type must match the account provider")
        self.account = account
        self.type = selected_type

    @property
    def provider(self) -> str:
        return self.type.value

    def __repr__(self) -> str:
        return f"<{type(self).__name__} type={self.type.value!r} account={self.account.id!r}>"
