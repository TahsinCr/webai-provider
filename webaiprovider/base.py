from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from .core.types import OperationContext

if TYPE_CHECKING:
    from .accounts.account import Account


class ProviderType(str, Enum):
    CHATGPT = "chatgpt"
    CLAUDE = "claude"
    GEMINI = "gemini"
    GROK = "grok"

    @classmethod
    def parse(cls, value: ProviderType | str) -> ProviderType:
        try:
            return value if isinstance(value, cls) else cls(value.casefold())
        except (AttributeError, ValueError):
            supported = ", ".join(item.value for item in cls)
            raise ValueError(f"Unsupported provider type; expected one of: {supported}") from None


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


_ChatsT = TypeVar("_ChatsT", covariant=True)
_ProjectsT = TypeVar("_ProjectsT", covariant=True)
_ModelsT = TypeVar("_ModelsT")


class BaseProvider(ABC, Generic[_ChatsT, _ProjectsT, _ModelsT]):
    """Account identity and typed resource collections common to both provider APIs."""

    provider_type: ClassVar[ProviderType | None] = None
    _capabilities: ClassVar[frozenset[Capability]] = frozenset()
    _chats: _ChatsT
    _projects: _ProjectsT
    models: _ModelsT

    @property
    def chats(self) -> _ChatsT:
        return self._chats

    @property
    def projects(self) -> _ProjectsT:
        return self._projects

    @property
    def capabilities(self) -> frozenset[Capability]:
        return self._capabilities

    @abstractmethod
    def close(self) -> None | Awaitable[None]:
        """Close the provider and release its owned resources."""
        raise NotImplementedError

    def __init__(
        self,
        account: Account,
        *,
        type: ProviderType | str | None = None,
    ) -> None:
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
    def id(self) -> str:
        return self.type.value

    @property
    def account_id(self) -> str:
        return self.account.id

    @property
    def provider(self) -> str:
        return self.type.value

    def new_operation_id(self) -> str:
        return str(uuid.uuid4())

    def new_context(
        self,
        name: str,
        *,
        resource_id: str | None = None,
        operation_id: str | None = None,
        **fields: Any,
    ) -> OperationContext:
        return OperationContext(
            name=name,
            operation_id=operation_id or self.new_operation_id(),
            provider_id=self.id,
            account_id=self.account_id,
            resource_id=resource_id,
            **fields,
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__} type={self.type.value!r} account={self.account.id!r}>"


def _provider_class(provider_type: ProviderType, *, asynchronous: bool):
    names = {
        ProviderType.CHATGPT: "ChatGPT",
        ProviderType.CLAUDE: "Claude",
        ProviderType.GEMINI: "Gemini",
        ProviderType.GROK: "Grok",
    }
    module_name = "async_provider" if asynchronous else "provider"
    module = import_module(f".providers.{provider_type.value}.{module_name}", __package__)
    return getattr(module, ("Async" if asynchronous else "") + names[provider_type] + "Provider")


def _construct_provider(cls, base_cls, args, options, *, asynchronous: bool):
    if cls is base_cls:
        if not args and "account" not in options:
            raise TypeError("A provider account is required")
        account = args[0] if args else options["account"]
        requested = options.get("type")
        selected = ProviderType.parse(account.provider if requested is None else requested)
        return object.__new__(_provider_class(selected, asynchronous=asynchronous))
    return object.__new__(cls)
