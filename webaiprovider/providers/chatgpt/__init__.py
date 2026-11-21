from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .. import resolve_lazy_export

if TYPE_CHECKING:
    from .async_client import AsyncChatGPTClient as AsyncChatGPTClient
    from .async_provider import AsyncChatGPTProvider as AsyncChatGPTProvider
    from .auth import BrowserSession as BrowserSession
    from .catalogs import ChatGPTModels as ChatGPTModels
    from .client import ChatGPTClient as ChatGPTClient
    from .collections import AsyncChatGPTProjectsCollection as AsyncChatGPTProjectsCollection
    from .collections import ChatGPTProjectsCollection as ChatGPTProjectsCollection
    from .errors import ChatGPTError as ChatGPTError
    from .errors import SentinelCompatibilityError as SentinelCompatibilityError
    from .provider import ChatGPTProvider as ChatGPTProvider
    from .resources import AsyncChatGPTChat as AsyncChatGPTChat
    from .resources import AsyncChatGPTMessage as AsyncChatGPTMessage
    from .resources import AsyncChatGPTProject as AsyncChatGPTProject
    from .resources import ChatGPTArtifact as ChatGPTArtifact
    from .resources import ChatGPTAttachment as ChatGPTAttachment
    from .resources import ChatGPTChat as ChatGPTChat
    from .resources import ChatGPTMessage as ChatGPTMessage
    from .resources import ChatGPTProject as ChatGPTProject
    from .resources import ChatGPTResponse as ChatGPTResponse
    from .resources import ChatGPTSource as ChatGPTSource

_EXPORTS = {
    "AsyncChatGPTClient": "async_client",
    "BrowserSession": "auth",
    "ChatGPTClient": "client",
    "ChatGPTError": "errors",
    "SentinelCompatibilityError": "errors",
    "ChatGPTModels": "catalogs",
    "ChatGPTChat": "resources",
    "AsyncChatGPTProject": "resources",
    "ChatGPTProject": "resources",
    "AsyncChatGPTMessage": "resources",
    "ChatGPTMessage": "resources",
    "ChatGPTResponse": "resources",
    "ChatGPTAttachment": "resources",
    "ChatGPTArtifact": "resources",
    "ChatGPTSource": "resources",
    "AsyncChatGPTChat": "resources",
    "ChatGPTProjectsCollection": "collections",
    "AsyncChatGPTProjectsCollection": "collections",
    "ChatGPTProvider": "provider",
    "AsyncChatGPTProvider": "async_provider",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
