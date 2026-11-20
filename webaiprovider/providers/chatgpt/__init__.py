from __future__ import annotations

from typing import Any

from .. import resolve_lazy_export

_EXPORTS = {
    "BrowserSession": "session",
    "SessionPaths": "session",
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
    "ChatGPTProjectsCollection": "provider",
    "AsyncChatGPTProjectsCollection": "collections",
    "ChatGPTProvider": "provider",
    "AsyncChatGPTProvider": "async_provider",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
