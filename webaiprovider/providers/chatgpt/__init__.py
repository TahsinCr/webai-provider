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
    "ChatGPTChat": "provider",
    "AsyncChatGPTChat": "resources",
    "ChatGPTProjectsCollection": "provider",
    "AsyncChatGPTProjectsCollection": "collections",
    "ChatGPTProvider": "provider",
    "AsyncChatGPTProvider": "provider",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
