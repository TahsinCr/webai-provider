from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .. import resolve_lazy_export

if TYPE_CHECKING:
    from .async_client import AsyncGrokClient as AsyncGrokClient
    from .async_provider import AsyncGrokWebAIProvider as AsyncGrokWebAIProvider
    from .catalogs import GrokModels as GrokModels
    from .client import GrokClient as GrokClient
    from .provider import GrokWebAIProvider as GrokWebAIProvider
    from .resources import AsyncGrokChat as AsyncGrokChat
    from .resources import AsyncGrokMessage as AsyncGrokMessage
    from .resources import AsyncGrokProject as AsyncGrokProject
    from .resources import GrokArtifact as GrokArtifact
    from .resources import GrokAttachment as GrokAttachment
    from .resources import GrokChat as GrokChat
    from .resources import GrokMessage as GrokMessage
    from .resources import GrokProject as GrokProject
    from .resources import GrokResponse as GrokResponse
    from .resources import GrokSource as GrokSource

_EXPORTS = {
    "AsyncGrokClient": "async_client",
    "GrokClient": "client",
    "GrokModels": "catalogs",
    "GrokChat": "resources",
    "AsyncGrokProject": "resources",
    "GrokProject": "resources",
    "AsyncGrokMessage": "resources",
    "GrokMessage": "resources",
    "GrokResponse": "resources",
    "GrokAttachment": "resources",
    "GrokArtifact": "resources",
    "GrokSource": "resources",
    "AsyncGrokChat": "resources",
    "GrokWebAIProvider": "provider",
    "AsyncGrokWebAIProvider": "async_provider",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
