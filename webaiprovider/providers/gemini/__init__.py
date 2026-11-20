from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .. import resolve_lazy_export

if TYPE_CHECKING:
    from .async_provider import AsyncGeminiProvider as AsyncGeminiProvider
    from .catalogs import GeminiModels as GeminiModels
    from .errors import GeminiCompatibilityError as GeminiCompatibilityError
    from .errors import GeminiError as GeminiError
    from .errors import GeminiStreamError as GeminiStreamError
    from .notebooks import Notebooks as Notebooks
    from .provider import GeminiProvider as GeminiProvider
    from .resources import AsyncGeminiChat as AsyncGeminiChat
    from .resources import AsyncGeminiMessage as AsyncGeminiMessage
    from .resources import AsyncGeminiProject as AsyncGeminiProject
    from .resources import GeminiArtifact as GeminiArtifact
    from .resources import GeminiAttachment as GeminiAttachment
    from .resources import GeminiChat as GeminiChat
    from .resources import GeminiMessage as GeminiMessage
    from .resources import GeminiProject as GeminiProject
    from .resources import GeminiResponse as GeminiResponse
    from .resources import GeminiSource as GeminiSource

_EXPORTS = {
    "GeminiCompatibilityError": "errors",
    "GeminiError": "errors",
    "GeminiStreamError": "errors",
    "GeminiModels": "catalogs",
    "GeminiChat": "resources",
    "AsyncGeminiProject": "resources",
    "GeminiProject": "resources",
    "AsyncGeminiMessage": "resources",
    "GeminiMessage": "resources",
    "GeminiResponse": "resources",
    "GeminiAttachment": "resources",
    "GeminiArtifact": "resources",
    "GeminiSource": "resources",
    "AsyncGeminiChat": "resources",
    "GeminiProvider": "provider",
    "AsyncGeminiProvider": "async_provider",
    "Notebooks": "notebooks",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
