from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .. import resolve_lazy_export

if TYPE_CHECKING:
    from .async_client import AsyncGeminiClient as AsyncGeminiClient
    from .async_provider import AsyncGeminiWebAIProvider as AsyncGeminiWebAIProvider
    from .catalogs import GeminiModels as GeminiModels
    from .client import GeminiClient as GeminiClient
    from .deep_research import DeepResearchSources as DeepResearchSources
    from .deep_research import GeminiDeepResearchTool as GeminiDeepResearchTool
    from .errors import GeminiCompatibilityError as GeminiCompatibilityError
    from .errors import GeminiError as GeminiError
    from .errors import GeminiStreamError as GeminiStreamError
    from .notebooks import GeminiNotebooks as GeminiNotebooks
    from .provider import GeminiWebAIProvider as GeminiWebAIProvider
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
    "AsyncGeminiClient": "async_client",
    "GeminiClient": "client",
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
    "GeminiWebAIProvider": "provider",
    "AsyncGeminiWebAIProvider": "async_provider",
    "GeminiNotebooks": "notebooks",
    "DeepResearchSources": "deep_research",
    "GeminiDeepResearchTool": "deep_research",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
