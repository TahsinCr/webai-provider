from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .. import resolve_lazy_export

if TYPE_CHECKING:
    from .async_provider import AsyncClaudeProvider as AsyncClaudeProvider
    from .catalogs import ClaudeModels as ClaudeModels
    from .client import ClaudeClient as ClaudeClient
    from .errors import ClaudeCompatibilityError as ClaudeCompatibilityError
    from .errors import ClaudeError as ClaudeError
    from .errors import ClaudeRequestNotSent as ClaudeRequestNotSent
    from .errors import ClaudeStreamError as ClaudeStreamError
    from .provider import ClaudeProvider as ClaudeProvider
    from .resources import AsyncClaudeChat as AsyncClaudeChat
    from .resources import AsyncClaudeMessage as AsyncClaudeMessage
    from .resources import AsyncClaudeProject as AsyncClaudeProject
    from .resources import ClaudeArtifact as ClaudeArtifact
    from .resources import ClaudeAttachment as ClaudeAttachment
    from .resources import ClaudeChat as ClaudeChat
    from .resources import ClaudeMessage as ClaudeMessage
    from .resources import ClaudeProject as ClaudeProject
    from .resources import ClaudeResponse as ClaudeResponse
    from .resources import ClaudeSource as ClaudeSource

_EXPORTS = {
    "ClaudeClient": "client",
    "ClaudeCompatibilityError": "errors",
    "ClaudeError": "errors",
    "ClaudeRequestNotSent": "errors",
    "ClaudeStreamError": "errors",
    "ClaudeModels": "catalogs",
    "ClaudeChat": "resources",
    "AsyncClaudeProject": "resources",
    "ClaudeProject": "resources",
    "AsyncClaudeMessage": "resources",
    "ClaudeMessage": "resources",
    "ClaudeResponse": "resources",
    "ClaudeAttachment": "resources",
    "ClaudeArtifact": "resources",
    "ClaudeSource": "resources",
    "AsyncClaudeChat": "resources",
    "ClaudeProvider": "provider",
    "AsyncClaudeProvider": "async_provider",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
