from __future__ import annotations

from typing import Any

from .. import resolve_lazy_export

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
