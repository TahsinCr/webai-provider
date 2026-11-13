from __future__ import annotations

from typing import Any

from .. import resolve_lazy_export

_EXPORTS = {
    "ClaudeClient": "client",
    "ClaudeCompatibilityError": "errors",
    "ClaudeError": "errors",
    "ClaudeRequestNotSent": "errors",
    "ClaudeStreamError": "errors",
    "ClaudeModels": "catalogs",
    "ClaudeChat": "provider",
    "AsyncClaudeChat": "resources",
    "ClaudeProvider": "provider",
    "AsyncClaudeProvider": "provider",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
