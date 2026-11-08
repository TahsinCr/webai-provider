from __future__ import annotations

from importlib import import_module
from typing import Any

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
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module}", __name__), name)
    globals()[name] = value
    return value
