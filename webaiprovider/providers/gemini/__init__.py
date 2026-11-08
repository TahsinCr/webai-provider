from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "GeminiCompatibilityError": "errors",
    "GeminiError": "errors",
    "GeminiStreamError": "errors",
    "GeminiModels": "catalogs",
    "GeminiChat": "resources",
    "GeminiProvider": "provider",
    "AsyncGeminiProvider": "provider",
    "Notebooks": "notebooks",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module}", __name__), name)
    globals()[name] = value
    return value
