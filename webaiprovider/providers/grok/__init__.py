from __future__ import annotations

from typing import Any

from .. import resolve_lazy_export

_EXPORTS = {
    "GrokClient": "client",
    "GrokModels": "catalogs",
    "GrokChat": "provider",
    "AsyncGrokChat": "resources",
    "GrokProvider": "provider",
    "AsyncGrokProvider": "provider",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(__name__, _EXPORTS, name, globals())
