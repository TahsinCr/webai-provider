"""Provider-specific web session implementations."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def resolve_lazy_export(
    package_name: str,
    exports: dict[str, str],
    name: str,
    namespace: dict[str, Any],
) -> Any:
    """Resolve and cache a lazily imported provider symbol."""
    module = exports.get(name)
    if module is None:
        raise AttributeError(f"module {package_name!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module}", package_name), name)
    namespace[name] = value
    return value
