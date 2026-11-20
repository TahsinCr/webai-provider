"""High-throughput JSON serialization and deserialization helpers."""

from __future__ import annotations

import json as _stdlib_json
from typing import Any

try:
    import orjson as _orjson
except ImportError:  # pragma: no cover
    _orjson = None  # type: ignore[assignment]


def json_loads(data: bytes | bytearray | memoryview | str) -> Any:
    """Parse JSON from UTF-8 bytes or str using orjson when available."""
    if _orjson is not None:
        if isinstance(data, memoryview):
            data = bytes(data)
        return _orjson.loads(data)
    if isinstance(data, memoryview):
        data = data.tobytes()
    return _stdlib_json.loads(data)


def json_dumps(obj: Any) -> str:
    """Serialize an object to a compact JSON string."""
    if _orjson is not None:
        return _orjson.dumps(obj).decode("utf-8")
    return _stdlib_json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
