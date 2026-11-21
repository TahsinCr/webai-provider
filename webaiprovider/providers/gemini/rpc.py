"""Gemini notebook RPC helpers and blocking RPC client."""

from __future__ import annotations

import json
from dataclasses import dataclass

from ...core.json import json_loads
from ...errors import ProviderError
from .errors import GeminiCompatibilityError
from .notebooks import notebook_id


def rpc_value(value, *path, default=None):
    for index in path:
        if not isinstance(value, list) or not isinstance(index, int) or not 0 <= index < len(value):
            return default
        value = value[index]
    return default if value is None else value


def rpc_parts(text, rpc):
    for line in text.splitlines():
        if not line.startswith("[["):
            continue
        try:
            rows = json_loads(line)
        except (ValueError, TypeError) as error:
            raise GeminiCompatibilityError("Gemini RPC response format has changed") from error
        for row in rows:
            if rpc_value(row, 0) == "wrb.fr" and rpc_value(row, 1) == rpc:
                body = rpc_value(row, 2)
                if body is not None:
                    try:
                        yield json_loads(body)
                    except (ValueError, TypeError) as error:
                        raise GeminiCompatibilityError(
                            "Gemini RPC response format has changed"
                        ) from error


def decode_rpc_response(response, rpc, allow_empty):
    parts = []
    for line in response.text.splitlines():
        if not line.startswith('[["wrb.fr"'):
            continue
        try:
            rows = json_loads(line)
        except (ValueError, TypeError) as error:
            raise GeminiCompatibilityError("Gemini notebook RPC response changed") from error
        if not isinstance(rows, list):
            raise GeminiCompatibilityError("Gemini notebook RPC response changed")
        for row in rows:
            if not isinstance(row, list) or len(row) < 3:
                continue
            if row[0] == "wrb.fr" and row[1] == rpc:
                if row[2] is None:
                    if allow_empty:
                        return []
                    raise ProviderError("Gemini denied permission or the RPC contract changed.")
                try:
                    parts.append(json_loads(row[2]))
                except (ValueError, TypeError) as error:
                    raise GeminiCompatibilityError(
                        "Gemini notebook RPC response changed"
                    ) from error
    if not parts:
        raise ProviderError("Gemini notebook RPC response changed")
    return parts


def resolve_notebook_rpc_read(mode, *, remote=None, operation="list", cursor=None):
    if mode != "notebooks":
        raise ValueError("Unsupported Gemini RPC group")
    if remote:
        remote = notebook_id(remote)
    if operation == "chats":
        if not remote:
            raise ValueError("Notebook required")
        return "MaZiqc", [10, cursor, [None, None, 1, remote, 1]], "/notebooks/view", False
    if remote:
        path = "/notebook/" + remote.split("/")[1]
        return "HcT8bb", [remote, ["en"], 0], path, True
    return "CNgdBe", [2, ["en"], 0, None, [2]], "/notebooks/view", False


@dataclass(frozen=True)
class RPCPayload:
    rpcid: str
    payload: str

    def serialize(self):
        return [self.rpcid, self.payload, None, "generic"]


class GeminiRPC:
    """Blocking Gemini RPC client running directly on the caller thread."""

    def __init__(self, connect):
        self.connect = connect

    def call(self, rpc, args, source_path="/notebooks/view", allow_empty=False):
        client = self.connect()
        response = client.batch(
            [RPCPayload(rpcid=rpc, payload=json.dumps(args))],
            source_path=source_path,
        )
        return decode_rpc_response(response, rpc, allow_empty)

    def read(self, mode, *, remote=None, operation="list", cursor=None):
        rpc, args, path, allow_empty = resolve_notebook_rpc_read(
            mode, remote=remote, operation=operation, cursor=cursor
        )
        return self.call(rpc, args, path, allow_empty=allow_empty)
