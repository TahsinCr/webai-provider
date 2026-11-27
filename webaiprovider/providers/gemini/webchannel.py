"""Google WebChannel processSession protocol helpers for Gemini."""

from __future__ import annotations

import base64
import hashlib
import random
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ...core.json import json_loads
from ...resources.message.protocol import ProviderRequest
from ...resources.message.types import MessageAction
from .errors import GeminiCompatibilityError, GeminiStreamError
from .image import extract_proto_image_files
from .proto import ProtoView, encode_message

WEBCHANNEL_ORIGIN = "https://geminiweb-pa.clients6.google.com"
PROCESS_SESSION_PATH = "/v1/processSession"
_GEMINI_APP_ORIGIN = "https://gemini.google.com"


@dataclass(slots=True)
class WebChannelSessionState:
    """Reusable Google WebChannel session identifiers for multi-turn Gemini chats."""

    gsessionid: str
    sid: str
    rid: int
    aid: int = 0
    cver: int = 22

    def next_rid(self) -> int:
        self.rid += 1
        return self.rid

    def next_aid(self) -> int:
        self.aid += 1
        return self.aid


def sapisidhash_header(
    cookies: Mapping[str, Any], *, origin: str = _GEMINI_APP_ORIGIN
) -> str | None:
    """Build Google SAPISIDHASH Authorization header from session cookies."""
    sapisid = cookies.get("__Secure-1PAPISID") or cookies.get("SAPISID")
    if not isinstance(sapisid, str) or not sapisid:
        return None
    ts = int(time.time())
    digest = hashlib.sha1(f"{ts} {sapisid} {origin}".encode("utf-8")).hexdigest()
    return f"SAPISIDHASH {ts}_{digest}"


def build_process_session_headers(
    cookies: Mapping[str, Any],
    *,
    temporary: bool = False,
    gsessionid: str | None = None,
) -> dict[str, str]:
    """Build headers for Gemini processSession WebChannel requests."""
    headers: dict[str, str] = {
        "Origin": _GEMINI_APP_ORIGIN,
        "Referer": _GEMINI_APP_ORIGIN + "/",
        "X-Goog-AuthUser": "0",
        "x-goog-ext-525001261-bin": base64.b64encode(b"\x01" if temporary else b"\x00").decode(
            "ascii"
        ),
    }
    auth = sapisidhash_header(cookies)
    if auth:
        headers["Authorization"] = auth
    if gsessionid:
        headers["X-HTTP-Session-Id"] = gsessionid
    return headers


def select_gemini_model(
    session: Any, request: ProviderRequest
) -> tuple[Any, int | None, int | None]:
    """Resolve selected model metadata, model number, and thinking effort value."""
    selected = (
        next((m for m in session.models if request.model in (m.id, m.metadata["model_id"])), None)
        if request.model
        else next(iter(session.models), None)
    )
    if selected is None:
        raise ValueError("This model is not available for this Gemini account")
    effort = request.effort or selected.default_effort
    if effort is not None and effort not in {choice.id for choice in selected.efforts}:
        raise ValueError("This effort level is not available for this Gemini model")
    thinking = selected.metadata["effort_values"].get(effort)
    number = selected.metadata.get("number")
    return selected, number, thinking


def build_process_session_turn_data(
    session: Any,
    request: ProviderRequest,
    metadata: list[Any],
    files: list[Any],
    notebook: Any = None,
    *,
    tool_fields: Mapping[int, Any] | None = None,
    raw_override_fields: Mapping[int, Any] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Build form-encoded req0___data__ / req1___data__ payload for processSession."""
    _selected, model_number, thinking = select_gemini_model(session, request)

    if raw_override_fields is not None:
        inner: dict[int, Any] = dict(raw_override_fields)
    else:
        inner = {
            1: request.prompt,
            81: 1,
        }
        if model_number is not None:
            inner[80] = model_number
        if thinking is not None:
            inner[97] = thinking
        if request.temporary:
            inner[46] = 1

        conv_id = metadata[0] if metadata and metadata[0] else request.conversation_id
        turn_token = metadata[9] if len(metadata) > 9 else None
        if request.action is not None:
            conv_ctx: dict[int, Any] = {
                1: conv_id or "",
                2: metadata[1] if len(metadata) > 1 and metadata[1] else "",
                3: metadata[2] if len(metadata) > 2 and metadata[2] else "",
            }
            if isinstance(turn_token, str) and turn_token:
                conv_ctx[10] = turn_token
            inner[3] = conv_ctx
            inner[33] = 1
            inner[64] = {1: 1001, 3: 0}
            inner[73] = 5 if request.action == MessageAction.RETRY else 2
        elif conv_id:
            conv_ctx = {
                1: conv_id,
                2: metadata[1] if len(metadata) > 1 and metadata[1] else "",
                3: metadata[2] if len(metadata) > 2 and metadata[2] else "",
            }
            if isinstance(turn_token, str) and turn_token:
                conv_ctx[10] = turn_token
            inner[3] = conv_ctx

        if notebook:
            inner[20] = notebook[0]
        if tool_fields:
            inner.update(tool_fields)

    req0_bytes = encode_message({1: {8: {5: {1: inner}}}})
    req0_b64 = base64.b64encode(req0_bytes).decode("ascii")

    form: dict[str, str] = {
        "count": "2" if files else "1",
        "ofs": "0",
        "req0___data__": req0_b64,
    }

    if files:
        file_entries: list[dict[int, Any]] = []
        for item in files:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                ref_part, name = item[0], item[1]
                ref = ref_part[0] if isinstance(ref_part, (list, tuple)) else ref_part
                file_entries.append({3: {1: str(ref), 2: str(name)}})
            elif isinstance(item, dict):
                ref = item.get("reference") or item.get("ref") or ""
                name = item.get("name") or "attachment"
                file_entries.append({3: {1: str(ref), 2: str(name)}})
        if file_entries:
            req1_bytes = encode_message({2: {9: file_entries}})
            form["req1___data__"] = base64.b64encode(req1_bytes).decode("ascii")
        else:
            form["count"] = "1"
    else:
        form["count"] = "1"

    headers = build_process_session_headers(
        getattr(session, "_cookies", {}),
        temporary=request.temporary,
    )
    return headers, form


class WebChannelFrameDecoder:
    """Incremental decoder for Google WebChannel length-prefixed chunked frames."""

    def __init__(self, *, max_buffer_bytes: int = 64 * 1024 * 1024) -> None:
        self._buffer = ""
        self._max_buffer_bytes = max_buffer_bytes

    def feed(self, chunk: bytes, *, final: bool = False) -> list[Any]:
        if chunk:
            self._buffer += chunk.decode("utf-8", errors="replace")
            if len(self._buffer) > self._max_buffer_bytes:
                raise GeminiStreamError("Gemini WebChannel frame exceeded buffer limit")
        frames: list[Any] = []
        while self._buffer:
            stripped = self._buffer.lstrip("\r\n")
            if not stripped:
                self._buffer = ""
                break
            newline_idx = stripped.find("\n")
            if newline_idx == -1:
                if final:
                    self._try_parse_json(stripped, frames)
                    self._buffer = ""
                break
            length_line = stripped[:newline_idx].strip()
            if length_line.isdigit():
                expected_len = int(length_line)
                rest = stripped[newline_idx + 1 :]
                if len(rest) < expected_len:
                    if final:
                        self._try_parse_json(rest, frames)
                        self._buffer = ""
                    break
                payload = rest[:expected_len]
                self._buffer = rest[expected_len:]
                self._try_parse_json(payload, frames)
            else:
                self._try_parse_json(stripped, frames)
                self._buffer = ""
        return frames

    @staticmethod
    def _try_parse_json(text: str, out: list[Any]) -> None:
        cleaned = text.strip()
        if not cleaned:
            return
        try:
            out.append(json_loads(cleaned))
        except ValueError as error:
            raise GeminiCompatibilityError(
                "Gemini WebChannel frame payload is not valid JSON"
            ) from error


def extract_handshake_sid(frames: list[Any]) -> str | None:
    """Extract WebChannel SID from initial handshake response frames."""
    for batch in frames:
        if not isinstance(batch, list):
            continue
        for entry in batch:
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            payload = entry[1]
            if isinstance(payload, list) and payload and payload[0] == "c" and len(payload) >= 2:
                return str(payload[1])
            if entry[0] == "c" and isinstance(entry[1], str):
                return str(entry[1])
    return None


def iter_protobuf_packets(frame_batch: Any) -> list[bytes]:
    """Decode base64 protobuf packets from a WebChannel batch array."""
    packets: list[bytes] = []
    if not isinstance(frame_batch, list):
        return packets
    for entry in frame_batch:
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        payload = entry[1]
        if not isinstance(payload, list) or not payload:
            continue
        first = payload[0]
        if not isinstance(first, str) or first in ("c", "noop", "stop"):
            continue
        try:
            packets.append(base64.b64decode(first))
        except ValueError:
            continue
    return packets


def initial_rid() -> int:
    return random.randint(10000, 89999)


def new_gsessionid() -> str:
    return uuid.uuid4().hex[:16]


class ProcessSessionFrameProcessor:
    """Shared processSession protobuf frame reducer for sync and async Gemini streams."""

    def __init__(self, request: ProviderRequest, metadata: list[Any], timing: Any) -> None:
        self.request = request
        self.metadata = list(metadata)
        self.timing = timing
        self.completed = False
        self.last_text = ""
        self.last_thoughts = ""
        self.response_opened: int | None = None
        self.first_event = False
        self.first_assistant_text = False
        self.last_event_at: int | None = None

    def feed_packet(self, packet: bytes):
        event_at = time.perf_counter_ns() if self.timing is not None else None
        if self.timing is not None:
            self.last_event_at = event_at
            if not self.first_event and self.response_opened is not None and event_at is not None:
                self.first_event = True
                self.timing.record("sse_first_event_ms", event_at - self.response_opened)
                self.timing.record(
                    "button_to_first_sse_event_ms", event_at - self.timing.started_ns
                )
        try:
            view = ProtoView(packet)
        except ValueError as error:
            raise GeminiCompatibilityError(
                "Gemini processSession protobuf decode failed"
            ) from error

        cid = view.str_at(2, 7, 1, 4, 4, 2) or self.request.conversation_id
        rid = view.str_at(2, 7, 1, 2, 1) or view.str_at(2, 7, 1, 4, 4, 18)
        turn_token = view.str_at(2, 7, 1, 4, 4, 26)
        title = view.str_at(2, 7, 1, 4, 4, 11)

        if cid:
            self._ensure_metadata_len(1)
            self.metadata[0] = cid
        if rid:
            self._ensure_metadata_len(2)
            self.metadata[1] = rid
        if turn_token:
            self._ensure_metadata_len(10)
            self.metadata[9] = turn_token
            self.completed = True

        block = view.at(2, 7, 1, 4)
        text = ""
        thoughts = ""
        files: list[dict[str, Any]] = extract_proto_image_files(view)
        sources: list[dict[str, Any]] = []

        if block is not None:
            # Candidate ID (rc_...) & text/thoughts extraction from protobuf fields
            rc_id = block.str_at(4, 1) or block.get_str(1)
            if rc_id and rc_id.startswith("rc_"):
                self._ensure_metadata_len(3)
                self.metadata[2] = rc_id
            for cand_msg in block.get_msgs(4):
                # Thoughts or text segments inside repeated/nested fields
                for t_str in (cand_msg.get_str(2), cand_msg.str_at(1, 1)):
                    if t_str and not t_str.startswith(("c_", "r_", "rc_")):
                        text = t_str
                thought_str = cand_msg.str_at(37, 1) or cand_msg.get_str(37)
                if thought_str:
                    thoughts = thought_str
                status_val = cand_msg.get_int(8)
                if status_val == 2:
                    self.completed = True

            # Fallback: direct text on block (field 1 or 2) if not already found
            if not text:
                for candidate_field in (2, 1, 3):
                    val = block.get_str(candidate_field)
                    if val and not val.startswith(("c_", "r_", "rc_")):
                        text = val
                        break

        if (
            self.timing is not None
            and not self.first_assistant_text
            and text.strip()
            and self.response_opened is not None
            and event_at is not None
        ):
            self.first_assistant_text = True
            self.timing.record("sse_first_assistant_text_ms", event_at - self.response_opened)
            self.timing.record(
                "button_to_first_assistant_text_ms", event_at - self.timing.started_ns
            )

        if text:
            self.last_text = text
        if thoughts:
            self.last_thoughts = thoughts

        if cid or text or thoughts or turn_token or title:
            yield {
                "cid": cid,
                "metadata": list(self.metadata),
                "text": self.last_text,
                "thoughts": self.last_thoughts,
                "files": files,
                "sources": sources,
                "title": title,
                "context_only": not (text or thoughts or files),
            }

    def _ensure_metadata_len(self, size: int) -> None:
        if len(self.metadata) < size:
            self.metadata.extend([None] * (size - len(self.metadata)))

    def verify_complete(self) -> None:
        if (
            self.completed
            and self.last_event_at is not None
            and self.response_opened is not None
            and self.timing is not None
        ):
            self.timing.record("sse_last_event_ms", self.last_event_at - self.response_opened)
            self.timing.record(
                "button_to_last_sse_event_ms", self.last_event_at - self.timing.started_ns
            )
        if not self.completed and not self.last_text:
            raise GeminiStreamError(
                "Gemini processSession stream ended before completion; the request was not retried"
            )
