"""Minimal pure-Python Protobuf wire encoder and decoder for Gemini processSession."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

WIRE_VARINT = 0
WIRE_FIXED64 = 1
WIRE_BYTES = 2
WIRE_FIXED32 = 5


def encode_varint(value: int) -> bytes:
    """Encode a non-negative (or 64-bit signed) integer as a Protobuf base-128 varint."""
    if value < 0:
        value &= (1 << 64) - 1
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def encode_field(field_number: int, wire_type: int, payload: bytes) -> bytes:
    """Encode a single Protobuf tag + payload."""
    tag = encode_varint((field_number << 3) | wire_type)
    if wire_type == WIRE_BYTES:
        return tag + encode_varint(len(payload)) + payload
    return tag + payload


def encode_message(fields: Mapping[int, Any] | Sequence[tuple[int, Any]]) -> bytes:
    """Encode a mapping or sequence of (field_number, value) pairs into Protobuf wire bytes."""
    items = fields.items() if isinstance(fields, Mapping) else fields
    out = bytearray()
    for field_number, value in sorted(items, key=lambda pair: pair[0]):
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                out.extend(_encode_single(field_number, item))
        else:
            out.extend(_encode_single(field_number, value))
    return bytes(out)


def _encode_single(field_number: int, value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bool):
        return encode_field(field_number, WIRE_VARINT, encode_varint(int(value)))
    if isinstance(value, int):
        return encode_field(field_number, WIRE_VARINT, encode_varint(value))
    if isinstance(value, bytes):
        return encode_field(field_number, WIRE_BYTES, value)
    if isinstance(value, str):
        return encode_field(field_number, WIRE_BYTES, value.encode("utf-8"))
    if isinstance(value, Mapping):
        return encode_field(field_number, WIRE_BYTES, encode_message(value))
    if isinstance(value, ProtoMessage):
        return encode_field(field_number, WIRE_BYTES, value.to_bytes())
    raise TypeError(f"Unsupported protobuf value type for field {field_number}: {type(value)!r}")


@dataclass(frozen=True, slots=True)
class ProtoMessage:
    """Explicit ordered container for Protobuf fields."""

    fields: tuple[tuple[int, Any], ...]

    def to_bytes(self) -> bytes:
        return encode_message(self.fields)


def decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Decode a varint starting at offset, returning (value, next_offset)."""
    result = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, offset
        shift += 7
        if shift > 70:
            raise ValueError("Protobuf varint is too long")
    raise ValueError("Truncated protobuf varint")


def decode_fields(data: bytes) -> dict[int, list[tuple[int, Any]]]:
    """Decode a Protobuf message into {field_number: [(wire_type, raw_value), ...]}."""
    parsed: dict[int, list[tuple[int, Any]]] = {}
    offset = 0
    length = len(data)
    while offset < length:
        tag, offset = decode_varint(data, offset)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if wire_type == WIRE_VARINT:
            val, offset = decode_varint(data, offset)
        elif wire_type == WIRE_FIXED64:
            if offset + 8 > length:
                raise ValueError("Truncated fixed64 field")
            val = int.from_bytes(data[offset : offset + 8], "little")
            offset += 8
        elif wire_type == WIRE_BYTES:
            size, offset = decode_varint(data, offset)
            if offset + size > length:
                raise ValueError("Truncated length-delimited field")
            val = data[offset : offset + size]
            offset += size
        elif wire_type == WIRE_FIXED32:
            if offset + 4 > length:
                raise ValueError("Truncated fixed32 field")
            val = int.from_bytes(data[offset : offset + 4], "little")
            offset += 4
        else:
            raise ValueError(f"Unsupported protobuf wire type {wire_type}")
        parsed.setdefault(field_number, []).append((wire_type, val))
    return parsed


class ProtoView:
    """Lightweight read-only view over decoded Protobuf fields."""

    __slots__ = ("_raw", "_fields")

    def __init__(self, data: bytes) -> None:
        self._raw = data
        self._fields = decode_fields(data)

    def has(self, field_number: int) -> bool:
        return field_number in self._fields

    def all_raw(self, field_number: int) -> list[Any]:
        return [val for _, val in self._fields.get(field_number, ())]

    def get_int(self, field_number: int) -> int | None:
        for wire_type, val in self._fields.get(field_number, ()):
            if wire_type in (WIRE_VARINT, WIRE_FIXED64, WIRE_FIXED32):
                return int(val)
        return None

    def get_bytes(self, field_number: int) -> bytes | None:
        for wire_type, val in self._fields.get(field_number, ()):
            if wire_type == WIRE_BYTES:
                return val
        return None

    def get_str(self, field_number: int) -> str | None:
        raw = self.get_bytes(field_number)
        if raw is None:
            return None
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return None

    def get_msg(self, field_number: int) -> ProtoView | None:
        raw = self.get_bytes(field_number)
        if raw is None:
            return None
        try:
            return ProtoView(raw)
        except ValueError:
            return None

    def get_msgs(self, field_number: int) -> list[ProtoView]:
        out: list[ProtoView] = []
        for wire_type, val in self._fields.get(field_number, ()):
            if wire_type == WIRE_BYTES:
                try:
                    out.append(ProtoView(val))
                except ValueError:
                    continue
        return out

    def at(self, *path: int) -> ProtoView | None:
        cur: ProtoView | None = self
        for part in path:
            if cur is None:
                return None
            cur = cur.get_msg(part)
        return cur

    def str_at(self, *path: int) -> str | None:
        if not path:
            return None
        parent = self.at(*path[:-1])
        return parent.get_str(path[-1]) if parent is not None else None

    def int_at(self, *path: int) -> int | None:
        if not path:
            return None
        parent = self.at(*path[:-1])
        return parent.get_int(path[-1]) if parent is not None else None

    def bytes_at(self, *path: int) -> bytes | None:
        if not path:
            return None
        parent = self.at(*path[:-1])
        return parent.get_bytes(path[-1]) if parent is not None else None
