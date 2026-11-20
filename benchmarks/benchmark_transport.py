"""Shared HTTP read paths and byte-oriented stream framing, with no sockets."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager, contextmanager
from functools import partial
from types import SimpleNamespace

from webaiprovider import Account, Provider
from webaiprovider.transport import AsyncHTTPTransport, HTTPTransport
from webaiprovider.transport.frames import JSPBFrameDecoder, SSEFrameDecoder

from ._runner import Case, async_case, time_sync

_ORIGIN = "https://benchmark.invalid"
_CHUNK_BYTES = 16 * 1024
_BODY_CHUNKS = 512
_STREAM_CHUNKS = 4_000


def _body(count: int):
    for _ in range(count):
        yield bytes(_CHUNK_BYTES)


async def _async_body(count: int, size: int):
    for _ in range(count):
        yield bytes(size)


def _sync_transport(count: int) -> HTTPTransport:
    @contextmanager
    def stream(*args, **options):
        yield SimpleNamespace(status_code=200, headers={}, iter_content=partial(_body, count))

    client = SimpleNamespace(stream=stream)
    return HTTPTransport(_ORIGIN, client_getter=lambda: client)


def _async_transport(count: int, size: int, *, origin: str = _ORIGIN) -> AsyncHTTPTransport:
    @asynccontextmanager
    async def stream(*args, **options):
        yield SimpleNamespace(
            status_code=200, headers={}, aiter_content=partial(_async_body, count, size)
        )

    client = SimpleNamespace(stream=stream)
    return AsyncHTTPTransport(origin, client_getter=lambda: client)


def _buffered(count: int) -> tuple[Case, Case]:
    sync = _sync_transport(count)
    asynchronous = _async_transport(count, _CHUNK_BYTES)
    size = count * _CHUNK_BYTES
    label = "16k" if count == 1 else "8m"

    def receive():
        if len(sync.send("GET", "/fixture").content) != size:
            raise AssertionError("Unexpected buffered response size")

    async def receive_async():
        if len((await asynchronous.send("GET", "/fixture")).content) != size:
            raise AssertionError("Unexpected buffered response size")

    return (
        Case(
            f"transport.buffer.sync.{label}",
            partial(time_sync, operation=receive),
            {"body_bytes": size, "chunks": count},
        ),
        async_case(f"transport.buffer.async.{label}", receive_async, body_bytes=size, chunks=count),
    )


def _provider_stream(loops: int) -> float:
    transport = _async_transport(_STREAM_CHUNKS, 64, origin="https://claude.ai")

    async def offline_session():
        pass

    provider = Provider(Account.from_cookies("claude", {}), transport=transport)
    provider._provider._prepare_session = offline_session
    provider._provider._close_session = offline_session

    def consume():
        with provider.transport.stream("GET", "/fixture") as response:
            if sum(map(len, response.iter_bytes())) != _STREAM_CHUNKS * 64:
                raise AssertionError("Unexpected stream size")

    with provider:
        return time_sync(loops, consume)


def _frame_case(*, jspb: bool) -> Case:
    count = 2_000
    if jspb:
        payload = json.dumps([["fixture", "Merhaba 🚀"]], ensure_ascii=False)
        units = len(("\n" + payload).encode("utf-16-le")) // 2
        wire = (")]}'\n\n" + f"{units}\n{payload}\n" * count).encode()
    else:
        wire = b'data: {"delta":"fixture"}\r\n\r\n' * count
    chunks = tuple(wire[index : index + 257] for index in range(0, len(wire), 257))

    def decode():
        if jspb:
            decoder = JSPBFrameDecoder(prefix=")]}'", length_includes_separator=True)
            frames = sum(1 for chunk in chunks for _ in decoder.feed(chunk))
            frames += sum(1 for _ in decoder.feed(b"", final=True))
        else:
            frames = sum(1 for _ in SSEFrameDecoder().decode(chunks))
        if frames != count:
            raise AssertionError("Unexpected decoded frame count")

    return Case(
        f"transport.frames.{'jspb' if jspb else 'sse'}.2000",
        partial(time_sync, operation=decode),
        {"frames": count, "chunk_bytes": 257},
    )


def cases() -> tuple[Case, ...]:
    return (
        *_buffered(1),
        *_buffered(_BODY_CHUNKS),
        Case("transport.provider.stream.4000", _provider_stream, {"chunks": _STREAM_CHUNKS}),
        _frame_case(jspb=False),
        _frame_case(jspb=True),
    )
