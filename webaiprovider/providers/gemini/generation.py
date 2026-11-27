"""Bounded Gemini generation protocol, shared stream processors, and blocking uploads."""

from __future__ import annotations

import mimetypes
import os
import stat
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ...core.file_stat import same_file_version
from ...core.timing import _STREAM_PAUSED, _StreamPause, current_send_timing
from ...resources.message.attachment import GEMINI_ATTACHMENTS
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.response.artifact import files_from
from ...resources.response.source import sources_from
from ...transport.http import HTTPTransport, _message_stream, _wait_for_stream_head
from .errors import (
    GeminiError,
    GeminiStreamError,
)
from .media import GEMINI_FILE_RULE, resolve_image_links
from .webchannel import (
    PROCESS_SESSION_PATH,
    ProcessSessionFrameProcessor,
    WebChannelFrameDecoder,
    WebChannelSessionState,
    build_process_session_headers,
    build_process_session_turn_data,
    extract_handshake_sid,
    initial_rid,
    iter_protobuf_packets,
    new_gsessionid,
)


def _gemini_upload_spec(value):
    item = value if isinstance(value, dict) else {"path": value}
    content, raw_path = item.get("bytes"), item.get("path")
    if content is not None:
        filename = item.get("name") or (
            Path(raw_path).name if raw_path is not None else "attachment"
        )
        size, local_path = len(content), None
    elif raw_path is not None:
        resolved = Path(raw_path).expanduser().resolve(strict=True)
        try:
            with resolved.open("rb") as handle:
                info = os.fstat(handle.fileno())
        except OSError:
            raise ValueError("File was not found") from None
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("File was not found")
        filename = item.get("name") or resolved.name
        size, local_path = info.st_size, str(resolved)
    else:
        raise ValueError("Gemini attachment requires a file path or byte content")

    content_type = (
        item.get("mime_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    )
    limit = (
        GEMINI_ATTACHMENTS.max_video_bytes
        if content_type.startswith("video/") and GEMINI_ATTACHMENTS.max_video_bytes is not None
        else GEMINI_ATTACHMENTS.max_bytes
    )
    if not 0 < size <= limit:
        raise ValueError("Gemini file is empty or exceeds the size limit")
    return filename, content_type, size, content, local_path


def _prepare_upload_specs(items):
    if len(items) > GEMINI_ATTACHMENTS.max_files:
        raise ValueError("Too many Gemini attachments")
    prepared = []
    for value in items:
        spec = _gemini_upload_spec(value)
        before = os.stat(spec[4]) if spec[4] is not None else None
        prepared.append((spec, before))
    return prepared


def upload_files(client, items):
    return [
        [[reference], filename] for reference, filename, _ in upload_file_references(client, items)
    ]


def upload_file_references(client, items):
    from curl_cffi import CurlMime

    specs = _prepare_upload_specs(items)
    results = []
    for spec, before in specs:
        filename, content_type, _size, content, local_path = spec
        mime = CurlMime()
        try:
            mime.addpart(
                name="file",
                content_type=content_type,
                filename=filename,
                **({"data": content} if content is not None else {"local_path": local_path}),
            )
            response = client.upload_transport.send(
                "POST",
                "/upload",
                headers={
                    "Origin": "https://gemini.google.com",
                    "Referer": "https://gemini.google.com/",
                    "X-Tenant-Id": "bard-storage",
                    "Push-ID": client.push_id,
                },
                multipart=mime,
                impersonate="chrome",
                discard_cookies=True,
                allow_redirects=False,
            )
            if local_path is not None and before is not None:
                after = os.stat(local_path)
                if not same_file_version(before, after):
                    raise GeminiError("File changed while it was being uploaded")
            results.append((response.text, filename, content_type))
        finally:
            mime.close()
    return results


def _ensure_webchannel_session(client: Any, *, temporary: bool = False) -> WebChannelSessionState:
    state = getattr(client, "_webchannel_session", None)
    if state is not None:
        return state
    transport = getattr(client, "webchannel_transport", None) or client.transport
    gsessionid = new_gsessionid()
    rid = initial_rid()
    headers = build_process_session_headers(
        getattr(client, "_cookies", {}),
        temporary=temporary,
        gsessionid=gsessionid,
    )
    resp = transport.send(
        "POST",
        PROCESS_SESSION_PATH,
        params={"VER": "8", "RID": str(rid), "CVER": "22", "X-HTTP-Session-Id": gsessionid},
        headers=headers,
        data="",
        allow_redirects=False,
    )
    HTTPTransport.check(resp)
    decoder = WebChannelFrameDecoder()
    frames = decoder.feed(resp.content or resp.text.encode("utf-8"), final=True)
    sid = extract_handshake_sid(frames)
    if not sid:
        raise GeminiStreamError("Gemini processSession handshake did not return a session ID")
    state = WebChannelSessionState(gsessionid=gsessionid, sid=sid, rid=rid)
    client._webchannel_session = state
    return state


class _GeminiResponseProcessor:
    """Shared response event reducer for sync and async Gemini streams."""

    def __init__(
        self, request: ProviderRequest, metadata: list[Any], media: dict, remember
    ) -> None:
        self.request = request
        self.metadata = metadata
        self.media = media
        self.remember = remember
        self.bound = bool(request.conversation_id)
        self.visible = False
        self.cid = request.conversation_id
        self.final_rid: str | None = None
        self.raw_files: list[dict] = []
        self.seen_file_keys: set[tuple[str, str]] = set()
        self.raw_sources: list[dict] = []
        self.seen_source_keys: set[str] = set()
        self.rendered_by_channel = {"analysis": "", "final": ""}

    def feed_chunk(self, chunk: dict[str, Any]):
        self.cid = chunk.get("cid") or self.cid
        self.metadata = chunk["metadata"]
        if self.cid and not self.bound and self.cid != self.request.conversation_id:
            yield ConversationBound(self.cid)
            self.bound = True
        if chunk.get("context_only"):
            return
        images = chunk["files"]
        for image in images:
            key = (str(image["alias"]), str(image["url"]))
            if key not in self.seen_file_keys:
                self.seen_file_keys.add(key)
                self.raw_files.append(
                    {
                        "id": image["alias"],
                        "name": image["name"],
                        "remote_url": image["url"],
                        "media_type": image.get("media_type"),
                    }
                )
            self.media[self.cid, image["alias"]] = image
            self.media[self.cid, image["url"]] = image
        while len(self.media) > 256:
            self.media.pop(next(iter(self.media)))
        chunk_sources = chunk.get("sources")
        if chunk_sources:
            for item in chunk_sources:
                if not isinstance(item, dict):
                    continue
                source_key = item.get("url") or item.get("title")
                if source_key:
                    if source_key not in self.seen_source_keys:
                        self.seen_source_keys.add(source_key)
                        self.raw_sources.append(item)
                elif item not in self.raw_sources:
                    self.raw_sources.append(item)
        self.visible = self.visible or bool(chunk["text"] or images)
        for channel, content_text in (
            ("analysis", chunk["thoughts"]),
            ("final", resolve_image_links(chunk["text"], images)),
        ):
            rendered = self.rendered_by_channel[channel]
            if not content_text or content_text == rendered:
                continue
            if content_text.startswith(rendered):
                yield TextDelta(content_text[len(rendered) :], channel, channel)
            else:
                yield TextReplacement(content_text, channel, channel)
            self.rendered_by_channel[channel] = content_text
        rid = self.metadata[1] if len(self.metadata) > 1 else None
        if rid:
            self.final_rid = rid

    def finish(self):
        if not self.visible or not self.cid:
            raise GeminiStreamError("Gemini response did not complete; the request was not retried")
        yield ResponseComplete()
        if self.final_rid:
            yield MessageMetadata(
                message_id=self.final_rid + ":model",
                user_message_id=self.final_rid + ":user",
                model=self.request.model,
                effort=self.request.effort,
                sources=sources_from({"sources": self.raw_sources}) if self.raw_sources else (),
                files=files_from({"files": self.raw_files}, rule=GEMINI_FILE_RULE)
                if self.raw_files
                else (),
            )
        self.remember(self.cid, self.metadata, temporary=self.request.temporary)


def generate(
    client,
    request,
    metadata,
    notebook=None,
    *,
    files=None,
    request_timeout=(15, 310),
    deadline: float | None = None,
    tool_fields=None,
    raw_override_fields=None,
) -> Iterator[dict | _StreamPause]:
    from curl_cffi.const import CurlECode
    from curl_cffi.requests.exceptions import RequestException

    if files is None:
        files = upload_files(client, request.attachments)
    headers, form = build_process_session_turn_data(
        client,
        request,
        metadata,
        files,
        notebook,
        tool_fields=tool_fields,
        raw_override_fields=raw_override_fields,
    )
    transport = getattr(client, "webchannel_transport", None) or client.transport
    decoder = WebChannelFrameDecoder()
    timing = current_send_timing()
    sse_connect_started = time.perf_counter_ns() if timing is not None else None
    processor = ProcessSessionFrameProcessor(request, metadata, timing)
    remaining: float | None = None
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise GeminiStreamError("Gemini request exceeded the time limit before submission")
    try:
        wc_state = None
        for attempt in range(2):
            wc_state = _ensure_webchannel_session(client, temporary=request.temporary)
            submit_params = {
                "VER": "8",
                "gsessionid": wc_state.gsessionid,
                "SID": wc_state.sid,
                "RID": str(wc_state.next_rid()),
                "AID": str(wc_state.aid),
                "t": "1",
            }
            submit_resp = transport.send(
                "POST",
                PROCESS_SESSION_PATH,
                params=submit_params,
                headers=headers,
                data=form,
                allow_redirects=False,
                timeout=request_timeout,
            )
            if submit_resp.status_code in (400, 404, 410) and attempt == 0:
                client._webchannel_session = None
                continue
            HTTPTransport.check(submit_resp)
            break
        assert wc_state is not None
        stream_options: dict[str, Any] = {
            "params": {
                "gsessionid": wc_state.gsessionid,
                "VER": "8",
                "RID": "rpc",
                "SID": wc_state.sid,
                "AID": str(wc_state.aid),
                "CI": "0",
                "TYPE": "xmlhttp",
            },
            "headers": headers,
            "allow_redirects": False,
            "timeout": request_timeout,
        }
        if remaining is not None and isinstance(transport, HTTPTransport):
            stream_options["total_timeout"] = remaining
        with _message_stream(
            transport,
            "GET",
            PROCESS_SESSION_PATH,
            **stream_options,
        ) as response:
            yield from _wait_for_stream_head(response)
            processor.response_opened = time.perf_counter_ns() if timing is not None else None
            if sse_connect_started is not None and processor.response_opened is not None:
                timing.record("sse_connect_ms", processor.response_opened - sse_connect_started)
            for chunk in response.iter_bytes():
                if chunk is _STREAM_PAUSED:
                    yield chunk
                    continue
                for batch in decoder.feed(chunk):
                    wc_state.next_aid()
                    for packet in iter_protobuf_packets(batch):
                        yield from processor.feed_packet(packet)
            for batch in decoder.feed(b"", final=True):
                wc_state.next_aid()
                for packet in iter_protobuf_packets(batch):
                    yield from processor.feed_packet(packet)
    except RequestException as error:
        if error.code != CurlECode.OPERATION_TIMEDOUT:
            raise
        raise GeminiStreamError(
            "The Gemini request timed out; it was not retried automatically "
            "and conversation history was preserved."
        ) from None
    processor.verify_complete()
    client.persist(force=False)


def response_events(
    request: ProviderRequest,
    *,
    prepare,
    request_timeout,
    media,
    remember,
    deadline: float | None = None,
    tool_fields=None,
) -> Iterator[ProviderStreamEvent]:
    client, request, metadata, context, action_files = prepare(request)
    reducer = _GeminiResponseProcessor(request, metadata, media, remember)
    for chunk in generate(
        client,
        request,
        metadata,
        context,
        files=action_files,
        request_timeout=request_timeout,
        deadline=deadline,
        tool_fields=tool_fields,
    ):
        if chunk is _STREAM_PAUSED:
            yield chunk
        else:
            yield from reducer.feed_chunk(chunk)
    yield from reducer.finish()
