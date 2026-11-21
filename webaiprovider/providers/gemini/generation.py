"""Bounded Gemini generation protocol and streaming disk uploads."""

import asyncio
import json
import mimetypes
import os
import stat
import time
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

from ...core.concurrency import gather_bounded
from ...core.file_stat import same_file_version
from ...core.json import json_loads
from ...core.timing import current_send_timing
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
from ...resources.message.types import MessageAction
from ...resources.response.artifact import files_from
from ...resources.response.source import sources_from
from ...transport.frames import JSPBFrameDecoder
from .errors import (
    GeminiCompatibilityError,
    GeminiError,
    GeminiGenerationRejected,
    GeminiStreamError,
)
from .history import candidate_metadata, published_activity, published_thoughts
from .media import GEMINI_FILE_RULE, candidate_files, resolve_image_links
from .rpc import rpc_value
from .session import BASE, HEADERS


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


async def upload_files(session, items):
    return [
        [[reference], filename]
        for reference, filename, _ in await upload_file_references(session, items)
    ]


async def upload_file_references(session, items):
    from curl_cffi import CurlMime

    if len(items) > GEMINI_ATTACHMENTS.max_files:
        raise ValueError("Too many Gemini attachments")

    def _prepare_specs():
        prepared = []
        for value in items:
            spec = _gemini_upload_spec(value)
            before = os.stat(spec[4]) if spec[4] is not None else None
            prepared.append((spec, before))
        return prepared

    specs = await asyncio.to_thread(_prepare_specs)

    async def upload_one(entry):
        spec, before = entry
        filename, content_type, _size, content, local_path = spec
        mime = CurlMime()
        try:
            mime.addpart(
                name="file",
                content_type=content_type,
                filename=filename,
                **({"data": content} if content is not None else {"local_path": local_path}),
            )
            response = await session.upload_transport.send(
                "POST",
                "/upload",
                headers={
                    "Origin": "https://gemini.google.com",
                    "Referer": "https://gemini.google.com/",
                    "X-Tenant-Id": "bard-storage",
                    "Push-ID": session.push_id,
                },
                multipart=mime,
                impersonate="chrome",
                discard_cookies=True,
                allow_redirects=False,
            )
            if local_path is not None and before is not None:
                after = await asyncio.to_thread(os.stat, local_path)
                if not same_file_version(before, after):
                    raise GeminiError("File changed while it was being uploaded")
            return response.text, filename, content_type
        finally:
            mime.close()

    return await gather_bounded(specs, upload_one, limit=2)


def generation_request(session, request, metadata, files, notebook=None):
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
    info = selected.metadata
    number = info.get("number")
    selector_id = info.get("selector_id", info.get("model_id"))
    body = [None] * 81
    body[0], body[1], body[2] = (
        [request.prompt, 0, None, files or None, None, None, 0],
        [session.language],
        metadata,
    )
    for index, value in {
        6: [1],
        7: 1,
        10: 1,
        11: 0,
        17: [[0]],
        18: 0,
        27: 1,
        30: [4],
        41: [1],
        53: 0,
        61: [],
        68: 1,
        79: number,
        80: thinking,
    }.items():
        body[index] = value
    if request.action is not None:
        body.extend([None] * 18)
        retry = request.action == MessageAction.RETRY
        body[6], body[17], body[18], body[32] = [0], None, 2 if retry else 0, 1
        body[55], body[61], body[72] = [], [None, None, None, 0], 7 if retry else 2
        body[91], body[96], body[98] = 1, 0, 1
    if request.temporary:
        body[45] = 1
    if notebook:
        body[19] = notebook[0]
        body[40] = [None] * 14
        body[40][13] = notebook[1]
    identifier = str(uuid.uuid4()).upper()
    body[59] = identifier
    headers = {**HEADERS, "x-goog-ext-525005358-jspb": json.dumps([identifier, 1])}
    selector = [
        1,
        None,
        None,
        None,
        selector_id,
        None,
        None,
        int(request.temporary),
        [4, 5, 6, 8],
        None,
        None,
    ]
    selector.extend([None] * (info["capacity_slot"] - 1 - len(selector)))
    selector.extend([info["capacity"], None, None, number, thinking, session.session_id])
    headers.update(
        {
            "x-goog-ext-525001261-jspb": json.dumps(selector),
            "x-goog-ext-73010989-jspb": "[0]",
            "x-goog-ext-73010990-jspb": "[0,0,0]",
        }
    )
    body[79] = number
    return headers, {"at": session.token, "f.req": json.dumps([None, json.dumps(body)])}


async def generate(
    session, request, metadata, notebook=None, *, files=None, request_timeout=(15, 310)
) -> AsyncGenerator[dict, None]:
    from curl_cffi.const import CurlECode
    from curl_cffi.requests.exceptions import RequestException

    if files is None:
        files = await upload_files(session, request.attachments)
    headers, data = generation_request(session, request, metadata, files, notebook)
    frames = JSPBFrameDecoder(prefix=")]}'", length_includes_separator=True)
    completed = False
    last_text = last_thoughts = ""
    timing = current_send_timing()
    sse_connect_started = time.perf_counter_ns() if timing is not None else None
    response_opened = None
    first_event = False
    first_assistant_text = False
    last_event_at = None
    try:
        async with session.transport.stream(
            "POST",
            "/" + (BASE + "assistant.lamda.BardFrontendService/StreamGenerate").split("/", 3)[3],
            params=session.params(),
            headers=headers,
            data=data,
            allow_redirects=False,
            timeout=request_timeout,
        ) as response:
            response_opened = time.perf_counter_ns() if timing is not None else None
            if sse_connect_started is not None:
                timing.record("sse_connect_ms", response_opened - sse_connect_started)

            async def parts():
                async for chunk in response.aiter_bytes():
                    for packet in frames.feed(chunk):
                        for frame in packet if isinstance(packet, list) else (packet,):
                            yield frame
                for packet in frames.feed(b"", final=True):
                    for frame in packet if isinstance(packet, list) else (packet,):
                        yield frame

            async for frame in parts():
                event_at = time.perf_counter_ns() if timing is not None else None
                if timing is not None:
                    last_event_at = event_at
                    if not first_event:
                        first_event = True
                        timing.record("sse_first_event_ms", event_at - response_opened)
                        timing.record("button_to_first_sse_event_ms", event_at - timing.started_ns)
                if rpc_value(frame, 5, 2, 0, 1, 0):
                    raise GeminiGenerationRejected("Gemini rejected the generation request")
                payload = rpc_value(frame, 2)
                if not isinstance(payload, str):
                    continue
                try:
                    body = json_loads(payload)
                except ValueError as error:
                    raise GeminiCompatibilityError(
                        "Gemini response body format has changed"
                    ) from error
                returned = rpc_value(body, 1)
                if isinstance(returned, list) and returned:
                    cand_id = metadata[2] if len(metadata) > 2 else None
                    metadata = list(returned)
                    if cand_id and len(metadata) >= 2:
                        metadata.extend([None] * max(0, 3 - len(metadata)))
                        metadata[2] = cand_id
                    if rpc_value(metadata, 0) and not rpc_value(body, 4):
                        yield {
                            "cid": metadata[0],
                            "metadata": metadata,
                            "context_only": True,
                        }
                context = rpc_value(body, 25)
                if isinstance(context, str):
                    completed = True
                for candidate in rpc_value(body, 4, default=[]):
                    if not rpc_value(candidate, 0):
                        continue
                    cid = rpc_value(metadata, 0) or request.conversation_id
                    current = list(metadata)
                    current.extend([None] * max(0, 3 - len(current)))
                    current[2] = candidate[0]
                    if len(current) >= 3 and current[2]:
                        metadata = list(current)
                    response_complete = rpc_value(candidate, 8, 0) == 2
                    completed = completed or response_complete
                    text = rpc_value(candidate, 1, 0)
                    if isinstance(text, str) and text.startswith(
                        (
                            "http://googleusercontent.com/card_content/",
                            "https://googleusercontent.com/card_content/",
                        )
                    ):
                        text = rpc_value(candidate, 22, 0) or text
                    if (
                        timing is not None
                        and not first_assistant_text
                        and isinstance(text, str)
                        and text.strip()
                    ):
                        first_assistant_text = True
                        timing.record("sse_first_assistant_text_ms", event_at - response_opened)
                        timing.record(
                            "button_to_first_assistant_text_ms", event_at - timing.started_ns
                        )
                    last_text = text or last_text
                    activity = published_activity(candidate)
                    last_thoughts = published_thoughts(candidate, activity[0], last_thoughts)
                    yield {
                        "cid": cid,
                        "metadata": current,
                        "text": last_text,
                        "thoughts": last_thoughts,
                        "files": candidate_files(candidate),
                        "sources": candidate_metadata(candidate, activity)["sources"],
                        "response_complete": response_complete,
                    }
                if isinstance(context, str):
                    metadata = [None] * 9 + [context]
                    yield {
                        "cid": request.conversation_id,
                        "metadata": metadata,
                        "context_only": True,
                    }
    except RequestException as error:
        if error.code != CurlECode.OPERATION_TIMEDOUT:
            raise
        raise GeminiStreamError(
            "The Gemini request timed out; it was not retried automatically "
            "and conversation history was preserved."
        ) from None
    if completed and last_event_at is not None and response_opened is not None:
        timing.record("sse_last_event_ms", last_event_at - response_opened)
        timing.record("button_to_last_sse_event_ms", last_event_at - timing.started_ns)
    if not completed:
        raise GeminiStreamError(
            "Gemini stream ended before completion; the request was not retried"
        )
    await session.persist(force=False)


async def response_events(
    request: ProviderRequest, *, prepare, request_timeout, media, remember
) -> AsyncGenerator[ProviderStreamEvent, None]:
    client, request, metadata, context, action_files = await prepare(request)
    bound, visible, cid = bool(request.conversation_id), False, request.conversation_id
    final_rid = None
    raw_files: list[dict] = []
    seen_file_keys: set[tuple[str, str]] = set()
    raw_sources: list[dict] = []
    seen_source_keys: set[str] = set()
    rendered_by_channel = {"analysis": "", "final": ""}
    async for chunk in generate(
        client,
        request,
        metadata,
        context,
        files=action_files,
        request_timeout=request_timeout,
    ):
        cid = chunk.get("cid") or cid
        metadata = chunk["metadata"]
        if cid and not bound and cid != request.conversation_id:
            yield ConversationBound(cid)
            bound = True
        if chunk.get("context_only"):
            continue
        images = chunk["files"]
        for image in images:
            key = (str(image["alias"]), str(image["url"]))
            if key not in seen_file_keys:
                seen_file_keys.add(key)
                raw_files.append(
                    {
                        "id": image["alias"],
                        "name": image["name"],
                        "remote_url": image["url"],
                        "media_type": image.get("media_type"),
                    }
                )
            media[cid, image["alias"]] = image
            media[cid, image["url"]] = image
        while len(media) > 256:
            media.pop(next(iter(media)))
        chunk_sources = chunk.get("sources")
        if chunk_sources:
            for item in chunk_sources:
                if not isinstance(item, dict):
                    continue
                source_key = item.get("url") or item.get("title")
                if source_key:
                    if source_key not in seen_source_keys:
                        seen_source_keys.add(source_key)
                        raw_sources.append(item)
                elif item not in raw_sources:
                    raw_sources.append(item)
        visible = visible or bool(chunk["text"] or images)
        for channel, content_text in (
            ("analysis", chunk["thoughts"]),
            ("final", resolve_image_links(chunk["text"], images)),
        ):
            rendered = rendered_by_channel[channel]
            if not content_text or content_text == rendered:
                continue
            if content_text.startswith(rendered):
                yield TextDelta(content_text[len(rendered) :], channel)
            else:
                yield TextReplacement(content_text, channel)
            rendered_by_channel[channel] = content_text
        if chunk.get("response_complete"):
            yield ResponseComplete()
        rid = metadata[1] if len(metadata) > 1 else None
        if rid:
            final_rid = rid
    if not visible or not cid:
        raise GeminiStreamError("Gemini response did not complete; the request was not retried")
    if final_rid:
        yield MessageMetadata(
            message_id=final_rid + ":model",
            user_message_id=final_rid + ":user",
            model=request.model,
            effort=request.effort,
            sources=sources_from({"sources": raw_sources}) if raw_sources else (),
            files=files_from({"files": raw_files}, rule=GEMINI_FILE_RULE) if raw_files else (),
        )
    remember(cid, metadata, temporary=request.temporary)
