"""Bounded Gemini generation protocol and streaming disk uploads."""

import json
import mimetypes
import time
import uuid
from pathlib import Path

from ...frames import JSPBFrameDecoder
from ...resources.message.types import MessageAction
from ...utils.attachments import GEMINI_ATTACHMENTS
from ...utils.timing import current_send_timing
from .errors import GeminiCompatibilityError, GeminiError, GeminiStreamError
from .media import candidate_files
from .rpc import nested
from .session import BASE, HEADERS


def _gemini_upload_spec(value):
    if isinstance(value, dict):
        content = value.get("bytes")
        raw_path = value.get("path")
        if content is not None:
            filename = value.get("name") or (
                Path(raw_path).name if raw_path is not None else "attachment"
            )
            content_type = (
                value.get("mime_type")
                or mimetypes.guess_type(filename)[0]
                or "application/octet-stream"
            )
            size = len(content)
            local_path = None
        elif raw_path is not None:
            resolved = Path(raw_path).expanduser().resolve(strict=True)
            if not resolved.is_file():
                raise ValueError("File was not found")
            filename = value.get("name") or resolved.name
            content_type = (
                value.get("mime_type")
                or mimetypes.guess_type(filename)[0]
                or "application/octet-stream"
            )
            size = resolved.stat().st_size
            local_path = str(resolved)
        else:
            raise ValueError("Gemini attachment requires a file path or byte content")
    else:
        resolved = Path(value).expanduser().resolve(strict=True)
        if not resolved.is_file():
            raise ValueError("File was not found")
        filename = resolved.name
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        size = resolved.stat().st_size
        content = None
        local_path = str(resolved)

    limit = (
        GEMINI_ATTACHMENTS.max_video_bytes
        if content_type.startswith("video/") and GEMINI_ATTACHMENTS.max_video_bytes is not None
        else GEMINI_ATTACHMENTS.max_bytes
    )
    if not 0 < size <= limit:
        raise ValueError("Gemini file is empty or exceeds the size limit")
    return filename, content_type, content, local_path


async def upload_files(session, items):
    from curl_cffi import CurlMime

    if len(items) > GEMINI_ATTACHMENTS.max_files:
        raise ValueError("Too many Gemini attachments")
    specs = [_gemini_upload_spec(value) for value in items]

    files = []
    for filename, content_type, content, local_path in specs:
        mime = CurlMime()
        try:
            if content is not None:
                mime.addpart(
                    name="file",
                    content_type=content_type,
                    filename=filename,
                    data=content,
                )
            else:
                mime.addpart(
                    name="file",
                    content_type=content_type,
                    filename=filename,
                    local_path=local_path,
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
                allow_redirects=False,
            )
            files.append([[response.text], filename])
        finally:
            mime.close()
    return files


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
    session, request, metadata, notebook=None, *, files=None, request_timeout=(15, 130)
):
    from curl_cffi.const import CurlECode
    from curl_cffi.requests.exceptions import RequestException

    from .history import candidate_metadata, published_activity, published_thoughts

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
                if nested(frame, 5, 2, 0, 1, 0):
                    raise GeminiError(
                        "Gemini rejected the request; check your quota or model availability."
                    )
                payload = nested(frame, 2)
                if not isinstance(payload, str):
                    continue
                try:
                    body = json.loads(payload)
                except ValueError as error:
                    raise GeminiCompatibilityError(
                        "Gemini response body format has changed"
                    ) from error
                returned = nested(body, 1)
                if isinstance(returned, list) and returned:
                    cand_id = metadata[2] if len(metadata) > 2 else None
                    metadata = list(returned)
                    if cand_id and len(metadata) >= 2:
                        metadata.extend([None] * max(0, 3 - len(metadata)))
                        metadata[2] = cand_id
                    if nested(metadata, 0) and not nested(body, 4):
                        yield {
                            "cid": metadata[0],
                            "metadata": metadata,
                            "context_only": True,
                        }
                context = nested(body, 25)
                if isinstance(context, str):
                    completed = True
                for candidate in nested(body, 4, default=[]):
                    if not nested(candidate, 0):
                        continue
                    cid = nested(metadata, 0) or request.conversation_id
                    current = list(metadata)
                    current.extend([None] * max(0, 3 - len(current)))
                    current[2] = candidate[0]
                    if len(current) >= 3 and current[2]:
                        metadata = list(current)
                    response_complete = nested(candidate, 8, 0) == 2
                    completed = completed or response_complete
                    text = nested(candidate, 1, 0)
                    if isinstance(text, str) and text.startswith(
                        (
                            "http://googleusercontent.com/card_content/",
                            "https://googleusercontent.com/card_content/",
                        )
                    ):
                        text = nested(candidate, 22, 0) or text
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
    await session.persist()
