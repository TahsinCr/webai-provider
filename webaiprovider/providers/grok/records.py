"""Grok conversation and workspace records mapped to the shared contract."""

import re
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from io import StringIO
from itertools import chain
from mimetypes import guess_type
from typing import Any

from ...core.timing import _STREAM_PAUSED
from ...errors import ProviderError
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.message.protocol import (
    ConversationBound,
    MessageMetadata,
    ProviderRequest,
    ProviderStreamEvent,
    ResponseComplete,
    TextDelta,
    TextReplacement,
)
from ...resources.message.types import (
    Content,
    MessageAction,
    MessageData,
)
from ...resources.project.types import ProjectData
from ...resources.response.artifact import MAX_FILE_REFERENCES, FileTargetRule, files_from
from ...resources.response.source import sources_from

_GROK_ASSET_ORIGIN = "https://assets.grok.com"
_GROK_ATTACHMENT_KEYS = ("fileAttachments", "imageAttachments", "fileAttachmentsMetadata")
_GROK_FILE_KEYS = ("generatedImageUrls", *_GROK_ATTACHMENT_KEYS)
_GROK_FILE_RULE = FileTargetRule(
    markdown_link_re=re.compile(r"\[([^\]\r\n]{1,200})\]\((https://assets\.grok\.com/[^\s)]+)\)"),
    markdown_hint="assets.grok.com",
)


def _grok_asset_url(raw_url: Any) -> str | None:
    if not isinstance(raw_url, str) or not raw_url.strip():
        return None
    target = raw_url.strip()
    if target.startswith(("http://", "https://")):
        return target
    if target.startswith("/"):
        return f"{_GROK_ASSET_ORIGIN}{target}"
    return f"{_GROK_ASSET_ORIGIN}/{target}"


def _assistant_files(
    root: dict[str, Any], *, text: str = "", seen: set[str] | None = None
) -> tuple[dict[str, Any], ...]:
    collected = []
    images = root.get("generatedImageUrls")
    if isinstance(images, (list, tuple)):
        for raw_url in images[:MAX_FILE_REFERENCES]:
            url = _grok_asset_url(raw_url)
            if not url or (seen is not None and url in seen):
                continue
            name = url.rsplit("/", 1)[-1].split("?", 1)[0].split("#", 1)[0] or "generated-image"
            media_type, _ = guess_type(name, strict=False)
            collected.append(dict(url=url, name=name, media_type=media_type))
    for key in _GROK_ATTACHMENT_KEYS:
        attachments = root.get(key)
        if not isinstance(attachments, (list, tuple)):
            continue
        for item in attachments[:MAX_FILE_REFERENCES]:
            if not isinstance(item, dict):
                continue
            url = _grok_asset_url(
                item.get("url") or item.get("fileUri") or item.get("parsedFileUri")
            )
            if not url or (seen is not None and url in seen):
                continue
            collected.append(
                {
                    "url": url,
                    "name": item.get("name") or item.get("fileName"),
                    "media_type": item.get("mimeType") or item.get("fileMimeType"),
                    "size_bytes": item.get("size", item.get("fileSize")),
                }
            )
    if text:
        return files_from({"files": collected}, text, rule=_GROK_FILE_RULE, seen=seen)
    if collected:
        return files_from({"files": collected}, rule=_GROK_FILE_RULE, seen=seen)
    return ()


def _text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(
            part
            for item in value
            if isinstance(item, dict)
            for part in (item.get("text") or item.get("summary") or item.get("message"),)
            if isinstance(part, str)
        )
    return ""


class GrokTextFilter:
    _TOOL_OPEN = "<xai:tool_usage_card"
    _TOOL_CLOSE = "</xai:tool_usage_card>"
    _RENDER_OPEN = "<grok:render"
    _RENDER_CLOSE = "</grok:render>"
    _MARKERS = (_TOOL_OPEN, _TOOL_CLOSE, _RENDER_OPEN, _RENDER_CLOSE)
    _MAX_MARKER_LENGTH = max(map(len, _MARKERS))
    _MARKUP = re.compile(r"<xai:tool_usage_card\b|</?grok:render\b")

    def __init__(self):
        self._pending = ""
        self._inside_tool_card = False
        self._started = False

    def _visible(self, text):
        if not self._started:
            text = text.lstrip()
            self._started = bool(text)
        return text

    @classmethod
    def _safe_end(cls, text):
        index = text.rfind("<", max(0, len(text) - cls._MAX_MARKER_LENGTH))
        if index >= 0 and any(marker.startswith(text[index:]) for marker in cls._MARKERS):
            return index
        return len(text)

    @staticmethod
    def _find_tag_end(text):
        quote_char = None
        for i, char in enumerate(text):
            if quote_char is not None:
                if char == quote_char:
                    quote_char = None
            elif char in ('"', "'"):
                quote_char = char
            elif char == ">":
                return i
        return -1

    def feed(self, text="", *, final=False):
        self._pending += text
        output = []
        while self._pending:
            if self._inside_tool_card:
                end = self._pending.find(self._TOOL_CLOSE)
                if end < 0:
                    safe_end = self._safe_end(self._pending)
                    self._pending = "" if final else self._pending[safe_end:]
                    break
                self._pending = self._pending[end + len(self._TOOL_CLOSE) :]
                self._inside_tool_card = False
                continue

            match = self._MARKUP.search(self._pending)
            if match is None:
                safe_end = self._safe_end(self._pending)
                output.append(self._visible(self._pending[:safe_end]))
                self._pending = "" if final else self._pending[safe_end:]
                break

            index = match.start()
            output.append(self._visible(self._pending[:index]))
            self._pending = self._pending[index:]
            end = self._find_tag_end(self._pending)
            if end < 0:
                if final:
                    self._pending = ""
                elif len(self._pending) > 4096:
                    cut = len(self._pending) - 4096
                    output.append(self._visible(self._pending[:cut]))
                    self._pending = self._pending[cut:]
                    continue
                break
            marker = self._pending[:end]
            self._pending = self._pending[end + 1 :]
            self._inside_tool_card = marker.startswith(self._TOOL_OPEN)
        return "".join(output)


def clean_text(value):
    text = _text(value)
    if "<" not in text:
        return text.lstrip()
    return GrokTextFilter().feed(text, final=True)


def active_responses(history):
    if not isinstance(history, dict):
        return ()
    rows = history.get("responses", history.get("responseNodes", []))
    if not isinstance(rows, list):
        return ()
    rows = [row for row in rows if isinstance(row, dict) and row.get("responseId")]
    by_id = {row["responseId"]: row for row in rows}
    container = history.get("conversation")
    leaf_id = (
        history.get("lastResponseId")
        or history.get("currentLeafResponseId")
        or (container.get("lastResponseId") if isinstance(container, dict) else None)
    )
    if leaf_id not in by_id:
        parents = {row.get("parentResponseId") for row in rows}
        leaf = max(
            (row for row in reversed(rows) if row["responseId"] not in parents),
            key=lambda row: str(row.get("createTime") or row.get("createdAt") or ""),
            default=None,
        )
        if leaf is None:
            return tuple(rows)
        leaf_id = leaf["responseId"]

    branch, seen = [], set()
    while leaf_id in by_id and leaf_id not in seen:
        seen.add(leaf_id)
        row = by_id[leaf_id]
        branch.append(row)
        leaf_id = row.get("parentResponseId")
    return tuple(reversed(branch))


def _messages(rows, temporary=False):
    messages = []
    for row in rows:
        response_id = row["responseId"]
        sender = str(row.get("sender", "")).lower()
        if sender in ("human", "user"):
            text = row.get("message") or row.get("query") or ""
            seen_ids = set()
            resources = []
            for item in chain(
                row.get("fileAttachments") or (),
                row.get("imageAttachments") or (),
                row.get("attachments") or (),
            ):
                if isinstance(item, str):
                    file_id, name, mime_type = item, "Attachment", None
                elif isinstance(item, dict):
                    file_id = item.get("assetId") or item.get("fileMetadataId") or item.get("id")
                    name = item.get("name") or item.get("fileName") or "Attachment"
                    mime_type = item.get("mimeType") or item.get("fileMimeType")
                else:
                    continue
                if file_id and (identifier := str(file_id)) not in seen_ids:
                    seen_ids.add(identifier)
                    resources.append(
                        Content("resource", resource=Attachment(identifier, str(name), mime_type))
                    )
            messages.append(
                MessageData(
                    response_id,
                    "user",
                    (Content("text", str(text)), *resources),
                    actions=() if temporary else (MessageAction.EDIT.value,),
                )
            )
            continue
        if sender not in ("assistant", "model", "grok"):
            continue
        thinking = clean_text(row.get("thinkingTrace"))
        if thinking:
            messages.append(
                MessageData(
                    response_id + ":analysis",
                    "assistant",
                    (Content("text", thinking),),
                    "analysis",
                    {"native_response_id": response_id},
                )
            )
        answer = clean_text(row.get("message"))
        results = row.get("webSearchResults")
        sources = sources_from({"sources": results}) if results else ()
        files = _assistant_files(row, text=answer)
        metadata: dict[str, Any] = {
            "model": row.get("model"),
            "sources": sources,
            "native_response_id": response_id,
        }
        if files:
            metadata["files"] = files
        messages.append(
            MessageData(
                response_id,
                "assistant",
                (Content("text", answer),),
                "final",
                metadata,
                () if temporary else ((MessageAction.RETRY.value,) if answer else ()),
            )
        )
    return tuple(messages)


def conversation(value, history=None, *, active_rows=None):
    if not isinstance(value, dict):
        raise ProviderError("Grok conversation response contract changed")
    identifier = value.get("conversationId") or value.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise ProviderError("Grok conversation response contract changed")
    workspaces = value.get("workspaces") or ()
    first_ws = workspaces[0] if workspaces and isinstance(workspaces[0], dict) else {}
    project_id = value.get("workspaceId") or first_ws.get("workspaceId") or first_ws.get("id")
    project_name = first_ws.get("name") or value.get("workspaceName") or ""
    is_temporary = bool(value.get("temporary"))
    if active_rows is None:
        active_rows = active_responses(history) if history is not None else ()
    return Conversation(
        identifier,
        value.get("title") or "Grok conversation",
        _messages(active_rows, temporary=is_temporary) if active_rows else (),
        ProjectData(project_id, str(project_name)) if project_id else None,
        {
            "temporary": is_temporary,
            "created_at": value.get("createTime"),
            "modified_at": value.get("modifyTime"),
        },
    )


def project(value, files=()):
    if not isinstance(value, dict):
        raise ProviderError("Grok workspace response contract changed")
    identifier = value.get("workspaceId") or value.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise ProviderError("Grok workspace response contract changed")
    files = files or ()
    resources = tuple(
        Attachment(
            str(item.get("path") or item.get("assetId")),
            str(item.get("name") or item.get("path") or "Source"),
            item.get("mimeType"),
            metadata={
                key: item[key]
                for key in ("path", "assetId", "size", "createdAt", "modifiedAt")
                if item.get(key) is not None
            },
        )
        for item in files
        if isinstance(item, dict)
        and not item.get("isDirectory")
        and (item.get("assetId") or item.get("path"))
    )
    return ProjectData(
        identifier,
        str(value.get("name") or "Grok project"),
        value.get("customPersonality"),
        resources,
        {"preferred_model": value.get("preferredModel"), "kind": value.get("kind")},
    )


def _append_web_search_results(
    native: dict, raw_search_results: list, seen_search_keys: set[str]
) -> None:
    results = native.get("webSearchResults")
    if not isinstance(results, list):
        return
    for item in results:
        if not isinstance(item, dict):
            continue
        key = item.get("url") or item.get("title")
        if key:
            if key not in seen_search_keys:
                seen_search_keys.add(key)
                raw_search_results.append(item)
        elif item not in raw_search_results:
            raw_search_results.append(item)


def _flush_grok_filters(text_filter, streamed_text, analysis_filter, streamed_analysis):
    for cleaner, output, channel in (
        (text_filter, streamed_text, None),
        (analysis_filter, streamed_analysis, "analysis"),
    ):
        remainder = cleaner.feed(final=True)
        if remainder:
            output.write(remainder)
            yield TextDelta(remainder, channel) if channel else TextDelta(remainder)


def _reconcile_grok_native(raw_native_text, raw_native_analysis, streamed_text, streamed_analysis):
    resolved_analysis = clean_text(raw_native_analysis) if raw_native_analysis is not None else None
    resolved_text = clean_text(raw_native_text) if raw_native_text is not None else None
    for native, output, channel in (
        (resolved_analysis, streamed_analysis, "analysis"),
        (resolved_text, streamed_text, None),
    ):
        if native is not None and native != output.getvalue():
            output.seek(0)
            output.truncate()
            output.write(native)
            yield TextReplacement(native, channel) if channel else TextReplacement(native)


class _GrokStreamProcessor:
    """Shared incremental Grok event state machine for both sync and async streams."""

    def __init__(
        self,
        request: ProviderRequest,
        *,
        model: str | None,
        raise_stream_error: Callable[[object], None],
        remember: Callable[[str, str], None],
    ) -> None:
        self.existing_conversation_id = request.conversation_id
        self.conversation_id = request.conversation_id
        self.response_id: str | None = None
        self.user_response_id: str | None = None
        self.streamed_text = StringIO()
        self.streamed_analysis = StringIO()
        self.raw_native_text: str | None = None
        self.raw_native_analysis: str | None = None
        self.sources: tuple[dict[str, Any], ...] = ()
        self.raw_search_results: list[dict] = []
        self.seen_search_keys: set[str] = set()
        self.file_records: list[dict[str, Any]] = []
        self.seen_file_urls: set[str] = set()
        self.response_model = model
        self.complete = False
        self.completion_notified = False
        self.bound = False
        self.text_filter = GrokTextFilter()
        self.analysis_filter = GrokTextFilter()
        self._raise_stream_error = raise_stream_error
        self._remember = remember

    def _finalize_turn(self):
        if self.raw_search_results and not self.sources:
            self.sources = sources_from({"sources": self.raw_search_results})
        yield from _flush_grok_filters(
            self.text_filter, self.streamed_text, self.analysis_filter, self.streamed_analysis
        )
        yield from _reconcile_grok_native(
            self.raw_native_text,
            self.raw_native_analysis,
            self.streamed_text,
            self.streamed_analysis,
        )

    def feed(self, chunk: dict[str, Any]):
        result = chunk.get("result")
        if not isinstance(result, dict):
            result = chunk
        native_conversation = result.get("conversation")
        response = result.get("response")
        if not isinstance(response, dict):
            response = result
        if not self.conversation_id and isinstance(native_conversation, dict):
            self.conversation_id = native_conversation.get("conversationId")
            if self.conversation_id and not self.bound:
                yield ConversationBound(self.conversation_id)
                self.bound = True
        if not self.conversation_id and not self.existing_conversation_id:
            self.conversation_id = result.get("conversationId") or response.get("conversationId")
            if self.conversation_id and not self.bound:
                yield ConversationBound(self.conversation_id)
                self.bound = True
        error = response.get("error") or result.get("error") or chunk.get("error")
        if error:
            self._raise_stream_error(error)
        user = response.get("userResponse")
        if isinstance(user, dict) and user.get("responseId"):
            self.user_response_id = user["responseId"]
        native = response.get("modelResponse")
        if isinstance(native, dict):
            self.response_id = native.get("responseId") or self.response_id
            self.response_model = native.get("model") or self.response_model
            _append_web_search_results(native, self.raw_search_results, self.seen_search_keys)
            if len(self.seen_file_urls) < MAX_FILE_REFERENCES and any(
                native.get(key) for key in _GROK_FILE_KEYS
            ):
                self.file_records.extend(_assistant_files(native, seen=self.seen_file_urls))
            if isinstance(native.get("message"), str):
                self.raw_native_text = native["message"]
            if native.get("thinkingTrace") is not None:
                self.raw_native_analysis = native["thinkingTrace"]
            if native.get("partial") is False:
                self.complete = True
        token = response.get("token")
        if isinstance(token, str) and token:
            analysis = response.get("isThinking") or response.get("messageTag") in (
                "header",
                "summary",
            )
            cleaner = self.analysis_filter if analysis else self.text_filter
            visible = cleaner.feed(token)
            if visible:
                (self.streamed_analysis if analysis else self.streamed_text).write(visible)
                yield TextDelta(visible, "analysis") if analysis else TextDelta(visible)
        if response.get("responseId") and not self.response_id:
            self.response_id = response["responseId"]
        if response.get("done") is True or response.get("finished") is True:
            self.complete = True
        if (
            self.complete
            and not self.completion_notified
            and (self.existing_conversation_id or self.conversation_id)
        ):
            yield from self._finalize_turn()
            yield ResponseComplete()
            self.completion_notified = True

    def finish(self):
        yield from self._finalize_turn()
        if not self.conversation_id:
            raise ProviderError("Grok did not return a conversation ID")
        if not self.complete:
            raise ProviderError("Grok stream ended before response completion; it was not resent")
        if self.response_id:
            self._remember(self.conversation_id, self.response_id)
        if not self.bound and not self.existing_conversation_id:
            yield ConversationBound(self.conversation_id)
        if self.streamed_text.tell() and len(self.seen_file_urls) < MAX_FILE_REFERENCES:
            self.file_records.extend(
                files_from(
                    self.streamed_text.getvalue(), rule=_GROK_FILE_RULE, seen=self.seen_file_urls
                )
            )
        if self.response_id and (self.streamed_text.tell() or self.file_records or self.sources):
            yield MessageMetadata(
                message_id=self.response_id,
                user_message_id=self.user_response_id,
                model=self.response_model,
                effort=None,
                sources=self.sources,
                files=tuple(self.file_records),
            )
        if not self.completion_notified:
            yield ResponseComplete()


def response_events(
    request: ProviderRequest,
    *,
    prepare: Callable[[ProviderRequest], tuple[Any, str | None]],
    raise_stream_error: Callable[[object], None],
    remember: Callable[[str, str], None],
):
    chunks, model = prepare(request)
    processor = _GrokStreamProcessor(
        request,
        model=model,
        raise_stream_error=raise_stream_error,
        remember=remember,
    )
    for chunk in chunks:
        if chunk is _STREAM_PAUSED:
            yield chunk
        else:
            yield from processor.feed(chunk)
    yield from processor.finish()


async def async_response_events(
    request: ProviderRequest,
    *,
    prepare: Callable[[ProviderRequest], Awaitable[tuple[AsyncIterator[dict], str | None]]],
    raise_stream_error: Callable[[object], None],
    remember: Callable[[str, str], None],
) -> AsyncGenerator[ProviderStreamEvent, None]:
    chunks, model = await prepare(request)
    processor = _GrokStreamProcessor(
        request,
        model=model,
        raise_stream_error=raise_stream_error,
        remember=remember,
    )
    async for chunk in chunks:
        for event in processor.feed(chunk):
            yield event
    for event in processor.finish():
        yield event
