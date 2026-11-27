from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Iterator
from copy import copy
from dataclasses import dataclass
from time import monotonic_ns
from typing import TYPE_CHECKING, Any

from flowcoord import AsyncSingleFlight

from ...collections import (
    AsyncRemoteCollection,
    BaseCollection,
    RemoteCollection,
    _async_report_read,
    _report_read,
    validate_limit,
    validate_predicate,
)
from ...errors import RemoteNotFound, UnsupportedOperationError, WebAIProviderError
from ..chat.chat import AsyncChat, _project_from_data
from ..chat.types import Conversation
from ..message.attachment import Attachment, Attachments
from ..message.message import AsyncMessage, Message
from ..message.types import MessageData
from ..model.model import EffortRef, ModelRef, ModelSelection
from ..model.types import Models
from .artifact import Artifact
from .operation import (
    AsyncMessageOperation,
    MessageOperation,
)
from .parts import ResponsePart, TextPart, ThinkingPart
from .response import Response
from .source import Source

if TYPE_CHECKING:
    from ..chat.chat import Chat


def _build_sources(raw_sources: Iterable[Any], source_type: type[Source]) -> tuple[Source, ...]:
    sources: list[Source] = []
    for index, raw in enumerate(raw_sources):
        if isinstance(raw, Source):
            sources.append(raw)
        elif isinstance(raw, dict):
            url = str(raw.get("url") or raw.get("uri") or "")
            sid = str(
                raw.get("id")
                or raw.get("source_id")
                or raw.get("file_id")
                or url
                or f"source-{index}"
            )
            sources.append(
                source_type(
                    id=sid,
                    url=url,
                    title=str(raw.get("title") or raw.get("name") or ""),
                    snippet=str(raw.get("description") or raw.get("snippet") or ""),
                    kind=str(raw.get("kind") or ("file" if raw.get("file_id") else "web")),
                    cited=raw.get("cited") if isinstance(raw.get("cited"), bool) else None,
                    metadata=dict(raw),
                )
            )
    return tuple(sources)


def _build_artifacts(
    item: Any, raw_files: Iterable[Any], artifact_type: type[Artifact]
) -> tuple[Artifact, ...]:
    artifacts: list[Artifact] = []
    seen: set[str] = set()
    for index, entry in enumerate(raw_files):
        if isinstance(entry, Artifact):
            key = entry.id or entry.download_url or entry.name
            if key not in seen:
                seen.add(key)
                artifacts.append(entry)
        elif isinstance(entry, dict):
            url = entry.get("remote_url") or entry.get("download_url") or entry.get("url")
            name = str(
                entry.get("name") or entry.get("filename") or entry.get("file_name") or "file"
            )
            aid = str(entry.get("id") or entry.get("file_id") or url or f"artifact-{index}")
            key = f"{aid}:{url or name}"
            if key in seen:
                continue
            seen.add(key)
            size = entry.get("size_bytes", entry.get("size"))
            width = entry.get("width")
            height = entry.get("height")
            title = entry.get("title")
            artifacts.append(
                artifact_type(
                    id=aid,
                    kind=str(entry.get("kind") or "file"),
                    name=name,
                    mime_type=str(
                        entry.get("media_type")
                        or entry.get("mime_type")
                        or "application/octet-stream"
                    ),
                    size=size if isinstance(size, int) and not isinstance(size, bool) else None,
                    download_url=str(url) if url else None,
                    content=entry.get("content"),
                    title=str(title) if isinstance(title, str) else None,
                    width=width if isinstance(width, int) and not isinstance(width, bool) else None,
                    height=(
                        height if isinstance(height, int) and not isinstance(height, bool) else None
                    ),
                    metadata=dict(entry),
                )
            )
    for part in getattr(item, "content", ()):
        resource = getattr(part, "resource", None)
        if resource is not None and item.role == "assistant":
            key = f"{resource.id}:{resource.remote_url or resource.name}"
            if key in seen:
                continue
            seen.add(key)
            artifacts.append(
                artifact_type(
                    id=resource.id,
                    kind="resource",
                    name=resource.name,
                    mime_type=resource.media_type or "application/octet-stream",
                    download_url=resource.remote_url,
                    metadata=dict(resource.metadata),
                )
            )
    return tuple(artifacts)


def _recent_messages(messages: tuple[MessageData, ...], limit: int) -> tuple[MessageData, ...]:
    """Keep entire turns when restricting the remote history tail."""
    for index in range(len(messages) - 1, -1, -1):
        if messages[index].role == "user":
            limit -= 1
            if limit == 0:
                return messages[index:]
    return messages


def _history_turns(messages: Iterable[MessageData]) -> Iterator[tuple[MessageData, ...]]:
    assistant: list[MessageData] = []
    for item in messages:
        if item.role == "assistant":
            assistant.append(item)
        elif item.role == "user":
            if assistant:
                yield tuple(assistant)
                assistant.clear()
            yield (item,)
    if assistant:
        yield tuple(assistant)


@dataclass
class _HistoryRead:
    items: list[Response]
    started_ns: int
    reported: bool = False


class _BaseResponsesCollection:
    """Shared turn tracking, history reconciliation, and message-operation builders."""

    _collection_name = "chat.responses"
    _default_message_type: type[Any] = AsyncMessage
    chat: Any
    _turns: list[Response]
    _known_responses: dict[str, Response]
    _known_messages: dict[str, Any]
    _history_read: int
    _history_applied: int

    def _execute_send(self, operation: AsyncMessageOperation | MessageOperation) -> Any:
        provider = self.chat.provider
        if provider is None:
            raise WebAIProviderError("No provider session is attached to this chat")
        return provider._require_provider()._execute_stream(operation)

    @property
    def _read_provider(self):
        return self.chat.provider

    @property
    def _scope_id(self):
        return self.chat.remote_id

    def _init_responses_state(self, chat: Any) -> None:
        self.chat = chat
        self._turns = []
        self._known_responses = {}
        self._known_messages = {}
        self._history_read = 0
        self._history_applied = 0

    @staticmethod
    def _extract_id(item: Any) -> str:
        if isinstance(item, Response):
            return item.remote_id or item.id
        return BaseCollection._extract_id(item)

    @staticmethod
    def _extract_name(item: Any) -> str:
        if isinstance(item, Response):
            return item.text
        return BaseCollection._extract_name(item)

    @classmethod
    def _item_matches(
        cls,
        item: Any,
        query: str,
        *,
        exact: bool = False,
        startswith: bool = False,
        endswith: bool = False,
    ) -> bool:
        if isinstance(item, Response):
            candidates = [item.text, item.id, item.remote_id or ""]
            if item.message is not None:
                candidates.extend(
                    (item.message.content, item.message.id, item.message.remote_id or "")
                )
            return any(
                text
                and cls._matches(
                    text,
                    query,
                    exact=exact,
                    startswith=startswith,
                    endswith=endswith,
                )
                for text in candidates
            )
        return BaseCollection._item_matches(
            item, query, exact=exact, startswith=startswith, endswith=endswith
        )

    @classmethod
    def _item_matches_exact_id(cls, item: Any, item_id: str) -> bool:
        if isinstance(item, Response):
            if item.id == item_id or item.remote_id == item_id:
                return True
            if item.message is not None:
                return item.message.id == item_id or item.message.remote_id == item_id
            return False
        return BaseCollection._item_matches_exact_id(item, item_id)

    def _track_local_message(self, message: Any) -> Response:
        self._known_messages[message.id] = message
        if message.remote_id:
            self._known_messages[message.remote_id] = message
        existing = next((r for r in self._turns if r.message is message), None)
        if existing is not None:
            return existing
        pending = message.response or getattr(self.chat.provider, "_response_type", Response)(
            "",
            message=message,
            id=f"pending:{message.id}",
            model=message.model,
            metadata={"pending": True},
            chat=self.chat,
        )
        message.response = pending
        self._known_responses[pending.id] = pending
        self._turns.append(pending)
        return pending

    def _reconcile_live_response(
        self,
        operation: Any,
        response: Response,
    ) -> Response:
        target_id = response.remote_id or response.id
        existing = next(
            (
                item
                for item in self._turns
                if target_id and (item.remote_id == target_id or item.id == target_id)
            ),
            None,
        )
        if existing is not None:
            existing._set_parts(response.parts)
            existing.model = response.model
            existing.remote_id = response.remote_id
            existing.sources = response.sources
            existing.artifacts = response.artifacts
            existing.research = response.research
            existing.timing = response.timing
            existing.metadata = dict(response.metadata)
            existing.chat = self.chat
            if response.message is not None:
                existing.message = response.message
                response.message.response = existing
            self._index_response(existing)
            return existing

        pending_idx = next(
            (idx for idx, item in enumerate(self._turns) if item.message is operation.message),
            None,
        )
        if pending_idx is not None:
            self._turns[pending_idx] = response
        else:
            self._turns.append(response)
        self._index_response(response)
        return response

    def _index_response(self, response: Response) -> None:
        if response.id:
            self._known_responses[response.id] = response
        if response.remote_id:
            self._known_responses[response.remote_id] = response
        if response.message is not None:
            if response.message.id:
                self._known_messages[response.message.id] = response.message
            if response.message.remote_id:
                self._known_messages[response.message.remote_id] = response.message

    def _apply_history(
        self,
        conversation: Conversation,
        provider: Any,
        *,
        limit: int | None = None,
    ) -> list[Response]:
        if not isinstance(conversation, Conversation):
            raise WebAIProviderError("Provider returned an invalid conversation")
        if conversation.title and not self.chat.title:
            self.chat.title = conversation.title
        self.chat.metadata = dict(conversation.metadata)
        if conversation.project is not None:
            resolved_project = _project_from_data(provider, conversation.project)
            if self.chat.project is None:
                self.chat.project = resolved_project
            elif conversation.project.name and not self.chat.project.title:
                self.chat.project.title = conversation.project.name

        existing_messages = self._known_messages.copy()
        existing_responses = self._known_responses.copy()
        for item in self._turns:
            existing_responses[item.remote_id or item.id] = item
            if item.message is not None:
                existing_messages[item.message.remote_id or item.message.id] = item.message

        responses: list[Response] = []
        seen_user_ids: set[str] = set()
        seen_resp_ids: set[str] = set()
        pending_user: Any = None
        answered = False

        def build_unanswered(user_msg: Any, parts: tuple[ResponsePart, ...] = ()) -> Response:
            resp_id = f"unanswered:{user_msg.id}"
            seen_resp_ids.add(resp_id)
            resp = existing_responses.get(resp_id)
            if resp is None or resp.remote_id is not None:
                resp = provider._response_type(
                    parts=parts,
                    message=user_msg,
                    id=resp_id,
                    model=user_msg.model,
                    metadata={"from_history": True, "unanswered": True},
                    chat=self.chat,
                )
            else:
                resp._set_parts(parts)
                resp.message = user_msg
                resp.chat = self.chat
            user_msg.response = resp
            return resp

        messages = conversation.messages
        if limit is not None and len(messages) > limit and not self._turns:
            messages = _recent_messages(messages, limit)
        for items in _history_turns(messages):
            item = items[0]
            if item.role == "user":
                content = "\n".join(
                    part.text for part in item.content if part.kind == "text" and part.text
                )
                if pending_user is not None and not answered:
                    responses.append(build_unanswered(pending_user))
                seen_user_ids.add(item.id)
                user_msg = existing_messages.get(item.id)
                if user_msg is None:
                    user_msg = provider._message_type(role="user", id=item.id, chat=self.chat)
                user_msg.remote_id = item.id
                user_msg.content = content
                user_msg.metadata = {**item.metadata, "actions": item.actions}
                pending_user = user_msg
                answered = False
                continue

            target = None
            target_rank = -1
            response_parts: list[ResponsePart] = []
            sources: list[Source] = []
            artifacts: list[Artifact] = []
            for row in items:
                thinking = row.channel == "analysis"
                parts = row.content
                text = "\n".join(part.text for part in parts if part.kind == "text" and part.text)
                if text:
                    part_type = ThinkingPart if thinking else TextPart
                    response_parts.append(
                        part_type(f"{'thinking' if thinking else 'text'}:{row.id}", text)
                    )
                if not thinking:
                    # File-only records cannot displace an actionable text reply.
                    rank = 2 if text and row.actions else int(bool(text))
                    if rank >= target_rank:
                        target, target_rank = row, rank
                raw_sources = row.metadata.get("sources")
                if raw_sources:
                    sources.extend(_build_sources(raw_sources, provider._source_type))
                raw_files = row.metadata.get("files") or ()
                if raw_files or len(parts) > 1 or (parts and parts[0].resource is not None):
                    artifacts.extend(_build_artifacts(row, raw_files, provider._artifact_type))
            if target is None:
                if pending_user is not None and not answered:
                    responses.append(build_unanswered(pending_user, tuple(response_parts)))
                    answered = True
                continue
            item = target
            seen_resp_ids.add(item.id)
            resp_meta = {
                **item.metadata,
                "actions": item.actions,
                "from_history": True,
                "message_id": item.id,
            }
            resp = existing_responses.get(item.id)
            if resp is None:
                resp = provider._response_type(
                    parts=response_parts,
                    message=pending_user,
                    id=item.id,
                    remote_id=item.id,
                    sources=sources,
                    artifacts=artifacts,
                    metadata=resp_meta,
                    timing=None,
                    chat=self.chat,
                )
            else:
                resp._set_parts(response_parts)
                resp.remote_id = item.id
                resp.message = pending_user
                resp.sources = type(resp.sources)(sources)
                resp.artifacts = type(resp.artifacts)(artifacts)
                resp.metadata = resp_meta
                resp.chat = self.chat
            if pending_user is not None:
                pending_user.response = resp
                answered = True
            responses.append(resp)

        if pending_user is not None and not answered:
            responses.append(build_unanswered(pending_user))

        for item in self._turns:
            if item.remote_id is not None or item.id in seen_resp_ids:
                continue
            msg = item.message
            if msg is not None and (
                msg.remote_id is not None
                or msg.id in seen_user_ids
                or msg.metadata.get("action") in ("retry", "edit")
            ):
                continue
            if item.metadata.get("action") in ("retry", "edit"):
                continue
            responses.append(item)

        self._turns = list(responses)
        self._known_responses.clear()
        self._known_messages.clear()
        for resp in responses:
            self._index_response(resp)

        return responses

    @staticmethod
    def _filter_history_responses(
        responses: list[Response],
        predicate: Callable[[Response], bool] | None,
        validated: int | None,
    ) -> list[Response]:
        if predicate is not None:
            responses = [item for item in responses if predicate(item)]
        if validated is None:
            return list(responses) if predicate is None else responses
        return responses[-validated:]

    def _prepare_create_message(
        self,
        message: AsyncMessage | Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        tool: Any = None,
        tool_timeout: float | None = None,
        metadata: dict[str, Any],
    ) -> Any:
        resolved_tool = (
            tool
            if tool is not None
            else (message.metadata.get("tool") if not isinstance(message, str) else None)
        )
        resolved_timeout = (
            tool_timeout
            if tool_timeout is not None
            else (message.metadata.get("tool_timeout") if not isinstance(message, str) else None)
        )
        provider = getattr(self.chat, "provider", None)
        if provider is not None:
            provider._validate_tool(resolved_tool, tool_timeout=resolved_timeout)
        if resolved_tool is not None:
            metadata = {**metadata, "tool": resolved_tool}
        if resolved_timeout is not None:
            metadata = {**metadata, "tool_timeout": resolved_timeout}
        if isinstance(message, str):
            msg = getattr(self.chat.provider, "_message_type", self._default_message_type)(
                content=message,
                model=model,
                attachments=attachments,
                metadata=metadata,
                chat=self.chat,
            )
        else:
            msg = message
            if msg.chat is not None and msg.chat is not self.chat:
                raise ValueError("Message belongs to a different chat")
            if any(item.message is msg for item in self._turns):
                raise ValueError("Message is already in this chat")
            msg.chat = self.chat
            if model is not None:
                msg.model = model
            if attachments is not None:
                for item in attachments:
                    msg.attachments.create(item)
            if resolved_tool is not None:
                msg.metadata["tool"] = resolved_tool
            if resolved_timeout is not None:
                msg.metadata["tool_timeout"] = resolved_timeout
        self._track_local_message(msg)
        return msg

    def _resolve_known_message(self, target: Response | AsyncMessage | Message | str) -> Any:
        if isinstance(target, Response):
            if target.message is None:
                raise UnsupportedOperationError("Response has no associated user message")
            return target.message
        if isinstance(target, (Message, AsyncMessage)):
            return target
        found_msg = self._known_messages.get(target)
        if found_msg is not None:
            return found_msg
        found_resp = self._known_responses.get(target) or next(
            (r for r in self._turns if self._item_matches_exact_id(r, target)),
            None,
        )
        if found_resp is not None and found_resp.message is not None:
            return found_resp.message
        raise RemoteNotFound(f"Record was not found: {target}")

    def _resolve_retry_target(
        self, target: Response | AsyncMessage | Message | str
    ) -> tuple[str, Any, Any]:
        if isinstance(target, Response):
            action_id = (
                target.remote_id
                or target.id
                or (target.message.remote_id or target.message.id if target.message else "")
            )
            return action_id, target.message, target.model
        if isinstance(target, (Message, AsyncMessage)):
            resp = target.response
            if resp is not None:
                action_id = resp.remote_id or resp.id
                return action_id, target, resp.model or target.model
            return target.remote_id or target.id, target, target.model
        found_resp = self._known_responses.get(target) or next(
            (r for r in self._turns if self._item_matches_exact_id(r, target)),
            None,
        )
        if found_resp is not None:
            return (
                found_resp.remote_id or found_resp.id,
                found_resp.message,
                found_resp.model,
            )
        found_msg = self._resolve_known_message(target)
        return found_msg.remote_id or found_msg.id, found_msg, found_msg.model

    def _build_edit_message(
        self,
        target: Response | AsyncMessage | Message | str,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        options: dict[str, Any],
    ) -> Any:
        msg_target = self._resolve_known_message(target)
        msg = getattr(self.chat.provider, "_message_type", self._default_message_type)(
            content=new_content,
            model=model if model is not None else msg_target.model,
            attachments=(copy(item) for item in msg_target.attachments),
            metadata={
                **msg_target.metadata,
                **options,
                "parent_message_id": msg_target.remote_id or msg_target.id,
                "action": "edit",
            },
            chat=self.chat,
        )
        self._known_messages[msg.id] = msg
        return msg

    def _build_retry_message(
        self,
        target: Response | AsyncMessage | Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        options: dict[str, Any],
    ) -> Any:
        action_id, prompt_msg, default_model = self._resolve_retry_target(target)
        attachments = (
            (copy(item) for item in prompt_msg.attachments) if prompt_msg is not None else None
        )
        msg = getattr(self.chat.provider, "_message_type", self._default_message_type)(
            content=prompt_msg.content if prompt_msg is not None else "",
            model=model if model is not None else default_model,
            attachments=attachments,
            metadata={
                **(prompt_msg.metadata if prompt_msg is not None else {}),
                **options,
                "parent_message_id": action_id,
                "action": "retry",
            },
            chat=self.chat,
        )
        self._known_messages[msg.id] = msg
        return msg

    def _delete_local(self, target: Response | AsyncMessage | Message | str) -> None:
        target_id = target if isinstance(target, str) else target.id
        index = next(
            (
                i
                for i, item in enumerate(self._turns)
                if self._item_matches_exact_id(item, target_id)
            ),
            None,
        )
        response = self._turns[index] if index is not None else self._known_responses.get(target_id)
        message = response.message if response is not None else self._known_messages.get(target_id)
        if response is None and message is not None:
            response = message.response
        target_message = target.message if isinstance(target, Response) else None
        if (
            (not isinstance(target, str) and target.remote_id)
            or (target_message is not None and target_message.remote_id)
            or (response is not None and response.remote_id)
            or (message is not None and message.remote_id)
        ):
            raise UnsupportedOperationError("Deleting persisted remote responses is not supported")

        if index is not None:
            self._turns.pop(index)
        if response is not None:
            self._known_responses.pop(response.id, None)
        if message is not None:
            self._known_messages.pop(message.id, None)


class AsyncResponsesCollection(_BaseResponsesCollection, AsyncRemoteCollection[Response]):
    """Conversation turn collection pairing assistant Responses with their triggering user Messages."""

    _default_message_type = AsyncMessage

    def __init__(self, chat: AsyncChat) -> None:
        self._init_responses_state(chat)
        self._history_flight: AsyncSingleFlight[str] | None = None

    async def _load_history(self, *, fresh: bool = False) -> list[Response]:
        remote_id = self.chat.remote_id
        if not remote_id:
            return list(self._turns)
        provider = self.chat.provider
        if provider is None:
            raise WebAIProviderError("Chat is not attached to an open provider")
        provider._require_provider()

        async def read() -> _HistoryRead:
            # SingleFlight shields its worker from waiter cancellation. Track
            # the worker itself so shutdown cannot close its transport early.
            async with provider._activity():
                provider._require_provider()
                self._history_read += 1
                read_id = self._history_read
                started_ns = monotonic_ns()
                conversation = await provider._get_chat_data(remote_id)
                if read_id < self._history_applied or self.chat.remote_id != remote_id:
                    return _HistoryRead(list(self._turns), started_ns)
                responses = self._apply_history(conversation, provider)
                self._history_applied = read_id
                return _HistoryRead(responses, started_ns)

        if fresh:
            result = await read()
        else:
            if provider.closing and not provider._has_active_operation():
                raise WebAIProviderError("Provider is closing or is closed")
            if self._history_flight is None:
                self._history_flight = AsyncSingleFlight()
            result = await self._history_flight.run(remote_id, read)
        if not result.reported:
            self._mark_remote_read(result.started_ns)
            result.reported = True
        return result.items

    @_async_report_read
    async def list(
        self,
        predicate: Callable[[Response], bool] | None = None,
        *,
        limit: int | None = 50,
        fresh: bool = False,
    ) -> list[Response]:
        validate_predicate(predicate)
        validated = validate_limit(limit)
        if validated == 0:
            return []
        responses = await self._load_history(fresh=fresh)
        return self._filter_history_responses(responses, predicate, validated)

    async def _iter_batches(self, *, limit=None, fresh=False, **options):
        del options
        items = await self._load_history(fresh=fresh)
        yield items if limit is None else items[:limit]

    def create(
        self,
        message: AsyncMessage | Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        tool: Any = None,
        tool_timeout: float | None = None,
        **metadata: Any,
    ) -> AsyncMessageOperation:
        msg = self._prepare_create_message(
            message,
            model=model,
            attachments=attachments,
            tool=tool,
            tool_timeout=tool_timeout,
            metadata=metadata,
        )
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    def update(
        self,
        target: Response | AsyncMessage | Message | str,
        *,
        content: str | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        msg = self._resolve_known_message(target)
        new_content = content if content is not None else msg.content
        return self.edit(msg, new_content, model=model, **options)

    def edit(
        self,
        target: Response | AsyncMessage | Message | str,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        msg = self._build_edit_message(target, new_content, model=model, options=options)
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    def retry(
        self,
        target: Response | AsyncMessage | Message | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> AsyncMessageOperation:
        msg = self._build_retry_message(target, model=model, options=options)
        return AsyncMessageOperation(self.chat, msg, executor=self._execute_send)

    async def delete(self, target: Response | AsyncMessage | Message | str) -> None:
        """Delete an unsent local response/message; deleting persisted remote turns is unsupported."""
        self._delete_local(target)


class ResponsesCollection(_BaseResponsesCollection, RemoteCollection[Response]):
    """Blocking conversation turn collection running directly on the caller thread."""

    _default_message_type = Message

    def __init__(self, chat: Chat) -> None:
        self._init_responses_state(chat)
        self._history_lock = threading.RLock()

    def _track_local_message(self, message: Any) -> Response:
        with self._history_lock:
            return super()._track_local_message(message)

    def _reconcile_live_response(self, operation: Any, response: Response) -> Response:
        with self._history_lock:
            result = super()._reconcile_live_response(operation, response)
            self._history_applied = self._history_read + 1
            return result

    def _load_history(self, *, fresh: bool = False) -> list[Response]:
        del fresh
        remote_id = self.chat.remote_id
        if not remote_id:
            with self._history_lock:
                return list(self._turns)
        provider = self.chat.provider
        if provider is None:
            raise WebAIProviderError("Chat is not attached to an open provider")
        provider._require_provider()
        with provider._activity():
            provider._require_provider()
            with self._history_lock:
                self._history_read += 1
                read_id = self._history_read
            started_ns = monotonic_ns()
            conversation = provider._get_chat_data(remote_id)
            with self._history_lock:
                if read_id < self._history_applied or self.chat.remote_id != remote_id:
                    items = list(self._turns)
                else:
                    items = self._apply_history(conversation, provider)
                    self._history_applied = read_id
        self._mark_remote_read(started_ns)
        return items

    @_report_read
    def list(
        self,
        predicate: Callable[[Response], bool] | None = None,
        *,
        limit: int | None = 50,
        fresh: bool = False,
    ) -> list[Response]:
        validate_predicate(predicate)
        validated = validate_limit(limit)
        if validated == 0:
            return []
        responses = self._load_history(fresh=fresh)
        return self._filter_history_responses(responses, predicate, validated)

    def _iter_batches(self, *, limit=None, fresh=False, **options):
        del options
        items = self._load_history(fresh=fresh)
        yield items if limit is None else items[:limit]

    def create(
        self,
        message: Message | AsyncMessage | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        attachments: Iterable[Attachment] | Attachments | None = None,
        tool: Any = None,
        tool_timeout: float | None = None,
        **metadata: Any,
    ) -> MessageOperation:
        with self._history_lock:
            msg = self._prepare_create_message(
                message,
                model=model,
                attachments=attachments,
                tool=tool,
                tool_timeout=tool_timeout,
                metadata=metadata,
            )
        return MessageOperation(self.chat, msg, executor=self._execute_send)

    def update(
        self,
        target: Response | Message | AsyncMessage | str,
        *,
        content: str | None = None,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        with self._history_lock:
            msg = self._resolve_known_message(target)
        new_content = content if content is not None else msg.content
        return self.edit(msg, new_content, model=model, **options)

    def edit(
        self,
        target: Response | Message | AsyncMessage | str,
        new_content: str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        with self._history_lock:
            msg = self._build_edit_message(target, new_content, model=model, options=options)
        return MessageOperation(self.chat, msg, executor=self._execute_send)

    def retry(
        self,
        target: Response | Message | AsyncMessage | str,
        *,
        model: Models | EffortRef | ModelRef | ModelSelection | str | None = None,
        **options: Any,
    ) -> MessageOperation:
        with self._history_lock:
            msg = self._build_retry_message(target, model=model, options=options)
        return MessageOperation(self.chat, msg, executor=self._execute_send)

    def delete(self, target: Response | Message | AsyncMessage | str) -> None:
        with self._history_lock:
            self._delete_local(target)
