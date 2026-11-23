from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from flowpipeline import AsyncPipeline, Pipeline, PipelineExecutionError

from ...core.types import OperationContext
from ...errors import RemoteNotFound, UnsupportedOperationError
from ..chat.types import DEFAULT_CHAT_TITLE
from ..model.model import EffortRef, ModelRef, ModelSelection
from .protocol import ProviderRequest
from .types import MessageAction


def run_pipeline(
    pipeline: Pipeline[OperationContext], context: OperationContext
) -> OperationContext:
    """Run a blocking Pipeline while preserving original cause exceptions at API boundaries."""
    try:
        return pipeline.run(context)
    except PipelineExecutionError as exc:
        if exc.__cause__ is not None:
            raise exc.__cause__ from None
        raise


async def async_run_pipeline(
    pipeline: AsyncPipeline[OperationContext], context: OperationContext
) -> OperationContext:
    """Run an AsyncPipeline while preserving original cause exceptions at API boundaries."""
    try:
        return await pipeline.run(context)
    except PipelineExecutionError as exc:
        if exc.__cause__ is not None:
            raise exc.__cause__ from None
        raise


def _normalize_input(context: OperationContext) -> OperationContext:
    message = context.parameters["message"]
    chat = context.parameters["chat"]
    metadata = getattr(message, "metadata", {})
    raw_action = metadata.get("action")
    try:
        action = MessageAction(raw_action) if raw_action is not None else None
    except ValueError:
        raise UnsupportedOperationError("Unsupported message action") from None
    context.parameters["action"] = action
    context.parameters["parent_message_id"] = metadata.get("parent_message_id")
    context.parameters["temporary"] = bool(chat.temporary or metadata.get("temporary", False))
    context.parameters["conversation_id"] = chat.remote_id
    context.parameters["project_id"] = (
        (chat.project.remote_id or chat.project.id) if chat.project is not None else None
    )
    if context.prompt is None:
        context.prompt = message.content
    if context.model is None:
        context.model = message.model
    return context


def _validate_request(
    context: OperationContext, *, provider_id: str, account_id: str
) -> OperationContext:
    chat = context.parameters["chat"]
    action = context.parameters["action"]
    message_id = context.parameters["parent_message_id"]
    if action is not None:
        if action not in (MessageAction.EDIT, MessageAction.RETRY):
            raise UnsupportedOperationError("Unsupported message action")
        if not chat.remote_id or not message_id:
            raise UnsupportedOperationError("Message actions require remote message identities")
    provider_account = getattr(getattr(chat, "provider", None), "account", None)
    if provider_account is not None and (
        provider_account.provider != provider_id or provider_account.id != account_id
    ):
        raise ValueError("message operation belongs to a different provider account")
    return context


def _try_resolve_static_model(context: OperationContext, *, provider_id: str) -> bool:
    selected = context.model
    if selected is None:
        context.parameters["resolved_model"] = None
        context.parameters["resolved_effort"] = None
        return True
    if isinstance(selected, ModelSelection):
        target_provider = selected.model.provider
        model_id = selected.model.id
        effort_id = selected.effort.id if selected.effort is not None else None
    elif isinstance(selected, ModelRef):
        target_provider = selected.provider
        model_id = selected.id
        effort_id = None
    elif isinstance(selected, EffortRef):
        target_provider = selected.provider
        model_id = selected.model_id
        effort_id = selected.id
    else:
        return False
    if target_provider != provider_id:
        raise RemoteNotFound(f"{target_provider} model is not available in this catalog")
    context.parameters["resolved_model"] = model_id
    context.parameters["resolved_effort"] = effort_id
    return True


def _prepare_attachments(context: OperationContext) -> OperationContext:
    message = context.parameters["message"]
    attachments = context.attachments or tuple(message.attachments)
    serialized = tuple(
        {
            key: value
            for key, value in {
                "id": item.id,
                "path": str(item.path) if item.path is not None else None,
                "bytes": item._content,
                "name": item.name,
                "mime_type": item.mime_type,
                "size_bytes": getattr(item, "size", None),
                "grok_file": getattr(item, "grok_file", None),
            }.items()
            if value is not None
        }
        for item in attachments
    )
    title = getattr(context.parameters["chat"], "title", None)
    context.request = ProviderRequest(
        prompt=context.prompt if context.prompt is not None else message.content,
        model=context.parameters["resolved_model"],
        effort=context.parameters["resolved_effort"],
        attachments=serialized,
        action=context.parameters["action"],
        message_id=context.parameters["parent_message_id"],
        temporary=context.parameters["temporary"],
        conversation_id=context.parameters["conversation_id"],
        project_id=context.parameters["project_id"],
        draft_title=title if title != DEFAULT_CHAT_TITLE else None,
    )
    return context


def _set_model_selection(context: OperationContext, selection: ModelSelection) -> OperationContext:
    context.parameters["resolved_model"] = selection.model.id
    context.parameters["resolved_effort"] = selection.effort.id if selection.effort else None
    return context


def build_message_request_pipeline(
    *,
    provider_id: str,
    account_id: str,
    model_resolver: Callable[[Any], ModelSelection],
) -> Pipeline[OperationContext]:
    """Build the four-stage blocking request preparation pipeline for chat sends."""
    pipeline: Pipeline[OperationContext] = Pipeline()

    def validate_request(context: OperationContext) -> OperationContext:
        return _validate_request(context, provider_id=provider_id, account_id=account_id)

    def resolve_model(context: OperationContext) -> OperationContext:
        if _try_resolve_static_model(context, provider_id=provider_id):
            return context
        chat = context.parameters["chat"]
        if getattr(chat, "provider", None) is None:
            raise UnsupportedOperationError("Model selection requires an active provider session")
        resolved = model_resolver(context.model)
        return _set_model_selection(context, resolved)

    pipeline.add("normalize_input", _normalize_input)
    pipeline.add("validate_request", validate_request)
    pipeline.add("resolve_model", resolve_model)
    pipeline.add("prepare_attachments", _prepare_attachments)
    return pipeline


def async_build_message_request_pipeline(
    *,
    provider_id: str,
    account_id: str,
    model_resolver: Callable[[Any], Awaitable[ModelSelection]],
) -> AsyncPipeline[OperationContext]:
    """Build the four-stage async request preparation pipeline for chat sends."""
    pipeline: AsyncPipeline[OperationContext] = AsyncPipeline()

    async def normalize_input(context: OperationContext) -> OperationContext:
        return _normalize_input(context)

    async def validate_request(context: OperationContext) -> OperationContext:
        return _validate_request(context, provider_id=provider_id, account_id=account_id)

    async def resolve_model(context: OperationContext) -> OperationContext:
        if _try_resolve_static_model(context, provider_id=provider_id):
            return context
        chat = context.parameters["chat"]
        if getattr(chat, "provider", None) is None:
            raise UnsupportedOperationError("Model selection requires an active provider session")
        resolved = await model_resolver(context.model)
        return _set_model_selection(context, resolved)

    async def prepare_attachments(context: OperationContext) -> OperationContext:
        return _prepare_attachments(context)

    pipeline.add("normalize_input", normalize_input)
    pipeline.add("validate_request", validate_request)
    pipeline.add("resolve_model", resolve_model)
    pipeline.add("prepare_attachments", prepare_attachments)
    return pipeline
