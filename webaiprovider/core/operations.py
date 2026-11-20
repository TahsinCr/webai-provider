from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from eventflow import AsyncPipeline, PipelineExecutionError

from ..errors import OperationCancelledError, RemoteNotFound, UnsupportedOperationError
from ..resources.chat.types import DEFAULT_CHAT_TITLE
from ..resources.message.protocol import ProviderRequest
from ..resources.message.types import MessageAction
from ..resources.model.model import EffortRef, ModelRef, ModelSelection
from .async_hooks import AsyncOperationHooks
from .types import OperationContext

_log = logging.getLogger(__name__)


async def run_pipeline_unwrapped(
    pipeline: AsyncPipeline[OperationContext], context: OperationContext
) -> OperationContext:
    """Run an AsyncPipeline while preserving original cause exceptions at API boundaries."""
    try:
        return await pipeline.run(context)
    except PipelineExecutionError as exc:
        if exc.__cause__ is not None:
            raise exc.__cause__ from None
        raise


def build_message_request_pipeline(
    *,
    provider_id: str,
    account_id: str,
    model_resolver: Callable[[Any], Awaitable[ModelSelection]],
) -> AsyncPipeline[OperationContext]:
    """Build the four-stage request preparation pipeline for chat sends."""
    pipeline: AsyncPipeline[OperationContext] = AsyncPipeline()

    async def normalize_input(context: OperationContext) -> OperationContext:
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

    async def validate_request(context: OperationContext) -> OperationContext:
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

    async def resolve_model(context: OperationContext) -> OperationContext:
        selected = context.model
        if selected is None:
            context.parameters["resolved_model"] = None
            context.parameters["resolved_effort"] = None
            return context

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
            target_provider = None
            model_id = None
            effort_id = None

        if target_provider is not None:
            if target_provider != provider_id:
                raise RemoteNotFound(f"{target_provider} model is not available in this catalog")
            context.parameters["resolved_model"] = model_id
            context.parameters["resolved_effort"] = effort_id
            return context

        chat = context.parameters["chat"]
        if getattr(chat, "provider", None) is None:
            raise UnsupportedOperationError("Model selection requires an active provider session")
        resolved = await model_resolver(selected)
        context.parameters["resolved_model"] = resolved.model.id
        context.parameters["resolved_effort"] = (
            resolved.effort.id if resolved.effort is not None else None
        )
        return context

    async def prepare_attachments(context: OperationContext) -> OperationContext:
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

    pipeline.add("normalize_input", normalize_input)
    pipeline.add("validate_request", validate_request)
    pipeline.add("resolve_model", resolve_model)
    pipeline.add("prepare_attachments", prepare_attachments)
    return pipeline


async def execute_with_hooks(
    *,
    operation_name: str,
    context: OperationContext,
    hooks_chain: Sequence[AsyncOperationHooks],
    work: Callable[[OperationContext], Awaitable[Any]],
    on_success_event: Callable[[OperationContext], Awaitable[None]] | None = None,
    on_cancelled_event: Callable[[OperationContext], Awaitable[None]] | None = None,
    on_error_event: Callable[[OperationContext], Awaitable[None]] | None = None,
) -> Any:
    """Execute one operation through before -> work -> success/error/cancelled -> finally."""
    cancelled = False
    try:
        context = await AsyncOperationHooks.run_before(hooks_chain, operation_name, context)
        result = await work(context)
        context.result = result
        if on_success_event is not None:
            # The remote work already succeeded; an observer failure must not turn it into an error.
            try:
                await on_success_event(context)
            except Exception as event_error:
                context.metadata["success_event_error"] = type(event_error).__name__
                _log.warning(
                    "Success event dispatch failed: operation=%s error_type=%s",
                    operation_name,
                    type(event_error).__name__,
                )
        await AsyncOperationHooks.run_lifecycle(hooks_chain, "on_success", operation_name, context)
        return context.result
    except (asyncio.CancelledError, OperationCancelledError) as error:
        cancelled = True
        context.cancelled = True
        context.error = error
        if on_cancelled_event is not None:
            await asyncio.shield(on_cancelled_event(context))
        await asyncio.shield(
            AsyncOperationHooks.run_lifecycle(hooks_chain, "on_cancelled", operation_name, context)
        )
        raise
    except Exception as error:
        context.error = error
        if on_error_event is not None:
            await on_error_event(context)
        await AsyncOperationHooks.run_lifecycle(hooks_chain, "on_error", operation_name, context)
        raise
    finally:
        if cancelled:
            await asyncio.shield(
                AsyncOperationHooks.run_lifecycle(
                    hooks_chain, "on_finally", operation_name, context
                )
            )
        else:
            await AsyncOperationHooks.run_lifecycle(
                hooks_chain, "on_finally", operation_name, context
            )
