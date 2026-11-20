from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from ..core.async_hooks import AsyncOperationHooks
from ..core.types import OperationContext
from ..errors import OperationCancelledError

_log = logging.getLogger(__name__)


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
