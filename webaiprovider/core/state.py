from __future__ import annotations

from enum import Enum


class LifecycleState(str, Enum):
    CREATED = "CREATED"
    OPENING = "OPENING"
    OPEN = "OPEN"
    OPEN_FAILED = "OPEN_FAILED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    CLOSE_FAILED = "CLOSE_FAILED"


_LIFECYCLE_TRANSITIONS: dict[LifecycleState, set[LifecycleState]] = {
    LifecycleState.CREATED: {LifecycleState.OPENING, LifecycleState.CLOSING},
    LifecycleState.OPENING: {LifecycleState.OPEN, LifecycleState.OPEN_FAILED},
    LifecycleState.OPEN: {LifecycleState.CLOSING},
    LifecycleState.OPEN_FAILED: {LifecycleState.OPENING, LifecycleState.CLOSING},
    LifecycleState.CLOSING: {LifecycleState.CLOSED, LifecycleState.CLOSE_FAILED},
    LifecycleState.CLOSED: set(),
    LifecycleState.CLOSE_FAILED: {LifecycleState.CLOSING, LifecycleState.CLOSED},
}
_LIFECYCLE_TERMINAL_STATES = {LifecycleState.CLOSED}


class OperationState(str, Enum):
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    SENDING = "SENDING"
    STREAMING = "STREAMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


_OPERATION_TRANSITIONS: dict[OperationState, set[OperationState]] = {
    OperationState.PENDING: {OperationState.PREPARING, OperationState.CANCELLED},
    OperationState.PREPARING: {
        OperationState.SENDING,
        OperationState.FAILED,
        OperationState.CANCELLED,
    },
    OperationState.SENDING: {
        OperationState.STREAMING,
        OperationState.COMPLETED,
        OperationState.FAILED,
        OperationState.CANCELLED,
    },
    OperationState.STREAMING: {
        OperationState.COMPLETED,
        OperationState.FAILED,
        OperationState.CANCELLED,
    },
    OperationState.COMPLETED: set(),
    OperationState.FAILED: set(),
    OperationState.CANCELLED: set(),
}
_OPERATION_TERMINAL_STATES = {
    OperationState.COMPLETED,
    OperationState.FAILED,
    OperationState.CANCELLED,
}
