from __future__ import annotations

from enum import Enum

from eventflow import StateMachine


class OperationState(str, Enum):
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    SENDING = "SENDING"
    STREAMING = "STREAMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


_OPERATION_TRANSITIONS: dict[str, set[str]] = {
    OperationState.PENDING.value: {
        OperationState.PREPARING.value,
        OperationState.CANCELLED.value,
    },
    OperationState.PREPARING.value: {
        OperationState.SENDING.value,
        OperationState.FAILED.value,
        OperationState.CANCELLED.value,
    },
    OperationState.SENDING.value: {
        OperationState.STREAMING.value,
        OperationState.COMPLETED.value,
        OperationState.FAILED.value,
        OperationState.CANCELLED.value,
    },
    OperationState.STREAMING.value: {
        OperationState.COMPLETED.value,
        OperationState.FAILED.value,
        OperationState.CANCELLED.value,
    },
    OperationState.COMPLETED.value: set(),
    OperationState.FAILED.value: set(),
    OperationState.CANCELLED.value: set(),
}

_TERMINAL_STATES: set[str] = {
    OperationState.COMPLETED.value,
    OperationState.FAILED.value,
    OperationState.CANCELLED.value,
}


class OperationStateMachine:
    __slots__ = ("_machine",)

    def __init__(self, initial: OperationState | str = OperationState.PENDING) -> None:
        init_val = initial.value if isinstance(initial, OperationState) else str(initial)
        self._machine = StateMachine(
            initial=init_val,
            transitions=_OPERATION_TRANSITIONS,
            terminal=_TERMINAL_STATES,
        )

    @property
    def state(self) -> str:
        return self._machine.state

    def transition(self, target: OperationState | str) -> str:
        target_val = target.value if isinstance(target, OperationState) else str(target)
        self._machine.transition(target_val)
        return self._machine.state

    def can_transition(self, target: OperationState | str) -> bool:
        target_val = target.value if isinstance(target, OperationState) else str(target)
        return self._machine.can_transition(target_val)

    @property
    def is_terminal(self) -> bool:
        return self._machine.state in _TERMINAL_STATES
