from __future__ import annotations

from collections.abc import Collection, Mapping
from enum import Enum
from typing import Generic, TypeVar

S = TypeVar("S")


class InvalidTransitionError(ValueError):
    """Raised when a state machine receives a disallowed transition."""

    def __init__(self, current: object, target: object) -> None:
        self.current = current
        self.target = target
        super().__init__(f"Invalid state transition: {current!r} -> {target!r}")


class StateMachine(Generic[S]):
    """Minimal finite state machine tailored to provider and operation lifecycles."""

    __slots__ = ("_state", "_transitions", "_terminal")

    def __init__(
        self,
        *,
        initial: S,
        transitions: Mapping[S, Collection[S]],
        terminal: Collection[S] = (),
    ) -> None:
        self._state: S = initial
        self._transitions: dict[S, frozenset[S]] = {
            state: frozenset(targets) for state, targets in transitions.items()
        }
        self._terminal: frozenset[S] = frozenset(terminal)

    @property
    def state(self) -> S:
        return self._state

    @property
    def is_terminal(self) -> bool:
        return self._state in self._terminal

    def can_transition(self, target: S) -> bool:
        if self._state in self._terminal:
            return False
        allowed = self._transitions.get(self._state)
        return allowed is not None and target in allowed

    def transition(self, target: S) -> S:
        if not self.can_transition(target):
            raise InvalidTransitionError(self._state, target)
        self._state = target
        return target


class LifecycleState(str, Enum):
    CREATED = "CREATED"
    OPENING = "OPENING"
    OPEN = "OPEN"
    OPEN_FAILED = "OPEN_FAILED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    CLOSE_FAILED = "CLOSE_FAILED"


_LIFECYCLE_TRANSITIONS: dict[LifecycleState, frozenset[LifecycleState]] = {
    LifecycleState.CREATED: frozenset({LifecycleState.OPENING, LifecycleState.CLOSING}),
    LifecycleState.OPENING: frozenset({LifecycleState.OPEN, LifecycleState.OPEN_FAILED}),
    LifecycleState.OPEN: frozenset({LifecycleState.CLOSING}),
    LifecycleState.OPEN_FAILED: frozenset({LifecycleState.OPENING, LifecycleState.CLOSING}),
    LifecycleState.CLOSING: frozenset({LifecycleState.CLOSED, LifecycleState.CLOSE_FAILED}),
    LifecycleState.CLOSED: frozenset(),
    LifecycleState.CLOSE_FAILED: frozenset({LifecycleState.CLOSING, LifecycleState.CLOSED}),
}
_LIFECYCLE_TERMINAL_STATES: frozenset[LifecycleState] = frozenset({LifecycleState.CLOSED})
