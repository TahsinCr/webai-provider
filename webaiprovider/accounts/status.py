from dataclasses import dataclass
from enum import Enum


class SessionState(str, Enum):
    UNKNOWN = "unknown"
    LOGIN_REQUIRED = "login_required"
    AUTHENTICATED = "authenticated"


@dataclass(frozen=True)
class SessionStatus:
    state: SessionState
    saved_snapshot: bool = False
    reason: str | None = None
