from dataclasses import dataclass
from enum import Enum


class FeatureState(str, Enum):
    AVAILABLE = "available"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"
    UNAVAILABLE = "unavailable"
    LOGIN_REQUIRED = "login_required"


class SessionState(str, Enum):
    UNKNOWN = "unknown"
    LOGIN_REQUIRED = "login_required"
    AUTHENTICATED = "authenticated"


@dataclass(frozen=True)
class SessionStatus:
    state: SessionState
    saved_snapshot: bool = False
    reason: str | None = None
