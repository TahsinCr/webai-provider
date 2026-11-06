from __future__ import annotations

from enum import Enum


class Models(str, Enum):
    AUTO = "auto"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Efforts(str, Enum):
    AUTO = "auto"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
