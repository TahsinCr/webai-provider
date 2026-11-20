from __future__ import annotations

from ...provider import Provider
from .async_provider import AsyncClaudeProvider
from .base import BaseClaudeProvider
from .resources import (
    ClaudeChat,
    ClaudeMessage,
    ClaudeProject,
)


class ClaudeProvider(BaseClaudeProvider, Provider):
    async_provider_type = AsyncClaudeProvider
    chat_wrapper_type = ClaudeChat
    project_wrapper_type = ClaudeProject
    message_wrapper_type = ClaudeMessage
