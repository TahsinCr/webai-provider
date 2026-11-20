from __future__ import annotations

from ...provider import Provider
from .async_provider import AsyncGeminiProvider
from .base import BaseGeminiProvider
from .resources import (
    GeminiChat,
    GeminiMessage,
    GeminiProject,
)


class GeminiProvider(BaseGeminiProvider, Provider):
    async_provider_type = AsyncGeminiProvider
    chat_wrapper_type = GeminiChat
    project_wrapper_type = GeminiProject
    message_wrapper_type = GeminiMessage
