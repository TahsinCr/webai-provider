from __future__ import annotations

from ...provider import Provider
from .async_provider import AsyncGrokProvider
from .base import BaseGrokProvider
from .resources import (
    GrokChat,
    GrokMessage,
    GrokProject,
)


class GrokProvider(BaseGrokProvider, Provider):
    async_provider_type = AsyncGrokProvider
    chat_wrapper_type = GrokChat
    project_wrapper_type = GrokProject
    message_wrapper_type = GrokMessage
