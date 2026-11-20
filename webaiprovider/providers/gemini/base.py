from __future__ import annotations

from typing import Any

from ...base import BaseProvider, Capability, ProviderType


class BaseGeminiProvider(BaseProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = ProviderType.GEMINI

    _capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.HISTORY,
            Capability.DELETE,
            Capability.AUTH,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
            Capability.RENAME,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.RESOURCES,
        }
    )

    @staticmethod
    def _usable_chat_metadata(metadata: Any, conversation_id: str | None) -> bool:
        return (
            isinstance(metadata, (list, tuple))
            and len(metadata) >= 3
            and metadata[0] == conversation_id
            and all(isinstance(metadata[index], str) and metadata[index] for index in (1, 2))
        )
