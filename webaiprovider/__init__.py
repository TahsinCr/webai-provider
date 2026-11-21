from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventflow import AsyncEventBus, EventBus, EventEnvelope, EventFilter

from .accounts.account import Account
from .accounts.credentials import CredentialStore, FileCredentialStore, MemoryCredentialStore
from .base import BaseProvider, Capability
from .collections import (
    AsyncBaseCollection,
    AsyncCollection,
    AsyncRemoteCollection,
    BaseCollection,
    Collection,
    RemoteCollection,
)
from .core.async_hooks import AsyncOperationHooks
from .core.hooks import OperationHooks
from .core.state import LifecycleState, OperationState
from .errors import (
    AccountPaused,
    AmbiguousMatchError,
    ConfigurationError,
    ContentError,
    FeatureUnavailable,
    OperationCancelledError,
    ProfileError,
    ProviderError,
    RateLimited,
    RemoteNotFound,
    UnsupportedOperationError,
)
from .provider import Provider, ProviderType
from .providers import resolve_lazy_export as _resolve_lazy_export
from .providers.chatgpt.catalogs import ChatGPTModels
from .providers.chatgpt.collections import AsyncChatGPTProjectsCollection, ChatGPTProjectsCollection
from .providers.chatgpt.errors import ChatGPTError, SentinelCompatibilityError
from .providers.chatgpt.provider import (
    ChatGPTChat,
    ChatGPTProvider,
)
from .providers.chatgpt.resources import AsyncChatGPTChat
from .providers.claude.catalogs import ClaudeModels
from .providers.claude.provider import ClaudeProvider
from .providers.claude.resources import AsyncClaudeChat
from .providers.gemini.catalogs import GeminiModels
from .providers.gemini.provider import GeminiProvider
from .providers.gemini.resources import AsyncGeminiChat
from .providers.grok.catalogs import GrokModels
from .providers.grok.provider import GrokProvider
from .providers.grok.resources import AsyncGrokChat
from .resources.chat.chat import (
    AsyncBranchableChat,
    AsyncChat,
    AsyncMovableChat,
    BranchableChat,
    Chat,
    MovableChat,
)
from .resources.chat.collections import AsyncChatsCollection, ChatsCollection
from .resources.message.attachment import Attachment, Attachments
from .resources.message.message import AsyncMessage, Message
from .resources.model.collections import AsyncModelsCollection, ModelsCollection
from .resources.model.model import (
    EffortRef,
    EffortSet,
    ModelRef,
    ModelSelection,
)
from .resources.model.resolver import SelectionResolver
from .resources.model.types import Models, StaticModelCatalog
from .resources.project.chats import AsyncProjectChatsCollection, ProjectChatsCollection
from .resources.project.collections import (
    AsyncProjectAttachments,
    AsyncProjectsCollection,
    ProjectAttachments,
    ProjectsCollection,
)
from .resources.project.project import (
    AsyncProject,
    AsyncProjectInstruction,
    Project,
    ProjectInstruction,
)
from .resources.response.artifact import Artifact, ArtifactsCollection
from .resources.response.collections import AsyncResponsesCollection, ResponsesCollection
from .resources.response.operation import (
    ArtifactDiscovered,
    AsyncMessageOperation,
    MessageOperation,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    SourceDiscovered,
    StreamEvent,
    TerminalStatus,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
    ThinkingReplace,
)
from .resources.response.response import Response, TimingMetrics
from .resources.response.source import Source, SourcesCollection

if TYPE_CHECKING:
    from .async_provider import AsyncProvider
    from .providers.chatgpt.async_provider import AsyncChatGPTProvider
    from .providers.claude.async_provider import AsyncClaudeProvider
    from .providers.gemini.async_provider import AsyncGeminiProvider
    from .providers.grok.async_provider import AsyncGrokProvider

_ASYNC_EXPORTS = {
    "AsyncProvider": "async_provider",
    "AsyncChatGPTProvider": "providers.chatgpt.async_provider",
    "AsyncClaudeProvider": "providers.claude.async_provider",
    "AsyncGeminiProvider": "providers.gemini.async_provider",
    "AsyncGrokProvider": "providers.grok.async_provider",
}


def __getattr__(name: str) -> Any:
    return _resolve_lazy_export(__name__, _ASYNC_EXPORTS, name, globals())


def __dir__() -> list[str]:
    return sorted(set(globals()) | _ASYNC_EXPORTS.keys())


__version__ = "1.3.0b0"

__all__ = [
    "__version__",
    "Account",
    "CredentialStore",
    "FileCredentialStore",
    "MemoryCredentialStore",
    "BaseProvider",
    "Provider",
    "AsyncProvider",
    "ProviderType",
    "ChatGPTProvider",
    "AsyncChatGPTProvider",
    "ClaudeProvider",
    "AsyncClaudeProvider",
    "GeminiProvider",
    "AsyncGeminiProvider",
    "GrokProvider",
    "AsyncGrokProvider",
    "Capability",
    "ChatGPTModels",
    "ClaudeModels",
    "GeminiModels",
    "GrokModels",
    "Models",
    "ModelsCollection",
    "AsyncModelsCollection",
    "SelectionResolver",
    "StaticModelCatalog",
    "AsyncEventBus",
    "EventBus",
    "EventEnvelope",
    "EventFilter",
    "LifecycleState",
    "BaseCollection",
    "Collection",
    "RemoteCollection",
    "AsyncBaseCollection",
    "AsyncCollection",
    "AsyncRemoteCollection",
    "ChatsCollection",
    "AsyncChatsCollection",
    "ProjectsCollection",
    "AsyncProjectsCollection",
    "ProjectChatsCollection",
    "AsyncProjectChatsCollection",
    "ProjectAttachments",
    "AsyncProjectAttachments",
    "ResponsesCollection",
    "AsyncResponsesCollection",
    "SourcesCollection",
    "ArtifactsCollection",
    "AsyncChatGPTProjectsCollection",
    "ChatGPTProjectsCollection",
    "Artifact",
    "ArtifactDiscovered",
    "EffortRef",
    "EffortSet",
    "ModelRef",
    "ModelSelection",
    "OutputBegin",
    "OutputDelta",
    "OutputEnd",
    "OutputReplace",
    "Source",
    "SourceDiscovered",
    "StreamEvent",
    "TerminalStatus",
    "ThinkingBegin",
    "ThinkingDelta",
    "ThinkingReplace",
    "ThinkingEnd",
    "TimingMetrics",
    "AccountPaused",
    "AmbiguousMatchError",
    "ChatGPTError",
    "ConfigurationError",
    "ContentError",
    "FeatureUnavailable",
    "OperationCancelledError",
    "ProfileError",
    "ProviderError",
    "RateLimited",
    "RemoteNotFound",
    "SentinelCompatibilityError",
    "UnsupportedOperationError",
    "Attachment",
    "Attachments",
    "Chat",
    "AsyncChat",
    "BranchableChat",
    "AsyncBranchableChat",
    "MovableChat",
    "AsyncMovableChat",
    "ChatGPTChat",
    "AsyncChatGPTChat",
    "AsyncClaudeChat",
    "AsyncGeminiChat",
    "AsyncGrokChat",
    "Message",
    "AsyncMessage",
    "MessageOperation",
    "AsyncMessageOperation",
    "AsyncOperationHooks",
    "OperationHooks",
    "OperationState",
    "Project",
    "AsyncProject",
    "ProjectInstruction",
    "AsyncProjectInstruction",
    "Response",
]
