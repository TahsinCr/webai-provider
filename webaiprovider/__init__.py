from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flowevent import AsyncEventBus, EventBus, EventEnvelope, EventFilter

from .accounts.account import WebAIAccount
from .accounts.credentials import CredentialStore, FileCredentialStore, MemoryCredentialStore
from .base import BaseWebAIProvider, Capability
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
from .core.state import LifecycleState
from .errors import (
    AccountPaused,
    AmbiguousMatchError,
    ConfigurationError,
    ContentError,
    FeatureUnavailable,
    OperationCancelledError,
    ProfileError,
    RateLimited,
    RemoteNotFound,
    UnsupportedOperationError,
    WebAIProviderError,
)
from .provider import WebAIProvider, WebAIProviderType
from .providers import resolve_lazy_export as _resolve_lazy_export
from .providers.chatgpt.catalogs import ChatGPTModels
from .providers.chatgpt.collections import AsyncChatGPTProjectsCollection, ChatGPTProjectsCollection
from .providers.chatgpt.errors import ChatGPTError, SentinelCompatibilityError
from .providers.chatgpt.provider import (
    ChatGPTChat,
    ChatGPTWebAIProvider,
)
from .providers.chatgpt.resources import AsyncChatGPTChat
from .providers.claude.catalogs import ClaudeModels
from .providers.claude.provider import ClaudeWebAIProvider
from .providers.claude.resources import AsyncClaudeChat, ClaudeChat
from .providers.gemini.catalogs import GeminiModels
from .providers.gemini.provider import GeminiWebAIProvider
from .providers.gemini.resources import AsyncGeminiChat, GeminiChat
from .providers.grok.catalogs import GrokModels
from .providers.grok.provider import GrokWebAIProvider
from .providers.grok.resources import AsyncGrokChat, GrokChat
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
    GenerationEnd,
    MessageOperation,
    OperationEvent,
    OperationState,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    ResponsePartsResolved,
    SourceDiscovered,
    StreamEvent,
    TerminalStatus,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
    ThinkingReplace,
    VerificationBegin,
)
from .resources.response.parts import ResponsePart, ResponseParts, TextPart, ThinkingPart
from .resources.response.response import Response, TimingMetrics
from .resources.response.source import Source, SourcesCollection

if TYPE_CHECKING:
    from .async_provider import AsyncWebAIProvider
    from .providers.chatgpt.async_provider import AsyncChatGPTWebAIProvider
    from .providers.claude.async_provider import AsyncClaudeWebAIProvider
    from .providers.gemini.async_provider import AsyncGeminiWebAIProvider
    from .providers.grok.async_provider import AsyncGrokWebAIProvider

_ASYNC_EXPORTS = {
    "AsyncWebAIProvider": "async_provider",
    "AsyncChatGPTWebAIProvider": "providers.chatgpt.async_provider",
    "AsyncClaudeWebAIProvider": "providers.claude.async_provider",
    "AsyncGeminiWebAIProvider": "providers.gemini.async_provider",
    "AsyncGrokWebAIProvider": "providers.grok.async_provider",
}


def __getattr__(name: str) -> Any:
    return _resolve_lazy_export(__name__, _ASYNC_EXPORTS, name, globals())


def __dir__() -> list[str]:
    return sorted(set(globals()) | _ASYNC_EXPORTS.keys())


__version__ = "2.0.0"

__all__ = [
    "__version__",
    "WebAIAccount",
    "CredentialStore",
    "FileCredentialStore",
    "MemoryCredentialStore",
    "BaseWebAIProvider",
    "WebAIProvider",
    "AsyncWebAIProvider",
    "WebAIProviderType",
    "ChatGPTWebAIProvider",
    "AsyncChatGPTWebAIProvider",
    "ClaudeWebAIProvider",
    "AsyncClaudeWebAIProvider",
    "GeminiWebAIProvider",
    "AsyncGeminiWebAIProvider",
    "GrokWebAIProvider",
    "AsyncGrokWebAIProvider",
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
    "GenerationEnd",
    "ResponsePart",
    "ResponseParts",
    "TextPart",
    "ThinkingPart",
    "VerificationBegin",
    "ResponsePartsResolved",
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
    "WebAIProviderError",
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
    "ClaudeChat",
    "AsyncClaudeChat",
    "GeminiChat",
    "AsyncGeminiChat",
    "GrokChat",
    "AsyncGrokChat",
    "Message",
    "AsyncMessage",
    "MessageOperation",
    "AsyncMessageOperation",
    "AsyncOperationHooks",
    "OperationHooks",
    "OperationEvent",
    "OperationState",
    "Project",
    "AsyncProject",
    "ProjectInstruction",
    "AsyncProjectInstruction",
    "Response",
]
