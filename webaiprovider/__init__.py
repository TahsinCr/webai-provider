from __future__ import annotations

from .accounts.account import Account
from .accounts.credentials import CredentialStore, FileCredentialStore, MemoryCredentialStore
from .async_provider import AsyncProvider
from .base import BaseProvider
from .catalogs import Models, ModelsCollection, SelectionResolver, StaticModelCatalog
from .collections.base import AsyncQueryableCollection, ManageableCollection, QueryableCollection
from .collections.chats import ChatsCollection
from .collections.messages import MessagesCollection
from .collections.project_chats import ProjectChatsCollection
from .collections.projects import ProjectsCollection
from .contracts import (
    Artifact,
    ArtifactDiscovered,
    Attribution,
    EffortRef,
    EffortSet,
    ModelRef,
    ModelSelection,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    Source,
    SourceDiscovered,
    StreamEvent,
    TerminalStatus,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
    TimingMetrics,
)
from .core.state import OperationState
from .core.telemetry import ProviderTelemetry
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
from .models import Capability
from .provider import (
    Provider,
    SyncChat,
    SyncChatsCollection,
    SyncMessageOperation,
    SyncMessagesCollection,
    SyncModelsCollection,
    SyncProject,
    SyncProjectChatsCollection,
    SyncProjectInstruction,
    SyncProjectResources,
    SyncProjectsCollection,
)
from .provider_type import ProviderType
from .providers.chatgpt.catalogs import ChatGPTModels
from .providers.chatgpt.collections import AsyncChatGPTProjectsCollection
from .providers.chatgpt.errors import ChatGPTWebError, SentinelCompatibilityError
from .providers.chatgpt.provider import AsyncChatGPTProvider, ChatGPTProvider
from .providers.chatgpt.resources import AsyncChatGPTChat
from .providers.claude.catalogs import ClaudeModels
from .providers.claude.provider import AsyncClaudeProvider, ClaudeProvider
from .providers.claude.resources import AsyncClaudeChat
from .providers.gemini.catalogs import GeminiModels
from .providers.gemini.provider import AsyncGeminiProvider, GeminiProvider
from .providers.gemini.resources import AsyncGeminiChat
from .providers.grok.catalogs import GrokModels
from .providers.grok.provider import AsyncGrokProvider, GrokProvider
from .providers.grok.resources import AsyncGrokChat
from .resources.attachments import Attachment, Attachments
from .resources.chat import Chat
from .resources.message import Message
from .resources.operation import MessageOperation, OperationHooks
from .resources.project import Project
from .resources.response import Response

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
    "SelectionResolver",
    "StaticModelCatalog",
    "AsyncQueryableCollection",
    "QueryableCollection",
    "ManageableCollection",
    "ChatsCollection",
    "ProjectsCollection",
    "ProjectChatsCollection",
    "MessagesCollection",
    "AsyncChatGPTProjectsCollection",
    "Artifact",
    "ArtifactDiscovered",
    "Attribution",
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
    "ThinkingEnd",
    "TimingMetrics",
    "AccountPaused",
    "AmbiguousMatchError",
    "ChatGPTWebError",
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
    "AsyncChatGPTChat",
    "AsyncClaudeChat",
    "AsyncGeminiChat",
    "AsyncGrokChat",
    "Message",
    "MessageOperation",
    "OperationHooks",
    "OperationState",
    "Project",
    "Response",
    "ProviderTelemetry",
    "SyncChat",
    "SyncChatsCollection",
    "SyncMessageOperation",
    "SyncMessagesCollection",
    "SyncModelsCollection",
    "SyncProject",
    "SyncProjectChatsCollection",
    "SyncProjectInstruction",
    "SyncProjectResources",
    "SyncProjectsCollection",
]
