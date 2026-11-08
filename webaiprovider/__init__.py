from __future__ import annotations

from .accounts.account import Account
from .accounts.credentials import (
    CredentialStore,
    FileCredentialStore,
    MemoryCredentialStore,
)
from .catalogs.base import StaticModelCatalog
from .catalogs.chatgpt import ChatGPTModels
from .catalogs.claude import ClaudeModels
from .catalogs.dynamic import ModelsCollection
from .catalogs.gemini import GeminiModels
from .catalogs.grok import GrokModels
from .catalogs.policies import Models
from .catalogs.resolver import SelectionResolver
from .collections.base import (
    AsyncQueryableCollection,
    ManageableCollection,
    QueryableCollection,
)
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
from .facade import (
    ChatGPTProvider,
    ClaudeProvider,
    GeminiProvider,
    GrokProvider,
)
from .models import Capability
from .provider_type import ProviderType
from .providers.chatgpt.collections import ChatGPTProjectsCollection
from .providers.chatgpt.errors import (
    ChatGPTWebError,
    SentinelCompatibilityError,
)
from .resources.attachments import Attachment, Attachments
from .resources.chat import Chat
from .resources.message import Message
from .resources.operation import MessageOperation, OperationHooks
from .resources.project import Project
from .resources.response import Response
from .session import Provider
from .sync import (
    SyncChat,
    SyncChatGPTProjectsCollection,
    SyncChatGPTProvider,
    SyncChatsCollection,
    SyncClaudeProvider,
    SyncGeminiProvider,
    SyncGrokProvider,
    SyncMessageOperation,
    SyncMessagesCollection,
    SyncModelsCollection,
    SyncProject,
    SyncProjectChatsCollection,
    SyncProjectInstruction,
    SyncProjectResources,
    SyncProjectsCollection,
    SyncProvider,
)

__version__ = "1.3.0b0"

__all__ = [
    "__version__",
    "Account",
    "CredentialStore",
    "FileCredentialStore",
    "MemoryCredentialStore",
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
    "ChatGPTProvider",
    "ClaudeProvider",
    "GeminiProvider",
    "GrokProvider",
    "SyncProvider",
    "SyncChatGPTProvider",
    "SyncChatGPTProjectsCollection",
    "SyncClaudeProvider",
    "SyncGeminiProvider",
    "SyncGrokProvider",
    "SyncChat",
    "SyncChatsCollection",
    "SyncMessageOperation",
    "SyncMessagesCollection",
    "SyncModelsCollection",
    "SyncProject",
    "SyncProjectChatsCollection",
    "SyncProjectInstruction",
    "SyncProjectsCollection",
    "SyncProjectResources",
    "ProviderType",
    "ChatGPTProjectsCollection",
    "Attachment",
    "Attachments",
    "Chat",
    "Message",
    "MessageOperation",
    "OperationHooks",
    "OperationState",
    "Project",
    "Response",
    "Provider",
    "ProviderTelemetry",
]
