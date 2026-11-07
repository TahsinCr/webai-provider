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
    ManageableCollection,
    QueryableCollection,
    ResourceCollection,
)
from .collections.chats import ChatsCollection
from .collections.messages import MessagesCollection
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
from .errors import (
    AccountPaused,
    AmbiguousMatchError,
    CapabilityError,
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
    AsyncChatGPTProvider,
    AsyncClaudeProvider,
    AsyncGeminiProvider,
    AsyncGrokProvider,
    BaseAsyncProvider,
    BaseProvider,
    ChatGPTProvider,
    ClaudeProvider,
    GeminiProvider,
    GrokProvider,
)
from .models import Capability
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
from .session import ProviderSession

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
    "QueryableCollection",
    "ManageableCollection",
    "ResourceCollection",
    "ChatsCollection",
    "ProjectsCollection",
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
    "CapabilityError",
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
    "AsyncChatGPTProvider",
    "AsyncClaudeProvider",
    "AsyncGeminiProvider",
    "AsyncGrokProvider",
    "BaseAsyncProvider",
    "BaseProvider",
    "ChatGPTProvider",
    "ClaudeProvider",
    "GeminiProvider",
    "GrokProvider",
    "Attachment",
    "Attachments",
    "Chat",
    "Message",
    "MessageOperation",
    "OperationHooks",
    "Project",
    "Response",
    "ProviderSession",
]
