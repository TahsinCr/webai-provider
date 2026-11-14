from __future__ import annotations

from .accounts.account import Account
from .accounts.credentials import CredentialStore, FileCredentialStore, MemoryCredentialStore
from .async_provider import AsyncProvider
from .base import BaseProvider, Capability
from .collections import (
    AsyncManageableCollection,
    AsyncQueryableCollection,
    ManageableCollection,
    QueryableCollection,
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
from .provider import Provider, ProviderType
from .providers.chatgpt.catalogs import ChatGPTModels
from .providers.chatgpt.collections import AsyncChatGPTProjectsCollection
from .providers.chatgpt.errors import ChatGPTError, SentinelCompatibilityError
from .providers.chatgpt.provider import (
    AsyncChatGPTProvider,
    ChatGPTChat,
    ChatGPTProjectsCollection,
    ChatGPTProvider,
)
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
from .resources.attachment import Attachment
from .resources.chat.chat import (
    AsyncBranchableChat,
    AsyncChat,
    AsyncMovableChat,
    BranchableChat,
    Chat,
    MovableChat,
)
from .resources.chat.collections import AsyncChatsCollection, ChatsCollection
from .resources.message.attachments import Attachments
from .resources.message.collections import AsyncMessagesCollection, MessagesCollection
from .resources.message.message import AsyncMessage, Message
from .resources.message.operation import (
    ArtifactDiscovered,
    AsyncMessageOperation,
    MessageOperation,
    OperationHooks,
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
from .resources.message.response import (
    Artifact,
    Attribution,
    Response,
    Source,
    TimingMetrics,
)
from .resources.model.collections import (
    AsyncModelsCollection,
    Models,
    ModelsCollection,
    SelectionResolver,
    StaticModelCatalog,
)
from .resources.model.model import (
    EffortRef,
    EffortSet,
    ModelRef,
    ModelSelection,
)
from .resources.project.chats import AsyncProjectChatsCollection, ProjectChatsCollection
from .resources.project.collections import AsyncProjectsCollection, ProjectsCollection
from .resources.project.project import (
    AsyncProject,
    AsyncProjectInstruction,
    AsyncProjectResources,
    Project,
    ProjectInstruction,
    ProjectResources,
)

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
    "QueryableCollection",
    "ManageableCollection",
    "AsyncQueryableCollection",
    "AsyncManageableCollection",
    "ChatsCollection",
    "AsyncChatsCollection",
    "ProjectsCollection",
    "AsyncProjectsCollection",
    "ProjectChatsCollection",
    "AsyncProjectChatsCollection",
    "MessagesCollection",
    "AsyncMessagesCollection",
    "AsyncChatGPTProjectsCollection",
    "ChatGPTProjectsCollection",
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
    "OperationHooks",
    "OperationState",
    "Project",
    "AsyncProject",
    "ProjectInstruction",
    "AsyncProjectInstruction",
    "ProjectResources",
    "AsyncProjectResources",
    "Response",
    "ProviderTelemetry",
]
