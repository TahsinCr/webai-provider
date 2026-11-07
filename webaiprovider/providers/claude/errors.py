from ...errors import CompatibilityError, ProviderError, RequestNotSentError, StreamError


class ClaudeError(ProviderError):
    code = "claude_error"


class ClaudeCompatibilityError(CompatibilityError, ClaudeError):
    code = "claude_compatibility"


class ClaudeRequestNotSent(RequestNotSentError, ClaudeError):
    code = "claude_request_not_sent"


class ClaudeStreamError(StreamError, ClaudeError):
    code = "claude_stream_error"
