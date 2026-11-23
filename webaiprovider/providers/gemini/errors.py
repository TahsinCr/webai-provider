from ...errors import (
    AccountPaused,
    CompatibilityError,
    RemoteNotFound,
    StreamError,
    WebAIProviderError,
)


class GeminiError(WebAIProviderError):
    code = "gemini_error"


class GeminiSignInRequired(AccountPaused, GeminiError):
    code = "gemini_sign_in_required"


class GeminiGenerationRejected(GeminiError):
    code = "gemini_generation_rejected"


class GeminiNotFoundError(RemoteNotFound, GeminiError):
    code = "gemini_not_found"


class GeminiCompatibilityError(CompatibilityError, GeminiError):
    code = "gemini_compatibility"


class GeminiStreamError(StreamError, GeminiError):
    code = "gemini_stream_error"
