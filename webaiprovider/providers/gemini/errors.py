from ...errors import CompatibilityError, ProviderError, RemoteNotFound, StreamError


class GeminiError(ProviderError):
    code = "gemini_error"


class GeminiNotFoundError(RemoteNotFound, GeminiError):
    code = "gemini_not_found"


class GeminiCompatibilityError(CompatibilityError, GeminiError):
    code = "gemini_compatibility"


class GeminiStreamError(StreamError, GeminiError):
    code = "gemini_stream_error"
