from ...errors import CompatibilityError, ProviderError, StreamError


class GeminiError(ProviderError):
    code = "gemini_error"


class GeminiCompatibilityError(CompatibilityError, GeminiError):
    code = "gemini_compatibility"


class GeminiStreamError(StreamError, GeminiError):
    code = "gemini_stream_error"
