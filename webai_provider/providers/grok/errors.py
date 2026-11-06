from ...errors import CompatibilityError, ProviderError, StreamError


class GrokError(ProviderError):
    code = "grok_error"


class GrokCompatibilityError(CompatibilityError, GrokError):
    code = "grok_compatibility"


class GrokStreamError(StreamError, GrokError):
    code = "grok_stream_error"
