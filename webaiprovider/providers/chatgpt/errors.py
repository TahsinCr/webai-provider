from ...errors import AuthenticationError, WebAIProviderError


class ChatGPTError(WebAIProviderError):
    code = "chatgpt_error"


class ChatGPTAuthenticationError(ChatGPTError, AuthenticationError):
    code = "chatgpt_authentication_error"


class SentinelCompatibilityError(ChatGPTError):
    code = "chatgpt_sentinel_incompatible"
    message_submitted = False
