from ...errors import ProviderError


class ChatGPTError(ProviderError):
    code = "chatgpt_error"


ChatGPTWebError = ChatGPTError


class SentinelCompatibilityError(ChatGPTError):
    code = "chatgpt_sentinel_incompatible"
    message_submitted = False
