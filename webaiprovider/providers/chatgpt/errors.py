from ...errors import ProviderError


class ChatGPTError(ProviderError):
    code = "chatgpt_error"


class SentinelCompatibilityError(ChatGPTError):
    code = "chatgpt_sentinel_incompatible"
    message_submitted = False
