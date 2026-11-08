from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Capability


class ProviderError(RuntimeError):
    code = "provider_error"
    message_submitted: bool | None = None


class NetworkError(ProviderError):
    code = "network_error"
    message_submitted = False


class RequestNotSentError(ProviderError):
    code = "request_not_sent"
    message_submitted = False


class StreamError(ProviderError):
    code = "stream_error"


class AuthenticationError(ProviderError):
    code = "authentication_error"


class AccountPaused(AuthenticationError):
    code = "account_paused"


class FeatureUnavailable(ProviderError):
    code = "feature_unavailable"

    def __init__(self, capability: Capability, reason: str = "unsupported"):
        super().__init__(f"{capability.value}: {reason}")
        self.capability = capability
        self.reason = reason


class RateLimited(ProviderError):
    code = "rate_limited"

    def __init__(self, retry_after: float | None = None):
        suffix = (
            f"; retry after {retry_after:g} seconds"
            if isinstance(retry_after, (int, float))
            else ""
        )
        super().__init__(f"provider rate limit{suffix}")
        self.retry_after = retry_after


class CompatibilityError(ProviderError):
    code = "compatibility_error"

    def __init__(self, message: str = "provider capability changed") -> None:
        super().__init__(message)


class RemoteNotFound(ProviderError):
    code = "remote_not_found"


class ProfileError(ProviderError):
    code = "profile_error"
    default_message = "Could not read the browser profile (unexpected error); check Python dependencies and profile access."

    def __init__(self, message: str | None = None):
        super().__init__(message or self.default_message)


class ProfileNotFoundError(ProfileError):
    code = "profile_not_found"
    default_message = "The default browser profile was not found"


class ProfileSessionNotFoundError(ProfileError):
    code = "profile_session_not_found"
    default_message = "No active provider session was found in this profile."


class ProfileDependencyError(ProfileError):
    code = "profile_dependency_missing"
    default_message = "Browser profile reader dependencies are missing; update dependencies in the application's Python environment."


class ProfilePermissionError(ProfileError):
    code = "profile_permission_denied"
    default_message = (
        "Access to the profile file was denied; run the application as the profile owner."
    )


class ProfileDatabaseError(ProfileError):
    code = "profile_database_error"
    default_message = "Could not read the cookie database; close the browser and try again."


class ProfileKeyringError(ProfileError):
    code = "profile_keyring_error"
    default_message = "Could not access the system keyring; check the desktop session and whether the keyring is unlocked."


class ProfileDecryptionError(ProfileError):
    code = "profile_decryption_error"
    default_message = "Could not decrypt browser cookies; keyring access or browser encryption may be incompatible."


class ProfileTimeoutError(ProfileError):
    code = "profile_timeout"
    default_message = "Timed out reading the profile; unlock the system keyring."


class ProfileInvalidResultError(ProfileError):
    code = "profile_invalid_result"
    default_message = "The profile reader returned an invalid result."


class AmbiguousMatchError(ProviderError):
    code = "ambiguous_match"


class UnsupportedOperationError(ProviderError):
    code = "unsupported_operation"


class OperationCancelledError(ProviderError):
    code = "operation_cancelled"


class ConfigurationError(ProviderError):
    code = "configuration_error"


class ContentError(ProviderError):
    code = "content_error"
