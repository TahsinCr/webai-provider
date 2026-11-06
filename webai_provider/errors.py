from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .api import Capability


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

    def __init__(self, capability_or_message: Any = None):
        if hasattr(capability_or_message, "value"):
            super().__init__(f"provider capability changed: {capability_or_message.value}")
            self.capability = capability_or_message
        elif isinstance(capability_or_message, str):
            super().__init__(capability_or_message)
            self.capability = None
        else:
            super().__init__("provider capability changed")
            self.capability = None


class RemoteNotFound(ProviderError):
    code = "remote_not_found"


class ProfileError(ProviderError):
    code = "profile_error"
    default_message = "Profil okunamadı (beklenmeyen hata); Python bağımlılıklarını ve profil erişimini kontrol edin"

    def __init__(self, message: str | None = None):
        super().__init__(message or self.default_message)


class ProfileNotFoundError(ProfileError):
    code = "profile_not_found"
    default_message = "Varsayılan tarayıcı profili bulunamadı"


class ProfileSessionNotFoundError(ProfileError):
    code = "profile_session_not_found"
    default_message = "Bu profilde sağlayıcıya ait açık oturum bulunamadı"


class ProfileDependencyError(ProfileError):
    code = "profile_dependency_missing"
    default_message = "Profil okuyucu bağımlılıkları eksik; uygulamanın Python ortamında bağımlılıkları güncelleyin"


class ProfilePermissionError(ProfileError):
    code = "profile_permission_denied"
    default_message = "Profil dosyasına erişim reddedildi; uygulamayı profilin sahibi olan kullanıcıyla çalıştırın"


class ProfileDatabaseError(ProfileError):
    code = "profile_database_error"
    default_message = "Çerez veritabanı okunamadı; tarayıcıyı kapatıp yeniden deneyin"


class ProfileKeyringError(ProfileError):
    code = "profile_keyring_error"
    default_message = (
        "Sistem anahtarlığına erişilemedi; masaüstü oturumunu ve anahtarlık kilidini kontrol edin"
    )


class ProfileDecryptionError(ProfileError):
    code = "profile_decryption_error"
    default_message = "Tarayıcı çerezleri çözülemedi; anahtarlık erişimi veya tarayıcı şifrelemesi uyumsuz olabilir"


class ProfileTimeoutError(ProfileError):
    code = "profile_timeout"
    default_message = "Profil okuma zaman aşımına uğradı; sistem anahtarlığının kilidini açın"


class ProfileInvalidResultError(ProfileError):
    code = "profile_invalid_result"
    default_message = "Profil okuma sonucu geçersiz"
