import asyncio
import time
from urllib.parse import urlsplit

from ..accounts.credentials import read_session, save_session_async
from ..accounts.filters import ProviderCookieFilter
from ..core import ProviderResilienceRegistry
from ..errors import AccountPaused, ProviderError, RateLimited, RemoteNotFound
from .retry import parse_retry_after
from .worker import DEFAULT_JSON_OFFLOAD_THRESHOLD, BoundedWorkPool

_ORIGIN_RESILIENCE = ProviderResilienceRegistry()
_GLOBAL_WORK_POOL = BoundedWorkPool(max_workers=4)


class HttpSession:
    resilience = _ORIGIN_RESILIENCE
    work_pool = _GLOBAL_WORK_POOL

    def __init__(self, origin, credential_file, required_cookie, *, client_factory=None):
        ProviderCookieFilter.validate_origin(origin)
        self.origin = origin
        self.path = credential_file
        self.required_cookie = required_cookie
        self.factory = client_factory
        self.client = None
        self.authenticated = False
        self._saved_cookies = None
        self._save_lock = asyncio.Lock()
        self.persistence_warning = None

    def connect(self):
        if self.client is None:
            values = read_session(self.path)
            raw_cookies = values.get("cookies", values)
            cookies = ProviderCookieFilter.filter_cookies(
                self.origin, raw_cookies, self.required_cookie
            )
            self._saved_cookies = dict(cookies)
            factory = self.factory
            if factory is None:
                from curl_cffi.requests import AsyncSession

                factory = AsyncSession
            self.client = factory(
                cookies=cookies,
                impersonate="chrome",
                timeout=60,
                headers={
                    "origin": self.origin,
                    "referer": self.origin + "/",
                    "accept": "application/json",
                },
            )
        return self.client

    def url(self, path):
        parsed = urlsplit(path)
        if (
            parsed.scheme
            or parsed.netloc
            or not path.startswith("/")
            or ".." in parsed.path
            or path.startswith("//")
        ):
            raise ValueError("session request must remain on its origin")
        return self.origin + path

    @staticmethod
    def check(response):
        code = response.status_code
        if code == 401 or code in (301, 302, 303, 307, 308):
            raise AccountPaused("Sağlayıcı oturumu sona ermiş; Hesaplar bölümünden doğrulayın")
        if code == 403:
            raise ProviderError("Sağlayıcı bu işleme erişim vermedi (403); istek tekrarlanmadı")
        if code == 404:
            raise RemoteNotFound("Uzak kayıt bulunamadı")
        if code == 429:
            value = response.headers.get("retry-after", "")
            delay = parse_retry_after(value, now=time.time())
            raise RateLimited(delay)
        if not 200 <= code < 300:
            raise ProviderError(f"Sağlayıcı isteği tamamlanamadı (HTTP {code})")

    async def request(self, method, path, **options):
        if _ORIGIN_RESILIENCE.is_rate_limited(self.origin):
            remaining = _ORIGIN_RESILIENCE.rate_limit_remaining(self.origin)
            raise RateLimited(remaining if remaining > 0 else 1.0)
        url = self.url(path)
        try:
            response = await self.connect().request(method, url, allow_redirects=False, **options)
            self.check(response)
        except RateLimited as exc:
            delay = getattr(exc, "retry_after", None)
            if delay and isinstance(delay, (int, float)) and delay > 0:
                _ORIGIN_RESILIENCE.register_rate_limit(self.origin, float(delay))
            raise
        self.authenticated = True
        await self.persist()
        if response.status_code == 204 or not response.content:
            return None
        if len(response.content) > 16 * 1024 * 1024:
            raise ProviderError("Sağlayıcı yanıtı boyut sınırını aştı")
        try:
            if len(response.content) >= DEFAULT_JSON_OFFLOAD_THRESHOLD:
                return await asyncio.to_thread(response.json)
            return response.json()
        except ValueError:
            raise ProviderError("Sağlayıcı yanıt biçimi değişmiş") from None

    async def persist(self):
        if self.client is None or self._saved_cookies is None:
            return
        async with self._save_lock:
            jar = getattr(self.client.cookies, "jar", None)
            if jar is None:
                cookies = {
                    k: self.client.cookies.get(k) or v for k, v in self._saved_cookies.items()
                }
            else:
                cookies = ProviderCookieFilter.filter_from_jar(
                    self.origin, jar, self._saved_cookies
                )
            if cookies != self._saved_cookies:
                try:
                    await save_session_async(self.path, cookies)
                    self._saved_cookies = cookies
                except OSError:
                    self.persistence_warning = "Oturum yenilendi ancak yerel kayıt güncellenemedi"

    async def close(self):
        client = self.client
        if client is not None:
            try:
                await self.persist()
            finally:
                self.client = None
                await client.close()


WebSession = HttpSession
