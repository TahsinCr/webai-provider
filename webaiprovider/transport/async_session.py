"""Asynchronous HTTP session for saved accounts."""

import asyncio

from ..accounts.credentials import read_session_async, save_session_async
from ..accounts.filters import ProviderCookieFilter
from .async_http import AsyncHTTPTransport
from .cookie import _SessionCookies


class AsyncHttpSession(_SessionCookies, AsyncHTTPTransport):
    def __init__(
        self, origin, credential_file, required_cookie, *, client_factory=None, config=None
    ):
        ProviderCookieFilter.validate_origin(origin)
        super().__init__(
            origin,
            client_factory=client_factory,
            after_response=self._persist_after_response,
            config=config,
        )
        self._init_session(credential_file, required_cookie)
        self._connect_lock = asyncio.Lock()
        self._save_lock = asyncio.Lock()

    def _create_client(self, cookies):
        from curl_cffi.requests import AsyncSession

        return self._build_client(cookies, AsyncSession)

    async def _connect(self):
        if self.client is not None:
            return self.client
        async with self._connect_lock:
            if self.client is None:
                values = await read_session_async(self.path)
                self.client = self._create_client(self._load_initial_cookies(values))
        return self.client

    async def get_saved_cookies(self) -> dict[str, str]:
        async with self._activity():
            await self._connect()
            return dict(self._saved_cookies or {})

    async def _reset_owned_client(self, failed_client):
        if (
            self._client_getter is None
            and self.client is failed_client
            and (self._dirty or self.client is not None)
        ):
            await self.persist(force=True)
        await super()._reset_owned_client(failed_client)

    async def send(self, method, path, **options):
        response = await super().send(method, path, **options)
        self.authenticated = True
        return response

    async def _persist_after_response(self):
        await self.persist(force=False)

    async def persist(self, *, force: bool = True):
        async with self._save_lock:
            if (update := self._cookie_update(force=force)) is None:
                return
            cookies, now = update
            try:
                await save_session_async(self.path, cookies)
            except OSError:
                self._warn_persist()
            else:
                self._record_persisted(cookies, now)

    async def _before_close(self):
        if self._dirty or self.client is not None:
            await self.persist(force=True)
