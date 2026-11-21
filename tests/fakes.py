"""Small transport and cookie fakes shared by offline provider tests."""

import asyncio
import json as _json
from contextlib import asynccontextmanager, contextmanager
from http.cookiejar import Cookie, CookieJar
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_UNSET = object()


class _CaseInsensitiveHeaders(dict):
    """Minimal case-insensitive header mapping matching HTTP response headers."""

    def __init__(self, headers=None):
        super().__init__()
        if headers:
            for key, value in dict(headers).items():
                super().__setitem__(str(key).lower(), str(value))

    def get(self, key, default=None):
        return super().get(str(key).lower(), default)

    def __getitem__(self, key):
        return super().__getitem__(str(key).lower())

    def __contains__(self, key):
        return super().__contains__(str(key).lower())


class FakeURL:
    """Minimal parsed URL object supporting ``.path``, ``.params``, and ``str(url)``."""

    def __init__(self, url: str):
        self._raw = url
        parts = urlsplit(url)
        self.scheme = parts.scheme
        self.netloc = parts.netloc
        self.path = parts.path
        self.query = parts.query
        self.params = dict(parse_qsl(parts.query))

    def __str__(self) -> str:
        return self._raw


class FakeCookiesJar:
    """Minimal cookie container exposing ``.jar`` and ``.set()`` for session tests."""

    def __init__(self):
        self.jar = CookieJar()

    def set(self, name: str, value: str, domain: str = "", path: str = "/") -> None:
        self.jar.set_cookie(cookie(name, value, domain=domain or "localhost", expires=None))


class FakeHTTPRequest:
    """Minimal HTTP request passed to ``FakeHTTPClient`` handlers."""

    def __init__(
        self,
        method: str,
        url: str,
        *,
        headers=None,
        content=None,
        data=None,
        json=_UNSET,
    ):
        self.method = method.upper()
        self.url = FakeURL(url)
        self.headers = _CaseInsensitiveHeaders(headers)
        if json is not _UNSET:
            self.content = _json.dumps(json, separators=(",", ":")).encode("utf-8")
        elif content is not None:
            if isinstance(content, (bytes, bytearray)):
                self.content = bytes(content)
            elif isinstance(content, str):
                self.content = content.encode("utf-8")
            else:
                self.content = b"".join(content)
        elif data is not None:
            if isinstance(data, (bytes, bytearray)):
                self.content = bytes(data)
            elif isinstance(data, str):
                self.content = data.encode("utf-8")
            else:
                self.content = urlencode(data).encode("utf-8")
        else:
            self.content = b""


class FakeHTTPResponse:
    """Minimal HTTP response supporting sync/async byte streaming and JSON decoding."""

    def __init__(
        self,
        status_code: int = 200,
        *,
        headers=None,
        content=None,
        text: str | None = None,
        json=_UNSET,
        stream=None,
        cookies=None,
        request=None,
        url: str | None = None,
    ):
        self.status_code = status_code
        self.headers = _CaseInsensitiveHeaders(headers)
        self._stream = stream
        if json is not _UNSET:
            self._content = _json.dumps(json).encode("utf-8")
            self.headers.setdefault("content-type", "application/json")
        elif text is not None:
            self._content = text.encode("utf-8")
        elif content is not None:
            self._content = bytes(content)
        else:
            self._content = b""
        self.cookies = cookies if cookies is not None else FakeCookiesJar()
        self.request = request
        if url is not None:
            self.url = FakeURL(url)
        elif request is not None:
            self.url = request.url
        else:
            self.url = FakeURL("")

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def text(self) -> str:
        return self._content.decode("utf-8", errors="replace")

    def json(self):
        return _json.loads(self._content)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_bytes(self):
        if self._stream is not None:
            yield from self._stream
        elif self._content:
            yield self._content

    def iter_content(self):
        return self.iter_bytes()

    async def aiter_bytes(self):
        if self._stream is not None:
            if hasattr(self._stream, "__aiter__"):
                async for chunk in self._stream:
                    yield chunk
            else:
                for chunk in self._stream:
                    yield chunk
        elif self._content:
            yield self._content

    async def aiter_content(self):
        async for chunk in self.aiter_bytes():
            yield chunk

    def read(self) -> bytes:
        if self._stream is not None and not self._content:
            self._content = b"".join(self.iter_bytes())
        return self._content

    def close(self) -> None:
        close = getattr(self._stream, "close", None)
        if callable(close):
            close()

    async def aclose(self) -> None:
        aclose = getattr(self._stream, "aclose", None)
        if callable(aclose):
            await aclose()
        else:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class FakeHTTPClient:
    """Minimal blocking HTTP client backed by a ``handler(request) -> response``."""

    def __init__(self, handler):
        self._handler = handler
        self.is_closed = False

    def _build_request(self, method: str, url: str, **kwargs) -> FakeHTTPRequest:
        params = kwargs.get("params")
        if params:
            parsed = urlsplit(url)
            existing = dict(parse_qsl(parsed.query))
            existing.update(params)
            url = urlunsplit(
                (parsed.scheme, parsed.netloc, parsed.path, urlencode(existing), parsed.fragment)
            )
        return FakeHTTPRequest(
            method,
            url,
            headers=kwargs.get("headers"),
            content=kwargs.get("content"),
            data=kwargs.get("data"),
            json=kwargs.get("json", _UNSET),
        )

    def request(self, method: str, url: str, **kwargs) -> FakeHTTPResponse:
        if self.is_closed:
            raise RuntimeError("FakeHTTPClient is closed")
        req = self._build_request(method, url, **kwargs)
        response = self._handler(req)
        if getattr(response, "request", None) is None:
            response.request = req
            response.url = req.url
        return response

    @contextmanager
    def stream(self, method: str, url: str, **kwargs):
        response = self.request(method, url, **kwargs)
        try:
            yield response
        finally:
            response.close()

    def close(self) -> None:
        self.is_closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class FakeAsyncHTTPClient(FakeHTTPClient):
    """Minimal async HTTP client backed by a ``handler(request) -> response``."""

    async def _build_async_request(self, method: str, url: str, **kwargs) -> FakeHTTPRequest:
        content = kwargs.get("content")
        if content is not None and hasattr(content, "__aiter__"):
            chunks = []
            async for chunk in content:
                chunks.append(chunk)
            kwargs = {**kwargs, "content": b"".join(chunks)}
        return self._build_request(method, url, **kwargs)

    async def request(self, method: str, url: str, **kwargs) -> FakeHTTPResponse:
        if self.is_closed:
            raise RuntimeError("FakeAsyncHTTPClient is closed")
        req = await self._build_async_request(method, url, **kwargs)
        response = self._handler(req)
        if getattr(response, "request", None) is None:
            response.request = req
            response.url = req.url
        return response

    @asynccontextmanager
    async def stream(self, method: str, url: str, **kwargs):
        response = await self.request(method, url, **kwargs)
        try:
            yield response
        finally:
            await response.aclose()

    async def aclose(self) -> None:
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()


class FakeClient:
    """Controllable async ChatGPT transport client for provider tests."""

    def __init__(self, session):
        self.session = session
        self.closed = asyncio.Event()
        self.started = asyncio.Event()
        self.failure = None
        self.raw = {
            "id": "remote",
            "title": "Example",
            "messages": [
                {
                    "id": "message",
                    "author": {"role": "assistant"},
                    "channel": "analysis",
                    "content": {"parts": ["Visible summary", {"asset_pointer": "image-ref"}]},
                    "citations": [{"url": "https://example.test"}],
                }
            ],
        }

    async def request(self, method, url, **kwargs):
        self.deleted = (method, url)
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def aclose(self):
        await self.close()

    async def close(self):
        self.closed.set()

    async def verify(self):
        return {"items": []}

    async def list_page(self, offset, limit):
        return {"items": [self.raw]}

    async def get_conversation(self, conversation_id):
        return self.raw

    async def send_prompt(self, conversation_id, prompt, on_event):
        on_event({"type": "delta", "text": "partial"})
        self.started.set()
        await asyncio.wait_for(self.closed.wait(), timeout=2)
        if self.failure:
            raise self.failure
        on_event({"type": "replace", "text": "finished"})
        return "remote", self.raw, set()


def cookie(name, value="secret", domain=".google.com", expires=None):
    return Cookie(
        0,
        name,
        value,
        None,
        False,
        domain,
        True,
        domain.startswith("."),
        "/",
        True,
        True,
        expires,
        False,
        None,
        None,
        {},
        False,
    )


def bind_stream(session, origin="https://claude.ai"):
    """Use the real transport stream policy with a fixture's HTTP client."""
    from webaiprovider.transport import AsyncHTTPTransport

    session.origin = origin
    session.stream = AsyncHTTPTransport(
        origin,
        client_getter=lambda: session.connect(),
        after_response=getattr(session, "persist", None),
    ).stream
    return session
