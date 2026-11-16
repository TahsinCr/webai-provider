# webai-provider

Independent Python clients for ChatGPT, Gemini, Claude, and Grok web sessions. The v2 API provides matching asynchronous and blocking interfaces, typed model references, and streaming message operations.

> **Beta:** Provider websites and private web protocols can change without notice. This project is independent and is not affiliated with the providers.

## Install

Requires Python 3.10 or newer.

The v2 API documented here is still in development and is not part of the published `v1.3-beta` package. Install it from a checkout that contains this API:

```bash
python -m pip install -e '.[dev]'
```

## Quick start

You can import credentials from the operating system's default browser or name a supported browser explicitly. You can also provide cookies yourself.

```python
import asyncio

from webaiprovider import Account, AsyncChatGPTProvider, OutputDelta, ProviderType


async def main() -> None:
    account = Account.from_browser(ProviderType.CHATGPT, browser="firefox")

    async with AsyncChatGPTProvider(account) as provider:
        models = await provider.models.list()
        if not models:
            raise RuntimeError("No models are available for this account")

        chat = await provider.chats.create("Quick start")
        operation = chat.messages.create("Hello!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

`Account.from_browser()` uses the default browser when `browser` is omitted. Browser-cookie access may require the browser to be closed or the operating system keyring to be unlocked. To supply credentials directly, use `Account.from_cookies(provider, cookies)` and keep the cookie values in your own secure store.

For blocking code, use the provider-specific sync class. It keeps one event-loop portal alive for the provider's lifetime:

```python
from webaiprovider import Account, ChatGPTProvider, ProviderType

account = Account.from_browser(ProviderType.CHATGPT)
with ChatGPTProvider(account) as provider:
    chat = provider.chats.create("Quick start")
    response = chat.messages.create("Hello!").start()
```

## Core API

- `BaseProvider` carries the shared account and provider identity. Use `AsyncChatGPTProvider` (or the matching async class) for asynchronous work and `ChatGPTProvider` for blocking work; generic `AsyncProvider` and `Provider` serve shared use through `ProviderType`.
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider`, and `GrokProvider` provide provider-specific extensions. ChatGPT projects use the default/global memory scope unless `memory_share=False` selects project-only memory.
- `provider.models.list()` loads and caches the account's available models for that session. Use `refresh=True` or `provider.models.refresh()` to request a new catalog. `find()`, `search()`, and `get()` select from that catalog.
- `chat.messages.create()`, `edit()`, and `retry()` return a `MessageOperation`. Consume `operation.stream()` for events, or await `operation.wait()` for the completed response. These entry points share one send operation.
- Project, attachment, retry, edit, branching, and chat-move capabilities vary by provider. Provider-specific chat classes expose only their supported operations; unsupported actions raise a typed `ProviderError` such as `FeatureUnavailable`.

The package exports model references and effort choices for typed use. Account-discovered models remain authoritative for the account and may differ by subscription or provider changes.

## Advanced transport API

Every provider exposes `provider.transport` for origin-scoped `send`, JSON `request`, and incremental `stream` operations. The provider owns the transport lifecycle; blocking and async calls reuse the provider's existing connection rather than opening a new pool or event loop per request.

```python
from webaiprovider import Account, AsyncClaudeProvider, ProviderType
from webaiprovider.transport import HttpSession, TransportConfig

account = Account.from_browser(ProviderType.CLAUDE)
transport = HttpSession(
    "https://claude.ai",
    account.store(),
    "sessionKey",
    config=TransportConfig(timeout=30, max_stream_bytes=64 * 1024 * 1024),
)

async with AsyncClaudeProvider(account, transport=transport) as provider:
    response = await provider.transport.send("GET", "/api/bootstrap")
    endpoint, payload = "/api/path", {}  # Use an endpoint supported by this provider.
    async with provider.transport.stream("POST", endpoint, json=payload) as stream:
        async for chunk in stream.aiter_bytes():
            ...
```

`send()` returns `TransportResponse`; `request()` decodes JSON and returns `None` for an empty response. A stream context owns its response until exit. Close the context when stopping early so the response and any worker are released. A blocking provider uses `with` and `iter_bytes()`.

| Type | Use |
| --- | --- |
| `AsyncTransport` / `Transport` | Async and blocking extension contracts |
| `AsyncHTTPTransport` | Async HTTP client with streaming context; defaults to curl-cffi |
| `AsyncRequestTransport` | Async client that opens streams with `request(stream=True)` |
| `HTTPTransport` | Blocking curl_cffi transport |
| `HttpSession` | Account cookie persistence over the shared async transport |
| `ThreadedTransport` | Bounded bridge for a scoped blocking transport |

Claude and Grok use `HttpSession`; Gemini uses `AsyncRequestTransport` for its native RPC and cookie-refresh flow. ChatGPT uses `HTTPTransport` behind its existing client and exposes it asynchronously through `ThreadedTransport`. Providers retain ownership of their native clients and synchronization gates.

A custom implementation can provide `send`, `stream`, and `close` on `AsyncTransport`, or configure a built-in implementation with `client_factory`. Standard HTTP options such as headers, params, JSON, and data pass through to the configured backend. Multipart formats and backend-specific options belong to that backend's extension API.

`TransportConfig` sets timeouts and buffered/streaming response limits; the default buffered limit is 16 MiB. Set a streaming limit when needed. Endpoint calls can override the timeout and select `check_status=False` when advanced code needs to inspect an HTTP error response. Origin and redirect checks still apply. Paths are relative to a fixed origin. Absolute or protocol-relative URLs, traversal, conflicting `Host` headers, and redirects are rejected. Default headers can be a mapping or a `(method, path)` callable; per-call overrides are case-insensitive. Use `inherit_headers=False` when a request, such as credential refresh, must omit inherited headers. Signed cross-origin uploads use a separate origin-bound transport without account headers.

Mutations are never replayed automatically. Cooldowns are scoped to the transport/account, so one account does not block other accounts using the same provider. A supplied transport must use the provider's exact origin and authenticate the same account; the provider closes it on shutdown. A borrowed `client_getter` client remains owned by its creator. Cookie persistence and connection pools are reused by high- and low-level operations.

Frame decoding is separate from HTTP. `SSEFrameDecoder` preserves `data`, `event`, `id`, and numeric `retry` fields; it handles split UTF-8, CR, LF, CRLF, multiline data, comments, and bounded data-line counts. It also provides sync and async byte-iterator helpers. `JSPBFrameDecoder` decodes bounded length-prefixed JSON frames and accepts `prefix`, `max_frame_units`, and `length_includes_separator`; Gemini supplies its anti-XSSI prefix and separator settings. Each output is a complete JSON frame. Pass `final=True` on the last feed to detect incomplete frames; incomplete JSPB frames raise `FrameDecodeError`. Provider code interprets the decoded frames and owns completion semantics.

Shared source and file normalization lives in `webaiprovider.normalizers`. Provider history, RPC payloads, continuation IDs, and response interpretation remain in each provider package.

For provider extensions, `webaiprovider.resources.message.protocol` exports `ProviderRequest` and normalized event types: `TextDelta`, `ThoughtDelta`, `TextReplacement`, `ResponseComplete`, `ConversationBound`, and `MessageMetadata`. Override `_stream_request` to customize a provider while preserving event order:

```python
from collections.abc import AsyncIterator
from webaiprovider import AsyncClaudeProvider
from webaiprovider.resources.message.protocol import ProviderEvent, ProviderRequest


class CustomClaudeProvider(AsyncClaudeProvider):
    async def _stream_request(self, request: ProviderRequest) -> AsyncIterator[ProviderEvent]:
        async for event in super()._stream_request(request):
            yield event
```

Built-in providers use the same contracts. Normal application code should send through `chat.messages` and let the provider own wire requests and retry semantics. `ProviderType` identifies the four supported web providers; using the transport and frame tools with another origin does not add that provider's authentication, model, capability, or resource support.

## Security and behavior

- Browser credentials are read locally. The library does not send credentials to a SiopeChat service.
- Keep cookies and credential files private; do not include them in logs, issue reports, or source control.
- Downloads accept provider-supplied `DownloadSource` values, validate public network destinations, pin the resolved address, and do not follow redirects.
- Provider failures are raised as English-language typed exceptions. Sending from a chat without an open provider session fails explicitly instead of returning a fabricated response.

## Development

```bash
ruff check .
ruff format --check .
python -m unittest discover -s . -t . -q
python -m build --sdist --wheel
twine check dist/*
```

## Diagnostics

Send timing is opt-in and records named durations and counters without message or credential contents. Activate it around an awaited operation; it does not run automatically.

```python
from webaiprovider.utils.timing import SendTiming, use_send_timing

timing = SendTiming(provider.id)
with use_send_timing(timing):
    response = await provider.ask("Hello")
durations_ms, counters = timing.snapshot()
```

`patch_project(project_id, name=...)` changes only the supplied fields; use `instructions=""` to clear instructions. Project mutations read current server state under their lock before writing. Operation streams retain their complete event history for replay, so retained operations use memory proportional to their event count.

## Links

- [Changelog](CHANGELOG.md)
- [Issue tracker](https://github.com/TahsinCr/webai-provider/issues)
- [Repository](https://github.com/TahsinCr/webai-provider)
- [SiopeChat](https://github.com/TahsinCr/chatgpt-web)

Licensed under GPL-3.0-only. See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT).
