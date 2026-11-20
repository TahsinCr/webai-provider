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
        operation = chat.responses.create("Hello!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

`Account.from_browser()` uses the default browser when `browser` is omitted. Browser-cookie access may require the browser to be closed or the operating system keyring to be unlocked. To supply credentials directly, use `Account.from_cookies(provider, cookies)` and keep the cookie values in your own secure store.

For blocking code, use the provider-specific sync class. Each provider reuses one event loop and runs operations on the calling thread:

```python
from webaiprovider import Account, ChatGPTProvider, ProviderType

account = Account.from_browser(ProviderType.CHATGPT)
with ChatGPTProvider(account) as provider:
    chat = provider.chats.create("Quick start")
    response = chat.responses.create("Hello!").start()
```

## Core API

- `BaseProvider` provides account and provider identity. Use `AsyncProvider` for async work and `Provider` for blocking calls. Use a provider-specific class such as `AsyncChatGPTProvider` for additional features.
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider`, and `GrokProvider` provide provider-specific extensions. ChatGPT projects use the default/global memory scope unless `memory_share=False` selects project-only memory. For ChatGPT chats, use `chat.move_to(project)` to assign or transfer a chat and `chat.move_to(None)` to detach it.
- Remote collections read provider data on demand. `list(limit=50)` hides pagination, `limit=0` skips remote work, and `limit=None` reads all available pages. Collections share `get()`, `search()`, `find()`, `first()`, `last()`, and `single()`. `find(single=True)` raises `AmbiguousMatchError` if more than one item matches.
- Model queries reuse the account catalog loaded when the provider session opens, including an empty catalog. Call `provider.models.refresh()` to reload it.
- Overlapping calls to `chat.responses.list()` share a history request. Use `chat.responses.list(fresh=True)` for a separate request.
- `chat.responses.create()`, `edit()`, and `retry()` prepare message operations. Use `operation.stream()` for events or `operation.wait()` for the completed `Response`. It exposes the associated user message, sources, artifacts, and timing for live sends.
- To open a chat with its recent responses in a single history read, use `chat, responses = await provider.chats.get_with_responses(chat_id, limit=25)`. Omit `await` with the blocking provider. The limit controls returned responses.
- Use `project.attachments.create()` to add a file or text, `list()` to inspect project attachments, and `delete()` to remove one.
- Providers manage lifecycle, locks, hooks, and events directly on `Provider` and `AsyncProvider`, backed by `flowevent`. `provider.events` exposes its native `AsyncEventBus` or `EventBus` and emits `EventEnvelope` values; filter by event name with `EventFilter`. Lifecycle and message states use `flowevent.StateMachine` directly. Hooks remain available through `provider.hooks` (`OperationHooks` / `AsyncOperationHooks`), and `provider.lifecycle_state` exposes `LifecycleState`.
- Subscribe to `collection.read.completed` for one batch of item identities per successful remote read and to `collection.changed` for confirmed creates, updates, and deletes. Both events put an `items` list in `event.metadata["payload"]`. The envelope carries the account and, for nested collections, the parent resource ID. Message revisions use the chat scope. Payloads omit message text, file bodies, and credentials.
- Project, attachment, retry, edit, branching, and chat-move capabilities vary by provider. Provider-specific chat classes expose only their supported operations; unsupported actions raise a typed `ProviderError` such as `FeatureUnavailable`.

The package exports model references and effort choices for typed use. Account-discovered models remain authoritative for the account and may differ by subscription or provider changes.

## Advanced transport API

Every provider exposes `provider.transport` for origin-scoped `send`, JSON `request`, and incremental `stream` operations. The provider owns the transport lifecycle; blocking and async calls reuse the provider's existing connection rather than opening a new pool or event loop per request.

```python
from webaiprovider import Account, AsyncClaudeProvider, ProviderType
from webaiprovider.transport import AsyncHttpSession, TransportConfig

account = Account.from_browser(ProviderType.CLAUDE)
transport = AsyncHttpSession(
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
| `HTTPTransport` | Blocking curl_cffi transport |
| `AsyncHttpSession` | Account cookie persistence over the shared async transport |
| `HttpSession` | Account cookie persistence over blocking HTTP on the calling thread |

Claude and Grok use `AsyncHttpSession`. Gemini and ChatGPT use `AsyncHTTPTransport` for their native web requests. The blocking API drives the same asynchronous HTTP session from the calling thread. Calls on the same provider instance are serialized.

A custom implementation can provide `send`, `stream`, and `close` on `AsyncTransport`, or configure a built-in implementation with `client_factory`. Standard HTTP options such as headers, params, JSON, and data pass through to the configured backend. Multipart formats and backend-specific options belong to that backend's extension API.

`TransportConfig` sets timeouts and buffered/streaming response limits; the default buffered limit is 16 MiB. Set a streaming limit when needed. Endpoint calls can override the timeout and select `check_status=False` when advanced code needs to inspect an HTTP error response. Origin and redirect checks still apply. Paths are relative to a fixed origin. Absolute or protocol-relative URLs, traversal, conflicting `Host` headers, and redirects are rejected. Default headers can be a mapping or a `(method, path)` callable; per-call overrides are case-insensitive. Use `inherit_headers=False` when a request, such as credential refresh, must omit inherited headers. Signed cross-origin uploads use a separate origin-bound transport without account headers.

Mutations are never replayed automatically. Cooldowns are scoped to the transport/account, so one account does not block other accounts using the same provider. A supplied transport must use the provider's exact origin and authenticate the same account; the provider closes it on shutdown. A borrowed `client_getter` client remains owned by its creator. Cookie persistence and connection pools are reused by high- and low-level operations.

Frame decoding is separate from HTTP. `SSEFrameDecoder` preserves `data`, `event`, `id`, and numeric `retry` fields; it handles split UTF-8, CR, LF, CRLF, multiline data, comments, and bounded data-line counts. It also provides sync and async byte-iterator helpers. `JSPBFrameDecoder` decodes bounded length-prefixed JSON frames and accepts `prefix`, `max_frame_units`, and `length_includes_separator`; Gemini supplies its anti-XSSI prefix and separator settings. Each output is a complete JSON frame. Pass `final=True` on the last feed to detect incomplete frames; incomplete JSPB frames raise `FrameDecodeError`. Provider code interprets the decoded frames and owns completion semantics.

Shared source and file normalization lives in `webaiprovider.normalizers`. Provider history, RPC payloads, continuation IDs, and response interpretation remain in each provider package.

Send messages through `chat.responses`. The provider handles its own requests and retry semantics.

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

For offline performance measurements, see the [benchmark guide](benchmarks/README.md).

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
