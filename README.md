[![CI][ci-shield]][ci-url]
[![PyPI][pypi-shield]][pypi-url]
[![Python 3.10+][python-shield]][python-url]
[![GPL-3.0-only][license-shield]][license]

**English** | [Türkçe][readme-tr] | [Русский][readme-ru]

# WebAI Provider

[Repository][repository] · [Changelog][changelog] · [Latest release][releases] · [GPL-3.0 License][license]

Independent Python clients for ChatGPT, Gemini, Claude, and Grok web sessions. The API provides matching asynchronous and blocking interfaces, typed model references, and streaming message operations.

> [English documentation][docs] covers installation, chat streaming, model selection, project management, and the API reference.

> **Note:** Provider websites and private web protocols can change without notice. This project is independent and is not affiliated with the providers.

## Install

Requires Python 3.10 or newer.

```bash
pip install webai-provider
```

## Quick start

You can import credentials from the operating system's default browser or name a supported browser explicitly. You can also provide cookies yourself.

```python
import asyncio

from webaiprovider import WebAIAccount, AsyncChatGPTWebAIProvider, OutputDelta, WebAIProviderType


async def main() -> None:
    account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT, browser="firefox")

    async with AsyncChatGPTWebAIProvider(account) as provider:
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

`WebAIAccount.from_browser()` uses the default browser when `browser` is omitted. Browser-cookie access may require the browser to be closed or the operating system keyring to be unlocked. To supply credentials directly, use `WebAIAccount.from_cookies(provider, cookies)` and keep the cookie values in your own secure store.

For blocking code, use the provider-specific sync class. Blocking providers run directly on the calling thread:

```python
from webaiprovider import WebAIAccount, ChatGPTWebAIProvider, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)
with ChatGPTWebAIProvider(account) as provider:
    chat = provider.chats.create("Quick start")
    response = chat.responses.create("Hello!").start()
```

## Core API

- `BaseWebAIProvider` provides account and provider identity. Use `AsyncWebAIProvider` for async work and `WebAIProvider` for blocking calls. Use a provider-specific class such as `AsyncChatGPTWebAIProvider` for additional features.
- `ChatGPTWebAIProvider`, `ClaudeWebAIProvider`, `GeminiWebAIProvider`, and `GrokWebAIProvider` provide provider-specific extensions. ChatGPT projects use the default/global memory scope unless `memory_share=False` selects project-only memory. For ChatGPT chats, use `chat.move_to(project)` to assign or transfer a chat and `chat.move_to(None)` to detach it.
- Remote collections read provider data on demand. `list(limit=50)` hides pagination, `limit=0` skips remote work, and `limit=None` reads all available pages. Collections share `get()`, `search()`, `find()`, `first()`, `last()`, and `single()`. `find(single=True)` raises `AmbiguousMatchError` if more than one item matches.
- Model queries reuse the account catalog loaded when the provider session opens, including an empty catalog. Call `provider.models.refresh()` to reload it.
- Overlapping calls to `chat.responses.list()` share a history request. Use `chat.responses.list(fresh=True)` for a separate request.
- `chat.responses.create()`, `edit()`, and `retry()` prepare message operations. Use `operation.stream()` for events or `operation.wait()` for the completed `Response`. It exposes the associated user message, sources, artifacts, and timing for live sends. `operation.wait_for(stage, timeout=...)` can pause a blocking send at an event. Resume with `stream()` or `wait()`, or call `cancel()` to release it. For built-in curl message streams, the stage deadline bounds native network polling and preserves the open transfer. Blocking preparation, user hooks, and custom transports observe the deadline once they return control.
- `response.parts` is a read-only local collection ordered by the provider's content blocks. Use `response.parts.list(lambda part: isinstance(part, ThinkingPart))` to select available reasoning summaries. `TextPart` holds visible text, and `response.text` joins visible blocks in order.
- Stream begin, delta, replacement and end events carry the affected part ID. `generation.end` closes the generated content. `verification.begin` reports a required remote check when one starts, and `response.parts.resolved` can carry corrected or recovered content from the confirmed record. `status.terminal` reports the final outcome. Providers without a reliable early completion signal report generation end when their stream ends. After a failure, consuming the stream still raises the operation error.
- Use `operation.wait_for(OperationEvent.THINKING_BEGIN, after=event, timeout=...)` to wait for a repeated occurrence after an event previously returned by the same operation. Use `stream()` to consume all events in order. Register persistent callbacks with `chat.hooks.on(OperationEvent.OUTPUT_DELTA, handler)`. Each operation still owns its result, event history, and cancellation.
- Call `chat.prepare()` (or `await chat.prepare()`) to warm reusable context for the next normal send. It shares the chat's send lock. The sync method runs on the caller thread. Resume or cancel a paused blocking send before mutating the same chat on that thread.

- To open a chat with its recent responses in a single history read, use `chat, responses = await provider.chats.get_with_responses(chat_id, limit=25)`. Omit `await` with the blocking provider. The limit controls returned responses.
- Use `project.attachments.create()` to add a file or text, `list()` to inspect project attachments, and `delete()` to remove one.
- Providers manage lifecycle, locks, hooks, and events directly on `WebAIProvider` and `AsyncWebAIProvider`, backed by `flowevent`. `provider.events` exposes its native `AsyncEventBus` or `EventBus` and emits `EventEnvelope` values; filter by event name with `EventFilter`. The library manages provider and message states internally. Hooks remain available through `provider.hooks` (`OperationHooks` / `AsyncOperationHooks`), and `provider.lifecycle_state` exposes `LifecycleState`.
- Subscribe to `collection.read.completed` for one batch of item identities per successful remote read and to `collection.changed` for confirmed creates, updates, and deletes. Both events put an `items` list in `event.metadata["payload"]`. The envelope carries the account and, for nested collections, the parent resource ID. Message revisions use the chat scope. Payloads omit message text, file bodies, and credentials. Local collections use stable snapshots without publishing provider events.
- Project, attachment, retry, edit, branching, and chat-move capabilities vary by provider. Provider-specific chat classes expose only their supported operations; unsupported actions raise a typed `WebAIProviderError` such as `FeatureUnavailable`.

The package exports model references and effort choices for typed use. Account-discovered models remain authoritative for the account and may differ by subscription or provider changes.

## Advanced transport API

Every provider exposes `provider.transport` for origin-scoped `send`, JSON `request`, and incremental `stream` operations. The provider owns the transport lifecycle; blocking and async calls reuse the provider's existing connection rather than opening a new pool or event loop per request.

```python
from webaiprovider import WebAIAccount, AsyncClaudeWebAIProvider, WebAIProviderType
from webaiprovider.transport import AsyncHttpSession, TransportConfig

account = WebAIAccount.from_browser(WebAIProviderType.CLAUDE)
transport = AsyncHttpSession(
    "https://claude.ai",
    account.store(),
    "sessionKey",
    config=TransportConfig(timeout=30, max_stream_bytes=64 * 1024 * 1024),
)

async with AsyncClaudeWebAIProvider(account, transport=transport) as provider:
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

Claude and Grok use `AsyncHttpSession` / `HttpSession`. Gemini and ChatGPT use `AsyncHTTPTransport` / `HTTPTransport` for their native web requests. The asynchronous and blocking APIs share the same origin, cookie, and security policies, while blocking providers run directly on the calling thread without starting a background event loop. Conflicting chat and project mutations are serialized by resource locks.

A custom implementation can provide `send`, `stream`, and `close` on `AsyncTransport` or `Transport`, or configure a built-in implementation with `client_factory`. Standard HTTP options such as headers, params, JSON, and data pass through to the configured backend. Multipart formats and backend-specific options belong to that backend's extension API.

`TransportConfig` sets timeouts and buffered/streaming response limits; the default buffered limit is 64 MiB. `max_stream_bytes` defaults to `None` (unbounded) so long streams are not truncated unexpectedly; set a streaming limit when needed. Endpoint calls can override the timeout and select `check_status=False` when advanced code needs to inspect an HTTP error response. Origin and redirect checks still apply. Paths are relative to a fixed origin. Absolute or protocol-relative URLs, traversal, conflicting `Host` headers, and redirects are rejected. Default headers can be a mapping or a `(method, path)` callable; per-call overrides are case-insensitive. Use `inherit_headers=False` when a request, such as credential refresh, must omit inherited headers. Signed cross-origin uploads use a separate origin-bound transport without account headers.

Mutations are never replayed automatically. Cooldowns are scoped to the transport/account, so one account does not block other accounts using the same provider. A supplied transport must use the provider's exact origin and authenticate the same account; the provider closes it on shutdown. A borrowed `client_getter` client remains owned by its creator. Cookie persistence and connection pools are reused by high- and low-level operations.

Frame decoding is separate from HTTP. `SSEFrameDecoder` preserves `data`, `event`, `id`, and numeric `retry` fields; it handles split UTF-8, CR, LF, CRLF, multiline data, comments, and bounded data-line counts. It also provides sync and async byte-iterator helpers. `JSPBFrameDecoder` decodes bounded length-prefixed JSON frames and accepts `prefix`, `max_frame_units`, and `length_includes_separator`; Gemini supplies its anti-XSSI prefix and separator settings. Each output is a complete JSON frame. Pass `final=True` on the last feed to detect incomplete frames; incomplete JSPB frames raise `FrameDecodeError`. Provider code interprets the decoded frames and owns completion semantics.

Provider history, RPC payloads, continuation IDs, and response interpretation remain in each provider package.

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
python run_tests.py
python -m build --sdist --wheel
twine check dist/*
```

For offline performance measurements, see the [benchmark guide][benchmarks-guide].

## Diagnostics

Send timing is opt-in and records named durations and counters without message or credential contents. Activate it around an awaited operation; it does not run automatically.

```python
from webaiprovider.core.timing import SendTiming, use_send_timing

timing = SendTiming(provider.id)
with use_send_timing(timing):
    response = await provider.ask("Hello")
durations_ms, counters = timing.snapshot()
```

`patch_project(project_id, name=...)` changes only the supplied fields; use `instructions=""` to clear instructions. Project mutations read current server state under their lock before writing. Operation streams retain their complete event history for replay, so retained operations use memory proportional to their event count.

## Dependencies

- [flowevent][flowevent-repo] — In-process sync and async event bus
- [flowpipeline][flowpipeline-repo] — Staged request preparation pipelines
- [flowcoord][flowcoord-repo] — Single-flight and execution coordination primitives
- [curl-cffi][curl-cffi-repo] — Browser-impersonating HTTPS and streaming transport
- [browser-cookie3][browser-cookie3-repo] — Local browser cookie extraction
- [orjson][orjson-repo] — Fast JSON serialization and decoding
- [lz4][lz4-repo] — Browser session decompression

Licensed under GPL-3.0-only. See [LICENSE][license] and [COPYRIGHT][copyright].

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/webai-provider/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/webai-provider?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-GPL--3.0--only-blue.svg?style=for-the-badge
[ci-url]: https://github.com/TahsinCr/webai-provider/actions/workflows/ci.yml
[pypi-url]: https://pypi.org/project/webai-provider/
[python-url]: https://www.python.org/downloads/
[readme-tr]: README-TR.md
[readme-ru]: README-RU.md
[docs]: docs/en/README.md
[benchmarks-guide]: benchmarks/README.md
[repository]: https://github.com/TahsinCr/webai-provider
[changelog]: CHANGELOG.md
[releases]: https://github.com/TahsinCr/webai-provider/releases/latest
[license]: LICENSE
[copyright]: COPYRIGHT
[flowevent-repo]: https://github.com/TahsinCr/python-flowevent
[flowpipeline-repo]: https://github.com/TahsinCr/python-flowpipeline
[flowcoord-repo]: https://github.com/TahsinCr/python-flowcoord
[curl-cffi-repo]: https://github.com/lexiforest/curl_cffi
[browser-cookie3-repo]: https://github.com/borisbabic/browser_cookie3
[orjson-repo]: https://github.com/ijl/orjson
[lz4-repo]: https://github.com/python-lz4/python-lz4
