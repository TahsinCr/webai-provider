# Transport and protocol extension API

The v2 transport API is available through `provider.transport` after the provider is open. The blocking `Provider` facade runs these operations on its existing async runtime. It does not create another HTTP pool or event loop for each call.

```python
from webaiprovider import Account, AsyncClaudeProvider, ProviderType
from webaiprovider.transport import HttpSession, TransportConfig

account = Account.from_browser(ProviderType.CLAUDE)
http = HttpSession(
    "https://claude.ai",
    account.store(),
    "sessionKey",
    config=TransportConfig(timeout=30, max_stream_bytes=64 * 1024 * 1024),
)

async with AsyncClaudeProvider(account, transport=http) as provider:
    response = await provider.transport.send("GET", "/api/bootstrap")
    # Inspect status_code, headers, content, text, or json() locally.
    # Do not log credential-bearing response bodies.
```

`send(method, path, ...)` returns the `TransportResponse` contract. `request(...)` also decodes JSON, returning `None` for empty responses. `stream(...)` is a context manager that owns the response for the lifetime of its block:

```python
async with provider.transport.stream("POST", endpoint, json=payload) as response:
    async for chunk in response.aiter_bytes():
        handle_bytes(chunk)
```

For a blocking provider, use `with provider.transport.stream(...)` and `response.iter_bytes()`. Exit the context when stopping early so the response and worker are released. Read each stream from one consumer.

## Reusable implementations

| Type | Network strategy |
| --- | --- |
| `AsyncTransport` / `Transport` | Public async / blocking extension contracts |
| `AsyncHTTPTransport` | Async HTTP client with a streaming context manager; defaults to curl-cffi |
| `AsyncRequestTransport` | Async client that opens streams with `request(stream=True)` |
| `AsyncHTTPXTransport` | HTTPX async client, including its redirect, byte-stream, and close conventions |
| `HTTPTransport` | HTTPX blocking client |
| `HttpSession` | Account cookie loading/persistence over the shared async HTTP transport |
| `ThreadedTransport` | Bounded async bridge over a scoped blocking transport |

Claude and Grok use `HttpSession`. Gemini uses `AsyncRequestTransport` while retaining its native RPC and cookie-refresh protocol. ChatGPT uses `HTTPTransport` inside its existing web client and exposes it asynchronously through `ThreadedTransport`. The provider retains ownership of its native client and synchronization gates.

A custom transport implements `send`, `stream`, and `close` on `AsyncTransport`, or configures one of the HTTP implementations through `client_factory`. A supplied transport must use the provider's exact origin and authenticate the same account. The provider closes a supplied transport on shutdown. `client_getter` borrows an existing backend client; its owner closes that client. Built-in browser import, Sentinel, model selection, and provider request semantics remain provider responsibilities.

`TransportConfig` controls timeout and buffered/streaming response size limits. Endpoint-specific timeout arguments can override the default. The default buffered-response limit is 16 MiB; set a streaming limit explicitly when needed. Standard HTTP options such as headers, params, JSON, and data pass through to the configured backend. Multipart representations and backend-specific options belong to that backend's extension API.

## Origin, credentials, and retry policy

Paths are relative to a fixed origin. Absolute URLs, protocol-relative URLs, path traversal, conflicting Host headers, and redirects are rejected. HTTP status validation is enabled by default; advanced code can use `check_status=False` to inspect an error response itself. This does not disable origin or redirect checks.

Default headers can be a mapping or a callable receiving `(method, path)`. Call-specific headers override them without case sensitivity. `inherit_headers=False` omits inherited headers, for example during a credential refresh. Signed cross-origin uploads use a separate origin-bound transport without inherited account headers.

There is no automatic replay of a failed mutation. Rate-limit cooldowns belong to the individual transport instance; one account does not block every account on the same provider. Cookie persistence and client pools are reused by high-level and low-level operations.

## Wire framing and normalization

Frame decoding is independent of HTTP I/O:

```python
from webaiprovider.frames import JSPBFrameDecoder, SSEFrameDecoder

sse = SSEFrameDecoder(max_event_bytes=1_048_576)
for frame in sse.feed(chunk):
    handle_event(frame.event, frame.data)

jspb = JSPBFrameDecoder()  # Plain length-prefixed UTF-16-counted JSON frames.
```

`JSPBFrameDecoder` accepts `prefix`, `max_frame_units`, and `length_includes_separator`. Gemini supplies its anti-XSSI prefix and separator-counting setting. Each yielded value is a whole JSON frame; the provider decides how to interpret or unpack its rows. Both decoders retain incomplete chunks and enforce their configured limits. Pass `final=True` on the last feed to finish decoding; incomplete JSPB frames raise `FrameDecodeError`.

`SSEFrameDecoder` preserves `data`, `event`, `id`, and numeric `retry` fields. It handles CR, LF, CRLF, multiline data, comments, split UTF-8, and bounded data-line counts. It provides synchronous and async byte-iterator helpers. Provider meaning and completion semantics are handled by provider code after framing.

Shared source and file normalization is in `webaiprovider.normalizers`. Provider history, RPC payloads, continuation IDs, and response interpretation remain in each provider package.
