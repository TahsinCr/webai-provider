[← Previous: 04 · Provider Guides](provider-guides.md) · [Documentation Map](README.md) · [Next: 06 · Quick API Reference →](api-reference.md)

# 05 · Events, Low-Level Transport, Frame Decoders, and Error Recovery

This guide is for developers subscribing directly to `EventBus` records, issuing custom origin-scoped HTTP requests via `provider.transport`, decoding SSE/JSPB frames, or building resilient error-recovery flows.

---

## Contents

- [Quick Example](#quick-example)
- [Event Bus (`EventBus` / `AsyncEventBus`)](#event-bus-eventbus--asynceventbus)
  - [Subscribing and `EventFilter`](#subscribing-and-eventfilter)
  - [Published Events](#published-events)
- [Low-Level HTTP Transport (`provider.transport`)](#low-level-http-transport-providertransport)
  - [`TransportConfig` and Transport Classes](#transportconfig-and-transport-classes)
  - [`send()`, `request()`, `stream()`, and Security Rules](#send-request-stream-and-security-rules)
- [Frame Decoders (`SSEFrameDecoder` and `JSPBFrameDecoder`)](#frame-decoders-sseframedecoder-and-jspbframedecoder)
- [Error Hierarchy and Safe Recovery](#error-hierarchy-and-safe-recovery)
- [Diagnostics and Performance](#diagnostics-and-performance)

---

## Quick Example

```python
from webaiprovider import (
    WebAIAccount,
    AccountPaused,
    ClaudeWebAIProvider,
    EventEnvelope,
    EventFilter,
    WebAIProviderError,
    WebAIProviderType,
    RateLimited,
)
from webaiprovider.core.timing import SendTiming, use_send_timing
from webaiprovider.transport import HttpSession, TransportConfig

account = WebAIAccount.from_browser(WebAIProviderType.CLAUDE)
transport = HttpSession(
    origin="https://claude.ai",
    credential_file=account.store(),
    required_cookie="sessionKey",
    config=TransportConfig(timeout=(10.0, 30.0), max_stream_bytes=64 * 1024 * 1024),
)

try:
    with ClaudeWebAIProvider(account, transport=transport) as provider:
        provider.events.subscribe(
            lambda env: print("Event:", env.event, env.metadata.get("payload")),
            event_filter=EventFilter(
                event_type=EventEnvelope,
                predicate=lambda env: env.event in ("collection.changed", "chat.send.completed"),
            ),
        )

        timing = SendTiming(provider.id)
        with use_send_timing(timing):
            response = provider.ask("Write a short summary.")
        durations_ms, counters = timing.snapshot()
        print("Timings (ms):", durations_ms)
except RateLimited as err:
    print(f"Rate limited (retry_after={err.retry_after}s, submitted={err.message_submitted})")
except AccountPaused:
    print("Session expired; sign in again.")
except WebAIProviderError as err:
    print(f"Error [{err.code}]: {err}")
```

[Back to top](#contents)

---

## Event Bus (`EventBus` / `AsyncEventBus`)

### Subscribing and `EventFilter`

Every provider exposes a `provider.events` bus configured with `ErrorPolicy.ISOLATE` so one failing subscriber never breaks others. Provider lifecycle transitions can be checked on `provider.lifecycle_state` (`LifecycleState`: `CREATED`, `OPENING`, `OPEN`, `OPEN_FAILED`, `CLOSING`, `CLOSED`, `CLOSE_FAILED`).

On `provider.events.subscribe(callback, *, event_filter=None, event_type=None, predicate=None, ...)`, filtering options are keyword-only. Subscribers receive `EventEnvelope` instances with `event`, `correlation_id`, `source`, and `metadata` (`account_id`, `resource_id`, `sequence`, `payload`).

By design, the event bus never emits prompt/response text, file contents, or cookies/tokens—only IDs, titles, action names, and timestamps.

```python
from webaiprovider import EventEnvelope, EventFilter

sub = provider.events.subscribe(
    lambda env: print(env.metadata["sequence"], env.event, env.metadata["payload"]),
    event_filter=EventFilter(
        event_type=EventEnvelope,
        predicate=lambda env: env.event in ("collection.read.completed", "collection.changed"),
    ),
)
sub.unsubscribe()
```

### Published Events

| Event Name | When Emitted | `metadata["payload"]` |
| --- | --- | --- |
| `provider.opening` / `opened` / `open_failed` | Provider session startup stages | `open_failed`: `{"error_type"}` |
| `provider.closing` / `closed` / `close_failed` | Provider session shutdown stages | `close_failed`: `{"error_type"}` |
| `collection.read.completed` | Once per successful remote collection read | `{"collection", "operation", "items", "complete", "include_projects", "started_ns"}` |
| `collection.changed` | After confirmed create / update / delete | `{"collection", "action", "items", "confirmed_ns"}` |
| `chat.prepared` / `created` / `branched` / `moved` | Chat lifecycle events | Related title or target ID details |
| `chat.send.started` / `completed` / `cancelled` / `failed` | Message send lifecycle | `completed`: `{"message_id"}`; `failed`: `{"error_type"}` |
| `project.created` / `updated` / `deleted` | Project mutations | `{"title"}` *(created/updated)* |
| `project.attachment.added` / `removed` | Project resource mutations | `{"attachment_id", "kind"}` / `{"attachment_id"}` |

[Back to top](#contents)

---

## Low-Level HTTP Transport (`provider.transport`)

Use `provider.transport` on an open provider when you need to call an endpoint not wrapped by the high-level collections, or inject a custom transport for timeouts and HTTP client control.

### `TransportConfig` and Transport Classes

In `TransportConfig(timeout=(10.0, 50.0), max_response_bytes=64 * 1024 * 1024, max_stream_bytes=None)`, `timeout` accepts a positive number or a `(connect, read)` tuple. Buffered responses default to a 64 MiB limit; set `max_stream_bytes` to cap long streams.

| Class | Execution Mode | Parameters |
| --- | --- | --- |
| `HTTPTransport` | Blocking `curl_cffi.Session` | `(origin, *, client_factory=None, client_getter=None, config=None, headers=None)` |
| `AsyncHTTPTransport` | Async `curl_cffi.AsyncSession` | `(origin, *, client_factory=None, client_getter=None, after_response=None, config=None, headers=None)` |
| `HttpSession` | Blocking + persistent cookie store | `(origin, credential_file, required_cookie, *, client_factory=None, config=None)` |
| `AsyncHttpSession` | Async + persistent cookie store | `(origin, credential_file, required_cookie, *, client_factory=None, config=None)` |

Clients produced by `client_factory(timeout=...)` are closed when the transport closes; borrowed clients returned by `client_getter()` remain the caller's responsibility.

### `send()`, `request()`, `stream()`, and Security Rules

```python
# Raw response (status_code, headers, content, text, json())
resp = provider.transport.send("GET", "/api/bootstrap", timeout=15.0, check_status=False)

# Direct JSON decoding (returns None on empty body)
data = provider.transport.request("GET", "/api/organizations", params={"limit": 10})

# Streaming (always use inside `with` / `async with`)
with provider.transport.stream("POST", "/api/conversation", json=payload) as s:
    for chunk in s.iter_bytes():
        process(chunk)
```

Calls accept `params`, `json`, `data`, `headers`, `timeout`, `check_status=False` (to inspect 4xx/5xx responses directly), and `inherit_headers=False` (to omit default session headers).

For security, every `path` must start with `/`: full URLs, `//` prefixes, `..`, `\`, `#fragment`, mismatched `Host` headers, and `allow_redirects=True` are rejected with `ValueError`. DNS resolution is pinned per origin. Custom transports passed to a provider must match its exact origin and are closed when `provider.close()` runs.

[Back to top](#contents)

---

## Frame Decoders (`SSEFrameDecoder` and `JSPBFrameDecoder`)

`webaiprovider.transport.frames` provides two incremental, HTTP-agnostic stream decoders:

- **`SSEFrameDecoder(*, max_event_bytes=2_000_000, max_data_lines=65_536, utf8_errors="replace")`:**
  Reassembles split UTF-8 byte sequences, `CR`/`LF`/`CRLF` line endings, and multi-line `data:` blocks into `SSEFrame(data, event, id, retry)` objects. Includes both sync `decode(chunks)` and async `decode_async(chunks)` helpers.

```python
from webaiprovider.transport.frames import SSEFrameDecoder

decoder = SSEFrameDecoder(max_event_bytes=4 * 1024 * 1024)
with provider.transport.stream("POST", "/backend-api/conversation", json=body) as s:
    for frame in decoder.decode(s.iter_bytes()):
        if frame.data == "[DONE]":
            break
        print(frame.event, len(frame.data))
```

- **`JSPBFrameDecoder(*, prefix="", max_frame_units=8 * 1024 * 1024, length_includes_separator=False)`:**
  Decodes length-prefixed (UTF-16 code-unit counted) JSON/JSPB streams via `feed(data, final=False)` and raises `FrameDecodeError` if an incomplete frame remains when `final=True`.

[Back to top](#contents)

---

## Error Hierarchy and Safe Recovery

Library exceptions inherit from `WebAIProviderError` and carry a machine-readable `err.code` field for programmatic handling:

```text
WebAIProviderError ("provider_error")
├── RequestNotSentError ("request_not_sent", message_submitted=False)
├── StreamError ("stream_error") -> FrameDecodeError ("frame_decode_error")
├── AuthenticationError ("authentication_error") -> AccountPaused ("account_paused")
├── FeatureUnavailable ("feature_unavailable")      [.capability, .reason]
├── RateLimited ("rate_limited")                    [.retry_after, .message_submitted]
├── CompatibilityError / RemoteNotFound / AmbiguousMatchError
├── UnsupportedOperationError / OperationCancelledError / ConfigurationError / ContentError
└── ProfileError ("profile_error")
    └── ProfileNotFoundError, ProfileSessionNotFoundError, ProfileDependencyError,
        ProfilePermissionError, ProfileDatabaseError, ProfileKeyringError,
        ProfileDecryptionError, ProfileTimeoutError, ProfileInvalidResultError
```

> **No Automatic Retry Policy:** The library **never** automatically retries `HTTP 429` (`RateLimited`) responses or `POST` message sends / mutations. Transport-level retries are restricted strictly to idempotent `GET` reads on transient network failures.

Rather than blindly retrying failed mutations, inspect `RequestNotSentError` or `RateLimited.message_submitted is False` so you only retry requests guaranteed not to have reached the server:

```python
from webaiprovider import AccountPaused, FeatureUnavailable, RateLimited
from webaiprovider.errors import RequestNotSentError

try:
    response = chat.ask("Hello")
except RateLimited as err:
    if err.retry_after is not None and err.message_submitted is False:
        print(f"Safe to retry after {err.retry_after}s.")
except RequestNotSentError:
    print("Request never left the client; safe to retry.")
except AccountPaused:
    print("Session needs re-authentication.")
except FeatureUnavailable as err:
    print(f"Unsupported capability: {err.capability.value}")
```

[Back to top](#contents)

---

## Diagnostics and Performance

- **Timing metrics (`SendTiming` / `use_send_timing`):** `SendTiming` in `webaiprovider.core.timing` captures per-stage durations (in milliseconds) and counters without recording prompts or credentials:

```python
from webaiprovider.core.timing import SendTiming, use_send_timing

timing = SendTiming(provider.id)
with use_send_timing(timing):
    resp = provider.ask("Hello")
durations_ms, counters = timing.snapshot()
```

- **Session reuse:** Keeping the same `WebAIAccount` instance in memory across short-lived `WebAIProvider(account)` opens reuses the verified session and model catalog for 300 seconds, automatically invalidating if credentials change or `AccountPaused` / `forget_session()` / `authenticate()` runs.
- **Practical performance tips:** Use `chats.get_with_responses()` when loading both a chat and its history; pass an explicit `limit` instead of `limit=None` on large accounts; release completed `MessageOperation` objects in long-running processes so event histories are freed. For the offline benchmark suite, see [benchmarks/README.md](../../benchmarks/README.md).

[Back to top](#contents)

---

[← Previous: 04 · Provider Guides](provider-guides.md) · [Documentation Map](README.md) · **Next:** [06 · Quick API Reference →](api-reference.md)
