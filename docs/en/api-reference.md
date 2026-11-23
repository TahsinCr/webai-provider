[← Previous: 05 · Events, Transport, and Errors](events-transport-and-errors.md) · [Documentation Map](README.md)

# 06 · Quick API Reference

Everyday constructors, collection operations, stream events, and exceptions. For walkthroughs and examples, see the [Documentation Map](README.md); for the full list of root exports, see the [package entrypoint](../../webaiprovider/__init__.py).

---

## Contents

- [Accounts and Credential Stores](#accounts-and-credential-stores)
- [Providers and Lifecycle](#providers-and-lifecycle)
- [Chats and Collection Queries](#chats-and-collection-queries)
- [Messages, Attachments, and Responses](#messages-attachments-and-responses)
- [Stream Operations, Events, and Hooks](#stream-operations-events-and-hooks)
- [Models and Effort Selection](#models-and-effort-selection)
- [Projects and Project Attachments](#projects-and-project-attachments)
- [Transport, Frame Decoders, and Timing](#transport-frame-decoders-and-timing)
- [Exceptions](#exceptions)

---

## Accounts and Credential Stores

```python
from webaiprovider import (
    WebAIAccount,
    CredentialStore,
    FileCredentialStore,
    MemoryCredentialStore,
    WebAIProviderType,
)
```

- `WebAIProviderType`: `CHATGPT = "chatgpt"`, `CLAUDE = "claude"`, `GEMINI = "gemini"`, `GROK = "grok"` (`WebAIProviderType.parse(value)`).
- `WebAIAccount(id, provider, credentials, capabilities=(), metadata={}, name=None, is_active=True)`
  - `WebAIAccount.from_browser(provider, *, browser=None, id=None, name=None) -> WebAIAccount`
  - `WebAIAccount.from_cookies(provider, cookies, *, id=None, name=None) -> WebAIAccount`
  - `account.store() -> CredentialStore`
- `CredentialStore` *(Protocol)*: `load() -> dict`, `save(value: dict) -> None`, `exists() -> bool`, `delete() -> None`
- `FileCredentialStore(path)`: `save(value, *, ensure_ascii=False, newline=False)` *(1 MiB max, atomic, `0o600`)*
- `MemoryCredentialStore(initial=None)`

[Back to top](#contents)

---

## Providers and Lifecycle

| Class | Constructor Signature |
| --- | --- |
| `WebAIProvider` / `AsyncWebAIProvider` | `(account, *, type=None, transport=None)` |
| `ChatGPTWebAIProvider` / `AsyncChatGPTWebAIProvider` | `(account, *, type=None, session: BrowserSession \| None = None, client_factory=None, transport=None)` |
| `ClaudeWebAIProvider` / `AsyncClaudeWebAIProvider` | `(account, *, type=None, transport=None)` |
| `GeminiWebAIProvider` | `(account, *, type=None, client_factory=None, notebooks: GeminiNotebooks \| None = None, transport=None)` |
| `AsyncGeminiWebAIProvider` | `(account, *, type=None, client_factory=None, notebooks: AsyncGeminiNotebooks \| None = None, transport=None)` |
| `GrokWebAIProvider` / `AsyncGrokWebAIProvider` | `(account, *, type=None, client_factory=None, transport=None)` |

`WebAIProvider(account)` and `AsyncWebAIProvider(account)` select the concrete implementation based on the account type. Import the concrete class directly when you want IDE completion for provider-specific options—such as `memory_share` on ChatGPT projects. See [04 · Provider Guides](provider-guides.md) for details.

**Shared properties and methods (async variants `await` I/O calls):**
- Properties: `id`, `type`, `account_id`, `provider`, `account`, `capabilities`, `opened`, `closing`, `closed`, `lifecycle_state`, `chats`, `projects`, `models`, `events`, `hooks`, `transport`
- `open() -> Self`, `close() -> None` (`with` / `async with`)
- `ask(prompt, *, model=None, attachments=None) -> Response`
- `authenticate() -> SessionStatus`, `session_status() -> SessionStatus`, `forget_session() -> SessionStatus` (`from webaiprovider.accounts.status import SessionState, SessionStatus`)
- `attachment_policy() -> dict | None`
- `upload_file(path) -> dict[str, Any]`, `conversation_attachment_count(conversation_id) -> int | None`
- `project_options(project_id=None) -> ProjectOptions`, `update_project(project: ProjectData) -> ProjectData`
- `LifecycleState`: `CREATED`, `OPENING`, `OPEN`, `OPEN_FAILED`, `CLOSING`, `CLOSED`, `CLOSE_FAILED`
- `Capability`: `CHAT`, `TEMPORARY`, `HISTORY`, `RENAME`, `DELETE`, `AUTH`, `ATTACHMENTS`, `RESOURCES`, `PROJECTS`, `PROJECT_CREATE`, `PROJECT_UPDATE`, `PROJECT_DELETE`, `PROJECT_MEMBERSHIP`, `MODELS`, `EFFORT`, `MESSAGE_EDIT`, `MESSAGE_RETRY`, `CHAT_BRANCH` *(Note: `EFFORT` is supported by ChatGPT, Claude, and Gemini; not Grok.)*

[Back to top](#contents)

---

## Chats and Collection Queries

Remote collections fetch records from the provider. A response's `parts`, `sources`, and `artifacts` collections read the in-memory response snapshot—listing a source or artifact does not download its linked remote file.

**Shared collection queries:**
- `list(predicate=None, *, limit=50, **options) -> list[T]` *(local `Collection` and `ModelsCollection` default to `limit=None`)*
- `get(item_id, /) -> T`
- `first(predicate=None, /, **options) -> T | None` · `last(...)` · `single(...)`
- `find(query, /, *, exact=False, startswith=False, endswith=False, single=False, last=False, **options) -> T | None`
- `search(query, /, *, exact=False, startswith=False, endswith=False, limit=None, **options) -> list[T]`

**`ChatsCollection` / `AsyncChatsCollection`:**
- `create(title="New Chat", *, project=None, temporary=False)`
- `get(item_id, /, *, include_projects=False, metadata_only=False)`
- `get_with_responses(item_id, /, *, limit=50, include_projects=False) -> tuple[Chat, list[Response]]`
- `list(predicate=None, *, limit=50, include_projects=False)`
- `update(chat, *, title=None)` · `delete(chat) -> None`

**`Chat` / `AsyncChat`:**
- `ask(prompt, *, model=None, attachments=None, **metadata)` *(Sync: `Response`, Async: `AsyncMessageOperation`)*
- `stream(prompt, *, model=None, attachments=None, **metadata)`
- `prepare() -> None` · `get(item_id) -> Response` · `rename(title)` · `delete()` · `download(target) -> bytes`
- `branch(message_id, *, title=None)` *(`ChatGPTChat`, `GeminiChat`)*
- `move_to(project)` *(`ChatGPTChat`: `Project | str | None`; `ClaudeChat`, `GrokChat`: `Project | str`)*

[Back to top](#contents)

---

## Messages, Attachments, and Responses

- **`Attachment`:** `Attachment(id, name, media_type=None, remote_url=None, *, path=None, content=None, mime_type=None, size=None, metadata=None)`, `from_path(path, *, name=None, mime_type=None)`, `from_bytes(data, name, *, mime_type=None)`, `from_text(text, name, *, mime_type="text/plain", id=None, metadata=None)`, `read_bytes() -> bytes`
- **`AttachmentPolicy`** (`webaiprovider.resources.message.attachment`): Describes upload constraints. The public `provider.attachment_policy()` method returns a dictionary or `None`.
- **`Attachments`:** `create(item, *, name=None, mime_type=None)`, `update(target, *, name=None, mime_type=None)`, `delete(target) -> bool`, `total_size -> int`
- **`Message` / `AsyncMessage`:** `(content="", *, role="user", id=None, remote_id=None, attachments=None, model=None, metadata=None, chat=None, response=None)` · `text`, `response`, `sources`, `artifacts`, `edit(new_content, *, model=None)`
- **`Response`:** `id`, `remote_id`, `text`, `parts -> ResponseParts` (`ThinkingPart`, `TextPart`), `message`, `model`, `sources`, `artifacts`, `timing`, `metadata`, `chat`, `retry(*, model=None, **options)`, `branch(*, title=None)`
- **`Source`:** `(id, url, title="", snippet="", kind="web", metadata={})`
- **`Artifact`:** `(id, kind, name, mime_type="application/octet-stream", size=None, download_url=None, content=None, metadata={})`
- **`TimingMetrics`:** `(started_at=0.0, first_token_at=None, completed_at=None, first_token_ms=None, duration_ms=0.0, metrics={})`
- **`ResponsesCollection` / `AsyncResponsesCollection`:** `create(message, *, model=None, attachments=None, **metadata)`, `edit(target, new_content, *, model=None, **options)`, `update(target, *, content=None, model=None, **options)`, `retry(target, *, model=None, **options)`, `list(predicate=None, *, limit=50, fresh=False)`, `delete(target)` *(local drafts only)*

[Back to top](#contents)

---

## Stream Operations, Events, and Hooks

- **`MessageOperation` / `AsyncMessageOperation`:** `stream()`, `wait()`, `start()`, `wait_for(event_kind, *, timeout=None, after=None)`, `cancel()`, `state`, `response`, `hook_results`
- **`OperationState`:** `PENDING`, `PREPARING`, `SENDING`, `STREAMING`, `COMPLETED`, `FAILED`, `CANCELLED`
- **`OperationEvent` → `StreamEvent`:**
  - `THINKING_BEGIN` → `ThinkingBegin(part_id=None)`
  - `THINKING_DELTA` → `ThinkingDelta(delta="", part_id=None)`
  - `THINKING_REPLACE` → `ThinkingReplace(text="", part_id=None)`
  - `THINKING_END` → `ThinkingEnd(text="", part_id=None)`
  - `OUTPUT_BEGIN` → `OutputBegin(part_id=None)`
  - `OUTPUT_DELTA` → `OutputDelta(delta="", part_id=None)`
  - `OUTPUT_REPLACE` → `OutputReplace(text="", part_id=None)`
  - `OUTPUT_END` → `OutputEnd(text="", part_id=None)`
  - `GENERATION_END` → `GenerationEnd()`
  - `VERIFICATION_BEGIN` → `VerificationBegin()`
  - `RESPONSE_PARTS_RESOLVED` → `ResponsePartsResolved(parts=())`
  - `SOURCE_DISCOVERED` → `SourceDiscovered(source=...)`
  - `ARTIFACT_DISCOVERED` → `ArtifactDiscovered(artifact=...)`
  - `STATUS_TERMINAL` → `TerminalStatus(status="completed", error=None)`
- **`OperationHooks` / `AsyncOperationHooks`:** `on(event_name, handler=None)`, `add(...)`, `before(...)`, `on_success(...)`, `on_error(...)`, `on_cancelled(...)`, `on_finally(...)`
- **`EventBus` / `AsyncEventBus`:** `subscribe(callback, *, event_filter=None, event_type=None, predicate=None, routing_key=None, once=False, times=None, replay=0)` · `EventFilter(event_type=None, predicate=None)` · `EventEnvelope`

[Back to top](#contents)

---

## Models and Effort Selection

- `Models`: `AUTO`, `LOW`, `MEDIUM`, `HIGH`
- `ModelsCollection` / `AsyncModelsCollection` *(default `ttl = 300.0` s)*: `list`, `get`, `find`, `search`, `resolve(target) -> ModelSelection`, `refresh()`
- `ModelRef(provider, id, name="", *, efforts=EffortSet, rank=None, metadata={})`: `default_effort_id`, `select(effort=None) -> ModelSelection`
- `EffortRef(provider, model_id, id, name="", rank=None, metadata={})`
- `ModelSelection(model, effort=None)`
- Static catalogs: `ChatGPTModels`, `ClaudeModels`, `GeminiModels`, `GrokModels`

[Back to top](#contents)

---

## Projects and Project Attachments

- **`ProjectsCollection` / `AsyncProjectsCollection`:** `create(title, *, instruction=None)` *(`ChatGPTProjectsCollection.create` adds `memory_share=True`)*, `update(project, *, title=None, instruction=None)`, `delete(project)` + standard collection queries
- **`Project` / `AsyncProject`:** `id`, `remote_id`, `title`, `instruction` (`get()`, `edit(text)`, `clear()`), `chats`, `attachments`, `rename(title)`, `delete()`
- **`ProjectAttachments` / `AsyncProjectAttachments`:** `create(file_or_data, resource_type=None, *, name=None, mime_type=None, **options)`, `create_many(*items)`, `delete(attachment)`, `delete_many(*attachments)`, `download(attachment)`, `update(attachment, **options)` + standard queries

[Back to top](#contents)

---

## Transport, Frame Decoders, and Timing

- **`webaiprovider.transport`:** `TransportConfig(timeout=(10.0, 50.0), max_response_bytes=67_108_864, max_stream_bytes=None)` *(stream total size is unbounded by default)*, `HTTPTransport`, `AsyncHTTPTransport`, `HttpSession`, `AsyncHttpSession`, `Transport`, `AsyncTransport`, `TransportResponse`, `StreamResponse`, `AsyncStreamResponse`, `parse_retry_after`
- **`DownloadSource`** (`webaiprovider.transport.downloader`): Internal source descriptor used by the downloader. In application code, download via `chat.download(artifact)` or `project.attachments.download(attachment)`.
- **`webaiprovider.transport.frames`:** `SSEFrameDecoder(*, max_event_bytes=2_000_000, max_data_lines=65_536, utf8_errors="replace")`, `SSEFrame`, `JSPBFrameDecoder(*, prefix="", max_frame_units=8_388_608, length_includes_separator=False)`
- **`webaiprovider.core.timing`:** `SendTiming(provider_id="unknown")`, `use_send_timing(timing)`, `current_send_timing()`

[Back to top](#contents)

---

## Exceptions

- **`from webaiprovider import ...`:**
  `WebAIProviderError`, `AccountPaused`, `AmbiguousMatchError`, `ConfigurationError`, `ContentError`, `FeatureUnavailable`, `OperationCancelledError`, `ProfileError`, `RateLimited`, `RemoteNotFound`, `UnsupportedOperationError`
- **`from webaiprovider.errors import ...`:**
  In addition to the above: `RequestNotSentError`, `StreamError`, `FrameDecodeError`, `AuthenticationError`, `CompatibilityError`, and all `Profile*` subclasses (`ProfileNotFoundError`, `ProfileSessionNotFoundError`, `ProfileDependencyError`, `ProfilePermissionError`, `ProfileDatabaseError`, `ProfileKeyringError`, `ProfileDecryptionError`, `ProfileTimeoutError`, `ProfileInvalidResultError`).
- **Provider modules:**
  - `webaiprovider` / `providers.chatgpt.errors`: `ChatGPTError`, `ChatGPTAuthenticationError`, `SentinelCompatibilityError`
  - `providers.claude.errors`: `ClaudeError`, `ClaudeCompatibilityError`, `ClaudeRequestNotSent`, `ClaudeStreamError`
  - `providers.gemini.errors`: `GeminiError`, `GeminiSignInRequired`, `GeminiGenerationRejected`, `GeminiNotFoundError`, `GeminiCompatibilityError`, `GeminiStreamError`
  - **Grok:** Defines no provider-specific exception subclasses; raises shared `WebAIProviderError`, `RateLimited`, and `AccountPaused`.

[Back to top](#contents)

---

[← Previous: 05 · Events, Transport, and Errors](events-transport-and-errors.md) · [Documentation Map](README.md)
