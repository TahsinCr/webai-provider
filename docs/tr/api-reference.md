[← Önceki: 05 · Olaylar, Transport ve Hatalar](events-transport-and-errors.md) · [Belge Haritası](README.md)

# 06 · Hızlı API Başvuru Kartı

Günlük kullanımda başvurulan kurucular, koleksiyon işlemleri, akış olayları ve istisnalar. Kullanım akışları ve örnekler için [belge haritasına](README.md), paket kökünden dışa aktarılan adların tam listesi için [paket tanımına](../../webaiprovider/__init__.py) bak.

---

## Konu Dizini

- [Hesaplar ve Kimlik Depoları](#hesaplar-ve-kimlik-depoları)
- [Sağlayıcılar ve Yaşam Döngüsü](#sağlayıcılar-ve-yaşam-döngüsü)
- [Sohbetler ve Koleksiyon Sorguları](#sohbetler-ve-koleksiyon-sorguları)
- [Mesajlar, Ekler ve Yanıtlar](#mesajlar-ekler-ve-yanıtlar)
- [Akış Operasyonları, Olaylar ve Kancalar](#akış-operasyonları-olaylar-ve-kancalar)
- [Modeller ve Efor Seçimi](#modeller-ve-efor-seçimi)
- [Projeler ve Proje Ekleri](#projeler-ve-proje-ekleri)
- [Transport, Frame Çözücüler ve Zamanlama](#transport-frame-çözücüler-ve-zamanlama)
- [İstisnalar](#istisnalar)

---

## Hesaplar ve Kimlik Depoları

```python
from webaiprovider import (
    Account,
    CredentialStore,
    FileCredentialStore,
    MemoryCredentialStore,
    ProviderType,
)
```

- `ProviderType`: `CHATGPT = "chatgpt"`, `CLAUDE = "claude"`, `GEMINI = "gemini"`, `GROK = "grok"` (`ProviderType.parse(value)`).
- `Account(id, provider, credentials, capabilities=(), metadata={}, name=None, is_active=True)`
  - `Account.from_browser(provider, *, browser=None, id=None, name=None) -> Account`
  - `Account.from_cookies(provider, cookies, *, id=None, name=None) -> Account`
  - `account.store() -> CredentialStore`
- `CredentialStore` *(Protocol)*: `load() -> dict`, `save(value: dict) -> None`, `exists() -> bool`, `delete() -> None`
- `FileCredentialStore(path)`: `save(value, *, ensure_ascii=False, newline=False)` *(maks. 1 MiB, atomik, `0o600`)*
- `MemoryCredentialStore(initial=None)`

[Yukarı dön](#konu-dizini)

---

## Sağlayıcılar ve Yaşam Döngüsü

| Sınıf | Kurucu İmzası |
| --- | --- |
| `Provider` / `AsyncProvider` | `(account, *, type=None, transport=None)` |
| `ChatGPTProvider` / `AsyncChatGPTProvider` | `(account, *, type=None, session: BrowserSession \| None = None, client_factory=None, transport=None)` |
| `ClaudeProvider` / `AsyncClaudeProvider` | `(account, *, type=None, transport=None)` |
| `GeminiProvider` | `(account, *, type=None, client_factory=None, notebooks: GeminiNotebooks \| None = None, transport=None)` |
| `AsyncGeminiProvider` | `(account, *, type=None, client_factory=None, notebooks: AsyncGeminiNotebooks \| None = None, transport=None)` |
| `GrokProvider` / `AsyncGrokProvider` | `(account, *, type=None, client_factory=None, transport=None)` |

`Provider(account)` ve `AsyncProvider(account)` hesap türüne göre somut sağlayıcıyı seçer. Sağlayıcıya özel tipleri ve seçenekleri IDE’de görmek için ilgili somut sınıfı doğrudan kullan. Örneğin `ChatGPTProvider` üzerinden açılan projelerde `memory_share` ayarlanabilir. Ayrıntılar [sağlayıcı rehberinde](provider-guides.md).

**Ortak Özellikler ve Metotlar (Async'te G/Ç çağrıları `await` edilir):**
- Özellikler: `id`, `type`, `account_id`, `provider`, `account`, `capabilities`, `opened`, `closing`, `closed`, `lifecycle_state`, `chats`, `projects`, `models`, `events`, `hooks`, `transport`
- `open() -> Self`, `close() -> None` (`with` / `async with` destekli)
- `ask(prompt, *, model=None, attachments=None) -> Response`
- `authenticate() -> SessionStatus`, `session_status() -> SessionStatus`, `forget_session() -> SessionStatus` (`SessionStatus`, `SessionState`: `from webaiprovider.accounts.status import SessionState, SessionStatus`)
- `attachment_policy() -> dict | None`
- `upload_file(path) -> dict[str, Any]`, `conversation_attachment_count(conversation_id) -> int | None`
- `project_options(project_id=None) -> ProjectOptions`, `update_project(project: ProjectData) -> ProjectData`
- `LifecycleState`: `CREATED`, `OPENING`, `OPEN`, `OPEN_FAILED`, `CLOSING`, `CLOSED`, `CLOSE_FAILED`
- `Capability`: `CHAT`, `TEMPORARY`, `HISTORY`, `RENAME`, `DELETE`, `AUTH`, `ATTACHMENTS`, `RESOURCES`, `PROJECTS`, `PROJECT_CREATE`, `PROJECT_UPDATE`, `PROJECT_DELETE`, `PROJECT_MEMBERSHIP`, `MODELS`, `EFFORT`, `MESSAGE_EDIT`, `MESSAGE_RETRY`, `CHAT_BRANCH` *(Not: `EFFORT` yeteneği ChatGPT, Claude ve Gemini'de vardır; Grok'ta yoktur.)*

[Yukarı dön](#konu-dizini)

---

## Sohbetler ve Koleksiyon Sorguları

Uzak koleksiyonlar sağlayıcıdan kayıt getirir. Yanıtın `parts`, `sources` ve `artifacts` koleksiyonları eldeki yanıt verisini okur. Bir kaynağın veya çıktının listelenmesi, bağlı dosyanın indirildiği anlamına gelmez.

**Ortak koleksiyon sorguları:**
- `list(predicate=None, *, limit=50, **options) -> list[T]` *(yerel `Collection` ve `ModelsCollection`: varsayılan `limit=None`)*
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

[Yukarı dön](#konu-dizini)

---

## Mesajlar, Ekler ve Yanıtlar

- **`Attachment`:** `Attachment(id, name, media_type=None, remote_url=None, *, path=None, content=None, mime_type=None, size=None, metadata=None)`, `from_path(path, *, name=None, mime_type=None)`, `from_bytes(data, name, *, mime_type=None)`, `from_text(text, name, *, mime_type="text/plain", id=None, metadata=None)`, `read_bytes() -> bytes`
- **`AttachmentPolicy`** (`webaiprovider.resources.message.attachment`) dosya ekleme sınırlarını temsil eder. Sağlayıcının genel API’sindeki `attachment_policy()` sözlük veya `None` döndürür.
- **`Attachments`:** `create(item, *, name=None, mime_type=None)`, `update(target, *, name=None, mime_type=None)`, `delete(target) -> bool`, `total_size -> int`
- **`Message` / `AsyncMessage`:** `(content="", *, role="user", id=None, remote_id=None, attachments=None, model=None, metadata=None, chat=None, response=None)` · `text`, `response`, `sources`, `artifacts`, `edit(new_content, *, model=None)`
- **`Response`:** `id`, `remote_id`, `text`, `parts -> ResponseParts` (`ThinkingPart`, `TextPart`), `message`, `model`, `sources`, `artifacts`, `timing`, `metadata`, `chat`, `retry(*, model=None, **options)`, `branch(*, title=None)`
- **`Source`:** `(id, url, title="", snippet="", kind="web", metadata={})`
- **`Artifact`:** `(id, kind, name, mime_type="application/octet-stream", size=None, download_url=None, content=None, metadata={})`
- **`TimingMetrics`:** `(started_at=0.0, first_token_at=None, completed_at=None, first_token_ms=None, duration_ms=0.0, metrics={})`
- **`ResponsesCollection` / `AsyncResponsesCollection`:** `create(message, *, model=None, attachments=None, **metadata)`, `edit(target, new_content, *, model=None, **options)`, `update(target, *, content=None, model=None, **options)`, `retry(target, *, model=None, **options)`, `list(predicate=None, *, limit=50, fresh=False)`, `delete(target)` *(yalnız yerel taslak)*

[Yukarı dön](#konu-dizini)

---

## Akış Operasyonları, Olaylar ve Kancalar

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

[Yukarı dön](#konu-dizini)

---

## Modeller ve Efor Seçimi

- `Models`: `AUTO`, `LOW`, `MEDIUM`, `HIGH`
- `ModelsCollection` / `AsyncModelsCollection` *(varsayılan `ttl = 300.0` sn)*: `list`, `get`, `find`, `search`, `resolve(target) -> ModelSelection`, `refresh()`
- `ModelRef(provider, id, name="", *, efforts=EffortSet, rank=None, metadata={})`: `default_effort_id`, `select(effort=None) -> ModelSelection`
- `EffortRef(provider, model_id, id, name="", rank=None, metadata={})`
- `ModelSelection(model, effort=None)`
- Statik kataloglar: `ChatGPTModels`, `ClaudeModels`, `GeminiModels`, `GrokModels`

[Yukarı dön](#konu-dizini)

---

## Projeler ve Proje Ekleri

- **`ProjectsCollection` / `AsyncProjectsCollection`:** `create(title, *, instruction=None)` *(`ChatGPTProjectsCollection.create` ek: `memory_share=True`)*, `update(project, *, title=None, instruction=None)`, `delete(project)` + standart koleksiyon sorguları
- **`Project` / `AsyncProject`:** `id`, `remote_id`, `title`, `instruction` (`get()`, `edit(text)`, `clear()`), `chats`, `attachments`, `rename(title)`, `delete()`
- **`ProjectAttachments` / `AsyncProjectAttachments`:** `create(file_or_data, resource_type=None, *, name=None, mime_type=None, **options)`, `create_many(*items)`, `delete(attachment)`, `delete_many(*attachments)`, `download(attachment)`, `update(attachment, **options)` + standart sorgular

[Yukarı dön](#konu-dizini)

---

## Transport, Frame Çözücüler ve Zamanlama

- **`webaiprovider.transport`:** `TransportConfig(timeout=(10.0, 50.0), max_response_bytes=67_108_864, max_stream_bytes=None)` *(toplam akış boyutu varsayılan olarak sınırsızdır)*, `HTTPTransport`, `AsyncHTTPTransport`, `HttpSession`, `AsyncHttpSession`, `Transport`, `AsyncTransport`, `TransportResponse`, `StreamResponse`, `AsyncStreamResponse`, `parse_retry_after`
- **`DownloadSource`** (`webaiprovider.transport.downloader`) indiricinin kullandığı kaynak tanımıdır. Kullanıcı kodunda indirme için `chat.download(artifact)` veya `project.attachments.download(attachment)` kullan.
- **`webaiprovider.transport.frames`:** `SSEFrameDecoder(*, max_event_bytes=2_000_000, max_data_lines=65_536, utf8_errors="replace")`, `SSEFrame`, `JSPBFrameDecoder(*, prefix="", max_frame_units=8_388_608, length_includes_separator=False)`
- **`webaiprovider.core.timing`:** `SendTiming(provider_id="unknown")`, `use_send_timing(timing)`, `current_send_timing()`

[Yukarı dön](#konu-dizini)

---

## İstisnalar

- **`from webaiprovider import ...`:**
  `ProviderError`, `AccountPaused`, `AmbiguousMatchError`, `ConfigurationError`, `ContentError`, `FeatureUnavailable`, `OperationCancelledError`, `ProfileError`, `RateLimited`, `RemoteNotFound`, `UnsupportedOperationError`
- **`from webaiprovider.errors import ...`:**
  Yukarıdakilere ek olarak `RequestNotSentError`, `StreamError`, `FrameDecodeError`, `AuthenticationError`, `CompatibilityError` ve tüm `Profile*` alt sınıfları (`ProfileNotFoundError`, `ProfileSessionNotFoundError`, `ProfileDependencyError`, `ProfilePermissionError`, `ProfileDatabaseError`, `ProfileKeyringError`, `ProfileDecryptionError`, `ProfileTimeoutError`, `ProfileInvalidResultError`).
- **Sağlayıcı modülleri:**
  - `webaiprovider` / `providers.chatgpt.errors`: `ChatGPTError`, `ChatGPTAuthenticationError`, `SentinelCompatibilityError`
  - `providers.claude.errors`: `ClaudeError`, `ClaudeCompatibilityError`, `ClaudeRequestNotSent`, `ClaudeStreamError`
  - `providers.gemini.errors`: `GeminiError`, `GeminiSignInRequired`, `GeminiGenerationRejected`, `GeminiNotFoundError`, `GeminiCompatibilityError`, `GeminiStreamError`
  - **Grok:** Özel bir alt sınıf tanımlamaz; ortak `ProviderError`, `RateLimited` ve `AccountPaused` sınıflarını kullanır.

[Yukarı dön](#konu-dizini)

---

[← Önceki: 05 · Olaylar, Transport ve Hatalar](events-transport-and-errors.md) · [Belge Haritası](README.md)
