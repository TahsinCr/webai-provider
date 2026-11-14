# Changelog

The format follows [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Add tag-triggered PyPI releases through Trusted Publishing, gated by CI, CodeQL, and verified platform distributions.
- Reorganize the library under the standalone `webaiprovider` package with PEP 561 `py.typed` metadata and a single-source `__version__`.
- Split the public API into `BaseProvider`, `AsyncProvider`, blocking `Provider` (whose generic constructor selects the concrete provider class directly), and provider-specific sync/async facades (`ChatGPTProvider`, `AsyncChatGPTProvider`, `ClaudeProvider`, `AsyncClaudeProvider`, `GeminiProvider`, `AsyncGeminiProvider`, `GrokProvider`, `AsyncGrokProvider`) with explicit `open()` and `close()` lifecycles.
- Publish the typed message-operation pipeline contracts in `webaiprovider.resources.message.protocol`, normalized stream events (`OutputBegin`, `OutputDelta`, `OutputReplace`, `OutputEnd`, `ThinkingBegin`, `ThinkingDelta`, `ThinkingEnd`, `SourceDiscovered`, `ArtifactDiscovered`, `TerminalStatus`), and origin-scoped `provider.transport` with custom transport injection, request/stream execution strategies, and bounded `PortalBridge` orchestration.
- Expose resource managers and typed collections across sync and async APIs for chats, messages, projects (including paginated `ProjectChatsCollection`, chat moves, and ChatGPT `memory_share: bool` selection), project instructions/resources, and attachments, while exposing and synchronizing `chat.metadata` across listing, branching, and `messages.refresh()`.
- Add account-scoped `provider.models` discovery (`list()`, `get()`, `find()`, `search()`, and explicit `refresh()`), retaining valid cached session catalogs if a refresh fails, alongside model-specific typed `EffortSet` namespaces returning bound `EffortRef` choices and active Claude Opus and Fable catalog entries.
- Export `read_session_async` and `save_session_async` in `webaiprovider.accounts` for non-blocking session persistence, and route explicit `Account.from_browser(browser=...)` selection through profile discovery and cookie import while keeping OS-default detection when omitted.

### Changed

- Align the module layout around clean package boundaries (`webaiprovider.base`, `webaiprovider.provider` with `ProviderType`, `webaiprovider.async_provider`, `webaiprovider.collections`, `webaiprovider.resources`, `webaiprovider.normalizers`, `webaiprovider.frames`, `webaiprovider.core`, `webaiprovider.transport`, and `webaiprovider.providers.<provider>`); use plain names for synchronous APIs and the `Async` prefix for asynchronous providers, resources, and collections.
- Rename immutable transfer snapshots to `MessageData`, `ProjectData`, and `ResourceData` to separate them from SDK resource objects, and give `Attachments` collection-style `create()`, metadata-only `update()`, and local `delete()` with provider-specific attachment validation and ChatGPT upload cancellation.
- Consolidate message send, edit, retry, and `ask()` model selection onto a single `model=` parameter accepting `Models` policies, `ModelRef`, or `EffortRef` (`AUTO` resolves to the provider default) checked against the account's live catalog without silent downgrades; separate ChatGPT web IDs from API IDs using dynamic Astra discovery and removing work-only Luna and Sol entries from `webaiprovider.providers.chatgpt.catalogs`.
- Run a single producer task per `MessageOperation` with attached-session enforcement (`ProviderError` on detached chats), scan each event once per stage waiter with unified terminal-event publication, and drain up to 32 buffered stream events per `stream()` iteration.
- Unify HTTP request and stream execution on `curl_cffi` across `HTTPTransport`, `AsyncHTTPTransport`, `AsyncRequestTransport`, `PortalTransport`, and `ThreadedTransport` (and stdlib `urllib.request` for CDP metadata) with immutable origins, single-owner sync stream closure, non-blocking `PortalTransport` reuse across `AsyncChatGPTProvider`, bounded DNS preparation across concurrent read-timeout retries, 300s cached DNS resolution and IPv4 fallback across sync/async `curl_cffi` sessions and downloads, cached curl resolve headers, off-loop IPv6 probing, and per-session HTTP `429 Retry-After` cooldown propagation across ChatGPT and Gemini.
- Share incremental wire framing in `webaiprovider.frames` (`SSEFrameDecoder` scanning coalesced packets once without tail copies across ChatGPT, Claude, and Grok, plus provider-independent `JSPBFrameDecoder` configured by Gemini), parse project-source upload streams without repeated buffer copies, offload async credential I/O and upload preparation via `read_session_async` and `asyncio.to_thread`, debounce auxiliary non-auth and Gemini `batch()` cookie writes while immediately flushing rotated session, Grok (`sso`/`sso-rw`), Cloudflare (`__cf_bm`/`cf_clearance`), and Gemini (`PSIDCC`/`SIDCC`) cookies, persist dirty `HttpSession` cookies before resetting failed clients, and store `_SESSION_WRITE_LOCKS` in a `WeakValueDictionary`.
- Optimize provider request and locking paths across `webaiprovider.providers`:
  - ChatGPT: isolate pre-signed uploads on a lazy cookie-free `_upload_http` client, accelerate Sentinel FNV-1a/XOR/proof-of-work paths via an optional C99 `_native` extension built from the standalone `sentinel/` C99 library (with pure-Python fallback), stream `image/png` uploads from disk after header inspection instead of buffering up to 20 MiB in RAM, URL-encode interpolated IDs, merge `refresh_session` cookie pairs directly without `SimpleCookie`, cache `system_timezone()` per `TZ`, release `_sentinel_lock` around `/prepare` I/O using a `threading.Event` while keeping verified pooled clients warm after `authenticate()`, skip Sentinel prewarm after close, optimize PoW prefix hashing and prewarm after temporary turns, skip duplicate `get_conversation` GETs once the primary endpoint is verified, preserve shared `temporary_conversations` and `pending_branches` state across client disposal, and run `list_project_chats` outside `_gate` with a read semaphore.
  - Claude, Gemini, and Grok: align Gemini's default TLS impersonation to `curl_cffi`'s `chrome` profile with single-pass bootstrap regex parsing, run `AsyncClaudeProvider.upload_file()` outside `_gate`, bound parallel attachment uploads to two ordered workers across Claude, Gemini, and Grok, fetch Claude project details, docs, and files concurrently, reuse `resource_id`-matched `ResourceData` and staged `ProjectData` snapshots across removals and single-argument `projects.update()`, cache Claude continuations and Gemini notebook context with invalidation on edits/deletes/errors/close, defer Gemini and Claude stream source/file normalization until completion, release `_gate` before delete readback polling, deduplicate Grok cookie writes, and paginate all project/project-chat pages via shared `gather_bounded` (canceling sibling reads and retaining cached state on invalid, repeated, or over-limit cursors).
- Type rejected ChatGPT sessions as `AuthenticationError` (restricting browser-cookie refresh and retry to the live test harness), run both `sync` and `async` APIs by default in `run_live_tests.py` with structured skip/failure accounting, `ActionGate` pacing across deletes and local operations, shared async cleanup over the sync provider portal, one-time session-open timeout retries without mutating retries, sanitized timeout reporting, pre-call `chat.branch` journaling, preserved `attempted=True` state across `RateLimited` failures, per-provider and legacy cleanup routing, and OS lock serialization fallback when file locks are unsupported.

### Removed

- Remove the legacy `webai_provider` compatibility namespace and old module layout (`session.py`, `sync.py`, `facade.py`, `models.py`, `contracts.py`, `streams.py`, `catalogs/`, `provider_type.py`).
- Remove `ProviderBridge`, `ProviderAdapter` and its factory, `providers/base.py`, legacy adapter request/event interfaces, the duplicate `Feature`/`ProviderDescription`/`describe()` API, compatibility aliases (`WebSession`, `ChatGPTWebError`, `CapabilityError` and dict-emulating cache helpers), unused connector and feature-only catalog status values (in favor of `ModelCatalogState`), generic message branching and collection move helpers (now exposed on supported provider chat types), and the no-op synchronous `Provider.__repr__` override.
- Remove the `httpx` / `httpcore` runtime dependency alongside `AsyncHTTPXTransport`, `safe_httpx_sync_transport`, and `safe_httpx_async_transport` (and migrate test doubles onto shared internal fakes).
- Remove unused v1 compatibility shims, dead helpers, and obsolete tests/scripts, including `webaiprovider.core.resilience` (`ProviderResilienceRegistry`), `webaiprovider.core.workers` (`BoundedWorkPool`), `RetryPolicy` and `calculate_backoff` from `webaiprovider.transport.retry`, `NetworkError`, `redact_sensitive_url` and `follow_redirects` on `NativeDownloadClient`, `Account.to_dict`, `ModelCatalog.from_dict`, `select_cookies`/`import_cookies` aliases in `webaiprovider.accounts.profiles`, `MultiDimensionBoundedCache.move_to_end`, `ConcurrencyCoordinator.run_keyed`, `ProviderResourceScope.spawn`/`cancel`, `OperationStateMachine.is_terminal`, dead v1 provider/client/session/notebook methods (`login`, `forget_temporary`, `forget_conversation`, `prepare_for_send`, `prepare_next_turn`, `BrowserSession.launch`, `find_chrome_executable`, unused `Notebooks` setup flags, `prefer_browser` on `refresh_auth`), the standalone package smoke script (folded into CI wheel/sdist checks), and redundant test helpers/modules such as `tests/test_api_types.py`.

### Fixed

- Preserve `Conversation.metadata` on `AsyncChat` and `Chat` across `AsyncBranchableChat.branch()`, chat listing, and `AsyncMessagesCollection.refresh()`, clear transient `"pending_branch"` metadata on `ConversationBound` in `AsyncProvider.execute_stream()`, bind shared `pending_branches` onto the ChatGPT client inside `AsyncChatGPTProvider.branch_chat()`, and fix an `UnboundLocalError` on `cached` in `ChatGPTClient._resolve_send_previous` when sending the first turn on a branched preview.
- Emit ChatGPT `ConversationBound` mid-stream and yield Claude `bound` before `move_chat` and completion, defer live ChatGPT `pending_branch` renames until the first branched message binds a remote ID (excluding synthetic preview IDs), map ChatGPT HTTP 429 and transport cooldown errors to `RateLimited(..., message_submitted=False)`, and clear pre-send cleanup journal `attempted` state when a pre-submission rate limit occurs without a bound ID.
- Normalize malformed SSE/RPC JSON frames to `ClaudeCompatibilityError` and `GeminiCompatibilityError`, preserve `ProviderError` size-limit errors in `download_source`, process ChatGPT project-source uploads to completion with expected source timestamps, reject oversized project-source events before JSON decoding even in single-chunk payloads, and use shared `Retry-After` parsing for upload rate limits.
- Resolve exact model and effort IDs before display names or partial matches (including IDs that prefix another catalog entry), await futures and custom awaitables returned by hooks before continuing operations, count multiline SSE separators toward payload limits while excluding an optional initial UTF-8 BOM, expose provider capability metadata on synchronous providers, and translate exception messages to English.
- Keep live test verification and teardown reliable: continue cleaning up the parent temporary chat in `tests/live/test_messages.py` if branch-entry cleanup raises while surfacing the first `CleanupError`, verify temporary-object identity and exhaust remote listing pages before completing uncertain deletes, preserve `CleanupError` root causes without duplicate retries, keep disposable project names within service limits, preserve async message-operation handles, and skip remaining tests for a failed provider/API setup without duplicating requests.

### Security

- Ignore ambient proxy environment variables across internal HTTP clients by default (`trust_env=False`), and enforce DNS pinning and private/link-local SSRF blocking directly across sync and async `curl_cffi` transports (`HTTPTransport`, `AsyncHTTPTransport`, `AsyncRequestTransport`) while preserving TLS SNI and hostname-level validation across `NO_PROXY` bypasses.
- Restrict downloads to provider-supplied `DownloadSource` values with public-IP pinning, connect-timeout budgets across candidate IPs, disabled redirects, and a 100 MiB streaming ceiling (`max_bytes`), and sanitize download and SAS upload transport/HTTP errors without leaking signed URL parameters.
- Enforce regular-file descriptor (`os.fstat` / `stat.S_ISREG`) and post-upload file identity checks for local file uploads and `FileCredentialStore.load()`, enforce thread and cross-process credential file locking (`flock`, `msvcrt.locking`, or exclusive create fallback), and reject invalid Sentinel VM regex patterns (length/nested quantifiers) and invalid PoW slot indices (`nonce_index <= 0`).

## [1.3-beta] - 2026-11-06

### Added

- Canonical facade `webai_provider.api` (re-exported by `webai_provider/__init__.py`) providing clean public exports for all providers, models, protocols, accounts, transformers, and utilities.
- Dedicated quarantine module `webai_provider.compat` isolating deprecated backwards-compatibility shims for future v2.0 removal.
- Consolidated `webai_provider.accounts` package unifying profile discovery, cookie filtering, SQLite WAL snapshotting with `webai-profile-` temp prefixes, and session importing.
- Hardened `NativeDownloadClient` with SSRF and link-local/cloud metadata IP protection (`169.254.169.254`).
- Sensitive URL query parameter and userinfo credential redaction with `redact_sensitive_url`.
- Implement Stage 3 identity, cookie, and networking layer across `webai_provider.accounts`, `webai_provider.auth`, and `webai_provider.transport`:
  - `accounts`: typed `Account` entity, `CredentialStore` protocol, `FileCredentialStore` with atomic replace, fsync, and 0o600 file permissions, and thread-safe `MemoryCredentialStore`.
  - `auth`: `ProviderCookieFilter` with origin enforcement and character sanitation, `CookieReader` profile discovery, and unified `import_browser_session`.
  - `transport`: origin-scoped `HttpSession` (with `WebSession` alias), RFC HTTP-Date and delta-seconds `parse_retry_after`, `calculate_backoff`, `RetryPolicy`, and `BoundedWorkPool` offloading payloads >= 256KB from the event loop.
- Add modular streaming framers in `webai_provider.framers`: `SSEFramer` for RFC-compliant Server-Sent Events with keep-alive skipping, and `JSPBFramer` for Gemini batchexecute Protobuf-over-JSON length-prefixed stream decoding.
- Add streaming text and citation transformers in `webai_provider.transformers`: `StreamReconciler` for diff-based snapshot reconciliation, `extract_citations` / `sources_from` for unified web/file citations, and `GrokTextFilter` / `clean_render_markup` / `strip_protocol_tags` for protocol and render markup sanitation.
- Expand core error hierarchy in `webai_provider.errors`: introduce unified `StreamError`, `RequestNotSentError`, `NetworkError`, and `AuthenticationError` base classes.
- Create dedicated `providers.grok.errors` module with `GrokError`, `GrokStreamError`, and `GrokCompatibilityError`.
- Consolidate common provider base layer in `webai_provider.base`: introduce `ProviderAdapter` replacing `Backend` protocol and `OptionalFeatures` mixin.
- Integrate EventFlow concurrency, cache, and resilience primitives in `webai_provider.core`:
  - `MultiDimensionBoundedCache`: dual-bounded (entry count and character size) cache with TTL expiration.
  - `ConcurrencyCoordinator`: fine-grained keyed concurrency limiting (`KeyedConcurrencyLimiter`) and single-flight request coalescing (`SingleFlight`).
  - `ProviderResilienceRegistry`: adaptive cooldown tracking (`CooldownRegistry`) and fail-fast circuit breaking (`CircuitBreaker`).
  - `ProviderResourceScope`: deterministic async task and resource cleanup scopes (`AsyncTaskScope`, `AsyncResourceScope`).
  - `SyncPortalBridge`: bidirectional sync-async loop portal bridge for off-loop thread orchestration.
- Introduce typed profile error hierarchy (`ProfileError`, `ProfileNotFoundError`, `ProfileKeyringError`, `ProfileDatabaseError`, `ProfilePermissionError`, `ProfileTimeoutError`, `ProfileDependencyError`, `ProfileDecryptionError`, `ProfileSessionNotFoundError`, `ProfileInvalidResultError`) replacing string-based failure lookups.

### Changed

- Optimize Gemini streaming performance by yielding SSE chunks outside `self._gate` lock, matching Grok's non-blocking concurrency pattern and eliminating stalls on concurrent tab interactions.
- Accelerate Gemini model discovery by parallelizing GraphQL catalog queries and batchexecute `otAQ7b` RPC via `asyncio.gather`, reducing catalog turnaround by ~400–600 ms.
- Accelerate UTF-16 payload buffer scanning in Gemini `Frames` with a C-speed BMP fast path slice, speeding up per-chunk processing up to 26x over character-by-character scans.
- Retain candidate response IDs (`rcid`) across trailing chunk frames in Gemini `generate()`, allowing continuation metadata caching to succeed and completely eliminating redundant `hNvQHb` RPC round-trips (saving 500–1,500 ms on every continuation turn).
- Prevent premature cache invalidation in `GeminiProvider._take_chat_metadata()` and add `prepare_next_turn()` for LRU ordering.
- Mark session activity fresh upon initialization, avoiding blocking `sync_activity()` network calls on initial user turns.
- Optimize Sentinel Proof-of-Work solver by pre-encoding JSON buffer segments and amortizing timing measurements to once per 128 iterations.
- Reuse persistent `json.JSONDecoder` instance across frames in `JSPBFramer`.
- Optimize `SSEFramer` with C-accelerated line scanning (`find`) handling CRLF, standalone CR, and split chunk boundaries with zero byte-by-byte loop overhead.
- Keep provider capability state and descriptions in `base.py`; keep account session state in `accounts/status.py`.
- Refactor `ProviderClient` parameters, local variables, and test doubles to replace legacy `backend` naming with `adapter`.
- Enhance `NativeDownloadClient` with hostname validation and resilient default timeout configuration.
- Expand `CredentialContractTests` to verify memory store injection across all four bundled providers (ChatGPT, Claude, Gemini, Grok).
- Move standalone utility modules (`cdp.py`, `timing.py`, `files.py`, `history.py`, `attachments.py`) into the dedicated `webai_provider.utils` package.
- Re-export unified provider error classes across `providers.chatgpt.errors`, `providers.claude.errors`, `providers.gemini.errors`, and `providers.grok.errors`.
- Migrate package layout to a clean, flat `webai_provider` root directory without requiring a `src/` wrapper or setuptools package mapping hacks.
- Group bundled provider adapters (ChatGPT, Claude, Gemini, Grok) under the canonical `webai_provider.providers` subpackage.
- Standardize `pyproject.toml` configuration using `[tool.setuptools.packages.find]` for `webai_provider*` and `known-first-party = ["webai_provider"]`.

### Fixed

- Default optional-reasoning models (such as Gemini 4 Argon) to fast generation mode without reasoning, and cache discovered model catalog across connections to eliminate pre-send discovery overhead.
- Omit `ConversationBound` on existing conversation turns to resolve Gemini continuation 403 Forbidden errors.
- Fix Gemini model catalog discovery HTTP 403 Forbidden errors by switching GraphQL query execution from GET to POST with JSON payload, framework XSRF token header, and automatic token refresh.
- Preserve specific `chatgpt_sentinel_incompatible` error code by having `SentinelCompatibilityError` inherit directly from `ChatGPTError`.

### Removed

- Remove scattered root-level legacy compatibility modules (`auth/`, `session.py`, `messages.py`, `projects.py`, `profiles.py`), isolating legacy fallback shims exclusively in `webai_provider.compat`.
- Remove ad-hoc `webai_provider.contracts` module and legacy `Backend` protocol naming in favor of `ProviderAdapter`.
- Remove legacy `[tool.setuptools.package-dir]` virtual path mappings and `providers = "."` hacks.
- Remove runtime `sys.modules` monkey-patching from provider initialization.
- Remove deprecated `__verison__` backward-compatibility typo alias from public exports.
- Remove static `PROFILE_FAILURES` dictionary in favor of the typed profile error hierarchy.

## [1.2-beta] - 2026-11-05

### Added

- Support user message editing on Grok through its parent-response tree branching contract.
- Route image attachments to `imageAttachments` on Grok to enable multi-modal vision model analysis.
- Preserve and deduplicate both file and image attachments when editing user prompts or loading conversation history on Grok.
- Expose `__version__` and `__verison__` package version identifiers on the top-level `providers` namespace.

### Changed

- Defer Grok web-search citation resolution to stream completion, avoiding repeated per-chunk tree traversals and deduplicating citation results in $O(1)$ time.
- Map Grok `RATE_LIMIT_EXCEEDED`, `TOO_MANY_REQUESTS`, `QUOTA`, and textual cooldown errors directly to typed `RateLimited` exceptions with parsed retry delays.
- Intercept HTTP 3xx redirect responses during `WebSession.check` to immediately raise `AccountPaused` when web sessions expire.
- Omit retry and edit actions on Grok temporary chats across live streaming and loaded conversation history.
- Update SiopeChat integration guidance for its pinned PyPI dependency; no provider submodule is required.

### Fixed

- Prevent premature tag termination in `GrokTextFilter` by ignoring `>` characters located inside attribute quotes.
- Ensure deterministic leaf selection in Grok response trees by sorting sibling leaf nodes chronologically by creation timestamp.
- Add null-safety guards for Grok conversation, workspace, and workspace file endpoints when server payloads return `null` lists.
- Guard against missing MIME metadata when inspecting Grok attachment structures.

## [1.1-beta] - 2026-11-05

### Added

- Add Grok web-session support with server-validated authentication, account-specific model discovery, conversation history and streaming, retry, workspace instructions and resources, chat membership operations, published thinking summaries, and web-search citations. Workspace source uploads are limited to 3 MiB; unsupported actions remain outside its declared capabilities.

- Support Grok document attachments through the web upload endpoint, with bounded file and prompt sizes.
- Expose confirmed ChatGPT and Grok remote generation activity from conversation history; use a start time only when the provider returns one.

### Changed

- Parse both delta-seconds and HTTP-date `Retry-After` values so account cooldowns follow the provider's stated rate-limit window.

- Reduce per-chunk Grok protocol-filter work and consolidate stream finalization. Preserve authoritative text arriving after the first completion event, keep output buffers appendable, and report in-stream authentication failures through the typed session contract.
- Join account-available Grok chat modes with the server's model catalog to display names such as Grok 3 and route the matching model and mode, including server-defined model IDs without a fixed model or subscription list. Preserve the selected mode's reasoning policy instead of forcing reasoning off. Fetch both catalogs concurrently and reuse their cache; exclude unsupported interactive Build, Imagine and Voice workflows from chat selection.
- Show Grok chat modes with the provider name, remove internal render and tool-card markup from live and loaded text, and trim leading response whitespace.
- Use Grok's account-scoped chat-mode catalog, expose only modes marked available by the server, and keep the server default only when it is selectable.
- Verify Grok sessions through its authenticated user-settings resource instead of assuming private auth-status payload fields.
- Reduce Grok send preparation by reusing a recently confirmed response ID and fetching compact response-node metadata on cache misses; retries still validate against full active-branch history.
- Document Python 3.10 as the minimum version for ChatGPT, Gemini, Claude and Grok clients.
- Keep provider failures inside the shared typed error contract, expose account identity in the provider protocol, and add an explicit account preparation entry point.
- Start the existing ChatGPT Sentinel prewarm in the background for the selected account before its first send. Add provider-gate wait to opt-in send timings; validation behavior is unchanged.
- Measure Gemini and Claude catalog, preparation, history, SSE and first-text stages in the content-free opt-in send timing context.

### Fixed

- Set Markdown workspace uploads to `text/markdown` consistently instead of relying on platform MIME databases.
- Stream Grok responses concurrently outside the send preparation lock, parse multi-line SSE `data:` payloads while safely ignoring keep-alive comments, slice buffered filter text in one operation instead of character-by-character, and preserve user file attachments in conversation history.
- Retry Grok chat workspace moves with backoff to handle server eventual consistency.
- Apply partial Grok workspace updates without a duplicate read, allow instructions on project creation, remove file resources by their path even when an asset ID is present, and reuse verified resource metadata for deletion. Reject empty, oversized or duplicate source uploads before mutation and validate file-list responses before reporting success.
- Bound Grok stream lines before buffering them and clear verified-session state when a stream reports an expired login.
- Limit Grok model selection to the authenticated account's chat models and send its catalog-provided model mode. Use the supported regeneration flag for retries.

- Attach Claude web-search results to the answer sources during streaming and when loading saved conversations.

### Removed

- Remove unused CDP and readiness options from the Claude provider constructor.

## [1.0-beta] - 2026-11-03

### Added

- Add an asynchronous `ProviderClient` and account-scoped `Backend` for ChatGPT,
  Gemini and Claude web sessions, with model and effort discovery.
- Add browser-profile session import for Chromium, Firefox and Zen profiles,
  plus Safari cookie reading on macOS and local CDP session capture.
- Add provider-owned conversation history, projects and workspaces, including
  Gemini notebooks and their instructions, sources and media.
- Add message edit, retry and branch operations where each provider supports
  them, plus ChatGPT response versions and stream events for text, replacement
  text, citations, files, published thinking and response metadata.
- Add Claude incognito conversations.
- Add authenticated upload and download resolution for provider files,
  generated media and sandbox outputs.
- Add caller-owned credential stores, account isolation and explicit
  authentication checks.

### Changed

- Keep credentials, connection pools, catalogs, conversations and concurrent
  work scoped to the owning provider account.
- Reuse HTTP sessions and validated conversation state where safe; cache model
  catalogs and provider continuation metadata to avoid duplicate work.
- Prepare eligible ChatGPT send validation and Gemini continuation state ahead
  of the next request, with bounded cache lifetimes and explicit invalidation.
- Deliver streamed responses incrementally with bounded buffers and provider
  event parsing; keep cancellation and ambiguous-send behavior explicit.
- Use Gemini's bundled HTTPS transport and a native Python ChatGPT Sentinel
  runtime. Browser discovery, profile reads and local CDP access also run in
  Python.
- Keep provider dependencies in the standalone distribution and development
  tools optional; SiopeChat declares the runtime dependencies it needs while
  its wheel still bundles the provider import package.
- Expose provider capabilities and machine-readable errors through shared
  contracts while retaining provider-specific request and response behavior.

### Fixed

- Correct ChatGPT conversation routes, active-branch history, stream boundaries
  and reconciliation when response metadata is incomplete.
- Correct Gemini history and stream parsing, turn identity, model routing,
  notebook context, effort identifiers, continuation expiry and retries of
  earlier responses.
- Recover Claude workspace and model discovery when the saved workspace is
  stale, while preserving unambiguous account selection.
- Preserve account and conversation identity during concurrent reads, retries,
  cancellation, uploads and downloads; do not replay an ambiguous send.
- Resolve protected provider files with the owning account and keep credentials
  scoped to approved origins.
- Bound browser-profile, CDP and streaming input; report compatibility failures
  without exposing cookie values, tokens or raw sensitive responses.

### Security

- Validate imported sessions before replacing saved credentials and keep
  provider cookies scoped to the selected account and origin.
- Validate download destinations and redirects, enforce response size limits,
  and avoid forwarding credentials across origins.
- Bound profile, CDP and challenge inputs and fail explicitly when a required
  provider challenge is unsupported.

### Removed

- Remove the Node.js and QuickJS runtime paths, obsolete Sentinel wrappers and
  cache plumbing, and retired browser-based conversation transports.
- Remove compatibility entry points that are no longer part of the standalone
  provider API.

### Maintenance

- Expand provider contract, streaming, history, profile, upload and packaging
  regression coverage, including installed wheel and source-archive smoke
  checks on Python 3.10 and 3.14; run source checks on Linux, macOS and Windows.
- Keep the provider build, lint, package metadata and test checks in a
  standalone GitHub Actions workflow.
