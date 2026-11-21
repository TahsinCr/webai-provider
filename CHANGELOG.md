# Changelog

The format follows [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Resolve synchronous model ID lookups from the loaded catalog on the calling thread.

- Move response operation classes and streaming events to `webaiprovider.resources.response.operation`. Top-level imports remain available.

- Use `New Chat` as the default title for new conversations.

- Construct chat response collections directly across providers.

- Reduce history decoding work across ChatGPT, Claude, Gemini, and Grok. Reuse message visibility and action checks, skip empty citation scans, and select the latest Grok branch in one pass.

- List and resolve models from the loaded catalog directly on the calling thread in the synchronous API.

- Read the synchronous provider session status on the calling thread without starting an event loop.

- Provide a calling-thread `HttpSession` for saved-account HTTP requests and streams. The asynchronous counterpart is `AsyncHttpSession`.

- Run the default blocking HTTP transport’s response streams on the calling thread with bounded incremental reads.

- Run buffered requests on the default blocking HTTP transport in the caller's thread while enforcing the response size limit.
- Use `OperationHooks` for synchronous registration and `AsyncOperationHooks` for asynchronous registration and dispatch. Both use the same registration implementation.
- Use FlowEvent's cache and single-flight primitives for bounded continuation state and overlapping discovery reads.
- Assign, transfer, or detach ChatGPT chats with `chat.move_to(project)` or `chat.move_to(None)`. The operation uses the conversation's send lock and applies the destination selected by hooks consistently.
- Keep browser authentication in ChatGPT's auth module. Construct `BrowserSession` directly with a credential store or path.
- Preserve concrete provider and resource types in IDEs, including collection queries and context managers. Construct chat and project resources through their typed collections.
- Use curl_cffi for local browser debugging WebSockets and remove the separate websocket-client dependency. Require curl_cffi 0.17 or later for blocking WebSocket timeouts and message limits.
- Define the shared `BaseProvider` as an abstract class with typed chat, project, and model collections.
- Cancel unfinished ChatGPT project uploads on batch failure and verify attachments by their remote file IDs before reporting success.

- Preserve explicit Claude draft titles when the remote chat is created, so an interrupted first send leaves an identifiable chat.
- Reduce project rename and instruction readbacks on Claude and Grok by validating changed fields independently of source lists.
- Keep local project options responsive while another operation is generating a response.
- Request smaller project chat pages on ChatGPT and Grok and reduce repeated prefixes in unbounded Gemini chat listings.
- Normalize only the selected active Claude message during retry and edit preparation.
- Reduce memory allocations for Grok file reads and retained streaming events across providers.

- Report native stream DNS, connection, TLS, first-byte timing, and connection reuse when send diagnostics are enabled.
- Use smaller first pages for short chat lists and expand subsequent reads when filtering or duplicate records leave too few results.
- Reuse complete ChatGPT project creation responses and fetch project details only when the server response lacks the saved settings or permissions.
- Read ChatGPT project options independently of active message generation.
- Read only the latest Gemini turns for a retry or edit when the server confirms the known active turn and its parent.

- Resolve Claude retry and edit targets by their active native message identity when stream and history block indexes differ.

- Reduce history normalization work across providers and parse only the selected Gemini turn when preparing a retry or edit.

- Use concrete provider types consistently for chats, projects, messages, responses, sources, artifacts, and project attachments across the blocking and async APIs.
- Standardize provider project editors and reuse the current project snapshot through the shared mutation path.
- Define browser-cookie selection, validation, and persistence rules once per provider, including known alternate session cookies.
- Share local file version checks during upload, keep provider file preflight off the event loop, and simplify Gemini session and RPC parsing.
- Rename the Python import package from `webai_provider` to `webaiprovider`. The PyPI distribution remains `webai-provider`. Ship inline typing metadata and use `webaiprovider.__version__` as the package version source.
- Replace the v1.3 `ProviderClient` API with account-scoped `Provider(account)` and `AsyncProvider(account)`. Open and close a provider explicitly or use `with` and `async with`. Both constructors select the concrete provider from the account or an explicit `ProviderType`. Use a provider-specific class for service-specific features, such as `ChatGPTProvider` or `AsyncChatGPTProvider`. Blocking calls reuse the same async implementation through a persistent event loop.
- Send and read through `chat.responses`. Create a chat with `provider.chats.create()` or find an existing one with `provider.chats.get(id)`. Then `chat.responses.create(prompt)` prepares an operation, `operation.stream()` yields events, and `operation.wait()` returns the completed response. Both API variants also offer `chat.ask()` and `chat.stream()` shortcuts.
- Read previous prompts and answers together with `chat.responses.list()`. A `Response` exposes the associated user `message` when available, along with answer text, sources, artifacts, and timing for live sends. Use `response.message.edit()` or `response.retry()` where supported. Both return an operation with the same streaming and waiting interface as a new send.
- Manage project files and text sources with `project.attachments.create()`, `list()`, and `delete()`. Message attachments use a local `Attachments` collection with `create()`, metadata-only `update()`, and `delete()`. ChatGPT project creation accepts `memory_share=False` for project-only memory. Chat moves and branches remain available on the provider-specific chat types that support them.
- Share query behavior across local and remote collections in the blocking and async APIs. Remote collections read on demand and follow provider pagination. Keep explicit `refresh()` on the session model catalog.
- Use one `model=` argument for model and effort selection. Pass a typed model reference for its default effort or a choice from `model_ref.efforts` for a specific effort. These references carry provider IDs directly. String queries and selection policies resolve against the account catalog, which `provider.models.list()` caches and `provider.models.refresh()` reloads.
- Use curl_cffi for the shared HTTP request and streaming transports. Reuse provider connections and share incremental SSE decoding across ChatGPT, Claude, and Grok and JSPB decoding with Gemini under `webaiprovider.transport.frames`.
- Overlap general and project chat pagination within the existing concurrency limit. Reuse a fixed set of async workers for bounded reads and uploads, and skip constructing chat objects excluded by scope.
- Share overlapping reads of the same chat's response collection. Use `responses.list(fresh=True)` to request an independent read, and an older result cannot replace a newer applied history.
- Reduce history reconstruction work for single-part text and turns without sources or artifacts.
- Build only the requested recent responses when opening a chat with a limit, keeping the original prompt for responses that share a turn.
- Reduce allocation during chat listing by creating hook tables on registration and reusing one ordered index when merging project chats.
- Reduce repeated Gemini listing work by creating chat records only for the requested page and using larger follow-up pages for bounded lists. Repeated project chat IDs no longer consume the per-project limit.
- Reuse the account model catalog loaded at session opening for the first model query. Explicit catalog refresh still requests up-to-date options.
- Request larger Grok chat pages when listing many chats, preserving the continuation token and the caller's result limit.
- Reduce buffering overhead in the shared sync and async transports, and avoid creating an additional task for each chunk passed through the blocking transport facade.
- Speed up JSPB frame decoding by counting UTF-16 units in bounded blocks while validating surrogate boundaries.
- Run offline and live API tests separately. The live runner exercises the shared async provider implementation once per account. Blocking API coverage runs offline. The live runner paces account requests and reports cleanup and rate-limit skips separately from failures.

### Added

- Attach local files such as PDFs to Gemini notebooks through project attachments and remove them with the existing source deletion operation. Multi-file selections register each file in order.
- Emit one `collection.read.completed` batch per successful remote read and a `collection.changed` event after a confirmed remote write. Both carry scoped, normalized identities for consumers.
- Add an offline pyperf suite for history, pagination, bounded workers, HTTP buffering, the blocking transport bridge, and SSE/JSPB framing. Install it with the optional `benchmark` extra.
- Add `chats.get_with_responses(id, limit=...)` to both APIs. Opening an existing chat with its recent responses uses one history load. The response limit is applied locally.
- Add `chats.get(id, metadata_only=True)` to the blocking and async APIs. Grok uses its existing conversation metadata response without loading message history. Providers without a separate lookup retain the existing `get()` behavior.
- Expose provider lifecycle state, native FlowEvent event buses, and operation hooks. Creating a local chat draft emits `chat.prepared`. Binding the draft to a remote conversation emits `chat.created`.
- Expose normalized stream events for answer text, thinking updates, sources, and artifacts. Message operations support incremental consumption, cancellation, and waiting for the completed response.
- Expose the origin-scoped `provider.transport` for advanced HTTP requests and streams, with custom transport injection for supported configurations.
- Add an optional C extension for parts of ChatGPT's Sentinel runtime. The Python implementation remains available when the extension cannot be loaded, and the public provider API stays the same.
- Add tag-triggered PyPI Trusted Publishing. The wheel workflow builds for CPython 3.10–3.14 on Linux and macOS (x86-64 and ARM64) and Windows (x86-64), and checks that the native extension loads from the installed wheel. The source distribution includes the C sources for local builds.

### Removed

- Make provider coordination helpers internal. Use the provider event bus and operation hooks as the public interfaces.

- Remove unused JSON serialization and duplicate cleanup journal routing from the live test runner.

- Remove the background provider portal and threaded HTTP facade. Synchronous callers drive a shared event loop, allowing independent requests to overlap.

- Remove the unused `ThreadedTransport` adapter and legacy thread dispatch for synchronous hooks and events.
- Remove `ProviderTelemetry`. Use the provider's lifecycle events for outcomes and `Response.timing` for per-response latency.

### Fixed

- Isolate Gemini's file uploads from account cookies while keeping their connection pool available for subsequent uploads.
- Persist refreshed live-test browser cookies only after the provider accepts the new session.

- Report a Gemini sign-in redirect as an account authentication failure without following it or exposing redirect parameters.

- Use the asynchronous credential loader for every asynchronous HTTP session connection.
- Retain Gemini browser cookies used for session continuity and read escaped bootstrap fields.
- Match ChatGPT's project move contract in the live suite and skip a redundant session open when a browser cookie refresh returns identical credentials.

- Preserve the blocking transport response type for raw HTTP sends. Match the provider utility methods and ChatGPT authentication options across both API variants.

- Lock the current source and selected destination during chat moves, including when the caller holds an older chat snapshot.
- Invalidate the loaded model catalog after reauthentication.
- Drain asynchronous HTTP requests and streams before closing their clients. Finish an in-progress client shutdown when its caller is cancelled, and reject attempts to await shutdown from inside the active stream.
- Deliver synchronous events emitted during a blocking call on that call's thread.

- Let blocking HTTP requests and streams finish before closing their client. Keep a client in use by another request alive during timeout recovery.
- Release the synchronous call scheduler when it rejects an awaitable bound to another event loop.
- Initialize the blocking HTTP client once during concurrent first access and keep network requests outside the initialization lock.

- Wait for active raw HTTP requests and streams before closing a synchronous provider. Closing its shared transport uses the same shutdown path.
- Allow independent synchronous calls to return while another caller is delivering an event.

- Return synchronous chat and message resources from `Provider.ask()` across all four providers.

- Keep synchronous providers available for a cleanup retry after a failed close. Resolve races between the first open and close.
- Require project mutation confirmations to match the target project and requested change.
- Run live API scenarios on Python 3.10 with isolated sessions and reuse a shared session on Python 3.11 and later.

- Preserve the triggering user message across visible assistant segments in conversation history and keep tool or system records out of response turns. Prefer ChatGPT's active branch when a history payload contains both a branch map and a flat list.
- Report Gemini server-side generation rejections distinctly from transport errors. Use the model's default effort in live availability probes and report safe HTTP status codes for known session failures.

- Verify sent responses by remote history identity and edits by the revised prompt and its persisted answer in the live suite. Report partial coverage separately and avoid repeat model setup and account-wide searches during project moves.

- Cancel and drain the sibling listing when either top-level chat or project listing fails during `include_projects=True` queries.
- Serialize sends within a conversation and coordinate project mutations. Apply only changed project fields and confirm incomplete server replies through readback without replaying the mutation.
- Keep accepted provider work tracked during shutdown, close nested streams and upload workers during cancellation, and preserve the original stream error if cleanup also fails. Async providers retain the cleanup scope after a failed close so callers can retry.
- Preserve local drafts during history refresh, reconcile sent messages with their remote IDs, and exclude internal retry and edit records from refreshed history.
- Resolve exact model and effort IDs before display-name matches. Normalize provider stream errors and rate-limit responses, and retain per-account cooldowns when the provider supplies a retry delay.
- Reduce redundant network and file work by reusing continuation metadata, bounding parallel attachment uploads, and batching routine credential writes while persisting important session changes promptly.

### Removed

- Remove the legacy `webai_provider` namespace and its compatibility layer. Built-in providers implement the shared async provider contract directly. The former adapter API, `chat.messages`, and top-level `Page` export are no longer available.
- Drop the HTTPX runtime dependency and replace duplicate event and state machine wrappers with FlowEvent primitives.

### Security

- Disable ambient proxy configuration by default for internal HTTP clients, enforce fixed origins and redirect restrictions, and validate and pin public addresses for provider downloads. Bound response bodies and sanitize download and upload failures to keep signed URL parameters out of errors.
- Isolate signed uploads from account credentials, validate local files before use, and serialize credential file access across threads and processes.

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
