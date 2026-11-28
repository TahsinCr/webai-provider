# Changelog

The format follows [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Extract inline Gemini Canvas (`immersive_entry_chip` / `cand[30]`) and code-execution generated file (`[file-tag: code-generated-file-...]` / `cand[12]["59"]`) artifacts across live `StreamGenerate` and `hNvQHb` history, stripping chip and file-tag placeholders from visible text.

### Fixed

- Skip `image_asset_pointer` media dicts in ChatGPT `text_from_parts()` so raw asset JSON is not emitted into `TextPart` text.
- Allow `contribution.usercontent.google.com` in Gemini `valid_media_url()`, attach `(__Secure-1PSID, __Secure-1PSIDTS)` cookies (omitting `OSID`) for generated file downloads, and deduplicate Gemini history `Attachment` and `metadata["files"]` artifact IDs.

## [2.1.0] - 2026-11-28

### Added

- Add explicit per-request tool contracts (`Tool`, `WebSearchTool`, `ImageGenerationTool`, `DeepResearchTool`) and `tool=` / `tool_timeout=` parameters across `ask()`, `stream()`, and `responses.create()` with pre-request capability validation (`Capability.WEB_SEARCH`, `Capability.IMAGE_GENERATION`, `Capability.DEEP_RESEARCH`), raising `FeatureUnavailable` or `UnsupportedOperationError` on invalid combinations.
- Add Gemini-specific `GeminiDeepResearchTool` and `DeepResearchSources` (`WEB`, `CHAT`, `WEB_AND_CHAT`), enforcing rejection on generic `WebAIProvider` / `AsyncWebAIProvider` facades while defaulting generic `DeepResearchTool` on Gemini to `WEB_AND_CHAT`.
- Add `Response.research` (`ResearchState`), `ResearchPlan`, `ResearchStatus`, `OperationEvent.RESEARCH_PLAN_READY` (`ResearchPlanReady`), `OperationEvent.RESEARCH_PROGRESS` (`ResearchProgressEvent`), and `response.start_research()`, `pause_research()`, `cancel_research()`, and `wait_research()`, backed by adaptive sync/async polling (`poll_until` / `async_poll_until`).
- Add `Source.cited`, inline `Artifact` helpers (`read_bytes()`, `read_text()`, `save()`, `files_from`), generated image artifacts (`image/png` on ChatGPT via `image_asset_pointer`, `image/jpeg` on Gemini, `generatedImageUrls` on Grok), and Deep Research Markdown report artifacts (`text/markdown`), allowing image-only responses with empty `parts`.
- Add `WebSearchTool`, `ImageGenerationTool`, and `DeepResearchTool` support on ChatGPT (with MCP polling) and Gemini, `WebSearchTool` and `ImageGenerationTool` support on Grok, and `Capability.WEB_SEARCH` support on Claude (treating `WebSearchTool()` as a no-op since `web_search_v0` is enabled by default).
- Add bilingual `07 · Tools and Deep Research` guides (`docs/en/tools-and-deep-research.md`, `docs/tr/tools-and-deep-research.md`), update `docs/{en,tr}/` cross-references and `README*.md`, and expand offline tool/typing/AST tests alongside `tests/live/test_tools.py`.

### Changed

- Deduplicate shared research response builders (`with_research_status`, `build_research_response`) across providers (`webaiprovider/resources/response/research.py`).
- Restore `BardFrontendService/StreamGenerate` for Gemini generation without BotGuard and remove the experimental `processSession` WebChannel modules (`proto.py`, `webchannel.py`).

### Fixed

- Add `Sec-Fetch-*` headers for Gemini requests and allow empty `hNvQHb` RPC responses (`decode_rpc_response(..., allow_empty=True)`).
- Parse Gemini Deep Research plan metadata and steps from `cand[12][0]["56"]`.
- Handle ChatGPT SSE `role == "tool"` frames for `multimodal_text` (`image_asset_pointer`) image generation responses and `chatgpt_sdk` (`venus_plan`) Deep Research frames, and run ChatGPT live image generation tests in non-temporary chats.

### Security

- Support SSRF-safe 302 redirect resolution for Google media downloads (`googleusercontent.com` / `lh3.google.com`) in `webaiprovider/transport/downloader.py`, applying public-network validation and DNS pinning to each redirect hop.

## [2.0.0] - 2026-11-24

### Added

- Add bilingual documentation (`docs/en/` and `docs/tr/`) covering installation, streaming, models, projects, events, transport, error handling, and a reference for common API calls.
- Expose ordered thinking and visible response blocks through `response.parts`, materializing `response.parts` at `GenerationEnd` across all providers.
- Expose normalized stream events for answer text, thinking updates, sources, artifacts, generation end, remote verification, resolved content, and terminal outcomes, supporting incremental `operation.stream()`, cancellation, `operation.wait()`, and `wait_for` resumption after a specific event.
- Add explicit sync and async `chat.prepare()` to warm provider continuation state under the chat lock.
- Export `ClaudeChat`, `GeminiChat`, and `GrokChat` from the package root, and expose the blocking Gemini notebook reader as `GeminiNotebooks` from the Gemini package.
- Attach local files such as PDFs to Gemini notebooks through project attachments in selection order and remove them with the existing source deletion operation.
- Expose provider lifecycle state, native FlowEvent event buses, and operation hooks, emitting `chat.prepared`, `chat.created`, `collection.read.completed`, and `collection.changed` with scoped, normalized identities.
- Add `chats.get_with_responses(id, limit=...)` and `chats.get(id, metadata_only=True)` to both APIs, allowing Grok to read metadata without loading message history and letting callers open recent responses in one history load.
- Expose origin-scoped `provider.transport` for advanced HTTP requests and streams, with custom transport injection for supported configurations.
- Add an optional C extension for parts of ChatGPT's Sentinel runtime, keeping the pure-Python implementation available as fallback.
- Add an offline `pyperf` benchmark suite (via the `benchmark` optional extra) for history, pagination, bounded workers, HTTP buffering, blocking transport, and SSE/JSPB framing.
- Add tag-triggered PyPI Trusted Publishing and wheel builds for CPython 3.10–3.14 on Linux and macOS (x86-64 and ARM64) and Windows (x86-64), including sdist C sources and post-install extension smoke checks.

### Changed

- Rename the Python import package from `webai_provider` to `webaiprovider` (PyPI distribution remains `webai-provider`), ship inline `py.typed` metadata, and use `webaiprovider.__version__` (`2.0.0`) as the package version source.
- Rename core public classes to explicit `WebAI*` names (`WebAIAccount`, `BaseWebAIProvider`, `WebAIProvider`, `AsyncWebAIProvider`, `WebAIProviderType`, `WebAIProviderError`, and provider-specific `*WebAIProvider` facades), defining `BaseWebAIProvider` as an abstract class with typed chat, project, and model collections.
- Replace the v1.3 `ProviderClient` API with account-scoped `WebAIProvider(account)` and `AsyncWebAIProvider(account)`, supporting `open()`/`close()`, `with`/`async with`, and native calling-thread execution across all four synchronous providers without starting a background event loop.
- Replace `chat.messages` with `chat.responses` (`create()`, `list()`, `chat.ask()`, `chat.stream()`), `response.message.edit()`, and `response.retry()`, grouping provider output blocks from a turn into one `Response` (`response.text` and `response.parts`).
- Manage project files and text sources with `project.attachments.create()`, `list()`, and `delete()`, manage message attachments with a local `Attachments` collection (`create()`, metadata-only `update()`, `delete()`), move chats with `chat.move_to(project | None)`, and accept `memory_share=False` on ChatGPT project creation.
- Preserve concrete provider and resource types across blocking and async collections, queries, and context managers, and move response operation and stream event classes to `webaiprovider.resources.response.operation` while keeping top-level exports.
- Update `ChatGPTModels` static catalog for GPT-6.2 (`GPT_6_2_AUTO`, `GPT_6_2_INSTANT`, `GPT_6_2_THINKING`, `GPT_5_6_THINKING_MINI`) and remove unlisted `GPT_6_AUTO` and `GPT_6_MINI` entries.
- Use a single `model=` argument for model and effort selection (`ModelRef`, `model_ref.efforts`, string queries, and selection policies), resolving synchronous lookups directly from the loaded catalog on the calling thread.
- Unify session model catalog caching (`ModelsCollection` and `AsyncModelsCollection`) with `MODEL_CATALOG_TTL`, reuse the catalog loaded at session opening for initial queries, and reuse verified per-account session/bootstrap snapshots across `open()` calls until credentials change, expire, pause, or are forgotten.
- Keep browser authentication in ChatGPT's auth module (`BrowserSession` constructed directly with a credential store or path), and define browser-cookie selection, validation, and persistence rules once per provider including known alternate session cookies.
- Standardize default English conversation titles (`New Chat`) and shared action labels across providers.
- Migrate to `flowevent` 2.0, `flowpipeline`, and `flowcoord`, and replace external state-machine and versioned-cache dependencies with minimal self-contained `StateMachine` and `BoundedCache` implementations in `webaiprovider.core`.
- Use `curl_cffi` (`>=0.17`) for `HttpSession`, `AsyncHttpSession`, and local browser debugging WebSockets, running default blocking HTTP requests, bounded incremental streams, and session status checks natively on the calling thread.
- Share incremental SSE and JSPB frame decoding under `webaiprovider.transport.frames`, speed up JSPB UTF-16 length counting in bounded surrogate-validated blocks, and report native DNS, connect, TLS, first-byte, and connection reuse timings when send diagnostics are enabled.
- Select stream milestones with `OperationEvent` in `wait_for()`, register persistent send/stream hooks on `chat.hooks` with shared `OperationHooks` / `AsyncOperationHooks` registration, and combine overlapping response reads via FlowCoord (`responses.list(fresh=True)` requests an independent read).
- Use stable snapshots for local collections, keep chat locks through terminal publication, reject same-thread mutations of paused sends, lock only the chat during moves, and publish confirmed project attachment changes under the project lock.
- Standardize bounded continuation caches across providers (keeping ChatGPT active node, model, and project and using lightweight Claude reads when preparing chats), read persisted attachment download ownership on demand, and run local file version checks and preflight off the event loop.
- Overlap general and project chat pagination within bounded concurrency, use smaller initial or project chat pages where appropriate, skip excluded records, reuse fixed async read/upload workers, and optimize unbounded Gemini and large Grok page queries.
- Optimize history decoding and memory usage across providers: skip redundant citation/single-part work, select the latest Grok branch in one pass, build only requested recent responses in bounded opens, reuse complete ChatGPT project creation responses, reduce Claude/Grok project edit readbacks, keep project options responsive during generation, normalize only selected turns for Gemini/Claude retry and edit, extract Gemini long-history media/sources faster, and preserve explicit Claude draft titles on interrupted first sends.
- Cancel unfinished ChatGPT project uploads on batch failure and verify attachments by remote file IDs; count Gemini send preparation against the synchronous stream deadline.
- Separate offline (`run_tests.py`) and live (`run_live_tests.py`) test suites, supporting `async` (default), `sync`, and `all` API modes, account request pacing, and distinct cleanup/rate-limit skip reporting.

### Removed

- Remove the legacy `webai_provider` namespace, its compatibility layer, the former `ProviderClient` adapter API, `chat.messages`, and the top-level `Page` export.
- Drop the `httpx` and `websocket-client` runtime dependencies and use `flowevent` for event delivery.
- Remove the shared sync/async event-loop bridge, compatibility wrappers, unused `ThreadedTransport` adapter, and legacy thread dispatch for synchronous hooks and events.
- Remove `ProviderTelemetry` (replaced by provider lifecycle events and `Response.timing`) and make internal coordination helpers private behind the event bus and operation hooks.
- Remove unused JSON serialization and duplicate cleanup journal routing from the live test runner.

### Fixed

- Guard Sentinel proof-of-work buffer sizing and final nonce boundaries, avoid null-pointer arithmetic for empty prefixes, initialize Windows performance-counter frequency safely across concurrent calls, and reuse stack storage for nonce suffixes.
- Drain active sync and async HTTP requests and streams before closing their clients, keep clients in use by another request alive during timeout recovery, finish in-progress async shutdowns when cancelled, reject awaiting shutdown from inside an active stream, and retain cleanup scopes after a failed close so callers can retry.
- Initialize the blocking HTTP client once during concurrent first access outside the initialization lock, release the synchronous call scheduler when rejecting foreign-loop awaitables, and wait for active raw HTTP requests and streams when closing a synchronous provider or its shared transport.
- Honor blocking stage deadlines during native curl polling and conversation lock acquisition, reject NaN timeouts, return immediately from `wait_for` when the requested event is already available, and publish one cancellation outcome when a send or driving iterator is cancelled.
- Publish provider opening events before session preparation, reject transport requests after failed cleanup until closed, deliver synchronous events on the calling thread without blocking independent callers, and return concrete chat and message resources from `WebAIProvider.ask()`.
- Keep a stable snapshot for warm synchronous model reads during catalog invalidation, invalidate the loaded model catalog after reauthentication, and resolve exact model and effort IDs before display-name matches.
- Serialize per-conversation sends, lock current source and selected destination snapshots during chat moves, coordinate project mutations with targeted readbacks without replaying incomplete updates, and cancel/drain sibling listings when a top-level or project listing fails under `include_projects=True`.
- Preserve triggering user messages, local drafts, and reconciled remote IDs in history, prefer ChatGPT's active branch when both branch maps and flat lists exist, exclude internal retry, edit, tool, and system records from response turns, and delete local turns by stable IDs when passed a copied message or response.
- Preserve Claude web search sources across live output and saved history, and resolve Claude retry and edit targets by active native message identity when stream and history block indexes differ.
- Report Gemini sign-in redirects as authentication failures without following them or exposing redirect parameters, distinguish server-side generation rejections from transport errors, isolate Gemini file uploads from account cookies, retain continuity cookies and escaped bootstrap fields, and refresh blocking Gemini credentials when due.
- Ignore malformed Grok attachment metadata, infer `.webp` and generated image formats from filenames (including on Windows Python 3.10), and deduplicate file references as the stream arrives.
- Normalize provider stream errors and rate-limit responses with per-account `Retry-After` cooldowns, bound parallel attachment uploads, and batch routine credential writes while persisting critical session changes promptly.
- Run live API tests with isolated sessions on Python 3.10 (reusing shared sessions on 3.11+), persist refreshed live-test cookies only after session acceptance, verify sent/edited responses by remote identity, and run `test_core` / `test_runners` cleanly in isolated wheel/sdist test directories.

### Security

- Disable ambient proxy configuration by default for internal HTTP clients, enforce fixed origins and redirect restrictions, validate and pin public addresses for provider downloads, bound response bodies, and sanitize download and upload errors to keep signed URL parameters out of exceptions.
- Isolate signed uploads from account credentials, validate local files before use, and serialize credential file access across threads and processes.

## [1.3-beta] - 2026-11-06

### Added

- Canonical facade `webai_provider.api` (re-exported by `webai_provider/__init__.py`) providing clean public exports for all providers, models, protocols, accounts, transformers, and utilities.
- Dedicated quarantine module `webai_provider.compat` isolating deprecated backwards-compatibility shims for future v2.0 removal.
- Consolidated `webai_provider.accounts` package unifying profile discovery, cookie filtering, SQLite WAL snapshotting with `webai-profile-` temp prefixes, and session importing.
- Hardened `NativeDownloadClient` with SSRF and link-local/cloud metadata IP protection (`169.254.169.254`).
- Sensitive URL query parameter and userinfo credential redaction with `redact_sensitive_url`.
- Implement Stage 3 identity, cookie, and networking layer across `webai_provider.accounts`, `webai_provider.auth`, and `webai_provider.transport`:
  - `accounts`: typed `WebAIAccount` entity, `CredentialStore` protocol, `FileCredentialStore` with atomic replace, fsync, and 0o600 file permissions, and thread-safe `MemoryCredentialStore`.
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
- Prevent premature cache invalidation in `GeminiWebAIProvider._take_chat_metadata()` and add `prepare_next_turn()` for LRU ordering.
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
