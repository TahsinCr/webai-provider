# Changelog

The format follows [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Include the PEP 561 `py.typed` marker in wheels and source distributions for the public typed API.
- Publish provider request and normalized stream event contracts in `webaiprovider.resources.message.protocol` for implementations using the same operation pipeline as built-in providers.

- Expose origin-scoped `provider.transport` in async and blocking APIs, custom transport injection, HTTP/request-stream strategies, and bounded `PortalBridge` orchestration.

- Add provider-specific async and sync provider facades, explicit `open()`/`close()` lifecycle, and paginated project chat access with chat moves.
- Add ChatGPT project memory selection as `memory_share: bool`; `True` uses the provider's default/global memory scope and `False` selects project-only memory.
- Model-specific typed effort namespaces; a static choice is available as `ChatGPTModels.GPT_5_6_THINKING.efforts.MEDIUM` and each value is an `EffortRef`.
- Account-scoped `provider.models` discovery with `list`, `get`, `find`, `search`, and explicit `refresh`; the valid catalog is cached for the session and retained if a refresh fails.
- Active Claude Opus and Fable catalog entries using first-party model IDs, alongside models available on other subscription tiers.
- `await provider.models.list()` preloads and caches the account's catalog for the current session; `refresh()` forces a fresh provider request.

### Changed

- Keep `webaiprovider.__version__` as the single package-version source and derive build metadata from it.

- Type rejected ChatGPT sessions as the common `AuthenticationError`; browser-cookie refresh and retry remain limited to the live-test harness.

- Group shared provider contracts in `base.py`, model catalog and typed selection APIs under `resources/model`, and chat, message, and project data by resource ownership.

- Remove duplicate Grok cookie persistence and ChatGPT header preparation; shared worker execution now supports keyword arguments through the same bounded submission path.

- Keep transport origins immutable and describe buffered/streamed responses with backend-independent public protocols; synchronous stream closure reaches its async owner exactly once.

- Make generic `Provider` and `AsyncProvider` constructors select the concrete provider class directly, preserving provider-specific behavior when switching `ProviderType`.

- Route provider HTTP streams through the shared transport contract; keep provider protocol clients in their own `client.py` modules and move generic work pools to `core`. Rate-limit cooldowns are isolated per transport session.

- Flatten reference normalization into `webaiprovider.normalizers` and use direct imports instead of routing normalization helpers through data models.

- Share bounded SSE framing across ChatGPT, Claude, and Grok; expose a provider-independent `JSPBFrameDecoder` with Gemini framing settings supplied by Gemini.

- Repair provider imports and sync/async resource exports after removing the old provider adapter layer.

- Rename immutable normalized model types to `MessageData`, `ProjectData`, and `ResourceData` to distinguish them from SDK resource objects.
- Define `ProviderType` in the canonical `webaiprovider.provider` module and remove the separate `provider_type.py` module.
- Give `Attachments` the collection-style `create`, metadata-only `update`, and local `delete` API.
- Group chat, message, and project implementations under `webaiprovider.resources`; keep shared collection behavior in `webaiprovider.collections`.
- Use plain names for the synchronous API and the `Async` prefix for asynchronous providers, resources, and collections; remove the former resource and collection module paths.
- Keep `Attachments` with message resources and use shared query behavior; keep `Attachment` as a resource value.
- Split the public API into shared `BaseProvider`, async `AsyncProvider`, blocking `Provider`, and provider-specific sync/async classes. Provider-only chat operations are exposed by the corresponding provider chat types.
- Move shared model selection, caching, and resolution into `webaiprovider.resources.model`; keep each provider's static model and effort definitions in `providers/<provider>/catalogs.py`.
- Make `webaiprovider` the sole import package for the v2 API and remove the `webai_provider` re-export namespace.
- `chat.messages.create`, edit, retry, and provider `ask` accept one `model=` selection; separate public `effort=` selection was removed. The selected model and effort are checked against that account's live catalog before send, with no silent downgrade.
- Keep ChatGPT web IDs separate from API IDs. ChatGPT's Pro thinking option is powered by Astra; discover its account-eligible web model and effort references instead of modeling it as API ID `gpt-6-astra` with API-only effort levels.
- Remove work-only Luna and Sol entries from the ChatGPT web catalog; retain dynamic account discovery as the authority for available web models.
- Represent effort options as each model's typed `Efforts` class across static provider catalogs. Account-discovered efforts use the same class-based interface, and an `EffortRef` can be passed through `model=` when the active account exposes it.
- Use a single resolver for model policies, model references, and model-bound effort references; `AUTO` uses the provider-reported default.
- Route an explicit `browser=` choice from `Account.from_browser()` through profile discovery and cookie import; an omitted choice still uses the operating system's default browser.
- Make each concrete async provider inherit directly from `AsyncProvider`; it owns the shared lifecycle, streaming, and resource flow while provider classes implement their own service operations.
- Run only one producer task for each message operation so `stream()`, `wait()`, `start()`, and `wait_for()` cannot submit duplicate requests.
- Require an attached provider session before sending a message; detached chats now raise a `ProviderError` instead of producing a local echo as if it were a provider response.

### Removed

- Remove the standalone package smoke script; run isolated wheel and source-distribution checks in CI and keep behavior coverage in `unittest`.

- Remove the duplicate `Feature`/`ProviderDescription` wrapper and `describe()` API; providers expose their declared capabilities directly through `provider.capabilities`.
- Move catalog status to `resources.model.ModelCatalogState` and remove unused connector and feature-only status values.
- Remove the no-op synchronous `Provider.__repr__` override and inherit the shared representation.

- Remove the former root `models.py`, `contracts.py`, `streams.py`, and `catalogs.py` modules without compatibility imports.

- Remove the former top-level `session.py`, `sync.py`, `facade.py`, and `catalogs/` package layout; no compatibility imports are provided for the new API.
- Remove the `WebSession` and `ChatGPTWebError` aliases; use `HttpSession` and `ChatGPTError` directly.
- Remove generic message branching and project/chat collection move helpers; use `Chat.branch()` or `Chat.move_to()` on provider-specific chat types that support them.
- Remove the `CapabilityError` alias and capability-enum compatibility branch; compatibility errors now accept a message, and bounded caches use explicit cache operations instead of dictionary emulation.
- Remove the legacy adapter request/event interface and bridge fallback; message sends now flow through the v2 `execute_stream` contract, with provider wire events confined to transport internals.
- Remove `ProviderBridge`, `ProviderAdapter`, its factory, and `providers/base.py`; provider-specific async classes now implement the shared provider API directly.

### Fixed

- Translate provider, profile, catalog, timeout, and persistence exception messages to English.

### Security

- Restrict downloads to provider-supplied `DownloadSource` values, reject non-public resolved addresses, pin the selected address for the request, and disable redirects.

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
