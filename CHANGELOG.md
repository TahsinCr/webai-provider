# Changelog

The format follows [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
