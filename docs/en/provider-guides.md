[← Previous: 03 · Models and Projects](models-and-projects.md) · [Documentation Map](README.md) · [Next: 05 · Events, Transport, and Errors →](events-transport-and-errors.md)

# 04 · Provider-Specific Guides

`WebAIProvider` and `AsyncWebAIProvider` unify all four platforms under a single interface, but each site's web protocol introduces specific differences in project memory, chat moving, cookie rules, attachment limits, and typed errors.

---

## Contents

- [Summary Comparison](#summary-comparison)
- [ChatGPT (`ChatGPTWebAIProvider` / `AsyncChatGPTWebAIProvider`)](#chatgpt-chatgptwebaiprovider--asyncchatgptwebaiprovider)
- [Claude (`ClaudeWebAIProvider` / `AsyncClaudeWebAIProvider`)](#claude-claudewebaiprovider--asyncclaudewebaiprovider)
- [Gemini (`GeminiWebAIProvider` / `AsyncGeminiWebAIProvider`)](#gemini-geminiwebaiprovider--asyncgeminiwebaiprovider)
- [Grok (`GrokWebAIProvider` / `AsyncGrokWebAIProvider`)](#grok-grokwebaiprovider--asyncgrokwebaiprovider)

---

## Summary Comparison

| Provider | Origin | Special Capabilities | Key Caveats |
| --- | --- | --- | --- |
| **ChatGPT** | `https://chatgpt.com` | `branch()`, `move_to(project \| None)`, `memory_share=False` | Uses bundled C Sentinel extension or Python fallback; `BrowserSession` supports CDP capture. |
| **Claude** | `https://claude.ai` | `move_to(project)` *(no unassign)* | No `branch()`; max 20 files per chat, max 30 MiB, strict extension allowlist. |
| **Gemini** | `https://gemini.google.com` | `branch()`, `GeminiNotebooks` | No `move_to()`; requires `OSID` + `__Secure-OSID` + `__Secure-1PSID`; never follows login redirects (`GeminiSignInRequired`). |
| **Grok** | `https://grok.com` | `move_to(project)` *(no unassign)*, `assets.grok.com` | No `branch()`; strict chat limits (3 MiB per file, 10 MiB total); raises `RateLimited` on quota exhaustion. |

If your application connects to multiple providers, `WebAIProvider(account)` is all you need. Import a concrete class directly when you want IDE type checking for platform-specific options such as ChatGPT's `memory_share=False`.

[Back to top](#contents)

---

## ChatGPT (`ChatGPTWebAIProvider` / `AsyncChatGPTWebAIProvider`)

**Additional constructor parameters:**
In addition to `(account, *, type=None, transport=None)`:
- `session` (`BrowserSession | None = None`): Pass `webaiprovider.providers.chatgpt.auth.BrowserSession(session_file, cdp_url="http://127.0.0.1:9222")` to capture headers/sessions over Chromium DevTools Protocol (CDP). Defaults to the account's own credential store.
- `client_factory` (`Callable[..., Any] | None = None`): Custom client hook for testing or instrumentation.

**Highlights and rules:**
- **Project Memory Isolation (`memory_share`):** Pass `projects.create(..., memory_share=False)` to create a project workspace isolated from account-wide memory. The default `True` shares general memory.
- **Moving and Unassigning Chats (`move_to`):** ChatGPT supports adding a chat to a project, reassigning it to another project, and calling `chat.move_to(None)` to return it to the main chat list.
- **Branching and Attachments:** `chat.branch(message_id, *, title=None)` is supported. `project.attachments.create_many()` runs at most 2 concurrent uploads and rolls back partial uploads if any item fails.
- **File Limits:** Up to 10 files per prompt; 512 MiB general limit, 20 MiB images, 50 MiB spreadsheets.
- **Typed Exceptions:** `ChatGPTError`, `ChatGPTAuthenticationError`, `SentinelCompatibilityError`.

```python
with ChatGPTWebAIProvider(account) as provider:
    proj_a = provider.projects.create("Client A", memory_share=False)
    proj_b = provider.projects.create("Client B", memory_share=False)

    chat = provider.chats.create("Proposal")
    chat.ask("Draft an outline.")

    chat.move_to(proj_a)  # Assign to Project A
    chat.move_to(proj_b)  # Reassign to Project B
    chat.move_to(None)  # Return to top-level chats
```

[Back to top](#contents)

---

## Claude (`ClaudeWebAIProvider` / `AsyncClaudeWebAIProvider`)

**Constructor parameters:** Standard `(account, *, type=None, transport=None)`.

**Highlights and rules:**
- **Organization Discovery:** Automatically picks an organization with `"chat"` capability during `/api/bootstrap` (honoring the `lastActiveOrg` cookie) and caches `(organization, catalog)` in the verified session snapshot.
- **Draft Title Persistence:** Titles passed to `chats.create("Custom Title")` are sent on the first turn so the remote conversation keeps the title even if the first stream is interrupted.
- **Web Search Sources:** Resolves both legacy (`web_search_tool_result`, `citations_delta`) and current (`tool_result` with `name="web_search"`, `citation_start_delta`) citation formats into `response.sources`.
- **Moving Chats:** `chat.move_to(project)` moves an unassigned chat into a project; reassigning between projects or `move_to(None)` is not supported. `chat.branch()` is unavailable.
- **File Limits:** Up to 20 files per chat, max 30 MiB each. Only supported extensions are accepted: `.pdf`, `.docx`, `.csv`, `.txt`, `.html`, `.htm`, `.odt`, `.rtf`, `.epub`, `.json`, `.xlsx`, `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`.
- **Typed Exceptions:** `ClaudeError`, `ClaudeCompatibilityError`, `ClaudeRequestNotSent`, `ClaudeStreamError` (`webaiprovider.providers.claude.errors`).

[Back to top](#contents)

---

## Gemini (`GeminiWebAIProvider` / `AsyncGeminiWebAIProvider`)

**Additional constructor parameters:**
In addition to `(account, *, type=None, transport=None)`:
- `client_factory` (`Any = None`): Custom client hook for testing or instrumentation.
- `notebooks`: Blocking `GeminiWebAIProvider` takes `webaiprovider.providers.gemini.GeminiNotebooks`, while `AsyncGeminiWebAIProvider` takes `webaiprovider.providers.gemini.async_client.AsyncGeminiNotebooks`. Automatically initialized from the account credentials by default.

**Highlights and rules:**
- **Required Cookies and Rotation:** Along with `__Secure-1PSID`, Gemini requires `OSID` and `__Secure-OSID` for `gemini.google.com`. If expired cookies trigger a Google sign-in redirect, the client refuses redirects and raises `GeminiSignInRequired` (`AccountPaused` subclass). Cookies refreshed via `RotateCookies` during an open session are persisted back to `CredentialStore`.
- **Notebook-Backed Projects:** `provider.projects` maps to Gemini Notebooks, supporting text and file resources. `chat.move_to(...)` is not supported on Gemini (`GeminiChat` omits it).
- **Branching:** `chat.branch(message_id, *, title=None)` and `response.branch()` are supported.
- **Isolated File Uploads:** The upload pool sends no account cookies and discards response cookies. Limits per prompt: up to 10 files, 100 MiB general limit, 2 GiB video.
- **Typed Exceptions:** `GeminiError`, `GeminiSignInRequired`, `GeminiGenerationRejected` *(carries the server numeric rejection `code=...`)*, `GeminiNotFoundError`, `GeminiCompatibilityError`, `GeminiStreamError` (`webaiprovider.providers.gemini.errors`).

[Back to top](#contents)

---

## Grok (`GrokWebAIProvider` / `AsyncGrokWebAIProvider`)

**Additional constructor parameters:** Standard `(account, *, type=None, transport=None)` + `client_factory: Any = None`.

**Highlights and rules:**
- **Workspaces and Moving Chats:** `provider.projects` manages Grok Workspaces; `chat.move_to(project)` is supported, while `chat.branch()` is unavailable.
- **Images and Artifacts:** Links on `assets.grok.com`, `generatedImageUrls`, attachment metadata, and inline Markdown asset links are extracted into `response.artifacts` and downloadable via `chat.download(artifact)`.
- **File Limits:** Up to 10 chat attachments, **3 MiB max per file** and **10 MiB total** (Workspace attachments allow up to 20 MiB). Only document and data extensions are allowed: `.pdf`, `.txt`, `.md`, `.csv`, `.json`, `.doc`, `.docx`, `.ppt`, `.pptx`, `.xls`, `.xlsx`, `.xlsm`, `.odt`, `.ods`, `.odp`, `.rtf`.
- **Models and Effort:** Model discovery via `GrokModels` is supported, but reasoning effort selection (`Capability.EFFORT`) is not offered (`efforts` is empty).
- **Quotas and Errors:** Grok defines no provider-specific exception subclasses; it raises shared `WebAIProviderError`, `RateLimited` (`err.retry_after` and `err.message_submitted`), and `AccountPaused` exceptions.

[Back to top](#contents)

---

[← Previous: 03 · Models and Projects](models-and-projects.md) · [Documentation Map](README.md) · **Next:** [05 · Events, Transport, and Errors →](events-transport-and-errors.md)
