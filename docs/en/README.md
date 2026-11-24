# WebAI Provider Documentation (v2.0.0)

`webai-provider` is an independent Python client library for working with **ChatGPT**, **Claude**, **Gemini**, and **Grok** web sessions. It exposes both blocking synchronous and `asyncio`-native asynchronous interfaces over the same resource model.

> **Note:** Provider websites and private web protocols can change without notice. This library is an independent project and is not affiliated with OpenAI, Anthropic, Google, or xAI.

---

## Contents

- [Where Should You Start?](#where-should-you-start)
- [Documentation Map](#documentation-map)
- [Provider Comparison](#provider-comparison)
- [Synchronous or Asynchronous?](#synchronous-or-asynchronous)

---

## Where Should You Start?

- To make your first connection, start with [Getting Started and Accounts](getting-started.md). You will connect a browser session and send your first prompt.
- If you are wiring live streaming into an application, see [Chats, Responses, and Streaming](chats-and-streaming.md) to track incremental text updates, generation completion, and the final verified result.
- If you already know the concept and just need a signature or return type, jump straight to the [API Reference](api-reference.md). For platform-specific quirks, check the [Provider Guides](provider-guides.md).

[Back to top](#contents)

---

## Documentation Map

| Guide | What It Covers |
| --- | --- |
| **[01 · Getting Started and Accounts](getting-started.md)** | Installation, creating an `WebAIAccount` from a browser or cookies, `FileCredentialStore` / `MemoryCredentialStore`, `WebAIProvider` lifecycle, and `provider.ask()`. |
| **[02 · Chats, Responses, and Streaming](chats-and-streaming.md)** | Creating and searching chats, messages and attachments, `Response.parts` (`ThinkingPart`, `TextPart`), live `stream()`, `edit` / `retry` / `branch`, `wait_for()`, cancellation, and `chat.hooks`. |
| **[03 · Models and Projects](models-and-projects.md)** | Model catalog discovery (`300 s` TTL), `Models` tiers, reasoning effort selection, static catalogs, projects, project instructions, and project file/text attachments. |
| **[04 · Provider Guides](provider-guides.md)** | ChatGPT (`memory_share`, `move_to(None)`, `BrowserSession`), Claude (organization discovery, web search), Gemini (`OSID` cookies, `GeminiNotebooks`, upload isolation), and Grok (`assets.grok.com`, `RateLimited`). |
| **[05 · Events, Transport, and Errors](events-transport-and-errors.md)** | `EventBus` subscriptions, `provider.transport` (`HTTPTransport`, `HttpSession`, `TransportConfig`), SSE/JSPB frame decoders, error hierarchy, and `SendTiming`. |
| **[06 · Quick API Reference](api-reference.md)** | Everyday constructors, collection methods, stream events, transport types, and exceptions. |

[Back to top](#contents)

---

## Provider Comparison

At runtime, you can inspect supported capabilities on an open or closed provider via `provider.capabilities`, and check attachment rules via `provider.attachment_policy()`.

| Feature | ChatGPT | Claude | Gemini | Grok |
| --- | :---: | :---: | :---: | :---: |
| Chats, history, rename, delete, temporary chat | Yes | Yes | Yes | Yes |
| Model discovery (`Capability.MODELS`) | Yes | Yes | Yes | Yes |
| Effort selection (`Capability.EFFORT`) | Yes | Yes | Yes | No |
| Message `edit` and `retry` | Yes | Yes | Yes | Yes |
| Chat branching (`chat.branch()`) | Yes | No | Yes | No |
| Projects and project file/text resources | Yes | Yes | Yes *(Notebook)* | Yes *(Workspace)* |
| Moving chats into a project (`chat.move_to(project)`) | Yes | Yes | No | Yes |
| Removing chats from a project (`chat.move_to(None)`) | Yes | No | No | No |
| Prompt/chat file limits | 10 files · 512 MiB *(images 20 MiB, spreadsheets 50 MiB)* | 20 files per chat · 30 MiB *(allowed extensions only)* | 10 files · 100 MiB *(video 2 GiB)* | 10 files · 3 MiB *(10 MiB total, docs/code only)* |

[Back to top](#contents)

---

## Synchronous or Asynchronous?

Choose based on your caller context—use the async interface inside an event loop, and the blocking interface in synchronous code:

- **Synchronous (`WebAIProvider`, `ChatGPTWebAIProvider`, `ClaudeWebAIProvider`, `GeminiWebAIProvider`, `GrokWebAIProvider`):**
  Runs blocking `curl_cffi` calls directly on the calling thread without spawning a hidden `asyncio` event loop. Best suited for CLI tools, traditional web frameworks (Flask, Django), and plain Python scripts.
- **Asynchronous (`AsyncWebAIProvider`, `AsyncChatGPTWebAIProvider`, `AsyncClaudeWebAIProvider`, `AsyncGeminiWebAIProvider`, `AsyncGrokWebAIProvider`):**
  Built for `async` / `await`. Ideal for FastAPI, `aiohttp`, bots, or applications juggling multiple concurrent chat streams. Overlapping `chat.responses.list()` calls on the same conversation are automatically coalesced into a single network request.

[Back to top](#contents)

---

**Next:** [01 · Getting Started and Accounts →](getting-started.md)
