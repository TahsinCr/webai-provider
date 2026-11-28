[← Previous: 06 · Quick API Reference](api-reference.md) · [Documentation Map](README.md)

# 07 · Tools and Deep Research

> **Added in v2.1.0:** Explicit tool contracts, Deep Research lifecycle APIs, and `Artifact` inline helpers were introduced in `v2.1.0`.

Pass explicit tool contracts (`WebSearchTool`, `ImageGenerationTool`, `DeepResearchTool`, or `GeminiDeepResearchTool`) via `tool=` and `tool_timeout=` on `chat.ask()`, `chat.stream()`, and `chat.responses.create()`. Each provider validates the requested tool against `provider.capabilities` before sending a network request.

---

## Contents

- [Supported Tools by Provider](#supported-tools-by-provider)
- [Web Search (`WebSearchTool`)](#web-search-websearchtool)
- [Image Generation (`ImageGenerationTool`)](#image-generation-imagegenerationtool)
- [Deep Research (`DeepResearchTool` and `GeminiDeepResearchTool`)](#deep-research-deeptresearchtool-and-geminideepresearchtool)
- [Research Lifecycle and Polling](#research-lifecycle-and-polling)
- [Streaming Research Events](#streaming-research-events)

---

## Supported Tools by Provider

| Tool | Capability | ChatGPT | Gemini | Grok | Claude |
| --- | --- | :---: | :---: | :---: | :---: |
| `WebSearchTool()` | `Capability.WEB_SEARCH` | Yes | Yes | Yes | Yes |
| `ImageGenerationTool()` | `Capability.IMAGE_GENERATION` | Yes | Yes | Yes | No |
| `DeepResearchTool(auto_start=False)` | `Capability.DEEP_RESEARCH` | Yes | Yes | No | No |
| `GeminiDeepResearchTool(auto_start=False, sources=...)` | `Capability.DEEP_RESEARCH` | No | Yes | No | No |

- Passing an unsupported tool raises `FeatureUnavailable` before any request is sent.
- Passing a provider-specific tool such as `GeminiDeepResearchTool` to another provider or through the generic `WebAIProvider` / `AsyncWebAIProvider` facade raises `UnsupportedOperationError`.
- `tool_timeout` accepts a finite positive `int` or `float` (seconds) or `None`.
- On **Claude**, web search (`web_search_v0`) is already enabled by default in completion requests, so passing `WebSearchTool()` succeeds without altering the request payload.

[Back to top](#contents)

---

## Web Search (`WebSearchTool`)

Use `WebSearchTool()` when you want the provider to ground its answer in live web results and populate `response.sources`:

```python
from webaiprovider import WebAIAccount, AsyncChatGPTWebAIProvider, WebAIProviderType, WebSearchTool

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)

async with AsyncChatGPTWebAIProvider(account) as provider:
    chat = await provider.chats.create("Web Search", temporary=True)
    response = await chat.ask(
        "What is the official Python 3.13.0 release date?",
        tool=WebSearchTool(),
    )

    print(response.text)
    for source in response.sources:
        print(source.title, source.url, f"cited={source.cited}")
```

`Source.cited` is `True`, `False`, or `None` depending on whether the provider distinguishes directly cited sources from general search results.

[Back to top](#contents)

---

## Image Generation (`ImageGenerationTool`)

Use `ImageGenerationTool()` to generate images and read or save the resulting `Artifact` objects from `response.artifacts`:

```python
from webaiprovider import WebAIAccount, GeminiWebAIProvider, ImageGenerationTool, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.GEMINI)

with GeminiWebAIProvider(account) as provider:
    chat = provider.chats.create("Images", temporary=True)
    response = chat.ask(
        "Generate a minimal flat red circle on a plain white background.",
        tool=ImageGenerationTool(),
    )

    for artifact in response.artifacts:
        if artifact.mime_type.startswith("image/"):
            data = chat.download(artifact)
            # Or, when the provider embeds inline bytes on the artifact:
            # data = artifact.read_bytes()
            # artifact.save("circle.png")
            print(artifact.name, artifact.mime_type, len(data))
```

- Image-only responses can return empty `response.parts` while still populating `response.artifacts`.
- On **ChatGPT**, AI image generation is not supported inside `temporary=True` chats; use a regular chat and delete it afterward if disposable.
- Inline artifacts expose `artifact.read_bytes()`, `artifact.read_text(encoding="utf-8")`, and `artifact.save(path)`. Remote download URLs use `chat.download(artifact)`.
- On **Gemini**, generated images, inline Canvas documents (`immersive_entry_chip`), and code-execution generated files (`contribution.usercontent.google.com`) are also exposed in `response.artifacts` and downloadable via `chat.download(artifact)` or `artifact.read_text()` *(v2.1.1)*.

[Back to top](#contents)

---

## Deep Research (`DeepResearchTool` and `GeminiDeepResearchTool`)

`DeepResearchTool(auto_start=False)` starts a multi-step research workflow on ChatGPT and Gemini:

- `auto_start=False` (default): stops after generating the research plan (`ResearchStatus.WAITING_FOR_APPROVAL`) so you can inspect `response.research.plan` before calling `response.start_research()`.
- `auto_start=True`: automatically approves/starts the research run when supported.

On Gemini, you can use `GeminiDeepResearchTool` from `webaiprovider.providers.gemini` to select which sources Deep Research searches:

```python
from webaiprovider import WebAIAccount, AsyncGeminiWebAIProvider, WebAIProviderType
from webaiprovider.providers.gemini import DeepResearchSources, GeminiDeepResearchTool

account = WebAIAccount.from_browser(WebAIProviderType.GEMINI)

async with AsyncGeminiWebAIProvider(account) as provider:
    chat = await provider.chats.create("Deep Research")
    response = await chat.ask(
        "Compare Python 3.13 free-threading trade-offs.",
        tool=GeminiDeepResearchTool(
            auto_start=False,
            sources=DeepResearchSources.WEB_AND_CHAT,
        ),
        tool_timeout=180.0,
    )
```

Available `DeepResearchSources` values:

- `DeepResearchSources.WEB` (`"web"` — default for `GeminiDeepResearchTool`)
- `DeepResearchSources.CHAT` (`"chat"`)
- `DeepResearchSources.WEB_AND_CHAT` (`"web_and_chat"` — default when passing generic `DeepResearchTool` to Gemini)

[Back to top](#contents)

---

## Research Lifecycle and Polling

When a response comes from `DeepResearchTool`, `response.research` holds a `ResearchState` snapshot:

- `state.status: ResearchStatus` — `PLANNING`, `WAITING_FOR_APPROVAL`, `RUNNING`, `PAUSED`, `COMPLETED`, `CANCELLED`, or `FAILED`
- `state.plan: ResearchPlan | None` — `plan_id`, `title`, `steps: tuple[str, ...]`, `countdown_seconds`
- `state.session_id`, `state.conversation_id`, `state.message_id`, `state.metadata`

Control methods on `Response`:

- `response.start_research(*, timeout=None)` — approves a waiting plan and starts execution (sync or `await` depending on provider).
- `response.pause_research()` — pauses a running research session when supported.
- `response.cancel_research()` — cancels the research session.
- `response.wait_research(*, timeout=None)` — polls until the research reaches a terminal state (`COMPLETED`, `CANCELLED`, or `FAILED`) and returns the updated `Response` (including report `response.text`, `response.sources`, and any `text/markdown` report `Artifact`).

```python
from webaiprovider import DeepResearchTool, ResearchStatus

response = await chat.ask(
    "Research recent advances in WebAssembly GC.",
    tool=DeepResearchTool(auto_start=False),
)

if response.research and response.research.status == ResearchStatus.WAITING_FOR_APPROVAL:
    print("Plan:", response.research.plan)
    response = await response.start_research()

final = await response.wait_research(timeout=300.0)
print(final.text)
for artifact in final.artifacts:
    if artifact.mime_type == "text/markdown":
        artifact.save("report.md")
```

[Back to top](#contents)

---

## Streaming Research Events

When consuming `operation.stream()` for a Deep Research request, two additional `OperationEvent` types can be emitted:

- `OperationEvent.RESEARCH_PLAN_READY` → `ResearchPlanReady(plan: ResearchPlan, state: ResearchState)`
- `OperationEvent.RESEARCH_PROGRESS` → `ResearchProgressEvent(status: ResearchStatus, message: str, state: ResearchState | None)`

```python
from webaiprovider import DeepResearchTool, ResearchPlanReady, ResearchProgressEvent

op = chat.responses.create(
    "Summarize the state of Python packaging in 2026.",
    tool=DeepResearchTool(auto_start=True),
)

async for event in op.stream():
    if isinstance(event, ResearchPlanReady):
        print("Plan steps:", event.plan.steps)
    elif isinstance(event, ResearchProgressEvent):
        print(f"[{event.status.value}] {event.message}")

response = await op.wait()
```

[Back to top](#contents)

---

[← Previous: 06 · Quick API Reference](api-reference.md) · [Documentation Map](README.md)
