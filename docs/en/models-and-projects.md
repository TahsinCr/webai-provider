[← Previous: 02 · Chats, Responses, and Streaming](chats-and-streaming.md) · [Documentation Map](README.md) · [Next: 04 · Provider Guides →](provider-guides.md)

# 03 · Models, Effort Selection, and Projects

This guide explains how to discover models for an account, choose reasoning effort levels, work with the 300-second catalog cache, and manage projects along with project files and text resources.

---

## Contents

- [Quick Example](#quick-example)
- [Model Catalog (`provider.models`)](#model-catalog-providermodels)
  - [Query Methods and `refresh()`](#query-methods-and-refresh)
  - [`MODEL_CATALOG_TTL = 300.0` Caching Behavior](#model_catalog_ttl--3000-caching-behavior)
- [Model and Effort Selection Types](#model-and-effort-selection-types)
  - [`Models` Tiers and `model.select()`](#models-tiers-and-modelselect)
  - [Static Catalogs (`ChatGPTModels`, `ClaudeModels`, `GeminiModels`, `GrokModels`)](#static-catalogs-chatgptmodels-claudemodels-geminimodels-grokmodels)
- [Projects (`provider.projects`)](#projects-providerprojects)
  - [Creating, Updating, and Deleting Projects](#creating-updating-and-deleting-projects)
  - [`Project` Instructions and Project Chats](#project-instructions-and-project-chats)
- [Project Attachments (`project.attachments`)](#project-attachments-projectattachments)

---

## Quick Example

```python
from webaiprovider import Account, ChatGPTProvider, Models, ProviderType

account = Account.from_browser(ProviderType.CHATGPT)
with ChatGPTProvider(account) as provider:
    thinking = provider.models.find("thinking")
    selection = thinking if thinking else Models.AUTO

    project = provider.projects.create(
        "Architecture Review",
        instruction="Answer in concise bullet points.",
        memory_share=False,
    )
    project.attachments.create(
        "The v2 API runs natively on the caller thread.", "text", name="notes.txt"
    )
    project.attachments.create("src/main.py")

    chat = provider.chats.create("Code Analysis", project=project)
    print(chat.ask("Summarize the attached file.", model=selection).text)
```

[Back to top](#contents)

---

## Model Catalog (`provider.models`)

### Query Methods and `refresh()`

`provider.models` holds the `ModelRef` entries discovered for the account. It is automatically warmed during `open()` so initial model queries incur no extra round-trip.

- **`list(predicate=None, *, limit=None) -> list[ModelRef]`:** Returns all discovered models (or those matching `predicate`).
- **`get(model_id) -> ModelRef`:** Looks up a model by exact `id`; raises `RemoteNotFound` if missing.
- **`find(query, /, *, exact=False, startswith=False, endswith=False, single=False, last=False)`:** Matches a tier like `Models.HIGH` or text such as `"thinking"` / `"Sonnet"`.
- **`search(query, ...)`:** Returns a list of all matching models.
- **`resolve(target) -> ModelSelection`:** Validates a string, `Models` enum, `ModelRef`, `EffortRef`, or `ModelSelection` against the account's live catalog.
- **`refresh() -> list[ModelRef]`:** Bypasses the provider's internal catalog cache (`force=True`) and reloads models from the server.

```python
for model in provider.models.list():
    efforts = [e.id for e in model.efforts.list()]
    print(f"{model.id} ({model.name}) -> efforts={efforts}, default={model.default_effort_id}")
```

### `MODEL_CATALOG_TTL = 300.0` Caching Behavior

`ModelsCollection` and `AsyncModelsCollection` share the same **`MODEL_CATALOG_TTL = 300.0` (300 seconds)** cache lifetime used internally across all four providers:

- Within 300 seconds, queries are served directly from an atomic in-memory snapshot (empty results are cached for the same window).
- Once expired, the next query refreshes the catalog from the provider.
- Calling `authenticate()` or `forget_session()` clears the cached model catalog immediately.

[Back to top](#contents)

---

## Model and Effort Selection Types

### `Models` Tiers and `model.select()`

The `model=` parameter on `ask()`, `responses.create()`, `edit()`, and `retry()` accepts five forms: `Models` enum, `str`, `ModelRef`, `EffortRef`, or `ModelSelection`.

`webaiprovider.Models` works in two places:
1. **Directly as `model=`:**
   - `Models.AUTO`: Selects the provider's default model.
   - `Models.LOW`: Selects the lightest/fastest model in catalog order.
   - `Models.MEDIUM`: Selects a balanced mid-tier model.
   - `Models.HIGH`: Selects the highest-capability model in catalog order.
2. **Choosing an effort on a model (`model_ref.select(effort)`):**
   Calling `model_ref.select(Models.HIGH)` or `model_ref.select("extended")` returns a `ModelSelection(model, effort)`. Passing `effort=None` or `"auto"` keeps the default effort.

Passing an `EffortRef` directly to `model=` automatically resolves both the parent model and that effort via `SelectionResolver`. Ambiguous or missing string queries raise `AmbiguousMatchError` or `RemoteNotFound`.

### Static Catalogs (`ChatGPTModels`, `ClaudeModels`, `GeminiModels`, `GrokModels`)

Static catalogs are exported for IDE autocomplete on known model IDs:

```python
from webaiprovider import ChatGPTModels, GeminiModels, Models

sel1 = ChatGPTModels.get("gpt-6-thinking").select("extended")
sel2 = GeminiModels["gemini-neon"].select(Models.HIGH)
```

Static catalogs support `.list()`, `.get()`, `.find()`, `.search()`, `.contains()`, and `Catalog["id"]`. They provide well-known identifiers only; use `provider.models.list()` to inspect models available to a specific account, and `provider.models.refresh()` to bypass the cache before expiration. Plan limits or account permissions can still reject a model at send time.

[Back to top](#contents)

---

## Projects (`provider.projects`)

Projects (ChatGPT Projects, Claude Projects, Gemini Notebooks, and Grok Workspaces) group conversations around shared instructions and reference sources.

### Creating, Updating, and Deleting Projects

- **`projects.create(title, *, instruction=None)`:** Creates a new project. *(ChatGPT also accepts `memory_share: bool = True`.)*
- **`projects.update(project, *, title=None, instruction=None)`:** Updates only non-`None` fields remotely; pass `instruction=""` to clear instructions. Writes are serialized under the project's `project_lock`.
- **`projects.delete(project)`:** Deletes the project remotely.
- Standard collection queries (`list`, `get`, `first`, `last`, `single`, `find`, `search`) work identically on `provider.projects`.

### `Project` Instructions and Project Chats

- `project.rename(title)` and `project.delete()`: Shorthand mutations on a project instance.
- `project.instruction`:
  - `get() -> str`: Fetches the current instruction text from the server.
  - `edit(text: str) -> str`: Updates the instruction text.
  - `clear() -> None`: Clears instructions (`edit("")`).
- `project.chats`: A full `RemoteCollection` scoped to chats inside that project (`project.chats.list()`, `find()`, `search()`).

```python
with ClaudeProvider(account) as provider:
    project = provider.projects.create("Backend", instruction="Use Python 3.10+ type hints.")
    chat = provider.chats.create("Split module", project=project)
    chat.ask("Split auth.py into focused helpers.")

    for c in project.chats.list(limit=10):
        print(c.title, c.remote_id)
```

[Back to top](#contents)

---

## Project Attachments (`project.attachments`)

Use `project.attachments` to attach local files or inline text sources to a project:

| Method | Parameters | Description |
| --- | --- | --- |
| `create` | `(file_or_data, resource_type=None, *, name=None, mime_type=None, **options)` | Accepts a `Path`/file path (`resource_type="file"`), string (`resource_type="text"`), or `Attachment`. Raw `bytes` are rejected (`UnsupportedOperationError`). |
| `create_many` | `(*items)` | Attaches multiple items; uses batch endpoints where supported (e.g. ChatGPT) and rolls back partial uploads on failure. |
| `list` / `get` | standard query arguments | Fetches the project's current attachments from the server. |
| `download` | `(attachment)` | Downloads remote resources safely or reads bytes from an unsent local draft. |
| `delete` / `delete_many` | `(attachment)` / `(*attachments)` | Removes one or more attachments from the project. |
| `update` | `(attachment, **options)` | Works on unsent local project drafts only. |

```python
from webaiprovider import Account, Attachment, GeminiProvider, ProviderType

account = Account.from_browser(ProviderType.GEMINI)
with GeminiProvider(account) as provider:
    nb = provider.projects.create("Research")
    r1 = nb.attachments.create("Benchmark result: 35% faster.", "text", name="result.txt")
    r2 = nb.attachments.create(Attachment.from_path("report.pdf"))

    print("Sources:", [a.name for a in nb.attachments.list()])
    nb.attachments.delete_many(r1, r2)
    nb.delete()
```

[Back to top](#contents)

---

[← Previous: 02 · Chats, Responses, and Streaming](chats-and-streaming.md) · [Documentation Map](README.md) · **Next:** [04 · Provider Guides →](provider-guides.md)
