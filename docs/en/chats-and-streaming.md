[← Previous: 01 · Getting Started and Accounts](getting-started.md) · [Documentation Map](README.md) · [Next: 03 · Models and Projects →](models-and-projects.md)

# 02 · Chats, Responses, Live Streaming, and Hooks

This guide covers creating and searching chats, attaching files, reading reasoning blocks (`ThinkingPart`), handling real-time streaming, editing or retrying earlier turns, and controlling execution with `wait_for()` and `chat.hooks`.

---

## Contents

- [Quick Example](#quick-example)
- [Chat Management (`provider.chats`)](#chat-management-providerchats)
  - [New and Temporary Chats (`create`)](#new-and-temporary-chats-create)
  - [Listing and Searching (`list`, `find`, `search`, `first`, `last`, `single`)](#listing-and-searching-list-find-search-first-last-single)
  - [Loading Chat + History Together (`get`, `get_with_responses`)](#loading-chat--history-together-get-get_with_responses)
  - [Renaming and Deleting (`update`, `delete`)](#renaming-and-deleting-update-delete)
- [The Chat Object (`Chat` / `AsyncChat`)](#the-chat-object-chat--asyncchat)
  - [`chat.ask()` — Sync vs. Async Return Types](#chatask--sync-vs-async-return-types)
  - [Other Methods (`stream`, `prepare`, `branch`, `move_to`, `download`)](#other-methods-stream-prepare-branch-move_to-download)
- [Messages and Attachments (`Message` & `Attachment`)](#messages-and-attachments-message--attachment)
- [Responses, Parts, Sources, and History (`Response`)](#responses-parts-sources-and-history-response)
  - [`response.text` and `response.parts`](#responsetext-and-responseparts)
  - [Sources, Artifacts, and Timing (`sources`, `artifacts`, `timing`)](#sources-artifacts-and-timing-sources-artifacts-timing)
  - [Edit, Retry, and History (`chat.responses`)](#edit-retry-and-history-chatresponses)
- [Live Streaming and Operation Control](#live-streaming-and-operation-control)
  - [Operation States (`OperationState`)](#operation-states-operationstate)
  - [Stream Events (`OperationEvent` and `StreamEvent`)](#stream-events-operationevent-and-streamevent)
  - [`wait_for()`, Pausing, and `cancel()`](#wait_for-pausing-and-cancel)
  - [Persistent Hooks (`chat.hooks`)](#persistent-hooks-chathooks)

---

## Quick Example

```python
import asyncio
from webaiprovider import (
    Account,
    AsyncChatGPTProvider,
    Attachment,
    GenerationEnd,
    OperationEvent,
    OutputDelta,
    ProviderType,
    ThinkingDelta,
    ThinkingPart,
)


async def main() -> None:
    account = Account.from_browser(ProviderType.CHATGPT)
    async with AsyncChatGPTProvider(account) as provider:
        chat = await provider.chats.create("Code Review")
        chat.hooks.on(OperationEvent.SOURCE_DISCOVERED, lambda ev: print("Source:", ev.source.url))

        op = chat.responses.create(
            "Check this file for potential bugs.",
            attachments=[Attachment.from_path("app.py")],
        )
        async for event in op.stream():
            if isinstance(event, ThinkingDelta):
                print(f"[Thinking] {event.delta}", end="", flush=True)
            elif isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)
            elif isinstance(event, GenerationEnd):
                print("\n--- Generation finished ---")

        response = await op.wait()
        print("Thinking blocks:", len(response.parts.list(lambda p: isinstance(p, ThinkingPart))))

        await chat.prepare()
        followup = await chat.ask("Fix the first issue with an example.").wait()
        print(followup.text)


asyncio.run(main())
```

[Back to top](#contents)

---

## Chat Management (`provider.chats`)

### New and Temporary Chats (`create`)

`provider.chats.create(title="New Chat", *, project=None, temporary=False)` prepares a new conversation locally without making a network request. The remote conversation is created when you send the first message, populating `chat.remote_id`.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `title` | `str` | `"New Chat"` | Conversation title. Where supported (e.g. Claude), applied on the first send. |
| `project` | `Project \| str \| None` | `None` | Target project or project ID to attach the conversation to. |
| `temporary` | `bool` | `False` | Opens a temporary conversation. Not written to remote history, cannot be moved into a project, and does not emit `chat.responses` collection-change events (`create`/`update`). |

### Listing and Searching (`list`, `find`, `search`, `first`, `last`, `single`)

All remote collections (`chats`, `projects`, `responses`, `project.chats`, `project.attachments`) share the same query interface:

- **`list(predicate=None, *, limit=50, include_projects=False)`:**
  - `limit=50` *(default)*: Walks pages automatically until up to 50 items are collected.
  - `limit=0`: Returns `[]` immediately with no network request.
  - `limit=None`: Fetches every available page (use cautiously on large accounts).
  - `include_projects=True`: Includes chats living inside projects alongside top-level chats.
  - `predicate`: Python-side filter predicate (e.g. `lambda c: not c.temporary`).
- **`first(predicate=None)` / `last(predicate=None)` / `single(predicate=None)`:**
  `first()` returns the first match (requesting only 1 item when `predicate=None`); `last()` returns the last match; `single()` requires exactly one match and raises `AmbiguousMatchError` if multiple items match.
- **`find(query, /, *, exact=False, startswith=False, endswith=False, single=False, last=False, **options)`:**
  Case-insensitive search by title or ID. At most one matching flag (`exact`, `startswith`, `endswith`) may be `True`; omitting them performs a substring match.
- **`search(query, /, *, exact=False, startswith=False, endswith=False, limit=None, **options)`:**
  Returns a list of all matches using the same matching rules as `find()`.

```python
with ChatGPTProvider(account) as provider:
    recent = provider.chats.list(limit=20, include_projects=True)
    report = provider.chats.find("Weekly Report", exact=True, single=True, include_projects=True)
    python_chats = provider.chats.search("Python", limit=5)
```

### Loading Chat + History Together (`get`, `get_with_responses`)

- **`chats.get(item_id, /, *, include_projects=False, metadata_only=False)`:** Fetches a chat by ID (raises `RemoteNotFound` if missing). Where supported, `metadata_only=True` reads title and project metadata without downloading the full message history.
- **`chats.get_with_responses(item_id, /, *, limit=50, include_projects=False)`:** When opening a conversation and its recent turns, returns `(chat, responses)` in **a single history fetch** instead of two separate calls. Passing `limit=0` reads metadata only and returns an empty list.

### Renaming and Deleting (`update`, `delete`)

- `provider.chats.update(chat, *, title=None)`: Updates the conversation title using a `Chat` instance or ID.
- `provider.chats.delete(chat)`: Deletes the conversation remotely.

[Back to top](#contents)

---

## The Chat Object (`Chat` / `AsyncChat`)

### `chat.ask()` — Sync vs. Async Return Types

The everyday shortcut for sending a prompt is `chat.ask(prompt, *, model=None, attachments=None, **metadata)`. Note the intentional return-type difference:

- **Synchronous (`Chat.ask`):** Starts the send, blocks until completion, and returns a **`Response` directly**.
- **Asynchronous (`AsyncChat.ask`):** Returns an **`AsyncMessageOperation`** so you can call `stream()` or `wait_for()`. To await the final response directly, use `await chat.ask("...").wait()` or simply `await chat.ask("...")` (`AsyncMessageOperation` is directly awaitable).

```python
# Synchronous: returns Response directly
response = sync_chat.ask("Summarize")

# Asynchronous (direct response):
response = await async_chat.ask("Summarize")

# Asynchronous (streaming events):
op = async_chat.ask("Summarize")
async for ev in op.stream():
    ...
response = await op.wait()
```

### Other Methods (`stream`, `prepare`, `branch`, `move_to`, `download`)

- **`chat.stream(prompt, *, model=None, attachments=None, **metadata)`:** Returns an iterator directly yielding stream events (`for ev in chat.stream(...)` / `async for ev in chat.stream(...)`).
- **`chat.prepare()`:** Pre-warms the provider's continuation context under the chat's send lock ahead of the next message (for example, pre-fetching continuation state on Claude).
- **`chat.rename(title)` / `chat.delete()`:** Shorthand for `chats.update` and `chats.delete`.
- **`chat.download(target)`:** Downloads an `Artifact` or remote URL as `bytes` using the SSRF-protected, no-redirect downloader.
- **`chat.branch(message_id, *, title=None)`** *(ChatGPT and Gemini)*: Forks a new parallel conversation from the specified message. Also available directly on `response.branch(title=...)`.
- **`chat.move_to(project)`** *(ChatGPT, Claude, Grok)*: Moves a chat into a project. Only ChatGPT additionally supports reassigning across projects and `chat.move_to(None)` to unassign a chat from its project. Holds only the chat lock.

[Back to top](#contents)

---

## Messages and Attachments (`Message` & `Attachment`)

Use `Attachment` to attach files, raw bytes, or text to a message or project:

- `Attachment.from_path(path, *, name=None, mime_type=None)`: Validates the file on disk and reads size and MIME metadata.
- `Attachment.from_bytes(data, name, *, mime_type=None)`: Creates an in-memory attachment (`name` is required).
- `Attachment.from_text(text, name, *, mime_type="text/plain", id=None, metadata=None)`: Creates a text-based attachment.
- `attachment.read_bytes() -> bytes`: Reads attachment bytes.

To manage multiple attachments on a draft `Message`, use `Attachments` (`create(item, *, name=None, mime_type=None)`, `update(target, *, name=None, mime_type=None)`, `delete(target)`, and `total_size`).

To build a `Message` / `AsyncMessage` object up front and edit it later:

```python
from webaiprovider import Attachment, Message

msg = Message(
    "Validate this JSON payload.",
    attachments=[Attachment.from_bytes(b'{"ok": true}', "data.json", mime_type="application/json")],
)
response = sync_chat.ask(msg)

# Edit the same user message in place:
edited = msg.edit("List schema errors only.").wait()
```

[Back to top](#contents)

---

## Responses, Parts, Sources, and History (`Response`)

### `response.text` and `response.parts`

Models may emit both reasoning (thinking) blocks and user-visible output blocks in sequence:

- **`response.text` (`str`):** Joins user-visible `TextPart` blocks with `\n` in provider order (never includes `ThinkingPart` text).
- **`response.parts` (`ResponseParts`):** Order-preserving read-only collection containing `ThinkingPart(id, text)` and `TextPart(id, text)`.

```python
from webaiprovider import TextPart, ThinkingPart

for part in response.parts:
    if isinstance(part, ThinkingPart):
        print(f"[Thinking {part.id}] {part.text[:80]}...")
    elif isinstance(part, TextPart):
        print(part.text)

# Filter thinking blocks only:
thoughts = response.parts.list(lambda p: isinstance(p, ThinkingPart))
```

### Sources, Artifacts, and Timing (`sources`, `artifacts`, `timing`)

- **`response.sources` (`SourcesCollection[Source]`):** Web search citations or grounding links (`id`, `url`, `title`, `snippet`, `kind`, `metadata`).
- **`response.artifacts` (`ArtifactsCollection[Artifact]`):** Generated files or images (`id`, `kind`, `name`, `mime_type`, `size`, `download_url`, `content`, `metadata`). Download via `chat.download(artifact)`.
- **`response.timing` (`TimingMetrics | None`):** Populated on live sends with `started_at`, `first_token_at`, `completed_at`, `first_token_ms`, and `duration_ms`.
- **`response.message`:** The user message that triggered this response.

### Edit, Retry, and History (`chat.responses`)

| Method | Parameters | Description |
| --- | --- | --- |
| `create` | `(message, *, model=None, attachments=None, **metadata)` | Prepares a new message operation (`MessageOperation` / `AsyncMessageOperation`). |
| `edit` | `(target, new_content, *, model=None, **options)` | Edits an earlier message (ID, `Message`, or `Response`) while preserving its attachments. |
| `update` | `(target, *, content=None, model=None, **options)` | Keeps existing content when `content=None` and updates fields such as `model`. |
| `retry` | `(target, *, model=None, **options)` | Regenerates the target response with the same or a different model. |
| `list` | `(predicate=None, *, limit=50, fresh=False)` | Lists conversation history. Concurrent async calls share a single request unless `fresh=True` is passed. |
| `delete` | `(target)` | Removes unsent local draft messages only; raises `UnsupportedOperationError` on persisted remote turns. |

[Back to top](#contents)

---

## Live Streaming and Operation Control

### Operation States (`OperationState`)

`chat.responses.create()`, `edit()`, and `retry()` return a `MessageOperation` (sync) or `AsyncMessageOperation` (async) handle:

- `operation.state` transitions through: `PENDING` → `PREPARING` → `SENDING` → `STREAMING` → `COMPLETED` | `FAILED` | `CANCELLED`.
- `operation.stream()`: Yields `StreamEvent` objects as they arrive. In synchronous mode, one driver thread reads the socket while any concurrent readers consume the shared event buffer; breaking out of the loop early (`GeneratorExit`) automatically calls `cancel()`.
- `operation.wait()` *(or `start()`)*: Drains the remaining stream and returns the completed `Response`.
- `operation.response` holds the response object and `operation.hook_results` records hook outputs. As soon as `GENERATION_END` fires, `operation.response`, `response.parts`, and the `chat.responses` entry are materialized immediately; any trailing `remote_id`, `ResponsePartsResolved`, `SourceDiscovered`, or `ArtifactDiscovered` updates reconcile onto that same `Response` instance in place. Always confirm terminal success via `wait()` or `STATUS_TERMINAL`.

### Stream Events (`OperationEvent` and `StreamEvent`)

Every item yielded from `stream()` is a `StreamEvent` subclass whose `.event` matches its `OperationEvent` enum value:

| `OperationEvent` | Class | Key Fields | Meaning |
| --- | --- | --- | --- |
| `THINKING_BEGIN` | `ThinkingBegin` | `part_id` | A reasoning block started. |
| `THINKING_DELTA` | `ThinkingDelta` | `part_id`, `delta` | Incremental reasoning text arrived. |
| `THINKING_REPLACE` | `ThinkingReplace` | `part_id`, `text` | Full text of the reasoning block changed. |
| `THINKING_END` | `ThinkingEnd` | `part_id`, `text` | Reasoning block finished. |
| `OUTPUT_BEGIN` | `OutputBegin` | `part_id` | Visible output block started. |
| `OUTPUT_DELTA` | `OutputDelta` | `part_id`, `delta` | Incremental output text arrived. |
| `OUTPUT_REPLACE` | `OutputReplace` | `part_id`, `text` | Full text of the output block changed. |
| `OUTPUT_END` | `OutputEnd` | `part_id`, `text` | Visible output block finished. |
| `GENERATION_END` | `GenerationEnd` | — | Model finished generating content. |
| `VERIFICATION_BEGIN` | `VerificationBegin` | — | Post-generation server reconciliation began. |
| `RESPONSE_PARTS_RESOLVED` | `ResponsePartsResolved` | `parts` | Final verified part sequence resolved. |
| `SOURCE_DISCOVERED` | `SourceDiscovered` | `source` | A citation `Source` was discovered. |
| `ARTIFACT_DISCOVERED` | `ArtifactDiscovered` | `artifact` | A generated `Artifact` was discovered. |
| `STATUS_TERMINAL` | `TerminalStatus` | `status`, `error` | Operation reached terminal state (`"completed"`, `"failed"`, `"cancelled"`). |

### `wait_for()`, Pausing, and `cancel()`

Update visible UI text directly from stream events. `OUTPUT_END` closes one text block, and more blocks may follow in the same turn. `GENERATION_END` signals that model generation is done and immediately populates `operation.response.parts` and `chat.responses`. The provider may still run post-generation verification afterward; rely on `wait()` or `STATUS_TERMINAL` to confirm final completion.

In async code, you can await individual milestones:

```python
from webaiprovider import OperationEvent

operation = chat.responses.create("Summarize the result in three points.")
generated = await operation.wait_for(OperationEvent.GENERATION_END, timeout=5.0)
if generated is not None:
    print("Generation complete; waiting for final confirmation.")

response = await operation.wait()
print(response.text)
```

`wait_for(event_kind, *, timeout=None, after=None)` waits until the requested event arrives:

- `timeout`: Timeout in seconds (`NaN` is rejected). Returns `None` if the deadline expires or the operation finishes without emitting the event. On built-in synchronous HTTP streams, a timeout leaves the underlying stream parked open so you can resume later with `wait()`, `stream()`, or another `wait_for()`.
- `after`: Limits the search to events emitted strictly after the given `StreamEvent` instance (useful for catching a second reasoning or output block).
- `operation.cancel()` requests cancellation. Even if the local stream closes, a prompt already accepted by the server may remain in remote history. In async code, await operation completion after requesting cancellation.

```python
from webaiprovider import OperationEvent, ThinkingEnd

op = chat.responses.create("Prepare a detailed breakdown.")
thinking = op.wait_for(OperationEvent.THINKING_END, timeout=5.0)
if isinstance(thinking, ThinkingEnd):
    print("Thinking finished:", thinking.text[:100])

# Drain the rest (or call op.cancel() if you no longer need it):
final_response = op.wait()
```

> **Synchronous Lock Rule:** When a synchronous send is parked via `wait_for(..., timeout=...)`, starting a new operation on the same chat from that thread before completing (`wait()` / `stream()`) or `cancel()`-ing the parked operation raises `ProviderError`.

### Persistent Hooks (`chat.hooks`)

Register listeners that run across all operations on a chat via `chat.hooks` (`provider.hooks`, `provider.chats.hooks`, and `provider.projects.hooks` follow the same API):

- **Stream listeners:** `chat.hooks.on(OperationEvent.OUTPUT_DELTA, fn)` or `"*"` for all events.
- **Lifecycle hooks:** `before("chat.send", fn)`, `on_success(...)`, `on_error(...)`, `on_cancelled(...)`, `on_finally(...)`.
  Modifying `ctx.prompt` or `ctx.model` inside `before("chat.send", ...)` applies those updated values to the outgoing request. Hooks cannot launch a new send on the same chat while inside a callback.

```python
@chat.hooks.before("chat.send")
def add_instruction(ctx):
    ctx.prompt = f"Keep your answer concise:\n{ctx.prompt}"


@chat.hooks.on_success("chat.send")
def on_done(ctx):
    print(f"Operation {ctx.operation_id} completed.")
```

[Back to top](#contents)

---

[← Previous: 01 · Getting Started and Accounts](getting-started.md) · [Documentation Map](README.md) · **Next:** [03 · Models and Projects →](models-and-projects.md)
