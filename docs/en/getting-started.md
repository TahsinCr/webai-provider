[← Documentation Map](README.md) · [Next: 02 · Chats, Responses, and Streaming →](chats-and-streaming.md)

# 01 · Installation, Account Setup, and First Message

This guide covers installing the library, connecting an account from a browser profile or cookie store, managing sessions, and sending your first message.

---

## Contents

- [Quick Example](#quick-example)
- [Installation and System Notes](#installation-and-system-notes)
- [Core Concepts](#core-concepts)
- [Creating an Account (`WebAIAccount`)](#creating-an-account-webaiaccount)
  - [`WebAIAccount` Fields](#webaiaccount-fields)
  - [Reading Cookies from a Browser (`from_browser`)](#reading-cookies-from-a-browser-from_browser)
  - [Connecting with a Dictionary or File (`from_cookies`)](#connecting-with-a-dictionary-or-file-from_cookies)
- [Credential Stores (`CredentialStore`)](#credential-stores-credentialstore)
- [Provider Session and Lifecycle](#provider-session-and-lifecycle)
  - [Constructor Parameters](#constructor-parameters)
  - [Opening, Closing, and `provider.ask()`](#opening-closing-and-providerask)
  - [Session Status and Sign-Out (`session_status`, `authenticate`, `forget_session`)](#session-status-and-sign-out-session_status-authenticate-forget_session)

---

## Quick Example

Reading your active browser session and getting your first reply takes just a few lines:

```python
from webaiprovider import WebAIAccount, ChatGPTWebAIProvider, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT, browser="firefox")

with ChatGPTWebAIProvider(account) as provider:
    response = provider.ask("Hello! Briefly introduce yourself.")
    print(response.text)
```

In asynchronous projects, use the `Async*` classes with `async with`:

```python
import asyncio
from webaiprovider import WebAIAccount, AsyncClaudeWebAIProvider, WebAIProviderType


async def main() -> None:
    account = WebAIAccount.from_browser(WebAIProviderType.CLAUDE)
    async with AsyncClaudeWebAIProvider(account) as provider:
        response = await provider.ask("Hello Claude!")
        print(response.text)


asyncio.run(main())
```

[Back to top](#contents)

---

## Installation and System Notes

`webai-provider` requires **Python 3.10 or newer**. Runtime dependencies (`curl-cffi`, `flowevent`, `orjson`, `browser-cookie3`, `lz4`) are installed automatically.

```bash
# Install the published package
python -m pip install webai-provider

# Development install from the repository root
python -m pip install -e .

# With development and packaging tools (build, ruff, twine)
python -m pip install -e '.[dev]'

# With the offline benchmark suite dependency (pyperf)
python -m pip install -e '.[benchmark]'
```

These guides document the `webai-provider` **v2.0.0** API. Check your installed version with `python -m pip show webai-provider` and upgrade with `python -m pip install -U webai-provider` if needed.

**Practical notes on browser cookie import:**
When calling `WebAIAccount.from_browser()`, keep OS-level browser storage rules in mind:
- **Linux:** GNOME Keyring or KWallet must be unlocked.
- **macOS:** You may be prompted for Keychain access on the first read.
- **Windows / Chromium:** Recent Chrome or Edge versions may lock their cookie SQLite database or use additional encryption. If reading fails, close the browser and retry, or export your cookies to a JSON file and pass it to `WebAIAccount.from_cookies()`.

[Back to top](#contents)

---

## Core Concepts

Every connection is built from two pieces:

1. **`WebAIAccount`:** Identity and credential holder specifying which platform (`chatgpt`, `claude`, `gemini`, `grok`) and which cookies to use. Account attributes cannot be reassigned, while the underlying credential store contents and `metadata` dictionary can be updated. The same `WebAIAccount` can back multiple provider sessions over time.
2. **`WebAIProvider` (or `AsyncWebAIProvider`):** The live session object that connects to the platform, discovers models, and manages chats and projects.

Opening a provider allocates an HTTP connection pool and session state that should be released cleanly when finished. Always prefer `with` (or `async with`), which guarantees `close()` runs on exit even if an exception is raised.

[Back to top](#contents)

---

## Creating an Account (`WebAIAccount`)

Most workflows only need `WebAIAccount.from_browser()` or `WebAIAccount.from_cookies()`. If you are plugging in a custom credential store, instantiate `WebAIAccount(...)` directly.

### `WebAIAccount` Fields

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `id` | `str` | Required | Unique identifier within your application. Emitted as `account_id` on `EventBus` records. |
| `provider` | `str` | Required | `"chatgpt"`, `"claude"`, `"gemini"`, or `"grok"` (case-insensitive). |
| `credentials` | `CredentialStore \| Path \| str \| dict` | Required | A `dict` wraps an in-memory `MemoryCredentialStore`; a `Path` or `str` wraps a `FileCredentialStore`. |
| `capabilities` | `tuple[Capability, ...]` | `()` | Optional application metadata; does not restrict the provider's own capabilities. |
| `metadata` | `dict[str, Any]` | `{}` | Arbitrary application metadata (for example, a user label). |
| `name` | `str \| None` | `None` | Human-readable account display name. |
| `is_active` | `bool` | `True` | Application-level enabled state for the account. |

After a successful open, an `WebAIAccount` caches its verified session status alongside a credential fingerprint for 300 seconds. Opening another `WebAIProvider(account)` within that window skips redundant startup checks. The cache is automatically invalidated whenever credentials change, expire, or fail verification.

### Reading Cookies from a Browser (`from_browser`)

```python
from webaiprovider import WebAIAccount, WebAIProviderType
from webaiprovider.errors import (
    ProfileDatabaseError,
    ProfileKeyringError,
    ProfileSessionNotFoundError,
)

try:
    account = WebAIAccount.from_browser(
        WebAIProviderType.GEMINI,
        browser="chrome",
        id="personal-gemini",
        name="Chrome Gemini",
    )
except ProfileSessionNotFoundError:
    print("No signed-in Gemini session found in Chrome.")
except ProfileDatabaseError:
    print("Cookie database is locked; close the browser and try again.")
except ProfileKeyringError:
    print("System keyring is locked.")
```

- **`provider` (`WebAIProviderType | str`):** Target platform (`WebAIProviderType.CHATGPT`, `"claude"`, etc.).
- **`browser` (`str | None = None`):** Browser to read cookies from. Leave as `None` to use the OS default browser. Supported values: `"chrome"`, `"chromium"`, `"brave"`, `"brave-origin"`, `"edge"` (`"msedge"`), `"vivaldi"`, `"firefox"`, `"zen"`, `"safari"` *(macOS)*.
- **`id` and `name` (`str | None = None`):** Optional identifier and display name; generated automatically if omitted.

`from_browser()` extracts only the allowed cookies for that provider inside an isolated subprocess and stores them in an in-memory `MemoryCredentialStore`—nothing is written to disk automatically.

### Connecting with a Dictionary or File (`from_cookies`)

You can bypass browser reading altogether by passing a cookie dictionary or a JSON file path. When a provider rotates cookies during an open session (such as Gemini's `RotateCookies`), providing a file-backed store persists updates atomically back to the same file.

```python
from pathlib import Path
from webaiprovider import WebAIAccount, WebAIProviderType

# 1) In-memory dictionary (great for tests or one-off scripts)
acc_mem = WebAIAccount.from_cookies(
    WebAIProviderType.CLAUDE,
    {"sessionKey": "sk-ant-sid01-..."},
    name="Temp Claude",
)

# 2) Persistent JSON file (recommended for long-lived apps)
acc_file = WebAIAccount.from_cookies(
    WebAIProviderType.GROK,
    Path("secrets/grok.json"),
    id="grok-main",
)
```

Primary cookies required by each platform for session verification:

| Provider | Required Key Cookie | Helpful Additional Cookies |
| --- | --- | --- |
| **ChatGPT** | `__Secure-next-auth.session-token` *(or `__Secure-authjs.session-token`)* | `__Host-next-auth.csrf-token`, `oai-did`, `__cf_bm`, `cf_clearance` |
| **Claude** | `sessionKey` | `lastActiveOrg`, `anthropic-device-id`, `__cf_bm`, `cf_clearance` |
| **Gemini** | `__Secure-1PSID` | `OSID`, `__Secure-OSID`, `__Secure-1PSIDTS`, `__Secure-1PSIDCC`, `SID`, `SIDCC` |
| **Grok** | `sso` *(or `sso-rw`)* | `sso-rw`, `__cf_bm`, `cf_clearance` |

[Back to top](#contents)

---

## Credential Stores (`CredentialStore`)

Calling `account.store()` returns the store backing that account's credentials:

- **`FileCredentialStore(path)`:**
  - `exists() -> bool`: Checks whether the credential file is present.
  - `load() -> dict`: Reads a regular JSON file up to 1 MiB; raises `AccountPaused` if missing or malformed.
  - `save(value, *, ensure_ascii=False, newline=False)`: Writes under a path lock to a `0o600` temporary file, `os.fsync`s it, and atomically replaces the target.
  - `delete()`: Removes the credential file from disk.

- **`MemoryCredentialStore(initial=None)`:**
  Stores the credential dictionary in memory protected by a `threading.Lock` and `deepcopy` isolation.

To import from a browser on first run and persist to disk for subsequent runs:

```python
from pathlib import Path
from webaiprovider import WebAIAccount, FileCredentialStore, WebAIProviderType

store = FileCredentialStore(Path.home() / ".config" / "myapp" / "gemini.json")
if not store.exists():
    browser_acc = WebAIAccount.from_browser(WebAIProviderType.GEMINI)
    store.save(browser_acc.store().load())

account = WebAIAccount.from_cookies(WebAIProviderType.GEMINI, store, id="gemini-local")
```

If you store credentials in SQLite or an encrypted vault, implement the `CredentialStore` protocol (`load`, `save`, `exists`, `delete`) and pass your instance directly to `WebAIAccount(..., credentials=...)`.

[Back to top](#contents)

---

## Provider Session and Lifecycle

### Constructor Parameters

The generic `WebAIProvider(account)` and `AsyncWebAIProvider(account)` constructors inspect `account.provider` and return the matching concrete class (`ChatGPTWebAIProvider`, `ClaudeWebAIProvider`, `GeminiWebAIProvider`, or `GrokWebAIProvider`).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `account` | `WebAIAccount` | Required | Account to connect with. |
| `type` | `WebAIProviderType \| str \| None` | `None` | Explicitly verify the provider type; raises `ValueError` if it conflicts with `account.provider`. |
| `transport` | `Transport \| AsyncTransport \| None` | `None` | Custom transport for timeouts, headers, or HTTP client injection (must match the provider's origin). |

*(Provider-specific parameters such as ChatGPT's `session` or Gemini's `notebooks` are covered in [04 · Provider Guides](provider-guides.md).)*

Once instantiated, a provider exposes: `id`, `type`, `account_id`, `capabilities`, `opened`, `closing`, `closed`, `lifecycle_state`, `chats`, `projects`, `models`, `events`, `hooks`, and `transport`.

### Opening, Closing, and `provider.ask()`

When you cannot use a `with` / `async with` block, pair `open()` and `close()` inside a `try / finally`:

```python
provider = AsyncWebAIProvider(account)
await provider.open()
try:
    models = await provider.models.list()
    print("Available models:", len(models))
finally:
    await provider.close()
```

A provider in the `CLOSED` state cannot be reopened; create a new `WebAIProvider(account)` instance for a fresh session.

`provider.ask()` creates a new conversation with default settings on each call, which may remain in the provider's chat history. To continue an existing conversation, call `provider.chats.create()` once and send follow-ups through that `chat` object. If `provider.ask()` is called on an unopened provider, it opens the session for that single call and closes it immediately afterward—wrap multiple operations in `with` / `async with` to reuse connections.

```python
from webaiprovider import WebAIAccount, Attachment, Models, WebAIProvider, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)
provider = WebAIProvider(account)

reply = provider.ask(
    "Summarize this document in three bullet points.",
    model=Models.HIGH,
    attachments=[Attachment.from_path("report.pdf")],
)
print(reply.text)
```

`ask()` parameters: `prompt: str`, `model: Models | EffortRef | ModelRef | ModelSelection | str | None = None`, `attachments=None`.

### Session Status and Sign-Out (`session_status`, `authenticate`, `forget_session`)

- **`session_status() -> SessionStatus`:** Reports the session state currently known to the library. `UNKNOWN` does not prove the session is invalid; call `authenticate()` for a live check.
- **`authenticate() -> SessionStatus`:** Verifies the session against the remote provider and clears internal model and verification caches.
- **`forget_session() -> SessionStatus`:** Deletes stored credentials (`store().delete()`) and clears in-memory session state.

[Back to top](#contents)

---

[← Documentation Map](README.md) · **Next:** [02 · Chats, Responses, and Streaming →](chats-and-streaming.md)
