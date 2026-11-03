[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![Version: v1.0-beta][version-shield]](CHANGELOG.md)
[![GPL-3.0-only][license-shield]](LICENSE)

**English** | [Türkçe](README-TR.md) | [Русский](README-RU.md)

# webai-provider

### Independent Python clients for ChatGPT, Gemini and Claude web sessions.

Use provider web sessions through one asynchronous Python interface. These
unofficial clients do not use developer API keys, Codex or Claude Code. Model
access and usage limits come from the signed-in account, and provider website
changes can affect compatibility.

<p align="center">
  <a href="#install"><strong>Install</strong></a> &nbsp; | &nbsp;
  <a href="#quick-start"><strong>Quick start</strong></a> &nbsp; | &nbsp;
  <a href="#backend-api"><strong>API</strong></a> &nbsp; | &nbsp;
  <a href="#development"><strong>Development</strong></a>
</p>

## Install

Python 3.10+ is required for ChatGPT and Claude. Gemini's web transport requires
Python 3.11+.

```sh
git clone https://github.com/TahsinCr/webai-provider.git
cd webai-provider
python -m pip install .
```

For development and local checks, install the optional tools:

```sh
python -m pip install -e ".[dev]"
```

## Runtime and browser access

ChatGPT Sentinel fingerprint generation, proof-of-work solving and the
supported Turnstile challenge path run natively in pure Python
(`providers/chatgpt/sentinel/`) via a declarative `SentinelSpec` profile without
any JavaScript engine dependency. Unsupported required challenges stop before
the chat request is submitted. Browser page discovery, local CDP access
and session capture run in Python. Importing the library or configuring an
account does not open a browser or access the network. Gemini uses the bundled
native HTTPS implementation.

## Provider capabilities

| Provider | Web-session features |
| --- | --- |
| ChatGPT | Conversations, projects, model discovery, streaming, supported message actions, sources and generated files |
| Gemini | Conversations, notebooks, model and effort discovery, streaming, supported message actions, sources and generated media |
| Claude | Conversations, workspaces and projects, model discovery, streaming, sources and generated files |

The account's live catalog and provider capability declarations are authoritative.
An empty result means no items were returned; it does not mean the operation is
unsupported.

## Quick start

```python
import asyncio
from providers import ProviderClient, SendRequest, TextDelta, ReplaceText


async def main():
    async with ProviderClient() as client:
        account = client.connect(
            "claude", "personal", credential_file="/private/claude-session.json"
        )
        await account.authenticate()
        catalog = await account.models()
        model = catalog.default_chat_model
        request = SendRequest(
            provider_id="claude",
            account_id="personal",
            chat_id="my-local-draft",
            prompt="Hello",
            model=model.id if model else None,
        )
        async for event in client.stream(request):
            if isinstance(event, (TextDelta, ReplaceText)):
                print(event.channel, event.text)


asyncio.run(main())
```

## Accounts and sessions

The caller supplies credential paths and imports a valid session through its own
login flow or the profile helpers in `providers.profiles`. No application config is
read. For ChatGPT, `create_provider` also accepts an existing `BrowserSession`, or
an explicit `profile_directory` when using a credential store. File-backed
credentials default to a sibling `chrome-profile` directory. Each account has its
own adapter, connections and concurrency controls.

All three backends accept caller-owned credential storage through
`providers.session.CredentialStore` (`load`, `save`, `exists`, `delete`); file
paths remain supported. Database implementations belong to the host application.

Call `await account.prepare()` to establish the initial account connection before
interactive work starts. Concurrent first requests wait for the account's initial
setup. Model catalogs are cached for up to five minutes; `await account.models(force=True)`
requests an explicit refresh. Normal ChatGPT reads and project edits reuse an
account-scoped HTTP pool, while uploads and generation keep dedicated transports.
Large Claude JSON and Gemini RPC responses are parsed in background workers so
decoding does not run directly on the shared provider event loop. Credential
writes are serialized and complete before cancellation releases the save lock.

## Backend API

`client.account(provider_id, account_id)` returns the complete `Backend` contract:
session status and authentication, model catalogs, paginated chats and projects,
chat history, rename/delete, project editing and sources, file uploads and download
resolution. Use `describe()` and `project_options()` to discover supported actions.
Unsupported actions raise `FeatureUnavailable`; an empty list does not indicate
unsupported functionality.

### History and downloads

History messages keep published sources and generated-file descriptors in their
metadata. Streaming `MessageMetadata` events expose the same `sources` and `files`
collections, plus the reply's available model and effort. File descriptors contain
the native reference and available name, media type and size; file bytes are not
included. Pass a descriptor's `url` to `resolve_download` with the owning chat and
message IDs to obtain an authenticated, transient `DownloadSource`. ChatGPT file
assets and sandbox outputs, Gemini image/video/audio references, and Claude's presented output
paths are resolved by their own provider. A provider may expire or remove a file.

To continue a chat, set `SendRequest.conversation_id` to the chat's
`origin.remote_id`. For a project, also set `project_id` to its remote ID. Local
`chat_id` is a caller-owned stable identity. Keep the provider and account identity
from `Origin` with all stored records; remote IDs alone are not globally unique.

### Streaming and cancellation

Streams yield `TextDelta`, `ReplaceText`, `ConversationBound` and `MessageMetadata`. Replacement events
replace the corresponding channel's accumulated text. Published thinking summaries
use the analysis channel. Cancellation closes the transport; requests are never
automatically resent after an ambiguous send failure. Closing a client cancels its
active streams and closes all accounts, while keeping credential files intact.

### Errors

Provider failures derive from `ProviderError` and expose a stable machine-readable
`code`. For send failures, `message_submitted is False` guarantees the prompt did
not reach the provider; `None` means the client cannot prove whether it did. The
ChatGPT Sentinel compatibility failure has its own code and is reported as
“message not sent.” Claude and Gemini expose provider-specific compatibility and
stream error types for callers that need to distinguish a changed web contract
from an interrupted response.

### Edit, retry and branch

Message tools use provider identities, not list positions. `Message.actions` contains
supported operations for that message on the current conversation path. Use
`SendRequest(action=MessageAction.EDIT, message_id=..., prompt=...)` to edit a user
message, or `MessageAction.RETRY` to retry an assistant response without adding a
user message. Preserve `conversation_id` and the owning provider/account. Model
and effort choices are checked against that account's current catalog. An edit
selects a revised path; retrieve the confirmed history before replacing local
records. Failed or cancelled requests are never automatically resubmitted.

`await account.branch_chat(conversation_id, message_id)` returns a separate
conversation. ChatGPT and Gemini support this operation; Claude does not expose
it in this package. ChatGPT returns a branch preview: if its metadata contains
`pending_branch`, pass `BranchContext(**chat.metadata["pending_branch"])` as
`SendRequest.branch` on the first turn. The resulting `ConversationBound` identifies
the persisted conversation; do not try to fetch the preview from the server.
Persist the small continuation context if the preview must survive a restart.

### Sources, model access and persistence

Published sources are distinct from analysis-channel summaries. Source metadata
comes from native citations, including file references without a public URL;
answer-text links are not treated as verified sources. `MessageMetadata` supplies
native message identities, current-response tools and sources at stream completion.
No subscription-name allowlist is maintained: account model access is discovered
from the provider, while message operations follow the native supported contracts
and any message restrictions. A successful check on one subscription does not
establish availability on every plan; remote authorization errors remain authoritative.

Applications own persistence and synchronization. `get_chat()` returns the history
available from the provider; the library does not invent remote message pagination
where a web transport has none. The local SQL paging used by SiopeChat belongs to
its archive extension.

## Development

The package's runtime dependencies are declared in `pyproject.toml`; development
tools are optional. After installing `.[dev]`, run:

```sh
ruff check .
ruff format --check .
python -m unittest discover -s tests -t . -p 'test_*.py'
python -m build --sdist --wheel
python -m twine check dist/*
```

## Use with SiopeChat

SiopeChat includes this repository as a pinned submodule. Its checkout records
the exact provider commit it uses. Clone SiopeChat with submodules enabled:

```sh
git clone --recurse-submodules https://github.com/TahsinCr/siopechat.git
```

After updating SiopeChat, run `git pull --ff-only` in the application repository,
then `git submodule update --init --recursive` to check out the provider commit
selected by that application revision. Do not pull `webai-provider/main` from
inside SiopeChat's submodule; the parent repository controls its pinned commit.
See the [SiopeChat README](https://github.com/TahsinCr/siopechat) for details.

SiopeChat's wheel also bundles the `providers` Python package. Do not install
the standalone `webai-provider` distribution into SiopeChat's environment; both
distributions would manage the same import files.

### Update a standalone checkout

Run `git pull --ff-only` in the `webai-provider` repository, then reinstall it
with `python -m pip install --upgrade .`.

## Changelog

The `v1.0-beta` release consolidates the provider-specific history in
[CHANGELOG.md](CHANGELOG.md). New changes after this tag belong under
**Unreleased**.

## License

Copyright © TahsinCr. Licensed under [GPL-3.0-only](LICENSE). See
[COPYRIGHT](COPYRIGHT) for the project notice.

[version-shield]: https://img.shields.io/badge/version-v1.0--beta-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/License-GPL--3.0--only-2F6F4E.svg?style=for-the-badge
