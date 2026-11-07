# webai-provider

Independent Python clients for ChatGPT, Gemini, Claude, and Grok web sessions. The v2 API provides matching asynchronous and blocking interfaces, typed model references, and streaming message operations.

> **Beta:** Provider websites and private web protocols can change without notice. This project is independent and is not affiliated with the providers.

## Install

Requires Python 3.10 or newer.

The v2 API documented here is still in development and is not part of the published `v1.3-beta` package. Install it from a checkout that contains this API:

```bash
python -m pip install -e '.[dev]'
```

## Quick start

You can import credentials from the operating system's default browser or name a supported browser explicitly. You can also provide cookies yourself.

```python
import asyncio

from webaiprovider import Account, OutputDelta, Provider, ProviderType


async def main() -> None:
    account = Account.from_browser(ProviderType.CHATGPT, browser="firefox")

    async with Provider(account, type=ProviderType.CHATGPT) as provider:
        models = await provider.models.list()
        if not models:
            raise RuntimeError("No models are available for this account")

        chat = await provider.chats.create("Quick start")
        operation = chat.messages.create("Hello!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

`Account.from_browser()` uses the default browser when `browser` is omitted. Browser-cookie access may require the browser to be closed or the operating system keyring to be unlocked. To supply credentials directly, use `Account.from_cookies(provider, cookies)` and keep the cookie values in your own secure store.

For blocking code, use `SyncProvider` with the same account and provider type. It keeps one event-loop portal alive for the provider's lifetime:

```python
from webaiprovider import Account, ProviderType, SyncProvider

account = Account.from_browser(ProviderType.CHATGPT)
with SyncProvider(account, type=ProviderType.CHATGPT) as provider:
    chat = provider.chats.create("Quick start")
    response = chat.messages.create("Hello!").start()
```

## Core API

- `Account` stores credentials. `Provider(account, type=ProviderType.CHATGPT)` owns the asynchronous session; `SyncProvider` offers the blocking equivalent. Both support context-manager cleanup.
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider`, and `GrokProvider` provide provider-specific extensions. ChatGPT projects use the default/global memory scope unless `memory_share=False` selects project-only memory.
- `provider.models.list()` loads and caches the account's available models for that session. Use `refresh=True` or `provider.models.refresh()` to request a new catalog. `find()`, `search()`, and `get()` select from that catalog.
- `chat.messages.create()`, `edit()`, and `retry()` return a `MessageOperation`. Consume `operation.stream()` for events, or await `operation.wait()` for the completed response. These entry points share one send operation.
- Project, attachment, retry, edit, and branching capabilities vary by provider. Unsupported actions raise a typed `ProviderError` such as `FeatureUnavailable`.

The package exports model references and effort choices for typed use. Account-discovered models remain authoritative for the account and may differ by subscription or provider changes.

## Security and behavior

- Browser credentials are read locally. The library does not send credentials to a SiopeChat service.
- Keep cookies and credential files private; do not include them in logs, issue reports, or source control.
- Downloads accept provider-supplied `DownloadSource` values, validate public network destinations, pin the resolved address, and do not follow redirects.
- Provider failures are raised as English-language typed exceptions. Sending from a chat without an open provider session fails explicitly instead of returning a fabricated response.

## Development

```bash
ruff check .
ruff format --check .
python -m unittest discover -s . -t . -q
python -m build --sdist --wheel
twine check dist/*
```

## Links

- [Changelog](CHANGELOG.md)
- [Issue tracker](https://github.com/TahsinCr/webai-provider/issues)
- [Repository](https://github.com/TahsinCr/webai-provider)
- [SiopeChat](https://github.com/TahsinCr/chatgpt-web)

Licensed under GPL-3.0-only. See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT).
