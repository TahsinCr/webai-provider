# webai-provider

Независимые Python-клиенты для веб-сессий ChatGPT, Gemini, Claude и Grok. API v2 предоставляет согласованные асинхронный и синхронный интерфейсы, типизированные ссылки на модели и потоковую обработку сообщений.

> **Бета-версия:** Веб-сайты и закрытые протоколы провайдеров могут меняться без предупреждения. Проект независимый и не связан с указанными провайдерами.

## Установка

Требуется Python 3.10 или новее.

Описанный здесь API v2 находится в разработке и пока не входит в опубликованный пакет `v1.3-beta`. После получения исходного кода с этой версией API установите его:

```bash
python -m pip install -e '.[dev]'
```

## Быстрый старт

Учетные данные можно импортировать из браузера, выбранного системой по умолчанию, или указать поддерживаемый браузер. Также можно передать cookies самостоятельно.

```python
import asyncio

from webaiprovider import Account, OutputDelta, Provider, ProviderType


async def main() -> None:
    account = Account.from_browser(ProviderType.CHATGPT, browser="firefox")

    async with Provider(account, type=ProviderType.CHATGPT) as provider:
        models = await provider.models.list()
        if not models:
            raise RuntimeError("No models are available for this account")

        chat = await provider.chats.create("Быстрый старт")
        operation = chat.messages.create("Здравствуйте!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

Если `browser` не указан, `Account.from_browser()` использует системный браузер по умолчанию. Для доступа к cookies может потребоваться закрыть браузер или разблокировать системное хранилище ключей. Чтобы передать учетные данные вручную, используйте `Account.from_cookies(provider, cookies)` и храните cookies в собственном защищенном хранилище.

Для блокирующего кода используйте тот же аккаунт с `SyncProvider`. Пока провайдер открыт, он повторно использует один портал событийного цикла:

```python
from webaiprovider import Account, ProviderType, SyncProvider

account = Account.from_browser(ProviderType.CHATGPT)
with SyncProvider(account, type=ProviderType.CHATGPT) as provider:
    chat = provider.chats.create("Быстрый старт")
    response = chat.messages.create("Здравствуйте!").start()
```

## Основной API

- `Account` хранит только учетные данные. `Provider(account, type=ProviderType.CHATGPT)` управляет асинхронной сессией, а `SyncProvider` предоставляет блокирующий интерфейс. Оба класса поддерживают контекстный менеджер.
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider` и `GrokProvider` предоставляют функции конкретных провайдеров. Проекты ChatGPT по умолчанию используют общую область памяти; укажите `memory_share=False`, чтобы оставить память только в проекте.
- `provider.models.list()` загружает доступные аккаунту модели и кеширует каталог до конца сессии. Для нового запроса используйте `refresh=True` или `provider.models.refresh()`. Методы `find()`, `search()` и `get()` выбирают модель из этого каталога.
- `chat.messages.create()`, `edit()` и `retry()` возвращают `MessageOperation`. Используйте `operation.stream()` для потока событий или дождитесь итогового ответа через `operation.wait()`. Эти методы используют одну отправку.
- Возможности проектов, вложений, повторной отправки, редактирования и ветвления зависят от провайдера. Неподдерживаемые действия завершаются типизированными исключениями `ProviderError`, например `FeatureUnavailable`.

Пакет предоставляет типизированные ссылки на модели и уровни усилий. Каталог, полученный от аккаунта, является источником доступных ему моделей и может различаться в зависимости от подписки и изменений на стороне провайдера.

## Безопасность и поведение

- Cookies браузера считываются локально. Библиотека не отправляет учетные данные на сервер SiopeChat.
- Защищайте cookies и файлы учетных данных; не добавляйте их в журналы, сообщения об ошибках или систему контроля версий.
- Для загрузки принимаются только значения `DownloadSource`, полученные от провайдера; публичные сетевые адреса проверяются, IP закрепляется, перенаправления не выполняются.
- Ошибки провайдера приводят к типизированным исключениям с сообщениями на английском языке. Отправка сообщения без открытой сессии провайдера завершается явной ошибкой, а не поддельным ответом.

## Разработка

```bash
ruff check .
ruff format --check .
python -m unittest discover -s . -t . -q
python -m build --sdist --wheel
twine check dist/*
```

## Ссылки

- [История изменений](CHANGELOG.md)
- [Сообщить об ошибке](https://github.com/TahsinCr/webai-provider/issues)
- [Репозиторий](https://github.com/TahsinCr/webai-provider)
- [SiopeChat](https://github.com/TahsinCr/chatgpt-web)

Лицензия GPL-3.0-only. Подробности см. в файлах [LICENSE](LICENSE) и [COPYRIGHT](COPYRIGHT).
