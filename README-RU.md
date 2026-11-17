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

from webaiprovider import Account, AsyncChatGPTProvider, OutputDelta, ProviderType


async def main() -> None:
    account = Account.from_browser(ProviderType.CHATGPT, browser="firefox")

    async with AsyncChatGPTProvider(account) as provider:
        models = await provider.models.list()
        if not models:
            raise RuntimeError("No models are available for this account")

        chat = await provider.chats.create("Быстрый старт")
        operation = chat.responses.create("Здравствуйте!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

Если `browser` не указан, `Account.from_browser()` использует системный браузер по умолчанию. Для доступа к cookies может потребоваться закрыть браузер или разблокировать системное хранилище ключей. Чтобы передать учетные данные вручную, используйте `Account.from_cookies(provider, cookies)` и храните cookies в собственном защищенном хранилище.

Для блокирующего кода используйте синхронный класс соответствующего провайдера. Пока провайдер открыт, он повторно использует один портал событийного цикла:

```python
from webaiprovider import Account, ChatGPTProvider, ProviderType

account = Account.from_browser(ProviderType.CHATGPT)
with ChatGPTProvider(account) as provider:
    chat = provider.chats.create("Быстрый старт")
    response = chat.responses.create("Здравствуйте!").start()
```

## Основной API

- `BaseProvider` содержит общие сведения об аккаунте и провайдере. Для асинхронной работы используйте `AsyncChatGPTProvider` (или соответствующий класс другого провайдера), а для блокирующей — `ChatGPTProvider`. Общие классы `AsyncProvider` и `Provider` подходят для выбора провайдера через `ProviderType` или внедрения собственного `BaseProviderAdapter` (`adapter=...`).
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider` и `GrokProvider` предоставляют функции конкретных провайдеров. Проекты ChatGPT по умолчанию используют общую область памяти; укажите `memory_share=False`, чтобы оставить память только в проекте.
- Удалённые коллекции (`chats`, `projects`, `project.chats`, `project.attachments`) скрывают пагинацию за `list(limit=50)` (`limit=0` не выполняет сетевых запросов; `limit=None` читает все страницы) и предоставляют единые методы `refresh()`, `get()`, `search()`, `find()` (вызывает `AmbiguousMatchError`), `first()` и `last()`. Класс `Page` используется только на границе адаптеров и не входит в пользовательский API.
- `provider.models.list()` загружает доступные аккаунту модели и кеширует каталог до конца сессии. Для нового запроса используйте `refresh=True` или `provider.models.refresh()`. Методы `find()`, `search()`, `first()`, `last()` и `get()` выбирают модель из этого каталога.
- Методы `chat.responses.create()`, `edit()` и `retry()` подготавливают операции с сообщениями. Используйте `operation.stream()` для потока событий или `operation.wait()` для получения готового `Response`. Ответ содержит связанное сообщение пользователя, источники, артефакты и данные о времени для текущей отправки.
- Чтобы открыть чат вместе с последними ответами за одно чтение истории, используйте `chat, responses = await provider.chats.get_with_responses(chat_id, limit=25)`. В синхронном API вызов выполняется без `await`. Лимит определяет число возвращаемых ответов.
- `project.attachments` предоставляет файловую проекцию `Attachment` поверх `project.resources` (`add()`, `remove()`, `list()`, `get()`, `find()`, `first()`, `last()`, `search()`).
- Провайдеры используют `ProviderRuntime` на базе `flowevent`. `provider.events` предоставляет штатный `AsyncEventBus` или `EventBus` и публикует события `EventEnvelope`; фильтровать их по имени можно через `EventFilter`. Состояния жизненного цикла и сообщений используют непосредственно `flowevent.StateMachine`. Обработчики хуков доступны через `provider.hooks` (`OperationHooks` / `SyncOperationHooks`), а `provider.lifecycle_state` возвращает `LifecycleState`.
- Возможности проектов, вложений, повторной отправки, редактирования, ветвления и переноса чатов зависят от провайдера. Классы чатов конкретных провайдеров предоставляют только поддерживаемые операции; неподдерживаемые действия завершаются типизированными исключениями `ProviderError`, например `FeatureUnavailable`.

Пакет предоставляет типизированные ссылки на модели и уровни усилий. Каталог, полученный от аккаунта, является источником доступных ему моделей и может различаться в зависимости от подписки и изменений на стороне провайдера.

## Низкоуровневый transport API

Все провайдеры предоставляют `provider.transport` для запросов `send`, обработки JSON через `request` и потокового чтения `stream` в пределах своего origin. Жизненным циклом transport управляет провайдер; синхронные и асинхронные вызовы используют существующее соединение, а не создают новый пул или цикл событий для каждого запроса.

```python
from webaiprovider import Account, AsyncClaudeProvider, ProviderType
from webaiprovider.transport import HttpSession, TransportConfig

account = Account.from_browser(ProviderType.CLAUDE)
transport = HttpSession(
    "https://claude.ai",
    account.store(),
    "sessionKey",
    config=TransportConfig(timeout=30, max_stream_bytes=64 * 1024 * 1024),
)

async with AsyncClaudeProvider(account, transport=transport) as provider:
    response = await provider.transport.send("GET", "/api/bootstrap")
    endpoint, payload = "/api/path", {}  # Укажите endpoint, поддерживаемый провайдером.
    async with provider.transport.stream("POST", endpoint, json=payload) as stream:
        async for chunk in stream.aiter_bytes():
            ...
```

`send()` возвращает `TransportResponse`. `request()` декодирует JSON и возвращает `None` для пустого ответа. Контекст потока владеет ответом до выхода из блока. При досрочном завершении закройте контекст, чтобы освободить ответ и связанный с ним рабочий поток. Для синхронного провайдера используйте `with` и `iter_bytes()`.

| Тип | Назначение |
| --- | --- |
| `AsyncTransport` / `Transport` | Контракты асинхронных и синхронных расширений |
| `AsyncHTTPTransport` | Асинхронный HTTP-клиент с потоковым контекстом; по умолчанию используется curl-cffi |
| `AsyncRequestTransport` | Асинхронный клиент, открывающий поток через `request(stream=True)` |
| `HTTPTransport` | Синхронный curl_cffi-клиент |
| `HttpSession` | Сохранение cookies аккаунта через общий асинхронный transport |
| `ThreadedTransport` | Ограниченный мост для scoped синхронного transport |

Claude и Grok используют `HttpSession`. Gemini использует `AsyncRequestTransport` для собственного RPC и обновления cookies. ChatGPT использует `HTTPTransport` внутри своего клиента и предоставляет его асинхронно через `ThreadedTransport`. Провайдеры управляют собственными клиентами и синхронизацией.

Собственная реализация может предоставить `send`, `stream` и `close` для `AsyncTransport` либо настроить встроенную реализацию через `client_factory`. Стандартные параметры HTTP, такие как заголовки, параметры, JSON и данные, передаются выбранному backend. Форматы multipart и специфичные для backend параметры относятся к его API расширения.

`TransportConfig` задаёт тайм-ауты и пределы буферизованных/потоковых ответов; стандартный предел буферизованного ответа — 16 MiB. При необходимости задайте предел потока отдельно. Вызовы отдельных endpoint могут переопределять тайм-аут. Расширенный код может указать `check_status=False`, чтобы самостоятельно проверить HTTP-ошибку; проверки origin и перенаправлений при этом остаются включёнными. Пути задаются относительно фиксированного origin. Абсолютные и protocol-relative URL, обход пути, конфликтующие заголовки `Host` и перенаправления отклоняются. Заголовки по умолчанию задаются отображением или функцией `(method, path)`; заголовки вызова имеют приоритет без учёта регистра. Используйте `inherit_headers=False`, если запрос, например обновление учётных данных, не должен наследовать заголовки. Подписанная загрузка на другой origin использует отдельный transport без заголовков аккаунта.

Мутации автоматически не повторяются. Периоды ожидания относятся к transport/аккаунту: ограничение одного аккаунта не блокирует другие аккаунты того же провайдера. Переданный transport должен использовать точный origin провайдера и тот же аккаунт; при завершении работы провайдер закрывает его. Клиент, переданный через `client_getter`, закрывает его владелец. Cookies и пул соединений переиспользуются операциями высокого и низкого уровня.

Декодирование кадров не зависит от HTTP. `SSEFrameDecoder` сохраняет поля `data`, `event`, `id` и числовой `retry`; обрабатывает разделённый UTF-8, CR, LF, CRLF, многострочные данные, комментарии и ограниченное число строк данных. Также доступны синхронные и асинхронные помощники для байтовых итераторов. `JSPBFrameDecoder` декодирует ограниченные JSON-кадры с префиксом длины и принимает `prefix`, `max_frame_units` и `length_includes_separator`; Gemini передаёт настройки anti-XSSI-префикса и разделителя. Каждый результат — целый JSON-кадр. Передайте `final=True` для последнего фрагмента, чтобы обнаружить незавершённые кадры; незавершённый JSPB-кадр приводит к `FrameDecodeError`. Смысл кадров и завершение операции определяет код провайдера.

Общие преобразования источников и файлов находятся в `webaiprovider.normalizers`. История провайдера, RPC-данные, идентификаторы продолжения и интерпретация ответов остаются в пакетах отдельных провайдеров.

Для расширений протокола `webaiprovider.base` предоставляет `BaseProviderAdapter` и субадаптеры (`BaseSessionAdapter`, `BaseModelAdapter`, `BaseChatAdapter`, `BaseProjectAdapter`, `EditorProjectAdapter`, `BaseMessageAdapter`, `BaseAttachmentAdapter`), а `webaiprovider.resources.message.protocol` предоставляет `ProviderRequest` и нормализованные события `ProviderStreamEvent` (`TextDelta`, `ThoughtDelta`, `TextReplacement`, `ResponseComplete`, `ConversationBound`, `MessageMetadata`):

```python
from collections.abc import AsyncIterator
from webaiprovider import AsyncProvider
from webaiprovider.base import BaseMessageAdapter, BaseProviderAdapter
from webaiprovider.resources.message.protocol import ProviderRequest, ProviderStreamEvent, TextDelta


class CustomMessageAdapter(BaseMessageAdapter):
    async def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderStreamEvent]:
        yield TextDelta(f"Echo: {request.prompt}")


# adapter = BaseProviderAdapter(..., messages=CustomMessageAdapter())
# provider = AsyncProvider(account, adapter=adapter)
```

Встроенные провайдеры используют те же контракты. Обычное приложение отправляет сообщения через `chat.messages`, оставляя wire-запросы и повторы провайдеру. `ProviderType` обозначает четыре поддерживаемых веб-провайдера. Использование transport и декодеров кадров с другим origin само по себе не добавляет поддержку его аутентификации, моделей, возможностей или ресурсов.

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

Замеры производительности без сети описаны в [руководстве по бенчмаркам](benchmarks/README.md).

## Диагностика

Измерение времени отправки включается явно и сохраняет именованные длительности и счётчики без содержимого сообщений и учётных данных. Активируйте его вокруг ожидаемой операции; автоматически оно не запускается.

```python
from webaiprovider.utils.timing import SendTiming, use_send_timing

timing = SendTiming(provider.id)
with use_send_timing(timing):
    response = await provider.ask("Hello")
durations_ms, counters = timing.snapshot()
```

`patch_project(project_id, name=...)` изменяет только переданные поля; для очистки инструкций используйте `instructions=""`. Перед записью изменений проекта текущее состояние сервера считывается под блокировкой. Потоки операций сохраняют всю историю событий для повторного чтения, поэтому потребление памяти сохранёнными операциями растёт с числом событий.

## Ссылки

- [История изменений](CHANGELOG.md)
- [Сообщить об ошибке](https://github.com/TahsinCr/webai-provider/issues)
- [Репозиторий](https://github.com/TahsinCr/webai-provider)
- [SiopeChat](https://github.com/TahsinCr/chatgpt-web)

Лицензия GPL-3.0-only. Подробности см. в файлах [LICENSE](LICENSE) и [COPYRIGHT](COPYRIGHT).
