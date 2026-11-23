[![CI][ci-shield]](https://github.com/TahsinCr/webai-provider/actions/workflows/ci.yml)
[![PyPI][pypi-shield]](https://pypi.org/project/webai-provider/)
[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![GPL-3.0-only][license-shield]](LICENSE)

[English](README.md) | [Türkçe](README-TR.md) | **Русский**

# webai-provider

Независимые Python-клиенты для веб-сессий ChatGPT, Gemini, Claude и Grok. API v2 предоставляет согласованные асинхронный и синхронный интерфейсы, типизированные ссылки на модели и потоковую обработку сообщений.

> [Документация на английском языке](docs/en/README.md) охватывает установку, потоковую передачу чата, выбор моделей, управление проектами и справочник API.

> **Бета-версия:** Веб-сайты и закрытые протоколы провайдеров могут меняться без предупреждения. Проект независимый и не связан с указанными провайдерами.

## Установка

Требуется Python 3.10 или новее.

```bash
pip install webai-provider
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

Для блокирующего кода используйте синхронный класс соответствующего провайдера. Блокирующие вызовы выполняются непосредственно в вызывающем потоке без запуска фонового цикла событий:

```python
from webaiprovider import Account, ChatGPTProvider, ProviderType

account = Account.from_browser(ProviderType.CHATGPT)
with ChatGPTProvider(account) as provider:
    chat = provider.chats.create("Быстрый старт")
    response = chat.responses.create("Здравствуйте!").start()
```

## Основной API

- `BaseProvider` хранит идентификаторы аккаунта и провайдера. Для асинхронной работы используйте `AsyncProvider`, для блокирующих вызовов — `Provider`. Дополнительные возможности доступны в классах конкретных провайдеров, например `AsyncChatGPTProvider`.
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider` и `GrokProvider` предоставляют функции конкретных провайдеров. Проекты ChatGPT по умолчанию используют общую область памяти; укажите `memory_share=False`, чтобы оставить память только в проекте. Для чата ChatGPT используйте `chat.move_to(project)`, чтобы добавить его в проект или перенести в другой проект, и `chat.move_to(None)`, чтобы удалить его из проекта.
- Удалённые коллекции читают данные провайдера по запросу. `list(limit=50)` управляет пагинацией, `limit=0` пропускает сетевые запросы, а `limit=None` читает все доступные страницы. Общие методы коллекций — `get()`, `search()`, `find()`, `first()`, `last()` и `single()`. При нескольких совпадениях `find(single=True)` вызывает `AmbiguousMatchError`.
- Запросы к моделям используют каталог аккаунта, загруженный при открытии сессии провайдера. Пустой каталог также сохраняется на время сессии. Вызовите `provider.models.refresh()`, чтобы загрузить каталог заново.
- Одновременные вызовы `chat.responses.list()` используют один запрос истории. Для независимого запроса используйте `chat.responses.list(fresh=True)`.
- Методы `chat.responses.create()`, `edit()` и `retry()` подготавливают операции с сообщениями. Используйте `operation.stream()` для потока событий или `operation.wait()` для получения готового `Response`. Ответ содержит связанное сообщение пользователя, источники, артефакты и данные о времени для текущей отправки.
- Чтобы открыть чат вместе с последними ответами за одно чтение истории, используйте `chat, responses = await provider.chats.get_with_responses(chat_id, limit=25)`. В синхронном API вызов выполняется без `await`. Лимит определяет число возвращаемых ответов.
- Добавляйте файлы и текст в проект через `project.attachments.create()`. Для просмотра вложений используйте `list()`, для удаления — `delete()`.
- Провайдеры управляют жизненным циклом, блокировками, хуками и событиями непосредственно в `Provider` и `AsyncProvider` на базе `flowevent`. `provider.events` предоставляет штатный `AsyncEventBus` или `EventBus` и публикует события `EventEnvelope`; фильтровать их по имени можно через `EventFilter`. Состояния жизненного цикла и сообщений используют непосредственно `flowevent.StateMachine`. Обработчики хуков доступны через `provider.hooks` (`OperationHooks` / `AsyncOperationHooks`), а `provider.lifecycle_state` возвращает `LifecycleState`.
- Событие `collection.read.completed` передаёт пакет идентификаторов после успешного удалённого чтения. `collection.changed` сообщает о подтверждённых добавлениях, обновлениях и удалениях. В обоих событиях список `items` доступен в `event.metadata["payload"]`. Метаданные содержат аккаунт и идентификатор родительского ресурса для вложенных коллекций. Изменения ответов относятся к соответствующему чату. События не содержат текст сообщений, содержимое файлов или учётные данные.
- Возможности проектов, вложений, повторной отправки, редактирования, ветвления и переноса чатов зависят от провайдера. Классы чатов конкретных провайдеров предоставляют только поддерживаемые операции; неподдерживаемые действия завершаются типизированными исключениями `ProviderError`, например `FeatureUnavailable`.

Пакет предоставляет типизированные ссылки на модели и уровни усилий. Каталог, полученный от аккаунта, является источником доступных ему моделей и может различаться в зависимости от подписки и изменений на стороне провайдера.

## Низкоуровневый transport API

Все провайдеры предоставляют `provider.transport` для запросов `send`, обработки JSON через `request` и потокового чтения `stream` в пределах своего origin. Жизненным циклом transport управляет провайдер; синхронные и асинхронные вызовы используют существующее соединение, а не создают новый пул или цикл событий для каждого запроса.

```python
from webaiprovider import Account, AsyncClaudeProvider, ProviderType
from webaiprovider.transport import AsyncHttpSession, TransportConfig

account = Account.from_browser(ProviderType.CLAUDE)
transport = AsyncHttpSession(
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
| `HTTPTransport` | Синхронный curl_cffi-клиент |
| `AsyncHttpSession` | Сохранение cookies аккаунта через общий асинхронный transport |
| `HttpSession` | Сохранение cookies аккаунта через синхронный HTTP в вызывающем потоке |

Claude и Grok используют `AsyncHttpSession` / `HttpSession`. Gemini и ChatGPT выполняют веб-запросы через `AsyncHTTPTransport` / `HTTPTransport`. Асинхронный и синхронный API соблюдают общие правила origin, cookies и безопасности; блокирующие провайдеры выполняются непосредственно в вызывающем потоке без фонового цикла событий. Конфликтующие изменения чатов и проектов сериализуются блокировками ресурсов.

Собственная реализация может предоставить `send`, `stream` и `close` для `AsyncTransport` или `Transport` либо настроить встроенную реализацию через `client_factory`. Стандартные параметры HTTP, такие как заголовки, параметры, JSON и данные, передаются выбранному backend. Форматы multipart и специфичные для backend параметры относятся к его API расширения.

`TransportConfig` задаёт тайм-ауты и пределы буферизованных/потоковых ответов; стандартный предел буферизованного ответа — 64 MiB. `max_stream_bytes` по умолчанию не ограничен (`None`), чтобы длинные потоки не обрывались; при необходимости задайте предел потока отдельно. Вызовы отдельных endpoint могут переопределять тайм-аут. Расширенный код может указать `check_status=False`, чтобы самостоятельно проверить HTTP-ошибку; проверки origin и перенаправлений при этом остаются включёнными. Пути задаются относительно фиксированного origin. Абсолютные и protocol-relative URL, обход пути, конфликтующие заголовки `Host` и перенаправления отклоняются. Заголовки по умолчанию задаются отображением или функцией `(method, path)`; заголовки вызова имеют приоритет без учёта регистра. Используйте `inherit_headers=False`, если запрос, например обновление учётных данных, не должен наследовать заголовки. Подписанная загрузка на другой origin использует отдельный transport без заголовков аккаунта.

Мутации автоматически не повторяются. Периоды ожидания относятся к transport/аккаунту: ограничение одного аккаунта не блокирует другие аккаунты того же провайдера. Переданный transport должен использовать точный origin провайдера и тот же аккаунт; при завершении работы провайдер закрывает его. Клиент, переданный через `client_getter`, закрывает его владелец. Cookies и пул соединений переиспользуются операциями высокого и низкого уровня.

Декодирование кадров не зависит от HTTP. `SSEFrameDecoder` сохраняет поля `data`, `event`, `id` и числовой `retry`; обрабатывает разделённый UTF-8, CR, LF, CRLF, многострочные данные, комментарии и ограниченное число строк данных. Также доступны синхронные и асинхронные помощники для байтовых итераторов. `JSPBFrameDecoder` декодирует ограниченные JSON-кадры с префиксом длины и принимает `prefix`, `max_frame_units` и `length_includes_separator`; Gemini передаёт настройки anti-XSSI-префикса и разделителя. Каждый результат — целый JSON-кадр. Передайте `final=True` для последнего фрагмента, чтобы обнаружить незавершённые кадры; незавершённый JSPB-кадр приводит к `FrameDecodeError`. Смысл кадров и завершение операции определяет код провайдера.

История провайдера, RPC-данные, идентификаторы продолжения и интерпретация ответов остаются в пакетах отдельных провайдеров.

Отправляйте сообщения через `chat.responses`. Провайдер управляет сетевыми запросами и повторными попытками.

## Безопасность и поведение

- Cookies браузера считываются локально. Библиотека не отправляет учетные данные на сервер SiopeChat.
- Защищайте cookies и файлы учетных данных; не добавляйте их в журналы, сообщения об ошибках или систему контроля версий.
- Для загрузки принимаются только значения `DownloadSource`, полученные от провайдера; публичные сетевые адреса проверяются, IP закрепляется, перенаправления не выполняются.
- Ошибки провайдера приводят к типизированным исключениям с сообщениями на английском языке. Отправка сообщения без открытой сессии провайдера завершается явной ошибкой, а не поддельным ответом.

## Разработка

```bash
ruff check .
ruff format --check .
python run_tests.py
python -m build --sdist --wheel
twine check dist/*
```

Замеры производительности без сети описаны в [руководстве по бенчмаркам](benchmarks/README.md).

## Диагностика

Измерение времени отправки включается явно и сохраняет именованные длительности и счётчики без содержимого сообщений и учётных данных. Активируйте его вокруг ожидаемой операции; автоматически оно не запускается.

```python
from webaiprovider.core.timing import SendTiming, use_send_timing

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

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/webai-provider/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/webai-provider?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-GPL--3.0--only-blue.svg?style=for-the-badge
