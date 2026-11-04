[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![Версия: v1.0-beta][version-shield]](CHANGELOG.md)
[![GPL-3.0-only][license-shield]](LICENSE)

[English](README.md) | [Türkçe](README-TR.md) | **Русский**

# webai-provider

### Независимые Python-клиенты веб-сеансов ChatGPT, Gemini, Claude и Grok.

Используйте веб-сеансы провайдеров через единый асинхронный интерфейс Python.
Эти неофициальные клиенты не используют ключи API для разработчиков, Codex или
Claude Code. Доступ к моделям и лимиты зависят от аккаунта; изменения сайтов
провайдеров могут повлиять на совместимость.

<p align="center">
  <a href="#установка"><strong>Установка</strong></a> &nbsp; | &nbsp;
  <a href="#быстрый-старт"><strong>Быстрый старт</strong></a> &nbsp; | &nbsp;
  <a href="#поведение-api"><strong>API</strong></a> &nbsp; | &nbsp;
  <a href="#разработка"><strong>Разработка</strong></a>
</p>

## Установка

Для всех провайдеров требуется Python 3.10+.

```sh
git clone https://github.com/TahsinCr/webai-provider.git
cd webai-provider
python -m pip install .
```

Для разработки установите дополнительные инструменты:

```sh
python -m pip install -e ".[dev]"
```

## Работа во время выполнения

Создание отпечатка ChatGPT, proof-of-work и поддерживаемая проверка Turnstile
выполняются на чистом Python; Node.js и другие движки JavaScript не требуются.
Если необходимая проверка не поддерживается, ошибка возникает до отправки
сообщения. Профиль браузера и CDP используются только по явному запросу; импорт
пакета не запускает браузер и не обращается к сети. Gemini использует встроенный
HTTPS-транспорт.

## Возможности провайдеров

| Провайдер | Возможности веб-сеанса |
| --- | --- |
| ChatGPT | Чаты, проекты, поиск моделей, потоковая передача, поддерживаемые действия с сообщениями, источники и созданные файлы |
| Gemini | Чаты, блокноты, модели и уровень усилий, потоковая передача, поддерживаемые действия с сообщениями, источники и созданные медиафайлы |
| Claude | Чаты, рабочие пространства и проекты, поиск моделей, потоковая передача, источники и созданные файлы |
| Grok | Чаты, рабочие пространства, модели аккаунта, потоковая передача, повторная генерация ответа, инструкции и ресурсы проектов, опубликованные сводки рассуждений и источники веб-поиска |

Актуальный каталог аккаунта и заявленные возможности провайдера являются
источником истины. Пустой результат не означает, что операция не поддерживается.

## Быстрый старт

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
            chat_id="local-chat-id",
            prompt="Привет",
            model=model.id if model else None,
        )
        async for event in client.stream(request):
            if isinstance(event, (TextDelta, ReplaceText)):
                print(event.channel, event.text)


asyncio.run(main())
```

## Аккаунты и безопасность

Учётные данные хранит вызывающее приложение. Можно передать путь к файлу или
реализовать интерфейс `load`, `save`, `exists`, `delete` из
`providers.session.CredentialStore`. Импорт пакета не запускает браузер и не
обращается к сети; чтение профиля и операции CDP выполняются только по запросу.

У каждого аккаунта собственные адаптер, подключения и ограничения
параллельности. Метаданные файлов и источников передаются отдельно; для
скачивания используется аутентифицированный механизм соответствующего
провайдера. Отменённые или неоднозначные запросы не отправляются повторно
автоматически.
Перед интерактивной работой можно вызвать `await account.prepare()`. Каталоги
моделей кэшируются отдельно для каждого аккаунта на пять минут; для обновления
вызовите `await account.models(force=True)`.

## Поведение API

`ProviderClient` управляет аккаунтами, потоками и закрытием подключений;
`client.account(provider_id, account_id)` возвращает интерфейс `Backend` для
этого аккаунта. Он предоставляет проверку сеанса, каталог моделей, постраничные
списки чатов и проектов, историю чата, переименование и удаление, источники
проектов, загрузку файлов и разрешение ссылок на скачивание. Методы `describe()`
и `project_options()` показывают поддерживаемые операции. Пустой список не
означает, что функция не поддерживается.

Потоки передают части текста, события замены текста, идентификатор чата и
метаданные ответа. Источники и созданные файлы входят в метаданные; содержимое
файлов в поток не включается. `resolve_download` получает ссылку с учётными
данными аккаунта. Опубликованные источники обрабатываются отдельно от текста
анализа и обычных ссылок в ответе.

Редактирование и повтор выполняются по идентификатору сообщения провайдера, а не
по позиции в списке. Ветвление поддерживают ChatGPT и Gemini; в этой библиотеке
Claude его не предоставляет. Отменённые запросы и запросы с неоднозначным
результатом не отправляются повторно автоматически. `ProviderError.code` —
стабильный машинный код ошибки. Значение `False` у `message_submitted` означает,
что сообщение точно не дошло до провайдера; `None` указывает, что это нельзя
подтвердить.

Хранение данных и синхронизацию выполняет вызывающее приложение. `get_chat()`
возвращает доступную историю провайдера и не добавляет удалённую пагинацию, если
её нет в транспортном интерфейсе.

## Использование в SiopeChat

SiopeChat подключает этот репозиторий как Git submodule на фиксированном
коммите. При клонировании приложения используйте `--recurse-submodules`, а
после обновления выполните `git submodule update --init --recursive`, чтобы
получить commit провайдера, выбранный SiopeChat. Подробности приведены в
[README SiopeChat](https://github.com/TahsinCr/siopechat/blob/main/README-RU.md).

Wheel SiopeChat также содержит Python-пакет `providers`. Поэтому не
устанавливайте отдельный дистрибутив `webai-provider` в окружение SiopeChat:
файлы одного пакета будут принадлежать двум дистрибутивам.

Чтобы обновить отдельную копию provider, выполните `git pull --ff-only` в её
каталоге и повторно запустите `python -m pip install --upgrade .`. Не выполняйте
`git pull` внутри submodule SiopeChat: нужный commit провайдера выбирает
репозиторий приложения.

## Разработка

Зависимости для работы указаны в `pyproject.toml`, инструменты разработки
устанавливаются отдельно. Проверки:

```sh
ruff check .
ruff format --check .
python -m unittest discover -s tests -t . -p 'test_*.py'
python -m build --sdist --wheel
python -m twine check dist/*
```

## История изменений

В выпуске `v1.0-beta` изменения провайдеров собраны в файле
[CHANGELOG.md](CHANGELOG.md). Изменения после этого тега будут добавляться в
раздел **Unreleased**.

## Лицензия

Авторское право © TahsinCr. Пакет распространяется по лицензии
[GPL-3.0-only](LICENSE). Уведомление проекта находится в [COPYRIGHT](COPYRIGHT).

[version-shield]: https://img.shields.io/badge/%D0%B2%D0%B5%D1%80%D1%81%D0%B8%D1%8F-v1.0--beta-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/Лицензия-GPL--3.0--only-2F6F4E.svg?style=for-the-badge
