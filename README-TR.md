[![CI][ci-shield]][ci-url]
[![PyPI][pypi-shield]][pypi-url]
[![Python 3.10+][python-shield]][python-url]
[![GPL-3.0-only][license-shield]][license]

[English][readme-en] | **Türkçe** | [Русский][readme-ru]

# WebAI Provider

[Depo][repository] · [Değişiklik Günlüğü][changelog] · [Son Sürüm][releases] · [GPL-3.0 Lisansı][license]

ChatGPT, Gemini, Claude ve Grok web oturumları için bağımsız Python istemcileri. API; eşdeğer asenkron ve senkron arayüzler, tipli model başvuruları ve akış üzerinden çalışan mesaj işlemleri sunar.

> [Türkçe belgeler][docs] kurulum, sohbet akışı, model seçimi, proje yönetimi, [araçlar ve derin araştırma][tools-docs] ile API başvuru bölümlerini içerir.

> **Not:** Sağlayıcıların web siteleri ve özel web protokolleri önceden haber verilmeksizin değişebilir. Bu proje bağımsızdır ve sağlayıcılarla bağlantılı değildir.

## Kurulum

Python 3.10 veya üzeri gerekir.

```bash
pip install webai-provider
```

## Hızlı başlangıç

Kimlik bilgilerini işletim sisteminin varsayılan tarayıcısından alabilir veya desteklenen bir tarayıcıyı belirtebilirsiniz. Çerezleri kendiniz de sağlayabilirsiniz.

```python
import asyncio

from webaiprovider import WebAIAccount, AsyncChatGPTWebAIProvider, OutputDelta, WebAIProviderType


async def main() -> None:
    account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT, browser="firefox")

    async with AsyncChatGPTWebAIProvider(account) as provider:
        models = await provider.models.list()
        if not models:
            raise RuntimeError("No models are available for this account")

        chat = await provider.chats.create("Hızlı başlangıç")
        operation = chat.responses.create("Merhaba!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

`browser` verilmezse `WebAIAccount.from_browser()` varsayılan tarayıcıyı kullanır. Tarayıcı çerezlerine erişmek için tarayıcının kapatılması veya işletim sistemi anahtarlığının kilidinin açılması gerekebilir. Kimlik bilgilerini doğrudan vermek için `WebAIAccount.from_cookies(provider, cookies)` kullanın ve çerezleri kendi güvenli deponuzda saklayın.

Bloklayan kodlarda sağlayıcıya özel senkron sınıfı kullanabilirsiniz. Senkron sınıflar arka planda olay döngüsü başlatmadan doğrudan çağıran thread üzerinde bloklayan (`curl_cffi`) G/Ç ile çalışır:

```python
from webaiprovider import WebAIAccount, ChatGPTWebAIProvider, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)
with ChatGPTWebAIProvider(account) as provider:
    chat = provider.chats.create("Hızlı başlangıç")
    response = chat.responses.create("Merhaba!").start()
```

## Temel API

- `BaseWebAIProvider`, hesap ve sağlayıcı kimliğini taşır. Asenkron işlemler için `AsyncWebAIProvider`, bloklayan çağrılar için `WebAIProvider` kullanın. Sağlayıcıya özgü özellikler için `AsyncChatGPTWebAIProvider` gibi ilgili sınıfı seçin.
- `ChatGPTWebAIProvider`, `ClaudeWebAIProvider`, `GeminiWebAIProvider` ve `GrokWebAIProvider` sağlayıcıya özel özellikler sunar. ChatGPT projeleri varsayılan/genel bellek kapsamını kullanır; yalnızca proje belleği için `memory_share=False` verilir. Bir ChatGPT sohbetini projeye eklemek veya taşımak için `chat.move_to(project)`, projeden çıkarmak için `chat.move_to(None)` kullanın.
- Uzak koleksiyonlar sağlayıcı verisini istek üzerine okur. `list(limit=50)` sayfalamayı yönetir, `limit=0` uzak istek yapmaz, `limit=None` ulaşılabilen tüm sayfaları okur. Koleksiyonlar `get()`, `search()`, `find()`, `first()`, `last()` ve `single()` metotlarını paylaşır. `find(single=True)`, birden fazla eşleşmede `AmbiguousMatchError` yükseltir.
- Model sorguları, sağlayıcı oturumu açılırken yüklenen hesap kataloğunu kullanır. Boş katalog da oturum boyunca saklanır. Kataloğu yeniden yüklemek için `provider.models.refresh()` çağırın.
- Örtüşen `chat.responses.list()` çağrıları aynı geçmiş isteğini paylaşır. Bağımsız bir istek için `chat.responses.list(fresh=True)` kullanın.
- `chat.responses.create()`, `edit()` ve `retry()` mesaj işlemlerini hazırlar. Akış olayları için `operation.stream()`, tamamlanmış `Response` için `operation.wait()` kullanın. Yanıt; ilişkili kullanıcı mesajını, kaynakları, çıktıları ve canlı gönderim için süre bilgilerini içerir.
- Bir sohbeti son yanıtlarıyla birlikte tek geçmiş okumasında açmak için `chat, responses = await provider.chats.get_with_responses(chat_id, limit=25)` kullanın. Senkron sağlayıcıda `await` kaldırılır. Limit, dönen yanıt sayısını belirler.
- Projeye dosya veya metin eklemek için `project.attachments.create()`, ekleri görmek için `list()`, bir eki silmek için `delete()` kullanın.
- Sağlayıcılar yaşam döngüsü, kaynak kilitleri, kancalar ve olayları doğrudan `WebAIProvider` ve `AsyncWebAIProvider` üzerinde `flowevent` altyapısıyla yönetir. `provider.events`, yerel `AsyncEventBus` veya `EventBus` nesnesini sunar ve `EventEnvelope` olayları yayınlar; olay adına göre `EventFilter` ile filtreleyebilirsiniz. Sağlayıcı ve mesaj durumlarını kütüphane kendi içinde yönetir. `provider.hooks` (`OperationHooks` / `AsyncOperationHooks`) kancaları, `provider.lifecycle_state` ise `LifecycleState` değerini sunar.
- Her başarılı uzak okumada kayıt kimliklerini topluca almak için `collection.read.completed` olayına abone olun. `collection.changed`, doğrulanan ekleme, güncelleme ve silmeleri bildirir. İki olayın da `event.metadata["payload"]` alanında bir `items` listesi bulunur. Olay zarfı hesabı ve iç içe koleksiyonlarda üst kaydın kimliğini taşır. Mesaj düzenlemeleri ve yeniden denemeler sohbet kapsamında bildirilir. Olay verisi mesaj metni, dosya içeriği veya oturum bilgisi içermez.
- Proje, ek, yeniden deneme, düzenleme, dallandırma ve sohbet taşıma özellikleri sağlayıcıya göre değişir. Sağlayıcıya özel sohbet sınıfları yalnızca desteklenen işlemleri sunar; desteklenmeyen işlemler `FeatureUnavailable` gibi tipli `WebAIProviderError` alt sınıflarıyla bildirilir.
- İstek bazlı araç sözleşmeleri (`WebSearchTool`, `ImageGenerationTool`, `DeepResearchTool` ve Gemini için `GeminiDeepResearchTool`), `ask()`, `stream()` ve `responses.create()` çağrılarına `tool=` ve `tool_timeout=` ile verilir. Derin Araştırma akışı `response.research`, yaşam döngüsü metotları (`start_research()`, `pause_research()`, `cancel_research()`, `wait_research()`) ve `ResearchPlanReady` / `ResearchProgressEvent` akış olaylarını sunar; `Artifact` ise `read_bytes()`, `read_text()` ve `save()` yardımcılarını sağlar. Ayrıntılar için [Araçlar ve Derin Araştırma][tools-docs] rehberine bakın.

Paket, tip denetimli kullanım için model başvuruları ve efor seçenekleri sunar. Hesaptan keşfedilen modeller, o hesap için yetkili kaynaktır; aboneliğe ve sağlayıcı değişikliklerine göre farklılık gösterebilir.

## Alt seviye transport API’si

Tüm sağlayıcılar, kendi origin’iyle sınırlı `send`, JSON `request` ve parçalı `stream` işlemleri için `provider.transport` sunar. Transport’un yaşam döngüsünü sağlayıcı yönetir; senkron ve asenkron çağrılar her istekte yeni bir bağlantı havuzu veya olay döngüsü açmaz.

```python
from webaiprovider import WebAIAccount, AsyncClaudeWebAIProvider, WebAIProviderType
from webaiprovider.transport import AsyncHttpSession, TransportConfig

account = WebAIAccount.from_browser(WebAIProviderType.CLAUDE)
transport = AsyncHttpSession(
    "https://claude.ai",
    account.store(),
    "sessionKey",
    config=TransportConfig(timeout=30, max_stream_bytes=64 * 1024 * 1024),
)

async with AsyncClaudeWebAIProvider(account, transport=transport) as provider:
    response = await provider.transport.send("GET", "/api/bootstrap")
    endpoint, payload = "/api/path", {}  # Sağlayıcının desteklediği bir uç nokta kullanın.
    async with provider.transport.stream("POST", endpoint, json=payload) as stream:
        async for chunk in stream.aiter_bytes():
            ...
```

`send()` bir `TransportResponse` döndürür. `request()` JSON yanıtını çözer; yanıt boşsa `None` verir. Akış bağlamı, yanıtı bağlam kapatılana kadar açık tutar. Erken dururken yanıtın ve varsa iş parçacığının bırakılması için bağlamı kapatın. Senkron sağlayıcıda `with` ve `iter_bytes()` kullanılır.

| Tür | Kullanım |
| --- | --- |
| `AsyncTransport` / `Transport` | Asenkron ve senkron uzantı sözleşmeleri |
| `AsyncHTTPTransport` | Akış bağlamı sunan asenkron HTTP istemcisi; varsayılanı curl-cffi’dir |
| `HTTPTransport` | Senkron curl_cffi istemcisi |
| `AsyncHttpSession` | Paylaşılan asenkron transport üzerinden hesap çerezlerini saklama |
| `HttpSession` | Hesap çerezlerini kaydeden, çağıran thread’de çalışan senkron HTTP oturumu |

Claude ve Grok `AsyncHttpSession` / `HttpSession`, Gemini ve ChatGPT ise web isteklerinde `AsyncHTTPTransport` / `HTTPTransport` kullanır. Asenkron ve senkron katmanlar aynı origin, çerez ve güvenlik kurallarını paylaşır; senkron sağlayıcılar arka plan olay döngüsü açmadan doğrudan çağıran thread üzerinde çalışır. Çakışan sohbet ve proje değişiklikleri kaynak kilitleriyle sıralanır.

Özel bir uygulama, `AsyncTransport` veya `Transport` üzerinde `send`, `stream` ve `close` işlemlerini sağlayabilir veya yerleşik taşımalardan birini `client_factory` ile yapılandırabilir. Başlıklar, parametreler, JSON ve veri gibi standart HTTP seçenekleri seçili arka uca iletilir. Multipart biçimleri ve arka uca özgü seçenekler o arka ucun uzantı API’sine aittir.

`TransportConfig` zaman aşımını ve tamponlanmış/akış yanıtı boyut sınırlarını belirler; varsayılan tampon sınırı 64 MiB’dir. Gerektiğinde akış sınırını ayrıca belirleyin. Uç nokta çağrıları zaman aşımını geçersiz kılabilir; gelişmiş kod HTTP hata yanıtını incelemek için `check_status=False` seçebilir. Bu, origin ve yönlendirme denetimlerini kapatmaz. Yollar sabit bir origin’e göredir. Mutlak veya protokole göreli URL’ler, yol geçişi, çakışan `Host` başlıkları ve yönlendirmeler reddedilir. Varsayılan başlıklar eşleme veya `(method, path)` alan bir işlev olabilir; çağrı başlıkları büyük/küçük harf duyarsız biçimde önceliklidir. Kimlik bilgisi yenileme gibi bir istekte kalıtılan başlıkları göndermemek için `inherit_headers=False` kullanın. İmzalı origin dışı yüklemeler, hesap başlıklarını taşımayan ayrı bir origin-sınırlı transport kullanır.

Mutasyonlar kendiliğinden yeniden gönderilmez. Bekleme süreleri transport/hesap kapsamında tutulur; bir hesabın sınırlandırılması aynı sağlayıcıdaki diğer hesapları engellemez. Sağlanan transport, sağlayıcının tam origin’ini kullanmalı ve aynı hesabı doğrulamalıdır; kapanırken sağlayıcı transport’u kapatır. `client_getter` ile ödünç alınan istemciyi sahibi kapatır. Üst ve alt seviye işlemler çerezleri ve bağlantı havuzunu yeniden kullanır.

Frame çözümleme HTTP’den ayrıdır. `SSEFrameDecoder`, `data`, `event`, `id` ve sayısal `retry` alanlarını korur; parçalanmış UTF-8’i, CR/LF/CRLF satır sonlarını, çok satırlı veriyi, yorumları ve sınırlı satır sayılarını işler. Senkron ve asenkron bayt yineleyici yardımcıları da sunar. `JSPBFrameDecoder`, sınırlandırılmış uzunluk önekli JSON frame’lerini çözer; `prefix`, `max_frame_units` ve `length_includes_separator` seçeneklerini alır. Gemini anti-XSSI önekini ve ayırıcı ayarlarını sağlar. Her çıktı tam bir JSON frame’idir. Son parçada eksik frame’leri denetlemek için `final=True` verin; tamamlanmamış JSPB frame’i `FrameDecodeError` üretir. Çözümlenen frame’leri sağlayıcı kodu yorumlar ve tamamlanma davranışını yönetir.

Sağlayıcı geçmişi, RPC yükleri, devam kimlikleri ve yanıt yorumlama her sağlayıcının kendi paketinde kalır.

Mesajları `chat.responses` üzerinden gönderin. Ağ isteklerini ve yeniden deneme davranışını sağlayıcı yönetir.

## Güvenlik ve çalışma biçimi

- Tarayıcı kimlik bilgileri yerel olarak okunur. Kütüphane bunları SiopeChat sunucusuna göndermez.
- Çerezleri ve kimlik bilgisi dosyalarını koruyun; günlüklerde, hata bildirimlerinde veya kaynak kontrolünde paylaşmayın.
- İndirme işlemleri yalnızca sağlayıcının verdiği `DownloadSource` değerlerini kabul eder; herkese açık ağ hedeflerini doğrular, çözümlenen IP adresini sabitler ve yönlendirmeleri izlemez.
- Sağlayıcı hataları İngilizce mesajlı, tipli istisnalar olarak fırlatılır. Açık bir sağlayıcı oturumu olmadan mesaj göndermek sahte yanıt üretmek yerine açıkça hata verir.

## Geliştirme

```bash
ruff check .
ruff format --check .
python run_tests.py
python -m build --sdist --wheel
twine check dist/*
```

Çevrimdışı performans ölçümleri için [benchmark kılavuzuna][benchmarks-guide] bakın.

## Tanılama

Gönderim ölçümü isteğe bağlıdır; mesaj ve kimlik bilgisi içeriklerini kaydetmeden adlandırılmış süreleri ve sayaçları tutar. Beklenen işlemin çevresinde etkinleştirin; kendiliğinden çalışmaz.

```python
from webaiprovider.core.timing import SendTiming, use_send_timing

timing = SendTiming(provider.id)
with use_send_timing(timing):
    response = await provider.ask("Hello")
durations_ms, counters = timing.snapshot()
```

`patch_project(project_id, name=...)` yalnızca verilen alanları değiştirir; talimatları temizlemek için `instructions=""` kullanın. Proje değişiklikleri yazmadan önce kilit altında sunucunun güncel durumunu okur. İşlem akışları tekrar okunabilmek için tüm olay geçmişini saklar; bellekte tutulan işlemlerin bellek kullanımı olay sayısıyla artar.

## Bağımlılıklar

- [flowevent][flowevent-repo] — Süreç içi senkron ve asenkron olay veriyolu
- [flowpipeline][flowpipeline-repo] — Aşamalı istek hazırlama boru hatları
- [flowcoord][flowcoord-repo] — Tekil uçuş (single-flight) ve yürütme eşgüdüm yapıları
- [curl-cffi][curl-cffi-repo] — Tarayıcı taklitli HTTPS ve akış katmanı
- [browser-cookie3][browser-cookie3-repo] — Yerel tarayıcı çerez okuma
- [orjson][orjson-repo] — Hızlı JSON serileştirme ve çözme
- [lz4][lz4-repo] — Tarayıcı oturum açma/çözümleme sıkıştırması

GPL-3.0-only lisanslıdır. Ayrıntılar için [LICENSE][license] ve [COPYRIGHT][copyright] dosyalarına bakın.

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/webai-provider/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/webai-provider?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-GPL--3.0--only-blue.svg?style=for-the-badge
[ci-url]: https://github.com/TahsinCr/webai-provider/actions/workflows/ci.yml
[pypi-url]: https://pypi.org/project/webai-provider/
[python-url]: https://www.python.org/downloads/
[readme-en]: README.md
[readme-ru]: README-RU.md
[docs]: docs/tr/README.md
[tools-docs]: docs/tr/tools-and-deep-research.md
[benchmarks-guide]: benchmarks/README.md
[repository]: https://github.com/TahsinCr/webai-provider
[changelog]: CHANGELOG.md
[releases]: https://github.com/TahsinCr/webai-provider/releases/latest
[license]: LICENSE
[copyright]: COPYRIGHT
[flowevent-repo]: https://github.com/TahsinCr/python-flowevent
[flowpipeline-repo]: https://github.com/TahsinCr/python-flowpipeline
[flowcoord-repo]: https://github.com/TahsinCr/python-flowcoord
[curl-cffi-repo]: https://github.com/lexiforest/curl_cffi
[browser-cookie3-repo]: https://github.com/borisbabic/browser_cookie3
[orjson-repo]: https://github.com/ijl/orjson
[lz4-repo]: https://github.com/python-lz4/python-lz4
