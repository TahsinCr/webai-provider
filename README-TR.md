# webai-provider

ChatGPT, Gemini, Claude ve Grok web oturumları için bağımsız Python istemcileri. v2 API; eşdeğer asenkron ve senkron arayüzler, tipli model başvuruları ve akış üzerinden çalışan mesaj işlemleri sunar.

> **Beta:** Sağlayıcıların web siteleri ve özel web protokolleri önceden haber verilmeksizin değişebilir. Bu proje bağımsızdır ve sağlayıcılarla bağlantılı değildir.

## Kurulum

Python 3.10 veya üzeri gerekir.

Burada anlatılan v2 API geliştirme aşamasındadır ve yayımlanmış `v1.3-beta` paketinde bulunmaz. Bu API'yi içeren kaynak kodunu indirdikten sonra kurun:

```bash
python -m pip install -e '.[dev]'
```

## Hızlı başlangıç

Kimlik bilgilerini işletim sisteminin varsayılan tarayıcısından alabilir veya desteklenen bir tarayıcıyı belirtebilirsiniz. Çerezleri kendiniz de sağlayabilirsiniz.

```python
import asyncio

from webaiprovider import Account, AsyncChatGPTProvider, OutputDelta, ProviderType


async def main() -> None:
    account = Account.from_browser(ProviderType.CHATGPT, browser="firefox")

    async with AsyncChatGPTProvider(account) as provider:
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

`browser` verilmezse `Account.from_browser()` varsayılan tarayıcıyı kullanır. Tarayıcı çerezlerine erişmek için tarayıcının kapatılması veya işletim sistemi anahtarlığının kilidinin açılması gerekebilir. Kimlik bilgilerini doğrudan vermek için `Account.from_cookies(provider, cookies)` kullanın ve çerezleri kendi güvenli deponuzda saklayın.

Bloklayan kodlarda sağlayıcıya özel senkron sınıfı kullanabilirsiniz. Her sağlayıcı, çağıran thread’lerin yürüttüğü tek bir olay döngüsünü kullanır:

```python
from webaiprovider import Account, ChatGPTProvider, ProviderType

account = Account.from_browser(ProviderType.CHATGPT)
with ChatGPTProvider(account) as provider:
    chat = provider.chats.create("Hızlı başlangıç")
    response = chat.responses.create("Merhaba!").start()
```

## Temel API

- `BaseProvider`, hesap ve sağlayıcı kimliğini taşır. Asenkron işlemler için `AsyncProvider`, bloklayan çağrılar için `Provider` kullanın. Sağlayıcıya özgü özellikler için `AsyncChatGPTProvider` gibi ilgili sınıfı seçin.
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider` ve `GrokProvider` sağlayıcıya özel özellikler sunar. ChatGPT projeleri varsayılan/genel bellek kapsamını kullanır; yalnızca proje belleği için `memory_share=False` verilir. Bir ChatGPT sohbetini projeye eklemek veya taşımak için `chat.move_to(project)`, projeden çıkarmak için `chat.move_to(None)` kullanın.
- Uzak koleksiyonlar sağlayıcı verisini istek üzerine okur. `list(limit=50)` sayfalamayı yönetir, `limit=0` uzak istek yapmaz, `limit=None` ulaşılabilen tüm sayfaları okur. Koleksiyonlar `get()`, `search()`, `find()`, `first()`, `last()` ve `single()` metotlarını paylaşır. `find(single=True)`, birden fazla eşleşmede `AmbiguousMatchError` yükseltir.
- Model sorguları, sağlayıcı oturumu açılırken yüklenen hesap kataloğunu kullanır. Boş katalog da oturum boyunca saklanır. Kataloğu yeniden yüklemek için `provider.models.refresh()` çağırın.
- Örtüşen `chat.responses.list()` çağrıları aynı geçmiş isteğini paylaşır. Bağımsız bir istek için `chat.responses.list(fresh=True)` kullanın.
- `chat.responses.create()`, `edit()` ve `retry()` mesaj işlemlerini hazırlar. Akış olayları için `operation.stream()`, tamamlanmış `Response` için `operation.wait()` kullanın. Yanıt; ilişkili kullanıcı mesajını, kaynakları, çıktıları ve canlı gönderim için süre bilgilerini içerir.
- Bir sohbeti son yanıtlarıyla birlikte tek geçmiş okumasında açmak için `chat, responses = await provider.chats.get_with_responses(chat_id, limit=25)` kullanın. Senkron sağlayıcıda `await` kaldırılır. Limit, dönen yanıt sayısını belirler.
- Projeye dosya veya metin eklemek için `project.attachments.create()`, ekleri görmek için `list()`, bir eki silmek için `delete()` kullanın.
- Sağlayıcılar yaşam döngüsü, kaynak kilitleri, kancalar ve olayları doğrudan `Provider` ve `AsyncProvider` üzerinde `flowevent` altyapısıyla yönetir. `provider.events`, yerel `AsyncEventBus` veya `EventBus` nesnesini sunar ve `EventEnvelope` olayları yayınlar; olay adına göre `EventFilter` ile filtreleyebilirsiniz. Yaşam döngüsü ve mesaj durumları doğrudan `flowevent.StateMachine` ile yönetilir. `provider.hooks` (`OperationHooks` / `AsyncOperationHooks`) kancaları, `provider.lifecycle_state` ise `LifecycleState` değerini sunar.
- Her başarılı uzak okumada kayıt kimliklerini topluca almak için `collection.read.completed` olayına abone olun. `collection.changed`, doğrulanan ekleme, güncelleme ve silmeleri bildirir. İki olayın da `event.metadata["payload"]` alanında bir `items` listesi bulunur. Olay zarfı hesabı ve iç içe koleksiyonlarda üst kaydın kimliğini taşır. Mesaj düzenlemeleri ve yeniden denemeler sohbet kapsamında bildirilir. Olay verisi mesaj metni, dosya içeriği veya oturum bilgisi içermez.
- Proje, ek, yeniden deneme, düzenleme, dallandırma ve sohbet taşıma özellikleri sağlayıcıya göre değişir. Sağlayıcıya özel sohbet sınıfları yalnızca desteklenen işlemleri sunar; desteklenmeyen işlemler `FeatureUnavailable` gibi tipli `ProviderError` alt sınıflarıyla bildirilir.

Paket, tip denetimli kullanım için model başvuruları ve efor seçenekleri sunar. Hesaptan keşfedilen modeller, o hesap için yetkili kaynaktır; aboneliğe ve sağlayıcı değişikliklerine göre farklılık gösterebilir.

## Alt seviye transport API’si

Tüm sağlayıcılar, kendi origin’iyle sınırlı `send`, JSON `request` ve parçalı `stream` işlemleri için `provider.transport` sunar. Transport’un yaşam döngüsünü sağlayıcı yönetir; senkron ve asenkron çağrılar her istekte yeni bir bağlantı havuzu veya olay döngüsü açmaz.

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

Claude ve Grok `AsyncHttpSession` kullanır. Gemini ve ChatGPT web isteklerini `AsyncHTTPTransport` üzerinden gönderir. Senkron API, asenkron HTTP oturumunu ve tek bir olay döngüsünü paylaşır. Bir çağıran thread olay döngüsünü yürütürken bağımsız istekler örtüşebilir. Bir işlem başka bir çağrının thread’inde yürüyebilir ve işlem sürerken olay döngüsünü yürüten thread değişebilir. Çakışan sohbet ve proje değişiklikleri kaynak kilitleriyle sıralanır. Bir senkron çağrının olayları çağıran thread’de sırasıyla teslim edilir. Örtüşen çağrıların olayları farklı sırada teslim edilebilir. Sıra numarası olayın üretim sırasını gösterir.

Özel bir uygulama, `AsyncTransport` üzerinde `send`, `stream` ve `close` işlemlerini sağlayabilir veya yerleşik taşımalardan birini `client_factory` ile yapılandırabilir. Başlıklar, parametreler, JSON ve veri gibi standart HTTP seçenekleri seçili arka uca iletilir. Multipart biçimleri ve arka uca özgü seçenekler o arka ucun uzantı API’sine aittir.

`TransportConfig` zaman aşımını ve tamponlanmış/akış yanıtı boyut sınırlarını belirler; varsayılan tampon sınırı 64 MiB’dir. Gerektiğinde akış sınırını ayrıca belirleyin. Uç nokta çağrıları zaman aşımını geçersiz kılabilir; gelişmiş kod HTTP hata yanıtını incelemek için `check_status=False` seçebilir. Bu, origin ve yönlendirme denetimlerini kapatmaz. Yollar sabit bir origin’e göredir. Mutlak veya protokole göreli URL’ler, yol geçişi, çakışan `Host` başlıkları ve yönlendirmeler reddedilir. Varsayılan başlıklar eşleme veya `(method, path)` alan bir işlev olabilir; çağrı başlıkları büyük/küçük harf duyarsız biçimde önceliklidir. Kimlik bilgisi yenileme gibi bir istekte kalıtılan başlıkları göndermemek için `inherit_headers=False` kullanın. İmzalı origin dışı yüklemeler, hesap başlıklarını taşımayan ayrı bir origin-sınırlı transport kullanır.

Mutasyonlar kendiliğinden yeniden gönderilmez. Bekleme süreleri transport/hesap kapsamında tutulur; bir hesabın sınırlandırılması aynı sağlayıcıdaki diğer hesapları engellemez. Sağlanan transport, sağlayıcının tam origin’ini kullanmalı ve aynı hesabı doğrulamalıdır; kapanırken sağlayıcı transport’u kapatır. `client_getter` ile ödünç alınan istemciyi sahibi kapatır. Üst ve alt seviye işlemler çerezleri ve bağlantı havuzunu yeniden kullanır.

Frame çözümleme HTTP’den ayrıdır. `SSEFrameDecoder`, `data`, `event`, `id` ve sayısal `retry` alanlarını korur; parçalanmış UTF-8’i, CR/LF/CRLF satır sonlarını, çok satırlı veriyi, yorumları ve sınırlı satır sayılarını işler. Senkron ve asenkron bayt yineleyici yardımcıları da sunar. `JSPBFrameDecoder`, sınırlandırılmış uzunluk önekli JSON frame’lerini çözer; `prefix`, `max_frame_units` ve `length_includes_separator` seçeneklerini alır. Gemini anti-XSSI önekini ve ayırıcı ayarlarını sağlar. Her çıktı tam bir JSON frame’idir. Son parçada eksik frame’leri denetlemek için `final=True` verin; tamamlanmamış JSPB frame’i `FrameDecodeError` üretir. Çözümlenen frame’leri sağlayıcı kodu yorumlar ve tamamlanma davranışını yönetir.

Kaynak ve dosya normalizasyonu `webaiprovider.normalizers` içindedir. Sağlayıcı geçmişi, RPC yükleri, devam kimlikleri ve yanıt yorumlama ise her sağlayıcının kendi paketinde kalır.

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
python -m unittest discover -s . -t . -q
python -m build --sdist --wheel
twine check dist/*
```

Çevrimdışı performans ölçümleri için [benchmark kılavuzuna](benchmarks/README.md) bakın.

## Tanılama

Gönderim ölçümü isteğe bağlıdır; mesaj ve kimlik bilgisi içeriklerini kaydetmeden adlandırılmış süreleri ve sayaçları tutar. Beklenen işlemin çevresinde etkinleştirin; kendiliğinden çalışmaz.

```python
from webaiprovider.utils.timing import SendTiming, use_send_timing

timing = SendTiming(provider.id)
with use_send_timing(timing):
    response = await provider.ask("Hello")
durations_ms, counters = timing.snapshot()
```

`patch_project(project_id, name=...)` yalnızca verilen alanları değiştirir; talimatları temizlemek için `instructions=""` kullanın. Proje değişiklikleri yazmadan önce kilit altında sunucunun güncel durumunu okur. İşlem akışları tekrar okunabilmek için tüm olay geçmişini saklar; bellekte tutulan işlemlerin bellek kullanımı olay sayısıyla artar.

## Bağlantılar

- [Değişiklik günlüğü](CHANGELOG.md)
- [Hata ve istek bildirimi](https://github.com/TahsinCr/webai-provider/issues)
- [Depo](https://github.com/TahsinCr/webai-provider)
- [SiopeChat](https://github.com/TahsinCr/chatgpt-web)

GPL-3.0-only lisanslıdır. Ayrıntılar için [LICENSE](LICENSE) ve [COPYRIGHT](COPYRIGHT) dosyalarına bakın.
