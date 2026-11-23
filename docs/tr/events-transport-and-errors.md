[← Önceki: 04 · Sağlayıcı Rehberleri](provider-guides.md) · [Belge Haritası](README.md) · [Sonraki: 06 · API Başvuru Kartı →](api-reference.md)

# 05 · Olaylar, Düşük Seviye Transport, Frame Çözücüler ve Hata Yönetimi

Bu bölüm, doğrudan `EventBus` olaylarına abone olmak, `provider.transport` üzerinden özel HTTP istekleri yapmak, SSE/JSPB akışlarını ayrıştırmak veya sağlam hata kurtarma akışları kurmak isteyen geliştiriciler içindir.

---

## Konu Dizini

- [Hızlı Örnek](#hızlı-örnek)
- [Olay Veriyolu (`EventBus` / `AsyncEventBus`)](#olay-veriyolu-eventbus--asynceventbus)
  - [Abone Olma ve `EventFilter`](#abone-olma-ve-eventfilter)
  - [Yayımlanan Olaylar](#yayımlanan-olaylar)
- [Düşük Seviye HTTP Transport (`provider.transport`)](#düşük-seviye-http-transport-providertransport)
  - [`TransportConfig` ve Transport Sınıfları](#transportconfig-ve-transport-sınıfları)
  - [`send()`, `request()`, `stream()` ve Güvenlik Kuralları](#send-request-stream-ve-güvenlik-kuralları)
- [Frame Çözücüler (`SSEFrameDecoder` ve `JSPBFrameDecoder`)](#frame-çözücüler-sseframedecoder-ve-jspbframedecoder)
- [Hata Hiyerarşisi ve Güvenli Kurtarma](#hata-hiyerarşisi-ve-güvenli-kurtarma)
- [Tanılama ve Performans](#tanılama-ve-performans)

---

## Hızlı Örnek

```python
from webaiprovider import (
    WebAIAccount,
    AccountPaused,
    ClaudeWebAIProvider,
    EventEnvelope,
    EventFilter,
    WebAIProviderError,
    WebAIProviderType,
    RateLimited,
)
from webaiprovider.core.timing import SendTiming, use_send_timing
from webaiprovider.transport import HttpSession, TransportConfig

account = WebAIAccount.from_browser(WebAIProviderType.CLAUDE)
transport = HttpSession(
    origin="https://claude.ai",
    credential_file=account.store(),
    required_cookie="sessionKey",
    config=TransportConfig(timeout=(10.0, 30.0), max_stream_bytes=64 * 1024 * 1024),
)

try:
    with ClaudeWebAIProvider(account, transport=transport) as provider:
        provider.events.subscribe(
            lambda env: print("Olay:", env.event, env.metadata.get("payload")),
            event_filter=EventFilter(
                event_type=EventEnvelope,
                predicate=lambda env: env.event in ("collection.changed", "chat.send.completed"),
            ),
        )

        timing = SendTiming(provider.id)
        with use_send_timing(timing):
            response = provider.ask("Kısa bir özet yaz.")
        durations_ms, counters = timing.snapshot()
        print("Süreler (ms):", durations_ms)
except RateLimited as err:
    print(f"Hız sınırı (bekleme={err.retry_after}s, ulaştı={err.message_submitted})")
except AccountPaused:
    print("Oturum geçersiz; yeniden giriş gerekiyor.")
except WebAIProviderError as err:
    print(f"Hata [{err.code}]: {err}")
```

[Yukarı dön](#konu-dizini)

---

## Olay Veriyolu (`EventBus` / `AsyncEventBus`)

### Abone Olma ve `EventFilter`

Her sağlayıcı, dinleyici hatalarını birbirinden izole eden (`ErrorPolicy.ISOLATE`) bir `provider.events` veriyoluna sahiptir. Sağlayıcının genel yaşam döngüsü durumu ise `provider.lifecycle_state` (`LifecycleState`: `CREATED`, `OPENING`, `OPEN`, `OPEN_FAILED`, `CLOSING`, `CLOSED`, `CLOSE_FAILED`) üzerinden okunur.

`provider.events.subscribe(callback, *, event_filter=None, event_type=None, predicate=None, ...)` çağrısında filtre parametreleri keyword-only verilir. Dinleyiciye gelen `EventEnvelope` nesnesi `event`, `correlation_id`, `source` ve `metadata` (`account_id`, `resource_id`, `sequence`, `payload`) alanlarını taşır.

Olay veriyolu gizlilik ilkesi gereği asla mesaj metni, dosya gövdesi veya çerez/token bilgisi taşımaz; yalnızca kimlikler, başlıklar, eylem adı ve zaman damgaları yayımlanır.

```python
from webaiprovider import EventEnvelope, EventFilter

sub = provider.events.subscribe(
    lambda env: print(env.metadata["sequence"], env.event, env.metadata["payload"]),
    event_filter=EventFilter(
        event_type=EventEnvelope,
        predicate=lambda env: env.event in ("collection.read.completed", "collection.changed"),
    ),
)
sub.unsubscribe()
```

### Yayımlanan Olaylar

| Olay Adı | Ne Zaman Yayımlanır? | `metadata["payload"]` |
| --- | --- | --- |
| `provider.opening` / `opened` / `open_failed` | Oturum açılış aşamaları | `open_failed`: `{"error_type"}` |
| `provider.closing` / `closed` / `close_failed` | Oturum kapanış aşamaları | `close_failed`: `{"error_type"}` |
| `collection.read.completed` | Başarılı uzak okuma başına 1 kez | `{"collection", "operation", "items", "complete", "include_projects", "started_ns"}` |
| `collection.changed` | Onaylı create / update / delete sonrası | `{"collection", "action", "items", "confirmed_ns"}` |
| `chat.prepared` / `created` / `branched` / `moved` | Sohbet yaşam döngüsü olayları | İlgili başlık veya hedef ID bilgisi |
| `chat.send.started` / `completed` / `cancelled` / `failed` | Mesaj gönderim aşamaları | `completed`: `{"message_id"}`; `failed`: `{"error_type"}` |
| `project.created` / `updated` / `deleted` | Proje mutasyonları | `{"title"}` *(created/updated)* |
| `project.attachment.added` / `removed` | Proje eki ekleme/silme | `{"attachment_id", "kind"}` / `{"attachment_id"}` |

[Yukarı dön](#konu-dizini)

---

## Düşük Seviye HTTP Transport (`provider.transport`)

Yüksek seviye API'de olmayan bir uç noktaya istek atmak veya bağlantı sınırlarını özelleştirmek için açık bir sağlayıcı üzerinde `provider.transport` kullanılabilir.

### `TransportConfig` ve Transport Sınıfları

`TransportConfig(timeout=(10.0, 50.0), max_response_bytes=64 * 1024 * 1024, max_stream_bytes=None)` yapılandırmasında `timeout` tek pozitif sayı veya `(connect, read)` demeti olabilir. Tamponlu yanıtlar varsayılan olarak 64 MiB ile sınırlıdır; uzun akışlar için `max_stream_bytes` ayarlanabilir.

| Sınıf | Çalışma Şekli | Parametreler |
| --- | --- | --- |
| `HTTPTransport` | Bloklayan `curl_cffi.Session` | `(origin, *, client_factory=None, client_getter=None, config=None, headers=None)` |
| `AsyncHTTPTransport` | Asenkron `curl_cffi.AsyncSession` | `(origin, *, client_factory=None, client_getter=None, after_response=None, config=None, headers=None)` |
| `HttpSession` | Bloklayan + kalıcı çerez dosyası | `(origin, credential_file, required_cookie, *, client_factory=None, config=None)` |
| `AsyncHttpSession` | Asenkron + kalıcı çerez dosyası | `(origin, credential_file, required_cookie, *, client_factory=None, config=None)` |

`client_factory(timeout=...)` ile üretilen istemci transport kapatılırken kapatılır; `client_getter()` ile ödünç verilen istemcinin kapatma sorumluluğu ise çağıran tarafta kalır.

### `send()`, `request()`, `stream()` ve Güvenlik Kuralları

```python
# Ham yanıt (status_code, headers, content, text, json())
resp = provider.transport.send("GET", "/api/bootstrap", timeout=15.0, check_status=False)

# Doğrudan JSON çözme (boş yanıtta None döner)
data = provider.transport.request("GET", "/api/organizations", params={"limit": 10})

# Akış (her zaman `with` / `async with` bloğunda kullanılmalıdır)
with provider.transport.stream("POST", "/api/conversation", json=payload) as s:
    for chunk in s.iter_bytes():
        process(chunk)
```

Çağrılarda `params`, `json`, `data`, `headers`, `timeout`, `check_status=False` (4xx/5xx hatalarını incelemek için) ve `inherit_headers=False` (varsayılan oturum başlıklarını devralmamak için) kullanılabilir.

Güvenlik sınırları gereği tüm `path` değerleri `/` ile başlamalıdır; tam URL, `//` öneki, `..`, `\`, `#fragment`, farklı `Host` başlığı ve `allow_redirects=True` reddedilir (`ValueError`). DNS çözümlemesi origin bazlı sabitlenir. Sağlayıcıya dışarıdan verilen `transport`, oturumun origin'i ile birebir eşleşmelidir ve `provider.close()` çağrıldığında kapatılır.

[Yukarı dön](#konu-dizini)

---

## Frame Çözücüler (`SSEFrameDecoder` ve `JSPBFrameDecoder`)

`webaiprovider.transport.frames` altında HTTP katmanından bağımsız iki artımlı çözücü bulunur:

- **`SSEFrameDecoder(*, max_event_bytes=2_000_000, max_data_lines=65_536, utf8_errors="replace")`:**
  Parçalı UTF-8 baytlarını, `CR`/`LF`/`CRLF` satır sonlarını ve çok satırlı `data:` alanlarını `SSEFrame(data, event, id, retry)` nesnelerine çevirir. Senkron `decode(chunks)` ve asenkron `decode_async(chunks)` yardımcılarını sunar.

```python
from webaiprovider.transport.frames import SSEFrameDecoder

decoder = SSEFrameDecoder(max_event_bytes=4 * 1024 * 1024)
with provider.transport.stream("POST", "/backend-api/conversation", json=body) as s:
    for frame in decoder.decode(s.iter_bytes()):
        if frame.data == "[DONE]":
            break
        print(frame.event, len(frame.data))
```

- **`JSPBFrameDecoder(*, prefix="", max_frame_units=8 * 1024 * 1024, length_includes_separator=False)`:**
  Uzunluk önekli (UTF-16 kod birimi sayımlı) JSON/JSPB akışlarını `feed(data, final=False)` ile çözer; `final=True` anında yarım çerçeve kalırsa `FrameDecodeError` fırlatır.

[Yukarı dön](#konu-dizini)

---

## Hata Hiyerarşisi ve Güvenli Kurtarma

Aşağıdaki kütüphane istisnaları `WebAIProviderError` hiyerarşisindedir ve programatik kararlar için `err.code` alanını sunar. Hata mesajını kullanıcıya gösterebilir, uygulama davranışını istisna türü ve alanlarına göre belirleyebilirsin.

```text
WebAIProviderError ("provider_error")
├── RequestNotSentError ("request_not_sent", message_submitted=False)
├── StreamError ("stream_error") -> FrameDecodeError ("frame_decode_error")
├── AuthenticationError ("authentication_error") -> AccountPaused ("account_paused")
├── FeatureUnavailable ("feature_unavailable")      [.capability, .reason]
├── RateLimited ("rate_limited")                    [.retry_after, .message_submitted]
├── CompatibilityError / RemoteNotFound / AmbiguousMatchError
├── UnsupportedOperationError / OperationCancelledError / ConfigurationError / ContentError
└── ProfileError ("profile_error")
    └── ProfileNotFoundError, ProfileSessionNotFoundError, ProfileDependencyError,
        ProfilePermissionError, ProfileDatabaseError, ProfileKeyringError,
        ProfileDecryptionError, ProfileTimeoutError, ProfileInvalidResultError
```

> **Otomatik Yeniden Deneme Kuralı:** Kütüphane `HTTP 429` (`RateLimited`) yanıtlarında ve `POST` tabanlı mesaj gönderimi / mutasyon isteklerinde **asla otomatik yeniden deneme yapmaz**; transport katmanı yalnızca idempotent `GET` okumalarında sınırlı ağ hatası tekrar denemesi uygular.

Mutasyonları körü körüne tekrarlamak yerine `RequestNotSentError` veya `RateLimited.message_submitted is False` kontrolü yaparak yalnızca sunucuya ulaşmadığı kesin olan istekleri yeniden denemen önerilir:

```python
from webaiprovider import AccountPaused, FeatureUnavailable, RateLimited
from webaiprovider.errors import RequestNotSentError

try:
    response = chat.ask("Merhaba")
except RateLimited as err:
    if err.retry_after is not None and err.message_submitted is False:
        print(f"{err.retry_after} sn sonra güvenle tekrar denenebilir.")
except RequestNotSentError:
    print("İstek sunucuya ulaşmadı; tekrar denenebilir.")
except AccountPaused:
    print("Oturum yenilenmeli.")
except FeatureUnavailable as err:
    print(f"Desteklenmeyen özellik: {err.capability.value}")
```

[Yukarı dön](#konu-dizini)

---

## Tanılama ve Performans

- **Zaman ölçümü (`SendTiming` / `use_send_timing`):** `webaiprovider.core.timing` altındaki `SendTiming`, mesaj ve kimlik verisi kaydetmeden işlem adımlarının milisaniye sürelerini ve sayaçlarını toplar:

```python
from webaiprovider.core.timing import SendTiming, use_send_timing

timing = SendTiming(provider.id)
with use_send_timing(timing):
    resp = provider.ask("Merhaba")
durations_ms, counters = timing.snapshot()
```

- **Oturum yeniden kullanımı:** Aynı `WebAIAccount` nesnesini bellekte tutup kısa aralıklarla tekrar `WebAIProvider(account)` açtığında, ilk açılışta doğrulanan oturum ve model kataloğu 300 saniye boyunca yeniden kullanılır; kimlik değişirse veya `AccountPaused` / `forget_session()` / `authenticate()` olursa otomatik temizlenir.
- **Pratik performans kuralları:** Sohbeti yanıtlarıyla açarken `chats.get_with_responses()` kullan; büyük hesaplarda `chats.list(limit=None)` yerine gereken `limit` değerini ver; uzun ömürlü süreçlerde tamamlanmış `MessageOperation` nesnelerini serbest bırak. Offline benchmark paketini çalıştırmak için [benchmarks/README.md](../../benchmarks/README.md) kılavuzuna bakabilirsin.

[Yukarı dön](#konu-dizini)

---

[← Önceki: 04 · Sağlayıcı Rehberleri](provider-guides.md) · [Belge Haritası](README.md) · **Sonraki:** [06 · API Başvuru Kartı →](api-reference.md)
