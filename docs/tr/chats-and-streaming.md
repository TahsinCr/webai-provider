[← Önceki: 01 · Başlangıç ve Hesaplar](getting-started.md) · [Belge Haritası](README.md) · [Sonraki: 03 · Modeller ve Projeler →](models-and-projects.md)

# 02 · Sohbetler, Yanıtlar, Gerçek Zamanlı Akış ve Hook'lar

Bu bölümde sohbet oluşturmayı ve aramayı, dosya eklemeyi, düşünme bloklarını (`ThinkingPart`) okumayı, kelime kelime canlı akışı yönetmeyi, geçmiş mesajları düzenleyip yeniden denemeyi ve `wait_for()` / `chat.hooks` ile akış kontrolünü bulacaksın.

---

## Konu Dizini

- [Hızlı Örnek](#hızlı-örnek)
- [Sohbet Yönetimi (`provider.chats`)](#sohbet-yönetimi-providerchats)
  - [Yeni ve Geçici Sohbetler (`create`)](#yeni-ve-geçici-sohbetler-create)
  - [Listeleme ve Arama (`list`, `find`, `search`, `first`, `last`, `single`)](#listeleme-ve-arama-list-find-search-first-last-single)
  - [Tek Okumada Sohbet + Geçmiş (`get`, `get_with_responses`)](#tek-okumada-sohbet--geçmiş-get-get_with_responses)
  - [Ad Değiştirme ve Silme (`update`, `delete`)](#ad-değiştirme-ve-silme-update-delete)
- [Sohbet Nesnesi (`Chat` / `AsyncChat`)](#sohbet-nesnesi-chat--asyncchat)
  - [`chat.ask()` — Sync ve Async Dönüş Farkı](#chatask--sync-ve-async-dönüş-farkı)
  - [Diğer Metotlar (`stream`, `prepare`, `branch`, `move_to`, `download`)](#diğer-metotlar-stream-prepare-branch-move_to-download)
- [Mesajlar ve Ekler (`Message` & `Attachment`)](#mesajlar-ve-ekler-message--attachment)
- [Yanıtlar, Bloklar, Kaynaklar ve Geçmiş (`Response`)](#yanıtlar-bloklar-kaynaklar-ve-geçmiş-response)
  - [`response.text` ve `response.parts`](#responsetext-ve-responseparts)
  - [Kaynaklar, Çıktılar ve Zamanlama (`sources`, `artifacts`, `timing`)](#kaynaklar-çıktılar-ve-zamanlama-sources-artifacts-timing)
  - [Düzenleme, Yeniden Deneme ve Geçmiş (`chat.responses`)](#düzenleme-yeniden-deneme-ve-geçmiş-chatresponses)
- [Gerçek Zamanlı Akış ve Operasyon Kontrolü](#gerçek-zamanlı-akış-ve-operasyon-kontrolü)
  - [Operasyon Durumları (`OperationState`)](#operasyon-durumları-operationstate)
  - [Akış Olayları (`OperationEvent` ve `StreamEvent`)](#akış-olayları-operationevent-ve-streamevent)
  - [`wait_for()`, Duraklatma ve `cancel()`](#wait_for-duraklatma-ve-cancel)
  - [Kalıcı Kancalar (`chat.hooks`)](#kalıcı-kancalar-chathooks)

---

## Hızlı Örnek

```python
import asyncio
from webaiprovider import (
    WebAIAccount,
    AsyncChatGPTWebAIProvider,
    Attachment,
    GenerationEnd,
    OperationEvent,
    OutputDelta,
    WebAIProviderType,
    ThinkingDelta,
    ThinkingPart,
)


async def main() -> None:
    account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)
    async with AsyncChatGPTWebAIProvider(account) as provider:
        chat = await provider.chats.create("Kod İncelemesi")
        chat.hooks.on(OperationEvent.SOURCE_DISCOVERED, lambda ev: print("Kaynak:", ev.source.url))

        op = chat.responses.create(
            "Bu dosyadaki olası hataları incele.",
            attachments=[Attachment.from_path("app.py")],
        )
        async for event in op.stream():
            if isinstance(event, ThinkingDelta):
                print(f"[Düşünüyor] {event.delta}", end="", flush=True)
            elif isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)
            elif isinstance(event, GenerationEnd):
                print("\n--- Üretim tamamlandı ---")

        response = await op.wait()
        print(
            "Düşünme bloğu sayısı:", len(response.parts.list(lambda p: isinstance(p, ThinkingPart)))
        )

        await chat.prepare()
        followup = await chat.ask("İlk maddeyi örnek kodla düzelt.").wait()
        print(followup.text)


asyncio.run(main())
```

[Yukarı dön](#konu-dizini)

---

## Sohbet Yönetimi (`provider.chats`)

### Yeni ve Geçici Sohbetler (`create`)

`provider.chats.create(title="New Chat", *, project=None, temporary=False)` yeni bir sohbet hazırlar. Bu çağrı sunucuda hemen boş bir sohbet yaratmaz; ilk mesajı gönderdiğin anda sohbet uzak tarafta oluşturulur ve `chat.remote_id` alanı dolar.

| Parametre | Tip | Varsayılan | Açıklama |
| --- | --- | --- | --- |
| `title` | `str` | `"New Chat"` | Sohbet başlığı. Sağlayıcı destekliyorsa (ör. Claude) ilk gönderimde uzaktaki sohbete doğrudan yansıtılır. |
| `project` | `Project \| str \| None` | `None` | Sohbetin bağlı olacağı proje veya proje kimliği. |
| `temporary` | `bool` | `False` | `True` verilirse geçici sohbet olarak açılır. Kalıcı geçmişe yazılmaz, proje bağlamına taşınamaz ve `chat.responses` koleksiyon değişim olayı (`create`/`update`) yayımlamaz. |

### Listeleme ve Arama (`list`, `find`, `search`, `first`, `last`, `single`)

Tüm uzak koleksiyonlar (`chats`, `projects`, `responses`, `project.chats`, `project.attachments`) aynı sorgu metotlarını paylaşır:

- **`list(predicate=None, *, limit=50, include_projects=False)`:**
  - `limit=50` *(varsayılan)*: En fazla 50 sohbet toplayana kadar sayfaları otomatik gezer.
  - `limit=0`: Ağ isteği yapmadan hemen `[]` döndürür.
  - `limit=None`: Ulaşılabilen tüm sayfaları okur (büyük hesaplarda dikkatli kullan).
  - `include_projects=True`: Ana sohbet listesinin yanında proje içindeki sohbetleri de getirir.
  - `predicate`: Python tarafında filtre fonksiyonu (ör. `lambda c: not c.temporary`).
- **`first(predicate=None)` / `last(predicate=None)` / `single(predicate=None)`:**
  `first()` ilk eşleşeni (`predicate=None` iken sadece 1 öğe isteyerek), `last()` sonuncuyu döndürür. `single()` ise tam olarak 1 eşleşme bekler; birden fazla kayıt uyarsa `AmbiguousMatchError` yükseltir.
- **`find(query, /, *, exact=False, startswith=False, endswith=False, single=False, last=False, **options)`:**
  Başlık veya kimlik üzerinde büyük/küçük harf duyarsız arama yapar. Eşleşme bayraklarından (`exact`, `startswith`, `endswith`) en fazla biri `True` olabilir; hiçbiri verilmezse alt dize araması yapar.
- **`search(query, /, *, exact=False, startswith=False, endswith=False, limit=None, **options)`:**
  `find()` ile aynı kurallarla eşleşen tüm sohbetleri liste olarak döndürür.

```python
with ChatGPTWebAIProvider(account) as provider:
    son_sohbetler = provider.chats.list(limit=20, include_projects=True)
    rapor = provider.chats.find("Haftalık Rapor", exact=True, single=True, include_projects=True)
    python_sohbetleri = provider.chats.search("Python", limit=5)
```

### Tek Okumada Sohbet + Geçmiş (`get`, `get_with_responses`)

- **`chats.get(item_id, /, *, include_projects=False, metadata_only=False)`:** Kimliğiyle sohbeti getirir (bulunamazsa `RemoteNotFound`). `metadata_only=True` destekleyen sağlayıcılarda tam mesaj geçmişini indirmeden sadece başlık/proje bilgisini okur.
- **`chats.get_with_responses(item_id, /, *, limit=50, include_projects=False)`:** Sohbeti açıp ardından son yanıtları da okumak istediğinde iki ayrı istek atmak yerine ikisini **tek bir geçmiş okumasında** döndürür (`chat, responses`). `limit=0` verilirse yalnızca metadata okunup boş liste döner.

### Ad Değiştirme ve Silme (`update`, `delete`)

- `provider.chats.update(chat, *, title=None)`: Sohbet nesnesi veya ID'si ile başlığı günceller.
- `provider.chats.delete(chat)`: Sohbeti uzaktan siler.

[Yukarı dön](#konu-dizini)

---

## Sohbet Nesnesi (`Chat` / `AsyncChat`)

### `chat.ask()` — Sync ve Async Dönüş Farkı

Günlük kullanımda en sık çağrılan metot `chat.ask(prompt, *, model=None, attachments=None, **metadata)` şeklindedir. Dikkat etmen gereken tek fark dönüş tipidir:

- **Senkron (`Chat.ask`):** Gönderimi başlatır, bitene kadar bekler ve **doğrudan `Response` döndürür**.
- **Asenkron (`AsyncChat.ask`):** Akışı tüketebilmen (`stream()`) veya aşama bekleyebilmen (`wait_for()`) için **`AsyncMessageOperation` döndürür**. Doğrudan yanıtı almak için `await chat.ask("...").wait()` ya da doğrudan `await chat.ask("...")` yazabilirsin (`AsyncMessageOperation` doğrudan `await` edilebilir).

```python
# Senkron: doğrudan Response döner
response = sync_chat.ask("Özetle")

# Asenkron (doğrudan yanıt):
response = await async_chat.ask("Özetle")

# Asenkron (akışı izlemek için):
op = async_chat.ask("Özetle")
async for ev in op.stream():
    ...
response = await op.wait()
```

### Diğer Metotlar (`stream`, `prepare`, `branch`, `move_to`, `download`)

- **`chat.stream(prompt, *, model=None, attachments=None, **metadata)`:** Doğrudan akış olaylarını yineleyen bir iterator (`for ev in chat.stream(...)` / `async for ev in chat.stream(...)`) döndürür.
- **`chat.prepare()`:** Sonraki normal gönderim için sağlayıcının devam bağlamını sohbetin gönderim kilidi altında önceden ısıtır (ör. Claude'da hafif devam okumasını önden yapar).
- **`chat.rename(title)` / `chat.delete()`:** `chats.update` ve `chats.delete` kısayollarıdır.
- **`chat.download(target)`:** Yanıttaki bir `Artifact` nesnesini veya indirme URL'sini güvenli indiriciyle (SSRF korumalı, yönlendirmesiz) `bytes` olarak indirir.
- **`chat.branch(message_id, *, title=None)`** *(ChatGPT ve Gemini)*: Belirtilen mesajdan yeni bir paralel sohbet dalı oluşturur. Doğrudan `response.branch(title=...)` üzerinden de çağrılabilir.
- **`chat.move_to(project)`** *(ChatGPT, Claude, Grok)*: Sohbeti projeye taşır. Yalnızca ChatGPT ayrıca mevcut projeden başka projeye yeniden atamayı ve `chat.move_to(None)` ile sohbeti projeden çıkarmayı destekler. Taşıma işlemi sadece sohbet kilidini tutar.

[Yukarı dön](#konu-dizini)

---

## Mesajlar ve Ekler (`Message` & `Attachment`)

Bir mesaja veya projeye dosya, bayt verisi ya da metin eklemek için `Attachment` kullanılır:

- `Attachment.from_path(path, *, name=None, mime_type=None)`: Diskteki dosyayı doğrular ve boyut/MIME bilgisini okur.
- `Attachment.from_bytes(data, name, *, mime_type=None)`: Bellekteki ham baytlardan ek oluşturur (`name` zorunludur).
- `Attachment.from_text(text, name, *, mime_type="text/plain", id=None, metadata=None)`: Metin tabanlı ek oluşturur.
- `attachment.read_bytes() -> bytes`: İçeriği bayt olarak okur.

Birden fazla eki düzenlemek için `Attachments` koleksiyonu `create(item, *, name=None, mime_type=None)`, `update(target, *, name=None, mime_type=None)`, `delete(target)` ve `total_size` özelliklerini sunar.

Önceden bir `Message` / `AsyncMessage` nesnesi oluşturup göndermek veya sonradan düzenlemek istersen:

```python
from webaiprovider import Attachment, Message

msg = Message(
    "Bu JSON dosyasını doğrula.",
    attachments=[Attachment.from_bytes(b'{"ok": true}', "data.json", mime_type="application/json")],
)
response = sync_chat.ask(msg)

# Aynı kullanıcı mesajını yeni metinle düzenle:
edited = msg.edit("Sadece şema hatalarını listele.").wait()
```

[Yukarı dön](#konu-dizini)

---

## Yanıtlar, Bloklar, Kaynaklar ve Geçmiş (`Response`)

### `response.text` ve `response.parts`

Modeller tek bir yanıtta hem akıl yürütme (düşünme) hem de görünür metin bloklarını art arda üretebilir.

- **`response.text` (`str`):** Yalnızca kullanıcıya görünür `TextPart` bloklarını sağlayıcının ürettiği sırada `\n` ile birleştirir (`ThinkingPart` metinlerini içermez).
- **`response.parts` (`ResponseParts`):** Sağlayıcı sırasını koruyan, salt okunur yerel koleksiyondur. İçinde `ThinkingPart(id, text)` ve `TextPart(id, text)` öğeleri yer alır.

```python
from webaiprovider import TextPart, ThinkingPart

for part in response.parts:
    if isinstance(part, ThinkingPart):
        print(f"[Düşünme {part.id}] {part.text[:80]}...")
    elif isinstance(part, TextPart):
        print(part.text)

# Sadece düşünme bloklarını seçmek için:
thoughts = response.parts.list(lambda p: isinstance(p, ThinkingPart))
```

### Kaynaklar, Çıktılar ve Zamanlama (`sources`, `artifacts`, `timing`)

- **`response.sources` (`SourcesCollection[Source]`):** Web araması veya belge alıntıları (`id`, `url`, `title`, `snippet`, `kind`, `metadata`).
- **`response.artifacts` (`ArtifactsCollection[Artifact]`):** Üretilen görsel veya dosyalar (`id`, `kind`, `name`, `mime_type`, `size`, `download_url`, `content`, `metadata`). İndirmek için `chat.download(artifact)` kullanılır.
- **`response.timing` (`TimingMetrics | None`):** Canlı gönderimlerde `started_at`, `first_token_at`, `completed_at`, `first_token_ms` ve `duration_ms` alanlarını doldurur.
- **`response.message`:** Yanıtı tetikleyen kullanıcı mesajı.

### Düzenleme, Yeniden Deneme ve Geçmiş (`chat.responses`)

| Metot | Parametreler | Açıklama |
| --- | --- | --- |
| `create` | `(message, *, model=None, attachments=None, **metadata)` | Yeni mesaj operasyonu (`MessageOperation` / `AsyncMessageOperation`) hazırlar. |
| `edit` | `(target, new_content, *, model=None, **options)` | Önceki bir mesajı (ID, `Message` veya `Response`) yeni içerikle düzenler; ekleri korur. |
| `update` | `(target, *, content=None, model=None, **options)` | `content=None` bırakılırsa mevcut metni koruyup sadece `model` gibi alanları günceller. |
| `retry` | `(target, *, model=None, **options)` | Hedef yanıtı aynı veya farklı bir modelle yeniden üretir. |
| `list` | `(predicate=None, *, limit=50, fresh=False)` | Sohbet geçmişini getirir. Async tarafta eşzamanlı çağrılar tek isteği paylaşır; `fresh=True` bağımsız taze okuma yapar. |
| `delete` | `(target)` | Yalnızca henüz sunucuya gönderilmemiş yerel taslak mesajları kaldırır; sunucudaki kayıtlar için `UnsupportedOperationError` verir. |

[Yukarı dön](#konu-dizini)

---

## Gerçek Zamanlı Akış ve Operasyon Kontrolü

### Operasyon Durumları (`OperationState`)

`chat.responses.create()`, `edit()` ve `retry()` çağrıları işlemi yöneten bir `MessageOperation` (sync) veya `AsyncMessageOperation` (async) nesnesi döndürür.

- `operation.state` sırasıyla şu durumlardan geçer: `PENDING` → `PREPARING` → `SENDING` → `STREAMING` → `COMPLETED` | `FAILED` | `CANCELLED`.
- `operation.stream()`: Olayları geldikçe üretir. Senkron operasyonda aynı anda tek bir sürücü thread ağı okur (diğer okuyucular paylaşılan olay tamponundan beslenir); döngüden `break` ile erken çıkılırsa (`GeneratorExit`) operasyon otomatik olarak iptal edilir (`cancel()`).
- `operation.wait()` *(veya `start()`)*: Tüm akışı tüketip tamamlanmış `Response` nesnesini döndürür.
- `operation.response` oluşan yanıtı, `operation.hook_results` ise çağrılan kancaların sonuçlarını taşır. `GENERATION_END` olayı tetiklendiği anda `operation.response`, `response.parts` ve `chat.responses` kaydı anında oluşturulur; ardından gelen `remote_id`, `ResponsePartsResolved`, `SourceDiscovered` ve `ArtifactDiscovered` bilgileri aynı `Response` nesnesini yerinde günceller. Başarıyı kesinleştirmek için `wait()` sonucunu veya `STATUS_TERMINAL` olayını kullan.

### Akış Olayları (`OperationEvent` ve `StreamEvent`)

Akıştan dönen her nesne `StreamEvent` alt sınıfıdır ve `.event` alanında karşılık gelen `OperationEvent` enum değerini taşır:

| `OperationEvent` | Sınıf | Önemli Alanlar | Anlamı |
| --- | --- | --- | --- |
| `THINKING_BEGIN` | `ThinkingBegin` | `part_id` | Düşünme bloğu başladı. |
| `THINKING_DELTA` | `ThinkingDelta` | `part_id`, `delta` | Düşünme metnine yeni parça eklendi. |
| `THINKING_REPLACE` | `ThinkingReplace` | `part_id`, `text` | Düşünme bloğunun tam metni değişti. |
| `THINKING_END` | `ThinkingEnd` | `part_id`, `text` | Düşünme bloğu kapandı. |
| `OUTPUT_BEGIN` | `OutputBegin` | `part_id` | Görünür yanıt bloğu başladı. |
| `OUTPUT_DELTA` | `OutputDelta` | `part_id`, `delta` | Görünür yanıta yeni parça eklendi. |
| `OUTPUT_REPLACE` | `OutputReplace` | `part_id`, `text` | Görünür yanıt bloğu tam metinle değişti. |
| `OUTPUT_END` | `OutputEnd` | `part_id`, `text` | Görünür yanıt bloğu kapandı. |
| `GENERATION_END` | `GenerationEnd` | — | Model üretimi bitti. |
| `VERIFICATION_BEGIN` | `VerificationBegin` | — | Sunucu tarafı doğrulama okuması başladı. |
| `RESPONSE_PARTS_RESOLVED` | `ResponsePartsResolved` | `parts` | Doğrulanmış nihai bloklar geldi. |
| `SOURCE_DISCOVERED` | `SourceDiscovered` | `source` | Yeni kaynak (`Source`) bulundu. |
| `ARTIFACT_DISCOVERED` | `ArtifactDiscovered` | `artifact` | Yeni çıktı (`Artifact`) bulundu. |
| `STATUS_TERMINAL` | `TerminalStatus` | `status`, `error` | İşlem sonlandı (`"completed"`, `"failed"`, `"cancelled"`). |

### `wait_for()`, Duraklatma ve `cancel()`

Bir arayüzde görünür metni akıştan güncelle. `OUTPUT_END` bir metin bloğunu kapatır ve aynı yanıtta başka bloklar gelebilir. `GENERATION_END` üretimin tamamlandığını bildirir ve bu anda `operation.response.parts` ile `chat.responses` kaydı hazır hale gelir. Sağlayıcı bundan sonra sunucudan doğrulama yapabilir; işlemi başarılı olarak kaydetmek için `wait()` sonucunu veya `STATUS_TERMINAL` olayını izle.

Asenkron bir arayüzde aşamaları ayrı bekleyebilirsin. Aşağıdaki örnekte `chat`, açık bir asenkron sağlayıcıdan alınmış sohbet nesnesidir:

```python
from webaiprovider import OperationEvent

operation = chat.responses.create("Sonucu üç maddede özetle.")
generated = await operation.wait_for(OperationEvent.GENERATION_END, timeout=5.0)
if generated is not None:
    print("Üretim tamamlandı. İşlemin sonucu bekleniyor.")

response = await operation.wait()
print(response.text)
```

`wait_for(event_kind, *, timeout=None, after=None)` seçilen olay gelene kadar bekler:

- `timeout`: Saniye cinsinden süre sınırı (`NaN` verilemez). Süre dolarsa veya olay hiç gelmeden işlem biterse `None` döner. Senkron built-in HTTP akışlarında süre dolduğunda ağ aktarımı açık bırakılır; daha sonra `wait()`, `stream()` veya yeni bir `wait_for()` ile kaldığın yerden devam edebilirsin.
- `after`: Aynı operasyondan daha önce dönen bir `StreamEvent` verilirse aramayı o olaydan sonrasıyla sınırlar (ör. ikinci bir düşünme/yanıt bloğunu yakalamak için).
- `operation.cancel()` iptal ister. Yerel akış kapansa bile sağlayıcıya ulaşmış bir mesaj sunucuda kalabilir. Asenkron kodda iptal isteğinden sonra işlemin sonlanmasını da bekle.

```python
from webaiprovider import OperationEvent, ThinkingEnd

op = chat.responses.create("Uzun bir analiz hazırla.")
thinking = op.wait_for(OperationEvent.THINKING_END, timeout=5.0)
if isinstance(thinking, ThinkingEnd):
    print("Düşünme bitti:", thinking.text[:100])

# Kalan akışı tamamla (veya istemiyorsan op.cancel() çağır):
final_response = op.wait()
```

> **Senkron Kilit Kuralı:** Senkron modda `wait_for(..., timeout=...)` nedeniyle bir gönderim duraklatılmış durumdaysa, aynı thread üzerinde onu tamamlamadan (`wait()` / `stream()`) veya `cancel()` etmeden aynı sohbette yeni bir işlem başlatamazsın (`WebAIProviderError`).

### Kalıcı Kancalar (`chat.hooks`)

Bir sohbete bağlı tüm gönderimlerde otomatik çalışacak dinleyicileri `chat.hooks` üzerine kaydedebilirsin (`provider.hooks`, `provider.chats.hooks`, `provider.projects.hooks` de aynı arayüzü sunar):

- **Akış dinleyicisi:** `chat.hooks.on(OperationEvent.OUTPUT_DELTA, fn)` veya tüm olaylar için `"*"`.
- **Yaşam döngüsü kancaları:** `before("chat.send", fn)`, `on_success(...)`, `on_error(...)`, `on_cancelled(...)`, `on_finally(...)`.
  Bu fonksiyonlara gelen `OperationContext` nesnesinde `ctx.prompt` veya `ctx.model` değiştirilirse gönderim güncellenmiş değerlerle yapılır. Kanca içinden aynı sohbette yeni gönderim başlatılamaz.

```python
@chat.hooks.before("chat.send")
def ek_yonerge(ctx):
    ctx.prompt = f"Yanıtı kısa tut:\n{ctx.prompt}"


@chat.hooks.on_success("chat.send")
def tamamlandi(ctx):
    print(f"Operasyon {ctx.operation_id} tamamlandı.")
```

[Yukarı dön](#konu-dizini)

---

[← Önceki: 01 · Başlangıç ve Hesaplar](getting-started.md) · [Belge Haritası](README.md) · **Sonraki:** [03 · Modeller ve Projeler →](models-and-projects.md)
