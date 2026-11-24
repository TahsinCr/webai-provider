[← Önceki: 02 · Sohbetler, Yanıtlar ve Akış](chats-and-streaming.md) · [Belge Haritası](README.md) · [Sonraki: 04 · Sağlayıcı Rehberleri →](provider-guides.md)

# 03 · Modeller, Efor Seçimi ve Projeler

Bu bölümde hesaptaki modelleri listelemeyi, akıl yürütme eforu (`effort`) seçmeyi, 300 saniyelik katalog önbelleğini ve projeler ile proje dosya/metin eklerini yönetmeyi öğreneceksin.

---

## Konu Dizini

- [Hızlı Örnek](#hızlı-örnek)
- [Model Kataloğu (`provider.models`)](#model-kataloğu-providermodels)
  - [Sorgu Metotları ve `refresh()`](#sorgu-metotları-ve-refresh)
  - [`MODEL_CATALOG_TTL = 300.0` Önbellek Davranışı](#model_catalog_ttl--3000-önbellek-davranışı)
- [Model ve Efor Seçim Türleri](#model-ve-efor-seçim-türleri)
  - [`Models` Katmanları ve `model.select()`](#models-katmanları-ve-modelselect)
  - [Statik Kataloglar (`ChatGPTModels`, `ClaudeModels`, `GeminiModels`, `GrokModels`)](#statik-kataloglar-chatgptmodels-claudemodels-geminimodels-grokmodels)
- [Projeler (`provider.projects`)](#projeler-providerprojects)
  - [Proje Oluşturma, Güncelleme ve Silme](#proje-oluşturma-güncelleme-ve-silme)
  - [`Project` Talimatları ve Proje Sohbetleri](#project-talimatları-ve-proje-sohbetleri)
- [Proje Ekleri (`project.attachments`)](#proje-ekleri-projectattachments)

---

## Hızlı Örnek

```python
from webaiprovider import WebAIAccount, ChatGPTWebAIProvider, Models, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)
with ChatGPTWebAIProvider(account) as provider:
    thinking = provider.models.find("thinking")
    selection = thinking if thinking else Models.AUTO

    proje = provider.projects.create(
        "Mimari İnceleme",
        instruction="Yanıtları Türkçe ve madde işaretli ver.",
        memory_share=False,
    )
    proje.attachments.create("v2.0.0 API native senkron çalışır.", "text", name="notlar.txt")
    proje.attachments.create("src/main.py")

    chat = provider.chats.create("Kod Analizi", project=proje)
    print(chat.ask("Ekli dosyayı özetle.", model=selection).text)
```

[Yukarı dön](#konu-dizini)

---

## Model Kataloğu (`provider.models`)

### Sorgu Metotları ve `refresh()`

`provider.models`, hesap için keşfedilen `ModelRef` nesnelerini tutar. Oturum açılışında (`open()`) otomatik olarak ısıtılır, böylece ilk model sorguları ağ gecikmesi yaratmaz.

- **`list(predicate=None, *, limit=None) -> list[ModelRef]`:** Hesaptaki tüm modelleri (veya `predicate` filtresine uyanları) döndürür.
- **`get(model_id) -> ModelRef`:** Birebir `id` ile getirir; yoksa `RemoteNotFound`.
- **`find(query, /, *, exact=False, startswith=False, endswith=False, single=False, last=False)`:** `Models.HIGH` gibi bir katman veya `"thinking"` / `"Sonnet"` gibi bir metin arar.
- **`search(query, ...)`:** Eşleşen tüm modelleri liste olarak döndürür.
- **`resolve(target) -> ModelSelection`:** String, `Models` enum, `ModelRef`, `EffortRef` veya `ModelSelection` verisini aktif hesabın kataloğuna göre doğrular.
- **`refresh() -> list[ModelRef]`:** Sağlayıcı alt katmanındaki önbelleği atlayıp (`force=True`) kataloğu sunucudan zorla yeniden yükler.

```python
for model in provider.models.list():
    efforts = [e.id for e in model.efforts.list()]
    print(f"{model.id} ({model.name}) -> eforlar={efforts}, varsayılan={model.default_effort_id}")
```

### `MODEL_CATALOG_TTL = 300.0` Önbellek Davranışı

`ModelsCollection` ve `AsyncModelsCollection`, dört sağlayıcının iç katalog önbellekleriyle aynı **`MODEL_CATALOG_TTL = 300.0` (300 saniye)** süresini kullanır:

- 300 saniye boyunca sorgular doğrudan bellekteki atomik snapshot'tan yanıtlanır (boş sonuçlar da aynı süre boyunca saklanır).
- Süre dolduğunda bir sonraki sorgu kataloğu tazeler.
- `authenticate()` veya `forget_session()` çağrıldığında model önbelleği temizlenir.

[Yukarı dön](#konu-dizini)

---

## Model ve Efor Seçim Türleri

### `Models` Katmanları ve `model.select()`

`ask()`, `responses.create()`, `edit()` ve `retry()` çağrılarındaki `model=` parametresi 5 farklı tip kabul eder: `Models` enum, `str`, `ModelRef`, `EffortRef` veya `ModelSelection`.

`webaiprovider.Models` enum'u iki yerde kullanılır:
1. **Doğrudan `model=` parametresinde:**
   - `Models.AUTO`: Sağlayıcının varsayılan modelini seçer.
   - `Models.LOW`: Katalog sırasındaki en hafif/hızlı modeli seçer.
   - `Models.MEDIUM`: Ortadaki dengeli modeli seçer.
   - `Models.HIGH`: Katalog sırasındaki en kapsamlı modeli seçer.
2. **Bir modelin eforunu seçerken (`model_ref.select(effort)`):**
   `model_ref.select(Models.HIGH)` veya `model_ref.select("extended")` çağrısı bir `ModelSelection(model, effort)` döndürür. `effort=None` veya `"auto"` varsayılan eforu korur.

Doğrudan bir `EffortRef` nesnesini `model=` parametresine verirsen `SelectionResolver` o eforun bağlı olduğu modeli de otomatik çözümler. String aramalarda tek eşleşme bulunmazsa `AmbiguousMatchError` veya `RemoteNotFound` yükselir.

### Statik Kataloglar (`ChatGPTModels`, `ClaudeModels`, `GeminiModels`, `GrokModels`)

Bilinen model kimliklerine IDE otomatik tamamlamasıyla ulaşmak için statik kataloglar dışa aktarılır:

```python
from webaiprovider import ChatGPTModels, GeminiModels, Models

secim1 = ChatGPTModels.get("gpt-6-thinking").select("extended")
secim2 = GeminiModels["gemini-neon"].select(Models.HIGH)
```

Statik kataloglarda `.list()`, `.get()`, `.find()`, `.search()`, `.contains()` ve `Catalog["id"]` kullanılabilir. Statik katalog bilinen kimlikleri sağlar. Hesabına sunulan seçenekleri görmek için `provider.models.list()`, katalog süresi dolmadan yeni bir okuma yapmak için `provider.models.refresh()` kullan. Katalogda görünen bir model için gönderim sırasında ayrıca kota veya erişim hatası alabilirsin.

[Yukarı dön](#konu-dizini)

---

## Projeler (`provider.projects`)

Projeler (ChatGPT Projects, Claude Projects, Gemini Notebooks, Grok Workspaces), ortak talimatlar ve kaynak dosyalar etrafında sohbetleri gruplar.

### Proje Oluşturma, Güncelleme ve Silme

- **`projects.create(title, *, instruction=None)`:** Yeni proje oluşturur. *(ChatGPT'de ek olarak `memory_share: bool = True` parametresi vardır.)*
- **`projects.update(project, *, title=None, instruction=None)`:** Yalnızca `None` olmayan alanları sunucuda günceller. Proje talimatını temizlemek için `instruction=""` verilir. Her yazma işlemi ilgili `project_lock` altında güvenli sırayla yürür.
- **`projects.delete(project)`:** Projeyi siler.
- Standart koleksiyon metotları (`list`, `get`, `first`, `last`, `single`, `find`, `search`) projelerde de birebir geçerlidir.

### `Project` Talimatları ve Proje Sohbetleri

- `project.rename(title)` ve `project.delete()`: Doğrudan proje nesnesi üzerinden ad değiştirme ve silme.
- `project.instruction`:
  - `get() -> str`: Güncel talimat metnini sunucudan okur.
  - `edit(text: str) -> str`: Talimatı günceller.
  - `clear() -> None`: `edit("")` ile talimatı temizler.
- `project.chats`: Yalnızca o projenin içindeki sohbetleri listeleyen ve arayan tam `RemoteCollection` arayüzüdür (`project.chats.list()`, `find()`, `search()`).

```python
with ClaudeWebAIProvider(account) as provider:
    proje = provider.projects.create("Backend", instruction="Python 3.10+ tipleri kullan.")
    chat = provider.chats.create("Modül Bölme", project=proje)
    chat.ask("auth.py dosyasını böl.")

    for c in proje.chats.list(limit=10):
        print(c.title, c.remote_id)
```

[Yukarı dön](#konu-dizini)

---

## Proje Ekleri (`project.attachments`)

Projeye yerel dosya veya doğrudan metin içeriği eklemek için `project.attachments` kullanılır:

| Metot | Parametreler | Açıklama |
| --- | --- | --- |
| `create` | `(file_or_data, resource_type=None, *, name=None, mime_type=None, **options)` | `Path`/dosya yolu (`resource_type="file"`), metin (`resource_type="text"`) veya `Attachment` alır. Ham `bytes` desteklenmez (`UnsupportedOperationError`). |
| `create_many` | `(*items)` | Birden fazla eki ekler; sağlayıcı destekliyorsa (ör. ChatGPT) toplu eklemeyi kullanır, hata olursa yarım kalan yüklemeleri temizler. |
| `list` / `get` | standart sorgu parametreleri | Projenin güncel eklerini sunucudan getirir. |
| `download` | `(attachment)` | Uzak URL varsa güvenli indiriciyle, yerel taslaksa disk/bellekten bayt okur. |
| `delete` / `delete_many` | `(attachment)` / `(*attachments)` | Bir veya birden fazla eki projeden kaldırır. |
| `update` | `(attachment, **options)` | Yalnız henüz uzakta oluşturulmamış yerel proje taslaklarında çalışır. |

```python
from webaiprovider import WebAIAccount, Attachment, GeminiWebAIProvider, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.GEMINI)
with GeminiWebAIProvider(account) as provider:
    nb = provider.projects.create("Araştırma")
    ek1 = nb.attachments.create("Deney sonucu: %35 hızlanma.", "text", name="sonuc.txt")
    ek2 = nb.attachments.create(Attachment.from_path("rapor.pdf"))

    print("Ekler:", [a.name for a in nb.attachments.list()])
    nb.attachments.delete_many(ek1, ek2)
    nb.delete()
```

[Yukarı dön](#konu-dizini)

---

[← Önceki: 02 · Sohbetler, Yanıtlar ve Akış](chats-and-streaming.md) · [Belge Haritası](README.md) · **Sonraki:** [04 · Sağlayıcı Rehberleri →](provider-guides.md)
