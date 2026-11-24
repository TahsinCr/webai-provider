[← Belge Haritası](README.md) · [Sonraki: 02 · Sohbetler, Yanıtlar ve Akış →](chats-and-streaming.md)

# 01 · Kurulum, Hesap Oluşturma ve İlk Mesaj

Bu rehber kütüphaneyi kurmayı, tarayıcıdan veya çerez dosyasından hesap bağlamayı, oturumu yönetmeyi ve ilk mesajı göndermeyi anlatır.

---

## Konu Dizini

- [Hızlı Örnek](#hızlı-örnek)
- [Kurulum ve Sistem Notları](#kurulum-ve-sistem-notları)
- [Temel Kavramlar](#temel-kavramlar)
- [Hesap Oluşturma (`WebAIAccount`)](#hesap-oluşturma-webaiaccount)
  - [`WebAIAccount` Alanları](#webaiaccount-alanları)
  - [Tarayıcıdan Çerez Alma (`from_browser`)](#tarayıcıdan-çerez-alma-from_browser)
  - [Sözlük veya Dosyadan Bağlanma (`from_cookies`)](#sözlük-veya-dosyadan-bağlanma-from_cookies)
- [Kimlik Depoları (`CredentialStore`)](#kimlik-depoları-credentialstore)
- [Sağlayıcı Oturumu ve Yaşam Döngüsü](#sağlayıcı-oturumu-ve-yaşam-döngüsü)
  - [Kurucu Parametreleri](#kurucu-parametreleri)
  - [Oturum Açma, Kapatma ve `provider.ask()`](#oturum-açma-kapatma-ve-providerask)
  - [Oturum Durumu ve Çıkış (`session_status`, `authenticate`, `forget_session`)](#oturum-durumu-ve-çıkış-session_status-authenticate-forget_session)

---

## Hızlı Örnek

Tarayıcıdaki aktif oturumunu okuyup ilk cevabı almak yalnızca birkaç satır sürer:

```python
from webaiprovider import WebAIAccount, ChatGPTWebAIProvider, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT, browser="firefox")

with ChatGPTWebAIProvider(account) as provider:
    response = provider.ask("Merhaba! Kısaca kendini tanıt.")
    print(response.text)
```

Asenkron projelerde `Async*` sınıflarını ve `async with` yapısını kullanırsın:

```python
import asyncio
from webaiprovider import WebAIAccount, AsyncClaudeWebAIProvider, WebAIProviderType


async def main() -> None:
    account = WebAIAccount.from_browser(WebAIProviderType.CLAUDE)
    async with AsyncClaudeWebAIProvider(account) as provider:
        response = await provider.ask("Merhaba Claude!")
        print(response.text)


asyncio.run(main())
```

[Yukarı dön](#konu-dizini)

---

## Kurulum ve Sistem Notları

`webai-provider` için **Python 3.10 veya üzeri** gerekir. Çalışma zamanı bağımlılıkları (`curl-cffi`, `flowevent`, `orjson`, `browser-cookie3`, `lz4`) kurulum sırasında otomatik yüklenir.

```bash
# Yayımlanmış paketi kur
python -m pip install webai-provider

# Depo kökünden geliştirme kurulumu
python -m pip install -e .

# Geliştirme ve test araçlarıyla (build, ruff, twine)
python -m pip install -e '.[dev]'

# Offline benchmark paketiyle (pyperf)
python -m pip install -e '.[benchmark]'
```

Bu rehberler `webai-provider` **v2.0.0** API’sini anlatır. Kurulu sürümünü `python -m pip show webai-provider` ile kontrol edebilir, gerekirse `python -m pip install -U webai-provider` ile güncelleyebilirsin.

**Tarayıcı çerezleri hakkında pratik notlar:**
`WebAIAccount.from_browser()` kullanırken işletim sistemine bağlı bazı kısıtlarla karşılaşabilirsin:
- **Linux:** GNOME Keyring veya KWallet kilidinin açık olması gerekir.
- **macOS:** İlk okumada Keychain erişim onayı istenebilir.
- **Windows / Chromium:** Güncel Chrome veya Edge sürümleri çerez veritabanını kilitleyebilir ya da ek şifreleme uygulayabilir. Sorun yaşarsan tarayıcıyı kapatıp tekrar deneyebilir veya çerezleri bir JSON dosyasına kaydedip `WebAIAccount.from_cookies()` ile verebilirsin.

[Yukarı dön](#konu-dizini)

---

## Temel Kavramlar

Bağlantının iki parçası vardır:

1. **`WebAIAccount`:** Hangi platforma (`chatgpt`, `claude`, `gemini`, `grok`) hangi çerezlerle bağlanacağını tutan kimlik nesnesidir. Hesap alanları yeniden atanamaz. Kimlik deposunun içeriği ve `metadata` sözlüğü ise değişebilir. Aynı hesabı kullanarak yeni sağlayıcı oturumları oluşturabilirsin.
2. **`WebAIProvider` (veya `AsyncWebAIProvider`):** Bu hesabı kullanarak siteyle bağlantıyı kuran, modelleri okuyan ve sohbetleri yöneten oturum nesnesidir.

Bir sağlayıcı açıldığında ağ bağlantı havuzu ve oturum kaynakları oluşturur. İşin bittiğinde bunların serbest bırakılması gerekir. Bu yüzden sağlayıcıyı her zaman `with` (asenkron kodda `async with`) bloğu içinde açman önerilir; bloktan çıkıldığında — arada hata oluşsa bile — `close()` otomatik çağrılır.

[Yukarı dön](#konu-dizini)

---

## Hesap Oluşturma (`WebAIAccount`)

Çoğu senaryoda `WebAIAccount.from_browser()` veya `WebAIAccount.from_cookies()` kullanmak yeterlidir; özel bir kimlik deposu bağlayacaksan doğrudan `WebAIAccount(...)` de oluşturabilirsin.

### `WebAIAccount` Alanları

| Parametre | Tip | Varsayılan | Açıklama |
| --- | --- | --- | --- |
| `id` | `str` | Zorunlu | Uygulama içindeki benzersiz hesap kimliği. EventBus kayıtlarında `account_id` olarak geçer. |
| `provider` | `str` | Zorunlu | `"chatgpt"`, `"claude"`, `"gemini"` veya `"grok"` (büyük/küçük harf duyarsız). |
| `credentials` | `CredentialStore \| Path \| str \| dict` | Zorunlu | `dict` verilirse bellek içi `MemoryCredentialStore`, `Path`/`str` verilirse `FileCredentialStore` sarılır. |
| `capabilities` | `tuple[Capability, ...]` | `()` | İsteğe bağlı uygulama metadatası; sağlayıcının kendi destek listesini kısıtlamaz. |
| `metadata` | `dict[str, Any]` | `{}` | Hesapla ilgili kendi ek bilgilerin (ör. kullanıcı etiketi). |
| `name` | `str \| None` | `None` | Okunabilir hesap adı. |
| `is_active` | `bool` | `True` | Uygulama tarafında hesabın aktiflik durumu. |

Aynı `WebAIAccount` nesnesi başarılı bir açılıştan sonra oturum doğrulama bilgisini kimlik parmak iziyle birlikte kısa süreli (300 saniye) önbellekte tutar. Böylece kısa aralıklarla tekrar `WebAIProvider(account)` açtığında gereksiz açılış istekleri atlanır. Çerezler değiştiğinde, süre dolduğunda veya oturum geçersiz kaldığında önbellek otomatik temizlenir.

### Tarayıcıdan Çerez Alma (`from_browser`)

```python
from webaiprovider import WebAIAccount, WebAIProviderType
from webaiprovider.errors import (
    ProfileDatabaseError,
    ProfileKeyringError,
    ProfileSessionNotFoundError,
)

try:
    account = WebAIAccount.from_browser(
        WebAIProviderType.GEMINI,
        browser="chrome",
        id="kisisel-gemini",
        name="Chrome Gemini",
    )
except ProfileSessionNotFoundError:
    print("Chrome içinde açık bir Gemini oturumu bulunamadı.")
except ProfileDatabaseError:
    print("Çerez veritabanı kilitli; tarayıcıyı kapatıp tekrar deneyin.")
except ProfileKeyringError:
    print("Sistem anahtarlığı kilitli.")
```

- **`provider` (`WebAIProviderType | str`):** Hedef platform (`WebAIProviderType.CHATGPT`, `"claude"` vb.).
- **`browser` (`str | None = None`):** Çerezlerin okunacağı tarayıcı. `None` bırakılırsa işletim sisteminin varsayılan tarayıcısı kullanılır. Desteklenen değerler: `"chrome"`, `"chromium"`, `"brave"`, `"brave-origin"`, `"edge"` (`"msedge"`), `"vivaldi"`, `"firefox"`, `"zen"`, `"safari"` *(macOS)*.
- **`id` ve `name` (`str | None = None`):** İsteğe bağlı kimlik ve görünen ad; verilmezse otomatik oluşturulur.

`from_browser()`, yalnızca ilgili sağlayıcıya ait izinli çerezleri izole bir alt süreçte okuyup bellekteki `MemoryCredentialStore` içine koyar; diske kendiliğinden dosya yazmaz.

### Sözlük veya Dosyadan Bağlanma (`from_cookies`)

Tarayıcıya bağımlı kalmadan kendi çerez sözlüğünle veya kalıcı bir JSON dosyasıyla hesap oluşturabilirsin. Sağlayıcı oturum sırasında çerez yenilerse (ör. Gemini'nin düzenli çerez döndürmesi), dosya yolu verilmişse güncellemeler aynı dosyaya atomik olarak kaydedilir.

```python
from pathlib import Path
from webaiprovider import WebAIAccount, WebAIProviderType

# 1) Bellek içi sözlük ile (test veya tek seferlik script için)
acc_mem = WebAIAccount.from_cookies(
    WebAIProviderType.CLAUDE,
    {"sessionKey": "sk-ant-sid01-..."},
    name="Geçici Claude",
)

# 2) Kalıcı JSON dosyası ile (üretim uygulamaları için önerilir)
acc_file = WebAIAccount.from_cookies(
    WebAIProviderType.GROK,
    Path("secrets/grok.json"),
    id="grok-ana",
)
```

Sağlayıcıların oturum doğrulaması için aradığı temel çerezler şunlardır:

| Sağlayıcı | Zorunlu Ana Çerez | Yararlı Ek Çerezler |
| --- | --- | --- |
| **ChatGPT** | `__Secure-next-auth.session-token` *(veya `__Secure-authjs.session-token`)* | `__Host-next-auth.csrf-token`, `oai-did`, `__cf_bm`, `cf_clearance` |
| **Claude** | `sessionKey` | `lastActiveOrg`, `anthropic-device-id`, `__cf_bm`, `cf_clearance` |
| **Gemini** | `__Secure-1PSID` | `OSID`, `__Secure-OSID`, `__Secure-1PSIDTS`, `__Secure-1PSIDCC`, `SID`, `SIDCC` |
| **Grok** | `sso` *(veya `sso-rw`)* | `sso-rw`, `__cf_bm`, `cf_clearance` |

[Yukarı dön](#konu-dizini)

---

## Kimlik Depoları (`CredentialStore`)

`account.store()` çağrısı kimlik verilerini okuyan/yazan depo nesnesini döndürür.

- **`FileCredentialStore(path)`:**
  - `exists() -> bool`: Dosyanın var olup olmadığını kontrol eder.
  - `load() -> dict`: En fazla 1 MiB boyutundaki normal JSON dosyasını okur; dosya yoksa veya bozuksa `AccountPaused` fırlatır.
  - `save(value, *, ensure_ascii=False, newline=False)`: Yol bazlı kilit altında `0o600` izinli geçici dosyaya yazıp `os.fsync` sonrası atomik olarak değiştirir.
  - `delete()`: Kimlik dosyasını siler.

- **`MemoryCredentialStore(initial=None)`:**
  Diske yazmadan, `threading.Lock` korumasıyla ve `deepcopy` izolasyonuyla bellekte sözlük saklar.

Tarayıcıdan aldığın çerezleri ilk seferde kalıcı dosyaya kaydetmek için şu kalıbı kullanabilirsin:

```python
from pathlib import Path
from webaiprovider import WebAIAccount, FileCredentialStore, WebAIProviderType

store = FileCredentialStore(Path.home() / ".config" / "myapp" / "gemini.json")
if not store.exists():
    browser_acc = WebAIAccount.from_browser(WebAIProviderType.GEMINI)
    store.save(browser_acc.store().load())

account = WebAIAccount.from_cookies(WebAIProviderType.GEMINI, store, id="gemini-local")
```

Veritabanı veya şifreli bir kasa kullanacaksan `CredentialStore` protokolünü (`load`, `save`, `exists`, `delete`) uygulayan kendi sınıfını doğrudan `WebAIAccount(..., credentials=...)` parametresine verebilirsin.

[Yukarı dön](#konu-dizini)

---

## Sağlayıcı Oturumu ve Yaşam Döngüsü

### Kurucu Parametreleri

Genel `WebAIProvider(account)` ve `AsyncWebAIProvider(account)` sınıfları, verdiğin `WebAIAccount` nesnesinin sağlayıcısına bakarak otomatik olarak uygun alt sınıfı (`ChatGPTWebAIProvider`, `ClaudeWebAIProvider`, `GeminiWebAIProvider`, `GrokWebAIProvider`) döndürür.

| Parametre | Tip | Varsayılan | Açıklama |
| --- | --- | --- | --- |
| `account` | `WebAIAccount` | Zorunlu | Bağlanılacak hesap. |
| `type` | `WebAIProviderType \| str \| None` | `None` | Sağlayıcı türünü açıkça doğrulamak için; `account.provider` ile çelişirse `ValueError` verir. |
| `transport` | `Transport \| AsyncTransport \| None` | `None` | Özel zaman aşımı, başlık veya HTTP istemcisi vermek için (origin eşleşmelidir). |

*(ChatGPT'nin `session` veya Gemini'nin `notebooks` gibi sağlayıcıya özel parametreleri [04 · Sağlayıcı Rehberleri](provider-guides.md) sayfasındadır.)*

Açık bir sağlayıcı üzerinde şu özellikler her zaman hazırdır: `id`, `type`, `account_id`, `capabilities`, `opened`, `closing`, `closed`, `lifecycle_state`, `chats`, `projects`, `models`, `events`, `hooks`, `transport`.

### Oturum Açma, Kapatma ve `provider.ask()`

Context manager kullanamadığın durumlarda `open()` ve `close()` çağrılarını `try / finally` ile eşleştir:

```python
provider = AsyncWebAIProvider(account)
await provider.open()
try:
    models = await provider.models.list()
    print("Kullanılabilir model sayısı:", len(models))
finally:
    await provider.close()
```

Kapanmış (`CLOSED`) bir sağlayıcı nesnesi tekrar `open()` edilemez; yeni bir oturum için aynı `account` ile yeni bir `WebAIProvider(account)` örneği açılır.

`provider.ask()` her çağrıda varsayılan ayarlarla yeni bir sohbet oluşturur. Sohbet sağlayıcının geçmişinde kalabilir. Aynı konuşmayı sürdürmek için bir kez `provider.chats.create()` çağır ve takip mesajlarını aynı `chat` üzerinden gönder. Sağlayıcı henüz açılmamışsa `provider.ask()` oturumu açıp işlem sonunda kapatır. Art arda çağrılarda bağlantıyı yeniden kullanmak için işlemleri aynı `with` veya `async with` bloğunda yap.

```python
from webaiprovider import WebAIAccount, Attachment, Models, WebAIProvider, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)
provider = WebAIProvider(account)

cevap = provider.ask(
    "Bu belgenin üç maddelik özetini çıkar.",
    model=Models.HIGH,
    attachments=[Attachment.from_path("rapor.pdf")],
)
print(cevap.text)
```

`ask()` parametreleri: `prompt: str`, `model: Models | EffortRef | ModelRef | ModelSelection | str | None = None`, `attachments=None`.

### Oturum Durumu ve Çıkış (`session_status`, `authenticate`, `forget_session`)

- **`session_status() -> SessionStatus`:** Kütüphanenin bildiği oturum durumunu verir. `UNKNOWN`, oturumun geçersiz olduğunun kanıtı değildir. Uzak doğrulama için `authenticate()` kullan.
- **`authenticate() -> SessionStatus`:** Oturumu uzak tarafta doğrular ve iç model/oturum önbelleğini sıfırlar.
- **`forget_session() -> SessionStatus`:** Kayıtlı çerezleri (`store().delete()`) ve bellek içi oturum önbelleğini temizler.

[Yukarı dön](#konu-dizini)

---

[← Belge Haritası](README.md) · **Sonraki:** [02 · Sohbetler, Yanıtlar ve Akış →](chats-and-streaming.md)
