[← Önceki: 03 · Modeller ve Projeler](models-and-projects.md) · [Belge Haritası](README.md) · [Sonraki: 05 · Olaylar, Transport ve Hatalar →](events-transport-and-errors.md)

# 04 · Sağlayıcıya Özel Rehberler

Ortak `WebAIProvider` / `AsyncWebAIProvider` sınıfı dört platformu tek arayüzde birleştirir. Ancak platformların kendi web protokollerinden gelen önemli farkları (proje belleği, sohbet taşıma, çerez kuralları, dosya limitleri ve özel hatalar) bu bölümde ayrıntılı olarak bulabilirsin.

---

## Konu Dizini

- [Özet Karşılaştırma](#özet-karşılaştırma)
- [ChatGPT (`ChatGPTWebAIProvider` / `AsyncChatGPTWebAIProvider`)](#chatgpt-chatgptwebaiprovider--asyncchatgptwebaiprovider)
- [Claude (`ClaudeWebAIProvider` / `AsyncClaudeWebAIProvider`)](#claude-claudewebaiprovider--asyncclaudewebaiprovider)
- [Gemini (`GeminiWebAIProvider` / `AsyncGeminiWebAIProvider`)](#gemini-geminiwebaiprovider--asyncgeminiwebaiprovider)
- [Grok (`GrokWebAIProvider` / `AsyncGrokWebAIProvider`)](#grok-grokwebaiprovider--asyncgrokwebaiprovider)

---

## Özet Karşılaştırma

| Sağlayıcı | Origin | Desteklediği Özel Akışlar | Dikkat Edilecek Noktalar |
| --- | --- | --- | --- |
| **ChatGPT** | `https://chatgpt.com` | `branch()`, `move_to(project \| None)`, `memory_share=False` | Yerleşik C Sentinel uzantısı veya fallback gerekir; `BrowserSession` CDP yakalama destekler. |
| **Claude** | `https://claude.ai` | `move_to(project)` *(çıkarma yok)* | `branch()` yok; sohbet başına maks. 20 dosya, maks. 30 MiB ve yalnız izinli uzantılar. |
| **Gemini** | `https://gemini.google.com` | `branch()`, `GeminiNotebooks` | `move_to()` yok; `OSID` + `__Secure-OSID` + `__Secure-1PSID` gerekir; login yönlendirmesini takip etmez (`GeminiSignInRequired`). |
| **Grok** | `https://grok.com` | `move_to(project)` *(çıkarma yok)*, `assets.grok.com` | `branch()` yok; sohbet dosyası sınırı katı (3 MiB, toplam 10 MiB); kota aşımında `RateLimited`. |

Uygulaman birden fazla sağlayıcıya bağlanıyorsa `WebAIProvider(account)` kullanman yeterlidir. Yalnızca tek bir platformun özel parametrelerini (ör. ChatGPT'de `memory_share=False`) IDE tip denetimiyle kullanmak istediğinde somut sınıfı doğrudan içe aktarabilirsin.

[Yukarı dön](#konu-dizini)

---

## ChatGPT (`ChatGPTWebAIProvider` / `AsyncChatGPTWebAIProvider`)

**Ek kurucu parametreleri:**
Ortak `(account, *, type=None, transport=None)` parametrelerine ek olarak:
- `session` (`BrowserSession | None = None`): Chromium DevTools Protocol (CDP) üzerinden tarayıcı başlığı/oturumu yakalamak için `webaiprovider.providers.chatgpt.auth.BrowserSession(session_file, cdp_url="http://127.0.0.1:9222")` örneği verebilirsin. Varsayılanı hesabın kendi kimlik deposunu kullanır.
- `client_factory` (`Callable[..., Any] | None = None`): Test veya özel istemci enjeksiyonu için.

**Öne çıkan özellikler:**
- **Proje Bellek Kapsamı (`memory_share`):** `projects.create(..., memory_share=False)` ile hesabın genel belleğinden izole, yalnızca o projeye özel bir çalışma alanı açabilirsin. Varsayılan `True` genel belleği paylaşır.
- **Sohbet Taşıma ve Çıkarma (`move_to`):** ChatGPT hem projeye eklemeyi, hem bir projeden diğerine aktarmayı, hem de `chat.move_to(None)` ile sohbeti projeden tamamen çıkarıp ana sohbet listesine döndürmeyi destekler.
- **Dallandırma ve Ekler:** `chat.branch(message_id, *, title=None)` desteklenir. `project.attachments.create_many()` aynı anda en fazla 2 paralel yükleme ile çalışır; batch içinde hata olursa yarım kalan dosyalar temizlenir.
- **Dosya Limitleri:** İstem başına en fazla 10 dosya; genel dosya 512 MiB, görseller 20 MiB, elektronik tablolar 50 MiB.
- **Özel Hatalar:** `ChatGPTError`, `ChatGPTAuthenticationError`, `SentinelCompatibilityError`.

```python
with ChatGPTWebAIProvider(account) as provider:
    proje_a = provider.projects.create("Müşteri A", memory_share=False)
    proje_b = provider.projects.create("Müşteri B", memory_share=False)

    chat = provider.chats.create("Teklif")
    chat.ask("Taslak hazırla.")

    chat.move_to(proje_a)  # Proje A'ya ata
    chat.move_to(proje_b)  # Proje B'ye aktar
    chat.move_to(None)  # Projeden çıkar
```

[Yukarı dön](#konu-dizini)

---

## Claude (`ClaudeWebAIProvider` / `AsyncClaudeWebAIProvider`)

**Kurucu parametreleri:** Ortak `(account, *, type=None, transport=None)`.

**Öne çıkan özellikler:**
- **Organizasyon Keşfi:** Açılışta `/api/bootstrap` üzerinden `"chat"` yeteneğine sahip organizasyonu otomatik seçer (`lastActiveOrg` çerezini dikkate alır) ve `(organization, catalog)` bilgisini hesabın doğrulanmış oturum önbelleğinde tutar.
- **Taslak Başlık Koruma:** `chats.create("Özel Başlık")` ile verdiğin başlık ilk mesajla birlikte sunucuya yazılır; ilk gönderim yarıda kesilse bile sohbet o başlıkla kalır.
- **Web Araması Kaynakları:** Hem eski (`web_search_tool_result`, `citations_delta`) hem güncel (`tool_result` + `name="web_search"`, `citation_start_delta`) Claude kaynak/alıntı biçimlerini `response.sources` içine çözümler.
- **Sohbet Taşıma:** `chat.move_to(project)` ile sohbeti projeye ekleyebilirsin; mevcut projeden başka projeye taşıma veya `None` ile çıkarma desteklenmez. `chat.branch()` bulunmaz.
- **Dosya Limitleri:** Sohbet başına en fazla 20 dosya, dosya başına maks. 30 MiB. Yalnızca desteklenen uzantılar kabul edilir: `.pdf`, `.docx`, `.csv`, `.txt`, `.html`, `.htm`, `.odt`, `.rtf`, `.epub`, `.json`, `.xlsx`, `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`.
- **Özel Hatalar:** `ClaudeError`, `ClaudeCompatibilityError`, `ClaudeRequestNotSent`, `ClaudeStreamError` (`webaiprovider.providers.claude.errors`).

[Yukarı dön](#konu-dizini)

---

## Gemini (`GeminiWebAIProvider` / `AsyncGeminiWebAIProvider`)

**Ek kurucu parametreleri:**
Ortak `(account, *, type=None, transport=None)` parametrelerine ek olarak:
- `client_factory` (`Any = None`): Test veya özel protokol istemcisi için.
- `notebooks`: Senkron sağlayıcı `webaiprovider.providers.gemini.GeminiNotebooks`, asenkron sağlayıcı `webaiprovider.providers.gemini.async_client.AsyncGeminiNotebooks` kullanır. Varsayılan okuyucu hesap kimliğinden otomatik oluşturulur.

**Öne çıkan özellikler:**
- **Çerez Gereksinimi ve Yenileme:** Tam oturum için `__Secure-1PSID` yanında `gemini.google.com` alan adına ait `OSID` ve `__Secure-OSID` çerezleri gerekir. Eksik veya süresi dolmuş oturum Google giriş sayfasına yönlendirirse kütüphane yönlendirmeyi takip etmez, doğrudan `GeminiSignInRequired` (`AccountPaused` alt sınıfı) yükseltir. Açık oturum sırasında `RotateCookies` ile yenilenen çerezler `CredentialStore` deposuna kaydedilir.
- **Notebook Tabanlı Projeler:** `provider.projects`, Gemini Notebook kaynakları üzerinden çalışır; metin ve dosya ekleyip silebilirsin. Ancak Gemini'de `chat.move_to(...)` desteklenmez (`GeminiChat` sınıfında bulunmaz).
- **Dallandırma:** `chat.branch(message_id, *, title=None)` ve `response.branch()` desteklenir.
- **İzole Dosya Yükleme:** Dosya yükleme havuzu hesap çerezleri gönderilmeden ve yükleme yanıtından çerez kaydedilmeden ayrı çalışır. İstem başına maks. 10 dosya, genel 100 MiB, video 2 GiB sınırı uygulanır.
- **Özel Hatalar:** `GeminiError`, `GeminiSignInRequired`, `GeminiGenerationRejected` *(sunucunun sayısal red kodunu `code=...` olarak taşır)*, `GeminiNotFoundError`, `GeminiCompatibilityError`, `GeminiStreamError` (`webaiprovider.providers.gemini.errors`).

[Yukarı dön](#konu-dizini)

---

## Grok (`GrokWebAIProvider` / `AsyncGrokWebAIProvider`)

**Ek kurucu parametreleri:** Ortak `(account, *, type=None, transport=None)` + `client_factory: Any = None`.

**Öne çıkan özellikler:**
- **Workspaces ve Taşıma:** `provider.projects` üzerinden çalışma alanları yönetilir; `chat.move_to(project)` desteklenir, `chat.branch()` yoktur.
- **Görseller ve Çıktılar:** `assets.grok.com` bağlantıları, `generatedImageUrls`, ek dosya metadataları ve yanıt içi Markdown bağlantıları `response.artifacts` olarak çıkarılır ve `chat.download(artifact)` ile indirilebilir.
- **Dosya Limitleri:** Sohbet eklerinde en fazla 10 dosya, **dosya başına en fazla 3 MiB** ve **toplam 10 MiB** sınırı vardır (Workspace proje ekleri 20 MiB'a kadar çıkabilir). Yalnızca belge ve veri uzantıları (`.pdf`, `.txt`, `.md`, `.csv`, `.json`, `.doc`, `.docx`, `.ppt`, `.pptx`, `.xls`, `.xlsx`, `.xlsm`, `.odt`, `.ods`, `.odp`, `.rtf`) kabul edilir.
- **Model ve Efor:** `GrokModels` ile model seçimi desteklenir ancak akıl yürütme eforu seçimi (`Capability.EFFORT`) bulunmaz (`efforts` boştur).
- **Kota ve Hatalar:** Grok ayrı bir hata alt sınıfı tanımlamaz; doğrudan ortak `WebAIProviderError`, `RateLimited` (`err.retry_after` ve `err.message_submitted`) ve `AccountPaused` istisnalarını yükseltir.

[Yukarı dön](#konu-dizini)

---

[← Önceki: 03 · Modeller ve Projeler](models-and-projects.md) · [Belge Haritası](README.md) · **Sonraki:** [05 · Olaylar, Transport ve Hatalar →](events-transport-and-errors.md)
