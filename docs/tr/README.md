# WebAI Provider Türkçe Belgeleri

`webai-provider`, **ChatGPT**, **Claude**, **Gemini** ve **Grok** web oturumlarıyla Python üzerinden çalışmanı sağlayan bağımsız bir istemci kütüphanesidir. Aynı nesne modeli üzerinde hem doğrudan bloklayan (senkron) hem de `asyncio` tabanlı (asenkron) arayüz sunar.

> **Not:** Sağlayıcıların web arayüzleri ve özel web protokolleri önceden haber verilmeksizin değişebilir. Bu kütüphane bağımsız bir projedir; OpenAI, Anthropic, Google veya xAI ile resmi bir bağlantısı yoktur.

---

## Konu Dizini

- [Nereden Başlamalısın?](#nereden-başlamalısın)
- [Doküman Haritası](#doküman-haritası)
- [Sağlayıcı Karşılaştırması](#sağlayıcı-karşılaştırması)
- [Senkron mu Asenkron mu?](#senkron-mu-asenkron-mu)

---

## Nereden Başlamalısın?

- İlk bağlantıyı kurmak için [Başlangıç ve hesaplar](getting-started.md) rehberini izle. Tarayıcı oturumunu bağlayıp ilk mesajı gönderirsin.
- Bir uygulamaya akış ekliyorsan [Sohbetler, yanıtlar ve akış](chats-and-streaming.md) rehberinde metin güncellemelerini, üretim sonunu ve kesin işlem sonucunu nasıl izleyeceğini bulabilirsin.
- Bir metot imzası veya dönüş tipi arıyorsan [API başvurusuna](api-reference.md) geç. Sağlayıcılar arasındaki farklar için [sağlayıcı rehberlerine](provider-guides.md) bak.

[Yukarı dön](#konu-dizini)

---

## Doküman Haritası

| Doküman | İçerik |
| --- | --- |
| **[01 · Başlangıç ve Hesaplar](getting-started.md)** | Kurulum, tarayıcıdan veya çerezden `WebAIAccount` oluşturma, `FileCredentialStore` / `MemoryCredentialStore`, `WebAIProvider` yaşam döngüsü ve `provider.ask()`. |
| **[02 · Sohbetler, Yanıtlar ve Akış](chats-and-streaming.md)** | Sohbet oluşturma/arama, mesaj ve dosya ekleri, `Response.parts` (`ThinkingPart`, `TextPart`), canlı akış (`stream`), `edit` / `retry` / `branch`, `wait_for()`, iptal ve `chat.hooks`. |
| **[03 · Modeller ve Projeler](models-and-projects.md)** | Model kataloğu (`300 sn` TTL), `Models` katmanları, düşünme eforu seçimi, statik kataloglar, projeler, proje talimatları ve proje dosya/metin ekleri. |
| **[04 · Sağlayıcı Rehberleri](provider-guides.md)** | ChatGPT (`memory_share`, `move_to(None)`, `BrowserSession`), Claude (organizasyon keşfi, web search), Gemini (`OSID` çerezleri, `GeminiNotebooks`, yükleme izolasyonu) ve Grok (`assets.grok.com`, `RateLimited`). |
| **[05 · Olaylar, Transport ve Hatalar](events-transport-and-errors.md)** | `EventBus` abonelikleri, `provider.transport` (`HTTPTransport`, `HttpSession`, `TransportConfig`), SSE/JSPB frame çözücüler, hata hiyerarşisi ve `SendTiming`. |
| **[06 · API Başvuru Kartı](api-reference.md)** | Sık kullanılan imzalar, dönüş tipleri, akış olayları ve istisnalar. |

[Yukarı dön](#konu-dizini)

---

## Sağlayıcı Karşılaştırması

Çalışma anında açık veya kapalı bir sağlayıcının desteklediği yetenekleri `provider.capabilities`, dosya sınırlarını ise `provider.attachment_policy()` ile okuyabilirsin.

| Özellik | ChatGPT | Claude | Gemini | Grok |
| --- | :---: | :---: | :---: | :---: |
| Sohbet, geçmiş, ad değiştirme, silme, geçici sohbet | Evet | Evet | Evet | Evet |
| Model keşfi (`Capability.MODELS`) | Evet | Evet | Evet | Evet |
| Efor seçimi (`Capability.EFFORT`) | Evet | Evet | Evet | Hayır |
| Mesaj düzenleme (`edit`) ve yeniden deneme (`retry`) | Evet | Evet | Evet | Evet |
| Sohbet dallandırma (`chat.branch()`) | Evet | Hayır | Evet | Hayır |
| Projeler ve proje dosya/metin ekleri | Evet | Evet | Evet *(Notebook)* | Evet *(Workspace)* |
| Sohbeti projeye taşıma (`chat.move_to(project)`) | Evet | Evet | Hayır | Evet |
| Sohbeti projeden çıkarma (`chat.move_to(None)`) | Evet | Hayır | Hayır | Hayır |
| İstem/sohbet dosya sınırı | 10 dosya · 512 MiB *(görsel 20 MiB, tablo 50 MiB)* | 20 dosya (sohbet) · 30 MiB *(belirli uzantılar)* | 10 dosya · 100 MiB *(video 2 GiB)* | 10 dosya · 3 MiB *(toplam 10 MiB, belge/kod)* |

[Yukarı dön](#konu-dizini)

---

## Senkron mu Asenkron mu?

Çağrının bulunduğu ortamı esas al. Olay döngüsü içinde asenkron arayüzü, bloklayan kodda senkron arayüzü kullan:

- **Senkron (`WebAIProvider`, `ChatGPTWebAIProvider`, `ClaudeWebAIProvider`, `GeminiWebAIProvider`, `GrokWebAIProvider`):**
  Arka planda gizli bir `asyncio` olay döngüsü açmaz. Her çağrı doğrudan çağıran thread üzerinde bloklayan `curl_cffi` ile yürür. Komut satırı araçları, klasik web çatıları (Flask, Django) ve düz Python script'leri için en sade seçenektir.
- **Asenkron (`AsyncWebAIProvider`, `AsyncChatGPTWebAIProvider`, `AsyncClaudeWebAIProvider`, `AsyncGeminiWebAIProvider`, `AsyncGrokWebAIProvider`):**
  `async` / `await` ile çalışır. FastAPI, aiohttp, botlar veya aynı anda birden fazla sohbet akışını paralel yöneten uygulamalar için uygundur. Ayrıca aynı sohbetteki örtüşen `chat.responses.list()` çağrılarını tek ağ isteğinde birleştirir.

[Yukarı dön](#konu-dizini)

---

**Sonraki:** [01 · Başlangıç ve Hesaplar →](getting-started.md)
