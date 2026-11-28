[← Önceki: 06 · Hızlı API Başvuru Kartı](api-reference.md) · [Belge Haritası](README.md)

# 07 · Araçlar ve Derin Araştırma

> **v2.1.0 ile eklendi:** Açık araç sözleşmeleri, Derin Araştırma yaşam döngüsü API'leri ve `Artifact` okuma/kaydetme yardımcıları `v2.1.0` sürümünde eklendi.

`chat.ask()`, `chat.stream()` ve `chat.responses.create()` çağrılarına `tool=` ve `tool_timeout=` parametreleriyle açık araç sözleşmeleri (`WebSearchTool`, `ImageGenerationTool`, `DeepResearchTool` veya `GeminiDeepResearchTool`) verebilirsin. Sağlayıcı, ağ isteği göndermeden önce istenen aracı `provider.capabilities` üzerinden doğrular.

---

## Konu Dizini

- [Sağlayıcıya Göre Desteklenen Araçlar](#sağlayıcıya-göre-desteklenen-araçlar)
- [Web Araması (`WebSearchTool`)](#web-araması-websearchtool)
- [Görsel Üretimi (`ImageGenerationTool`)](#görsel-üretimi-imagegenerationtool)
- [Derin Araştırma (`DeepResearchTool` ve `GeminiDeepResearchTool`)](#derin-araştırma-deepresearchtool-ve-geminideepresearchtool)
- [Araştırma Yaşam Döngüsü ve Bekleme](#araştırma-yaşam-döngüsü-ve-bekleme)
- [Akışta Araştırma Olayları](#akışta-araştırma-olayları)

---

## Sağlayıcıya Göre Desteklenen Araçlar

| Araç | Yetenek (`Capability`) | ChatGPT | Gemini | Grok | Claude |
| --- | --- | :---: | :---: | :---: | :---: |
| `WebSearchTool()` | `Capability.WEB_SEARCH` | Evet | Evet | Evet | Evet |
| `ImageGenerationTool()` | `Capability.IMAGE_GENERATION` | Evet | Evet | Evet | Hayır |
| `DeepResearchTool(auto_start=False)` | `Capability.DEEP_RESEARCH` | Evet | Evet | Hayır | Hayır |
| `GeminiDeepResearchTool(auto_start=False, sources=...)` | `Capability.DEEP_RESEARCH` | Hayır | Evet | Hayır | Hayır |

- Desteklenmeyen bir araç verildiğinde istek atılmadan önce `FeatureUnavailable` fırlatılır.
- `GeminiDeepResearchTool` gibi sağlayıcıya özel bir aracı başka bir sağlayıcıya veya genel `WebAIProvider` / `AsyncWebAIProvider` arayüzüne verirsen `UnsupportedOperationError` yükselir.
- `tool_timeout` pozitif sonlu bir `int` / `float` (saniye) veya `None` kabul eder.
- **Claude** üzerinde web araması (`web_search_v0`) gönderim isteklerinde zaten varsayılan olarak açıktır; bu nedenle `WebSearchTool()` vermek istek gövdesini değiştirmeden kabul edilir.

[Yukarı dön](#konu-dizini)

---

## Web Araması (`WebSearchTool`)

Sağlayıcının canlı web sonuçlarına dayanarak yanıt üretmesi ve `response.sources` alanını doldurması için `WebSearchTool()` kullan:

```python
from webaiprovider import WebAIAccount, AsyncChatGPTWebAIProvider, WebAIProviderType, WebSearchTool

account = WebAIAccount.from_browser(WebAIProviderType.CHATGPT)

async with AsyncChatGPTWebAIProvider(account) as provider:
    chat = await provider.chats.create("Web Search", temporary=True)
    response = await chat.ask(
        "Python 3.13.0 resmi çıkış tarihi nedir?",
        tool=WebSearchTool(),
    )

    print(response.text)
    for source in response.sources:
        print(source.title, source.url, f"cited={source.cited}")
```

`Source.cited` alanı, sağlayıcı doğrudan alıntılanan kaynakları genel arama sonuçlarından ayırabiliyorsa `True` veya `False`, aksi hâlde `None` olur.

[Yukarı dön](#konu-dizini)

---

## Görsel Üretimi (`ImageGenerationTool`)

Görsel üretmek ve `response.artifacts` içinden dönen `Artifact` nesnelerini okumak veya diske kaydetmek için `ImageGenerationTool()` kullan:

```python
from webaiprovider import WebAIAccount, GeminiWebAIProvider, ImageGenerationTool, WebAIProviderType

account = WebAIAccount.from_browser(WebAIProviderType.GEMINI)

with GeminiWebAIProvider(account) as provider:
    chat = provider.chats.create("Images", temporary=True)
    response = chat.ask(
        "Düz beyaz arka plan üzerinde sade kırmızı bir daire çiz.",
        tool=ImageGenerationTool(),
    )

    for artifact in response.artifacts:
        if artifact.mime_type.startswith("image/"):
            data = chat.download(artifact)
            # Sağlayıcı ham baytları doğrudan artifact içine koyduğunda:
            # data = artifact.read_bytes()
            # artifact.save("daire.png")
            print(artifact.name, artifact.mime_type, len(data))
```

- Yalnızca görsel dönen yanıtlarda `response.parts` boş kalabilir, üretilen görsel `response.artifacts` içinde gelir.
- **ChatGPT** üzerinde `temporary=True` geçici sohbetlerde yapay zekâ ile görsel üretimi desteklenmez; normal bir sohbet açıp işin bitince silebilirsin.
- Satır içi (inline) içerik taşıyan çıktılar `artifact.read_bytes()`, `artifact.read_text(encoding="utf-8")` ve `artifact.save(path)` yardımcılarını sunar. Uzak indirme bağlantısı olanlar `chat.download(artifact)` ile indirilir.
- **Gemini** üzerinde üretilen görsellerin yanı sıra satır içi Canvas belgeleri (`immersive_entry_chip`) ve kod yürütme çıktısı dosyalar (`contribution.usercontent.google.com`) da `response.artifacts` içinde sunulur ve `chat.download(artifact)` ya da `artifact.read_text()` ile okunup indirilebilir *(v2.1.1)*.

[Yukarı dön](#konu-dizini)

---

## Derin Araştırma (`DeepResearchTool` ve `GeminiDeepResearchTool`)

`DeepResearchTool(auto_start=False)`, ChatGPT ve Gemini üzerinde çok adımlı derin araştırma akışını başlatır:

- `auto_start=False` (varsayılan): Araştırma planı oluştuktan sonra durur (`ResearchStatus.WAITING_FOR_APPROVAL`); böylece `response.start_research()` çağırmadan önce `response.research.plan` içeriğini inceleyebilirsin.
- `auto_start=True`: Desteklendiğinde plan onayını beklemeden araştırmayı başlatır.

Gemini'de araştırmanın hangi kaynakları tarayacağını seçmek için `webaiprovider.providers.gemini` altından `GeminiDeepResearchTool` kullanabilirsin:

```python
from webaiprovider import WebAIAccount, AsyncGeminiWebAIProvider, WebAIProviderType
from webaiprovider.providers.gemini import DeepResearchSources, GeminiDeepResearchTool

account = WebAIAccount.from_browser(WebAIProviderType.GEMINI)

async with AsyncGeminiWebAIProvider(account) as provider:
    chat = await provider.chats.create("Deep Research")
    response = await chat.ask(
        "Python 3.13 free-threading yaklaşımının artı ve eksilerini karşılaştır.",
        tool=GeminiDeepResearchTool(
            auto_start=False,
            sources=DeepResearchSources.WEB_AND_CHAT,
        ),
        tool_timeout=180.0,
    )
```

`DeepResearchSources` seçenekleri:

- `DeepResearchSources.WEB` (`"web"` — `GeminiDeepResearchTool` için varsayılan)
- `DeepResearchSources.CHAT` (`"chat"`)
- `DeepResearchSources.WEB_AND_CHAT` (`"web_and_chat"` — Gemini'ye genel `DeepResearchTool` verildiğinde varsayılan)

[Yukarı dön](#konu-dizini)

---

## Araştırma Yaşam Döngüsü ve Bekleme

`DeepResearchTool` ile dönen yanıtlarda `response.research` alanı bir `ResearchState` nesnesi taşır:

- `state.status: ResearchStatus` — `PLANNING`, `WAITING_FOR_APPROVAL`, `RUNNING`, `PAUSED`, `COMPLETED`, `CANCELLED` veya `FAILED`
- `state.plan: ResearchPlan | None` — `plan_id`, `title`, `steps: tuple[str, ...]`, `countdown_seconds`
- `state.session_id`, `state.conversation_id`, `state.message_id`, `state.metadata`

`Response` üzerindeki kontrol metotları:

- `response.start_research(*, timeout=None)` — Onay bekleyen planı başlatır (sağlayıcıya göre senkron veya `await`).
- `response.pause_research()` — Desteklendiğinde süren araştırmayı duraklatır.
- `response.cancel_research()` — Araştırmayı iptal eder.
- `response.wait_research(*, timeout=None)` — Araştırma bitiş durumuna (`COMPLETED`, `CANCELLED`, `FAILED`) ulaşana kadar bekler ve raporu (`response.text`, `response.sources` ve varsa `text/markdown` rapor `Artifact`'ı) içeren güncel `Response` nesnesini döndürür.

```python
from webaiprovider import DeepResearchTool, ResearchStatus

response = await chat.ask(
    "WebAssembly GC alanındaki güncel gelişmeleri araştır.",
    tool=DeepResearchTool(auto_start=False),
)

if response.research and response.research.status == ResearchStatus.WAITING_FOR_APPROVAL:
    print("Plan:", response.research.plan)
    response = await response.start_research()

final = await response.wait_research(timeout=300.0)
print(final.text)
for artifact in final.artifacts:
    if artifact.mime_type == "text/markdown":
        artifact.save("rapor.md")
```

[Yukarı dön](#konu-dizini)

---

## Akışta Araştırma Olayları

Derin araştırma isteğini `operation.stream()` ile izlerken iki yeni `OperationEvent` olayı gelebilir:

- `OperationEvent.RESEARCH_PLAN_READY` → `ResearchPlanReady(plan: ResearchPlan, state: ResearchState)`
- `OperationEvent.RESEARCH_PROGRESS` → `ResearchProgressEvent(status: ResearchStatus, message: str, state: ResearchState | None)`

```python
from webaiprovider import DeepResearchTool, ResearchPlanReady, ResearchProgressEvent

op = chat.responses.create(
    "2026 yılında Python paketleme ekosisteminin durumunu özetle.",
    tool=DeepResearchTool(auto_start=True),
)

async for event in op.stream():
    if isinstance(event, ResearchPlanReady):
        print("Plan adımları:", event.plan.steps)
    elif isinstance(event, ResearchProgressEvent):
        print(f"[{event.status.value}] {event.message}")

response = await op.wait()
```

[Yukarı dön](#konu-dizini)

---

[← Önceki: 06 · Hızlı API Başvuru Kartı](api-reference.md) · [Belge Haritası](README.md)
