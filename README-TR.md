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
        operation = chat.messages.create("Merhaba!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

`browser` verilmezse `Account.from_browser()` varsayılan tarayıcıyı kullanır. Tarayıcı çerezlerine erişmek için tarayıcının kapatılması veya işletim sistemi anahtarlığının kilidinin açılması gerekebilir. Kimlik bilgilerini doğrudan vermek için `Account.from_cookies(provider, cookies)` kullanın ve çerezleri kendi güvenli deponuzda saklayın.

Bloklayan kodlarda sağlayıcıya özel senkron sınıfı kullanabilirsiniz. Sağlayıcı açık kaldığı sürece tek bir event loop portalı kullanılır:

```python
from webaiprovider import Account, ChatGPTProvider, ProviderType

account = Account.from_browser(ProviderType.CHATGPT)
with ChatGPTProvider(account) as provider:
    chat = provider.chats.create("Hızlı başlangıç")
    response = chat.messages.create("Merhaba!").start()
```

## Temel API

- `BaseProvider`, ortak hesap ve sağlayıcı kimliğini taşır. Asenkron işlemlerde `AsyncChatGPTProvider` (ve diğer sağlayıcıların karşılık gelen sınıfı), bloklayan işlemlerde `ChatGPTProvider` kullanılır; `AsyncProvider` ve `Provider`, `ProviderType` ile ortak kullanım içindir.
- `ChatGPTProvider`, `ClaudeProvider`, `GeminiProvider` ve `GrokProvider` sağlayıcıya özel özellikler sunar. ChatGPT projeleri varsayılan/genel bellek kapsamını kullanır; yalnızca proje belleği için `memory_share=False` verilir.
- `provider.models.list()` hesabın kullanılabilir modellerini yükler ve oturum boyunca önbelleğe alır. Yeni katalog için `refresh=True` veya `provider.models.refresh()` kullanın. `find()`, `search()` ve `get()` bu katalogda seçim yapar.
- `chat.messages.create()`, `edit()` ve `retry()` birer `MessageOperation` döndürür. Akış olayları için `operation.stream()` kullanın veya tamamlanmış yanıtı `operation.wait()` ile alın. Bu yöntemler aynı gönderim işlemini paylaşır.
- Proje, ek, yeniden deneme, düzenleme, dallandırma ve sohbet taşıma özellikleri sağlayıcıya göre değişir. Sağlayıcıya özel sohbet sınıfları yalnızca desteklenen işlemleri sunar; desteklenmeyen işlemler `FeatureUnavailable` gibi tipli `ProviderError` alt sınıflarıyla bildirilir.

Paket, tip denetimli kullanım için model başvuruları ve efor seçenekleri sunar. Hesaptan keşfedilen modeller, o hesap için yetkili kaynaktır; aboneliğe ve sağlayıcı değişikliklerine göre farklılık gösterebilir.

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

## Bağlantılar

- [Değişiklik günlüğü](CHANGELOG.md)
- [Hata ve istek bildirimi](https://github.com/TahsinCr/webai-provider/issues)
- [Depo](https://github.com/TahsinCr/webai-provider)
- [SiopeChat](https://github.com/TahsinCr/chatgpt-web)

GPL-3.0-only lisanslıdır. Ayrıntılar için [LICENSE](LICENSE) ve [COPYRIGHT](COPYRIGHT) dosyalarına bakın.
