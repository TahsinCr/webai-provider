# webai-provider

ChatGPT, Gemini, Claude ve Grok web oturumları için bağımsız Python istemcileri. v2 API; hesap kapsamlı asenkron oturumlar, tipli model başvuruları ve akış üzerinden çalışan mesaj işlemleri üzerine kuruludur.

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

from webaiprovider import Account, OutputDelta


async def main() -> None:
    account = Account.from_browser("chatgpt", browser="firefox")
    session = account.connect()

    async with session:
        models = await session.models.list()
        if not models:
            raise RuntimeError("No models are available for this account")

        chat = await session.chats.create("Hızlı başlangıç")
        operation = chat.messages.create("Merhaba!", model=models[0])

        async for event in operation.stream():
            if isinstance(event, OutputDelta):
                print(event.delta, end="", flush=True)


asyncio.run(main())
```

`browser` verilmezse `Account.from_browser()` varsayılan tarayıcıyı kullanır. Tarayıcı çerezlerine erişmek için tarayıcının kapatılması veya işletim sistemi anahtarlığının kilidinin açılması gerekebilir. Kimlik bilgilerini doğrudan vermek için `Account.from_cookies(provider, cookies)` kullanın ve çerezleri kendi güvenli deponuzda saklayın.

## Temel API

- `Account`, bir sağlayıcının kimlik bilgilerini tutar ve `account.connect()` ile `ProviderSession` oluşturur.
- `ProviderSession`; hesaba ait `chats`, `projects` ve `models` koleksiyonlarını yönetir, `async with` ile güvenli biçimde kapatılabilir.
- `session.models.list()` hesabın kullanılabilir modellerini yükler ve oturum boyunca önbelleğe alır. Yeni katalog için `refresh=True` veya `session.models.refresh()` kullanın. `find()`, `search()` ve `get()` bu katalogda seçim yapar.
- `chat.messages.create()`, `edit()` ve `retry()` birer `MessageOperation` döndürür. Akış olayları için `operation.stream()` kullanın veya tamamlanmış yanıtı `operation.wait()` ile alın. Bu yöntemler aynı gönderim işlemini paylaşır.
- Proje, ek, yeniden deneme, düzenleme ve dallandırma özellikleri sağlayıcıya göre değişir. Desteklenmeyen işlemler `FeatureUnavailable` gibi tipli `ProviderError` alt sınıflarıyla bildirilir.

Paket, tip denetimli kullanım için model başvuruları ve efor seçenekleri sunar. Hesaptan keşfedilen modeller, o hesap için yetkili kaynaktır; aboneliğe ve sağlayıcı değişikliklerine göre farklılık gösterebilir.

## Güvenlik ve çalışma biçimi

- Tarayıcı kimlik bilgileri yerel olarak okunur. Kütüphane bunları SiopeChat sunucusuna göndermez.
- Çerezleri ve kimlik bilgisi dosyalarını koruyun; günlüklerde, hata bildirimlerinde veya kaynak kontrolünde paylaşmayın.
- İndirme işlemleri yalnızca sağlayıcının verdiği `DownloadSource` değerlerini kabul eder; herkese açık ağ hedeflerini doğrular, çözümlenen IP adresini sabitler ve yönlendirmeleri izlemez.
- Sağlayıcı hataları İngilizce mesajlı, tipli istisnalar olarak fırlatılır. Sağlayıcı oturumu bağlı olmayan sohbet, sahte yanıt döndürmek yerine açıkça hata verir.

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
