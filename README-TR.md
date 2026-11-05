[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![Sürüm: v1.1-beta][version-shield]](CHANGELOG.md)
[![GPL-3.0-only][license-shield]](LICENSE)

[English](README.md) | **Türkçe** | [Русский](README-RU.md)

# webai-provider

### ChatGPT, Gemini, Claude ve Grok web oturumları için bağımsız Python istemcileri.

Sağlayıcıların web oturumlarını ortak bir asenkron Python arayüzü üzerinden
kullanın. Bu resmî olmayan istemciler geliştirici API anahtarı, Codex veya
Claude Code kullanmaz. Model erişimi ve kullanım sınırları oturum açılan hesaba
bağlıdır; sağlayıcı sitelerindeki değişiklikler uyumluluğu etkileyebilir.

<p align="center">
  <a href="#kurulum"><strong>Kurulum</strong></a> &nbsp; | &nbsp;
  <a href="#hızlı-başlangıç"><strong>Hızlı başlangıç</strong></a> &nbsp; | &nbsp;
  <a href="#api-davranışı"><strong>API</strong></a> &nbsp; | &nbsp;
  <a href="#geliştirme"><strong>Geliştirme</strong></a>
</p>

## Kurulum

Tüm sağlayıcılar için Python 3.10+ gerekir.

```sh
git clone https://github.com/TahsinCr/webai-provider.git
cd webai-provider
python -m pip install .
```

Geliştirme araçlarını da kurmak için:

```sh
python -m pip install -e ".[dev]"
```

## Çalışma zamanı

ChatGPT'nin parmak izi ve kanıt üretimi ile desteklenen Turnstile yolu saf
Python'da çalışır; Node.js veya başka bir JavaScript motoru gerekmez. Gerekli bir
kontrol desteklenmiyorsa mesaj gönderilmeden hata verilir. Tarayıcı profili ve
CDP erişimi açıkça çağrılmadıkça başlamaz; paketi içe aktarmak tarayıcı açmaz ve
ağ isteği yapmaz. Gemini kendi HTTPS taşımasını kullanır.

## Sağlayıcı özellikleri

| Sağlayıcı | Web oturumu özellikleri |
| --- | --- |
| ChatGPT | Sohbetler, projeler, model keşfi, akış, desteklenen mesaj işlemleri, kaynaklar ve oluşturulan dosyalar |
| Gemini | Sohbetler, not defterleri, model ve efor keşfi, akış, desteklenen mesaj işlemleri, kaynaklar ve oluşturulan medya |
| Claude | Sohbetler, çalışma alanları ve projeler, model keşfi, akış, kaynaklar ve oluşturulan dosyalar |
| Grok | Sohbetler, çalışma alanları, hesaba özel model keşfi, akış ve yeniden deneme; yayımlanan düşünme özetleri, web arama kaynakları, talimatlar, 3 MiB’a kadar proje kaynakları ve sohbet üyeliği |

Hesabın güncel kataloğu ve sağlayıcı yetenekleri belirleyicidir. Boş bir sonuç,
işlemin desteklenmediği anlamına gelmez.

## Hızlı başlangıç

```python
import asyncio

from providers import ProviderClient, SendRequest, TextDelta, ReplaceText


async def main():
    async with ProviderClient() as client:
        account = client.connect(
            "claude", "personal", credential_file="/private/claude-session.json"
        )
        await account.authenticate()
        catalog = await account.models()
        model = catalog.default_chat_model
        request = SendRequest(
            provider_id="claude",
            account_id="personal",
            chat_id="yerel-sohbet-kimligi",
            prompt="Merhaba",
            model=model.id if model else None,
        )
        async for event in client.stream(request):
            if isinstance(event, (TextDelta, ReplaceText)):
                print(event.channel, event.text)


asyncio.run(main())
```

## Hesaplar ve güvenlik

Kimlik bilgilerini çağıran uygulama yönetir. Bir dosya yolu verebilir veya
`providers.session.CredentialStore` içindeki `load`, `save`, `exists` ve
`delete` arayüzünü uygulayabilirsiniz. Paket içe aktarılırken tarayıcı açılmaz
ve ağa bağlanılmaz; profil okuma ve CDP işlemleri açıkça çağrılır.

Her hesabın adaptörü, bağlantıları ve eşzamanlılık sınırları ayrıdır. Dosya
tanımlayıcıları ve kaynaklar ileti metaverisinde taşınır; indirme için
sağlayıcının kendi kimlik doğrulamalı çözümleyicisi kullanılır. İptal edilen
veya sonucu belirsiz istekler otomatik olarak yeniden gönderilmez.
İlk etkileşimli işlemden önce `await account.prepare()` çağrılabilir. Model
katalogları hesap başına beş dakika önbelleğe alınır; yenilemek için
`await account.models(force=True)` kullanın.

## API davranışı

`ProviderClient` hesapları, akışları ve bağlantıların kapanışını yönetir;
`client.account(provider_id, account_id)` ise o hesaba ait `Backend` arayüzünü
verir. Bu arayüz oturum doğrulama, model kataloğu, sayfalı sohbet ve proje
listeleri, sohbet geçmişi, ad değiştirme/silme, proje kaynakları, dosya yükleme
ve indirme çözümleme işlemlerini sunar. `describe()` ve `project_options()` ile
sağlayıcının desteklediği işlemleri öğrenebilirsiniz; boş liste, özelliğin
desteklenmediği anlamına gelmez.

Akışlar metin parçaları, metnin yerini alan güncellemeler, sohbet kimliği ve yanıt
metaverisi üretir. Kaynaklar ve oluşturulan dosyalar metaveride yer alır; dosya
içeriği akışa eklenmez. `resolve_download` indirme bağlantısını hesabın
kimlik bilgileriyle çözer. Yayınlanmış kaynaklar, analiz metninden ve yanıt
metnindeki sıradan bağlantılardan ayrı değerlendirilir.

Düzenleme ve yeniden deneme işlemleri listedeki sıra numarasıyla değil, sağlayıcı
mesaj kimliğiyle yapılır. Dallanma ChatGPT ve Gemini'de desteklenir; Claude bu
pakette dallanma sunmaz. İptal edilen veya gönderim sonucu belirsiz kalan bir
istek otomatik yinelenmez. `ProviderError.code` kararlı bir hata kodudur;
`message_submitted` değerinin `False` olması mesajın sağlayıcıya ulaşmadığını
garanti eder, `None` ise sonucun kanıtlanamadığını belirtir.

Kalıcı depolama ve senkronizasyon çağıran uygulamanın sorumluluğundadır.
`get_chat()` sağlayıcıdan erişilebilen geçmişi döndürür; taşımanın sunmadığı
uzak sayfalamayı kendiliğinden oluşturmaz.

## SiopeChat ile kullanım

SiopeChat bu dağıtımı sabitlenmiş bir PyPI bağımlılığı olarak kurar; uygulama
wheel'i artık `providers` içe aktarma paketini ayrıca içermez. SiopeChat'i normal
şekilde, Git submodule olmadan klonlayın. Sağlayıcı sürümünü uygulamanın bağımlılık
sabitlemesi ve kilit dosyası belirler; uygulamayı güncelleyip kurulum betiğini
çalıştırmak seçilmiş provider sürümünü de kurar. Kurulum ayrıntıları için
[SiopeChat README](https://github.com/TahsinCr/siopechat/blob/main/README-TR.md)
dosyasına bakın.

Provider değişikliklerini SiopeChat ile sınamak için belirli bir provider sürümü
yayımlayın veya kurun; ardından uygulamanın bağımlılık sabitlemesini ve kilit
dosyasını güncelleyin. İki proje kaynak kodunu, testlerini ve sürüm geçmişini ayrı
tutar.

## Geliştirme

Çalışma zamanı bağımlılıkları `pyproject.toml` içinde, geliştirme araçları isteğe
bağlı tanımlanır. Kontroller:

```sh
ruff check .
ruff format --check .
python -m unittest discover -s tests -t . -p 'test_*.py'
python -m build --sdist --wheel
python -m twine check dist/*
```

## Değişiklikler

`v1.1-beta` sürümü Grok için web oturumu desteği ekler. Hesabın kullanabildiği
modelleri dinamik olarak listeler; akışlı yanıtları, yeniden denemeyi, proje
talimatı ve kaynak işlemlerini, belge eklerini ve web araması kaynaklarını
destekler. Oturum doğrulama, istek sınırı bekleme süreleri, akış güvenilirliği ve
gönderim hazırlığı da iyileştirildi. Tam liste [CHANGELOG.md](CHANGELOG.md)
dosyasındadır. Bu sürümden sonraki değişiklikler **Unreleased** altında toplanır.

## Lisans

Telif hakkı © TahsinCr. Paket [GPL-3.0-only](LICENSE) ile lisanslanmıştır.
[COPYRIGHT](COPYRIGHT) proje bildirimini içerir.

[version-shield]: https://img.shields.io/badge/s%C3%BCr%C3%BCm-v1.1--beta-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/Lisans-GPL--3.0--only-2F6F4E.svg?style=for-the-badge
