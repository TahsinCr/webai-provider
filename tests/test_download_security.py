from __future__ import annotations

import socket
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import Mock, patch

from webaiprovider.errors import UnsupportedOperationError, WebAIProviderError
from webaiprovider.transport.downloader import (
    DownloadSource,
    NativeDownloadClient,
    async_download_source,
    cookie_header,
    resolve_network_addresses,
)


class DownloadAddressTests(unittest.TestCase):
    @patch("webaiprovider.transport.downloader.socket.getaddrinfo")
    def test_hostname_resolution_rejects_private_and_pins_public_ip(self, getaddrinfo):
        getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))
        ]
        self.assertEqual(resolve_network_addresses("cdn.example"), ("93.184.216.34",))

        getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0)),
        ]
        with self.assertRaisesRegex(ValueError, "Prohibited destination"):
            resolve_network_addresses("mixed.example")

    def test_stream_pins_the_validated_dns_address(self):
        class Response:
            status_code = 200
            headers: ClassVar[dict[str, str]] = {}

            def iter_content(self, chunk_size):
                return iter((b"file",))

        class Session:
            def __init__(self):
                self.curl_options = {}

            @contextmanager
            def stream(self, method, url, *, headers, allow_redirects):
                self.request = method, url, headers, allow_redirects
                yield Response()

        download = NativeDownloadClient.__new__(NativeDownloadClient)
        download.allow_private = False
        download.client = Session()
        with (
            patch(
                "webaiprovider.transport.downloader.resolve_network_addresses",
                return_value=("93.184.216.34",),
            ),
            download.stream("GET", "https://cdn.example/file", headers={}) as response,
        ):
            self.assertEqual(list(response.iter_bytes()), [b"file"])

        from curl_cffi import CurlOpt

        self.assertEqual(
            download.client.curl_options[CurlOpt.RESOLVE],
            ["cdn.example:443:93.184.216.34"],
        )
        self.assertEqual(download.client.request[3], False)


class DownloadSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_download_requires_provider_resolved_source(self):
        with self.assertRaises(UnsupportedOperationError):
            await async_download_source(None)

    async def test_download_streams_success_and_rejects_http_failure(self):
        class Client(NativeDownloadClient):
            def __init__(self, status):
                self.status = status

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @contextmanager
            def stream(self, *_args, **_kwargs):
                yield SimpleNamespace(
                    status_code=self.status, iter_bytes=lambda: iter((b"a", b"b"))
                )

        success = DownloadSource("https://files.example/test", client_factory=lambda: Client(200))
        self.assertEqual(await async_download_source(success), b"ab")

        failure = DownloadSource("https://files.example/test", client_factory=lambda: Client(403))
        with self.assertRaisesRegex(WebAIProviderError, "HTTP 403"):
            await async_download_source(failure)

    async def test_download_rejects_custom_transport_outside_native_boundary(self):
        source = DownloadSource("https://files.example/test", client_factory=object)

        with self.assertRaisesRegex(WebAIProviderError, "unsupported download transport"):
            await async_download_source(source)

    def test_sensitive_cookie_values_are_validated(self):
        self.assertEqual(
            cookie_header({"sessionKey": "cookie"}, ("sessionKey",)), "sessionKey=cookie"
        )
        with self.assertRaises(ValueError):
            cookie_header({"sessionKey": "bad\r\nvalue"}, ("sessionKey",))

    def test_resolve_network_addresses_uses_af_inet_when_ipv6_unreachable(self):
        import socket
        from unittest.mock import patch

        from webaiprovider.transport.downloader import resolve_network_addresses

        seen_families = []

        def fake_getaddrinfo(_host, _port, family=0, type=0, *_extra):
            seen_families.append(family)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]

        with (
            patch("webaiprovider.transport.downloader._has_ipv6_connectivity", return_value=False),
            patch("socket.getaddrinfo", side_effect=fake_getaddrinfo),
        ):
            resolve_network_addresses("https://files.example/asset.png")

        with (
            patch("webaiprovider.transport.downloader._has_ipv6_connectivity", return_value=True),
            patch("socket.getaddrinfo", side_effect=fake_getaddrinfo),
        ):
            resolve_network_addresses("https://files.example/asset.png")

        self.assertEqual(seen_families, [socket.AF_INET, socket.AF_UNSPEC])

    async def test_download_enforces_max_bytes_and_sanitizes_transport_errors(self):
        from curl_cffi.requests.exceptions import RequestException

        class FakeSession:
            def __init__(self, *, chunks=None, error=None):
                self.curl_options = {}
                self.cookies = {}
                self._chunks = chunks or []
                self._error = error

            def close(self):
                return None

            @contextmanager
            def stream(self, _method, _url, **_kw):
                if self._error is not None:
                    raise self._error
                yield SimpleNamespace(
                    status_code=200,
                    headers={},
                    iter_content=lambda chunk_size=65536: iter(self._chunks),
                )

        oversized_session = FakeSession(chunks=[b"123456", b"7890"])
        with (
            patch("curl_cffi.requests.Session", return_value=oversized_session),
            patch(
                "webaiprovider.transport.downloader.resolve_network_addresses",
                return_value=("93.184.216.34",),
            ),
            patch("webaiprovider.transport.downloader._has_ipv6_connectivity", return_value=False),
        ):
            oversized = DownloadSource("https://files.example/huge.bin?sig=secret")
            with self.assertRaisesRegex(WebAIProviderError, "exceeds size limit") as ctx:
                await async_download_source(oversized, max_bytes=8)
            self.assertNotIn("secret", str(ctx.exception))

        failing_session = FakeSession(
            error=RequestException("failed with https://files.example/blob?sig=topsecret")
        )
        with (
            patch("curl_cffi.requests.Session", return_value=failing_session),
            patch(
                "webaiprovider.transport.downloader.resolve_network_addresses",
                return_value=("93.184.216.34",),
            ),
            patch("webaiprovider.transport.downloader._has_ipv6_connectivity", return_value=False),
        ):
            failing = DownloadSource("https://files.example/blob?sig=topsecret")
            with self.assertRaisesRegex(WebAIProviderError, "File transfer failed") as ctx:
                await async_download_source(failing)
            self.assertNotIn("topsecret", str(ctx.exception))

    def test_no_proxy_bypass_still_triggers_ssrf_and_dns_pinning(self):
        from webaiprovider.transport.network import _has_proxy_configured

        with patch.dict(
            "os.environ",
            {"HTTPS_PROXY": "http://127.0.0.1:8888", "NO_PROXY": "files.example,.internal"},
            clear=False,
        ):
            self.assertFalse(_has_proxy_configured(None, {}, hostname="files.example"))
            self.assertFalse(_has_proxy_configured(None, {}, hostname="api.internal"))
            self.assertTrue(_has_proxy_configured(None, {}, hostname="chatgpt.com"))

    def test_native_download_client_pins_all_resolved_ips(self):
        from curl_cffi import CurlOpt

        @contextmanager
        def fake_stream(_method, _url, **_kw):
            yield SimpleNamespace(
                status_code=200,
                headers={},
                iter_content=lambda chunk_size=65536: iter([b"ok"]),
            )

        client_obj = SimpleNamespace(
            curl_options={},
            cookies={},
            close=Mock(),
            stream=Mock(side_effect=fake_stream),
        )
        with (
            patch(
                "socket.getaddrinfo",
                return_value=[
                    (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
                    (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.35", 443)),
                ],
            ),
            patch("webaiprovider.transport.downloader._has_ipv6_connectivity", return_value=False),
            patch("curl_cffi.requests.Session", return_value=client_obj),
        ):
            with (
                NativeDownloadClient() as downloader,
                downloader.stream("GET", "https://files.example/asset.bin", headers={}) as resp,
            ):
                self.assertEqual(list(resp.iter_bytes()), [b"ok"])

        self.assertEqual(
            client_obj.curl_options[CurlOpt.RESOLVE],
            ["files.example:443:93.184.216.34,93.184.216.35"],
        )


if __name__ == "__main__":
    unittest.main()
