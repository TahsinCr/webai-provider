from __future__ import annotations

import socket
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import patch

from webaiprovider.errors import ProviderError, UnsupportedOperationError
from webaiprovider.utils.files import (
    DownloadSource,
    NativeDownloadClient,
    cookie_header,
    download_source,
    redact_sensitive_url,
    validate_network_address,
)


class DownloadAddressTests(unittest.TestCase):
    @patch("webaiprovider.utils.files.socket.getaddrinfo")
    def test_hostname_resolution_rejects_private_and_pins_public_ip(self, getaddrinfo):
        getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))
        ]
        self.assertEqual(validate_network_address("cdn.example"), "93.184.216.34")

        getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0)),
        ]
        with self.assertRaisesRegex(ValueError, "Prohibited destination"):
            validate_network_address("mixed.example")

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
                "webaiprovider.utils.files.validate_network_address",
                return_value="93.184.216.34",
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
            await download_source(None)

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
        self.assertEqual(await download_source(success), b"ab")

        failure = DownloadSource("https://files.example/test", client_factory=lambda: Client(403))
        with self.assertRaisesRegex(ProviderError, "HTTP 403"):
            await download_source(failure)

    async def test_download_rejects_custom_transport_outside_native_boundary(self):
        source = DownloadSource("https://files.example/test", client_factory=object)

        with self.assertRaisesRegex(ProviderError, "unsupported download transport"):
            await download_source(source)

    def test_sensitive_cookie_and_url_values_are_redacted(self):
        self.assertEqual(
            cookie_header({"sessionKey": "cookie"}, ("sessionKey",)), "sessionKey=cookie"
        )
        with self.assertRaises(ValueError):
            cookie_header({"sessionKey": "bad\r\nvalue"}, ("sessionKey",))

        redacted = redact_sensitive_url(
            "https://user:password@files.example/a?token=secret&name=file#part"
        )
        self.assertNotIn("password", redacted)
        self.assertNotIn("secret", redacted)
        self.assertIn("[REDACTED]", redacted)


if __name__ == "__main__":
    unittest.main()
