from __future__ import annotations

import socket
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from webaiprovider.utils.files import NativeDownloadClient, validate_network_address


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
            headers = {}

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
        with patch(
            "webaiprovider.utils.files.validate_network_address",
            return_value="93.184.216.34",
        ):
            with download.stream("GET", "https://cdn.example/file", headers={}) as response:
                self.assertEqual(list(response.iter_bytes()), [b"file"])

        from curl_cffi import CurlOpt

        self.assertEqual(
            download.client.curl_options[CurlOpt.RESOLVE],
            ["cdn.example:443:93.184.216.34"],
        )
        self.assertEqual(download.client.request[3], False)


if __name__ == "__main__":
    unittest.main()
