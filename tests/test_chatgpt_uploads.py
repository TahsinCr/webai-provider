"""Provider upload and attachment-limit contracts."""

import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, Mock, patch

from webaiprovider.providers.chatgpt.client import ChatGPTClient
from webaiprovider.providers.chatgpt.errors import ChatGPTError
from webaiprovider.utils.attachments import policy_for


class AttachmentPolicyContractTests(unittest.TestCase):
    def test_provider_limits_are_runtime_owned_and_distinct(self):
        self.assertEqual(policy_for("gemini").max_files, 10)
        self.assertEqual(policy_for("gemini").max_video_bytes, 2 * 1024 * 1024 * 1024)
        self.assertEqual(policy_for("claude").max_files, 20)
        self.assertEqual(policy_for("claude").max_files_scope, "conversation")
        self.assertEqual(policy_for("chatgpt").max_bytes, 512 * 1024 * 1024)
        self.assertEqual(policy_for("chatgpt").max_image_bytes, 20 * 1024 * 1024)
        self.assertEqual(policy_for("chatgpt").max_spreadsheet_bytes, 50 * 1024 * 1024)
        self.assertIsNone(policy_for("unknown"))


class ChatGPTStreamingUploadTests(unittest.TestCase):
    def test_project_upload_validates_processing_events_before_finalization(self):
        ready = b'{"type":"file.processing.file_ready"}\n'
        completed = b'{"type":"file.processing.completed"}'
        cases = (
            ("completed", (ready, completed), None),
            ("fragmented", (ready + completed[:13], completed[13:] + b"\n"), None),
            ("coalesced", (ready * 100 + completed,), None),
            ("truncated", (ready,), "without completion"),
            ("failed", (b'{"type":"file.processing.failed"}\n',), "could not process"),
            ("invalid", (b"not JSON\n",), "invalid project source event"),
            ("nonobject", (b"[]\n",), "invalid project source event"),
            (
                "oversized",
                (b'{"data":"' + b"x" * (1024 * 1024) + b'"}\n' + completed,),
                "oversized",
            ),
        )
        for label, chunks, error in cases:
            with self.subTest(case=label):
                client = object.__new__(ChatGPTClient)
                client.request = Mock(
                    return_value=SimpleNamespace(
                        json=lambda: {"file_id": "file-id", "upload_url": "https://upload"}
                    )
                )
                client._upload_request = Mock(return_value=SimpleNamespace(raise_for_status=Mock()))
                response = SimpleNamespace(
                    status_code=200,
                    headers={"content-type": "text/event-stream"},
                    iter_bytes=lambda chunks=chunks: iter(chunks),
                )
                client.transport = SimpleNamespace(stream=Mock(return_value=nullcontext(response)))
                with self.assertRaisesRegex(ChatGPTError, error) if error else nullcontext():
                    result = client.upload_file(
                        "source.txt", "text/plain", b"source", project_id="project-id"
                    )
                    self.assertEqual(result["id"], "file-id")

                client.request.assert_called_once()
                client.transport.stream.assert_called_once_with(
                    "POST",
                    "/backend-api/files/process_upload_stream",
                    check_status=False,
                    json={
                        "entry_surface": "project_sources",
                        "file_id": "file-id",
                        "file_name": "source.txt",
                        "gizmo_id": "project-id",
                        "index_for_retrieval": True,
                        "mime_type": "text/plain",
                        "use_case": "gizmo",
                    },
                    timeout=ANY,
                )

    def test_path_upload_streams_chunks_instead_of_reading_the_whole_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "large.txt"
            path.write_bytes(b"a" * (2 * 1024 * 1024 + 7))
            client = object.__new__(ChatGPTClient)
            client.request = Mock(
                side_effect=[
                    SimpleNamespace(
                        json=lambda: {"file_id": "file-id", "upload_url": "https://upload"}
                    ),
                    SimpleNamespace(json=lambda: {"status": "success"}),
                ]
            )
            uploaded = []

            def put(_method, _url, *, content, **_kwargs):
                uploaded.extend(content)
                return SimpleNamespace(raise_for_status=Mock(), content=b"", status_code=200)

            client.http = SimpleNamespace(request=Mock(side_effect=put))
            with patch.object(Path, "read_bytes", side_effect=AssertionError("full file read")):
                result = client.upload_file(path.name, "text/plain", path)
            self.assertEqual(b"".join(uploaded), b"a" * (2 * 1024 * 1024 + 7))
            self.assertEqual(result["size_bytes"], path.stat().st_size)

    def test_path_upload_cancel_stops_later_chunks_and_skips_finalization(self):
        import threading

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "cancel.txt"
            path.write_bytes(b"a" * (512 * 1024))
            cancel_event = threading.Event()
            client = object.__new__(ChatGPTClient)
            client.request = Mock(
                return_value=SimpleNamespace(
                    json=lambda: {"file_id": "file-id", "upload_url": "https://upload"}
                )
            )

            def put(_method, _url, *, content, **_kwargs):
                chunks = iter(content)
                self.assertEqual(len(next(chunks)), 256 * 1024)
                cancel_event.set()
                with self.assertRaisesRegex(ChatGPTError, "cancelled"):
                    next(chunks)
                return SimpleNamespace(raise_for_status=Mock(), content=b"", status_code=200)

            client.http = SimpleNamespace(request=Mock(side_effect=put))
            with self.assertRaisesRegex(ChatGPTError, "cancelled"):
                client.upload_file(path.name, "text/plain", path, cancel_event=cancel_event)
            self.assertEqual(client.request.call_count, 1)
            client.http.request.assert_called_once()

    def test_chatgpt_client_rejects_files_above_documented_ceiling_before_network(self):
        client = object.__new__(ChatGPTClient)
        client.request = Mock()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "too-large.txt"
            with path.open("wb") as stream:
                stream.truncate(512 * 1024 * 1024 + 1)
            with self.assertRaisesRegex(ChatGPTError, "size limit"):
                client.upload_file(path.name, "text/plain", path)
        client.request.assert_not_called()
