from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from webaiprovider.resources.attachments import Attachment, Attachments


class AttachmentsTests(unittest.TestCase):
    def test_from_bytes(self) -> None:
        data = b"print('hello world')"
        att = Attachment.from_bytes(data, "main.py", mime_type="text/x-python")

        self.assertEqual(att.name, "main.py")
        self.assertEqual(att.mime_type, "text/x-python")
        self.assertEqual(att.size, len(data))
        self.assertEqual(att.read_bytes(), data)

    def test_from_path(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"sample file content")
            temp_path = Path(f.name)

        try:
            att = Attachment.from_path(temp_path)
            self.assertEqual(att.size, 19)
            self.assertEqual(att.read_bytes(), b"sample file content")
            self.assertEqual(att.mime_type, "text/plain")
        finally:
            temp_path.unlink()

    def test_attachments_collection_operations(self) -> None:
        col = Attachments()
        att1 = col.add(b"123", name="test1.bin")
        col.add(b"45678", name="test2.bin")

        self.assertEqual(len(col), 2)
        self.assertEqual(col.total_size, 8)
        self.assertIn(att1, col)
        self.assertIn("test2.bin", col)

        removed = col.remove("test1.bin")
        self.assertTrue(removed)
        self.assertEqual(len(col), 1)

        col.clear()
        self.assertEqual(len(col), 0)
