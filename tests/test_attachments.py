from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from webaiprovider.collections.attachments import Attachments
from webaiprovider.resources.attachment import Attachment


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
        att1 = col.create(b"123", name="test1.bin")
        att2 = col.create(b"45678", name="test2.bin")

        self.assertEqual(len(col), 2)
        self.assertEqual(col.total_size, 8)
        self.assertIn(att1, col)
        self.assertIs(col.find("test2.bin"), att2)

        updated = col.update(att1.id, name="renamed.bin", mime_type="application/x-test")
        self.assertIs(updated, att1)
        self.assertEqual(updated.name, "renamed.bin")
        self.assertEqual(updated.mime_type, "application/x-test")

        removed = col.delete(att1.id)
        self.assertTrue(removed)
        self.assertEqual(len(col), 1)

        col.delete(att2.id)
        self.assertEqual(len(col), 0)
