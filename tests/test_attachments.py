from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from webaiprovider.resources.attachment import Attachment
from webaiprovider.resources.message.attachments import Attachments
from webaiprovider.utils.attachments import AttachmentPolicy, fit_attachments


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


class AttachmentPolicyTests(unittest.TestCase):
    def test_policy_filters_by_file_type_size_and_count(self):
        policy = AttachmentPolicy(
            max_files=2,
            max_bytes=10,
            max_image_bytes=3,
            max_spreadsheet_bytes=5,
            allowed_extensions=(".txt", ".png", ".csv"),
        )
        items = [
            {"name": "first.txt", "size_bytes": 8},
            {"name": "large.png", "mime_type": "image/png", "size_bytes": 4},
            {"name": "large.csv", "size_bytes": 6},
            {"name": "unsupported.bin", "size_bytes": 1},
            {"name": "second.txt", "size_bytes": 2},
            {"name": "third.txt", "size_bytes": 1},
        ]

        accepted, rejected = fit_attachments(items, policy)

        self.assertEqual(accepted, [items[0], items[4]])
        self.assertEqual(rejected, [items[1], items[2], items[3], items[5]])

    def test_total_limit_keeps_earliest_and_unknown_size_can_pass(self):
        items = [
            {"name": "first.txt", "size_bytes": 4},
            {"name": "unknown.txt"},
            {"name": "last.txt", "size_bytes": 4},
        ]

        accepted, rejected = fit_attachments(
            items,
            AttachmentPolicy(max_files=3, max_bytes=10, max_total_bytes=6),
        )

        self.assertEqual(accepted, items[:2])
        self.assertEqual(rejected, [items[2]])

    def test_missing_policy_rejects_all_attachments(self):
        items = [{"name": "file.txt", "size_bytes": 1}]

        accepted, rejected = fit_attachments(items, None)

        self.assertEqual(accepted, [])
        self.assertEqual(rejected, items)
