"""Unit tests for StorageSink abstraction."""

import shutil
import tempfile
import unittest
from pathlib import Path

from ingestion.config.s3_storage import StorageSink


class TestStorageSink(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.sink = StorageSink(force_local=True)
        self.sink.local_base_dir = Path(self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_local_write_and_read(self):
        test_data = b"UrbanPulse Test Payload"
        test_key = "raw/test/sample.dat"

        uri = self.sink.upload_bytes(test_data, test_key)
        self.assertTrue(uri.startswith("file://"))

        # Check existence
        self.assertTrue(self.sink.object_exists(test_key))

        # Check content
        retrieved = self.sink.read_bytes(test_key)
        self.assertEqual(retrieved, test_data)

    def test_nonexistent_object(self):
        self.assertFalse(self.sink.object_exists("raw/does/not/exist.parquet"))


if __name__ == "__main__":
    unittest.main()
