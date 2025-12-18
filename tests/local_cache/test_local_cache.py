import hashlib
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock
from unittest.mock import patch

from alluxiofs.client.cache import BlockStatus
from alluxiofs.client.cache import CachedFileReader
from alluxiofs.client.cache import LocalCacheManager


class MockConfig:
    def __init__(self, cache_dir, size="1", block_size_mb=1):
        self.local_cache_dir = cache_dir
        self.local_cache_size_gb = size
        self.local_cache_block_size_mb = block_size_mb
        self.http_max_retries = 1
        self.http_timeouts = 1
        self.local_cache_eviction_high_watermark = 0.8
        self.local_cache_eviction_low_watermark = 0.6
        self.local_cache_eviction_scan_interval_minutes = 1
        self.local_cache_ttl_time_minutes = 60
        self.local_cache_prefetch_policy = "none"
        self.log_dir = None
        self.log_level = "INFO"
        self.log_tag_allowlist = []


class TestLocalCacheManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = MockConfig(self.test_dir)
        self.manager = LocalCacheManager(self.config)

    def tearDown(self):
        self.manager.shutdown()
        shutil.rmtree(self.test_dir)

    def test_init_creates_dirs(self):
        """Test that cache directories are created upon initialization."""
        for d in self.manager.cache_data_dirs:
            self.assertTrue(os.path.exists(d))
        for d in self.manager.cache_tmp_pool_dirs:
            self.assertTrue(os.path.exists(d))

    def test_compute_hash(self):
        """Test hash computation."""
        file_path = "/test/file"
        expected = hashlib.sha256(file_path.encode("utf-8")).hexdigest()
        self.assertEqual(self.manager.compute_hash(file_path), expected)

    def test_get_local_path(self):
        """Test local path generation."""
        file_path = "/test/file"
        part_index = 0
        path_hash = self.manager.compute_hash(file_path)

        path = self.manager._get_local_path(file_path, part_index)
        self.assertIn(path_hash, path)
        self.assertTrue(path.endswith(f"{path_hash}_{part_index}"))

    @patch("alluxiofs.client.cache.LocalCacheManager._fetch_range_via_shell")
    def test_atomic_write_success(self, mock_fetch):
        """Test successful atomic write."""
        file_path = "/test/file"
        part_index = 0
        path_hash = self.manager.compute_hash(file_path)
        assert path_hash is not None
        local_path = self.manager._get_local_path(file_path, part_index)

        # Mock fetch to write some data
        def side_effect(f, *args):
            f.write(b"data")

        mock_fetch.side_effect = side_effect

        success = self.manager._atomic_write(
            file_path, local_path, "host", 80, "/path", 0, 4
        )

        self.assertTrue(success)
        self.assertTrue(os.path.exists(local_path))
        with open(local_path, "rb") as f:
            self.assertEqual(f.read(), b"data")

        # Check status
        self.assertEqual(
            self.manager.get_file_status(file_path, part_index),
            BlockStatus.CACHED,
        )

    def test_read_from_cache(self):
        """Test reading from cache."""
        file_path = "/test/file"
        part_index = 0
        local_path = self.manager._get_local_path(file_path, part_index)

        # Case 1: Absent
        data, status = self.manager.read_from_cache(
            file_path, part_index, 0, 10
        )
        self.assertEqual(status, BlockStatus.ABSENT)
        self.assertIsNone(data)

        # Case 2: Loading
        with open(local_path + "_loading", "w") as f:
            pass
        data, status = self.manager.read_from_cache(
            file_path, part_index, 0, 10
        )
        self.assertEqual(status, BlockStatus.LOADING)
        self.assertIsNone(data)
        os.remove(local_path + "_loading")

        # Case 3: Cached
        with open(local_path, "wb") as f:
            f.write(b"hello world")

        data, status = self.manager.read_from_cache(
            file_path, part_index, 0, 5
        )
        self.assertEqual(status, BlockStatus.CACHED)
        self.assertEqual(data, b"hello")

    def test_eviction(self):
        """Test LRU eviction."""
        # Re-init with small size (e.g., 10 bytes)
        # Note: config size is in GB string, but internally converted.
        # Let's mock the sizes directly to control it better or use a very small size string if supported.
        # The class converts "1GB" -> 1024^3.
        # We can manually set max_cache_sizes after init for testing.

        self.manager.max_cache_sizes = [100]  # 100 bytes
        self.manager.eviction_high_watermark = 0.8  # 80 bytes
        self.manager.eviction_low_watermark = 0.5  # 50 bytes

        # Create dummy files
        # We need to simulate adding files and updating current_cache_sizes

        # File 1: 40 bytes
        path1 = os.path.join(self.manager.cache_data_dirs[0], "file1")
        with open(path1, "wb") as f:
            f.write(b"a" * 40)
        self.manager.current_cache_sizes[0].value += 40

        # File 2: 30 bytes
        path2 = os.path.join(self.manager.cache_data_dirs[0], "file2")
        with open(path2, "wb") as f:
            f.write(b"b" * 30)
        self.manager.current_cache_sizes[0].value += 30

        # Total 70 bytes. < 80. No eviction yet.
        self.manager._evict_if_needed(0, 0)
        self.assertTrue(os.path.exists(path1))
        self.assertTrue(os.path.exists(path2))

        # Add File 3: 20 bytes. Total 90 bytes > 80. Trigger eviction.
        # Eviction should remove oldest accessed file until size < 50.
        # Access times: file1 (old), file2 (new).
        # Should evict file1. Remaining: 50 bytes.
        # If still >= 50, might evict file2?
        # Logic: while size > low_watermark (50).
        # 90 > 50. Evict file1 (40). New size 50. 50 is not > 50. Stop.

        # We need to ensure atime difference
        os.utime(path1, (time.time() - 100, time.time() - 100))
        os.utime(path2, (time.time(), time.time()))

        self.manager._evict_if_needed(0, 20)

        # Check if file1 is evicted (renamed to _evicted or deleted)
        # The _perform_eviction calls _truly_evict_file which removes it.
        self.assertFalse(os.path.exists(path1))
        self.assertTrue(os.path.exists(path2))
        self.assertEqual(self.manager.current_cache_sizes[0].value, 30)

    def test_ttl_expiration(self):
        """Test TTL expiration."""
        self.manager.ttl_time_seconds = 1  # 1 second TTL

        file_path = "/test/file_ttl"
        part_index = 0
        local_path = self.manager._get_local_path(file_path, part_index)

        with open(local_path, "wb") as f:
            f.write(b"data")

        # Set mtime to past
        past_time = time.time() - 2
        os.utime(local_path, (past_time, past_time))

        # Should be treated as ABSENT (and marked evicted)
        status = self.manager.get_file_status(file_path, part_index)
        self.assertEqual(status, BlockStatus.ABSENT)
        self.assertFalse(os.path.exists(local_path))
        self.assertTrue(os.path.exists(local_path + "_evicted"))

    def test_background_cleanup(self):
        """Test background cleanup of evicted files."""
        path = os.path.join(self.manager.cache_data_dirs[0], "file_evicted")
        with open(path + "_evicted", "wb") as f:
            f.write(b"data")

        self.manager._scan_and_clean_evicted()
        self.assertFalse(os.path.exists(path + "_evicted"))

    def test_set_file_loading(self):
        """Test setting file loading status."""
        file_path = "/test/file_loading_test"
        part_index = 0
        local_path = self.manager._get_local_path(file_path, part_index)

        # Success
        self.assertTrue(self.manager.set_file_loading(file_path, part_index))
        self.assertTrue(os.path.exists(local_path + "_loading"))

        # Fail (already loading)
        self.assertFalse(self.manager.set_file_loading(file_path, part_index))

        # Cleanup
        os.remove(local_path + "_loading")

        # Fail (already cached)
        with open(local_path, "w"):
            pass
        self.assertFalse(self.manager.set_file_loading(file_path, part_index))


class TestCachedFileReader(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = MockConfig(self.test_dir)
        self.manager = LocalCacheManager(self.config)
        self.alluxio_client = MagicMock()
        self.alluxio_client.config = self.config
        self.alluxio_client._get_s3_worker_address.return_value = (
            "localhost",
            29998,
        )
        self.alluxio_client._get_path_hash.side_effect = (
            lambda p: self.manager.compute_hash(p)
        )

        self.reader = CachedFileReader(
            alluxio=self.alluxio_client,
            data_manager=self.manager,
            thread_pool=ThreadPoolExecutor(max_workers=2),
            config=self.config,
        )

    def tearDown(self):
        self.reader.close()
        self.manager.shutdown()
        shutil.rmtree(self.test_dir)

    def test_read_file_range_cached(self):
        """Test reading fully cached file."""
        file_path = "/test/file"
        file_size = 1024 * 1024 * 2  # 2MB
        self.alluxio_client.get_file_status.return_value.size = file_size

        # Pre-populate cache
        # Block 0
        path0 = self.manager._get_local_path(file_path, 0)
        with open(path0, "wb") as f:
            f.write(b"a" * (1024 * 1024))
        # Block 1
        path1 = self.manager._get_local_path(file_path, 1)
        with open(path1, "wb") as f:
            f.write(b"b" * (1024 * 1024))

        # Read
        data = self.reader.read_file_range(
            file_path, "/alluxio/path", 0, file_size, file_size
        )
        self.assertEqual(len(data), file_size)
        self.assertEqual(data[:10], b"aaaaaaaaaa")
        self.assertEqual(data[1024 * 1024 : 1024 * 1024 + 10], b"bbbbbbbbbb")

    @patch("alluxiofs.client.cache.CachedFileReader._parallel_download_file")
    def test_read_file_range_miss_triggers_download(self, mock_download):
        """Test that cache miss triggers download."""
        file_path = "/test/file_miss"
        file_size = 100
        self.alluxio_client.get_file_status.return_value.size = file_size

        # Mock read_from_cache to return ABSENT then CACHED
        # We need to mock manager.read_from_cache because it's called inside read_file_range

        original_read = self.manager.read_from_cache
        assert original_read is not None

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None, BlockStatus.ABSENT
            else:
                # Simulate download finished
                return b"data", BlockStatus.CACHED

        with patch.object(
            self.manager, "read_from_cache", side_effect=side_effect
        ):
            data = self.reader.read_file_range(
                file_path, "/alluxio/path", 0, 4, file_size
            )

            self.assertTrue(mock_download.called)
            self.assertEqual(data, b"data")

    def test_parallel_download_file(self):
        """Test parallel download submission."""
        file_path = "/test/file_dl"
        file_size = 1024 * 1024 * 3  # 3 blocks
        self.alluxio_client.get_file_status.return_value.size = file_size

        # Mock pool.submit
        with patch.object(self.reader.pool, "submit") as mock_submit:
            self.reader._parallel_download_file(
                file_path, "/alluxio/path", 0, file_size, file_size
            )

            # Should submit 3 tasks (one for each block)
            self.assertEqual(mock_submit.call_count, 3)

    def test_get_blocks(self):
        """Test block calculation."""
        # Block size is 1MB (from MockConfig)
        bs = 1024 * 1024

        # Case 1: Start of file
        start, end = self.reader.get_blocks(0, 100, 1000)
        self.assertEqual(start, 0)
        self.assertEqual(end, 0)

        # Case 2: Cross boundary
        start, end = self.reader.get_blocks(bs - 10, 20, 2 * bs)
        self.assertEqual(start, 0)
        self.assertEqual(end, 1)

        # Case 3: Middle block
        start, end = self.reader.get_blocks(bs + 10, 100, 3 * bs)
        self.assertEqual(start, 1)
        self.assertEqual(end, 1)


class TestLocalCacheConcurrency(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = MockConfig(self.test_dir)
        self.manager = LocalCacheManager(self.config)

    def tearDown(self):
        self.manager.shutdown()
        shutil.rmtree(self.test_dir)

    def test_concurrent_set_file_loading(self):
        """Test that only one thread can set file loading status."""
        file_path = "/test/race_loading"
        part_index = 0

        success_count = 0
        lock = threading.Lock()

        def try_set_loading():
            nonlocal success_count
            if self.manager.set_file_loading(file_path, part_index):
                with lock:
                    success_count += 1

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(try_set_loading) for _ in range(10)]
            for f in futures:
                f.result()

        self.assertEqual(success_count, 1)

        # Verify file exists
        local_path = self.manager._get_local_path(file_path, part_index)
        self.assertTrue(os.path.exists(local_path + "_loading"))

    def test_concurrent_writes_eviction(self):
        """Test concurrent writes triggering eviction."""
        # Set small limit
        self.manager.max_cache_sizes = [1024]  # 1KB
        self.manager.eviction_high_watermark = 0.8
        self.manager.eviction_low_watermark = 0.5

        def write_file(i):
            path = f"/test/file_{i}"
            # Each file 100 bytes
            self.manager.add_to_cache(path, 0, "host", 80, path, 0, 100)
            return i

        # Mock _fetch_range_via_shell to just write bytes
        with patch(
            "alluxiofs.client.cache.LocalCacheManager._fetch_range_via_shell"
        ) as mock_fetch:
            mock_fetch.side_effect = lambda f, *args: f.write(b"a" * 100)

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(write_file, i) for i in range(50)]
                for f in futures:
                    f.result()

            # Check total size is within limits
            # Note: current_cache_sizes is approximate during high concurrency due to lock granularity
            # but should be eventually consistent-ish or at least not crash.

            # Verify size
            current_size = self.manager.current_cache_sizes[0].value
            # Due to concurrency, size might slightly exceed limit before eviction catches up
            # Allow some buffer (e.g. 2x limit)
            self.assertLessEqual(current_size, 2048)

    def test_read_while_writing(self):
        """Test reading while writing is in progress."""
        file_path = "/test/read_write_race"
        part_index = 0
        local_path = self.manager._get_local_path(file_path, part_index)

        # Start writing (set loading)
        self.manager.set_file_loading(file_path, part_index)

        def reader():
            # Should return LOADING
            data, status = self.manager.read_from_cache(
                file_path, part_index, 0, 10
            )
            return status

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(reader)
            status = future.result()
            self.assertEqual(status, BlockStatus.LOADING)

        # Finish writing
        with open(local_path, "wb") as f:
            f.write(b"data")
        self.manager._set_file_cached(local_path)

        # Read again
        data, status = self.manager.read_from_cache(
            file_path, part_index, 0, 10
        )
        self.assertEqual(status, BlockStatus.CACHED)
        self.assertEqual(data, b"data")


if __name__ == "__main__":
    unittest.main()
