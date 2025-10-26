import os
import queue

import requests
import tempfile
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from alluxiofs.client.const import FULL_RANGE_URL_FORMAT
import hashlib
import os
import time
import tempfile
import threading
from collections import OrderedDict
from enum import Enum, auto

# === Configuration ===
DEFAULT_BLOCK_SIZE = 16 * 1024 * 1024  # 16 MB per block
DEFAULT_CACHE_SIZE_MB = 1024  # Local cache capacity: 1 GB


# =========================================================
# LocalCacheManager: Handles local cache operations, LRU eviction, and atomic writes
# =========================================================
LOCAL_CACHE_DIR = "/tmp/mcap_local_cache"
DEFAULT_CACHE_SIZE_MB = 512


class BlockStatus(Enum):
    ABSENT = auto()
    LOADING = auto()
    CACHED = auto()

class LocalCacheManager:
    def __init__(self, cache_dir=LOCAL_CACHE_DIR,
                 block_size=16 * 1024 * 1024,
                 max_cache_size_mb=DEFAULT_CACHE_SIZE_MB,
                 logger=None
    ):
        self.cache_dir = cache_dir
        self.max_cache_size = max_cache_size_mb * 1024 * 1024  # Convert MB to bytes
        self.block_size = block_size
        self.logger = logger if logger is not None else self.logger.debug
        self.cache_fd = OrderedDict()
        self.Loading = set()

        # Thread lock for concurrent safety of cache operations
        self.lock = threading.Lock()
        os.makedirs(self.cache_dir, exist_ok=True)
        self._load_existing_cache()

    def _load_existing_cache(self):
        """Scan existing cache files and rebuild cache index at startup."""
        for f in os.listdir(self.cache_dir):
            fp = os.path.join(self.cache_dir, f)
            if os.path.isfile(fp):
                self._set_file_cached(fp)

    def _get_local_path(self, file_path, part_index):
        """Generate local cache file path for a given block."""
        hash_obj = hashlib.sha256(file_path.encode("utf-8"))
        path_hash = hash_obj.hexdigest()
        return os.path.join(self.cache_dir, f"{path_hash}_{part_index}")

    def _atomic_write(self, file_path_hashed, data):
        """
        Write data to cache atomically using a two-phase commit:
        1. Atomically transition status from ABSENT to LOADING
        2. Write to a temporary file
        3. Rename to the final target file (atomic operation on the same filesystem)
        4. Atomically transition status from LOADING to CACHED
        """
        status = self._get_block_status(file_path_hashed)

        temp_file = None
        try:
            # Step 2: Write to temporary file
            temp_file = tempfile.NamedTemporaryFile(dir=self.cache_dir, delete=False)
            temp_file.write(data)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_file.close()

            # Step 3: Atomic rename
            os.rename(temp_file.name, file_path_hashed)
            self._set_file_cached(file_path_hashed)
            self._update_cache_index(file_path_hashed)

            self.logger.debug(f"[CACHE] Atomic write completed: {file_path_hashed}")
            return True

        except Exception as e:
            # On failure, reset status to ABSENT and clean up
            if temp_file and os.path.exists(temp_file.name):
                os.remove(temp_file.name)
            self.logger.debug(f"[CACHE] Write failed for {file_path_hashed}: {e}")
            return False

    def get_file_status(self, file_path, part_index):
        file_path_hashed = self._get_local_path(file_path, part_index)
        return self._get_block_status(file_path_hashed)

    def _get_block_status(self, file_path_hashed):
        """Get or create AtomicBlockStatus for a file path."""
        with self.lock:
            if file_path_hashed in self.Loading:
                return BlockStatus.LOADING
            elif self.cache_fd.get(file_path_hashed) is not None:
                return BlockStatus.CACHED
            else:
                return BlockStatus.ABSENT

    def set_file_loading(self, file_path, part_index):
        file_path_hashed = self._get_local_path(file_path, part_index)
        self._set_file_loading(file_path_hashed)

    def _set_file_loading(self, file_path_hashed):
        """Set the file status to LOADING."""
        with self.lock:
            self.Loading.add(file_path_hashed)

    def _set_file_cached(self, file_path_hashed):
        """Set the file status to CACHED."""
        with self.lock:
            if file_path_hashed not in self.cache_fd:
                self.cache_fd[file_path_hashed] = time.time()
            if file_path_hashed in self.Loading:
                self.Loading.remove(file_path_hashed)

    def _set_file_absent(self, file_path_hashed):
        """Set the file status to ABSENT."""
        with self.lock:
            if file_path_hashed in self.Loading:
                self.Loading.remove(file_path_hashed)
            if file_path_hashed in self.cache_fd:
                self.cache_fd.pop(file_path_hashed)

    def _update_cache_index(self, file_path_hashed):
        """Update access time and perform LRU eviction if needed."""
        self.cache_fd.move_to_end(file_path_hashed)
        self._evict_if_needed()

    def _evict_if_needed(self):
        """Perform LRU eviction when total cache size exceeds the limit."""
        with self.lock:
            total_size = len(self.cache_fd) * self.block_size
            while total_size > self.max_cache_size and self.cache_fd:
                old_path = self.cache_fd.popitem(last=False)
                try:
                    os.remove(old_path)
                    self.logger.debug(f"[LRU] Evicted old cache: {old_path}")
                except FileNotFoundError:
                    pass
                total_size -= self.block_size

    def add_to_cache(self, file_path, part_index, data):
        path_hashed = self._get_local_path(file_path, part_index)
        return self._atomic_write(path_hashed, data)

    def read_from_cache(self, file_path, part_index, offset, length):
        """
        Read data from cache block if available.
        Returns a tuple: (data, status)
        - If block is not cached: (None, BlockStatus.ABSENT)
        - If block is being written: (None, BlockStatus.LOADING)
        - If block is ready: (data, BlockStatus.CACHED)
        """
        s1 = time.time()
        file_path_hashed = self._get_local_path(file_path, part_index)
        status = self._get_block_status(file_path_hashed)
        s2 = time.time()
        if status == BlockStatus.ABSENT:
            return None, BlockStatus.ABSENT
        elif status == BlockStatus.LOADING:
            self.logger.debug(f"[CACHE] Block is currently loading: {file_path_hashed}")
            return None, BlockStatus.LOADING
        s3 = time.time()
        # If cached, read data
        if not os.path.exists(file_path_hashed):
            self._set_file_absent(file_path_hashed)
            return None, BlockStatus.ABSENT
        with open(file_path_hashed, "rb") as f:
            f.seek(offset)
            data = f.read(length if length != -1 else None)
        s4 = time.time()
        self._update_cache_index(file_path_hashed)
        s5 = time.time()
        self.logger.debug(f"1 {s2-s1:.5f}")
        self.logger.debug(f"2 {s3 - s2:.5f}")
        self.logger.debug(f"3 {s4 - s3:.5f}")
        self.logger.debug(f"4 {s5 - s4:.5f}")
        self.logger.debug(f"5 {s5 - s1:.5f}")
        self.logger.debug(f"[CACHE] Read cache block: {file_path_hashed}")
        return data, BlockStatus.CACHED


# =========================================================
# CachedFileReader: Handles remote fetch + cache coordination
# =========================================================
class CachedFileReader:
    def __init__(self, alluxio=None, data_manager=None, max_workers=32, block_size=DEFAULT_BLOCK_SIZE, logger=None):
        self.cache = data_manager
        self.block_size = block_size
        self.logger = logger if logger is not None else None
        self.max_workers = max_workers
        self.alluxio_client = alluxio
        self.pool = ThreadPoolExecutor(max_workers=self.max_workers)

    def close(self):
        self.logger.debug("[FileReader] Closing worker pool...")
        self.pool.close()
        self.pool.join()

    def _get_preferred_worker_address(self, file_path):
        """Mock: Returns the preferred worker host and HTTP port."""
        return self.alluxio_client._get_preferred_worker_address(file_path)

    def _get_path_hash(self, file_path):
        """Generate a stable hash for the given file path."""
        return hex(hash(file_path))

    def _fetch_block(self, args):
        """
        Function executed by each process:
        Download a specific file block and write it to cache atomically.
        """
        file_path, worker_host, worker_http_port, path_id, block_index, start, end, cache_dir = args
        states = self.cache.get_file_status(file_path, block_index)  # Ensure status is initialized
        if states == BlockStatus.CACHED or states == BlockStatus.LOADING:
            return
        headers = {"transfer-type": "chunked"}

        url = FULL_RANGE_URL_FORMAT.format(
            worker_host=worker_host,
            http_port=worker_http_port,
            path_id=path_id,
            file_path=file_path,
            offset=start,
            length=end - start,
        )
        try:
            self.cache.set_file_loading(file_path, block_index)
            start_time = time.time()
            data = b''
            with requests.get(
                    url, headers=headers, stream=True
            ) as response:
                end_time = time.time()
                print(
                    f"[BLOCK] Downloaded block: {file_path}_{block_index}, size={end - start}B, time={end_time - start_time:.5f}s")
                # Check for connection reset error (status code 104)
                if response.status_code == 104:
                    raise ConnectionResetError("Connection reset by peer")

                response.raise_for_status()
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        data += chunk
            self.cache.add_to_cache(file_path, block_index, data)
            self.logger.debug(f"[BLOCK] Block download complete: {file_path}_{block_index}, size={len(data)}B")
        except Exception as e:
            self.logger.debug(f"[ERROR] Failed to download block ({block_index}): {e}")

    def _parallel_download_file(self, file_path, offset=0, length=-1, file_size=None):
        """Use multiprocessing to download the entire file in parallel (per block)."""
        worker_host, worker_http_port = self._get_preferred_worker_address(file_path)
        path_id = self._get_path_hash(file_path)
        if file_size is None:
            file_size = self.get_file_length(file_path)
        start_block, end_block = self.get_blocks_prefetch(offset, length, file_size)
        args_list = []
        for i in range(start_block, end_block + 1):
            start = i * self.block_size
            end = (i + 1) * self.block_size
            args_list.append((
                file_path, worker_host, worker_http_port, path_id, i, start, end, self.cache.cache_dir
            ))

        self.logger.debug(f"[DOWNLOAD] Launching {end_block-start_block} processes to download {file_path}")
        # self.pool.map_async(self._fetch_block, args_list)
        for arg in args_list:
            self.pool.submit(self._fetch_block, arg)

    def read_file_range(self, file_path, offset=0, length=-1, file_size=None):
        """
        Read the requested file range.
        1. Try reading from the local cache.
        2. If cache miss occurs, trigger background download of missing blocks.
        """
        start_block, end_block = self.get_blocks(offset, length, file_size)
        data = b""
        for blk in range(start_block, end_block + 1):
            part_offset = offset - blk * self.block_size if blk == start_block else 0
            part_length = min(length, self.block_size - part_offset) if length != -1 else -1
            s1 = time.time()
            chunk, state = self.cache.read_from_cache(file_path, blk, part_offset, part_length)
            s2 = time.time()
            self.logger.debug(f"[WAIT] 1 {s2 - s1:.5f}")
            if chunk is None:
                self.logger.debug(f"[MISS] Cache miss, triggering background download: {file_path}")
                if state == BlockStatus.ABSENT:
                    self._parallel_download_file(file_path, offset, length, file_size)
                start_time = time.time()
                chunk, state = self.cache.read_from_cache(file_path, blk, part_offset, part_length)
                end_time = time.time()
                if state != BlockStatus.CACHED:
                    chunk = self.alluxio_client.read_file_range_normal(file_path, offset, length)
                    return chunk
                self.logger.debug(f"[WAIT] 2 {end_time - start_time:.5f}")

            data = data + chunk
        return data

    def get_blocks_prefetch(self, offset=0, length=-1, file_size=None):
        if length == -1 and file_size is None:
            raise ValueError("file_size or length must be provided to determine block boundaries.")
        start_block = offset // self.block_size
        end_block = (
            (offset + length - 1) // self.block_size
            if length != -1
            else (file_size - 1) // self.block_size
        )

        # Expand the prefetch range by 16 blocks beyond the current end block
        prefetch_ahead = 32
        end_block = min(end_block + prefetch_ahead, (file_size - 1) // self.block_size)
        return start_block, end_block

    def get_file_length(self, file_path):
        """Mock: Returns the file size for the given file path."""
        file_status = self.alluxio_client.get_file_status(file_path)
        if file_status is None:
            raise FileNotFoundError(f"File {file_path} not found")
        length = file_status.length
        return length

    def get_blocks(self, offset=0, length=-1, file_size=None):
        if length == -1 and file_size is None:
            raise ValueError("file_size or length must be provided to determine block boundaries.")

        start_block = offset // self.block_size
        end_block = (offset + length - 1) // self.block_size if length != -1 else (file_size - 1) // self.block_size
        return start_block, end_block


