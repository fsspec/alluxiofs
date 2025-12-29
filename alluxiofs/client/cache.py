import hashlib
import os
import tempfile
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from enum import auto
from enum import Enum
from multiprocessing import Value
from pathlib import Path

from fsspec.caching import BaseCache
from fsspec.caching import Fetcher
from fsspec.caching import ReadAheadCache

from .const import MAGIC_SIZE
from .log import setup_logger
from .log import TagAdapter
from .utils import _c_send_get_request_write_file
from .utils import get_prefetch_policy


# =========================================================
# LocalCacheManager: Handles local cache operations, LRU eviction, and atomic writes
# =========================================================


class BlockStatus(Enum):
    ABSENT = auto()
    LOADING = auto()
    CACHED = auto()


class LocalCacheManager:
    def __init__(
        self,
        config,
    ):
        self.config = config
        base_logger = setup_logger(
            self.config.log_dir,
            self.config.log_level,
            self.__class__.__name__,
            self.config.log_tag_allowlist,
        )
        self.logger = TagAdapter(base_logger, {"tag": "[LOCAL_CACHE]"})

        self.block_size = (
            int(self.config.local_cache_block_size_mb) * 1024 * 1024
        )
        self.http_max_retries = self.config.http_max_retries
        self.http_timeouts = self.config.http_timeouts
        self.eviction_high_watermark = (
            self.config.local_cache_eviction_high_watermark
        )
        self.eviction_low_watermark = (
            self.config.local_cache_eviction_low_watermark
        )
        self.eviction_scan_interval = int(
            self.config.local_cache_eviction_scan_interval_minutes * 60
        )
        self.ttl_time_seconds = int(
            self.config.local_cache_ttl_time_minutes * 60
        )
        self.cache_dirs, self.max_cache_sizes = self._param_local_cache_dirs(
            self.config.local_cache_dir, self.config.local_cache_size_gb
        )
        self.current_cache_sizes = [Value("l", 0) for _ in self.cache_dirs]

        # Initialize cache directories
        self.cache_data_dirs = []
        self.cache_tmp_pool_dirs = []
        for d in self.cache_dirs:
            data_dir = os.path.join(d, "data")
            tmp_dir = os.path.join(d, "tmp_pool")
            os.makedirs(data_dir, exist_ok=True)
            os.makedirs(tmp_dir, exist_ok=True)
            self.cache_data_dirs.append(data_dir)
            self.cache_tmp_pool_dirs.append(tmp_dir)

        self._load_existing_cache()
        self._stop_eviction_event = threading.Event()
        self._eviction_thread = threading.Thread(
            target=self._run_eviction_monitor,
            name="CacheEvictionMonitor",
            daemon=True,
        )
        self._eviction_thread.start()
        if self.logger:
            self.logger.debug(
                f"Eviction monitor thread started with interval {self.eviction_scan_interval}s"
            )
        # ----------------------------

    def has_files_with_prefix(self, folder_path, prefix):
        folder = Path(folder_path)
        if not folder.is_dir():
            return False
        return any(
            file.name.startswith(prefix)
            for file in folder.iterdir()
            if file.is_file()
        )

    def is_file_in_local_cache(self, file_path):
        """Check if a file is fully cached locally."""
        hash_index = self._get_local_cache_index_dir_for_file(file_path)
        cache_data_dir = self._get_cache_data_dir(hash_index)
        path_hash = hashlib.sha256(file_path.encode("utf-8")).hexdigest()
        return self.has_files_with_prefix(cache_data_dir, path_hash)

    def _run_eviction_monitor(self):
        """Background thread to periodically scan and clean evicted files."""
        while not self._stop_eviction_event.is_set():
            if self._stop_eviction_event.wait(self.eviction_scan_interval):
                break

            try:
                self._scan_and_clean_evicted()
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error in eviction monitor: {e}")

    def _scan_and_clean_evicted(self):
        """Scan cache directories for files marked as evicted and delete them."""
        if self.logger:
            self.logger.debug("Starting background eviction scan...")

        cleaned_count = 0

        for dir_index in range(len(self.cache_dirs)):
            data_dir = self._get_cache_data_dir(dir_index)
            if not os.path.exists(data_dir):
                continue

            try:
                with os.scandir(data_dir) as entries:
                    for entry in entries:
                        if entry.is_file() and entry.name.endswith("_evicted"):
                            file_path = entry.path

                            self._truly_evict_file(
                                file_path, hash_index=dir_index
                            )

                            cleaned_count += 1
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Failed to scan dir {data_dir}: {e}")

        if self.logger and cleaned_count > 0:
            self.logger.debug(
                f"Background scan finished. Cleaned {cleaned_count} files."
            )

    def shutdown(self):
        """Shut down the eviction monitor thread."""
        self._stop_eviction_event.set()
        if self._eviction_thread.is_alive():
            self._eviction_thread.join(timeout=2)
        if self.logger:
            self.logger.debug("LocalCacheManager shut down.")

    def _param_local_cache_dirs(self, cache_dir, max_cache_size):
        """Parse cache directories and their respective sizes."""
        dirs = []
        sizes = []
        dir_list = cache_dir.split(",")
        size_list = str(max_cache_size).split(",")
        if len(size_list) == 1:
            size_list = size_list * len(dir_list)
        assert len(dir_list) == len(
            size_list
        ), "Number of cache directories must match number of sizes"
        for d, s in zip(dir_list, size_list):
            dirs.append(d.strip())
            sizes.append(int(float(s.strip()) * 1024 * 1024 * 1024))
        return dirs, sizes

    def _get_cache_data_dir(self, hash_index):
        """Get the data directory for a given cache index."""
        return self.cache_data_dirs[hash_index]

    def _get_cache_tmp_pool_dir(self, hash_index):
        """Get the temporary pool directory for a given cache index."""
        return self.cache_tmp_pool_dirs[hash_index]

    def _get_local_cache_index_dir_for_file(self, file_path):
        """Determine which cache directory to use for a given file based on hash."""
        hash_obj = hashlib.sha256(file_path.encode("utf-8"))
        path_hash = hash_obj.hexdigest()
        dir_index = int(path_hash, 16) % len(self.cache_dirs)
        return dir_index

    def _load_existing_cache(self):
        """Scan existing cache files and rebuild cache index at startup."""
        for dir_index in range(len(self.cache_dirs)):
            if os.path.exists(self.cache_dirs[dir_index]):
                self._load_existing_cache_single(dir_index)

    def _load_existing_cache_single(self, hash_index):
        """Scan existing cache files and rebuild cache index at startup."""
        for f in os.listdir(self._get_cache_data_dir(hash_index)):
            fp = os.path.join(self._get_cache_data_dir(hash_index), f)
            if fp.endswith("_loading") or fp.endswith("_evicted"):
                try:
                    os.remove(fp)
                except FileNotFoundError:
                    pass
                continue
            if os.path.isfile(fp):
                try:
                    size = os.path.getsize(fp)
                    with self.current_cache_sizes[hash_index].get_lock():
                        self.current_cache_sizes[hash_index].value += size
                except FileNotFoundError:
                    continue
        self._evict_if_needed(hash_index, 0)

    def compute_hash(self, file_path):
        return hashlib.sha256(file_path.encode("utf-8")).hexdigest()

    def _get_local_path(self, file_path, part_index, path_hash=None):
        """Generate local cache file path for a given block."""
        if path_hash is None:
            path_hash = self.compute_hash(file_path)
        dir_index = int(path_hash, 16) % len(self.cache_dirs)
        cache_data_dir = self._get_cache_data_dir(dir_index)
        return os.path.join(cache_data_dir, f"{path_hash}_{part_index}")

    def _atomic_write(
        self,
        file_path,
        file_path_hashed,
        worker_host,
        worker_http_port,
        alluxio_path,
        start,
        end,
    ):
        """
        Write data to cache atomically using a two-phase commit:
        1. Atomically transition status from ABSENT to LOADING
        2. Write to a temporary file
        3. Rename to the final target file (atomic operation on the same filesystem)
        4. Atomically transition status from LOADING to CACHED
        """
        import traceback

        tmp_pool_dir = "/tmp/local_cache_tmp_pool"
        hash_index = 0
        try:
            hash_index = self._get_local_cache_index_dir_for_file(file_path)
            tmp_pool_dir = self._get_cache_tmp_pool_dir(hash_index)

            self._evict_if_needed(hash_index, end - start)
            temp_file = None
        except Exception as e:
            self.logger.error(
                "Exception in _atomic_write pre-check:",
                e,
                traceback.print_exc(),
            )
            return False

        try:
            # Step 2: Write to temporary file
            with tempfile.NamedTemporaryFile(
                dir=tmp_pool_dir, delete=False, mode="wb"
            ) as temp_file:
                temp_file_name = temp_file.name

                self._fetch_range_via_shell(
                    temp_file,
                    worker_host,
                    worker_http_port,
                    alluxio_path,
                    start,
                    end,
                )
                temp_file.flush()
                os.fsync(temp_file.fileno())
        except Exception as e:
            # On failure, reset status to ABSENT and clean up
            if temp_file and os.path.exists(temp_file.name):
                os.remove(temp_file.name)
            self._set_file_absent(file_path_hashed)
            if self.logger:
                self.logger.debug(f"Write failed for {file_path_hashed}: {e}")
            return False

        try:
            # Step 3: Atomic rename
            os.rename(temp_file_name, file_path_hashed)
            self._set_file_cached(file_path_hashed)
            with self.current_cache_sizes[hash_index].get_lock():
                self.current_cache_sizes[hash_index].value += end - start
            if self.logger:
                self.logger.debug(
                    f"Atomic write completed: {file_path_hashed}"
                )
            return True
        except FileExistsError as e:
            # On failure, reset status to ABSENT and clean up
            if temp_file and os.path.exists(temp_file.name):
                os.remove(temp_file.name)
            self._set_file_absent(file_path_hashed)
            if self.logger:
                self.logger.debug(f"Write failed for {file_path_hashed}: {e}")
            return False

    def get_file_status(self, file_path, part_index):
        file_path_hashed = self._get_local_path(file_path, part_index)
        return self._get_block_status(file_path_hashed)

    def _get_block_status(self, file_path_hashed):
        """Get or create AtomicBlockStatus for a file path."""
        if os.path.exists(file_path_hashed):
            if self._is_ttl_timeout(file_path_hashed):
                self._fake_evict_file(file_path_hashed)
                return BlockStatus.ABSENT
            return BlockStatus.CACHED
        elif os.path.exists(file_path_hashed + "_loading"):
            return BlockStatus.LOADING
        else:
            return BlockStatus.ABSENT

    def _is_ttl_timeout(self, file_path_hashed):
        file_path = Path(file_path_hashed)
        if not file_path.exists():
            return True
        try:
            stat_info = file_path.stat()
            file_mtime = stat_info.st_mtime
            return time.time() - file_mtime > self.ttl_time_seconds
        except OSError:
            return True

    def _fake_evict_file(self, file_path_hashed):
        """Mark the file as evicted without deleting it."""
        if not file_path_hashed.endswith("_evicted"):
            try:
                os.rename(file_path_hashed, file_path_hashed + "_evicted")
            except FileExistsError:
                pass
            except Exception:
                try:
                    os.remove(file_path_hashed)
                except FileNotFoundError:
                    pass

    def _truly_evict_file(self, file_path_evicted, hash_index=None):
        """Truly delete the cached file."""
        try:
            size = os.path.getsize(file_path_evicted)
        except FileNotFoundError:
            size = 0
        try:
            os.remove(file_path_evicted)
            if size:
                with self.current_cache_sizes[hash_index].get_lock():
                    self.current_cache_sizes[hash_index].value -= size
            if self.logger:
                self.logger.debug(
                    f"Evicted old cache: {file_path_evicted} ({size} bytes)"
                )
        except FileNotFoundError:
            pass
        except Exception as e:
            if self.logger:
                self.logger.debug(
                    f"Failed to evict cache {file_path_evicted}: {e}"
                )

    def set_file_loading(self, file_path, part_index):
        file_path_hashed = self._get_local_path(file_path, part_index)
        return self._set_file_loading(file_path_hashed)

    def _set_file_loading(self, file_path_hashed):
        """
        Atomically try to set file status to LOADING.
        Returns True if successful, False if already loading/cached.
        """
        # Check if already cached first
        if os.path.exists(file_path_hashed):
            return False
        try:
            # Atomic exclusive create - only one thread can succeed
            fd = os.open(
                file_path_hashed + "_loading",
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            os.close(fd)
            return True
        except FileExistsError:
            # Another thread is already loading this block
            return False
        except OSError as e:
            if self.logger:
                self.logger.debug(f"Failed to set loading status: {e}")
            return False

    def _set_file_cached(self, file_path_hashed):
        """Set the file status to CACHED."""
        try:
            os.remove(file_path_hashed + "_loading")
        except FileNotFoundError:
            pass

    def _set_file_absent(self, file_path_hashed):
        """Set the file status to ABSENT."""
        try:
            os.remove(file_path_hashed + "_loading")
        except FileNotFoundError:
            pass
        try:
            os.remove(file_path_hashed)
        except FileNotFoundError:
            pass

    def _evict_if_needed(self, hash_index, length):
        """Perform LRU eviction when total cache size exceeds the limit."""
        with self.current_cache_sizes[hash_index].get_lock():
            cache_size = self.current_cache_sizes[hash_index].value
        if (
            cache_size + length
            <= self.max_cache_sizes[hash_index] * self.eviction_high_watermark
        ):
            return
        self._perform_eviction(hash_index, length)

    def _perform_eviction(self, hash_index, length):
        cached_files = self._get_files_sorted_by_atime_scandir(
            self._get_cache_data_dir(hash_index)
        )
        while (
            self.current_cache_sizes[hash_index].value + length
            > self.max_cache_sizes[hash_index] * self.eviction_low_watermark
            and len(cached_files) > 0
        ):
            old_path = cached_files.pop(0)["path"]
            self._truly_evict_file(old_path, hash_index)

    def add_to_cache(
        self,
        file_path,
        part_index,
        worker_host,
        worker_http_port,
        alluxio_path,
        start,
        end,
    ):
        path_hashed = self._get_local_path(file_path, part_index)
        self._atomic_write(
            file_path,
            path_hashed,
            worker_host,
            worker_http_port,
            alluxio_path,
            start,
            end,
        )

    def read_from_cache(
        self,
        file_path,
        part_index,
        offset,
        length,
        path_hash=None,
        base_path=None,
    ):
        """
        Read data from cache block if available.
        Returns a tuple: (data, status)
        - If block is not cached: (None, BlockStatus.ABSENT)
        - If block is being written: (None, BlockStatus.LOADING)
        - If block is ready: (data, BlockStatus.CACHED)
        """
        if base_path:
            file_path_hashed = f"{base_path}_{part_index}"
        else:
            file_path_hashed = self._get_local_path(
                file_path, part_index, path_hash
            )

        try:
            fd = os.open(file_path_hashed, os.O_RDONLY)
            try:
                data = os.pread(fd, length, offset)
            finally:
                os.close(fd)
            return data, BlockStatus.CACHED
        except FileNotFoundError:
            if os.path.exists(file_path_hashed + "_loading"):
                return None, BlockStatus.LOADING
            return None, BlockStatus.ABSENT
        except (IOError, OSError) as e:
            if self.logger:
                self.logger.debug(f"Read error: {file_path_hashed}: {e}")
            self._set_file_absent(file_path_hashed)
            return None, BlockStatus.ABSENT

    def _get_files_sorted_by_atime_scandir(
        self, cache_data_dir, reverse=False
    ):
        """Get list of files sorted by access time using os.scandir for efficiency."""
        files = []
        with os.scandir(cache_data_dir) as entries:
            for entry in entries:
                if entry.is_file() and not entry.name.endswith("_loading"):
                    try:
                        stat = entry.stat()
                        files.append(
                            {
                                "name": entry.name,
                                "path": entry.path,
                                "atime": stat.st_atime,
                                "size": stat.st_size,
                                "is_evicted": entry.name.endswith(
                                    "_evicted"
                                ),  # 新增字段
                            }
                        )
                    except FileNotFoundError:
                        continue

        files_sorted = sorted(
            files, key=lambda x: (x["is_evicted"], x["atime"]), reverse=reverse
        )
        return files_sorted

    def _fetch_range_via_shell(
        self, f, worker_host, worker_http_port, alluxio_path, start, end
    ):
        """
        Fetch a byte range from the Alluxio worker using curl via subprocess.
        - worker_host, worker_http_port, file_path: worker address and file path
        - start, end: range [start, end) to fetch
        """
        headers = {"Range": f"bytes={start}-{end - 1}"}
        S3_RANGE_URL_FORMAT = "http://{worker_host}:{http_port}{alluxio_path}"
        url = S3_RANGE_URL_FORMAT.format(
            worker_host=worker_host,
            http_port=worker_http_port,
            alluxio_path=alluxio_path,
        )
        _c_send_get_request_write_file(
            url,
            headers,
            f,
            time_out=self.http_timeouts,
            retry_tries=self.http_max_retries,
        )


# =========================================================
# CachedFileReader: Handles remote fetch + cache coordination
# =========================================================
class CachedFileReader:
    def __init__(
        self,
        alluxio=None,
        data_manager=None,
        thread_pool=ThreadPoolExecutor(4),
        config=None,
    ):
        self.config = config
        self.logger = setup_logger(
            self.config.log_dir,
            self.config.log_level,
            self.__class__.__name__,
            self.config.log_tag_allowlist,
        )

        self.logger = TagAdapter(self.logger, {"tag": "[LOCAL_CACHE]"})

        self.cache = data_manager
        self.block_size = data_manager.block_size
        self.alluxio_client = alluxio
        self.pool = thread_pool
        self.prefetch_policy = get_prefetch_policy(
            alluxio.config,
            self.block_size,
        )

    def close(self):
        if self.logger:
            self.logger.debug("Closing CacheFileReader...")
        self.pool.shutdown(wait=True)
        if self.cache:
            self.cache.shutdown()

    def _get_s3_worker_address(self, file_path):
        """Mock: Returns the preferred worker host and HTTP port."""
        return self.alluxio_client._get_s3_worker_address(file_path)

    def _get_path_hash(self, file_path):
        """Generate a stable hash for the given file path."""
        return self.alluxio_client._get_path_hash(file_path)

    def _fetch_block(self, args):
        """
        Function executed by each process:
        Download a specific file block and write it to cache atomically.
        """
        (
            file_path,
            alluxio_path,
            worker_host,
            worker_http_port,
            block_index,
            start,
            end,
        ) = args
        try:
            if not self.cache.set_file_loading(file_path, block_index):
                return
            self.cache.add_to_cache(
                file_path,
                block_index,
                worker_host,
                worker_http_port,
                alluxio_path,
                start,
                end,
            )
            if self.logger:
                self.logger.debug(
                    f"Block download complete: {file_path}_{block_index}, size={end - start}B"
                )
        except FileExistsError:
            return
        except Exception as e:
            if self.logger:
                self.logger.warning(
                    f"Failed to download block ({block_index}): {e}"
                )

    def _parallel_download_file(
        self,
        file_path,
        alluxio_path,
        offset=0,
        length=-1,
        file_size=None,
        prefetch_policy=None,
    ):
        """Use multiprocessing to download the entire file in parallel (per block)."""
        worker_host, worker_http_port = self._get_s3_worker_address(file_path)
        if file_size is None:
            file_size = self.get_file_length(file_path)
        start_block, end_block = self.get_blocks_prefetch(
            offset, length, file_size, prefetch_policy
        )
        args_list = []
        for i in range(start_block, end_block + 1):
            states = self.cache.get_file_status(
                file_path, i
            )  # Ensure status is initialized
            if states == BlockStatus.CACHED or states == BlockStatus.LOADING:
                continue
            start = i * self.block_size
            end = (i + 1) * self.block_size
            end = min(end, file_size)
            args_list.append(
                (
                    file_path,
                    alluxio_path,
                    worker_host,
                    worker_http_port,
                    i,
                    start,
                    end,
                )
            )

        self.logger.debug(
            f"Launching {end_block - start_block} processes to download {file_path}"
        )
        # self.pool.map_async(self._fetch_block, args_list)
        for arg in args_list:
            self.pool.submit(self._fetch_block, arg)

    def read_file_range(
        self,
        file_path,
        alluxio_path,
        offset=0,
        length=-1,
        file_size=None,
        prefetch_policy=None,
    ):
        """
        Read the requested file range.
        1. Try reading from the local cache.
        2. If cache miss occurs, trigger background download of missing blocks.
        """
        if length == 0:
            return b""
        start_block, end_block = self.get_blocks(offset, length, file_size)

        # Pre-allocate list for chunks to avoid repeated string concatenation
        chunks = []

        # Calculate remaining length for accurate part_length computation
        remaining_length = length

        path_hash = self.cache.compute_hash(file_path)
        cache_data_dir = self.cache.cache_data_dirs[
            int(path_hash, 16) % len(self.cache.cache_dirs)
        ]
        base_path = os.path.join(cache_data_dir, path_hash)

        for blk in range(start_block, end_block + 1):
            part_offset = (
                offset - blk * self.block_size if blk == start_block else 0
            )
            # Calculate part_length more efficiently
            if length != -1:
                block_available = self.block_size - part_offset
                part_length = min(remaining_length, block_available)
                remaining_length -= part_length
            else:
                part_length = -1
            chunk, state = self.cache.read_from_cache(
                file_path,
                blk,
                part_offset,
                part_length,
                path_hash=path_hash,
                base_path=base_path,
            )
            if chunk is None:
                if state == BlockStatus.ABSENT:
                    self._parallel_download_file(
                        file_path,
                        alluxio_path,
                        offset,
                        length,
                        file_size,
                        prefetch_policy,
                    )

                # Wait for the block to become available
                chunk, state = self.cache.read_from_cache(
                    file_path,
                    blk,
                    part_offset,
                    part_length,
                    path_hash=path_hash,
                    base_path=base_path,
                )

                if state != BlockStatus.CACHED:
                    # Fall back to direct read - return immediately
                    return self.alluxio_client.read_file_range_normal(
                        file_path, alluxio_path, offset, length
                    )
            if chunk is not None:
                offset += len(chunk)
                length -= len(chunk)
            chunks.append(chunk)

        # Use join() instead of repeated concatenation - much faster for multiple chunks
        return b"".join(chunks)

    def get_blocks_prefetch(
        self, offset=0, length=-1, file_size=None, prefetch_policy=None
    ):
        if prefetch_policy:
            return prefetch_policy.get_blocks(offset, length, file_size)
        return self.prefetch_policy.get_blocks(offset, length, file_size)

    def get_file_length(self, file_path):
        """Mock: Returns the file size for the given file path."""
        file_status = self.alluxio_client.get_file_status(file_path)
        if file_status is None:
            raise FileNotFoundError(f"File {file_path} not found")
        length = file_status.size
        return length

    def get_blocks(self, offset=0, length=-1, file_size=None):
        if length == -1 and file_size is None:
            raise ValueError(
                "file_size or length must be provided to determine block boundaries."
            )

        start_block = offset // self.block_size
        end_block = (
            (offset + length - 1) // self.block_size
            if length != -1
            else (file_size - 1) // self.block_size
        )
        return start_block, end_block

    def read_magic_bytes(self, file_path, alluxio_path):
        """Read the magic bytes from the beginning of the file."""
        file_path_hashed = self.cache._get_local_path(file_path, 0)
        if os.path.exists(file_path_hashed):
            with open(file_path_hashed, "rb") as f:
                data = f.read(MAGIC_SIZE)
        else:
            data = self.alluxio_client.read_file_range_normal(
                file_path, alluxio_path, 0, MAGIC_SIZE
            )
        return data


class McapMemoryCache(BaseCache):
    name = "mcap"

    def __init__(self, blocksize: int, fetcher: Fetcher, size: int) -> None:
        super().__init__(blocksize, fetcher, size)

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        return self.fetcher(start, stop)


class McapMemoryCache(BaseCache):
    name = "mcap"

    def __init__(
        self, blocksize: int, fetcher: Fetcher, size: int, **cache_options
    ) -> None:
        super().__init__(blocksize, fetcher, size)
        self.magic_bytes = None
        self.cache = cache_options.get("cache", None)
        self.file_path = cache_options.get("file_path", "")
        # Cache isinstance check result to avoid repeated checks
        self._is_memory_cache_pool = isinstance(
            self.cache, MemoryReadAHeadCachePool
        )

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        # Fast path: magic bytes check (most common for mcap files)
        if start == 0 and stop == MAGIC_SIZE:
            if self.magic_bytes is not None:
                return self.magic_bytes
            self.magic_bytes = self.fetcher(start, stop)
            return self.magic_bytes

        # Use cached isinstance check result
        if self._is_memory_cache_pool:
            return self.cache.fetch(
                self.file_path, self.size, self.fetcher, start, stop
            )
        return self.cache._fetch(start, stop)


class MemoryReadAHeadCachePool:
    """
    Block-based in-memory cache with file-level LRU eviction.
    Each cache key is file_path, containing a ReadAheadCache instance.
    Optimized for frequent small reads with reduced lock contention.
    """

    def __init__(
        self,
        block_size=8 * 1024 * 1024,
        max_size_bytes=1024 * 1024 * 1024,
        num_shards=16,
        logger=None,
    ):
        self.block_size = block_size
        self.max_size_bytes = max_size_bytes
        self.num_shards = num_shards
        self.logger = logger

        self.logger = TagAdapter(self.logger, {"tag": "[MEMORY_CACHE]"})

        # Use regular dict with separate access tracking for better performance
        self.cache_shards = {}  # file_path -> ReadAheadCache
        self.access_order = OrderedDict()  # file_path -> access_count
        self.global_lock = threading.RLock()
        self.current_size_bytes = 0
        # Lazy LRU update: only update every N accesses or after eviction
        self._lru_update_counter = 0
        self._lru_update_interval = 10

    def _update_access_time_lazy(self, file_path):
        """Lazy LRU update: only update every N accesses to reduce overhead"""
        # Only update if file is actually in cache (prevent memory leak)
        if file_path not in self.cache_shards:
            return

        # Periodically update LRU order
        self._lru_update_counter += 1
        if self._lru_update_counter >= self._lru_update_interval:
            self._lru_update_counter = 0
            # Move to end (most recently used) - only if in access_order
            if file_path in self.access_order:
                self.access_order.move_to_end(file_path)
            else:
                self.access_order[file_path] = 1

    def _update_access_time_immediate(self, file_path):
        """Immediate LRU update for cache misses"""
        # Move to end (most recently used)
        if file_path in self.access_order:
            self.access_order.move_to_end(file_path)
        else:
            self.access_order[file_path] = 1

    def _evict_if_needed(self, additional_size=0):
        """Evict least recently used files if cache exceeds max size"""
        if self.current_size_bytes + additional_size <= self.max_size_bytes:
            return

        if self.logger:
            self.logger.debug(
                f"Cache full ({self.current_size_bytes}/{self.max_size_bytes} bytes), starting eviction..."
            )

        target_size = self.max_size_bytes * 0.8  # Evict to 80% capacity
        bytes_evicted = 0

        # Evict files in LRU order (oldest first)
        while (
            self.access_order
            and (self.current_size_bytes - bytes_evicted) > target_size
        ):
            # Get least recently used file
            file_path, _ = next(iter(self.access_order.items()))

            if file_path in self.cache_shards:
                if self.logger:
                    self.logger.debug(
                        f"Evicting file: {file_path} (size: {self.block_size} bytes)"
                    )
                # Remove cache object and clear references to prevent memory leaks
                cache_obj = self.cache_shards[file_path]
                del self.cache_shards[file_path]
                # Clear cache object to break potential reference cycles
                if hasattr(cache_obj, "cache"):
                    cache_obj.cache = None
                if hasattr(cache_obj, "fetcher"):
                    cache_obj.fetcher = None
                del cache_obj
                self.current_size_bytes -= self.block_size
                bytes_evicted += self.block_size
            # Remove from access order (clean up to prevent memory leak)
            del self.access_order[file_path]

        # Clean up any orphaned access_order entries (defensive cleanup)
        # This prevents memory leak from stale entries
        if len(self.access_order) > len(self.cache_shards) * 2:
            # Remove entries not in cache_shards (use generator to reduce memory)
            orphaned = [
                fp
                for fp in list(self.access_order.keys())
                if fp not in self.cache_shards
            ]
            for fp in orphaned:
                try:
                    del self.access_order[fp]
                except KeyError:
                    pass  # Already removed

        if self.logger:
            self.logger.debug(
                f"Eviction completed: freed {bytes_evicted} bytes, new size: {self.current_size_bytes} bytes"
            )

    def fetch(self, file_path, size, fetcher, start, stop):
        # Fast path: cache hit - most common case (simplified to reduce CPU overhead)
        with self.global_lock:
            cache = self.cache_shards.get(file_path)
            if cache is not None:
                # Cache hit - use lazy LRU update
                self._update_access_time_lazy(file_path)
                cache_ref = cache
            else:
                cache_ref = None

        # Fetch outside lock to reduce contention
        if cache_ref is not None:
            return cache_ref._fetch(start, stop)

        # Cache miss - need full lock
        with self.global_lock:
            # Double-check after acquiring lock (in case another thread added it)
            cache = self.cache_shards.get(file_path)
            if cache is not None:
                self._update_access_time_lazy(file_path)
                cache_ref = cache
            else:
                # Cache miss confirmed - create new cache
                if self.logger:
                    self.logger.debug(f"Cache miss for file: {file_path}")

                # Evict if needed
                self._evict_if_needed(self.block_size)

                # Create new cache
                cache = ReadAheadCache(
                    blocksize=self.block_size,
                    fetcher=fetcher,
                    size=size,
                )
                self.current_size_bytes += self.block_size

                # Add to cache
                self.cache_shards[file_path] = cache
                self._update_access_time_immediate(file_path)
                cache_ref = cache

        # Fetch outside lock to reduce contention
        return cache_ref._fetch(start, stop)

    def clear(self):
        """Clear all cache contents"""
        with self.global_lock:
            self.cache_shards.clear()
            self.access_order.clear()
            self.current_size_bytes = 0
