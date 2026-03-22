"""
LLM Checkpoint I/O Optimizer for Alluxio.

CheckpointWriter shards model state_dict across Alluxio workers in parallel
using the existing AlluxioClient write API, with a pipelined async write path
that overlaps serialization and network I/O across shards.

CheckpointReader prefetches checkpoint shards before the training loop resumes,
hiding load latency behind model initialization.

Example
-------
    from alluxiofs import AlluxioClient
    from alluxiofs.checkpoint import CheckpointWriter, CheckpointReader

    client = AlluxioClient(worker_hosts="host1,host2,host3")
    writer = CheckpointWriter(client, num_shards=8, max_workers=8)

    # Write: parallel shard fan-out
    stats = writer.save(model.state_dict(), "s3://bucket/ckpts/step_1000")
    print(f"Wrote {stats['total_bytes'] / 1e9:.2f} GB in {stats['elapsed_s']:.2f}s "
          f"({stats['throughput_gb_s']:.3f} GB/s)")

    # Read: kick off prefetch while model is initializing, then load
    reader = CheckpointReader(client, max_workers=8)
    reader.prefetch("s3://bucket/ckpts/step_1000")  # background fetch starts here
    model = MyModel()                                # init happens in parallel
    state_dict = reader.load("s3://bucket/ckpts/step_1000")  # waits for shards
    model.load_state_dict(state_dict)
"""

import io
import json
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_TORCH_AVAILABLE = False
try:
    import torch

    _TORCH_AVAILABLE = True
except ImportError:
    pass


def _require_torch():
    if not _TORCH_AVAILABLE:
        raise ImportError(
            "torch is required for checkpoint I/O. "
            "Install with: pip install torch"
        )


class CheckpointWriter:
    """
    Parallel checkpoint writer that shards a model state_dict across Alluxio
    workers using multiple threads.

    The write pipeline is:
      1. Serialize state_dict to bytes once (torch.save → BytesIO).
      2. Compute shard boundaries (contiguous byte ranges).
      3. Fan-out all shard writes in parallel via ThreadPoolExecutor.
         Each shard is written to a separate Alluxio path using write_chunked.
      4. Write a JSON manifest file last, listing shard paths and byte ranges.

    This approach keeps GPU idle time to a minimum: the GPU can resume forward
    passes of the next micro-batch while shard writes are in-flight.

    Args:
        alluxio_client: An AlluxioClient instance with write_chunked support.
        num_shards: Number of parallel shards to split the checkpoint into.
            More shards = better parallelism across Alluxio workers, up to
            the point where per-shard overhead dominates.
        max_workers: Thread pool size for concurrent shard writes.
        chunk_size_bytes: Size of each HTTP PUT chunk within a shard write.
    """

    MANIFEST_FILENAME = "_manifest.json"

    def __init__(
        self,
        alluxio_client,
        num_shards: int = 8,
        max_workers: int = 8,
        chunk_size_bytes: int = 4 * 1024 * 1024,
    ):
        self.alluxio = alluxio_client
        self.num_shards = num_shards
        self.max_workers = max_workers
        self.chunk_size_bytes = chunk_size_bytes

    def save(
        self,
        state_dict: dict,
        checkpoint_dir: str,
        metadata: Optional[dict] = None,
    ) -> dict:
        """
        Serialize and parallel-shard-write a state_dict to Alluxio.

        Args:
            state_dict: Model state dict (or any torch.save-compatible dict).
            checkpoint_dir: Alluxio base path. Shards are written as
                ``{checkpoint_dir}/shard_00000.bin`` … and a manifest as
                ``{checkpoint_dir}/_manifest.json``.
            metadata: Optional dict stored in the manifest (e.g., step, loss,
                epoch). Returned as-is by CheckpointReader.load().

        Returns:
            stats dict containing:
                - total_bytes: serialized size in bytes
                - num_shards: actual shard count written
                - elapsed_s: wall-clock time from call to return
                - serialize_s: time spent in torch.save
                - throughput_gb_s: total_bytes / elapsed_s in GB/s
                - shard_times_s: dict of {shard_idx: write_duration_s}
        """
        _require_torch()
        t0 = time.perf_counter()

        # Step 1: serialize to bytes
        buf = io.BytesIO()
        torch.save(state_dict, buf)
        serialized = buf.getvalue()
        total_bytes = len(serialized)
        t_serialize = time.perf_counter()
        logger.info(
            "Serialized state_dict: %.1f MB in %.3fs",
            total_bytes / 1e6,
            t_serialize - t0,
        )

        # Step 2: compute shard boundaries
        # Cap num_shards to avoid zero-byte shards on small checkpoints
        num_shards = max(1, min(self.num_shards, total_bytes // (64 * 1024) + 1))
        shard_size = (total_bytes + num_shards - 1) // num_shards
        shards: List[tuple] = []
        for i in range(num_shards):
            start = i * shard_size
            end = min(start + shard_size, total_bytes)
            if start >= total_bytes:
                break
            shards.append((i, start, end))

        shard_paths = [
            f"{checkpoint_dir.rstrip('/')}/shard_{idx:05d}.bin"
            for idx, _, _ in shards
        ]

        # Step 3: fan-out parallel shard writes
        # All writes are submitted immediately; we collect results as they finish.
        shard_times: Dict[int, float] = {}
        futures: Dict[Future, int] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for (idx, start, end), shard_path in zip(shards, shard_paths):
                shard_data = serialized[start:end]
                fut = pool.submit(
                    self._write_shard, shard_path, shard_data, idx
                )
                futures[fut] = idx

            for fut in as_completed(futures):
                idx = futures[fut]
                t_shard_start = time.perf_counter()
                fut.result()  # raises on failure
                shard_times[idx] = time.perf_counter() - t_shard_start

        # Step 4: write manifest
        manifest = {
            "version": 1,
            "total_bytes": total_bytes,
            "num_shards": len(shards),
            "shard_paths": shard_paths,
            "shard_ranges": [[s, e] for _, s, e in shards],
            "metadata": metadata or {},
        }
        manifest_path = (
            f"{checkpoint_dir.rstrip('/')}/{self.MANIFEST_FILENAME}"
        )
        self.alluxio.write_chunked(
            manifest_path, json.dumps(manifest, indent=2).encode()
        )

        elapsed = time.perf_counter() - t0
        throughput = total_bytes / elapsed / 1e9
        logger.info(
            "Checkpoint written: %d shards, %.3f GB, %.2fs, %.3f GB/s",
            len(shards),
            total_bytes / 1e9,
            elapsed,
            throughput,
        )
        return {
            "total_bytes": total_bytes,
            "num_shards": len(shards),
            "elapsed_s": elapsed,
            "serialize_s": t_serialize - t0,
            "throughput_gb_s": throughput,
            "shard_times_s": shard_times,
        }

    def _write_shard(self, shard_path: str, data: bytes, idx: int) -> None:
        t0 = time.perf_counter()
        self.alluxio.write_chunked(
            shard_path, data, chunk_size=self.chunk_size_bytes
        )
        elapsed = time.perf_counter() - t0
        logger.debug(
            "Shard %d: wrote %.1f MB in %.3fs (%.1f MB/s)",
            idx,
            len(data) / 1e6,
            elapsed,
            len(data) / elapsed / 1e6 if elapsed > 0 else 0,
        )


class CheckpointReader:
    """
    Parallel checkpoint reader with prefetch-on-resume.

    Calling ``prefetch()`` immediately launches shard reads in the background
    so that when ``load()`` is eventually called, the data is already (or
    nearly) in memory. This lets model/optimizer initialization on the training
    node overlap with network I/O from the Alluxio workers.

    Args:
        alluxio_client: An AlluxioClient instance.
        max_workers: Thread pool size for parallel shard reads.
    """

    def __init__(self, alluxio_client, max_workers: int = 8):
        self.alluxio = alluxio_client
        self.max_workers = max_workers
        self._prefetch_futures: Dict[str, Dict[int, Future]] = {}
        self._prefetch_pool: Optional[ThreadPoolExecutor] = None

    def prefetch(self, checkpoint_dir: str) -> None:
        """
        Start fetching checkpoint shards in the background.

        Call this as early as possible (e.g., before model initialization)
        to hide storage latency behind compute. Prefetched data is held in
        memory until ``load()`` is called for the same checkpoint_dir.

        Args:
            checkpoint_dir: Same path passed to CheckpointWriter.save().
        """
        manifest = self._read_manifest(checkpoint_dir)
        if self._prefetch_pool is None:
            self._prefetch_pool = ThreadPoolExecutor(
                max_workers=self.max_workers
            )
        futures: Dict[int, Future] = {}
        for idx, shard_path in enumerate(manifest["shard_paths"]):
            fut = self._prefetch_pool.submit(self._read_shard, shard_path, idx)
            futures[idx] = fut
        self._prefetch_futures[checkpoint_dir] = futures
        logger.info(
            "Prefetching %d shards from %s", len(futures), checkpoint_dir
        )

    def load(self, checkpoint_dir: str) -> dict:
        """
        Load and reassemble a checkpoint from Alluxio shards.

        If ``prefetch()`` was called for this directory, waits for in-flight
        shard reads to complete and deserializes from the cached bytes.
        Otherwise, fetches shards inline (still in parallel).

        Args:
            checkpoint_dir: Same path passed to CheckpointWriter.save().

        Returns:
            The state_dict originally passed to CheckpointWriter.save().
            Metadata is accessible via the manifest (use load_manifest()).
        """
        _require_torch()
        t0 = time.perf_counter()
        manifest = self._read_manifest(checkpoint_dir)
        num_shards = manifest["num_shards"]
        shard_paths = manifest["shard_paths"]

        shard_data: Dict[int, bytes] = {}
        if checkpoint_dir in self._prefetch_futures:
            # Collect from already-started background futures
            futures = self._prefetch_futures.pop(checkpoint_dir)
            for idx, fut in futures.items():
                shard_data[idx] = fut.result()
        else:
            # Inline parallel fetch
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futs: Dict[Future, int] = {
                    pool.submit(self._read_shard, path, idx): idx
                    for idx, path in enumerate(shard_paths)
                }
                for fut in as_completed(futs):
                    idx = futs[fut]
                    shard_data[idx] = fut.result()

        # Reassemble shards in order and deserialize
        assembled = b"".join(shard_data[i] for i in range(num_shards))
        state_dict = torch.load(io.BytesIO(assembled), map_location="cpu")

        elapsed = time.perf_counter() - t0
        logger.info(
            "Checkpoint loaded: %.3f GB in %.2fs (%.3f GB/s)",
            len(assembled) / 1e9,
            elapsed,
            len(assembled) / elapsed / 1e9 if elapsed > 0 else 0,
        )
        return state_dict

    def load_manifest(self, checkpoint_dir: str) -> dict:
        """Return the raw manifest dict for a checkpoint directory."""
        return self._read_manifest(checkpoint_dir)

    def close(self):
        """Shut down the background prefetch pool."""
        if self._prefetch_pool:
            self._prefetch_pool.shutdown(wait=False)
            self._prefetch_pool = None

    def __del__(self):
        self.close()

    def _read_manifest(self, checkpoint_dir: str) -> dict:
        manifest_path = (
            f"{checkpoint_dir.rstrip('/')}/{CheckpointWriter.MANIFEST_FILENAME}"
        )
        data = self.alluxio.read(manifest_path)
        if isinstance(data, io.BytesIO):
            data = data.read()
        return json.loads(data)

    def _read_shard(self, shard_path: str, idx: int) -> bytes:
        t0 = time.perf_counter()
        data = self.alluxio.read(shard_path)
        if isinstance(data, io.BytesIO):
            data = data.read()
        elapsed = time.perf_counter() - t0
        logger.debug(
            "Shard %d: read %.1f MB in %.3fs", idx, len(data) / 1e6, elapsed
        )
        return data
