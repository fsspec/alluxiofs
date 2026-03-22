"""
PyTorch DataLoader-compatible Dataset classes backed by AlluxioFileSystem.

AlluxioDataset
    Map-style dataset. Each item is one file in ``root_path``. Works with
    torch.utils.data.DistributedSampler for DDP training.

AlluxioIterableDataset
    Streaming dataset. Files are automatically sharded across DDP ranks *and*
    DataLoader worker processes so every sample is read exactly once per epoch.

Both classes accept an optional ``transform`` callable that converts raw bytes
into whatever tensor/label structure your model expects.

Example – ImageNet-style image loader
--------------------------------------
    import io
    from PIL import Image
    import torchvision.transforms as T
    from torch.utils.data import DataLoader
    from alluxiofs import AlluxioFileSystem
    from alluxiofs.torch_dataset import AlluxioDataset

    def decode_image(raw: bytes):
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        return T.ToTensor()(img)

    fs = AlluxioFileSystem(worker_hosts="host1,host2", target_protocol="s3")
    dataset = AlluxioDataset(fs, "s3://bucket/imagenet/train", transform=decode_image)
    loader = DataLoader(dataset, batch_size=64, num_workers=8, shuffle=True)

Example – streaming parquet tokens
------------------------------------
    dataset = AlluxioIterableDataset(
        fs, "s3://bucket/tokens/train",
        transform=parse_parquet_row,
        rank=dist.get_rank(),
        world_size=dist.get_world_size(),
    )
    loader = DataLoader(dataset, batch_size=32, num_workers=4)
"""

import logging
import math
from typing import Callable, Iterator, List, Optional

logger = logging.getLogger(__name__)

_TORCH_AVAILABLE = False
try:
    from torch.utils.data import Dataset, IterableDataset, get_worker_info

    _TORCH_AVAILABLE = True
except ImportError:
    pass


def _require_torch():
    if not _TORCH_AVAILABLE:
        raise ImportError(
            "torch is required. Install with: pip install torch"
        )


class AlluxioDataset:
    """
    Map-style PyTorch Dataset backed by AlluxioFileSystem.

    The dataset is a flat list of all files directly under ``root_path``
    (non-recursive). Each ``__getitem__`` opens the corresponding file,
    reads its full content, and optionally applies ``transform``.

    Compatible with ``torch.utils.data.DistributedSampler`` for DDP training
    and with standard ``DataLoader`` prefetch_factor for pipelining.

    Args:
        alluxio_fs: An AlluxioFileSystem instance (or any fsspec filesystem).
        root_path: Remote directory containing dataset files.
        transform: Optional callable ``(bytes) -> Any`` applied to each file's
            raw bytes before returning. Common uses: decode JPEG, parse parquet.
        file_filter: Optional callable ``(str) -> bool`` to include only
            matching paths (e.g., ``lambda p: p.endswith('.jpg')``).
        cache_file_list: Cache the directory listing after the first ``ls``
            call. Set to False if the dataset is written to during training.
    """

    def __init__(
        self,
        alluxio_fs,
        root_path: str,
        transform: Optional[Callable] = None,
        file_filter: Optional[Callable[[str], bool]] = None,
        cache_file_list: bool = True,
    ):
        _require_torch()
        self.fs = alluxio_fs
        self.root_path = root_path.rstrip("/")
        self.transform = transform
        self.file_filter = file_filter
        self._cache_file_list = cache_file_list
        self._file_paths: Optional[List[str]] = None

    def _get_file_paths(self) -> List[str]:
        if self._file_paths is not None:
            return self._file_paths
        entries = self.fs.ls(self.root_path, detail=True)
        paths = []
        for entry in entries:
            if isinstance(entry, dict):
                if entry.get("type") == "file":
                    paths.append(entry["name"])
            elif isinstance(entry, str):
                paths.append(entry)
        if self.file_filter:
            paths = [p for p in paths if self.file_filter(p)]
        paths = sorted(paths)
        if self._cache_file_list:
            self._file_paths = paths
        return paths

    def __len__(self) -> int:
        return len(self._get_file_paths())

    def __getitem__(self, idx: int):
        paths = self._get_file_paths()
        if idx < 0 or idx >= len(paths):
            raise IndexError(
                f"Index {idx} out of range for dataset with {len(paths)} files"
            )
        path = paths[idx]
        with self.fs.open(path, "rb") as f:
            data = f.read()
        if self.transform is not None:
            data = self.transform(data)
        return data

    def __repr__(self) -> str:
        n = len(self._get_file_paths()) if self._file_paths is not None else "?"
        return (
            f"AlluxioDataset(root={self.root_path!r}, "
            f"num_files={n}, "
            f"transform={self.transform!r})"
        )


class AlluxioIterableDataset:
    """
    Iterable-style PyTorch Dataset backed by AlluxioFileSystem.

    Files are automatically sharded across DDP ranks (via ``rank`` /
    ``world_size``) and across DataLoader worker processes (via PyTorch's
    ``get_worker_info()``). Every file is read by exactly one
    (rank, worker) pair per epoch.

    Files are streamed one at a time; the full dataset does not need to fit
    in memory. For large files, ``transform`` can lazily parse records.

    Args:
        alluxio_fs: An AlluxioFileSystem instance.
        root_path: Remote directory containing dataset files.
        transform: Optional callable ``(bytes) -> Any``.
        file_filter: Optional callable ``(str) -> bool``.
        rank: DDP rank of this process (default 0 = single-process).
        world_size: Total DDP ranks (default 1 = single-process).
        shuffle: Shuffle the file order each epoch. Reproducible when
            combined with ``seed``.
        seed: Base random seed for shuffling.
    """

    def __init__(
        self,
        alluxio_fs,
        root_path: str,
        transform: Optional[Callable] = None,
        file_filter: Optional[Callable[[str], bool]] = None,
        rank: int = 0,
        world_size: int = 1,
        shuffle: bool = False,
        seed: int = 42,
    ):
        _require_torch()
        self.fs = alluxio_fs
        self.root_path = root_path.rstrip("/")
        self.transform = transform
        self.file_filter = file_filter
        self.rank = rank
        self.world_size = world_size
        self.shuffle = shuffle
        self.seed = seed
        self._all_paths: Optional[List[str]] = None
        self._epoch: int = 0

    def set_epoch(self, epoch: int) -> None:
        """Update the epoch counter for deterministic per-epoch shuffling."""
        self._epoch = epoch

    def _get_all_paths(self) -> List[str]:
        if self._all_paths is not None:
            return self._all_paths
        entries = self.fs.ls(self.root_path, detail=True)
        paths = []
        for entry in entries:
            if isinstance(entry, dict):
                if entry.get("type") == "file":
                    paths.append(entry["name"])
            elif isinstance(entry, str):
                paths.append(entry)
        if self.file_filter:
            paths = [p for p in paths if self.file_filter(p)]
        self._all_paths = sorted(paths)
        return self._all_paths

    def _get_shard_paths(self) -> List[str]:
        """Return the file paths assigned to this (rank, worker) shard."""
        all_paths = list(self._get_all_paths())

        if self.shuffle:
            import random

            rng = random.Random(self.seed + self._epoch)
            rng.shuffle(all_paths)

        # DDP-level sharding: divide files evenly across ranks
        # Pad to make the per-rank slice the same size for all ranks
        # (mirrors DistributedSampler behavior)
        total = len(all_paths)
        per_rank = math.ceil(total / self.world_size)
        rank_start = self.rank * per_rank
        rank_end = min(rank_start + per_rank, total)
        rank_paths = all_paths[rank_start:rank_end]

        # DataLoader worker-level sharding within this rank's slice
        worker_info = get_worker_info()
        if worker_info is not None:
            num_workers = worker_info.num_workers
            worker_id = worker_info.id
            per_worker = math.ceil(len(rank_paths) / num_workers)
            w_start = worker_id * per_worker
            w_end = min(w_start + per_worker, len(rank_paths))
            rank_paths = rank_paths[w_start:w_end]

        return rank_paths

    def __iter__(self) -> Iterator:
        for path in self._get_shard_paths():
            try:
                with self.fs.open(path, "rb") as f:
                    data = f.read()
                if self.transform is not None:
                    data = self.transform(data)
                yield data
            except Exception as exc:
                logger.warning("Skipping %s due to error: %s", path, exc)
                continue

    def __repr__(self) -> str:
        n = len(self._all_paths) if self._all_paths is not None else "?"
        return (
            f"AlluxioIterableDataset(root={self.root_path!r}, "
            f"total_files={n}, rank={self.rank}/{self.world_size})"
        )
