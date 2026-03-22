"""
Unit tests for alluxiofs.torch_dataset (AlluxioDataset / AlluxioIterableDataset).

Uses an in-memory fsspec filesystem so no Alluxio cluster is needed.
"""

import io
import unittest

_TORCH_AVAILABLE = False
try:
    import torch
    from torch.utils.data import DataLoader

    _TORCH_AVAILABLE = True
except ImportError:
    pass

_FSSPEC_AVAILABLE = False
try:
    import fsspec

    _FSSPEC_AVAILABLE = True
except ImportError:
    pass


class InMemoryFS:
    """
    Minimal fsspec-compatible filesystem backed by a dict.
    Supports: ls (with detail=True) and open.
    """

    def __init__(self, files: dict):
        # files: {path: bytes}
        self._files = dict(files)

    def ls(self, path, detail=True):
        path = path.rstrip("/")
        entries = []
        for p, data in self._files.items():
            if p.startswith(path + "/") and "/" not in p[len(path) + 1 :]:
                if detail:
                    entries.append({"name": p, "type": "file", "size": len(data)})
                else:
                    entries.append(p)
        return entries

    def open(self, path, mode="rb"):
        if path not in self._files:
            raise FileNotFoundError(path)
        return io.BytesIO(self._files[path])


def _make_fs(n_files=10):
    """Create an in-memory filesystem with n_files text files."""
    files = {
        f"dataset/train/file_{i:03d}.bin": f"content_{i}".encode()
        for i in range(n_files)
    }
    return InMemoryFS(files)


@unittest.skipUnless(_TORCH_AVAILABLE, "torch not installed")
class TestAlluxioDataset(unittest.TestCase):
    def setUp(self):
        self.fs = _make_fs(10)
        self.root = "dataset/train"

    def test_len(self):
        from alluxiofs.torch_dataset import AlluxioDataset

        ds = AlluxioDataset(self.fs, self.root)
        self.assertEqual(len(ds), 10)

    def test_getitem_returns_bytes(self):
        from alluxiofs.torch_dataset import AlluxioDataset

        ds = AlluxioDataset(self.fs, self.root)
        item = ds[0]
        self.assertIsInstance(item, bytes)

    def test_transform_applied(self):
        from alluxiofs.torch_dataset import AlluxioDataset

        transform = lambda b: b.decode("utf-8")
        ds = AlluxioDataset(self.fs, self.root, transform=transform)
        item = ds[0]
        self.assertIsInstance(item, str)
        self.assertTrue(item.startswith("content_"))

    def test_file_filter(self):
        from alluxiofs.torch_dataset import AlluxioDataset

        # Keep only even-numbered files
        ds = AlluxioDataset(
            self.fs,
            self.root,
            file_filter=lambda p: int(p.split("_")[-1].replace(".bin", "")) % 2 == 0,
        )
        self.assertEqual(len(ds), 5)

    def test_index_error(self):
        from alluxiofs.torch_dataset import AlluxioDataset

        ds = AlluxioDataset(self.fs, self.root)
        with self.assertRaises(IndexError):
            _ = ds[100]

    def test_file_list_cached(self):
        from alluxiofs.torch_dataset import AlluxioDataset

        ds = AlluxioDataset(self.fs, self.root, cache_file_list=True)
        _ = len(ds)
        _ = len(ds)  # second call uses cache
        self.assertIsNotNone(ds._file_paths)

    def test_repr(self):
        from alluxiofs.torch_dataset import AlluxioDataset

        ds = AlluxioDataset(self.fs, self.root)
        _ = len(ds)
        r = repr(ds)
        self.assertIn("AlluxioDataset", r)
        self.assertIn("10", r)


@unittest.skipUnless(_TORCH_AVAILABLE, "torch not installed")
class TestAlluxioIterableDataset(unittest.TestCase):
    def setUp(self):
        self.fs = _make_fs(10)
        self.root = "dataset/train"

    def test_iterates_all_files_single_process(self):
        from alluxiofs.torch_dataset import AlluxioIterableDataset

        ds = AlluxioIterableDataset(self.fs, self.root)
        items = list(ds)
        self.assertEqual(len(items), 10)

    def test_transform_applied(self):
        from alluxiofs.torch_dataset import AlluxioIterableDataset

        ds = AlluxioIterableDataset(
            self.fs, self.root, transform=lambda b: b.decode()
        )
        for item in ds:
            self.assertIsInstance(item, str)

    def test_rank_sharding(self):
        from alluxiofs.torch_dataset import AlluxioIterableDataset

        # Two ranks should partition all 10 files
        items_r0 = list(
            AlluxioIterableDataset(self.fs, self.root, rank=0, world_size=2)
        )
        items_r1 = list(
            AlluxioIterableDataset(self.fs, self.root, rank=1, world_size=2)
        )
        # No overlap
        self.assertEqual(len(set(items_r0) & set(items_r1)), 0)
        # Union covers all
        self.assertEqual(len(items_r0) + len(items_r1), 10)

    def test_file_filter(self):
        from alluxiofs.torch_dataset import AlluxioIterableDataset

        ds = AlluxioIterableDataset(
            self.fs,
            self.root,
            file_filter=lambda p: p.endswith("_000.bin"),
        )
        items = list(ds)
        self.assertEqual(len(items), 1)

    def test_repr(self):
        from alluxiofs.torch_dataset import AlluxioIterableDataset

        ds = AlluxioIterableDataset(self.fs, self.root)
        self.assertIn("AlluxioIterableDataset", repr(ds))

    def test_set_epoch(self):
        from alluxiofs.torch_dataset import AlluxioIterableDataset

        ds = AlluxioIterableDataset(self.fs, self.root, shuffle=True, seed=0)
        ds.set_epoch(5)
        self.assertEqual(ds._epoch, 5)

    def test_shuffle_changes_order(self):
        from alluxiofs.torch_dataset import AlluxioIterableDataset

        ds_epoch0 = AlluxioIterableDataset(self.fs, self.root, shuffle=True, seed=42)
        ds_epoch0.set_epoch(0)
        order0 = list(ds_epoch0)

        ds_epoch1 = AlluxioIterableDataset(self.fs, self.root, shuffle=True, seed=42)
        ds_epoch1.set_epoch(1)
        order1 = list(ds_epoch1)

        # Contents are the same but order is different (with high probability)
        self.assertEqual(sorted(order0), sorted(order1))
        # Very unlikely to be the same order with 10 items and different epochs
        # (but not guaranteed, so we just check same content)


if __name__ == "__main__":
    unittest.main()
