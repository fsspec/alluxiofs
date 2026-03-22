"""
Unit tests for alluxiofs.checkpoint (CheckpointWriter / CheckpointReader).

These tests use a mock AlluxioClient backed by an in-memory dict so they run
without a live Alluxio cluster or GPU.
"""

import io
import json
import unittest
from unittest.mock import MagicMock, patch

_TORCH_AVAILABLE = False
try:
    import torch

    _TORCH_AVAILABLE = True
except ImportError:
    pass


def _make_state_dict():
    if not _TORCH_AVAILABLE:
        return None
    return {
        "layer1.weight": torch.randn(64, 64),
        "layer1.bias": torch.zeros(64),
        "layer2.weight": torch.randn(32, 64),
        "step": torch.tensor(42),
    }


class InMemoryAlluxioClient:
    """Minimal AlluxioClient stand-in using an in-process dict for storage."""

    def __init__(self):
        self._store: dict = {}

    def write_chunked(self, file_path: str, file_bytes: bytes, chunk_size: int = 1024 * 1024):
        self._store[file_path] = file_bytes
        return True

    def read(self, file_path: str):
        if file_path not in self._store:
            raise FileNotFoundError(f"Not found: {file_path}")
        return io.BytesIO(self._store[file_path])


@unittest.skipUnless(_TORCH_AVAILABLE, "torch not installed")
class TestCheckpointWriter(unittest.TestCase):
    def setUp(self):
        self.client = InMemoryAlluxioClient()
        self.state_dict = _make_state_dict()
        self.ckpt_dir = "s3://bucket/ckpt/step_001"

    def test_save_returns_stats(self):
        from alluxiofs.checkpoint import CheckpointWriter

        writer = CheckpointWriter(self.client, num_shards=4, max_workers=4)
        stats = writer.save(self.state_dict, self.ckpt_dir)

        self.assertIn("total_bytes", stats)
        self.assertIn("num_shards", stats)
        self.assertIn("elapsed_s", stats)
        self.assertIn("throughput_gb_s", stats)
        self.assertIn("shard_times_s", stats)
        self.assertGreater(stats["total_bytes"], 0)
        self.assertGreater(stats["elapsed_s"], 0)

    def test_manifest_written(self):
        from alluxiofs.checkpoint import CheckpointWriter

        writer = CheckpointWriter(self.client, num_shards=2)
        writer.save(self.state_dict, self.ckpt_dir)
        manifest_path = f"{self.ckpt_dir}/_manifest.json"
        self.assertIn(manifest_path, self.client._store)
        manifest = json.loads(self.client._store[manifest_path])
        self.assertEqual(manifest["version"], 1)
        self.assertIn("shard_paths", manifest)
        self.assertIn("total_bytes", manifest)

    def test_shards_written(self):
        from alluxiofs.checkpoint import CheckpointWriter

        num_shards = 3
        writer = CheckpointWriter(self.client, num_shards=num_shards)
        stats = writer.save(self.state_dict, self.ckpt_dir)
        actual_shards = stats["num_shards"]
        for i in range(actual_shards):
            shard_path = f"{self.ckpt_dir}/shard_{i:05d}.bin"
            self.assertIn(shard_path, self.client._store)

    def test_single_shard_small_data(self):
        """Very small data should produce 1 shard (no zero-byte splits)."""
        from alluxiofs.checkpoint import CheckpointWriter

        tiny = {"x": torch.tensor([1.0, 2.0])}
        writer = CheckpointWriter(self.client, num_shards=8)
        stats = writer.save(tiny, self.ckpt_dir)
        self.assertGreaterEqual(stats["num_shards"], 1)


@unittest.skipUnless(_TORCH_AVAILABLE, "torch not installed")
class TestCheckpointReader(unittest.TestCase):
    def setUp(self):
        self.client = InMemoryAlluxioClient()
        self.state_dict = _make_state_dict()
        self.ckpt_dir = "s3://bucket/ckpt/step_001"
        # Pre-write a checkpoint
        from alluxiofs.checkpoint import CheckpointWriter

        writer = CheckpointWriter(self.client, num_shards=4)
        writer.save(self.state_dict, self.ckpt_dir)

    def test_load_roundtrip(self):
        from alluxiofs.checkpoint import CheckpointReader

        reader = CheckpointReader(self.client, max_workers=4)
        loaded = reader.load(self.ckpt_dir)
        reader.close()

        for key in self.state_dict:
            self.assertIn(key, loaded)
            if isinstance(self.state_dict[key], torch.Tensor):
                self.assertTrue(
                    torch.allclose(self.state_dict[key].float(), loaded[key].float()),
                    f"Mismatch in key '{key}'",
                )

    def test_prefetch_then_load(self):
        from alluxiofs.checkpoint import CheckpointReader

        reader = CheckpointReader(self.client, max_workers=4)
        reader.prefetch(self.ckpt_dir)
        # Futures are stored
        self.assertIn(self.ckpt_dir, reader._prefetch_futures)
        loaded = reader.load(self.ckpt_dir)
        # Futures should be consumed after load
        self.assertNotIn(self.ckpt_dir, reader._prefetch_futures)
        reader.close()

        for key in self.state_dict:
            self.assertIn(key, loaded)

    def test_load_manifest(self):
        from alluxiofs.checkpoint import CheckpointReader

        reader = CheckpointReader(self.client)
        manifest = reader.load_manifest(self.ckpt_dir)
        self.assertEqual(manifest["version"], 1)
        self.assertIn("shard_paths", manifest)
        self.assertIn("metadata", manifest)
        reader.close()


if __name__ == "__main__":
    unittest.main()
