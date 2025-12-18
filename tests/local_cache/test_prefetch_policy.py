from unittest.mock import MagicMock
from alluxiofs.client.prefetch_policy import (
    NoPrefetchPolicy,
    FixedWindowPrefetchPolicy,
    AdaptiveWindowPrefetchPolicy,
)

class TestNoPrefetchPolicy:
    def test_get_blocks(self):
        policy = NoPrefetchPolicy(block_size=1024)
        # offset=0, length=1024 -> block 0
        assert policy.get_blocks(0, 1024, 10000) == (0, 0)
        # offset=0, length=2048 -> blocks 0, 1
        assert policy.get_blocks(0, 2048, 10000) == (0, 1)
        # offset=1024, length=1024 -> block 1
        assert policy.get_blocks(1024, 1024, 10000) == (1, 1)
        # offset=512, length=1024 -> blocks 0, 1 (spans across boundary)
        assert policy.get_blocks(512, 1024, 10000) == (0, 1)

    def test_get_blocks_full_file(self):
        policy = NoPrefetchPolicy(block_size=1024)
        # length=-1 -> read until end
        # file_size=5000 -> blocks 0, 1, 2, 3, 4. (4*1024=4096, 5000 is in block 4)
        assert policy.get_blocks(0, -1, 5000) == (0, 4)

class TestFixedWindowPrefetchPolicy:
    def test_init_defaults(self):
        policy = FixedWindowPrefetchPolicy(block_size=1024)
        assert policy.prefetch_ahead == 0

    def test_init_with_config(self):
        config = MagicMock()
        config.local_cache_prefetch_ahead_blocks = 5
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        policy = FixedWindowPrefetchPolicy(block_size=1024, config=config)
        assert policy.prefetch_ahead == 5

    def test_get_blocks(self):
        config = MagicMock()
        config.local_cache_prefetch_ahead_blocks = 2
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        policy = FixedWindowPrefetchPolicy(block_size=1024, config=config)

        # Request block 0. Prefetch ahead 2 -> end block should be 0 + 2 = 2
        assert policy.get_blocks(0, 1024, 10000) == (0, 2)

        # Request block 1. Prefetch ahead 2 -> end block should be 1 + 2 = 3
        assert policy.get_blocks(1024, 1024, 10000) == (1, 3)

    def test_get_blocks_boundary(self):
        config = MagicMock()
        config.local_cache_prefetch_ahead_blocks = 10
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        policy = FixedWindowPrefetchPolicy(block_size=1024, config=config)

        # File size 5000 (approx 5 blocks: 0,1,2,3,4).
        # Request block 0. Prefetch 10. Max block is 4.
        # Should return (0, 4)
        assert policy.get_blocks(0, 1024, 5000) == (0, 4)

class TestAdaptiveWindowPrefetchPolicy:
    def test_init(self):
        config = MagicMock()
        config.local_cache_max_prefetch_blocks = 8
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        policy = AdaptiveWindowPrefetchPolicy(block_size=1024, config=config)
        assert policy.max_prefetch == 8
        assert policy.prefetch_ahead == 0

    def test_sequential_access_increases_window(self):
        config = MagicMock()
        config.local_cache_max_prefetch_blocks = 8
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        policy = AdaptiveWindowPrefetchPolicy(block_size=1024, config=config)

        # Initial state
        assert policy.prefetch_ahead == 0

        # Access block 0.
        # index=0, last_index=0. index-last_index-1 = -1 < 0. Returns.
        policy.get_blocks(0, 1024, 100000)
        assert policy.prefetch_ahead == 0

        # Access block 1.
        # index=1, last_index=0. index-last_index-1 = 0 == 0.
        # prefetch_ahead becomes 1.
        policy.get_blocks(1024, 1024, 100000)
        assert policy.prefetch_ahead == 1

        # Access block 2.
        # index=2, last_index=1. index-last_index-1 = 0 < 1. Returns.
        policy.get_blocks(2048, 1024, 100000)
        assert policy.prefetch_ahead == 1

        # Access block 3.
        # index=3, last_index=1. index-last_index-1 = 1 == 1.
        # prefetch_ahead becomes 2.
        policy.get_blocks(3072, 1024, 100000)
        assert policy.prefetch_ahead == 2

    def test_random_access_resets_window(self):
        config = MagicMock()
        config.local_cache_max_prefetch_blocks = 8
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        policy = AdaptiveWindowPrefetchPolicy(block_size=1024, config=config)
        policy.prefetch_ahead = 5
        policy.last_offset = 10240 # block 10

        # Jump back to block 0
        # index=0, last_index=10. index < last_index.
        # prefetch_ahead = min_prefetch (0).
        policy.get_blocks(0, 1024, 100000)
        assert policy.prefetch_ahead == 0

    def test_jump_forward_resets_window(self):
        config = MagicMock()
        config.local_cache_max_prefetch_blocks = 8
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        policy = AdaptiveWindowPrefetchPolicy(block_size=1024, config=config)
        policy.prefetch_ahead = 2
        policy.last_offset = 0 # block 0

        # Jump far forward to block 10
        # index=10, last_index=0. index-last_index-1 = 9 > prefetch_ahead(2).
        # else branch: prefetch_ahead = min_prefetch (0).
        policy.get_blocks(10240, 1024, 100000)
        assert policy.prefetch_ahead == 0

