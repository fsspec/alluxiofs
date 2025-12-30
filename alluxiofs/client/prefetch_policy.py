from abc import ABC
from abc import abstractmethod

from alluxiofs.client.log import setup_logger
from alluxiofs.client.log import TagAdapter


class PrefetchPolicy(ABC):
    """
    Base class for all prefetch policies.
    Implementations must override get_blocks().
    """

    def __init__(self, block_size):
        self.block_size = block_size

        # common state
        self.last_end_block = None
        self.last_time = None
        self.last_request_rtt = None  # latest measured RTT
        self.average_rtt = None  # moving average

    @abstractmethod
    def get_blocks(self, offset, length, file_size):
        pass

    def update_rtt(self, rtt):
        """Update RTT statistics for adaptive prefetch."""
        self.last_request_rtt = rtt
        if self.average_rtt is None:
            self.average_rtt = rtt
        else:
            self.average_rtt = 0.8 * self.average_rtt + 0.2 * rtt


class NoPrefetchPolicy(PrefetchPolicy):
    def __init__(self, block_size, config=None):
        super().__init__(block_size)
        if config is None:
            base_logger = setup_logger(
                class_name=self.__class__.__name__,
            )
        else:
            self.config = config
            base_logger = setup_logger(
                self.config.log_dir,
                self.config.log_level,
                self.__class__.__name__,
                self.config.log_tag_allowlist,
            )
        self.logger = TagAdapter(base_logger, {"tag": "[PREFETCH]"})

    def get_blocks(self, offset, length, file_size):
        start_block = offset // self.block_size

        if length == -1:
            end_block = (file_size - 1) // self.block_size
        else:
            end_block = (offset + length - 1) // self.block_size

        self.logger.debug(f"The window size is {end_block - start_block}")
        return start_block, end_block


class FixedWindowPrefetchPolicy(PrefetchPolicy):
    def __init__(self, block_size, config=None):
        super().__init__(block_size)
        if config is None:
            base_logger = setup_logger(
                class_name=self.__class__.__name__,
            )
            self.prefetch_ahead = 0
        else:
            self.config = config
            base_logger = setup_logger(
                self.config.log_dir,
                self.config.log_level,
                self.__class__.__name__,
                self.config.log_tag_allowlist,
            )
            self.prefetch_ahead = self.config.local_cache_prefetch_ahead_blocks
        self.logger = TagAdapter(base_logger, {"tag": "[PREFETCH]"})

    def get_windows_size(self):
        return self.prefetch_ahead

    def get_blocks(self, offset, length, file_size):

        start_block = offset // self.block_size

        if length == -1:
            end_block = (file_size - 1) // self.block_size
        else:
            end_block = (offset + length - 1) // self.block_size

        # apply prefetch window
        end_block = min(
            end_block + self.prefetch_ahead, (file_size - 1) // self.block_size
        )
        self.logger.debug(f"The prefetch_window size is {self.prefetch_ahead}")
        return start_block, end_block


class AdaptiveWindowPrefetchPolicy(PrefetchPolicy):
    def __init__(self, block_size, config=None):
        super().__init__(block_size)
        self.prefetch_ahead = 0
        if config is None:
            base_logger = setup_logger(
                class_name=self.__class__.__name__,
            )
            self.max_prefetch = 0
        else:
            self.config = config
            base_logger = setup_logger(
                self.config.log_dir,
                self.config.log_level,
                self.__class__.__name__,
                self.config.log_tag_allowlist,
            )
            self.max_prefetch = self.config.local_cache_max_prefetch_blocks
        self.logger = TagAdapter(base_logger, {"tag": "[PREFETCH]"})
        self.min_prefetch = 0
        self.last_end_block = 0

    def _adjust_prefetch_by_offset(self, start_block):
        if self.last_end_block is None:
            return

        if start_block < self.last_end_block:
            self.prefetch_ahead = self.min_prefetch
        elif start_block - self.last_end_block - 1 < self.prefetch_ahead:
            return
        elif start_block - self.last_end_block - 1 == self.prefetch_ahead:
            if self.prefetch_ahead == 0:
                self.prefetch_ahead = 1
            else:
                self.prefetch_ahead = min(
                    self.prefetch_ahead * 2, self.max_prefetch
                )
        else:
            self.prefetch_ahead = self.min_prefetch

    def get_window_size(self):
        return self.prefetch_ahead

    def get_blocks(self, offset, length, file_size):
        start_block = offset // self.block_size

        if length == -1:
            end_block = (file_size - 1) // self.block_size
        else:
            end_block = (offset + length - 1) // self.block_size

        self._adjust_prefetch_by_offset(start_block)
        self.last_end_block = end_block

        # apply prefetch window
        end_block = min(
            end_block + self.prefetch_ahead,
            (file_size - 1) // self.block_size,
        )
        self.logger.debug(f"The prefetch_window size is {self.prefetch_ahead}")
        return start_block, end_block
