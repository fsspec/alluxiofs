from abc import ABC
from abc import abstractmethod


class PrefetchPolicy(ABC):
    """
    Base class for all prefetch policies.
    Implementations must override get_blocks().
    """

    def __init__(self, block_size):
        self.block_size = block_size

        # common state
        self.last_offset = None
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
    def __init__(self, block_size):
        super().__init__(block_size)

    def get_blocks(self, offset, length, file_size):
        start_block = offset // self.block_size

        if length == -1:
            end_block = (file_size - 1) // self.block_size
        else:
            end_block = (offset + length - 1) // self.block_size

        return start_block, end_block


class FixedWindowPrefetchPolicy(PrefetchPolicy):
    def __init__(self, block_size, mcap_prefetch_ahead_blocks):
        super().__init__(block_size)
        self.mcap_prefetch_ahead_blocks = mcap_prefetch_ahead_blocks

    def get_blocks(self, offset, length, file_size):
        prefetch_ahead = self.mcap_prefetch_ahead_blocks

        start_block = offset // self.block_size

        if length == -1:
            end_block = (file_size - 1) // self.block_size
        else:
            end_block = (offset + length - 1) // self.block_size

        # apply prefetch window
        end_block = min(
            end_block + prefetch_ahead, (file_size - 1) // self.block_size
        )

        return start_block, end_block


class AdaptiveWindowPrefetchPolicy(PrefetchPolicy):
    def __init__(self, block_size, mcap_max_prefetch_blocks):
        super().__init__(block_size)
        self.prefetch_ahead = 0
        self.max_prefetch = mcap_max_prefetch_blocks
        self.min_prefetch = 0
        self.last_offset = 0

    def _adjust_prefetch_by_offset(self, offset):
        if self.last_offset is None:
            return

        last_index = self.last_offset // self.block_size
        index = offset // self.block_size
        if index < last_index:
            self.prefetch_ahead = self.min_prefetch
        elif index - last_index - 1 < self.prefetch_ahead:
            return
        elif index - last_index - 1 == self.prefetch_ahead:
            if self.prefetch_ahead == 0:
                self.prefetch_ahead = 1
            else:
                self.prefetch_ahead = min(
                    self.prefetch_ahead * 2, self.max_prefetch
                )
        else:
            self.prefetch_ahead = self.min_prefetch
        self.last_offset = offset

    def get_blocks(self, offset, length, file_size):
        start_block = offset // self.block_size

        if length == -1:
            end_block = (file_size - 1) // self.block_size
        else:
            end_block = (offset + length - 1) // self.block_size

        self._adjust_prefetch_by_offset(offset)

        # apply prefetch window
        end_block = min(
            end_block + self.prefetch_ahead,
            (file_size - 1) // self.block_size,
        )
        return start_block, end_block
