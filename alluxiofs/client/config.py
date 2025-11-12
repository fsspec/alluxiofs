from typing import Optional

from .const import ALLUXIO_REQUEST_MAX_RETRIES
from .const import ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS
from .const import ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE


class AlluxioClientConfig:
    """
    Class responsible for creating the configuration for Alluxio Client.
    """

    def __init__(
        self,
        load_balance_domain: str = None,
        worker_hosts: Optional[str] = None,
        worker_http_port=ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
        concurrency=64,
        use_mem_cache=False,
        mem_map_capacity=1024,
        use_local_disk_cache=False,
        local_disk_cache_dir="/tmp/local_cache/",
        local_cache_dir="/tmp/local_cache/",
        mcap_enabled=False,
        mcap_prefetch_ahead_blocks=16,
        mcap_prefetch_concurrency=32,
        local_cache_size_gb=64,
        local_cache_block_size_mb=16,
        use_memory_cache=False,
        memory_cache_size_mb=256,
        http_max_retries=ALLUXIO_REQUEST_MAX_RETRIES,
        http_timeouts=ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS,
        read_buffer_size_mb=0.064,
        **kwargs,
    ):
        """
        Initializes Alluxio client configuration.
        Args:
            worker_hosts (Optional[str], optional): The worker hostnames in 'host1,host2,host3' format.
            concurrency (int, optional): The maximum number of concurrent operations for HTTP requests, default to 64.
            worker_http_port (int, optional): The port of the HTTP server on each Alluxio worker node.
        """
        assert (
            isinstance(load_balance_domain, str) or load_balance_domain is None
        ), "'load_balance_domain' should be string"

        assert isinstance(worker_http_port, int) and (
            1 <= worker_http_port <= 65535
        ), "'worker_http_port' should be an integer in the range 1-65535"

        assert (
            isinstance(concurrency, int) and concurrency > 0
        ), "'concurrency' should be a positive integer"

        assert isinstance(
            use_mem_cache, bool
        ), "'use_mem_cache' should be a boolean"

        assert (
            isinstance(mem_map_capacity, int) and mem_map_capacity > 0
        ), "'mem_map_capacity' should be a positive integer"

        assert isinstance(
            use_local_disk_cache, bool
        ), "'use_local_disk_cache' should be a boolean"

        assert isinstance(
            local_disk_cache_dir, str
        ), "'local_disk_cache_dir' should be a string"

        assert isinstance(
            mcap_enabled, bool
        ), "'mcap_enabled' should be a boolean"

        assert isinstance(
            local_cache_dir, str
        ), "'local_cache_dir' should be a string"

        assert (
            isinstance(mcap_prefetch_ahead_blocks, int)
            and mcap_prefetch_ahead_blocks >= 0
        ), "'mcap_prefetch_ahead_blocks' should be a non-negative integer"

        assert (
            isinstance(mcap_prefetch_concurrency, int)
            and mcap_prefetch_concurrency > 0
        ), "'mcap_prefetch_concurrency' should be a positive integer"

        assert (
            (
                isinstance(local_cache_size_gb, int)
                or isinstance(local_cache_size_gb, float)
            )
            and local_cache_size_gb > 0
            or isinstance(local_cache_size_gb, str)
        ), "'local_cache_size_gb' should be a positive integer or float"

        assert (
            isinstance(local_cache_block_size_mb, int)
            or isinstance(local_cache_block_size_mb, float)
        ) and local_cache_block_size_mb > 0, (
            "'local_cache_block_size_mb' should be a positive integer or float"
        )

        assert isinstance(
            use_memory_cache, bool
        ), "'use_memory_range_cache' should be a boolean"

        assert (
            isinstance(memory_cache_size_mb, int)
            or isinstance(memory_cache_size_mb, float)
        ) and memory_cache_size_mb > 0, "'memory_range_cache_size_mb' should be a positive integer or float"

        assert (
            isinstance(http_max_retries, int) and http_max_retries >= 0
        ), "'http_max_retries' should be a non-negative integer"

        assert (
            isinstance(http_timeouts, int) and http_timeouts > 0
        ), "'http_timeouts' should be a positive integer"

        assert (
            isinstance(read_buffer_size_mb, int)
            or isinstance(read_buffer_size_mb, float)
        ) and read_buffer_size_mb > 0, (
            "'read_buffer_size_mb' should be a positive integer or float"
        )

        self.load_balance_domain = load_balance_domain
        self.worker_hosts = worker_hosts
        self.worker_http_port = worker_http_port
        self.concurrency = concurrency
        self.use_mem_cache = use_mem_cache
        self.mem_map_capacity = mem_map_capacity
        self.use_local_disk_cache = use_local_disk_cache
        self.local_disk_cache_dir = local_disk_cache_dir
        self.mcap_enabled = mcap_enabled
        self.local_cache_dir = local_cache_dir
        self.mcap_prefetch_ahead_blocks = mcap_prefetch_ahead_blocks
        self.mcap_prefetch_concurrency = mcap_prefetch_concurrency
        self.local_cache_size_gb = local_cache_size_gb
        self.local_cache_block_size_mb = local_cache_block_size_mb
        self.use_memory_cache = use_memory_cache
        self.memory_cache_size_mb = memory_cache_size_mb
        self.http_max_retries = http_max_retries
        self.http_timeouts = http_timeouts
        self.read_buffer_size_mb = read_buffer_size_mb
