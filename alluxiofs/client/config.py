from typing import Optional

from .const import ALLUXIO_REQUEST_MAX_RETRIES
from .const import ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS
from .const import ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE
from .const import ALLUXIO_WORKER_S3_SERVER_PORT_DEFAULT_VALUE


class AlluxioClientConfig:
    """
    Class responsible for creating the configuration for Alluxio Client.
    """

    def __init__(
        self,
        load_balance_domain: str = "localhost",
        worker_hosts: Optional[str] = None,
        worker_http_port=ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
        worker_data_port: int = ALLUXIO_WORKER_S3_SERVER_PORT_DEFAULT_VALUE,
        fallback_to_ufs_enabled=True,
        ufs_info_refresh_interval_minutes=2,
        log_level="INFO",
        log_dir=None,
        log_tag_allowlist=None,
        use_mem_cache=False,
        mem_map_capacity=1024,
        use_local_disk_cache=False,
        local_disk_cache_dir="/tmp/local_cache/",
        local_cache_dir="/tmp/local_cache/",
        local_cache_enabled=False,
        local_cache_prefetch_ahead_blocks=2,
        local_cache_prefetch_concurrency=32,
        local_cache_size_gb=64,
        local_cache_block_size_mb=4,
        local_cache_eviction_high_watermark=0.8,
        local_cache_eviction_low_watermark=0.7,
        local_cache_max_prefetch_blocks=16,
        local_cache_prefetch_policy="adaptive_window",
        local_cache_eviction_scan_interval_minutes=0.5,
        local_cache_ttl_time_minutes=10,
        use_memory_cache=False,
        memory_cache_size_mb=256,
        http_max_retries=ALLUXIO_REQUEST_MAX_RETRIES,
        http_timeouts=ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS,
        read_buffer_size_mb=0.008,
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

        assert (
            isinstance(worker_hosts, str) or worker_hosts is None
        ), "'worker_hosts' should be string or None"

        assert isinstance(worker_http_port, int) and (
            1 <= worker_http_port <= 65535
        ), "'worker_http_port' should be an integer in the range 1-65535"

        assert isinstance(worker_data_port, int) and (
            1 <= worker_data_port <= 65535
        ), "'worker_data_port' should be an integer in the range 1-65535"

        assert isinstance(
            fallback_to_ufs_enabled, bool
        ), "'fallback_to_ufs_enabled' should be a boolean"

        assert isinstance(log_level, str) and log_level in [
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        ], "'log_level' should be one of 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'"

        assert (
            isinstance(log_dir, str) or log_dir is None
        ), "'log_dir' should be a string or None"

        assert (
            isinstance(log_tag_allowlist, str) or log_tag_allowlist is None
        ), "'log_tag_allowlist' should be a string"

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
            local_cache_enabled, bool
        ), "'local_cache_enabled' should be a boolean"

        assert isinstance(
            local_cache_dir, str
        ), "'local_cache_dir' should be a string"

        assert (
            isinstance(local_cache_prefetch_ahead_blocks, int)
            and local_cache_prefetch_ahead_blocks >= 0
        ), "'local_cache_prefetch_ahead_blocks' should be a non-negative integer"

        assert (
            isinstance(local_cache_prefetch_concurrency, int)
            and local_cache_prefetch_concurrency > 0
        ), "'local_cache_prefetch_concurrency' should be a positive integer"

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

        assert (
            isinstance(local_cache_max_prefetch_blocks, int)
            and local_cache_max_prefetch_blocks >= 0
        ), "'local_cache_max_prefetch_blocks' should be a non-negative integer"

        assert isinstance(
            local_cache_prefetch_policy, str
        ), "'local_cache_prefetch_policy' should be a string"

        assert (
            isinstance(ufs_info_refresh_interval_minutes, float)
            or isinstance(ufs_info_refresh_interval_minutes, int)
        ) and ufs_info_refresh_interval_minutes > 0, "'ufs_info_refresh_interval_minutes' should be a positive float or integer"

        assert (
            isinstance(local_cache_eviction_high_watermark, float)
            and 0 < local_cache_eviction_high_watermark < 1
        ), "'local_cache_eviction_high_watermark' should be a float between 0 and 1"

        assert (
            isinstance(local_cache_eviction_low_watermark, float)
            and 0 < local_cache_eviction_low_watermark < 1
        ), "'local_cache_eviction_low_watermark' should be a float between 0 and 1"

        assert (
            local_cache_eviction_high_watermark
            > local_cache_eviction_low_watermark
        ), "'local_cache_eviction_high_watermark' must be greater than 'local_cache_eviction_low_watermark'"

        assert (
            isinstance(
                local_cache_eviction_scan_interval_minutes, (int, float)
            )
            and local_cache_eviction_scan_interval_minutes > 0
        ), "'local_cache_eviction_scan_interval_minutes' should be a positive integer or float"

        assert (
            isinstance(local_cache_ttl_time_minutes, (int, float))
            and local_cache_ttl_time_minutes > 0
        ), "'local_cache_ttl_time_minutes' should be a positive integer or float"

        self.load_balance_domain = load_balance_domain
        self.worker_hosts = worker_hosts
        self.worker_http_port = worker_http_port
        self.worker_data_port = worker_data_port
        self.log_level = log_level
        self.log_dir = log_dir
        self.log_tag_allowlist = log_tag_allowlist
        self.use_mem_cache = use_mem_cache
        self.mem_map_capacity = mem_map_capacity
        self.use_local_disk_cache = use_local_disk_cache
        self.local_disk_cache_dir = local_disk_cache_dir
        self.local_cache_enabled = local_cache_enabled
        self.local_cache_dir = local_cache_dir
        self.local_cache_prefetch_ahead_blocks = (
            local_cache_prefetch_ahead_blocks
        )
        self.local_cache_prefetch_concurrency = (
            local_cache_prefetch_concurrency
        )
        self.local_cache_size_gb = local_cache_size_gb
        self.local_cache_block_size_mb = local_cache_block_size_mb
        self.use_memory_cache = use_memory_cache
        self.memory_cache_size_mb = memory_cache_size_mb
        self.http_max_retries = http_max_retries
        self.http_timeouts = http_timeouts
        self.read_buffer_size_mb = read_buffer_size_mb
        self.local_cache_max_prefetch_blocks = local_cache_max_prefetch_blocks
        self.local_cache_prefetch_policy = local_cache_prefetch_policy
        self.fallback_to_ufs_enabled = fallback_to_ufs_enabled
        self.ufs_info_refresh_interval_minutes = (
            ufs_info_refresh_interval_minutes
        )
        self.local_cache_eviction_high_watermark = (
            local_cache_eviction_high_watermark
        )
        self.local_cache_eviction_low_watermark = (
            local_cache_eviction_low_watermark
        )
        self.local_cache_eviction_scan_interval_minutes = (
            local_cache_eviction_scan_interval_minutes
        )
        self.local_cache_ttl_time_minutes = local_cache_ttl_time_minutes
