from typing import Optional

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

        self.load_balance_domain = load_balance_domain
        self.worker_hosts = worker_hosts
        self.worker_http_port = worker_http_port
        self.concurrency = concurrency
        self.use_mem_cache = use_mem_cache
        self.mem_map_capacity = mem_map_capacity
        self.use_local_disk_cache = use_local_disk_cache
        self.local_disk_cache_dir = local_disk_cache_dir
