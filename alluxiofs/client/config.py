from typing import Optional

import humanfriendly

from .const import ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE
from .const import ALLUXIO_HASH_NODE_PER_WORKER_DEFAULT_VALUE
from .const import ALLUXIO_PAGE_SIZE_DEFAULT_VALUE
from .const import ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE


class AlluxioClientConfig:
    """
    Class responsible for creating the configuration for Alluxio Client.
    """

    def __init__(
        self,
        etcd_hosts: Optional[str] = None,
        worker_hosts: Optional[str] = None,
        etcd_port=2379,
        worker_http_port=ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
        etcd_refresh_workers_interval=120,
        page_size=ALLUXIO_PAGE_SIZE_DEFAULT_VALUE,
        hash_node_per_worker=ALLUXIO_HASH_NODE_PER_WORKER_DEFAULT_VALUE,
        cluster_name=ALLUXIO_CLUSTER_NAME_DEFAULT_VALUE,
        etcd_username: Optional[str] = None,
        etcd_password: Optional[str] = None,
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
            etcd_hosts (Optional[str], optional): The hostnames of ETCD to get worker addresses from
                in 'host1,host2,host3' format. Either etcd_hosts or worker_hosts should be provided, not both.
            worker_hosts (Optional[str], optional): The worker hostnames in 'host1,host2,host3' format.
                Either etcd_hosts or worker_hosts should be provided, not both.
            concurrency (int, optional): The maximum number of concurrent operations for HTTP requests, default to 64.
            etcd_port (int, optional): The port of each etcd server.
            worker_http_port (int, optional): The port of the HTTP server on each Alluxio worker node.
            etcd_refresh_workers_interval (int, optional): The interval to refresh worker list from ETCD membership service periodically.
                All negative values mean the service is disabled.
        """

        assert (
            etcd_hosts or worker_hosts
        ), "Must supply either 'etcd_hosts' or 'worker_hosts'"

        assert not (
            etcd_hosts and worker_hosts
        ), "Supply either 'etcd_hosts' or 'worker_hosts', not both"

        assert isinstance(etcd_port, int) and (
            1 <= etcd_port <= 65535
        ), "'etcd_port' should be an integer in the range 1-65535"

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
            etcd_refresh_workers_interval, int
        ), "'etcd_refresh_workers_interval' should be an integer"

        self.etcd_hosts = etcd_hosts
        self.worker_hosts = worker_hosts
        self.etcd_port = etcd_port
        self.worker_http_port = worker_http_port
        self.etcd_refresh_workers_interval = etcd_refresh_workers_interval

        assert (
            isinstance(hash_node_per_worker, int) and hash_node_per_worker > 0
        ), "'hash_node_per_worker' should be a positive integer"

        self.hash_node_per_worker = hash_node_per_worker
        self.page_size = humanfriendly.parse_size(page_size, binary=True)
        self.cluster_name = cluster_name

        assert (etcd_username is None) == (
            etcd_password is None
        ), "Both ETCD username and password must be set or both should be unset."

        self.etcd_username = etcd_username
        self.etcd_password = etcd_password
        self.concurrency = concurrency
        self.use_mem_cache = use_mem_cache
        self.mem_map_capacity = mem_map_capacity
        self.use_local_disk_cache = use_local_disk_cache
        self.local_disk_cache_dir = local_disk_cache_dir
