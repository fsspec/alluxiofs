import socket
from dataclasses import dataclass
from typing import List

from alluxiofs.client.config import AlluxioClientConfig
from alluxiofs.client.const import (
    ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
)


@dataclass(frozen=True)
class WorkerNetAddress:
    host: str = "localhost"
    http_server_port: int = ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE

    @staticmethod
    def from_host_and_port(worker_host, worker_http_port):
        return WorkerNetAddress(worker_host, worker_http_port)

    @staticmethod
    def from_host(worker_host):
        return WorkerNetAddress(
            worker_host, ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE
        )

    @staticmethod
    def from_dict(d):
        return WorkerNetAddress(d["host"], d["http_server_port"])

    @staticmethod
    def to_dict(worker: "WorkerNetAddress"):
        return {
            "host": worker.host,
            "http_server_port": worker.http_server_port,
        }


class WorkerListLoadBalancer:
    def __init__(self, config: AlluxioClientConfig):
        self.workers, self.ports = self._param_worker_addr(config)

    def get_worker(self, path: str) -> WorkerNetAddress:
        # Simple hash-based load balancing
        index = hash(path) % len(self.workers)
        return WorkerNetAddress.from_host_and_port(
            self.workers[index], self.ports[index]
        )

    def get_multiple_worker(self, path, worker_num):
        # Simple hash-based load balancing
        index = hash(path) % len(self.workers)
        selected_workers = []
        for i in range(worker_num):
            selected_workers.append(
                WorkerNetAddress.from_host_and_port(
                    self.workers[(index + i) % len(self.workers)],
                    self.ports[(index + i) % len(self.workers)],
                )
            )
        return selected_workers

    def _param_worker_addr(self, config: AlluxioClientConfig):
        worker_hosts = []
        ports = []
        for worker_host in config.worker_hosts.split(","):
            if ":" in worker_host:
                host, port_str = worker_host.split(":")
            else:
                host = worker_host
                port_str = config.worker_http_port
            worker_hosts.append(host.replace(" ", ""))
            ports.append(int(port_str.replace(" ", "")))
        return worker_hosts, ports


class DNSLoadBalancer:
    """
    A load balancer that uses DNS to discover worker nodes.

    This load balancer resolves a configured hostname to get all associated
    IP addresses (the worker nodes) and distributes requests among them
    using a path-hashing strategy.
    """

    def __init__(self, config: AlluxioClientConfig):
        """
        Initializes the DNSLoadBalancer.

        Args:
            config (AlluxioClientConfig): The client configuration, where `worker_hosts`
                                         should be a single hostname that can be
                                         resolved to multiple IP addresses.
        """
        self.config = config
        self.domain = config.load_balance_domain

    def resolve_address(self) -> str:
        """
        Resolves a domain name to a single IP address using a DNS service.

        This method performs a direct DNS lookup and returns the first IPv4
        address found for the given domain.

        Returns:
            str: The resolved IP address as a string.

        Raises:
            ValueError: If the domain cannot be resolved or no IP address is found.
        """
        try:
            # Request address information for the domain, filtering for IPv4 sockets.
            # socket.getaddrinfo returns a list of tuples.
            addr_info = socket.getaddrinfo(
                self.domain, None, family=socket.AF_INET
            )

            # Check if the result list is not empty.
            if addr_info:
                # The structure of each tuple is (family, type, proto, canonname, sockaddr).
                # The 'sockaddr' is a tuple containing (ip_address, port).
                ip_address = addr_info[0][4][0]
                return ip_address
            else:
                # This case is rare but handled for robustness.
                raise ValueError(f"No address found for domain: {self.domain}")

        except socket.gaierror:
            # This exception is raised for address-related errors, like a domain not found.
            raise ValueError(f"Could not resolve domain: {self.domain}")

    def get_worker(self, path: str) -> WorkerNetAddress:
        """
        Selects a single worker based on the path domain.

        Args:
            domain (str): The domain address of DNS.

        Returns:
            The IP address of the selected worker.
        """
        host = self.resolve_address()
        return WorkerNetAddress.from_host_and_port(
            host, self.config.worker_http_port
        )

    def get_multiple_worker(
        self, path: str, worker_num: int
    ) -> List[WorkerNetAddress]:
        """
        Selects multiple workers based on the path domain.

        Args:
            domain (str): The domain address of DNS.
            worker_num (int): The number of workers to select.

        Returns:
            A list of IP addresses of the selected workers.
        """
        selected_workers = []
        for _ in range(worker_num):
            host = self.resolve_address()
            selected_workers.append(
                WorkerNetAddress.from_host_and_port(
                    host, self.config.worker_http_port
                )
            )
        return selected_workers


if __name__ == "__main__":
    # Example 1: Successfully resolve a domain
    try:
        google_ip = DNSLoadBalancer(
            AlluxioClientConfig(load_balance_domain="www.google.com")
        ).resolve_address()
        print(f"The IP address for www.google.com is: {google_ip}")
    except ValueError as e:
        print(e)

    # Example 2: Attempt to resolve a non-existent domain
    try:
        invalid_ip = DNSLoadBalancer(
            AlluxioClientConfig(
                load_balance_domain="non-existent-domain-12345.com"
            )
        ).resolve_address()
        print(f"The IP address is: {invalid_ip}")
    except ValueError as e:
        print(f"Error resolving domain: {e}")

    # Example 3: Resolve another valid domain
    try:
        github_ip = DNSLoadBalancer(
            AlluxioClientConfig(load_balance_domain="github.com")
        ).resolve_address()
        print(f"The IP address for github.com is: {github_ip}")
    except ValueError as e:
        print(e)
