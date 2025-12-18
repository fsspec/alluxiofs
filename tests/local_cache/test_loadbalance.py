import pytest
from unittest.mock import patch
import socket

from alluxiofs.client.loadbalance import (
    WorkerNetAddress,
    WorkerListLoadBalancer,
    DNSLoadBalancer,
)
from alluxiofs.client.config import AlluxioClientConfig
from alluxiofs.client.const import ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE

class TestWorkerNetAddress:
    def test_init_defaults(self):
        addr = WorkerNetAddress()
        assert addr.host == "localhost"
        assert addr.http_server_port == ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE

    def test_from_host_and_port(self):
        addr = WorkerNetAddress.from_host_and_port("worker1", 12345)
        assert addr.host == "worker1"
        assert addr.http_server_port == 12345

    def test_from_host(self):
        addr = WorkerNetAddress.from_host("worker1")
        assert addr.host == "worker1"
        assert addr.http_server_port == ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE

    def test_from_dict(self):
        d = {"host": "worker1", "http_server_port": 12345}
        addr = WorkerNetAddress.from_dict(d)
        assert addr.host == "worker1"
        assert addr.http_server_port == 12345

    def test_to_dict(self):
        addr = WorkerNetAddress("worker1", 12345)
        d = WorkerNetAddress.to_dict(addr)
        assert d == {"host": "worker1", "http_server_port": 12345}

class TestWorkerListLoadBalancer:
    def test_init_hosts_only(self):
        config = AlluxioClientConfig(worker_hosts="worker1,worker2", worker_http_port=29999)
        lb = WorkerListLoadBalancer(config)
        assert lb.workers == ["worker1", "worker2"]
        assert lb.ports == [29999, 29999]

    def test_init_hosts_with_ports(self):
        config = AlluxioClientConfig(worker_hosts="worker1:10001,worker2:10002")
        lb = WorkerListLoadBalancer(config)
        assert lb.workers == ["worker1", "worker2"]
        assert lb.ports == [10001, 10002]

    def test_init_mixed(self):
        config = AlluxioClientConfig(worker_hosts="worker1,worker2:10002", worker_http_port=29999)
        lb = WorkerListLoadBalancer(config)
        assert lb.workers == ["worker1", "worker2"]
        assert lb.ports == [29999, 10002]

    def test_get_worker_random(self):
        config = AlluxioClientConfig(worker_hosts="worker1", worker_http_port=29999)
        lb = WorkerListLoadBalancer(config)
        worker = lb.get_worker()
        assert worker.host == "worker1"
        assert worker.http_server_port == 29999

    def test_get_worker_hash(self):
        config = AlluxioClientConfig(worker_hosts="worker1,worker2", worker_http_port=29999)
        lb = WorkerListLoadBalancer(config)
        path = "test_path"
        worker1 = lb.get_worker(path)
        worker2 = lb.get_worker(path)
        assert worker1 == worker2
        assert worker1.host in ["worker1", "worker2"]

    def test_get_multiple_worker(self):
        config = AlluxioClientConfig(worker_hosts="worker1,worker2,worker3", worker_http_port=29999)
        lb = WorkerListLoadBalancer(config)
        workers = lb.get_multiple_worker("test_path", 2)
        assert len(workers) == 2
        assert workers[0] != workers[1]
        assert workers[0].host in ["worker1", "worker2", "worker3"]
        assert workers[1].host in ["worker1", "worker2", "worker3"]

class TestDNSLoadBalancer:
    @patch("socket.getaddrinfo")
    def test_resolve_address_success(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.1', 80))
        ]
        config = AlluxioClientConfig(load_balance_domain="example.com")
        lb = DNSLoadBalancer(config)
        ip = lb.resolve_address()
        assert ip == "192.168.1.1"
        mock_getaddrinfo.assert_called_with("example.com", None, family=socket.AF_INET)

    @patch("socket.getaddrinfo")
    def test_resolve_address_no_address(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = []
        config = AlluxioClientConfig(load_balance_domain="example.com")
        lb = DNSLoadBalancer(config)
        with pytest.raises(ValueError, match="No address found for domain"):
            lb.resolve_address()

    @patch("socket.getaddrinfo")
    def test_resolve_address_gaierror(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = socket.gaierror
        config = AlluxioClientConfig(load_balance_domain="example.com")
        lb = DNSLoadBalancer(config)
        with pytest.raises(ValueError, match="Could not resolve domain"):
            lb.resolve_address()

    @patch("alluxiofs.client.loadbalance.DNSLoadBalancer.resolve_address")
    def test_get_worker(self, mock_resolve):
        mock_resolve.return_value = "192.168.1.1"
        config = AlluxioClientConfig(load_balance_domain="example.com", worker_http_port=29999)
        lb = DNSLoadBalancer(config)
        worker = lb.get_worker("path")
        assert worker.host == "192.168.1.1"
        assert worker.http_server_port == 29999

    @patch("alluxiofs.client.loadbalance.DNSLoadBalancer.resolve_address")
    def test_get_worker_no_args(self, mock_resolve):
        mock_resolve.return_value = "192.168.1.1"
        config = AlluxioClientConfig(load_balance_domain="example.com", worker_http_port=29999)
        lb = DNSLoadBalancer(config)
        worker = lb.get_worker()
        assert worker.host == "192.168.1.1"
        assert worker.http_server_port == 29999

    @patch("alluxiofs.client.loadbalance.DNSLoadBalancer.resolve_address")
    def test_get_multiple_worker(self, mock_resolve):
        mock_resolve.side_effect = ["192.168.1.1", "192.168.1.2"]
        config = AlluxioClientConfig(load_balance_domain="example.com", worker_http_port=29999)
        lb = DNSLoadBalancer(config)
        workers = lb.get_multiple_worker("path", 2)
        assert len(workers) == 2
        assert workers[0].host == "192.168.1.1"
        assert workers[1].host == "192.168.1.2"
