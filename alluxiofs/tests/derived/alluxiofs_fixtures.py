import pytest
from fsspec.tests.abstract import AbstractFixtures

from alluxiofs import AlluxioFileSystem


class AlluxiofsFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        return AlluxioFileSystem(etcd_hosts="localhost", target_protocol="s3")

    @pytest.fixture
    def fs_path(self):
        return "lu-ai-test"

    @pytest.fixture
    def supports_empty_directories(self):
        return False
