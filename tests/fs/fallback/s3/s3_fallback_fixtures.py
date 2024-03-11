import pytest
from fsspec.tests.abstract import AbstractFixtures

from alluxiofs import AlluxioFileSystem


class S3FallbackFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        return AlluxioFileSystem(
            etcd_hosts="localhost",
            target_protocol="s3",
            test_options={"skip_alluxio": True},
        )

    @pytest.fixture
    def fs_path(self):
        return "ai-ref-arch"

    @pytest.fixture
    def supports_empty_directories(self):
        return False
