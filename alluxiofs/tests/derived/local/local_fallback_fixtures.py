import pytest
from fsspec.implementations.local import make_path_posix
from fsspec.tests.abstract import AbstractFixtures

from alluxiofs import AlluxioFileSystem


class LocalFallbackFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        return AlluxioFileSystem(
            etcd_hosts="localhost",
            target_protocol="file",
            target_options={"auto_mkdir": True},
            test_options={"skip_alluxio": True},
        )

    @pytest.fixture
    def fs_path(self, tmpdir):
        return str(tmpdir)

    @pytest.fixture
    def fs_sanitize_path(self):
        return make_path_posix
