import pytest
from fsspec.implementations.local import make_path_posix
from fsspec.tests.abstract import AbstractFixtures

from alluxiofs import AlluxioFileSystem


class LocalFallbackAlluxioPrefixFixtures(AbstractFixtures):
    protocol = "alluxiofs://"

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
        return self.protocol + str(tmpdir)

    @pytest.fixture
    def fs_join(self):
        def join_function(*args):
            processed_args = [arg.replace(self.protocol, "") for arg in args]
            return "/".join(processed_args)

        return join_function

    @pytest.fixture
    def fs_sanitize_path(self):
        return make_path_posix
