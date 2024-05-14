import pytest
from fsspec.implementations.local import make_path_posix
from fsspec.tests.abstract import AbstractFixtures

from alluxiofs import AlluxioFileSystem
from tests.utils import remove_alluxiofs_protocol
from tests.utils import use_alluxiofs_protocol


class LocalFallbackAlluxioPrefixFixtures(AbstractFixtures):
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
        return use_alluxiofs_protocol(str(tmpdir))

    @pytest.fixture
    def fs_join(self):
        def join_function(*args):
            processed_args = [remove_alluxiofs_protocol(arg) for arg in args]
            joined_path = "/".join(processed_args)
            return use_alluxiofs_protocol(joined_path)

        return join_function

    def make_alluxiofs_path_posix(self, path):
        path_without_protocol = remove_alluxiofs_protocol(path)
        posix_path = make_path_posix(path_without_protocol)
        return AlluxioFileSystem.protocol + posix_path

    @pytest.fixture
    def fs_sanitize_path(self):
        return self.make_alluxiofs_path_posix
