import pytest
from fsspec import filesystem
from fsspec.tests.abstract import AbstractFixtures

from alluxiofs import AlluxioFileSystem


class MemoryFallbackFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        m = filesystem("memory")
        m.store.clear()
        m.pseudo_dirs.clear()
        m.pseudo_dirs.append("")
        try:
            yield AlluxioFileSystem(
                etcd_hosts="localhost",
                fs=m,
                test_options={"skip_alluxio": True},
            )
        finally:
            m.store.clear()
            m.pseudo_dirs.clear()
            m.pseudo_dirs.append("")

    @pytest.fixture
    def fs_join(self):
        return lambda *args: "/".join(args)

    @pytest.fixture
    def fs_path(self):
        return ""
