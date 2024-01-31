import fsspec.tests.abstract as abstract

from alluxiofs.tests.derived.memory.memory_fallback_fixtures import (
    AlluxioMemoryFixtures,
)


class TestAlluxioGet(abstract.AbstractGetTests, AlluxioMemoryFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, AlluxioMemoryFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, AlluxioMemoryFixtures):
    pass
