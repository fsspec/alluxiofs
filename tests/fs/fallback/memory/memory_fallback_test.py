import fsspec.tests.abstract as abstract

from tests.fs.fallback.memory.memory_fallback_fixtures import (
    MemoryFallbackFixtures,
)


class TestAlluxioGet(abstract.AbstractGetTests, MemoryFallbackFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, MemoryFallbackFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, MemoryFallbackFixtures):
    pass
