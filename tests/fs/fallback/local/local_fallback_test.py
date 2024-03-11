import fsspec.tests.abstract as abstract

from tests.fs.fallback.local.local_fallback_fixtures import (
    LocalFallbackFixtures,
)


class TestAlluxioGet(abstract.AbstractGetTests, LocalFallbackFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, LocalFallbackFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, LocalFallbackFixtures):
    pass
