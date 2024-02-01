import fsspec.tests.abstract as abstract

from alluxiofs.tests.derived.local.local_fallback_fixtures import (
    LocalFallbackFixtures,
)


class TestAlluxioGet(abstract.AbstractGetTests, LocalFallbackFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, LocalFallbackFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, LocalFallbackFixtures):
    pass
