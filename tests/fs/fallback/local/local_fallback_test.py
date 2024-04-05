import fsspec.tests.abstract as abstract

from tests.fs.fallback.local.local_fallback_fixtures import (
    LocalFallbackAlluxioPrefixFixtures,
)
from tests.fs.fallback.local.local_fallback_fixtures import (
    LocalFallbackFixtures,
)


class TestAlluxioGet(abstract.AbstractGetTests, LocalFallbackFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, LocalFallbackFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, LocalFallbackFixtures):
    pass


class TestAlluxioGet(
    abstract.AbstractGetTests, LocalFallbackAlluxioPrefixFixtures
):
    pass


class TestAlluxioCopy(
    abstract.AbstractCopyTests, LocalFallbackAlluxioPrefixFixtures
):
    pass


class TestAlluxioPut(
    abstract.AbstractPutTests, LocalFallbackAlluxioPrefixFixtures
):
    pass
