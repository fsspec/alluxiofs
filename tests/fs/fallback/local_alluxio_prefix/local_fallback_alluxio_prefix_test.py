import fsspec.tests.abstract as abstract

from tests.fs.fallback.local_alluxio_prefix.local_fallback_alluxio_prefix_fixtures import (
    LocalFallbackAlluxioPrefixFixtures,
)


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
