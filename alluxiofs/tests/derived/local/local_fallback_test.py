import fsspec.tests.abstract as abstract

from alluxiofs.tests.derived.local.local_fallback_fixtures import (
    AlluxioLocalFixtures,
)


class TestAlluxioGet(abstract.AbstractGetTests, AlluxioLocalFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, AlluxioLocalFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, AlluxioLocalFixtures):
    pass
