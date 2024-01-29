import fsspec.tests.abstract as abstract

from alluxiofs.tests.derived.alluxiofs_fixtures import AlluxiofsFixtures


class TestAlluxioGet(abstract.AbstractGetTests, AlluxiofsFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, AlluxiofsFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, AlluxiofsFixtures):
    pass
