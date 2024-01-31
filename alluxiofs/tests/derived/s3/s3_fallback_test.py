import fsspec.tests.abstract as abstract

from alluxiofs.tests.derived.s3.s3_fallback_fixtures import AlluxioS3Fixtures


class TestAlluxioGet(abstract.AbstractGetTests, AlluxioS3Fixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, AlluxioS3Fixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, AlluxioS3Fixtures):
    pass
