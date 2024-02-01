import fsspec.tests.abstract as abstract

from alluxiofs.tests.derived.s3.s3_fallback_fixtures import S3FallbackFixtures


class TestAlluxioGet(abstract.AbstractGetTests, S3FallbackFixtures):
    pass


class TestAlluxioCopy(abstract.AbstractCopyTests, S3FallbackFixtures):
    pass


class TestAlluxioPut(abstract.AbstractPutTests, S3FallbackFixtures):
    pass
