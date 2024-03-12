import fsspec.tests.abstract as abstract
import pytest

from tests.fs.fallback.s3.s3_fallback_fixtures import S3FallbackFixtures


@pytest.mark.skip(reason="S3 credentials not set.")
class TestAlluxioGet(abstract.AbstractGetTests, S3FallbackFixtures):
    pass


@pytest.mark.skip(reason="S3 credentials not set.")
class TestAlluxioCopy(abstract.AbstractCopyTests, S3FallbackFixtures):
    pass


@pytest.mark.skip(reason="S3 credentials not set.")
class TestAlluxioPut(abstract.AbstractPutTests, S3FallbackFixtures):
    pass
