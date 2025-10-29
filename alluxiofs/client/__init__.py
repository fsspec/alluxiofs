from fsspec.caching import register_cache

from alluxiofs.client.cache import McapMemoryCache
from alluxiofs.client.core import AlluxioAsyncFileSystem
from alluxiofs.client.core import AlluxioClient
from alluxiofs.client.core import AlluxioPathStatus

register_cache(McapMemoryCache)

__all__ = [
    "AlluxioClient",
    "AlluxioAsyncFileSystem",
    "AlluxioPathStatus",
]
