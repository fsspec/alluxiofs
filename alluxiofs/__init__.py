import fsspec

from alluxiofs.client import AlluxioClient
from alluxiofs.core import AlluxioFileSystem

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)

__all__ = ["AlluxioFileSystem", "AlluxioClient"]
