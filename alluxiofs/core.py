import logging
import os

import fsspec
from alluxio import AlluxioFileSystem as AlluxioSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class AlluxioFileSystem(AbstractFileSystem):
    protocol = "s3"

    def __init__(
        self,
        etcd_host=None,
        worker_hosts=None,
        options=None,
        logger=None,
        concurrency=64,
        http_port="28080",
        preload_path=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.logger = logger or logging.getLogger("AlluxioFileSystem")
        self.alluxio = AlluxioSystem(
            etcd_host, worker_hosts, options, logger, concurrency, http_port
        )
        if preload_path is not None:
            self.alluxio.load(preload_path)

    def ls(self, path, detail=True, **kwargs):
        path = self.unstrip_protocol(path)
        paths = self.alluxio.listdir(path)
        if detail:
            return [
                {
                    "name": self._strip_protocol(p["mUfsPath"]),
                    "type": (
                        "directory" if p["mType"] == "directory" else "file"
                    ),
                    "size": p["mLength"] if p["mType"] == "file" else None,
                }
                for p in paths
            ]
        else:
            return [self._strip_protocol(p["mUfsPath"]) for f in files]

    def info(self, path, **kwargs):
        file_status = self.alluxio.get_file_status(path)
        result = {
            "name": file_status.name,
            "path": file_status.path,
            "size": file_status.human_readable_file_size,
            "type": file_status.type,
            "ufs_path": file_status.ufs_path,
            "last_modification_time_ms": file_status.last_modification_time_ms,
        }
        return result

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        return AlluxioFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def fetch_range(self, path, mode, start, end):
        return self.alluxio.read_range(path, start, end - start)


class AlluxioFile(AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError(
                'Remote Alluxio files can only be opened in "rb" mode'
            )
        super().__init__(fs, path, mode, **kwargs)
        self.full_path = self.fs.unstrip_protocol(path)

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        return self.fs.fetch_range(self.full_path, self.mode, start, end)
