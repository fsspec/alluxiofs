import logging
import os

import fsspec
from alluxio import AlluxioFileSystem as AlluxioSystem
from alluxio import AlluxioPathStatus
from fsspec import AbstractFileSystem
from fsspec import filesystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class AlluxioFileSystem(AbstractFileSystem):
    protocol = "alluxio"

    def __init__(
        self,
        etcd_host=None,
        worker_hosts=None,
        options=None,
        logger=None,
        concurrency=64,
        http_port="28080",
        preload_path=None,
        target_protocol=None,
        target_options=None,
        fs=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not (fs is None) ^ (target_protocol is None):
            raise ValueError(
                "Please provide one of filesystem instance (fs) or"
                " target_protocol, not both"
            )
        if fs is None and target_protocol is None:
            raise ValueError(
                "Please provide filesystem instance(fs) or target_protocol"
            )
        self.logger = logger or logging.getLogger("AlluxioFileSystem")
        self.alluxio = AlluxioSystem(
            etcd_host, worker_hosts, options, logger, concurrency, http_port
        )
        if preload_path is not None:
            self.alluxio.load(preload_path)
        self.kwargs = target_options or {}
        self.fs = (
            fs
            if fs is not None
            else filesystem(target_protocol, **self.kwargs)
        )

        def _strip_protocol(path):
            return self.fs._strip_protocol(type(self)._strip_protocol(path))

        self._strip_protocol: Callable = _strip_protocol

    def unstrip_protocol(self, path):
        # avoid adding Alluxio protocol to the full ufs url
        return self.fs.unstrip_protocol(path)

    def ls(self, path, detail=True, **kwargs):
        path = self.unstrip_protocol(path)
        paths = self.alluxio.listdir(path)
        if detail:
            return [
                {
                    "name": p.ufs_path,
                    "type": p.type,
                    "size": p.length if p.type == "file" else None,
                }
                for p in paths
            ]
        else:
            return [p.ufs_path for p in paths]

    def info(self, path, **kwargs):
        path = self.unstrip_protocol(path)
        file_status = self.alluxio.get_file_status(path)
        result = {
            "name": file_status.name,
            "path": file_status.path,
            "size": file_status.length,
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
        path = self.unstrip_protocol(path)
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
        path = self.unstrip_protocol(path)
        return self.alluxio.read_range(path, start, end - start)

    def mkdir(self, *args, **kwargs):
        self.fs.mkdir(*args, **kwargs)

    def rmdir(self, *args, **kwargs):
        self.fs.rmdir(*args, **kwargs)

    def copy(self, *args, **kwargs):
        self.fs.copy(*args, **kwargs)

    def mv(self, *args, **kwargs):
        self.fs.mv(*args, **kwargs)

    def __getattr__(self, name):
        """
        Delegate attribute access to the underlying filesystem object.
        """
        return getattr(self.fs, name)


class AlluxioFile(AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError(
                'Remote Alluxio files can only be opened in "rb" mode'
            )
        super().__init__(fs, path, mode, **kwargs)

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        return self.fs.fetch_range(self.path, self.mode, start, end)

    def __getattr__(self, name):
        """
        Delegate attribute access to the underlying filesystem object.
        """
        return getattr(self.fs, name)
