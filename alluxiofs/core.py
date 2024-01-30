import logging
from typing import Callable

from alluxio import AlluxioFileSystem as AlluxioSystem
from fsspec import AbstractFileSystem
from fsspec import filesystem
from fsspec.spec import AbstractBufferedFile

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class AlluxioErrorMetrics:
    def __init__(self):
        self.error_counts = {}
        self.last_errors = {}

    def record_error(self, method, error):
        key = f"{method}_{type(error).__name__}"
        self.error_counts.setdefault(key, 0)
        self.error_counts[key] += 1
        self.last_errors[key] = str(error)

    def get_metrics(self):
        return {
            "error_counts": self.error_counts,
            "last_errors": self.last_errors,
        }


class AlluxioFileSystem(AbstractFileSystem):
    protocol = "alluxio"

    def __init__(
        self,
        etcd_hosts=None,
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
            etcd_hosts, worker_hosts, options, logger, concurrency, http_port
        )
        if preload_path is not None:
            self.alluxio.load(preload_path)
        self.kwargs = target_options or {}

        self.fs = None
        if fs is not None:
            self.fs = fs
        elif target_protocol is not None:
            self.fs = filesystem(target_protocol, **self.kwargs)

        def _strip_protocol(path):
            return self.fs._strip_protocol(type(self)._strip_protocol(path))

        self._strip_protocol: Callable = _strip_protocol

        self.error_metrics = AlluxioErrorMetrics()

    def unstrip_protocol(self, path):
        # avoid adding Alluxio protocol to the full ufs url
        return self.fs.unstrip_protocol(path)

    def get_error_metrics(self):
        return self.error_metrics.get_metrics()

    def ls(self, path, detail=True, **kwargs):
        try:
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
        except Exception as e:
            self.error_metrics.record_error("ls", e)
            if self.fs:
                return self.fs.ls(path, detail=detail, **kwargs)
            else:
                raise e

    def info(self, path, **kwargs):
        try:
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
        except Exception as e:
            self.error_metrics.record_error("info", e)
            if self.fs:
                return self.fs.info(path, **kwargs)
            else:
                raise e

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        try:
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
        except Exception as e:
            self.error_metrics.record_error("open", e)
            if self.fs:
                return self.fs._open(
                    path,
                    mode=mode,
                    block_size=block_size,
                    autocommit=autocommit,
                    cache_options=cache_options,
                    **kwargs,
                )
            else:
                raise e

    def fetch_range(self, path, start, end):
        if end is None:
            length = -1
        else:
            length = end - start
        path = self.unstrip_protocol(path)
        return self.alluxio.read_range(path, start, length)

    def ukey(self, *args, **kwargs):
        if self.fs:
            self.fs.ukey(*args, **kwargs)
        else:
            raise NotImplementedError

    def mkdir(self, *args, **kwargs):
        if self.fs:
            self.fs.mkdir(*args, **kwargs)
        else:
            raise NotImplementedError

    def makedirs(self, *args, **kwargs):
        if self.fs:
            self.fs.makedirs(*args, **kwargs)
        else:
            raise NotImplementedError

    def rm(self, *args, **kwargs):
        if self.fs:
            self.fs.rm(*args, **kwargs)
        else:
            raise NotImplementedError

    def rmdir(self, *args, **kwargs):
        if self.fs:
            self.fs.rmdir(*args, **kwargs)
        else:
            raise NotImplementedError

    def _rm(self, *args, **kwargs):
        if self.fs:
            self.fs._rm(*args, **kwargs)
        else:
            raise NotImplementedError

    def copy(self, *args, **kwargs):
        if self.fs:
            self.fs.copy(*args, **kwargs)
        else:
            raise NotImplementedError

    def cp_file(self, *args, **kwargs):
        if self.fs:
            self.fs.cp_file(*args, **kwargs)
        else:
            raise NotImplementedError

    def put_file(self, *args, **kwargs):
        if self.fs:
            self.fs.put_file(*args, **kwargs)
        else:
            raise NotImplementedError

    def mv_file(self, *args, **kwargs):
        if self.fs:
            self.fs.mv_file(*args, **kwargs)
        else:
            raise NotImplementedError

    def pipe_file(self, *args, **kwargs):
        if self.fs:
            self.fs.pipe_file(*args, **kwargs)
        else:
            raise NotImplementedError

    def link(self, *args, **kwargs):
        if self.fs:
            self.fs.link(*args, **kwargs)
        else:
            raise NotImplementedError

    def symlink(self, *args, **kwargs):
        if self.fs:
            self.fs.symlink(*args, **kwargs)
        else:
            raise NotImplementedError

    def islink(self, *args, **kwargs) -> bool:
        if self.fs:
            return self.fs.islink(*args, **kwargs)
        else:
            raise NotImplementedError

    def rm_file(self, *args, **kwargs):
        if self.fs:
            self.fs.rm_file(*args, **kwargs)
        else:
            raise NotImplementedError

    def rm(self, *args, **kwargs):
        if self.fs:
            self.fs.rm(*args, **kwargs)
        else:
            raise NotImplementedError

    def touch(self, *args, **kwargs):
        if self.fs:
            self.fs.touch(*args, **kwargs)
        else:
            raise NotImplementedError

    def created(self, *args, **kwargs):
        if self.fs:
            return self.fs.created(*args, **kwargs)
        else:
            raise NotImplementedError

    def modified(self, *args, **kwargs):
        if self.fs:
            return self.fs.modified(*args, **kwargs)
        else:
            raise NotImplementedError

    def mv(self, *args, **kwargs):
        if self.fs:
            self.fs.mv(*args, **kwargs)
        else:
            raise NotImplementedError

    # The following methods may help with performance depending on fsspec implementations
    # def get(self, *args, **kwargs):
    #     if self.fs:
    #         return self.fs.get(*args, **kwargs)
    #     else:
    #         raise NotImplementedError
    #
    # def get_file(self, *args, **kwargs):
    #     if self.fs:
    #         return self.fs.get_file(*args, **kwargs)
    #     else:
    #         raise NotImplementedError
    #
    # def cat_file(self, *args, **kwargs):
    #     if self.fs:
    #         return self.fs.cat_file(*args, **kwargs)
    #     else:
    #         raise NotImplementedError


class AlluxioFile(AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError(
                'Remote Alluxio files can only be opened in "rb" mode'
            )
        super().__init__(fs, path, mode, **kwargs)

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        try:
            return self.fs.fetch_range(self.path, start, end)
        except Exception as e:
            self.fs.error_metrics.record_error("_fetch_range", e)
            if self.fs.fs:
                # TODO(lu) better fallback method?
                return self.fs.fs.cat_file(self.path, start=start, end=end)
            else:
                raise e

    def _upload_chunk(self, final=False):
        pass

    def _initiate_upload(self):
        pass
