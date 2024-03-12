import logging
from functools import wraps
from typing import Callable

from fsspec import AbstractFileSystem
from fsspec import filesystem
from fsspec.spec import AbstractBufferedFile

from alluxiofs.client import AlluxioClient

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
        target_protocol=None,
        target_options=None,
        fs=None,
        alluxio_client=None,
        **kwargs,
    ):
        """
        Initializes an Alluxio filesystem on top of underlying filesystem
        to leveraging the data caching and management features of Alluxio.

        The underlying filesystem args
            target_protocol (str, optional): Specifies the under storage protocol to create the under storage file system object.
                Common examples include 's3' for Amazon S3, 'hdfs' for Hadoop Distributed File System, and others.
            target_options (dict, optional): Provides a set of configuration options relevant to the `target_protocol`.
                These options might include credentials, endpoint URLs, and other protocol-specific settings required to successfully interact with the under storage system.
            fs (object, optional): Directly supplies an instance of a file system object for accessing the underlying storage of Alluxio
        The Alluxio client args:
            alluxio_client (AlluxioClient, Optional): the alluxio client to connects to Alluxio servers.
                If not provided, please add Alluxio client arguments to init a new Alluxio Client.
        **kwargs: other parameters for initializing Alluxio client or fsspec.
        """
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
        self.logger = kwargs.get("logger", logging.getLogger("Alluxiofs"))
        self.kwargs = target_options or {}
        self.fs = None
        if fs is not None:
            self.fs = fs
        elif target_protocol is not None:
            self.fs = filesystem(target_protocol, **self.kwargs)

        skip_alluxio = kwargs.get("skip_alluxio", False)
        if skip_alluxio:
            self.alluxio = None
        elif alluxio_client:
            self.alluxio = alluxio_client
        else:
            self.alluxio = AlluxioClient(**kwargs)

        def _strip_protocol(path):
            if self.fs:
                return self.fs._strip_protocol(
                    type(self)._strip_protocol(path)
                )
            return path

        self._strip_protocol: Callable = _strip_protocol

        self.error_metrics = AlluxioErrorMetrics()

    def unstrip_protocol(self, path):
        if self.fs:
            # avoid adding Alluxio protocol to the full ufs url
            return self.fs.unstrip_protocol(path)
        return path

    def get_error_metrics(self):
        return self.error_metrics.get_metrics()

    def alluxio_with_fallback_handler(alluxio_impl):
        @wraps(alluxio_impl)
        def fallback_wrapper(self, *args, **kwargs):
            if self.alluxio is None:
                if self.fs:
                    fs_method = getattr(self.fs, alluxio_impl.__name__, None)
                    if fs_method:
                        # TODO(lu) deal with the parameter sequence mismatch issue
                        return fs_method(*args, **kwargs)
                raise RuntimeError("Alluxio system is not initialized.")

            try:
                return alluxio_impl(self, *args, **kwargs)
            except Exception as e:
                self.error_metrics.record_error(alluxio_impl.__name__, e)
                if self.fs:
                    fs_method = getattr(self.fs, alluxio_impl.__name__, None)
                    if fs_method:
                        return fs_method(*args, **kwargs)
                else:
                    raise

        return fallback_wrapper

    @alluxio_with_fallback_handler
    def ls(self, path, detail=True, **kwargs):
        path = self.unstrip_protocol(path)
        paths = self.alluxio.listdir(path)
        return [
            self._translate_alluxio_info_to_fsspec_info(p, detail)
            for p in paths
        ]

    @alluxio_with_fallback_handler
    def info(self, path, **kwargs):
        path = self.unstrip_protocol(path)
        file_status = self.alluxio.get_file_status(path)
        return self._translate_alluxio_info_to_fsspec_info(file_status, True)

    def _translate_alluxio_info_to_fsspec_info(self, file_status, detail):
        if detail:
            return {
                "name": self._strip_protocol(file_status.ufs_path),
                "type": file_status.type,
                "size": file_status.length
                if file_status.type == "file"
                else None,
                "last_modification_time_ms": getattr(
                    file_status, "last_modification_time_ms", None
                ),
            }
        else:
            return self._strip_protocol(file_status.ufs_path)

    @alluxio_with_fallback_handler
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

    @alluxio_with_fallback_handler
    def cat_file(self, path, start=None, end=None, **kwargs):
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


class AlluxioFile(AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError(
                'Remote Alluxio files can only be opened in "rb" mode'
            )
        super().__init__(fs, path, mode, **kwargs)

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        return self.fs.cat_file(path=self.path, start=start, end=end)

    def _upload_chunk(self, final=False):
        pass

    def _initiate_upload(self):
        pass
