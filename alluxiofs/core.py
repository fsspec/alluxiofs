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
        etcd_hosts=None,
        worker_hosts=None,
        options=None,
        logger=None,
        concurrency=64,
        etcd_port=2379,
        worker_http_port=28080,
        preload_path=None,
        target_protocol=None,
        target_options=None,
        fs=None,
        test_options=None,
        **kwargs,
    ):
        """
        Initializes an Alluxio filesystem on top of underlying filesystem
        to leveraging the data caching and management features of Alluxio.

        The Alluxio args:
            etcd_hosts (str, optional): A comma-separated list of ETCD server hosts in the format "host1:port1,host2:port2,...".
                ETCD is used for dynamic discovery of Alluxio workers.
                Either `etcd_hosts` or `worker_hosts` must be specified, not both.
            worker_hosts (str, optional): A comma-separated list of Alluxio worker hosts in the format "host1:port1,host2:port2,...".
                Directly specifies workers without using ETCD.
                Either `etcd_hosts` or `worker_hosts` must be specified, not both.
            options (dict, optional): A dictionary of Alluxio configuration options where keys are property names and values are property values.
                These options configure the Alluxio client behavior.
            logger (logging.Logger, optional): A logger instance for logging messages.
                If not provided, a default logger with the name "AlluxioFileSystem" is used.
            concurrency (int, optional): The maximum number of concurrent operations (e.g., reads, writes) that the file system interface will allow. Defaults to 64.
            etcd_port (int, optional): The port number used by each etcd server.
                Relevant only if `etcd_hosts` is specified.
            worker_http_port (int, optional): The port number used by the HTTP server on each Alluxio worker.
                This is used for accessing Alluxio's HTTP-based APIs.
            preload_path (str, optional): Specifies a path to preload into the Alluxio file system cache at initialization.
                This can be useful for ensuring that certain critical data is immediately available in the cache.
        The underlying filesystem args
            target_protocol (str, optional): Specifies the under storage protocol to create the under storage file system object.
                Common examples include 's3' for Amazon S3, 'hdfs' for Hadoop Distributed File System, and others.
            target_options (dict, optional): Provides a set of configuration options relevant to the `target_protocol`.
                These options might include credentials, endpoint URLs, and other protocol-specific settings required to successfully interact with the under storage system.
            fs (object, optional): Directly supplies an instance of a file system object for accessing the underlying storage of Alluxio
        Other args:
            test_options (dict, optional): A dictionary of options used exclusively for testing purposes.
                These might include mock interfaces or specific configuration overrides for test scenarios.
            **kwargs: other parameters for core session.
        """
        super().__init__(**kwargs)
        self.logger = logger or logging.getLogger("Alluxiofs")
        if fs and target_protocol:
            raise ValueError(
                "Please provide one of filesystem instance (fs) or"
                " target_protocol, not both"
            )
        if fs is None and target_protocol is None:
            self.logger.warning(
                "Neither filesystem instance(fs) nor target_protocol is "
                "provided. Will not fall back to under file systems when "
                "accessed files are not in Alluxiofs"
            )
        self.kwargs = target_options or {}
        self.fs = None
        if fs is not None:
            self.fs = fs
        elif target_protocol is not None:
            self.fs = filesystem(target_protocol, **self.kwargs)

        test_options = test_options or {}
        if test_options.get("skip_alluxio") is True:
            self.alluxio = None
        else:
            self.alluxio = AlluxioClient(
                etcd_hosts=etcd_hosts,
                worker_hosts=worker_hosts,
                options=options,
                logger=logger,
                concurrency=concurrency,
                etcd_port=etcd_port,
                worker_http_port=worker_http_port,
            )
            if preload_path is not None:
                self.alluxio.load(preload_path)

        # Remove "alluxio::" from the given single path or list of path
        def _strip_alluxio_protocol(path):
            def _strip_individual_path(p):
                if p.startswith(self.protocol + "::"):
                    return p[len(self.protocol) + 2 :]
                return p

            if isinstance(path, str):
                return _strip_individual_path(path)
            elif isinstance(path, list):
                return [_strip_individual_path(p) for p in path]
            else:
                raise TypeError("Path must be a string or a list of strings")

        self._strip_alluxio_protocol: Callable = _strip_alluxio_protocol

        def _strip_protocol(path):
            path = self._strip_alluxio_protocol(path)
            if self.fs:
                return self.fs._strip_protocol(
                    type(self)._strip_protocol(path)
                )
            return path

        self._strip_protocol: Callable = _strip_protocol

        self.error_metrics = AlluxioErrorMetrics()

    def unstrip_protocol(self, path):
        path = self._strip_alluxio_protocol(path)
        if self.fs:
            # avoid adding Alluxio protocol to the full ufs url
            return self.fs.unstrip_protocol(path)
        return path

    def get_error_metrics(self):
        return self.error_metrics.get_metrics()

    def fallback_handler(alluxio_impl):
        @wraps(alluxio_impl)
        def fallback_wrapper(self, path, *args, **kwargs):
            path = self._strip_alluxio_protocol(path)
            try:
                if self.alluxio:
                    return alluxio_impl(self, path, *args, **kwargs)
            except Exception as e:
                if not isinstance(e, NotImplementedError):
                    self.error_metrics.record_error(alluxio_impl.__name__, e)
                if self.fs is None:
                    raise e

            fs_method = getattr(self.fs, alluxio_impl.__name__, None)
            if fs_method:
                return fs_method(path, *args, **kwargs)
            raise NotImplementedError(
                f"The method {alluxio_impl.__name__} is not implemented in the underlying filesystem."
            )

        return fallback_wrapper

    @fallback_handler
    def ls(self, path, detail=True, **kwargs):
        path = self.unstrip_protocol(path)
        paths = self.alluxio.listdir(path)
        return [
            self._translate_alluxio_info_to_fsspec_info(p, detail)
            for p in paths
        ]

    @fallback_handler
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

    @fallback_handler
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

    @fallback_handler
    def cat_file(self, path, start=None, end=None, **kwargs):
        if end is None:
            length = -1
        else:
            length = end - start
        path = self.unstrip_protocol(path)
        return self.alluxio.read_range(path, start, length)

    @fallback_handler
    def ukey(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def mkdir(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def makedirs(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def rm(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def rmdir(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def _rm(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def pipe_file(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def rm_file(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def touch(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def created(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def modified(self, path, *args, **kwargs):
        raise NotImplementedError

    def mv(self, path1, path2, *args, **kwargs):
        if self.fs:
            return self.fs.mv(
                self._strip_alluxio_protocol(path1),
                self._strip_alluxio_protocol(path2),
                *args,
                **kwargs,
            )
        else:
            raise NotImplementedError

    def copy(self, path1, path2, *args, **kwargs):
        if self.fs:
            return self.fs.copy(
                self._strip_alluxio_protocol(path1),
                self._strip_alluxio_protocol(path2),
                *args,
                **kwargs,
            )
        else:
            raise NotImplementedError

    def cp_file(self, path1, path2, *args, **kwargs):
        if self.fs:
            return self.fs.cp_file(
                self._strip_alluxio_protocol(path1),
                self._strip_alluxio_protocol(path2),
                *args,
                **kwargs,
            )
        else:
            raise NotImplementedError

    def put_file(self, lpath, rpath, *args, **kwargs):
        if self.fs:
            return self.fs.put_file(
                self._strip_alluxio_protocol(lpath),
                self._strip_alluxio_protocol(rpath),
                *args,
                **kwargs,
            )
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
