# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
import inspect
import logging
import time
from functools import wraps
from typing import Callable

from fsspec import AbstractFileSystem
from fsspec import filesystem
from fsspec.spec import AbstractBufferedFile

from alluxiofs.client import AlluxioClient
from alluxiofs.client.utils import set_log_level

logger = logging.getLogger(__name__)


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
    protocol = "alluxiofs"
    protocol_prefix = f"{protocol}://"

    def __init__(
        self,
        target_protocol=None,
        target_options=None,
        fs=None,
        alluxio_client=None,
        test_options=None,
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
        if fs and target_protocol:
            raise ValueError(
                "Please provide one of filesystem instance (fs) or"
                " target_protocol, not both"
            )
        if fs is None and target_protocol is None:
            logger.warning(
                "Neither filesystem instance(fs) nor target_protocol is "
                "provided. Will not fall back to under file systems when "
                "accessed files are not in Alluxiofs"
            )

        self.logger = kwargs.get("logger", logging.getLogger("Alluxiofs"))
        self.kwargs = target_options or {}
        self.fs = None
        self.target_protocol = None
        if fs is not None:
            self.fs = fs
            if isinstance(self.fs.protocol, tuple):
                # e.g. file or local both representing local filesystem
                self.target_protocol = self.fs.protocol[0]
            elif isinstance(self.fs.protocol, str):
                self.target_protocol = self.fs.protocol
            else:
                raise TypeError(
                    "target filesystem protocol should be str or tuple but found "
                    + self.fs.protocol
                )
        elif target_protocol is not None:
            self.fs = filesystem(target_protocol, **self.kwargs)
            self.target_protocol = target_protocol

        skip_alluxio = kwargs.get("skip_alluxio", False)
        if skip_alluxio:
            self.alluxio = None
        elif alluxio_client:
            self.alluxio = alluxio_client
        else:
            self.alluxio = AlluxioClient(**kwargs)

        # TODO: check test options
        test_options = test_options or {}
        set_log_level(logger, test_options)

        def _strip_alluxiofs_protocol(path):
            def _strip_individual_path(p):
                if p.startswith(self.protocol_prefix):
                    if self.target_protocol is None:
                        raise TypeError(
                            f"Filesystem instance(fs) or target_protocol should be provided to use {self.protocol_prefix} schema"
                        )
                    return p[len(self.protocol_prefix) :]
                return p

            if isinstance(path, str):
                return _strip_individual_path(path)
            elif isinstance(path, list):
                return [_strip_individual_path(p) for p in path]
            else:
                raise TypeError("Path must be a string or a list of strings")

        self._strip_alluxiofs_protocol: Callable = _strip_alluxiofs_protocol

        def _strip_protocol(path):
            path = self._strip_alluxiofs_protocol(path)
            if self.fs:
                return self.fs._strip_protocol(
                    type(self)._strip_protocol(path)
                )
            return path

        self._strip_protocol: Callable = _strip_protocol

        self.error_metrics = AlluxioErrorMetrics()

    def unstrip_protocol(self, path):
        if self.fs:
            # avoid adding Alluxiofs protocol to the full ufs url
            return self.fs.unstrip_protocol(path)
        return path

    def get_error_metrics(self):
        return self.error_metrics.get_metrics()

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

    def fallback_handler(alluxio_impl):
        @wraps(alluxio_impl)
        def fallback_wrapper(self, *args, **kwargs):
            signature = inspect.signature(alluxio_impl)

            # process path related arguments to remove alluxiofs protocol
            # since both alluxio and ufs cannot process alluxiofs protocol
            # Require s3://bucket/path or /bucket/path
            bound_args = signature.bind(self, *args, **kwargs)
            bound_args.apply_defaults()
            # fsspec path parameters has different names and sequences
            # use this approach to try to process all path related parameters
            for param in ["path", "path1", "path2", "lpath", "rpath"]:
                if param in bound_args.arguments:
                    bound_args.arguments[
                        param
                    ] = self._strip_alluxiofs_protocol(
                        bound_args.arguments[param]
                    )

            try:
                if self.alluxio:
                    start_time = time.time()
                    res = alluxio_impl(*bound_args.args, **bound_args.kwargs)
                    logger.debug(
                        f"Exit(Ok): alluxio op({alluxio_impl.__name__}) args({bound_args.args}) kwargs({bound_args.kwargs}) time({(time.time() - start_time):.2f}s)"
                    )
                    return res
            except Exception as e:
                if not isinstance(e, NotImplementedError):
                    logger.debug(
                        f"Exit(Error): alluxio op({alluxio_impl.__name__}) args({bound_args.args}) kwargs({bound_args.kwargs}) {e}"
                    )
                    self.error_metrics.record_error(alluxio_impl.__name__, e)
                if self.fs is None:
                    raise e

            fs_method = getattr(self.fs, alluxio_impl.__name__, None)
            if fs_method:
                res = fs_method(*bound_args.args[1:], **bound_args.kwargs)
                logger.debug(
                    f"Exit(Ok): ufs({self.target_protocol}) op({alluxio_impl.__name__}) args({bound_args.args}) kwargs({bound_args.kwargs})"
                )
                return res
            raise NotImplementedError(
                f"The method {alluxio_impl.__name__} is not implemented in the underlying filesystem {self.target_protocol}"
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

    @fallback_handler
    def isdir(self, path, **kwargs):
        return self.info(path)["type"] == "directory"

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

    @fallback_handler
    def head(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def tail(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def expand_path(self, path, *args, **kwargs):
        raise NotImplementedError

    # Comment it out as s3fs will return folder as well.
    # @fallback_handler
    # def find(self, path, *args, **kwargs):
    #     raise NotImplementedError

    @fallback_handler
    def mv(self, path1, path2, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def copy(self, path1, path2, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def cp_file(self, path1, path2, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def rename(self, path1, path2, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def move(self, path1, path2, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def put_file(self, lpath, rpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def put(self, lpath, rpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def upload(self, lpath, rpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def download(self, lpath, rpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def get(self, rpath, lpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def get_file(self, rpath, lpath, *args, **kwargs):
        raise NotImplementedError

    def read_block(self, *args, **kwargs):
        if self.fs:
            return self.fs.read_block(*args, **kwargs)
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
