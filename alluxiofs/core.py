# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
import inspect
import io
import logging
import os
import time
from dataclasses import dataclass
from functools import wraps
from typing import Callable

from cachetools import LRUCache
from fsspec import AbstractFileSystem
from fsspec import filesystem
from fsspec.spec import AbstractBufferedFile

from alluxiofs.client import AlluxioClient
from alluxiofs.client.utils import set_log_level


def setup_logger(
    file_path=None, level=os.getenv("PYTHON_LOGLEVEL", logging.INFO)
):
    # log dir
    file_name = "user.log"
    if file_path is None:
        project_dir = os.getcwd()
        logs_dir = os.path.join(project_dir, "logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir, exist_ok=True)
        log_file = os.path.join(logs_dir, file_name)
    else:
        log_file = file_path + "/" + file_name
    # set handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    # init logger
    logger = logging.getLogger(__name__)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(level)
    return logger


@dataclass
class RMOption:
    # delete files and subdirectories recursively
    recursive: bool = False
    recursiveAlias: bool = False
    # Marks a directory to either trigger a metadata sync or skip the metadata sync on next access.
    sync_parent_next_time: bool = False
    # remove directories without checking UFS contents are in sync
    remove_unchecked_option: bool = False
    # remove data and metadata from Alluxio space only
    remove_alluxio_only: bool = True
    # remove mount points in the directory
    delete_mount_point: bool = False


@dataclass(frozen=True)
class CPOption:
    # delete files and subdirectories recursively
    recursive: bool = True
    recursiveAlias: bool = False
    # forces to overwrite the destination file if it exists
    forced: bool = False
    # Number of threads used to copy files in parallel, default value is CPU cores * 2
    thread: int = None
    # Read buffer size in bytes, default is 8MB when copying from local, and 64MB when copying to local
    buffer_size: str = None
    # Preserve file permission attributes when copying files. All ownership, permissions and ACLs will be preserved
    preserve: bool = True


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
        preload_path=None,
        target_protocol=None,
        target_options=None,
        fs=None,
        logger=None,
        **kwargs,
    ):
        """
        Initializes an Alluxio filesystem on top of underlying filesystem
        to leveraging the data caching and management features of Alluxio.

        The Alluxio args:
            worker_hosts (str, optional): A comma-separated list of Alluxio worker hosts in the format "host1:port1,host2:port2,...".
                Directly specifies workers without using service discovery.
            options (dict, optional): A dictionary of Alluxio configuration options where keys are property names and values are property values.
                These options configure the Alluxio client behavior.
            concurrency (int, optional): The maximum number of concurrent operations (e.g., reads, writes) that the file system interface will allow. Defaults to 64.
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
        self.logger = logger
        if self.logger is None:
            self.logger = setup_logger()
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
        self.target_options = target_options or {}
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
            self.fs = filesystem(target_protocol, **self.target_options)
            self.target_protocol = target_protocol
        test_options = kwargs.get("test_options", {})
        set_log_level(self.logger, test_options)
        if test_options.get("skip_alluxio") is True:
            self.alluxio = None
        else:
            self.alluxio = AlluxioClient(
                logger=self.logger,
                **kwargs,
            )
            if preload_path is not None:
                self.alluxio.load(preload_path)

        self.file_info_cache = LRUCache(maxsize=1000)

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

    # TODO(littleEast7): there may be a bug.
    def unstrip_protocol(self, path):
        if self.fs:
            # avoid adding Alluxiofs protocol to the full ufs url
            # if path.startswith('/'):
            #     return self.fs.unstrip_protocol(path[1:])
            # else:
            path = self.fs.unstrip_protocol(path)
            if (
                not (path.startswith("file") or path.startswith("alluxiofs"))
                and "///" in path
            ):
                path = path.replace("///", "//", 1)
        return path

    def get_error_metrics(self):
        return self.error_metrics.get_metrics()

    def _translate_alluxio_info_to_fsspec_info(self, file_status, detail):
        if detail:
            res = file_status.__dict__
            res["size"] = res.pop("length")
            return res
        else:
            return file_status.path

    def fallback_handler(alluxio_impl):
        @wraps(alluxio_impl)
        def fallback_wrapper(self, *args, **kwargs):
            signature = inspect.signature(alluxio_impl)

            # process path related arguments to remove alluxiofs protocol
            # since both alluxio and ufs cannot process alluxiofs protocol
            # Require s3://bucket/path or /bucket/path
            # paths may be passed as positional argument or kwarg arguments
            # process accordingly and keep paths as positional argument if passed as positional,
            # and keep as kwarg arguments if passed as kwarg.

            positional_params = list(
                args
            )  # args is an immutable tuple so make a copy of it to change its elements

            # get a list of arguments defined in the function
            # argument_list is used to check which path parameter that needs processing appears
            argument_list = []
            for param in signature.parameters.values():
                argument_list.append(param.name)

            # fsspec path parameters has different names and sequences
            # check if the path parameters are passed as kwargs or positional args
            # process the path and put them back into kwargs or positional args
            possible_path_arg_names = [
                "path",
                "path1",
                "path2",
                "lpath",
                "rpath",
            ]
            for path in possible_path_arg_names:
                if path in argument_list:
                    if path in kwargs:
                        kwargs[path] = self._strip_alluxiofs_protocol(
                            kwargs[path]
                        )
                    else:
                        path_index = argument_list.index(path) - 1
                        positional_params[
                            path_index
                        ] = self._strip_alluxiofs_protocol(
                            positional_params[path_index]
                        )

            positional_params = tuple(positional_params)

            try:
                if self.alluxio:
                    start_time = time.time()
                    res = alluxio_impl(self, *positional_params, **kwargs)
                    self.logger.debug(
                        f"Exit(Ok): alluxio op({alluxio_impl.__name__}) args({positional_params}) kwargs({kwargs}) time({(time.time() - start_time):.2f}s)"
                    )
                    return res
            except Exception as e:
                if not isinstance(e, NotImplementedError):
                    if alluxio_impl.__name__ not in [
                        "write",
                        "upload",
                        "upload_data",
                    ]:
                        self.logger.error(
                            f"Exit(Error): alluxio op({alluxio_impl.__name__}) args({positional_params}) kwargs({kwargs}): {e}\nfallback to ufs"
                        )
                    else:
                        self.logger.error(
                            f"Exit(Error): alluxio op({alluxio_impl.__name__}) {e}\nfallback to ufs"
                        )
                    self.error_metrics.record_error(alluxio_impl.__name__, e)
                if self.fs is None:
                    raise e

            fs_method = getattr(self.fs, alluxio_impl.__name__, None)

            if fs_method:
                try:
                    res = fs_method(*positional_params, **kwargs)
                    self.logger.debug(
                        f"Exit(Ok): ufs({self.target_protocol}) op({alluxio_impl.__name__}) args({positional_params}) kwargs({kwargs})"
                    )
                    return res
                except Exception as e:
                    self.logger.error(f"fallback to ufs is failed: {e}")
                raise Exception("fallback to ufs is failed")
            raise NotImplementedError(
                f"The method {alluxio_impl.__name__} is not implemented in the underlying filesystem {self.target_protocol}"
            )

        return fallback_wrapper

    @fallback_handler
    def ls(self, path, detail=False, **kwargs):
        path = self.unstrip_protocol(path)
        paths = self.alluxio.listdir(path)
        return [
            self._translate_alluxio_info_to_fsspec_info(p, detail)
            for p in paths
        ]

    @fallback_handler
    def info(self, path, **kwargs):
        path = self.unstrip_protocol(path)
        if path in self.file_info_cache:
            return self.file_info_cache[path]
        else:
            file_status = self.alluxio.get_file_status(path)
            fsspec_info = self._translate_alluxio_info_to_fsspec_info(
                file_status, True
            )
            self.file_info_cache[path] = fsspec_info
            return fsspec_info

    @fallback_handler
    def exists(self, path, **kwargs):
        try:
            path = self.unstrip_protocol(path)
            self.alluxio.get_file_status(path)
            return True
        except FileNotFoundError:
            return False

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

        if self.alluxio.config.mcap_enabled:
            kwargs["cache_type"] = "none"
        raw_file = AlluxioFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            # cache=self.alluxio.mem_cache,
            **kwargs,
        )
        read_buffer_size_mb = self.alluxio.config.read_buffer_size_mb

        # Local read buffer for optimizing frequent small byte reads
        _read_buffer_size = int(1024 * 1024 * float(read_buffer_size_mb))
        return io.BufferedReader(raw_file, buffer_size=_read_buffer_size)

    @fallback_handler
    def cat_file(self, path, start=0, end=None, **kwargs):
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
        path = self.unstrip_protocol(path)
        return self.alluxio.mkdir(path)

    @fallback_handler
    def makedirs(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def rm(
        self,
        path,
        recursive=False,
        recursive_alias=False,
        remove_alluxio_only=False,
        delete_mount_point=False,
        sync_parent_next_time=False,
        remove_unchecked_option_char=False,
    ):
        path = self.unstrip_protocol(path)
        option = RMOption(
            recursive,
            recursive_alias,
            recursive_alias,
            delete_mount_point,
            sync_parent_next_time,
            remove_unchecked_option_char,
        )
        return self.alluxio.rm(path, option)

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
        path = self.unstrip_protocol(path)
        return self.alluxio.touch(path)

    @fallback_handler
    def created(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def modified(self, path, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def head(self, path, *args, **kwargs):
        path = self.unstrip_protocol(path)
        return self.alluxio.head(path, *args, **kwargs)

    @fallback_handler
    def tail(self, path, *args, **kwargs):
        path = self.unstrip_protocol(path)
        return self.alluxio.tail(path, *args, **kwargs)

    @fallback_handler
    def expand_path(self, path, *args, **kwargs):
        raise NotImplementedError

    # Comment it out as s3fs will return folder as well.
    # @fallback_handler
    # def find(self, path, *args, **kwargs):
    #     raise NotImplementedError

    @fallback_handler
    def mv(self, path1, path2, *args, **kwargs):
        path1 = self.unstrip_protocol(path1)
        path2 = self.unstrip_protocol(path2)
        return self.alluxio.mv(path1, path2)

    @fallback_handler
    def copy(
        self,
        path1,
        path2,
        recursive=False,
        recursiveAlias=False,
        force=False,
        thread=None,
        bufferSize=None,
        preserve=None,
    ):
        path1 = self.unstrip_protocol(path1)
        path2 = self.unstrip_protocol(path2)
        option = CPOption(
            recursive, recursiveAlias, force, thread, bufferSize, preserve
        )
        return self.alluxio.cp(path1, path2, option)

    @fallback_handler
    def cp_file(self, path1, path2, *args, **kwargs):
        return self.copy(path1, path2, *args, **kwargs)

    @fallback_handler
    def rename(self, path1, path2, **kwargs):
        return self.mv(path1, path2, **kwargs)

    @fallback_handler
    def move(self, path1, path2, **kwargs):
        return self.mv(path1, path2, **kwargs)

    @fallback_handler
    def put_file(self, lpath, rpath, *args, **kwargs):
        return self.upload(lpath, rpath, *args, **kwargs)

    @fallback_handler
    def put(self, lpath, rpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def write_bytes(self, path, value, **kwargs):
        path = self.unstrip_protocol(path)
        return self.alluxio.write_chunked(path, value)

    @fallback_handler
    def read_bytes(self, path, *args, **kwargs):
        path = self.unstrip_protocol(path)
        return self.alluxio.read_chunked(path).read()

    @fallback_handler
    def upload(self, lpath: str, rpath: str, *args, **kwargs) -> bool:
        rpath = self.unstrip_protocol(rpath)
        with open(lpath, "rb") as f:
            return self.alluxio.write_chunked(rpath, f.read())

    @fallback_handler
    def download(self, lpath, rpath, *args, **kwargs):
        rpath = self.unstrip_protocol(rpath)
        with open(lpath, "wb") as f:
            return f.write(self.alluxio.read_chunked(rpath).read())

    @fallback_handler
    def get(self, rpath, lpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def get_file(self, rpath, lpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def read_block(self, *args, **kwargs):
        if self.fs:
            return self.fs.read_block(*args, **kwargs)
        else:
            raise NotImplementedError


class AlluxioFile(AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        super().__init__(fs, path, mode, **kwargs)
        self.alluxio_path = fs.info(path)["path"]
        self.mcap_enabled = fs.alluxio.config.mcap_enabled
        read_buffer_size_mb = fs.alluxio.config.read_buffer_size_mb

        # Local read buffer for optimizing frequent small byte reads
        self._read_buffer_size = int(1024 * 1024 * float(read_buffer_size_mb))
        self._read_buffer_data = b""
        self._read_buffer_start = 0
        self._read_buffer_end = 0
        self._file_size = getattr(self, "size", None)

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        import traceback

        try:
            res = self.fs.alluxio.read_file_range(
                file_path=self.path,
                alluxio_path=self.alluxio_path,
                offset=start,
                length=end - start,
            )
        except Exception as e:
            raise IOError(
                f"Failed to fetch range {start}-{end} of {self.alluxio_path}: {e} {traceback.print_exc()}"
            )
        return res

    def _upload_chunk(self, final=False):
        data = self.buffer.getvalue()
        if not data:
            return False
        if self.fs.write(path=self.path, value=data):
            return True
        return False

    def _initiate_upload(self):
        pass

    def close(self):
        """Close file and clean up resources to prevent memory leaks"""
        if not self.closed:
            # Clear read buffer to help GC
            self._read_buffer_data = b""
            self._read_buffer_start = 0
            self._read_buffer_end = 0
            # Clear file size reference
            self._file_size = None
        super().close()

    def flush(self, force=False):
        if self.closed:
            raise ValueError("Flush on closed file")
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")
        if force:
            self.forced = True

        if self.mode not in {"wb", "ab"}:
            # no-op to flush on read-mode
            return

        if self.offset is None:
            # Initialize a multipart upload
            self.offset = 0
            try:
                self._initiate_upload()
            except Exception as e:
                self.closed = True
                raise e

        if self._upload_chunk(final=force) is not False:
            self.offset += self.buffer.seek(0, 2)
            self.buffer = io.BytesIO()
