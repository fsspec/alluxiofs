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
import os
import time
import traceback
from dataclasses import dataclass
from functools import wraps

import fsspec
import yaml
from cachetools import LRUCache
from fsspec import AbstractFileSystem
from fsspec import filesystem
from fsspec.spec import AbstractBufferedFile

from alluxiofs.client import AlluxioClient
from alluxiofs.client.config import AlluxioClientConfig
from alluxiofs.client.utils import convert_ufs_info_to
from alluxiofs.client.utils import setup_logger


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
        yaml_path=None,
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
        The underlying filesystem args
            ufs (object, optional): Directly supplies an instance of a file system object for accessing the underlying storage of Alluxio
        Other args:
            test_options (dict, optional): A dictionary of options used exclusively for testing purposes.
                These might include mock interfaces or specific configuration overrides for test scenarios.
            **kwargs: other parameters for core session.
        """
        assert (
            isinstance(yaml_path, str) or yaml_path is None
        ), f"{yaml_path} must be a string or None, got {type(yaml_path).__name__}."
        yaml_cfg = self.load_yaml_config(yaml_path) if yaml_path else {}
        kwargs = self.merge_config(yaml_cfg, kwargs)

        super().__init__(**kwargs)
        log_dir = kwargs.get("log_dir")
        log_level = kwargs.get("log_level")
        log_dir = (
            log_dir
            if log_dir is not None
            else os.getenv("ALLUXIO_PYTHON_SDK_LOG_DIR", None)
        )
        log_level = (
            log_level
            if log_level is not None
            else os.getenv("ALLUXIO_PYTHON_SDK_LOG_LEVEL", "INFO")
        )
        self.logger = setup_logger(log_dir, log_level)
        # init alluxio client
        test_options = kwargs.get("test_options", {})
        if test_options.get("skip_alluxio") is True:
            self.alluxio = None
        else:
            self.alluxio = AlluxioClient(
                logger=self.logger,
                **kwargs,
            )
        # init ufs
        ufs = kwargs.get("ufs")
        self.ufs = {}
        self.ufs_info = {}
        if ufs is None:
            self.logger.warning(
                "No 'ufs' parameter provided. Will not fall back to under file systems when "
                "accessed files failed in Alluxiofs."
            )
        else:
            self.target_protocols = [
                p.strip() for p in ufs.split(",") if p.strip()
            ]
            for protocol in self.target_protocols:
                self.register_unregistered_ufs_to_fsspec(protocol)
                if fsspec.get_filesystem_class(protocol) is None:
                    raise ValueError(f"Unsupported protocol: {protocol}")
                else:
                    target_options = self.get_target_options_from_worker(
                        protocol
                    )
                    self.ufs[protocol] = filesystem(protocol, **target_options)

        self.file_info_cache = LRUCache(maxsize=1000)
        self.error_metrics = AlluxioErrorMetrics()

    def load_yaml_config(self, path: str) -> dict:
        """Load YAML configuration from file. Returns empty dict if file is missing, None, or empty."""
        if path is None or not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            print(f"Error parsing YAML config file '{path}': {e}")
            return {}
        except Exception as e:
            print(f"Error loading YAML config file '{path}': {e}")
            return {}

    def merge_config(self, yaml_config: dict, other_config: dict) -> dict:
        # Extract defaults from AlluxioClientConfig.__init__
        defaults = {}
        sig = inspect.signature(AlluxioClientConfig.__init__)
        for k, v in sig.parameters.items():
            if k == "self" or v.default is inspect._empty:
                continue
            defaults[k] = v.default
        # Merge all keys, preserving everything from yaml_config and other_config
        merged = {**defaults, **yaml_config, **other_config}
        return merged

    def register_unregistered_ufs_to_fsspec(self, protocol):
        if protocol == "bos":
            try:
                from bosfs import BOSFileSystem
            except ImportError as e:
                raise ImportError(f"Please install bosfs, {e}")
            fsspec.register_implementation("bos", BOSFileSystem)

    def get_protocol_from_path(self, path):
        return path.split("://")[0]

    def get_target_options_from_worker(self, ufs):
        if ufs in self.ufs_info:
            return self.ufs_info[ufs]
        else:
            if self.alluxio:
                info = self.alluxio.get_target_options_from_worker(ufs)
                return convert_ufs_info_to(ufs, info)
            else:
                return {}

    def get_error_metrics(self):
        return self.error_metrics.get_metrics()

    def _translate_alluxio_info_to_fsspec_info(self, file_status, detail):
        if detail:
            res = file_status.__dict__
            return res
        else:
            return file_status.path

    def fallback_handler(func):
        """
        Decorator that attempts to perform an operation using the Alluxio implementation.
        If it fails (or is not implemented), it falls back to the Underlying File System (UFS).
        It also sanitizes path arguments by stripping protocol prefixes (e.g., 's3://').
        """

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # 1. Pre-processing: Argument Binding
            # Use inspect.bind() to map args/kwargs to parameter names safely.
            sig = inspect.signature(func)
            bound = sig.bind(self, *args, **kwargs)
            bound.apply_defaults()

            detected_protocol = None
            # Define constants at the module level or class level
            PATH_ARG_NAMES = {"path", "path1", "path2", "lpath", "rpath"}

            # 2. Argument Sanitization & Protocol Detection
            # We iterate ONCE. We must detect the protocol BEFORE stripping it.
            for name, value in bound.arguments.items():
                if name in PATH_ARG_NAMES and isinstance(value, str):
                    # A. Try to capture protocol if we haven't found one yet
                    if detected_protocol is None:
                        detected_protocol = self._get_protocol_from_path(value)

                    # B. Strip the protocol from the argument for Alluxio usage
                    # Modify the value directly in the bound arguments dictionary
                    bound.arguments[name] = self._strip_protocol(value)

            # Extract sanitized arguments for the Alluxio call
            sanitized_args = bound.args
            sanitized_kwargs = bound.kwargs

            # 3. Happy Path: Attempt to execute via Alluxio
            try:
                if self.alluxio:
                    start_time = time.time()
                    # Call the original method with sanitized arguments
                    result = func(*sanitized_args, **sanitized_kwargs)
                    duration = time.time() - start_time
                    self.logger.debug(
                        f"Exit(Ok): alluxio op({func.__name__}) "
                        f"time({duration:.2f}s)"
                    )
                    return result
                else:
                    raise ModuleNotFoundError("alluxio client is None")
            except Exception as e:
                # If not NotImplementedError, it's a real runtime error. Log it.
                self._log_alluxio_error(func.__name__, e)

            # 4. Fallback Path: Execute logic on UFS
            # FIX: Pass the 'bound' object, not the sanitized tuple/dict,
            # because _execute_fallback needs to inspect parameter names.
            return self._execute_fallback(
                func.__name__, detected_protocol, bound
            )

        return wrapper

    # ---------------------------------------------------------
    # The following methods should be part of your Class
    # (e.g., AlluxioFileSystem)
    # ---------------------------------------------------------

    def _get_protocol_from_path(self, path):
        """Extracts protocol (e.g., 's3') from 's3://bucket/key'."""
        if path and "://" in path:
            return path.split("://")[0]
        return None

    def _execute_fallback(self, method_name, protocol, bound_args):
        """
        Executes the operation using the Underlying File System (UFS).
        Includes Smart Argument Adaptation to prevent TypeErrors.
        """
        try:
            # Determine which protocol to use.
            # If no protocol was detected from args, use the FS default target protocol.
            target_protocol = protocol if protocol else None

            # Retrieve the UFS client
            fs = self.ufs.get(target_protocol)

            if fs is None:
                raise RuntimeError(
                    f"No UFS client found for protocol: {target_protocol}"
                )

            # Dynamically retrieve the corresponding method from the UFS client
            fs_method = getattr(fs, method_name, None)
            if not fs_method:
                raise NotImplementedError(
                    f"Method {method_name} is not implemented in UFS {target_protocol}"
                )

            # --- Smart Argument Adaptation ---

            # 1. Inspect the target method's signature (UFS implementation)
            target_sig = inspect.signature(fs_method)
            target_params = target_sig.parameters

            # 2. Check if the target method accepts generic **kwargs.
            accepts_kwargs = any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in target_params.values()
            )

            # 3. Construct the final arguments dictionary (Keyword Arguments only)
            final_kwargs = {}

            for name, value in bound_args.arguments.items():
                if name == "self":
                    continue  # Never pass the wrapper's 'self' to the UFS instance method

                # Pass the argument ONLY if:
                # A. The target method explicitly defines this parameter name, OR
                # B. The target method accepts **kwargs (wildcard)
                if name in target_params or accepts_kwargs:
                    final_kwargs[name] = value
                else:
                    # Argument is implicitly dropped because the UFS method doesn't support it.
                    pass

            # 4. Execute the UFS method
            # Using **final_kwargs maps arguments by name, avoiding positional mismatches.
            res = fs_method(**final_kwargs)

            self.logger.debug(
                f"Exit(Ok): ufs({target_protocol}) op({method_name})"
            )
            return res

        except Exception as e:
            self.logger.error(f"Fallback to UFS failed for {method_name}")
            # [Critical] Use 'from e' to preserve the original exception stack trace
            raise RuntimeError(
                f"Fallback to UFS failed for {method_name}"
            ) from e

    def _log_alluxio_error(self, method_name, error):
        """
        Helper method to handle error logging, keeping the main logic clean.
        """
        log_msg = f"Exit(Error): alluxio op({method_name}), fallback to ufs"
        self.logger.warning(log_msg)
        if len(self.ufs) > 0:
            self.logger.debug(f"{error} {traceback.format_exc()}")
        else:
            self.logger.info(f"{error} {traceback.format_exc()}")
        self.error_metrics.record_error(method_name, error)

    @fallback_handler
    def ls(self, path, detail=False, **kwargs):
        paths = self.alluxio.listdir(path)
        return [
            self._translate_alluxio_info_to_fsspec_info(p, detail)
            for p in paths
        ]

    @fallback_handler
    def info(self, path, **kwargs):
        if path in self.file_info_cache:
            return self.file_info_cache[path]
        file_status = self.alluxio.get_file_status(path)
        fsspec_info = self._translate_alluxio_info_to_fsspec_info(
            file_status, True
        )
        self.file_info_cache[path] = fsspec_info
        return fsspec_info

    @fallback_handler
    def exists(self, path, **kwargs):
        try:
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
        protocol = self.get_protocol_from_path(path)
        ufs = self.ufs.get(protocol)
        if self.alluxio.config.mcap_enabled:
            kwargs["cache_type"] = "none"
        raw_file = AlluxioFile(
            alluxio=self,
            ufs=ufs,
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
        alluxio_path = self.info(path)["name"]
        return self.alluxio.read_file_range(path, alluxio_path, start, length)

    @fallback_handler
    def mkdir(self, path, *args, **kwargs):
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
        option = RMOption(
            recursive,
            recursive_alias,
            remove_alluxio_only,
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
        return self.alluxio.touch(path)

    @fallback_handler
    def created(self, path, *args, **kwargs):
        return self.info(path).get("created", None)

    @fallback_handler
    def modified(self, path, *args, **kwargs):
        return self.info(path).get("mtime", None)

    @fallback_handler
    def head(self, path, *args, **kwargs):
        return self.alluxio.head(path, *args, **kwargs)

    @fallback_handler
    def tail(self, path, *args, **kwargs):
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
    def put_file(self, lpath, rpath, *args, **kwargs):
        return self.upload(lpath, rpath, *args, **kwargs)

    @fallback_handler
    def put(self, lpath, rpath, *args, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def write_bytes(self, path, value, **kwargs):
        return self.alluxio.write_chunked(path, value)

    @fallback_handler
    def read_bytes(self, path, *args, **kwargs):
        return self.alluxio.read_chunked(path).read()

    @fallback_handler
    def created(self, path):
        info = self.info(path)
        return info.get("created", None)

    @fallback_handler
    def modified(self, path):
        info = self.info(path)
        return info.get("mtime", None)

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


class AlluxioFile(AbstractBufferedFile):
    def __init__(self, alluxio, ufs, path, mode="rb", **kwargs):
        super().__init__(alluxio, path, mode, **kwargs)
        self.alluxio_path = alluxio.info(path)["name"]
        self.ufs = ufs
        if ufs and isinstance(ufs, AbstractFileSystem):
            self.f = ufs.open(path, mode, **kwargs)
        else:
            self.f = None
        self.kwargs = kwargs
        self.logger = alluxio.logger

    def fallback_handler(alluxio_impl):
        @wraps(alluxio_impl)
        def fallback_wrapper(self, *args, **kwargs):
            signature = inspect.signature(alluxio_impl)
            positional_params = list(args)
            argument_list = []
            for param in signature.parameters.values():
                argument_list.append(param.name)
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
                        kwargs[path] = self._strip_protocol(kwargs[path])
                    else:
                        path_index = argument_list.index(path) - 1
                        positional_params[path_index] = self._strip_protocol(
                            positional_params[path_index]
                        )

            positional_params = tuple(positional_params)

            try:
                if self.fs:
                    res = alluxio_impl(self, *positional_params, **kwargs)
                    return res
            except Exception as e:
                if not isinstance(e, NotImplementedError):
                    self.logger.warning(
                        f"alluxio's {alluxio_impl.__name__} failed, fallback to ufs"
                    )
                    self.logger.debug(f"{e} {traceback.format_exc()}")
                if self.ufs is None:
                    raise e
            fs_method = getattr(self.f, alluxio_impl.__name__, None)
            if fs_method:
                try:
                    res = fs_method(*positional_params, **kwargs)
                    return res
                except Exception:
                    self.logger.error("fallback to ufs is failed")
                raise Exception("fallback to ufs is failed")
            raise NotImplementedError(
                f"The method {alluxio_impl.__name__} is not implemented in the underlying filesystem {self.target_protocol}"
            )

        return fallback_wrapper

    @fallback_handler
    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""

        try:
            res = self.fs.alluxio.read_file_range(
                file_path=self.path,
                alluxio_path=self.alluxio_path,
                offset=start,
                length=end - start,
            )
        except Exception as e:
            raise IOError(
                f"Failed to fetch range {start}-{end} of {self.alluxio_path}: {e} "
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
            if self.f is not None:
                self.f.close()
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
