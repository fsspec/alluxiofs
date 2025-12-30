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

import yaml
from cachetools import LRUCache
from fsspec import AbstractFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile

from alluxiofs.client import AlluxioClient
from alluxiofs.client.config import AlluxioClientConfig
from alluxiofs.client.log import setup_logger
from alluxiofs.client.log import TagAdapter
from alluxiofs.client.ufs_manager import UFSUpdater
from alluxiofs.client.utils import get_prefetch_policy
from alluxiofs.client.utils import parameters_adapter


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
        self._closed = False
        assert (
            isinstance(yaml_path, str) or yaml_path is None
        ), f"{yaml_path} must be a string or None, got {type(yaml_path).__name__}."
        yaml_cfg = self.load_yaml_config(yaml_path) if yaml_path else {}
        kwargs = self.merge_config(yaml_cfg, kwargs)
        super().__init__(**kwargs)

        # setup logger
        kwargs["log_dir"] = (
            kwargs["log_dir"]
            if kwargs["log_dir"] is not None
            else os.getenv("ALLUXIO_PYTHON_SDK_LOG_DIR", None)
        )
        kwargs["log_level"] = (
            kwargs["log_level"]
            if kwargs["log_level"] is not None
            else os.getenv("ALLUXIO_PYTHON_SDK_LOG_LEVEL", "INFO")
        )
        log_level = kwargs.get("log_level")
        log_dir = kwargs.get("log_dir")
        log_tag_allowlist = kwargs.get("log_tag_allowlist", "")
        base_logger = setup_logger(
            log_dir,
            log_level,
            self.__class__.__name__,
            log_tag_allowlist,
        )
        self.logger = TagAdapter(base_logger, {"tag": "[FSSPEC]"})
        self.fallback_logger = TagAdapter(base_logger, {"tag": "[FALLBACK]"})
        # init alluxio client
        # init ufs updater
        test_options = kwargs.get("test_options", {})
        skip_alluxio = test_options.get("skip_alluxio") is True
        client = AlluxioClient(**kwargs)
        self.ufs_updater = UFSUpdater(client)
        self.ufs_updater.start_updater()
        self.alluxio = None if skip_alluxio else client

        self.fallback_to_ufs_enabled = (
            self.alluxio.config.fallback_to_ufs_enabled
            if self.alluxio
            else kwargs.get("fallback_to_ufs_enabled", True)
        )
        self.config = client.config
        self.file_info_cache = LRUCache(maxsize=1000)
        self.error_metrics = AlluxioErrorMetrics()

    def __del__(self):
        if not self._closed:
            self.close()

    @classmethod
    def _strip_protocol(cls, path):
        """Strips the protocol prefix from a given path."""
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = str(path)
        if path.startswith(cls.protocol_prefix):
            path = path[len(cls.protocol_prefix) :]
        return path.rstrip("/") or cls.root_marker

    def close(self):
        if self._closed:
            return

        try:
            if self.alluxio is not None:
                self.alluxio.close()
            if hasattr(self, "ufs_updater") and self.ufs_updater is not None:
                self.ufs_updater.stop_updater()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
        finally:
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def is_file_in_local_cache(self, file_path):
        """Check if a file is present in the Alluxio local cache."""
        if self.alluxio is None:
            raise RuntimeError("Alluxio client is not initialized.")
        if self.alluxio.config.local_cache_enabled is False:
            raise RuntimeError("Alluxio local cache is not enabled.")
        return self.alluxio.data_manager.cache.is_file_in_local_cache(
            file_path
        )

    def load_yaml_config(self, path: str) -> dict:
        """Load YAML configuration from file. Returns empty dict if file is missing, None, or empty."""
        if path is None or not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML config file '{path}': {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error loading YAML config file '{path}': {e}")
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

    def get_error_metrics(self):
        return self.error_metrics.get_metrics()

    def _translate_alluxio_info_to_fsspec_info(self, file_status, detail):
        if detail:
            res = file_status.__dict__
            return res
        else:
            return file_status.name

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

            detected_path = None
            # Define constants at the module level or class level
            PATH_ARG_NAMES = {"path", "path1", "path2", "lpath", "rpath"}

            # 2. Argument Sanitization & Protocol Detection
            # We iterate ONCE. We must detect the protocol BEFORE stripping it.
            for name, value in bound.arguments.items():
                if name in PATH_ARG_NAMES and isinstance(value, str):
                    # A. Try to capture protocol if we haven't found one yet
                    if detected_path is None:
                        detected_path = value

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
                if (
                    isinstance(e, FileNotFoundError)
                    and func.__name__ == "info"
                ):
                    raise e
                if not self.fallback_to_ufs_enabled:
                    raise e
                self._log_alluxio_error(func.__name__, e)

            # 4. Fallback Path: Execute logic on UFS
            # FIX: Pass the 'bound' object, not the sanitized tuple/dict,
            # because _execute_fallback needs to inspect parameter names.
            return self._execute_fallback(func.__name__, detected_path, bound)

        return wrapper

    # ---------------------------------------------------------
    # The following methods should be part of your Class
    # (e.g., AlluxioFileSystem)
    # ---------------------------------------------------------

    def _execute_fallback(self, method_name, detected_path, bound_args):
        """
        Executes the operation using the Underlying File System (UFS).
        Includes Smart Argument Adaptation to prevent TypeErrors.
        """
        try:
            # Determine which protocol to use.
            # If no protocol was detected from args, use the FS default target protocol.

            # Retrieve the UFS client
            fs = self.ufs_updater.must_get_ufs_from_path(detected_path)

            if fs is None:
                raise RuntimeError(
                    f"No UFS client found for path: {detected_path}"
                )
            # Dynamically retrieve the corresponding method from the UFS client
            fs_method = getattr(fs, method_name, None)
            if not fs_method:
                raise NotImplementedError(
                    f"Method {method_name} is not implemented in UFS {fs}"
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

            # Access the signature from the bound arguments to identify VAR_KEYWORD parameters
            source_params = bound_args.signature.parameters

            for name, value in bound_args.arguments.items():
                if name == "self":
                    continue  # Never pass the wrapper's 'self' to the UFS instance method

                # Check if the current argument corresponds to **kwargs in the source function
                is_var_keyword = (
                    name in source_params
                    and source_params[name].kind
                    == inspect.Parameter.VAR_KEYWORD
                )

                if is_var_keyword:
                    # If it is **kwargs, unpack the dictionary and merge valid keys
                    for k, v in value.items():
                        if k in target_params or accepts_kwargs:
                            final_kwargs[k] = v
                else:
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
            final_kwargs = parameters_adapter(fs, fs_method, final_kwargs)
            res = fs_method(**final_kwargs)

            self.fallback_logger.debug(
                f"Exit(Ok): ufs({fs}) op({method_name})"
            )
            return res

        except Exception as e:
            self.fallback_logger.error(
                f"Fallback to UFS failed for {method_name}"
            )
            # [Critical] Use 'from e' to preserve the original exception stack trace
            raise RuntimeError(
                f"Fallback to UFS failed for {method_name}"
            ) from e

    def _log_alluxio_error(self, method_name, error):
        """
        Helper method to handle error logging, keeping the main logic clean.
        """
        log_msg = f"Exit(Error): alluxio op({method_name}), fallback to ufs"
        self.fallback_logger.warning(log_msg)
        if self.ufs_updater.must_get_ufs_count() > 0:
            self.fallback_logger.debug(f"{error} {traceback.format_exc()}")
        else:
            self.fallback_logger.info(f"{error} {traceback.format_exc()}")
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
    def isdir(self, path):
        try:
            info = self.info(path)
            return info.get("type") == "directory"
        except FileNotFoundError:
            return path.endswith("/")

    @fallback_handler
    def exists(self, path, **kwargs):
        try:
            self.alluxio.get_file_status(path)
            return True
        except FileNotFoundError:
            return False

    def open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        compression=None,
        **kwargs,
    ):
        return super().open(
            path,
            mode,
            block_size,
            cache_options,
            compression,
            **kwargs,
        )

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """Open a file for reading or writing."""
        ufs = (
            self.ufs_updater.must_get_ufs_from_path(path)
            if path and self.fallback_to_ufs_enabled
            else None
        )
        if self.alluxio and self.alluxio.config.local_cache_enabled:
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
        read_buffer_size_mb = self.config.read_buffer_size_mb

        # Local read buffer for optimizing frequent small byte reads
        _read_buffer_size = int(1024 * 1024 * float(read_buffer_size_mb))
        return io.BufferedReader(raw_file, buffer_size=_read_buffer_size)

    @fallback_handler
    def mkdir(self, path, create_parents=False, **kwargs):
        if create_parents:
            raise NotImplementedError(
                "mkdir with create_parents=True is not supported. Only single directory creation is available."
            )
        return self.alluxio.mkdir(path)

    @fallback_handler
    def makedirs(self, path, exist_ok=False, **kwargs):
        raise NotImplementedError(
            "makedirs is not implemented. Use mkdir() for single directory creation."
        )

    @fallback_handler
    def _rm(
        self,
        path,
    ):
        return self.alluxio.rm(path, RMOption())

    @fallback_handler
    def rm(self, path, recursive=False, **kwargs):
        return self.alluxio.rm(path, RMOption(recursive=recursive))

    @fallback_handler
    def rmdir(self, path):
        contents = self.ls(path, detail=False)
        # Remove the directory itself from the listing, if present
        contents = [
            item
            for item in contents
            if item.rstrip("/\\") != path.rstrip("/\\")
        ]
        if contents:
            raise OSError(f"Directory not empty: {path}")
        return self.alluxio.rm(path, RMOption(recursive=True))

    @fallback_handler
    def touch(self, path, truncate=True, **kwargs):
        return self.alluxio.touch(path)

    @fallback_handler
    def created(self, path, **kwargs):
        info = self.info(path, **kwargs)
        return info.get("created", None)

    @fallback_handler
    def modified(self, path, **kwargs):
        """Get the modification time of a file."""
        info = self.info(path, **kwargs)
        return info.get("mtime", None)

    # Comment it out as s3fs will return folder as well.
    @fallback_handler
    def find(
        self, path, maxdepth=None, withdirs=False, detail=False, **kwargs
    ):
        raise NotImplementedError

    @fallback_handler
    def glob(self, path, maxdepth=None, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def walk(
        self, path, maxdepth=None, topdown=True, on_error="omit", **kwargs
    ):
        raise NotImplementedError

    @fallback_handler
    def du(self, path, total=True, maxdepth=None, withdirs=False, **kwargs):
        raise NotImplementedError

    @fallback_handler
    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        return self.alluxio.mv(path1, path2)

    @fallback_handler
    def cp_file(self, path1, path2, **kwargs):
        return self.alluxio.cp(path1, path2, CPOption())

    # TODO: need to implement after stream write is ready
    @fallback_handler
    def put_file(
        self,
        lpath,
        rpath,
        callback=DEFAULT_CALLBACK,
        mode="overwrite",
        **kwargs,
    ):
        raise NotImplementedError("put_file is not implemented")

    @fallback_handler
    def pipe_file(self, path, value, mode="overwrite", **kwargs):
        if mode != "overwrite":
            raise NotImplementedError(
                "only pipe_file with mode = overwrite is implemented."
            )
        return self.alluxio.write_chunked(path, value)

    @fallback_handler
    def tail(self, path, size=1024):
        with self.open(path, "rb") as f:
            f.seek(0, 2)
            file_len = f.tell()
            bytes_to_read = min(file_len, size)
            if bytes_to_read == 0:
                return b""
            f.seek(-bytes_to_read, 2)
            return f.read()

    @fallback_handler
    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        return super().cat(
            path, recursive=recursive, on_error=on_error, **kwargs
        )

    # need to implement
    @fallback_handler
    def cat_file(self, path, start=None, end=None, **kwargs):
        return super().cat_file(path, start=start, end=end, **kwargs)

    @fallback_handler
    def get_file(self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs):
        return super().get_file(rpath, lpath, callback=callback, **kwargs)

    @fallback_handler
    def sign(self, path, expiration=100, **kwargs):
        return super().sign(path, expiration=expiration, **kwargs)

    @fallback_handler
    def fsid(self):
        return super().fsid()

    @fallback_handler
    def ukey(self, path):
        """Hash of file properties, to tell if it has changed"""
        return super().ukey(path)

    @fallback_handler
    def checksum(self, path):
        """Get the checksum of a file."""
        return super().checksum(path)


class AlluxioFile(AbstractBufferedFile):
    def __init__(self, alluxio, ufs, path, mode="rb", **kwargs):
        super().__init__(alluxio, path, mode, **kwargs)
        if alluxio:
            self.alluxio_path = (
                alluxio.ufs_updater.must_get_alluxio_path_from_ufs_full_path(
                    path
                )
            )
        self.ufs = ufs
        self.kwargs = kwargs
        self.fallback_logger = alluxio.fallback_logger
        if alluxio.alluxio and alluxio.alluxio.config.local_cache_enabled:
            block_size = (
                int(alluxio.alluxio.config.local_cache_block_size_mb)
                * 1024
                * 1024
            )
            self.prefetch_policy = get_prefetch_policy(
                alluxio.alluxio.config, block_size
            )
        else:
            self.prefetch_policy = None

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
                        kwargs[path] = self.fs._strip_protocol(kwargs[path])
                    else:
                        path_index = argument_list.index(path) - 1
                        if path_index >= 0 and path_index < len(
                            positional_params
                        ):
                            positional_params[
                                path_index
                            ] = self.fs._strip_protocol(
                                positional_params[path_index]
                            )

            positional_params = tuple(positional_params)

            try:
                if self.fs and self.fs.alluxio:
                    res = alluxio_impl(self, *positional_params, **kwargs)
                    return res
                else:
                    raise ModuleNotFoundError("alluxio client is None")
            except Exception as e:
                if not isinstance(e, NotImplementedError):
                    self.fallback_logger.warning(
                        f"alluxio's {alluxio_impl.__name__} failed, fallback to ufs"
                    )
                    self.fallback_logger.debug(f"{e} {traceback.format_exc()}")
                if self.ufs is not None and isinstance(
                    self.ufs, AbstractFileSystem
                ):
                    self.f = self.ufs.open(self.path, self.mode, **self.kwargs)
                else:
                    self.f = None
                if (not self.fs.fallback_to_ufs_enabled) or self.f is None:
                    raise e
            fs_method = getattr(self.f, alluxio_impl.__name__, None)
            if fs_method:
                try:
                    res = fs_method(*positional_params, **kwargs)
                    return res
                except Exception as e:
                    self.fallback_logger.error(f"fallback to ufs failed: {e}")
                    raise Exception(f"fallback to ufs failed: {e}") from e
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
                prefetch_policy=self.prefetch_policy,
            )
        except Exception as e:
            self.fallback_logger.error(
                f"Failed to fetch range {start}-{end} of {self.alluxio_path}: {e}"
            )
            raise IOError(
                f"Failed to fetch range {start}-{end} of {self.alluxio_path}: {e} "
            ) from e
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
            if hasattr(self, "f") and self.f is not None:
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

    def get_local_cache_prefetch_window_size(self):
        if self.fs.alluxio and self.fs.alluxio.config.local_cache_enabled:
            return self.prefetch_policy.get_window_size()
        return 0
