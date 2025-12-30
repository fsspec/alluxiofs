# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
import json
import time
from functools import wraps
from io import BytesIO

import fsspec
import pycurl

from .const import ALLUXIO_REQUEST_MAX_RETRIES
from .const import ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS


OSS_SETUP_OPTIONS_MAP = {
    "access_key": "key",
    "secret_key": "secret",
    "endpoint": "endpoint",
}

S3_SETUP_OPTIONS_MAP = {
    "access_key": "key",
    "secret_key": "secret",
    "endpoint": "endpoint_url",
}


def convert_ufs_info_to(ufs, info):
    if ufs == "oss":
        res = {OSS_SETUP_OPTIONS_MAP[k]: info[k] for k in info}
    elif ufs == "s3" or ufs == "s3a":
        res = {
            S3_SETUP_OPTIONS_MAP[k]: info[k]
            for k in info
            if k in S3_SETUP_OPTIONS_MAP
        }
    else:
        res = info
    return res


def parameters_adapter(fs, fs_method, final_kwargs):
    """Adapts parameters for different filesystems and methods."""
    adapted_kwargs = final_kwargs.copy()
    method_name = getattr(fs_method, "__name__", str(fs_method))
    protocols = (fs.protocol,) if isinstance(fs.protocol, str) else fs.protocol

    if "bos" in protocols:
        if method_name in ["cp_file"]:
            if "path1" in final_kwargs and "path2" in final_kwargs:
                adapted_kwargs["src_path"] = final_kwargs.pop("path1")
                adapted_kwargs["target_path"] = final_kwargs.pop("path2")

    if "oss" in protocols:
        if method_name in ["pipe_file"]:
            headers = {}
            mode = adapted_kwargs.pop("mode", "overwrite")
            if mode == "overwrite":
                # Default behavior, explicitly allowing overwrite (optional, usually not needed)
                headers["x-oss-forbid-overwrite"] = "false"
            else:  # Or whatever logic you use for "don't overwrite"
                # This will cause the upload to fail if the file exists
                headers["x-oss-forbid-overwrite"] = "true"
            adapted_kwargs["headers"] = headers

    if any(p in ["s3", "s3a"] for p in protocols):
        if method_name in ["_pipe_file", "pipe_file"]:
            if "value" in final_kwargs:
                adapted_kwargs["data"] = final_kwargs.pop("value")

    return adapted_kwargs


def get_protocol_from_path(path):
    """Extracts protocol (e.g., 's3') from 's3://bucket/key'."""
    if path and "://" in path:
        return path.split("://")[0]
    return None


def register_unregistered_ufs_to_fsspec(protocol):
    if protocol == "bos":
        try:
            from bosfs import BOSFileSystem
        except ImportError as e:
            raise ImportError(f"Please install bosfs, {e}")
        fsspec.register_implementation("bos", BOSFileSystem)


def get_prefetch_policy(config, block_size):
    policy_name = config.local_cache_prefetch_policy.lower()
    if policy_name == "none":
        from alluxiofs.client.prefetch_policy import NoPrefetchPolicy

        return NoPrefetchPolicy(block_size, config)
    elif policy_name == "fixed_window":
        from alluxiofs.client.prefetch_policy import FixedWindowPrefetchPolicy

        return FixedWindowPrefetchPolicy(block_size, config)
    elif policy_name == "adaptive_window":
        from alluxiofs.client.prefetch_policy import (
            AdaptiveWindowPrefetchPolicy,
        )

        return AdaptiveWindowPrefetchPolicy(block_size, config)
    else:
        raise ValueError(
            f"Unsupported prefetch policy: {config.local_cache_prefetch_policy}"
        )


def retry_on_network(tries=3, delay=1, backoff=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _tries = kwargs.pop("retry_tries", tries)
            _delay = kwargs.pop("retry_delay", delay)
            _backoff = kwargs.pop("retry_backoff", backoff)

            while _tries > 0:
                try:
                    return func(*args, **kwargs)
                except (pycurl.error, ConnectionResetError, TimeoutError) as e:
                    _tries -= 1
                    if _tries == 0:
                        raise
                    print(
                        f"[retry_on_network] Network exception: {e}, retrying in {_delay}s..."
                    )
                    time.sleep(_delay)
                    _delay *= _backoff
                except RuntimeError as e:
                    if "cURL error: (28" in str(e):
                        _tries -= 1
                        if _tries == 0:
                            raise
                        print(
                            f"[retry_on_network] Timeout exception: {e}, retrying in {_delay}s..."
                        )
                        time.sleep(_delay)
                        _delay *= _backoff
                    else:
                        raise

        return wrapper

    return decorator


@retry_on_network(tries=ALLUXIO_REQUEST_MAX_RETRIES, delay=1)
def _c_send_get_request_write_bytes(
    url,
    headers,
    time_out=ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS,
    max_file_size=20 * 1024 * 1024,
):
    buffer = BytesIO()
    c = pycurl.Curl()
    try:
        c.setopt(c.MAXFILESIZE, max_file_size)
        headers_list = [
            f"{k}: {v}".encode("utf-8") for k, v in headers.items()
        ]
        c.setopt(c.URL, url.encode("utf-8"))
        c.setopt(c.HTTPHEADER, headers_list)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.CONNECTTIMEOUT, 10)
        c.setopt(c.TIMEOUT, time_out)
        c.setopt(c.BUFFERSIZE, 16384)
        c.setopt(c.FORBID_REUSE, True)
        c.setopt(c.FRESH_CONNECT, True)
        c.setopt(c.TCP_KEEPALIVE, 0)
        c.setopt(c.NOSIGNAL, 1)
        c.setopt(c.HTTP_VERSION, c.CURL_HTTP_VERSION_1_1)

        c.perform()
        status = c.getinfo(c.RESPONSE_CODE)
        result = buffer.getvalue()

        if status == 104:
            raise ConnectionResetError("Connection reset by peer")
        elif status >= 400:
            try:
                content = json.loads(result)
                message = content.get(
                    "message", result.decode("utf-8", errors="replace")
                )
            except Exception:
                message = result.decode("utf-8", errors="replace")

            if status == 404:
                raise FileNotFoundError(message)
            elif status == 400:
                raise ValueError(message)
            elif status == 403:
                raise PermissionError(message)
            elif status == 401:
                raise PermissionError(f"Unauthorized: {message}")
            elif status == 409:
                raise FileExistsError(message)
            elif status == 503:
                raise ConnectionError(message)
            elif status == 412:
                raise RuntimeError(f"Precondition failed: {message}")
            else:
                raise RuntimeError(f"HTTP error {status}: {message}")

        return result

    except pycurl.error as e:
        raise RuntimeError(f"cURL error: {e}")
    finally:
        c.close()
        buffer.close()


@retry_on_network(tries=ALLUXIO_REQUEST_MAX_RETRIES, delay=1)
def _c_send_get_request_write_file(
    url,
    headers,
    f,
    time_out=ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS,
    max_file_size=20 * 1024 * 1024,
):
    c = pycurl.Curl()
    try:
        c.setopt(c.MAXFILESIZE, max_file_size)
        headers_list = [
            f"{k}: {v}".encode("utf-8") for k, v in headers.items()
        ]
        c.setopt(c.URL, url.encode("utf-8"))
        c.setopt(c.HTTPHEADER, headers_list)
        c.setopt(c.WRITEDATA, f)
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.CONNECTTIMEOUT, 10)
        c.setopt(c.TIMEOUT, time_out)
        c.setopt(c.BUFFERSIZE, 16384)

        c.setopt(c.FORBID_REUSE, True)
        c.setopt(c.FRESH_CONNECT, True)
        c.setopt(c.TCP_KEEPALIVE, 0)
        c.setopt(c.NOSIGNAL, 1)
        c.setopt(c.HTTP_VERSION, c.CURL_HTTP_VERSION_1_1)

        c.perform()
        status = c.getinfo(c.RESPONSE_CODE)

        if status == 104:
            raise ConnectionResetError("Connection reset by peer")
        elif status >= 400:
            raise RuntimeError(f"HTTP error: {status}")
    except pycurl.error as e:
        raise RuntimeError(f"cURL error: {e}")
    finally:
        c.close()


@retry_on_network(tries=ALLUXIO_REQUEST_MAX_RETRIES, delay=1)
def _c_send_get_request_stream(url, time_out, headers=None):
    if headers is None:
        headers = {}
    buffer = BytesIO()
    c = pycurl.Curl()
    try:
        headers_list = [
            f"{k}: {v}".encode("utf-8") for k, v in headers.items()
        ]
        c.setopt(c.URL, url.encode("utf-8"))
        c.setopt(c.HTTPHEADER, headers_list)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.CONNECTTIMEOUT, 10)
        c.setopt(c.TIMEOUT, time_out)

        c.setopt(c.FORBID_REUSE, True)
        c.setopt(c.FRESH_CONNECT, True)
        c.setopt(c.TCP_KEEPALIVE, 0)
        c.setopt(c.NOSIGNAL, 1)
        c.setopt(c.HTTP_VERSION, c.CURL_HTTP_VERSION_1_1)

        c.perform()
        status = c.getinfo(c.RESPONSE_CODE)

        if status == 104:
            raise ConnectionResetError("Connection reset by peer")
        elif status >= 400:
            raise RuntimeError(f"HTTP error: {status}")

        return buffer
    except pycurl.error as e:
        raise RuntimeError(f"cURL error: {e}")
    finally:
        c.close()
        buffer.close()
