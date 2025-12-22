# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
import json
import logging
import os
import time
from functools import wraps
from io import BytesIO

import fsspec
import pycurl

from .const import ALLUXIO_REQUEST_MAX_RETRIES
from .const import ALLUXIO_REQUEST_MAX_TIMEOUT_SECONDS


LOG_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}

OSS_SETUP_OPTIONS_MAP = {
    "access_key": "key",
    "secret_key": "secret",
    "endpoint": "endpoint",
}


def convert_ufs_info_to(ufs, info):
    if ufs == "oss":
        res = {OSS_SETUP_OPTIONS_MAP[k]: info[k] for k in info}
    else:
        res = info
    return res


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


class TagAdapter(logging.LoggerAdapter):
    """Logger adapter that prefixes messages with a fixed tag."""

    def process(self, msg, kwargs):
        return f"{self.extra['tag']} {msg}", kwargs


class TagFilter(logging.Filter):
    """
    Filter for multi-tag log matching.
    Allows a log record to pass if its message contains any of the specified tags.
    """

    def __init__(self, tags):
        super().__init__()
        # Ensure tags is always a list for consistent iteration
        if isinstance(tags, str):
            self.tags = [t.strip() for t in tags.split(",") if t.strip()]
        elif tags is None:
            self.tags = []
        else:
            self.tags = tags

    def filter(self, record):
        # If no tags are specified, allow all logs
        if not self.tags:
            return True
        message = record.getMessage()
        # Returns True if any tag in the list is found within the message
        return any(tag in message for tag in self.tags)


def setup_logger(
    file_path=os.getenv("ALLUXIO_PYTHON_SDK_LOG_DIR", None),
    level_str=os.getenv("ALLUXIO_PYTHON_SDK_LOG_LEVEL", "INFO"),
    class_name=__name__,
    log_tags=None,
):
    # Map string level to logging constants
    level = LOG_LEVEL_MAP.get(level_str.upper(), logging.INFO)

    # Initialize handlers list
    handlers = []

    # 1. Console Handler (Always active)
    console_handler = logging.StreamHandler()
    handlers.append(console_handler)

    # 2. File Handler (Conditional)
    if file_path:
        file_name = "user.log"

        if not os.path.exists(file_path):
            os.makedirs(file_path, exist_ok=True)

        log_file = os.path.join(file_path, file_name)
        file_handler = logging.FileHandler(log_file)
        handlers.append(file_handler)

    # Set log message format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Prepare TagFilter if needed
    tag_filter = None
    if log_tags:
        tag_filter = TagFilter(log_tags)

    # Apply configuration to all active handlers
    for handler in handlers:
        handler.setFormatter(formatter)
        handler.setLevel(level)
        if tag_filter:
            handler.addFilter(tag_filter)

    # Initialize logger
    logger = logging.getLogger(class_name)

    # Prevent duplicate logs by disabling propagation and clearing existing handlers
    logger.propagate = False
    if logger.hasHandlers():
        logger.handlers.clear()

    # Add all configured handlers to the logger
    for handler in handlers:
        logger.addHandler(handler)

    logger.setLevel(level)

    return logger


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
