# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
"""Dedicated logging helpers shared across the Alluxio Python SDK."""
import logging
import os

__all__ = ["LOG_LEVEL_MAP", "TagAdapter", "TagFilter", "setup_logger"]


LOG_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


class TagAdapter(logging.LoggerAdapter):
    """Logger adapter that prefixes messages with a fixed tag."""

    def process(self, msg, kwargs):
        return f"{self.extra['tag']} {msg}", kwargs


class TagFilter(logging.Filter):
    """Filter for multi-tag log matching."""

    def __init__(self, tags):
        super().__init__()
        if isinstance(tags, str):
            self.tags = [t.strip() for t in tags.split(",") if t.strip()]
        elif tags is None:
            self.tags = []
        else:
            self.tags = tags

    def filter(self, record):
        if not self.tags:
            return True
        message = record.getMessage()
        return any(tag in message for tag in self.tags)


def setup_logger(
    file_path=os.getenv("ALLUXIO_PYTHON_SDK_LOG_DIR", None),
    level_str=os.getenv("ALLUXIO_PYTHON_SDK_LOG_LEVEL", "INFO"),
    class_name=__name__,
    log_tags=None,
):
    level = LOG_LEVEL_MAP.get(level_str.upper(), logging.INFO)
    handlers = []

    console_handler = logging.StreamHandler()
    handlers.append(console_handler)

    if file_path:
        file_name = "user.log"
        if not os.path.exists(file_path):
            os.makedirs(file_path, exist_ok=True)
        log_file = os.path.join(file_path, file_name)
        file_handler = logging.FileHandler(log_file)
        handlers.append(file_handler)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    tag_filter = TagFilter(log_tags) if log_tags else None

    for handler in handlers:
        handler.setFormatter(formatter)
        handler.setLevel(level)
        if tag_filter:
            handler.addFilter(tag_filter)

    logger = logging.getLogger(class_name)
    logger.propagate = False
    if logger.hasHandlers():
        logger.handlers.clear()

    for handler in handlers:
        logger.addHandler(handler)

    logger.setLevel(level)
    return logger
