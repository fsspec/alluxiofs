import logging
import os
import re

from alluxiofs import AlluxioFileSystem


def replace_protocol_with_alluxiofs(file_path):
    protocol_pattern = r"^\w+://"
    return re.sub(
        protocol_pattern, AlluxioFileSystem.protocol + "://", file_path
    )


def configure_logging(dir, name="alluxiofs", log_level=logging.INFO):
    log_path = os.path.join(dir, name + ".log")
    logging.basicConfig(
        filename=log_path,
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logger = logging.getLogger(name)
    return logger
