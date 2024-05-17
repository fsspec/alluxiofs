import logging
import os
import re

from alluxiofs import AlluxioFileSystem


def use_alluxiofs_protocol(file_path):
    protocol_pattern = r"^\w+://"
    if re.match(protocol_pattern, file_path):
        file_path = re.sub(
            protocol_pattern, AlluxioFileSystem.protocol + "://", file_path
        )
    else:
        file_path = AlluxioFileSystem.protocol + "://" + file_path
    return file_path


def remove_alluxiofs_protocol(file_path):
    return re.sub(r"^\w+://", "", file_path)


def configure_logging(dir, name="alluxiofs", log_level=logging.INFO):
    log_path = os.path.join(dir, name + ".log")
    logging.basicConfig(
        filename=log_path,
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logger = logging.getLogger(name)
    return logger
