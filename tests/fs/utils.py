import re

from alluxiofs import AlluxioFileSystem


def replace_protocol_with_alluxiofs(file_path):
    protocol_pattern = r"^\w+://"
    return re.sub(
        protocol_pattern, AlluxioFileSystem.protocol + "://", file_path
    )
