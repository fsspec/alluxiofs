import re


def replace_protocol_with_alluxio(file_path):
    protocol_pattern = r"^\w+://"
    return re.sub(protocol_pattern, "alluxio://", file_path)
