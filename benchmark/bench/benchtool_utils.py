import re

SIZES = {
    "KB": 1024,
    "MB": 1024 * 1024,
    "GB": 1024 * 1024 * 1024,
}


def parse_bytes_str(size_str: str) -> int:
    match = re.search(r"\d+\s*(MB|KB|GB|mb|kb|gb)?", size_str)
    if not match:
        raise ValueError("Malformatted bytes string.")
    group0 = match.group(0)
    group1 = match.group(1)
    if not group1 and len(group0) < len(size_str):
        raise ValueError("Malformatted bytes string.")
    if not group1:
        return int(group0)
    return SIZES[group1.upper()] * int(group0)
