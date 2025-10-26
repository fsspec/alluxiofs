import argparse
import json
from concurrent.futures import ThreadPoolExecutor
import fsspec
import pytest

from alluxiofs import AlluxioFileSystem

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)

alluxio_fs = fsspec.filesystem(
    "alluxiofs",
    load_balance_domain="localhost",
)


parser = argparse.ArgumentParser(description="Alluxio MCAP sequential read performance tester.")
parser.add_argument("--file-path", help="Unique ID for this process.")
args = parser.parse_args()

alluxio_fs.alluxio.read_file_range_normal(args.file_path)