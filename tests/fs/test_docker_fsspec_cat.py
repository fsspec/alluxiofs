import os
import random

from alluxiofs import AlluxioFileSystem
from tests.conftest import ALLUXIO_FILE_PATH
from tests.conftest import LOCAL_FILE_PATH
from tests.utils import use_alluxiofs_protocol

NUM_TESTS = 10

import logging

LOGGER = logging.getLogger(__name__)

FILE_PATH = "/opt/alluxio/ufs/test.csv"


def validate_read_range(
    alluxio_file_system: AlluxioFileSystem,
    alluxio_file_path,
    local_file_path,
    offset,
    length,
):
    alluxio_data = alluxio_file_system.cat_file(
        alluxio_file_path, offset, offset + length
    )

    with open(local_file_path, "rb") as local_file:
        local_file.seek(offset)
        local_data = local_file.read(length)

    try:
        assert alluxio_data == local_data
    except AssertionError:
        error_message = (
            f"Data mismatch between Alluxio and local file\n"
            f"Alluxio file path: {alluxio_file_path}\n"
            f"Local file path: {local_file_path}\n"
            f"Offset: {offset}\n"
            f"Length: {length}\n"
            f"Alluxio data: {alluxio_data}\n"
            f"Local data: {local_data}"
        )
        raise AssertionError(error_message)


def alluxio_fsspec_cat_file(alluxio_file_system, alluxio_path, local_path):
    file_size = os.path.getsize(local_path)

    # Validate normal case
    max_length = 13 * 1024
    for _ in range(NUM_TESTS):
        offset = random.randint(0, file_size - 1)
        length = min(random.randint(1, file_size - offset), max_length)
        validate_read_range(
            alluxio_file_system,
            alluxio_path,
            local_path,
            offset,
            length,
        )

    LOGGER.debug(
        f"Data matches between Alluxio file and local source file for {NUM_TESTS} times"
    )

    special_test_cases = [
        (file_size - 1, 0),
        (file_size - 2, 1),
        (file_size - 101, 100),
    ]

    for offset, length in special_test_cases:
        validate_read_range(
            alluxio_file_system,
            alluxio_path,
            local_path,
            offset,
            length,
        )
    LOGGER.debug("Passed corner test cases")


def test_alluxio_fsspec_cat_file(alluxio_file_system: AlluxioFileSystem):
    alluxio_fsspec_cat_file(
        alluxio_file_system, ALLUXIO_FILE_PATH, LOCAL_FILE_PATH
    )
    alluxio_fsspec_cat_file(
        alluxio_file_system,
        use_alluxiofs_protocol(ALLUXIO_FILE_PATH),
        LOCAL_FILE_PATH,
    )
    alluxio_fsspec_cat_file(alluxio_file_system, FILE_PATH, LOCAL_FILE_PATH)


def test_etcd_alluxio_fsspec_cat_file(
    etcd_alluxio_file_system: AlluxioFileSystem,
):
    test_alluxio_fsspec_cat_file(etcd_alluxio_file_system)
