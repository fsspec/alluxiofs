import os
import random
from hashlib import md5

from alluxiofs import AlluxioClient
from tests.conftest import ALLUXIO_FILE_PATH
from tests.conftest import LOCAL_FILE_PATH

NUM_TESTS = 10

import logging

LOGGER = logging.getLogger(__name__)


def _get_md5(payload):
    m = md5()
    m.update(payload)
    m.update(payload)
    return m.hexdigest()


def validate_read_range(
    alluxio_client: AlluxioClient,
    alluxio_file_path,
    local_file_path,
    offset,
    length,
):
    alluxio_data = alluxio_client.read_range(alluxio_file_path, offset, length)

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


def validate_invalid_read_range(
    alluxio_fs, alluxio_file_path, local_file_path, offset, length
):
    try:
        alluxio_fs.read_range(alluxio_file_path, offset, length)
    except Exception:
        pass
    else:
        raise AssertionError(
            "Expected an exception from Alluxio but none occurred."
        )

    try:
        with open(local_file_path, "rb") as local_file:
            local_file.seek(offset)
            local_file.read(length)
    except Exception:
        pass
    else:
        raise AssertionError(
            "Expected an exception from local file read but none occurred."
        )


def validate_full_read(
    alluxio_client: AlluxioClient,
    alluxio_file_path,
    local_file_path,
):
    alluxio_data = alluxio_client.read(alluxio_file_path)

    with open(local_file_path, "rb") as local_file:
        local_data = local_file.read()

    try:
        assert alluxio_data == local_data
    except AssertionError:
        md5_alluxio_data = _get_md5(alluxio_data)
        md5_local_data = _get_md5(local_data)
        error_message = (
            f"Data mismatch between Alluxio and local file\n"
            f"Alluxio file path: {alluxio_file_path}\n"
            f"Local file path: {local_file_path}\n"
            f"Alluxio data md5: {md5_alluxio_data}, length:{len(alluxio_data)}\n"
            f"Local data md5: {md5_local_data}, length:{len(local_data)}"
        )
        raise AssertionError(error_message)


def _test_alluxio_client(alluxio_client: AlluxioClient):
    file_size = os.path.getsize(LOCAL_FILE_PATH)
    assert alluxio_client.load(ALLUXIO_FILE_PATH, 200)
    invalid_test_cases = [(-1, 100), (file_size - 1, -2)]
    for offset, length in invalid_test_cases:
        validate_invalid_read_range(
            alluxio_client,
            ALLUXIO_FILE_PATH,
            LOCAL_FILE_PATH,
            offset,
            length,
        )
    LOGGER.debug("Passed invalid test cases")

    # Validate normal case
    max_length = 13 * 1024
    for _ in range(NUM_TESTS):
        offset = random.randint(0, file_size - 1)
        length = min(random.randint(1, file_size - offset), max_length)
        validate_read_range(
            alluxio_client,
            ALLUXIO_FILE_PATH,
            LOCAL_FILE_PATH,
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
            alluxio_client,
            ALLUXIO_FILE_PATH,
            LOCAL_FILE_PATH,
            offset,
            length,
        )
    LOGGER.debug("Passed corner test cases")

    # test full data read

    validate_full_read(alluxio_client, ALLUXIO_FILE_PATH, LOCAL_FILE_PATH)


def test_etcd_alluxio_client(etcd_alluxio_client: AlluxioClient):
    _test_alluxio_client(etcd_alluxio_client)


def test_alluxio_client_alluxiocommon(
    alluxio_client_alluxiocommon: AlluxioClient,
):
    _test_alluxio_client(alluxio_client_alluxiocommon)
