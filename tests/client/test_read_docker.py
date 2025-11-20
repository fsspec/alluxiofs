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
    assert alluxio_client.load(ALLUXIO_FILE_PATH, 200)
    LOGGER.debug("Passed invalid test cases")

    # test full data read

    validate_full_read(alluxio_client, ALLUXIO_FILE_PATH, LOCAL_FILE_PATH)


def test_etcd_alluxio_client(etcd_alluxio_client: AlluxioClient):
    _test_alluxio_client(etcd_alluxio_client)


def test_alluxio_client_alluxiocommon(
    alluxio_client_alluxiocommon: AlluxioClient,
):
    _test_alluxio_client(alluxio_client_alluxiocommon)
