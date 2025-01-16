import logging
import os

from alluxiofs import AlluxioFileSystem
from tests.conftest import ALLUXIO_FILE_PATH
from tests.conftest import LOCAL_FILE_PATH
from tests.fs.test_docker_fsspec_cat import FILE_PATH
from tests.utils import use_alluxiofs_protocol

LOGGER = logging.getLogger(__name__)


def check_file_info(file_info, file_size):
    assert file_info.get("path") == FILE_PATH
    assert file_info.get("type") == "file"
    assert file_info.get("size") == file_size


def alluxio_fsspec_test_file(alluxio_file_system, alluxio_path, local_path):
    file_size = os.path.getsize(local_path)

    file_list = alluxio_file_system.ls(alluxio_path, detail=True)
    assert len(file_list) == 1
    check_file_info(file_list[0], file_size)
    file_info = alluxio_file_system.info(alluxio_path)
    check_file_info(file_info, file_size)
    assert not alluxio_file_system.isdir(alluxio_path)
    assert alluxio_file_system.isfile(alluxio_path)

    with alluxio_file_system.open(alluxio_path) as f:
        alluxio_file_data = f.read()

    with open(local_path, "rb") as local_file:
        local_file_data = local_file.read()
    assert local_file_data == alluxio_file_data


def test_alluxio_fsspec_file_function(alluxio_file_system: AlluxioFileSystem):
    alluxio_fsspec_test_file(
        alluxio_file_system, ALLUXIO_FILE_PATH, LOCAL_FILE_PATH
    )
    alluxio_fsspec_test_file(
        alluxio_file_system,
        use_alluxiofs_protocol(ALLUXIO_FILE_PATH),
        LOCAL_FILE_PATH,
    )
    alluxio_fsspec_test_file(alluxio_file_system, FILE_PATH, LOCAL_FILE_PATH)


def test_etcd_alluxio_fsspec_file_function(
    etcd_alluxio_file_system: AlluxioFileSystem,
):
    test_alluxio_fsspec_file_function(etcd_alluxio_file_system)
