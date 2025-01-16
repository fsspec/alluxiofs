import logging
import os

from alluxiofs import AlluxioFileSystem
from tests.conftest import LOCAL_FILE_PATH
from tests.fs.test_docker_fsspec_file_function import check_file_info
from tests.utils import use_alluxiofs_protocol

LOGGER = logging.getLogger(__name__)

DIR_PATH = "/opt/alluxio/ufs"
SUB_DIR_PATH = "/opt/alluxio/ufs/hash_res"
FILE_PREFIX = "file://"


def check_dir_info(dir_info, dir_path):
    assert dir_info.get("path") == dir_path
    assert dir_info.get("type") == "directory"
    assert not dir_info.get("size")


def alluxio_fsspec_test_dir(alluxio_file_system, alluxio_dir_path):
    file_size = os.path.getsize(LOCAL_FILE_PATH)

    file_list = alluxio_file_system.ls(alluxio_dir_path, detail=True)
    assert len(file_list) == 2
    if file_list[0].get("type") == "file":
        check_file_info(file_list[0], file_size)
        check_dir_info(file_list[1], SUB_DIR_PATH)
    elif file_list[0].get("type") == "directory":
        check_dir_info(file_list[0], SUB_DIR_PATH)
        check_file_info(file_list[1], file_size)
    else:
        raise AssertionError("File type is invalid.")
    dir_info = alluxio_file_system.info(alluxio_dir_path)
    check_dir_info(dir_info, DIR_PATH)
    assert alluxio_file_system.isdir(alluxio_dir_path)
    assert not alluxio_file_system.isfile(alluxio_dir_path)


def test_alluxio_fsspec_dir_function(alluxio_file_system: AlluxioFileSystem):
    alluxio_fsspec_test_dir(alluxio_file_system, DIR_PATH)
    alluxio_fsspec_test_dir(alluxio_file_system, FILE_PREFIX + DIR_PATH)
    alluxio_fsspec_test_dir(
        alluxio_file_system, use_alluxiofs_protocol(DIR_PATH)
    )


def test_etcd_alluxio_fsspec_dir_function(
    etcd_alluxio_file_system: AlluxioFileSystem,
):
    test_alluxio_fsspec_dir_function(etcd_alluxio_file_system)
