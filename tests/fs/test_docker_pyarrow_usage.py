import logging
import os

from pyarrow.fs import FSSpecHandler
from pyarrow.fs import PyFileSystem

from alluxiofs import AlluxioFileSystem
from tests.conftest import ALLUXIO_FILE_PATH
from tests.conftest import LOCAL_FILE_PATH
from tests.fs.test_docker_fsspec_cat import FILE_PATH
from tests.utils import use_alluxiofs_protocol

LOGGER = logging.getLogger(__name__)


def alluxio_pyarrow_test(py_fs, alluxio_path, local_path):
    file_size = os.path.getsize(local_path)

    file_info = py_fs.get_file_info(alluxio_path)
    assert file_info.is_file
    assert file_info.size == file_size
    assert file_info.path == alluxio_path

    with py_fs.open_input_file(alluxio_path) as f:
        alluxio_file_data = f.read()

    with open(local_path, "rb") as local_file:
        local_file_data = local_file.read()
    assert local_file_data == alluxio_file_data


def test_alluxio_pyarrow(alluxio_file_system: AlluxioFileSystem):
    py_fs = PyFileSystem(FSSpecHandler(alluxio_file_system))

    alluxio_pyarrow_test(py_fs, ALLUXIO_FILE_PATH, LOCAL_FILE_PATH)
    alluxio_pyarrow_test(
        py_fs,
        use_alluxiofs_protocol(ALLUXIO_FILE_PATH),
        LOCAL_FILE_PATH,
    )
    alluxio_pyarrow_test(py_fs, FILE_PATH, LOCAL_FILE_PATH)


def test_etcd_alluxio_pyarrow(
    etcd_alluxio_file_system: AlluxioFileSystem,
):
    test_alluxio_pyarrow(etcd_alluxio_file_system)
