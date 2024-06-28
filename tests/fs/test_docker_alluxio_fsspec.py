from tests.conftest import TEST_ROOT


def test_simple_fsspec(alluxio_file_system):
    alluxio_file_system.ls(TEST_ROOT)  # no error


def test_simple_fsspec_alluxiocommon(alluxio_file_system_alluxiocommon):
    alluxio_file_system_alluxiocommon.ls(TEST_ROOT)


def test_simple_etcd_fsspec(etcd_alluxio_file_system):
    etcd_alluxio_file_system.ls(TEST_ROOT)  # no error
