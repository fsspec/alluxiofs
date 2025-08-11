import hashlib

import fsspec
import pytest

from alluxiofs import AlluxioFileSystem

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
alluxio_fs = fsspec.filesystem(
    "alluxiofs",
    etcd_hosts="localhost",
    etcd_port=2379,
    # target_options=oss_options,
    target_protocol="s3",
    page_size="1MB",
)

bucket_name = "yxd-fsspec"
test_folder_name = "python_sdk_test"
# the format of home_path: s3://{bucket_name}/{test_folder_name}
home_path = "s3://" + bucket_name + "/" + test_folder_name

file_names = [
    "random1MB.txt",
    "random10MB.txt",
    "random100MB.txt",
    "random1GB.txt",
    # "random10GB.txt"
]


@pytest.mark.skip(reason="no-mock test")
def read_test_disabled(file_name="test.csv"):
    file_path_ufs = home_path + "/" + file_name
    file_path_local = "../assets/" + file_name

    # # init
    if alluxio_fs.exists(home_path):
        alluxio_fs.rm(home_path, recursive=True)

    assert alluxio_fs.mkdir(home_path)
    assert alluxio_fs.touch(file_path_ufs)

    assert alluxio_fs.upload(lpath=file_path_ufs, rpath=file_path_local)

    md5_local = hashlib.md5()
    md5_remote_page = hashlib.md5()
    md5_remote_range = hashlib.md5()
    md5_remote_chunk = hashlib.md5()

    with open(file_path_local, "rb") as f:
        md5_local.update(f.read())

    # # three ways to read data from alluxio
    data_page = alluxio_fs.read(file_path_ufs)
    data_range = alluxio_fs.alluxio.read_file_range(file_path_ufs)
    data_chunk = alluxio_fs.download_data(file_path_ufs).read()

    md5_remote_page.update(data_page)
    md5_remote_range.update(data_range)
    md5_remote_chunk.update(data_chunk)

    assert md5_local.hexdigest() == md5_remote_page.hexdigest()
    assert md5_local.hexdigest() == md5_remote_range.hexdigest()
    assert md5_local.hexdigest() == md5_remote_chunk.hexdigest()


# for file_name in file_names:
#     print(file_name)
#     read_test_disabled(file_name)
read_test_disabled()
