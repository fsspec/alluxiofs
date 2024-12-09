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
def write_test_disabled(file_name="test.csv"):
    file_path_ufs = home_path + "/" + file_name
    file_path_local = "../assets/" + file_name

    # # init
    if alluxio_fs.exists(home_path):
        alluxio_fs.rm(home_path, recursive=True)
    assert alluxio_fs.mkdir(home_path)
    assert alluxio_fs.touch(file_path_ufs)

    # # write file to alluxio
    assert alluxio_fs.upload(lpath=file_path_ufs, rpath=file_path_local)

    # check md5
    md5_local = hashlib.md5()
    sha256_local = hashlib.sha256()
    with open(file_path_local, "rb") as f:
        data = f.read()
        md5_local.update(data)
        sha256_local.update(data)
    file_status = alluxio_fs.info(file_path_ufs)
    md5_remote = file_status["content_hash"]
    if "-" in md5_remote:
        md5_remote_calculated = hashlib.md5()
        data = alluxio_fs.download_data(file_path_ufs).read()
        md5_remote_calculated.update(data)
        md5_remote = md5_remote_calculated.hexdigest()
    assert md5_local.hexdigest() == md5_remote


# for file_name in file_names:
#     print(file_name)
#     write_test_disabled(file_name)
write_test_disabled()
