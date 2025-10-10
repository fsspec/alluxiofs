import hashlib

import fsspec
import pytest

from alluxiofs import AlluxioFileSystem

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
alluxio_fs = fsspec.filesystem(
    "alluxiofs",
    worker_hosts="127.0.0.1:28080",
)

bucket_name = "yxd-fsspec"
test_folder_name = "python_sdk_test"
# the format of home_path: s3://{bucket_name}/{test_folder_name}
# home_path = "s3://" + bucket_name + "/" + test_folder_name
home_path = "file:///home/yxd/alluxio/ufs/" + test_folder_name

file_names = [
    # "random1MB.txt",
    "random10MB.txt",
    # "random100MB.txt",
    # "random1GB.txt",
    # "random10GB.txt"
]
CHUNK_SIZE = 8 * 1024 * 1024

@pytest.mark.skip(reason="no-mock test")
def write_test_disabled(file_name="test.csv"):
    file_path_ufs = home_path + "/" + file_name
    file_path_local = "../assets/" + file_name

    # # init
    # if alluxio_fs.exists(home_path):
    #     alluxio_fs.rm(home_path, recursive=True)
    # assert alluxio_fs.mkdir(home_path)
    # assert alluxio_fs.touch(file_path_ufs)
    #
    # # # write file to alluxio
    # with open(file_path_local, "rb") as f:
    #     with alluxio_fs.open(file_path_ufs, "wb") as wf:
    #         while True:
    #             chunk = f.read(CHUNK_SIZE)
    #             if not chunk:
    #                 break
    #             wf.write(chunk)

    # check md5
    sha256_local = hashlib.sha256()
    with open(file_path_local, "rb") as f:
        data = f.read()
        sha256_local.update(data)
    sha256_remote = hashlib.sha256()
    data = alluxio_fs.download_data(file_path_ufs).read()
    sha256_remote.update(data)
    print(sha256_local.hexdigest())
    print(sha256_remote.hexdigest())
    assert sha256_local.hexdigest() == sha256_remote.hexdigest()


for file_name in file_names:
    print(file_name)
    write_test_disabled(file_name)
# write_test_disabled()
