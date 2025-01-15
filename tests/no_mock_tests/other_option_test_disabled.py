import json

import fsspec
import pytest

from alluxiofs import AlluxioFileSystem

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
alluxio_fs = fsspec.filesystem(
    "alluxiofs",
    etcd_hosts="localhost",
    etcd_port=2379,
    target_protocol="s3",
    page_size="1MB",
)

bucket_name = "yxd-fsspec"
test_folder_name = "python_sdk_test"
# the format of home_path: s3://{bucket_name}/{test_folder_name}
home_path = "s3://" + bucket_name + "/" + test_folder_name


def show_files(path):
    res = alluxio_fs.ls(path)
    formatted_res = json.dumps(res, indent=4, ensure_ascii=False)
    print(formatted_res)
    print()


def verify_result(num):
    res = alluxio_fs.ls(home_path)
    assert len(res) == num


@pytest.mark.skip(reason="no-mock test")
def other_option_test_disabled():

    # # init
    if alluxio_fs.exists(home_path):
        alluxio_fs.rm(home_path, recursive=True)

    # # mkdir
    res = alluxio_fs.mkdir(home_path)
    assert res

    # # ls
    res = alluxio_fs.ls(home_path)
    assert len(res) == 0

    # # create file
    print("create file python_sdk_test_file")
    res = alluxio_fs.touch(home_path + "/python_sdk_test_file")
    assert res
    show_files(home_path + "/python_sdk_test_file")
    verify_result(1)
    show_files(home_path)

    ## open file
    with alluxio_fs.open(home_path + "/python_sdk_test_file") as f:
        f.read()

    # # load file from ufs to alluxio
    # assert alluxio_fs.load_file_from_ufs_to_alluxio(home_path)

    # # get file status
    res_folder = alluxio_fs.info(home_path)
    assert res_folder and res_folder["type"] == "directory"
    res_file = alluxio_fs.info(home_path + "/python_sdk_test_file")
    print(res_file)

    # # remove file
    print("remove file python_sdk_test_file")
    res = alluxio_fs.rm(home_path + "/python_sdk_test_file", recursive=True)
    assert res
    verify_result(0)
    show_files(home_path)

    # # create folder and file
    print("create python_sdk_test_folder for test")
    assert alluxio_fs.mkdir(home_path + "/python_sdk_test_folder")
    assert alluxio_fs.touch(home_path + "/python_sdk_test_folder/file1")
    assert alluxio_fs.touch(home_path + "/python_sdk_test_folder/file2")
    assert alluxio_fs.touch(home_path + "/python_sdk_test_folder/file3")

    # # exists
    assert alluxio_fs.exists(home_path + "/python_sdk_test_folder")
    assert alluxio_fs.exists(home_path + "/python_sdk_test_folder/file1")
    assert alluxio_fs.exists(home_path + "/python_sdk_test_folder/file2")
    assert alluxio_fs.exists(home_path + "/python_sdk_test_folder/file3")
    show_files(home_path)
    verify_result(1)

    # # upload
    print("upload file for test")
    with open("../assets/test.csv", "rb") as f:
        data = f.read()
        assert alluxio_fs.upload_data(
            path="4564" + home_path + "/python_sdk_test_folder/file3",
            data=data,
        )

    # # move
    print("move file3 to another folder")
    print()
    assert alluxio_fs.mkdir(home_path + "/python_sdk_test_folder2")
    assert alluxio_fs.mv(
        home_path + "/python_sdk_test_folder/file3",
        home_path + "/python_sdk_test_folder2/file3",
    )
    assert alluxio_fs.exists(home_path + "/python_sdk_test_folder2/file3")
    assert not alluxio_fs.exists(home_path + "/python_sdk_test_folder/file3")
    show_files(home_path)

    # # copy
    print("copy file3")
    print()
    assert alluxio_fs.copy(
        home_path + "/python_sdk_test_folder2/file3",
        home_path + "/python_sdk_test_folder/words",
        recursive=True,
    )
    assert alluxio_fs.exists(home_path + "/python_sdk_test_folder/words")
    assert alluxio_fs.exists(home_path + "/python_sdk_test_folder2/file3")
    show_files(home_path)

    # # head and tail
    print("head and tail")
    res_head = alluxio_fs.head(
        path=home_path + "/python_sdk_test_folder/words", num_of_bytes=1024
    )
    res_tail = alluxio_fs.tail(
        path=home_path + "/python_sdk_test_folder/words", num_of_bytes=1024
    )
    with open("../assets/test.csv", "rb") as f:
        data = f.read()
        assert res_head == data[:1024]
        assert res_tail == data[-1024:]

    # # clear all
    alluxio_fs.rm(home_path, recursive=True)
    assert not alluxio_fs.exists(home_path)
    show_files(home_path)


# if __name__ == '__main__':
for i in range(1):
    other_option_test_disabled()
