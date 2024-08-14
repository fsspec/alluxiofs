import builtins
import os
import shutil

import fsspec
import yaml

from alluxiofs import AlluxioFileSystem

# Save the original python methods before replacing them
# Original python methods will be used when the file path doesn't start with supported protocols such as s3 and hdfs
original_open = builtins.open
original_ls = os.listdir
original_mkdir = os.mkdir
original_rmdir = os.rmdir
original_listdir = os.listdir
original_rename = os.rename
original_remove = os.remove
original_copy = shutil.copy
original_stat = os.stat
original_isdir = os.path.isdir
original_isfile = os.path.isfile
original_exists = os.path.exists
original_walk = os.walk

file_systems = {}
supported_file_paths = {"s3", "hdfs"}
user_config_file_path = ""


def set_user_config_file_path(file_path):
    global user_config_file_path
    user_config_file_path = file_path


def initialize_alluxio_file_systems(protocol):
    global file_systems
    global user_config_file_path

    # load user's alluxiofs configurations
    with open(user_config_file_path, "r") as file:
        config = yaml.safe_load(file)

    if config is None:
        config = {}

    # Register the Alluxio file system with fsspec for the given protocol
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    try:
        # Initialize the file system with the provided configuration
        file_systems[protocol] = fsspec.filesystem(
            "alluxiofs", target_protocol=protocol, **config
        )
    except Exception as e:
        print(
            f"Failed to initialize the Alluxio file system for protocol '{protocol}': {e}"
        )
        return None


def get_alluxio_fs(file):
    file_path_prefix = file.split("://")[0]
    if file_path_prefix in supported_file_paths:
        if file_path_prefix not in file_systems:
            initialize_alluxio_file_systems(file_path_prefix)
        return file_systems[file_path_prefix]
    return None


def alluxio_open(
    file,
    mode="r",
    buffering=-1,
    encoding=None,
    errors=None,
    newline=None,
    closefd=True,
    opener=None,
    **kwargs,
):
    alluxio_fs = get_alluxio_fs(file)
    if alluxio_fs:
        return alluxio_fs.open(
            file,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
            **kwargs,
        )

    else:
        # For other paths, use the original built-in open function
        return original_open(
            file,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
        )


def alluxio_ls(path, **kwargs):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        return alluxio_fs.ls(path, **kwargs)

    else:
        return original_ls(path)


def alluxio_mkdir(path, mode=0o777, **kwargs):
    alluxio_fs = get_alluxio_fs(path)

    if alluxio_fs:
        # s3 mkdir only create buckets, not directory in buckets. It accepts bucket name without s3:// prefix.
        file_path_prefix = path.split("://")[0]
        if file_path_prefix == "s3":
            bucket_name = path.split("://")[1]
            return alluxio_fs.mkdir(bucket_name, **kwargs)
        else:
            return alluxio_fs.mkdir(path)
    else:
        return original_mkdir(path, mode, **kwargs)


def alluxio_rmdir(path, **kwargs):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        # s3 rmdir only create buckets, not directory in buckets. It accepts bucket name without s3:// prefix.
        file_path_prefix = path.split("://")[0]
        if file_path_prefix == "s3":
            bucket_name = path.split("://")[1]
            return alluxio_fs.rmdir(bucket_name, **kwargs)
    else:
        return original_rmdir(path, **kwargs)


def alluxio_rename(src, dest, **kwargs):
    alluxio_fs = get_alluxio_fs(src)
    if alluxio_fs:
        alluxio_fs.mv(src, dest, **kwargs)
    else:
        original_rename(src, dest, **kwargs)


def alluxio_remove(path):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        alluxio_fs.rm_file(path)
    else:
        original_remove(path)


def alluxio_copy(src, dst, **kwargs):
    alluxio_fs = get_alluxio_fs(src)
    if alluxio_fs:
        alluxio_fs.copy(src, dst, **kwargs)
    else:
        original_copy(src, dst, **kwargs)


def alluxio_stat(path, **kwargs):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        return alluxio_fs.info(path, **kwargs)
    else:
        return original_stat(path, **kwargs)


def alluxio_isdir(path, **kwargs):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        return alluxio_fs.isdir(path, **kwargs)
    else:
        return original_isdir(path)


def alluxio_isfile(path):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        return alluxio_fs.isfile(path)
    else:
        return original_isfile(path)


def alluxio_exists(path, **kwargs):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        return alluxio_fs.exists(path, **kwargs)
    else:
        return original_exists(path)


def alluxio_walk(
    path, topdown=True, onerror=None, followlinks=False, **kwargs
):
    alluxio_fs = get_alluxio_fs(path)
    if alluxio_fs:
        return alluxio_fs.walk(path, **kwargs)
    else:
        return original_walk(path, topdown, onerror, followlinks)
