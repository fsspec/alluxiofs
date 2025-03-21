import argparse
import os
import random
import string

import fsspec

from alluxiofs import AlluxioFileSystem

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)

generate_data_chunk_size = 2 * 1024 * 1024 * 1024


def parse_args():
    parser = argparse.ArgumentParser(
        description="Preprocess files for benchmark"
    )
    parser.add_argument(
        "--etcd_hosts",
        type=str,
        required=False,
        help="The host address(es) for etcd",
    )
    parser.add_argument(
        "--etcd_port",
        type=int,
        required=False,
        help="The port for etcd",
    )
    parser.add_argument(
        "--cluster_name",
        type=str,
        required=False,
        help="The name of the cluster of alluxio",
    )
    parser.add_argument(
        "--target_protocol",
        type=str,
        required=False,
        help="The target's protocol of UFS",
    )
    parser.add_argument(
        "--worker_hosts",
        type=str,
        required=False,
        help="The host address(es) for etcd",
    )
    parser.add_argument(
        "--op",
        type=str,
        choices=["read", "write", "read_batch"],
        required=True,
        help="Operation type: read, write or read_batch",
    )
    parser.add_argument(
        "--size",
        nargs="+",
        required=True,
        help="List of file sizes to create (e.g., 1MB 10MB 100MB)",
    )
    parser.add_argument(
        "--number",
        type=str,
        required=False,
        help="The number of files to generate",
    )
    # parser.add_argument("--file_size", type=str, required=False, help="file size")
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="Path where files will be writen to in remote",
    )
    parser.add_argument(
        "--local_path",
        type=str,
        required=False,
        help="Path where files will be writen from in local",
    )
    return parser.parse_args()


def generate_random_data(size):
    num_bytes = convert_size_to_bytes(size)
    while num_bytes > 0:
        chunk_size = min(generate_data_chunk_size, num_bytes)
        yield "".join(
            random.choices(string.ascii_letters + string.digits, k=chunk_size)
        ).encode("utf-8")
        num_bytes -= chunk_size


def convert_size_to_bytes(size_str):
    size_map = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
    }
    size, unit = size_str[:-2], size_str[-2:]
    return int(size) * size_map.get(unit.upper(), 1)


def create_and_upload_files_read(alluxio_fs, path, size):
    print(f"Creating and uploading file of size {size} to {path}")
    file_path = os.path.join(path, size)
    if not alluxio_fs.exists(path):
        alluxio_fs.mkdir(path)
    if not alluxio_fs.exists(file_path):
        alluxio_fs.touch(file_path)
        for data_chunk in generate_random_data(size):
            assert alluxio_fs.upload_data(path=file_path, data=data_chunk)
    print(f"File {file_path} created successfully.")


def create_files_write(alluxio_fs, path, local_path, size):
    print(f"Creating local file of size {size} at {local_path}")

    file_path = os.path.join(local_path, size)
    if not os.path.exists(file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "ab") as f:
            for data_chunk in generate_random_data(size):
                f.write(data_chunk)

    if not alluxio_fs.exists(path):
        alluxio_fs.mkdir(path)
    file_path = os.path.join(path, size)
    if not alluxio_fs.exists(file_path):
        alluxio_fs.touch(file_path)

    print(f"File {file_path} created successfully.")


def create_files_read_batch(alluxio_fs, path, local_path, file_size, number):
    if not alluxio_fs.exists(path):
        alluxio_fs.mkdir(path)
    for i in range(number):
        file_path_fuse = os.path.join(local_path, str(i))
        if not os.path.exists(file_path_fuse):
            os.makedirs(os.path.dirname(file_path_fuse), exist_ok=True)
        with open(file_path_fuse, "ab") as f:
            for data_chunk in generate_random_data(file_size):
                f.write(data_chunk)

        file_path_fsspec = os.path.join(path, str(i))
        if not alluxio_fs.exists(file_path_fsspec):
            alluxio_fs.touch(file_path_fsspec)
        for data_chunk in generate_random_data(file_size):
            alluxio_fs.upload_data(path=file_path_fsspec, data=data_chunk)


def main():
    args = parse_args()

    size_list = args.size
    alluxio_args_dict = {}
    if args.etcd_hosts is None:
        alluxio_args_dict["etcd_hosts"] = "localhost"
    else:
        alluxio_args_dict["etcd_hosts"] = args.etcd_hosts

    if args.etcd_port is None:
        alluxio_args_dict["etcd_port"] = 2379
    else:
        alluxio_args_dict["etcd_port"] = args.etcd_port

    if args.cluster_name is not None:
        alluxio_args_dict["cluster_name"] = args.cluster_name
    if args.target_protocol is not None:
        alluxio_args_dict["target_protocol"] = args.target_protocol
    if args.worker_hosts is not None:
        alluxio_args_dict["worker_hosts"] = args.worker_hosts

    alluxio_fs = AlluxioFileSystem(**alluxio_args_dict)

    if args.op == "read":
        for s in size_list:
            create_and_upload_files_read(alluxio_fs, args.path, s)

    elif args.op == "write":
        for s in size_list:
            create_files_write(alluxio_fs, args.path, args.local_path, s)

    elif args.op == "read_batch":
        for s in size_list:
            create_files_read_batch(
                alluxio_fs, args.path, args.local_path, s, int(args.number)
            )


if __name__ == "__main__":
    main()
