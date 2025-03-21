import argparse
import os
import shutil
import time

from alluxiofs import AlluxioFileSystem


def parse_args():
    parser = argparse.ArgumentParser(
        description="Postprocess and clean up files for benchmark"
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
        "--path",
        type=str,
        required=False,
        help="Path where files will be cleaned from in remote",
    )
    parser.add_argument(
        "--local_path",
        type=str,
        required=False,
        help="Path where files will be cleaned from in local",
    )
    return parser.parse_args()


def clean_up_files(alluxio_fs, path):
    if alluxio_fs.exists(path):
        print(f"Cleaning up files in {path}")
        alluxio_fs.rm(path, recursive=True)
        print(f"Files at {path} cleaned up successfully.")
    else:
        print(f"No files found at {path} to clean up.")


def clean_local_files(local_path):
    if os.path.exists(local_path):
        print(f"Cleaning up local files at {local_path}")
        try:
            shutil.rmtree(local_path)
        except OSError:
            # Retry after delay if the error is due to file being used
            time.sleep(5)
            shutil.rmtree(local_path)
        print(f"Local files at {local_path} cleaned up successfully.")
    else:
        print(f"No local files found at {local_path} to clean up.")


def main():
    args = parse_args()

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

    # Clean up remote files on Alluxio
    if args.path:
        clean_up_files(alluxio_fs, args.path)

    # Clean up local files if a local path was provided
    if args.local_path:
        clean_local_files(args.local_path)


if __name__ == "__main__":
    main()
