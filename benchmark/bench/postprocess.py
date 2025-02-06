import argparse
import os
import shutil
import time

import fsspec

from alluxiofs import AlluxioFileSystem


def parse_args():
    parser = argparse.ArgumentParser(
        description="Postprocess and clean up files for benchmark"
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

    # Assuming alluxio is configured to use fsspec
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    alluxio_fs = fsspec.filesystem(
        "alluxiofs", etcd_hosts="localhost", etcd_port=2379
    )

    # Clean up remote files on Alluxio
    if args.path:
        clean_up_files(alluxio_fs, args.path)

    # Clean up local files if a local path was provided
    if args.local_path:
        clean_local_files(args.local_path)


if __name__ == "__main__":
    main()
