import argparse
import time

import fsspec
import humanfriendly

from alluxiofs import AlluxioFileSystem


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmarking script for reading files with Alluxio"
    )
    parser.add_argument(
        "--etcd_hosts",
        type=str,
        default="localhost",
        required=False,
        help="The host addresses for etcd",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="s3://ai-ref-arch/imagenet-mini/val",
        required=False,
        help="The path to the dataset in Alluxio",
    )
    return parser.parse_args()


def process_path(alluxio, path, metrics):
    if path["type"] == "file":
        metrics["total_files"] += 1

        file_read_start = time.time()
        with alluxio.open(path["name"], "rb") as f:
            data = f.read()
            metrics["total_bytes"] += len(data)
        file_read_end = time.time()

        metrics["read_time"] += file_read_end - file_read_start
    elif path["type"] == "directory":
        contents = alluxio.ls(path["name"], detail=True)
        for item in contents:
            process_path(alluxio, item, metrics)


def main(args):
    start_time = time.time()

    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    alluxio = fsspec.filesystem(
        "alluxiofs", etcd_hosts=args.etcd_hosts, target_protocol="s3"
    )

    metrics = {
        "total_files": 0,
        "read_time": 0,
        "total_bytes": 0,
    }

    initial_path_contents = alluxio.ls(args.dataset, detail=True)
    for path in initial_path_contents:
        process_path(alluxio, path, metrics)

    end_time = time.time()

    print(f"Total files processed: {metrics['total_files']}")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")
    print(f"Total file read time: {metrics['read_time']:.2f} seconds")
    print(
        f"Total bytes read: {humanfriendly.format_size(metrics['total_bytes'], binary=True)}"
    )

    if metrics["read_time"] > 0:
        images_per_second = metrics["total_files"] / metrics["read_time"]
        print(f"Images per second: {images_per_second:.2f}")
        throughput = metrics["total_bytes"] / metrics["read_time"]
        print(
            f"Throughput: {humanfriendly.format_size(throughput, binary=True)} per second"
        )


if __name__ == "__main__":
    args = parse_args()
    main(args)
