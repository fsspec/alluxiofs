import argparse

from alluxiofs.client.const import ALLUXIO_COMMON_EXTENSION_ENABLE
from alluxiofs.client.const import ALLUXIO_COMMON_ONDEMANDPOOL_DISABLE


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate Alluxio read_range with local file."
    )
    parser.add_argument(
        "--alluxio_file_path",
        default="s3://ai-ref-arch/small-dataset/iris.csv",
        required=False,
        help="The Alluxio file path to read",
    )
    parser.add_argument(
        "--local_file_path",
        default="/Users/alluxio/Downloads/iris.csv",
        required=False,
        help="The local file path to validate against",
    )
    parser.add_argument(
        "--etcd_hosts",
        type=str,
        default="localhost",
        required=False,
        help="The host address(es) for etcd",
    )
    parser.add_argument(
        "--num_tests",
        type=int,
        default=100,
        required=False,
        help="The total number of read range test to run",
    )
    parser.add_argument(
        "--enable-alluxiocommon",
        type=bool,
        default=False,
        help="To enable alluxiocommon extension",
    )
    parser.add_argument(
        "--disable-alluxiocommon-ondemandpool",
        type=bool,
        default=False,
        help="To disable alluxiocommon ondemand pool, "
        "effective when --enable-alluxiocommon is enabeld",
    )
    return parser.parse_args()


def main(args):
    options = {}
    if args.enable_alluxiocommon:
        options[ALLUXIO_COMMON_EXTENSION_ENABLE] = "True"
    if args.disable_alluxiocommon_ondemandpool:
        options[ALLUXIO_COMMON_ONDEMANDPOOL_DISABLE] = "True"


if __name__ == "__main__":
    args = parse_args()
    main(args)
