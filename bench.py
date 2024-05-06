#!/bin/python3
import argparse
import os
import time
from enum import Enum

from benchmark.AbstractBench import Metrics
from benchmark.bench import AlluxioFSSpecBench
from benchmark.bench import AlluxioFSSpecTrafficBench
from benchmark.bench import AlluxioRESTBench
from benchmark.bench import RayBench


class TestSuite(Enum):
    REST = "REST"
    FSSPEC = "FSSPEC"
    FSSPEC_TRAFFIC = "FSSPEC_TRAFFIC"
    RAY = "RAY"


def init_main_parser():
    parser = argparse.ArgumentParser(description="Main parser")
    parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="dataset dir uri, e.g. s3://air-example-data-2/10G-xgboost-data.parquet/",
    )

    parser.add_argument(
        "--numjobs",
        type=int,
        default=1,
        required=True,
        help="Num of bench jobs(python processes) to spawn",
    )
    parser.add_argument(
        "--testsuite",
        choices=[ts.value for ts in TestSuite],
        default=TestSuite.REST.name,
        required=True,
        help="The test suite name, choices:{}".format(list(TestSuite)),
    )
    parser.add_argument(
        "--runtime",
        type=int,
        required=True,
        help="run time in seconds",
    )
    parser.add_argument(
        "--etcd_hosts",
        type=str,
        required=False,
        help="The host address(es) for etcd",
    )
    parser.add_argument(
        "--worker_hosts",
        type=str,
        required=False,
        help="The host address(es) for etcd",
    )
    parser.add_argument(
        "--use-alluxiocommon",
        action="store_true",
        default=False,
        help="Whether to use AlluxioCommon native extensions.",
    )
    parser.add_argument(
        "--page-size",
        type=str,
        default=False,
        help="Size in bytes, or in KB,MB",
    )
    return parser


def get_test_suite(main_parser, main_args, process_id, num_process):
    if main_args.testsuite == TestSuite.REST.name:
        suite_parser = AlluxioRESTBench.AlluxioRESTArgumentParser(main_parser)
        testsuite = AlluxioRESTBench.AlluxioRESTBench(
            process_id, num_process, suite_parser.parse_args()
        )
    elif main_args.testsuite == TestSuite.FSSPEC.name:
        suite_parser = AlluxioFSSpecBench.AlluxioFSSpecArgumentParser(
            main_parser
        )
        testsuite = AlluxioFSSpecBench.AlluxioFSSpecBench(
            process_id, num_process, suite_parser.parse_args()
        )
    elif main_args.testsuite == TestSuite.FSSPEC_TRAFFIC.name:
        suite_parser = (
            AlluxioFSSpecTrafficBench.AlluxioFSSpecTrafficArgumentParser(
                main_parser
            )
        )
        testsuite = AlluxioFSSpecTrafficBench.AlluxioFSSpecTrafficBench(
            process_id, num_process, suite_parser.parse_args()
        )
    elif main_args.testsuite == TestSuite.RAY.name:
        suite_parser = RayBench.RayArgumentParser(main_parser)
        testsuite = RayBench.RayBench(
            process_id, num_process, suite_parser.parse_args()
        )
    else:
        raise ValueError("No test suite specified, bail.")
    return testsuite


def main():
    main_parser = init_main_parser()
    main_args, remaining_args = main_parser.parse_known_args()
    i_am_child = False
    for i in range(main_args.numjobs):
        processid = os.fork()
        if processid <= 0:
            i_am_child = True
            print(f"Child Process:{i}")
            test_suite = get_test_suite(
                main_parser, main_args, i, main_args.numjobs
            )
            test_suite.init()
            start_time = time.time()
            while time.time() - start_time < main_args.runtime:
                test_suite.execute()
            duration = time.time() - start_time
            if test_suite.metrics.get(Metrics.TOTAL_OPS):
                print(
                    f"Benchmark against {test_suite.args.op}: "
                    f"total ops: {test_suite.metrics.get(Metrics.TOTAL_OPS)}, "
                    f"ops/second: {test_suite.metrics.get(Metrics.TOTAL_OPS) / duration}"
                )
            if test_suite.metrics.get(Metrics.TOTAL_BYTES):
                print(
                    f"Benchmark against {test_suite.args.op}: "
                    f"total bytes: {test_suite.metrics.get(Metrics.TOTAL_BYTES)}, "
                    f"bytes/second: {test_suite.metrics.get(Metrics.TOTAL_BYTES) / duration}"
                )

            if not test_suite.metrics:
                print(
                    f"Benchmark against {test_suite.args.op}: total time: {duration} seconds"
                )
            print(f"Child Process:{i} exit")
            break
        else:
            print(f"Parent Process, {i}th Child process, id:{processid}")
    if not i_am_child:
        os.wait()


if __name__ == "__main__":
    main()
