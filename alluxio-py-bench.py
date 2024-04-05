#!/bin/python3
import argparse
import os
import time
from enum import Enum

from tests.benchmark import AbstractBench
from tests.benchmark import AlluxioFSSpecBench
from tests.benchmark import AlluxioRESTBench
from tests.benchmark import RayBench
from tests.benchmark.AbstractBench import Metrics


class TestSuite(Enum):
    REST = "REST"
    FSSPEC = "FSSPEC"
    RAY = "ALLUXIORAY"
    PYTORCH = "PYTORCH"


def init_main_parser():
    parser = argparse.ArgumentParser(description="Main parser")
    parser.add_argument(
        "--numjobs",
        type=int,
        default=1,
        required=True,
        help="Num of bench jobs(python processes) to spawn",
    )
    parser.add_argument(
        "--testsuite",
        type=str,
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
    return parser


def main(main_args, test_suite=AbstractBench):
    if not test_suite:
        print("No test suite specified, bail.")
        return
    test_suite.init()
    start_time = time.time()
    i_am_child = False
    for i in range(main_args.numjobs):
        processid = os.fork()
        if processid <= 0:
            i_am_child = True
            print(f"Child Process:{i}")
            before_time = time.time()
            while time.time() - start_time < main_args.runtime:
                result_metrics = test_suite.execute()
            duration = time.time() - before_time
            if result_metrics.get(Metrics.TOTAL_OPS):
                print(
                    f"Benchmark against {test_suite.args.op}: "
                    f"total ops: {result_metrics.get(Metrics.TOTAL_OPS)}, "
                    f"ops/second: {result_metrics.get(Metrics.TOTAL_OPS) / duration}"
                )
            if result_metrics.get(Metrics.TOTAL_BYTES):
                print(
                    f"Benchmark against {test_suite.args.op}: "
                    f"total bytes: {result_metrics.get(Metrics.TOTAL_BYTES)}, "
                    f"bytes/second: {result_metrics.get(Metrics.TOTAL_BYTES) / duration}"
                )

            if not result_metrics:
                print(
                    f"Benchmark against {test_suite.args.op}: iteration: {test_suite.args.iteration} total time: {duration} seconds"
                )
            print(f"Child Process:{i} exit")
            break
        else:
            print(f"Parent Process, {i}th Child process, id:{processid}")
    if not i_am_child:
        os.wait()
        # end_time = time.time()
        # print(f"total time:{end_time-start_time}")


if __name__ == "__main__":
    main_parser = init_main_parser()
    main_args, remaining_args = main_parser.parse_known_args()
    if main_args.testsuite == TestSuite.REST.name:
        suite_parser = AlluxioRESTBench.AlluxioRESTArgumentParser(main_parser)
        testsuite = AlluxioRESTBench.AlluxioRESTBench(
            suite_parser.parse_args()
        )
    elif main_args.testsuite == TestSuite.FSSPEC.name:
        suite_parser = AlluxioFSSpecBench.AlluxioFSSpecArgumentParser(
            main_parser
        )
        testsuite = AlluxioFSSpecBench.AlluxioFSSpecBench(
            suite_parser.parse_args()
        )
    elif main_args.testsuite == TestSuite.RAY.name:
        suite_parser = RayBench.RayArgumentParser(main_parser)
        testsuite = RayBench.RayBench(suite_parser.parse_args())
    main(main_args, testsuite)
