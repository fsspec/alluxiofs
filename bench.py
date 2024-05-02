#!/bin/python3
import argparse
import json
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
        "--profile",
        action="store_true",
        default=False,
        required=False,
        help="Whether to use cProfile to profile the benchmark",
    )
    parser.add_argument(
        "--result_dir",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "bench_result"),
        required=False,
        help="The location to store the benchmark result",
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


def runtest(start_time, runtime, test_suite):
    while time.time() - start_time < runtime:
        test_suite.execute()


def main():
    main_parser = init_main_parser()
    main_args, remaining_args = main_parser.parse_known_args()
    os.makedirs(main_args.result_dir, exist_ok=True)
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

            if main_args.profile:
                import cProfile

                profile_result_location = os.path.join(
                    main_args.result_dir, f"worker_{i}_profile_result"
                )
                cProfile.runctx(
                    "runtest(start_time, main_args.runtime, test_suite)",
                    globals(),
                    locals(),
                    filename=profile_result_location,
                )
                print(
                    f"Profile result of worker {i} saved to {profile_result_location}"
                )
            else:
                runtest(start_time, main_args.runtime, test_suite)

            duration = time.time() - start_time
            print(
                f"Benchmark against {test_suite.args.op}: "
                f"total time: {duration} seconds"
            )

            result = {
                "worker": i,
                "op": test_suite.args.op,
                "metrics": {
                    "duration": duration,
                },
            }
            if test_suite.metrics.get(Metrics.TOTAL_OPS):
                total_ops = test_suite.metrics.get(Metrics.TOTAL_OPS)
                ops_per_second = total_ops / duration
                result["metrics"]["total_ops"] = total_ops
                result["metrics"]["ops_per_second"] = ops_per_second
                print(
                    f"total ops: {total_ops}, " f"ops/second: {ops_per_second}"
                )
            if test_suite.metrics.get(Metrics.TOTAL_BYTES):
                total_bytes = test_suite.metrics.get(Metrics.TOTAL_BYTES)
                bytes_per_second = total_bytes / duration
                result["metrics"]["total_bytes"] = total_bytes
                result["metrics"]["bytes_per_second"] = bytes_per_second
                print(
                    f"total bytes: {total_bytes}, "
                    f"bytes/second: {bytes_per_second}"
                )
            json_result_location = os.path.join(
                main_args.result_dir, f"worker_{i}_bench_result.json"
            )
            with open(json_result_location, "w") as f:
                json.dump(result, f)
        else:
            print(f"Parent Process, {i}th Child process, id:{processid}")
    if not i_am_child:
        os.wait()


if __name__ == "__main__":
    main()
