# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#!/bin/python3
import argparse
import json
import logging
import os
import shutil
import time
from enum import Enum
from multiprocessing import Manager
from multiprocessing import Process

from benchmark.AbstractBench import Metrics
from benchmark.bench import AlluxioFSSpecBench
from benchmark.bench import AlluxioFSSpecTrafficBench
from benchmark.bench import AlluxioRESTBench
from benchmark.bench import RayBench

PROFILE_RESULT_FORMAT = "worker_{}_profile_result.prof"
BENCH_RESULT_FORMAT = "worker_{}_bench_result.json"
DURATION_METRIC_KEY = "duration"
TOTAL_OPS_METRIC_KEY = "total_ops"
TOTAL_BYTES_METRIC_KEY = "total_bytes"
OPS_PER_SECOND_METRIC_KEY = "ops_per_second"
BYTES_PER_SECOND_METRIC_KEY = "bytes_per_second"


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
        help="path of test file or dir, e.g. s3://air-example-data-2/10G-xgboost-data.parquet/",
    )

    parser.add_argument(
        "--local_path",
        type=str,
        required=False,
        help="the local path of the file to upload to alluxio, e.g. ./tests/assets/test.csv",
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
        "--use-alluxiocommon",
        action="store_true",
        default=False,
        help="Whether to use AlluxioCommon native extensions.",
    )
    parser.add_argument(
        "--page-size",
        type=str,
        default=False,
        help="Size in KB or MB",
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


def create_empty_dir(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    os.makedirs(path, exist_ok=True)


def configure_logging(path):
    log_path = os.path.join(path, "bench.log")
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logger = logging.getLogger("bench")
    return logger


def worker_task(i, main_args, main_parser, num_process):
    test_suite = get_test_suite(main_parser, main_args, i, num_process)
    test_suite.init()
    start_time = time.time()

    if main_args.profile:
        import cProfile

        profile_result_location = os.path.join(
            main_args.result_dir, PROFILE_RESULT_FORMAT.format(i)
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
        f"Benchmark against {main_args.testsuite}: total time: {duration} seconds"
    )

    result = {
        "worker": i,
        "op": main_args.testsuite,
        "metrics": {DURATION_METRIC_KEY: duration},
    }
    if test_suite.metrics.get(Metrics.TOTAL_OPS):
        total_ops = test_suite.metrics.get(Metrics.TOTAL_OPS)
        ops_per_second = total_ops / duration
        result["metrics"][TOTAL_OPS_METRIC_KEY] = total_ops
        result["metrics"][OPS_PER_SECOND_METRIC_KEY] = ops_per_second
        print(
            f"{TOTAL_OPS_METRIC_KEY}: {total_ops}, {OPS_PER_SECOND_METRIC_KEY}: {ops_per_second}"
        )
    if test_suite.metrics.get(Metrics.TOTAL_BYTES):
        total_bytes = test_suite.metrics.get(Metrics.TOTAL_BYTES)
        bytes_per_second = total_bytes / duration
        result["metrics"][TOTAL_BYTES_METRIC_KEY] = total_bytes
        result["metrics"][BYTES_PER_SECOND_METRIC_KEY] = bytes_per_second
        print(
            f"{TOTAL_BYTES_METRIC_KEY}: {total_bytes}, {BYTES_PER_SECOND_METRIC_KEY}: {bytes_per_second / (1024 * 1024)}MB"
        )

    json_result_location = os.path.join(
        main_args.result_dir, BENCH_RESULT_FORMAT.format(i)
    )
    with open(json_result_location, "w") as f:
        json.dump(result, f)
    print(f"Find more benchmark results in dir {main_args.result_dir}")


def main():
    main_parser = init_main_parser()
    main_args, remaining_args = main_parser.parse_known_args()
    create_empty_dir(main_args.result_dir)
    configure_logging(main_args.result_dir)

    with Manager():
        jobs = []
        for i in range(main_args.numjobs):
            process = Process(
                target=worker_task,
                args=(i, main_args, main_parser, main_args.numjobs),
            )
            jobs.append(process)
            process.start()

        for job in jobs:
            job.join()


if __name__ == "__main__":
    main()
