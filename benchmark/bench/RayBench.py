# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
from enum import Enum

try:
    import ray.data
except ModuleNotFoundError:
    print("[WARNING]pkg 'ray' not installed, relative tests unable to run.")

from alluxiofs import AlluxioFileSystem
from benchmark.AbstractBench import AbstractArgumentParser, Metrics
from benchmark.AbstractBench import AbstractBench


class Op(Enum):
    read_parquet = "read_parquet"
    read_images = "read_images"


class RayArgumentParser(AbstractArgumentParser):
    def __init__(self, main_parser):
        self.parser = main_parser
        self.parser.add_argument(
            "--op",
            type=str,
            default=Op.read_parquet.name,
            required=True,
            help="Ray read api to bench against",
        )

    def parse_args(self, args=None, namespace=None):
        parsed_args = self.parser.parse_args(args, namespace)
        return parsed_args


class RayBench(AbstractBench):
    def __init__(self, process_id, num_process, args, **kwargs):
        super().__init__(process_id, num_process, args, **kwargs)
        self.args = args

    def init(self):
        self.validate_args()
        self.alluxio_fs = AlluxioFileSystem(
            etcd_hosts=self.args.etcd_hosts,
            worker_hosts=self.args.worker_hosts,
        )

    def execute(self):
        if self.args.op == Op.read_parquet.name:
            print(f"Executing AlluxioRESTBench! Op:{self.args.op}")
            self.test_read_parquet()
        elif self.args.op == Op.read_images.name:
            self.test_read_images()
        else:
            raise Exception(
                f"Unknown Op:{self.args.op} for {self.__class__.__name__}"
            )

    def validate_args(self):
        if self.args.op == Op.read_parquet.name:
            pass
        elif self.args.op == Op.read_images.name:
            pass

    def test_read_parquet(self):
        try:
            ray.data.read_parquet(self.args.dataset, filesystem=self.alluxio)
            self.metrics.update(Metrics.TOTAL_OPS, 1)
        except Exception as e:
            print("Exception during test_read_parquet:", e)

    def test_read_images(self):
        try:
            ray.data.read_images(self.args.dataset, filesystem=self.alluxio)
            self.metrics.update(Metrics.TOTAL_OPS, 1)
        except Exception as e:
            print("Exception during test_read_images:", e)
