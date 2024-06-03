# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
from enum import Enum

from benchmark.AbstractBench import AbstractAlluxioFSSpecTraverseBench
from benchmark.AbstractBench import AbstractArgumentParser
from benchmark.AbstractBench import Metrics


class Op(Enum):
    # Deducted from tests/assets/traffic_pattern
    # Dataset: s3://ai-ref-arch/10G-xgboost-data/
    # 100 files * 100MB = 10GB
    ray_xgboost_parquet = "ray_xgboost_parquet"
    # Dataset: s3://ai-ref-arch/imagenet-full-parquet/val/ab0845140d46e98353a12/
    # 1 files * 230MB = 28.52GB
    ray_pytorch_parquet = "ray_pytorch_parquet"
    # Dataset: s3://ai-ref-arch/imagenet-mini/train/
    # 35K files * 102KB = 3.5GB
    ray_pytorch_jpeg = "ray_pytorch_jpeg"


class AlluxioFSSpecTrafficArgumentParser(AbstractArgumentParser):
    def __init__(self, main_parser):
        self.parser = main_parser
        self.parser.add_argument(
            "--op",
            choices=[op.value for op in Op],
            default=Op.ray_pytorch_parquet.value,
            help="Operation to perform.",
        )

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        print("args:{}".format(args))
        return args


class AlluxioFSSpecTrafficBench(AbstractAlluxioFSSpecTraverseBench):
    TOTAL_OPS = "total_ops"
    TOTAL_BYTES = "total_bytes"
    RAY_XGBOOST_PARQUET_CAT_SIZE = 38 * 1024 * 1024

    def __init__(self, process_id, num_process, args, **kwargs):
        super().__init__(process_id, num_process, args, **kwargs)
        self.file_type = (
            "parquet"
            if self.args.op == Op.ray_xgboost_parquet.value
            or self.args.op == Op.ray_pytorch_parquet.value
            else "jpeg"
        )

    def execute(self):
        if self.args.op == Op.ray_xgboost_parquet.value:
            self.bench_ray_xgboost_parquet(*self.next_file())
        elif self.args.op == Op.ray_pytorch_parquet.value:
            self.bench_ray_pytorch_parquet(*self.next_file())
        elif self.args.op == Op.ray_pytorch_jpeg.value:
            self.bench_ray_pytorch_jpeg(*self.next_file())
        else:
            raise Exception(
                f"Unknown Op:{self.args.op} for {self.__class__.__name__}"
            )

    def bench_ray_xgboost_parquet(self, file_path, file_size):
        self.alluxio_fs.info(file_path)
        offset = 4  # traffic pattern starts from 4 bytes
        with self.alluxio_fs.open(file_path, "rb") as alluxio_file:
            self.alluxio_fs.info(file_path)
            while offset < file_size:
                data = alluxio_file.read(
                    min(self.RAY_XGBOOST_PARQUET_CAT_SIZE, file_size - offset)
                )
                offset += len(data)
        self.metrics.update(Metrics.TOTAL_OPS, 1)
        self.metrics.update(Metrics.TOTAL_BYTES, file_size - 4)

    def bench_ray_pytorch_parquet(self, file_path, file_size):
        self.alluxio_fs.cat_file(file_path, 4, file_size - 1)
        self.metrics.update(Metrics.TOTAL_OPS, 1)
        self.metrics.update(Metrics.TOTAL_BYTES, file_size - 4)

    def bench_ray_pytorch_jpeg(self, file_path, file_size):
        self.alluxio_fs.ls(self.get_parent_path(file_path))
        self.alluxio_fs.info(file_path)
        with self.alluxio_fs.open(file_path, "rb") as alluxio_file:
            self.alluxio_fs.info(file_path)
            offset = 0
            while offset < file_size:
                data = alluxio_file.read(file_size - offset)
                offset += len(data)
            self.metrics.update(Metrics.TOTAL_OPS, 1)
            self.metrics.update(Metrics.TOTAL_BYTES, file_size)

    def get_parent_path(self, path):
        if path.endswith("/"):
            path = path[:-1]
        return "/".join(path.split("/")[:-1])
