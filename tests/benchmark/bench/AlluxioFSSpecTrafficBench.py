import time
from enum import Enum

from alluxiofs import AlluxioFileSystem
from tests.benchmark.AbstractBench import AbstractArgumentParser
from tests.benchmark.AbstractBench import AbstractBench


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


class FileType(Enum):
    parquet = "parquet"
    jpeg = "jpeg"


class AlluxioFSSpecTrafficArgumentParser(AbstractArgumentParser):
    def __init__(self, main_parser):
        self.parser = main_parser
        self.parser.add_argument(
            "--path",
            type=str,
            default="s3://ai-ref-arch/10G-xgboost-data/",
            help="Path for the operation contains the dataset of given file type.",
        )
        self.parser.add_argument(
            "--type",
            type=str,
            choices=[ft.value for ft in FileType],
            default=FileType.parquet,
            help="File type.",
        )
        self.parser.add_argument(
            "--op",
            choices=[op.value for op in Op],
            default=Op.ray_pytorch_parquet.value,
            help="Operation to perform.",
        )
        self.parser.add_argument(
            "--iteration", type=int, default=10, help="Iterations."
        )

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        print("args:{}", args)
        return args


class AlluxioFSSpecTrafficBench(AbstractBench):
    TOTAL_OPS = "total_ops"
    TOTAL_BYTES = "total_bytes"
    RAY_XGBOOST_PARQUET_CAT_SIZE = 38 * 1024 * 1024

    def __init__(self, args, **kwargs):
        self.args = args

    def init(self):
        print(f"{self.args.etcd_hosts}, {self.args.worker_hosts}")
        if (
            (
                self.args.op == Op.ray_xgboost_parquet.value
                and self.args.type != FileType.parquet.value
            )
            or (
                self.args.op == Op.ray_pytorch_parquet.value
                and self.args.type != FileType.parquet.value
            )
            or (
                self.args.op == Op.ray_pytorch_jpeg.value
                and self.args.type != FileType.jpeg.value
            )
        ):
            raise Exception(
                f"Operation {self.args.op} and file type {self.args.type} do not match"
            )
        self.alluxio_fs = AlluxioFileSystem(
            etcd_hosts=self.args.etcd_hosts,
            worker_hosts=self.args.worker_hosts,
        )
        self.files = {}
        self.traverse(self.args.path, self.directories, self.files)
        if not self.files:
            raise Exception(
                f"Path {self.args.path} does not contain any files of given type {self.args.type}"
            )

    def execute(self):
        start_time = time.time()
        result_metrics = {}
        for _ in range(self.args.iteration):
            for file_path, file_size in self.files.items():
                if self.args.op == Op.ray_xgboost_parquet.value:
                    result_metrics = self.bench_ray_xgboost_parquet(
                        file_path, file_size
                    )
                elif self.args.op == Op.ray_pytorch_parquet.value:
                    result_metrics = self.bench_ray_pytorch_parquet(
                        file_path, file_size
                    )
                elif self.args.op == Op.ray_pytorch_jpeg.value:
                    result_metrics = self.bench_ray_pytorch_jpeg(
                        file_path, file_size
                    )
                else:
                    raise Exception(
                        f"Unknown Op:{self.args.op} for {self.__class__.__name__}"
                    )
        duration = time.time() - start_time

        if result_metrics.get(self.TOTAL_OPS):
            print(
                f"Benchmark against {self.args.op}: total ops: {result_metrics[self.TOTAL_OPS]} ops/second: {result_metrics[self.TOTAL_OPS] / duration}"
            )
        if result_metrics.get(self.TOTAL_BYTES):
            print(
                f"Benchmark against {self.args.op}: total bytes: {result_metrics[self.TOTAL_BYTES]} bytes/second: {result_metrics[self.TOTAL_BYTES] / duration}"
            )

        if not result_metrics:
            print(
                f"Benchmark against {self.args.op}: iteration: {self.args.iteration} total time: {duration} seconds"
            )

    def traverse(self, path, directories, files):
        entry = self.alluxio_fs.info(path)
        entry_path = entry["name"]
        if entry["type"] == "directory":
            for subpath in self.alluxio_fs.ls(path, detail=False):
                self.traverse(subpath, directories, files)
        elif entry_path.endswith(self.args.type):
            files[entry_path] = entry["size"]

    def bench_ray_xgboost_parquet(self, path, size):
        self.alluxio_fs.info(path)
        offset = 4  # traffic pattern starts from 4 bytes
        with self.alluxio_fs.open(path, "rb") as alluxio_file:
            self.alluxio_fs.info(path)
            while offset < size:
                data = alluxio_file.read(
                    min(self.RAY_XGBOOST_PARQUET_CAT_SIZE, size - offset)
                )
                offset += len(data)
        return {
            self.TOTAL_BYTES: offset,
        }

    def bench_ray_pytorch_parquet(self, path, size):
        self.alluxio_fs.cat_file(path, 4, size - 1)
        return {
            self.TOTAL_OPS: 1,
            self.TOTAL_BYTES: size - 5,
        }

    def bench_ray_pytorch_jpeg(self, path, size):
        self.alluxio_fs.ls(self.get_parent_path(path))
        self.alluxio_fs.info(path)
        with self.alluxio_fs.open(path, "rb") as alluxio_file:
            self.alluxio_fs.info(path)
            data = alluxio_file.read(size)
            return {
                self.TOTAL_BYTES: len(data),
            }

    def get_parent_path(path):
        if path.endswith("/"):
            path = path[:-1]
        if path.startswith("s3://"):
            path = path.replace("s3://", "")
        return "/".join(path.split("/")[:-1])
