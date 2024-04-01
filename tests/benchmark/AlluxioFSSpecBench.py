import random
import time
from enum import Enum

from alluxiofs import AlluxioFileSystem
from tests.benchmark.AbstractBench import AbstractArgumentParser
from tests.benchmark.AbstractBench import AbstractBench


class Op(Enum):
    ls = "ls"
    info = "info"
    cat_file = "cat_file"
    open_seq_read = "open_seq_read"
    open_random_read = "open_random_read"


class AlluxioFSSpecArgumentParser(AbstractArgumentParser):
    def __init__(self, main_parser):
        self.parser = main_parser
        self.parser.add_argument(
            "--path", type=str, required=True, help="Path for the operation."
        )
        self.parser.add_argument(
            "--op",
            choices=[op.value for op in Op],
            default=Op.cat_file.value,
            help="Operation to perform.",
        )
        self.parser.add_argument(
            "--bs",
            type=int,
            default=256 * 1024,
            help="Buffer size for read operations.",
        )
        self.parser.add_argument(
            "--iteration", type=int, default=10, help="Iterations."
        )

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        print("args:{}", args)
        return args


class AlluxioFSSpecBench(AbstractBench):
    TOTAL_OPS = "total_ops"
    TOTAL_BYTES = "total_bytes"

    def __init__(self, args, **kwargs):
        self.args = args

    def init(self):
        print(f"{self.args.etcd_hosts}, {self.args.worker_hosts}")
        self.alluxio_fs = AlluxioFileSystem(
            etcd_hosts=self.args.etcd_hosts,
            worker_hosts=self.args.worker_hosts,
        )
        self.directories = []
        self.files = {}
        self.traverse(self.args.path, self.directories, self.files)

    def execute(self):
        start_time = time.time()
        result_metrics = {}
        for _ in range(self.args.iteration):
            if self.args.op == Op.ls.value:
                result_metrics = self.bench_ls()
            elif self.args.op == Op.info.value:
                result_metrics = self.bench_info()
            elif self.args.op == Op.cat_file.value:
                result_metrics = self.bench_cat_file()
            elif self.args.op == Op.open_seq_read.value:
                result_metrics = self.bench_open_seq_read()
            elif self.args.op == Op.open_random_read.value:
                result_metrics = self.bench_open_random_read()
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
            directories.append(entry_path)
            for subpath in self.alluxio_fs.ls(path, detail=False):
                self.traverse(subpath, directories, files)
        else:
            files[entry_path] = entry["size"]

    def bench_ls(self):
        for directory in self.directories:
            self.alluxio_fs.ls(directory)
        return {self.TOTAL_OPS: len(self.directories)}

    def bench_info(self):
        for file in self.files.keys():
            self.alluxio_fs.info(file)
        return {self.TOTAL_OPS: len(self.files.keys())}

    def bench_cat_file(self):
        total_bytes = 0
        for file_path, file_size in self.files.items():
            file_read = 0
            while file_read < file_size:
                read_bytes = min(self.args.bs, file_size - total_bytes)
                self.alluxio_fs.cat_file(file_path, 0, read_bytes)
                file_read += read_bytes
            total_bytes += file_size
        return {
            self.TOTAL_OPS: len(self.files.keys()),
            self.TOTAL_BYTES: total_bytes,
        }

    def bench_open_seq_read(self):
        total_bytes = 0
        for file_path, file_size in self.files.items():
            with self.alluxio_fs.open(file_path, "rb") as f:
                while True:
                    data = f.read(self.args.bs)
                    if not data:
                        break
            total_bytes += file_size
        return {
            self.TOTAL_OPS: len(self.files.keys()),
            self.TOTAL_BYTES: total_bytes,
        }

    def bench_open_random_read(self):
        total_bytes = 0
        total_ops = 0
        for file_path, file_size in self.files.items():
            bytes_read = 0
            with self.alluxio_fs.open(file_path, "rb") as f:
                bytes_to_read = min(file_size, 10 * self.args.bs)
                while bytes_read < bytes_to_read:
                    offset = random.nextInt(file_size)
                    read_bytes = min(self.args.bs, file_size - offset)
                    f.seek(offset)
                    data = f.read(read_bytes)
                    bytes_read += len(data)
                    total_ops += 1
            total_bytes += bytes_read

        return {self.TOTAL_OPS: total_ops, self.TOTAL_BYTES: total_bytes}
