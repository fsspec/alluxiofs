import random
from enum import Enum

import humanfriendly

from benchmark.AbstractBench import AbstractAlluxioFSSpecTraverseBench
from benchmark.AbstractBench import AbstractArgumentParser
from benchmark.AbstractBench import Metrics


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
            "--op",
            choices=[op.value for op in Op],
            default=Op.cat_file.value,
            help="Operation to perform.",
        )
        self.parser.add_argument(
            "--bs",
            type=str,
            default=256 * 1024,
            help="Buffer size for read operations, in KB or MB.",
        )

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        # print("args:{}", args)
        return args


class AlluxioFSSpecBench(AbstractAlluxioFSSpecTraverseBench):
    def __init__(self, process_id, num_process, args, **kwargs):
        super().__init__(process_id, num_process, args, **kwargs)

    def execute(self):
        self.buffer_size = humanfriendly.parse_size(self.args.bs, binary=True)
        if self.args.op == Op.ls.value:
            self.bench_ls(self.next_dir())
        elif self.args.op == Op.info.value:
            self.bench_info(*self.next_file())
        elif self.args.op == Op.cat_file.value:
            self.bench_cat_file(*self.next_file())
        elif self.args.op == Op.open_seq_read.value:
            self.bench_open_seq_read(*self.next_file())
        elif self.args.op == Op.open_random_read.value:
            self.bench_open_random_read(*self.next_file())
        else:
            raise Exception(
                f"Unknown Op:{self.args.op} for {self.__class__.__name__}"
            )

    def bench_ls(self, dir_path):
        self.alluxio_fs.ls(dir_path)
        self.metrics.update(Metrics.TOTAL_OPS, 1)

    def bench_info(self, file_path, size):
        self.alluxio_fs.info(file_path)
        self.metrics.update(Metrics.TOTAL_OPS, 1)

    def bench_cat_file(self, file_path, file_size):
        file_read = 0
        while file_read < file_size:
            read_bytes = min(self.buffer_size, file_size - file_read)
            self.alluxio_fs.cat_file(file_path, 0, read_bytes)
            file_read += read_bytes
        self.metrics.update(Metrics.TOTAL_OPS, 1)
        self.metrics.update(Metrics.TOTAL_BYTES, read_bytes)

    def bench_open_seq_read(self, file_path, file_size):
        with self.alluxio_fs.open(file_path, "rb") as f:
            while True:
                data = f.read(self.buffer_size)
                if not data:
                    break
        self.metrics.update(Metrics.TOTAL_OPS, 1)
        self.metrics.update(Metrics.TOTAL_BYTES, file_size)

    def bench_open_random_read(self, file_path, file_size):
        bytes_read = 0
        total_ops = 0
        with self.alluxio_fs.open(file_path, "rb") as f:
            bytes_to_read = min(file_size, self.buffer_size)
            while bytes_read < bytes_to_read:
                offset = random.nextInt(file_size)
                read_bytes = min(self.buffer_size, file_size - offset)
                f.seek(offset)
                data = f.read(read_bytes)
                bytes_read += len(data)
                total_ops += 1

        self.metrics.update(Metrics.TOTAL_OPS, total_ops)
        self.metrics.update(Metrics.TOTAL_BYTES, bytes_read)
