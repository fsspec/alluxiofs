from enum import Enum

try:
    import ray.data
except ModuleNotFoundError as e:
    print("[WARNING]pkg 'ray' not installed, relative tests unable to run.")

from alluxiofs import AlluxioFileSystem
from tests.benchmark.AbstractBench import AbstractArgumentParser, Metrics
from tests.benchmark.AbstractBench import AbstractBench


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
        # read_parquet args
        self.parser.add_argument(
            "--dataset",
            type=str,
            required=False,
            help="dataset dir uri, e.g. s3://air-example-data-2/10G-xgboost-data.parquet/",
        )

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        return args


class RayBench(AbstractBench):
    def __init__(self, args, **kwargs):
        self.args = args
        self.alluxio_fs = AlluxioFileSystem(
            etcd_hosts=self.args.etcd_hosts,
            worker_hosts=self.args.worker_hosts,
        )

    def init(self):
        self.validate_args()
        self.metrics = Metrics()

    def execute(self) -> Metrics:
        if self.args.op == Op.read_parquet.name:
            print(f"Executing AlluxioRESTBench! Op:{self.args.op}")
            self.test_read_parquet()
        elif self.args.op == Op.read_images.name:
            self.test_read_images()
        else:
            raise Exception(
                f"Unknown Op:{self.args.op} for {self.__class__.__name__}"
            )
        return self.metrics

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
