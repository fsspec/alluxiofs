from enum import Enum

from tests.benchmark.AbstractBench import AbstractArgumentParser, AbstractBench
# import ray.data

class Op(Enum):
    read_parquet = "read_parquet"
    read_images = "read_images"

class RayArgumentParser(AbstractArgumentParser):
    def __init__(self, main_parser):
        self.parser = main_parser
        self.parser.add_argument(
            '--op',
            type=str,
            default=Op.read_parquet.name,
            required=True,
            help='Ray read api to bench against')
        # read_parquet args
        self.parser.add_argument(
            '--dataset',
            type=str,
            required=False,
            help='dataset dir uri, e.g. s3://air-example-data-2/10G-xgboost-data.parquet/')

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        return args

class RayBench(AbstractBench):
    def __init__(self, args, **kwargs):
        self.args = args

    def init(self):
        self.validate_args()

    def execute(self):
        if self.args.op == Op.read_parquet.name:
            print(f"Executing AlluxioRESTBench! Op:{self.args.op}")
            self.test_read_parquet()
        elif self.args.op == Op.read_images.name:
            self.test_read_images()
        else:
            raise Exception(f"Unknown Op:{self.args.op} for {self.__class__.__name__}")

    def validate_args(self):
        if self.args.op == Op.read_parquet.name:
            pass
        elif self.args.op == Op.read_images.name:
            pass

    def test_read_parquet(self):
        try:
            ray.data.read_parquet(self.args.dataset, filesystem=alluxio)
        except Exception as e:
            print("Exception during test_read_parquet:", e)

    def test_read_images(self):
        try:
            ray.data.read_images(self.args.dataset, filesystem=alluxio)
        except Exception as e:
            print("Exception during test_read_images:", e)