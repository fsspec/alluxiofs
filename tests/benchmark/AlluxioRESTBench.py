from enum import Enum
import random
import re

from alluxiofs.client.const import ALLUXIO_PAGE_SIZE_DEFAULT_VALUE
from tests.benchmark.AbstractBench import AbstractBench, AbstractArgumentParser
from alluxiofs.client.core import AlluxioClient

class Op(Enum):
    GetPage = "GetPage"
    ListFiles = "ListFiles"
    GetFileInfo = "GetFileInfo"
    PutPage = "PutPage"

class AlluxioRESTArgumentParser(AbstractArgumentParser):
    def __init__(self, main_parser):
        self.parser = main_parser
        self.parser.add_argument(
            '--op',
            type=str,
            default=Op.GetPage.name,
            required=True,
            help='REST Op to bench against')
        # GetPage args
        self.parser.add_argument(
            '--filename',
            type=str,
            required=False,
            help='filename to read')
        self.parser.add_argument(
            '--irange',
            type=str,
            required=False,
            help='read range in bytes, <str>-<end> (e.g. 1-1024, end inclusive)')

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        return args

class AlluxioRESTBench(AbstractBench):
    def __init__(self, args, **kwargs):
        self.args = args

    def execute(self):
        if self.args.op == Op.GetPage.name:
            print(f"Executing AlluxioRESTBench! Op:{self.args.op}")
            self.testGetPage()
        elif self.args.op == Op.GetPage.name:
            pass
        elif self.args.op == Op.ListFiles.name:
            pass
        elif self.args.op == Op.PutPage.name:
            pass
        else:
            raise Exception(f"Unknown Op:{self.args.op} for {self.__class__.__name__}")

    def validate_args(self):
        if self.args.op == Op.GetPage.name:
            required_args_absence = any(arg is None for arg in [
                self.args.filename
            ])
            if required_args_absence:
                raise Exception(f"Missing args for {self.args.op}")

            if self.args.irange is not None:
                match = re.match(r"\d+-\d+", self.args.irange)
                if match:
                    nums = [int(x) for x in match.group().split('-')]
                    if nums[0] >= nums[1]:
                        raise Exception(f"Invalid irange")
                else:
                    raise Exception(f"Incorrect irange param passed.")
        elif self.args.op == Op.ListFiles.name:
            pass
        elif self.args.op == Op.GetFileInfo.name:
            pass
        elif self.args.op == Op.PutPage.name:
            pass

    def testGetPage(self):
        print(f"{self.args.etcd_hosts}, {self.args.worker_hosts}")
        alluxio_client = AlluxioClient(
            etcd_hosts=self.args.etcd_hosts,
            worker_hosts=self.args.worker_hosts
        )
        file_path = self.args.filename
        if self.args.irange is not None:
            range_str = re.match(r"\d+-\d+", self.args.irange).group()
            irange = [int(x) for x in range_str.split('-')]
            off = random.randint(irange[0],irange[1])
            len = random.randint(1, irange[1]-off+1)
            alluxio_client.read_range(file_path, off, len)
        else:
            alluxio_client.read(file_path)
