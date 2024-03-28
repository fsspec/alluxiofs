import random
import re
from enum import Enum

from alluxiofs import AlluxioClient
from benchmark.AbstractBench import AbstractBench, AbstractArgumentParser
import argparse

class Op(Enum):
    ls = "ls"
    info = "info"
    open = "open"
    cat_file = "cat_file"

class AlluxioFSSpecArgumentParser(AbstractArgumentParser):
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='AlluxioFSSpec Argument Parser')
        self.parser.add_argument(
            '--op',
            default=Op.ls,
            required=False,
            help='REST Op to bench against')
        # cat_file args
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
        print("args:{}", args)
        return args

class AlluxioFSSpecBench(AbstractBench):
    def __init__(self, *args, **kwargs):
        self.args = args

    def init(self):
        self.validate_args()

    def execute(self):
        print("Executing AlluxioFSSpecBench!")
        if self.args.op == Op.cat_file:
            self.test_cat_file()
        pass

    def validate_args(self):
        if self.args.op == Op.cat_file.name:
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
        elif self.args.op == Op.ls.name:
            pass
        elif self.args.op == Op.info.name:
            pass
        elif self.args.op == Op.open.name:
            pass

    def test_cat_file(self):
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
