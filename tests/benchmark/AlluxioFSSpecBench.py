from enum import Enum

from tests.benchmark.AbstractBench import AbstractBench, AbstractArgumentParser
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

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        print("args:{}", args)
        return args

class AlluxioFSSpecBench(AbstractBench):
    def __init__(self, *args, **kwargs):
        self.args = args

    def execute(self):
        print("Executing AlluxioFSSpecBench!")
        pass

    def validate_args(self):
        pass
