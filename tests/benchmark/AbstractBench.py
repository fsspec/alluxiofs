from abc import ABC
from abc import abstractmethod


class AbstractBench(ABC):
    def __init__(self, *args, **kwargs):
        # Initialize any fields needed
        pass

    @abstractmethod
    def execute(self):
        # This method is abstract and should be implemented in the concrete subclass
        pass

    @abstractmethod
    def init(self):
        pass


class AbstractArgumentParser(ABC):
    @abstractmethod
    def parse_args(self, args=None, namespace=None):
        pass
