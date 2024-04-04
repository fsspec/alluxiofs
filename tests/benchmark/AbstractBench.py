from abc import ABC
from abc import abstractmethod

class Metrics(ABC):
    TOTAL_OPS = "total_ops"
    TOTAL_BYTES = "total_bytes"

    def __init__(self):
        self.metrics_dict = {
            Metrics.TOTAL_OPS:0,
            Metrics.TOTAL_BYTES:0,
        }

    def update(self, metrics_key, metrics_value):
        self.metrics_dict[metrics_key] += metrics_value

    def get(self, metrics_key):
        return self.metrics_dict[metrics_key]

    def merge(self, metrics):
        for _,(k,v) in enumerate(metrics.metrics_dict.items()):
            self.metrics_dict[k] += v

    def to_str(self):
        return str(self.metrics_dict)

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

class AbstractBench(ABC):
    def __init__(self, *args, **kwargs):
        # Initialize any fields needed
        pass

    @abstractmethod
    def execute(self) -> Metrics:
        # This method is abstract and should be implemented in the concrete subclass
        pass

    @abstractmethod
    def init(self):
        pass


class AbstractArgumentParser(ABC):
    @abstractmethod
    def parse_args(self, args=None, namespace=None):
        pass

