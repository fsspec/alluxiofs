# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
from abc import ABC
from abc import abstractmethod
from typing import List
from typing import Tuple
from urllib.parse import urlparse

from alluxiofs import AlluxioFileSystem


class Metrics(ABC):
    TOTAL_OPS = "total_ops"
    TOTAL_BYTES = "total_bytes"

    def __init__(self):
        self.metrics_dict = {
            Metrics.TOTAL_OPS: 0,
            Metrics.TOTAL_BYTES: 0,
        }

    def update(self, metrics_key, metrics_value):
        self.metrics_dict[metrics_key] += metrics_value

    def get(self, metrics_key):
        return self.metrics_dict[metrics_key]

    def merge(self, metrics):
        for _, (k, v) in enumerate(metrics.metrics_dict.items()):
            self.metrics_dict[k] += v

    def to_str(self):
        return str(self.metrics_dict)

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()


class AbstractBench(ABC):
    def __init__(self, process_id, num_process, *args, **kwargs):
        self.process_id = process_id
        self.num_process = num_process
        self.args = args
        self.metrics = Metrics()
        self.file_type = None

    def next_dir(self) -> str:
        return ""

    def next_file(self) -> Tuple[str, float]:
        return "", 0.0

    @abstractmethod
    def execute(self):
        # This method is abstract and should be implemented in the concrete subclass
        pass

    @abstractmethod
    def init(self):
        pass

    def metrics(self) -> Metrics:
        return self.metrics


class AbstractAlluxioFSSpecTraverseBench(AbstractBench, ABC):
    def __init__(self, process_id, num_process, args, **kwargs):
        super().__init__(process_id, num_process, args, **kwargs)
        self.directories: List[str] = []
        self.files: List[Tuple[str, float]] = []
        self.args = args
        self.file_num = 0
        self.dir_num = 0

    def get_protocol(self, full_path: str) -> str:
        parsed_url = urlparse(full_path)
        return parsed_url.scheme

    def init(self):
        # protocol = self.get_protocol(self.args.path)
        alluxio_options = {}
        if self.args.use_alluxiocommon:
            alluxio_options["alluxio.common.extension.enable"] = "True"
        if self.args.page_size:
            alluxio_options[
                "alluxio.worker.page.store.page.size"
            ] = self.args.page_size
        print(f"options for AlluxioFileSystem:{alluxio_options}")
        self.alluxio_fs = AlluxioFileSystem(
            etcd_hosts=self.args.etcd_hosts,
            worker_hosts=self.args.worker_hosts,
            options=alluxio_options,
            # test_options={"log_level": "debug"}
            # target_protocol=protocol
        )
        self.traverse(self.args.path)

    def next_dir(self) -> str:
        if len(self.directories) < self.num_process:
            raise ValueError(
                f"Total number of directories is {len(self.directories)} but process num is {self.num_process}"
            )
        if self.dir_num > len(self.directories):
            self.dir_num = self.process_id
        next_dir = self.directories[self.dir_num]
        self.dir_num += self.num_process
        return next_dir

    def next_file(self) -> Tuple[str, float]:
        if len(self.files) < self.num_process:
            raise ValueError(
                f"Total number of files is {len(self.files)} but process num is {self.num_process}"
            )
        if self.file_num >= len(self.files):
            self.file_num = self.process_id
        next_file = self.files[self.file_num]
        self.file_num += self.num_process
        return next_file

    def traverse(self, path):
        entry = self.alluxio_fs.info(path)
        entry_path = entry["name"]
        if entry["type"] == "directory":
            self.directories.append(entry_path)
            for sub_path in self.alluxio_fs.ls(path, detail=False):
                self.traverse(sub_path)
        else:
            if self.file_type is not None:
                if entry_path.endswith(self.file_type) or entry_path.endswith(
                    self.file_type.upper()
                ):
                    self.files.append((entry_path, entry["size"]))
            else:
                self.files.append((entry_path, entry["size"]))


class AbstractArgumentParser(ABC):
    @abstractmethod
    def parse_args(self, args=None, namespace=None):
        pass
