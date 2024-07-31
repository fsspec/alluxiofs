# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
import json
import random
import re
from enum import Enum

import requests
from requests.adapters import HTTPAdapter

from alluxiofs.client.const import (
    ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
)
from alluxiofs.client.const import FULL_PAGE_URL_FORMAT
from alluxiofs.client.const import GET_FILE_STATUS_URL_FORMAT
from alluxiofs.client.const import LIST_URL_FORMAT
from benchmark.AbstractBench import AbstractArgumentParser
from benchmark.AbstractBench import AbstractBench
from benchmark.AbstractBench import Metrics


class Op(Enum):
    GetPage = "GetPage"
    ListFiles = "ListFiles"
    GetFileInfo = "GetFileInfo"
    PutPage = "PutPage"


class AlluxioRESTArgumentParser(AbstractArgumentParser):
    def __init__(self, main_parser):
        self.parser = main_parser
        self.parser.add_argument(
            "--op",
            type=str,
            choices=[op.value for op in Op],
            default=Op.GetPage.name,
            required=True,
            help="REST Op to bench against",
        )
        # GetPage args
        self.parser.add_argument(
            "--fileid",
            type=str,
            required=False,
            help="fileid, the hash of the uri, e.g. 0f63213559a69a4e0dab3774ff113f367e8ddbfe8966dc1062f366cc0b27b88b",
        )
        self.parser.add_argument(
            "--page_id_range",
            type=str,
            required=False,
            help="page id start and end range, <str_pageid>-<end_pageid> (e.g. 0-39, end inclusive)",
        )

    def parse_args(self, args=None, namespace=None):
        args = self.parser.parse_args(args, namespace)
        return args


class AlluxioRESTBench(AbstractBench):
    def __init__(self, process_id, num_process, args, **kwargs):
        super().__init__(process_id, num_process, args, **kwargs)

    def init(self):
        super().init()
        self.validate_args()
        self.worker_host = self.args.worker_hosts.split(",")[0]
        self.page_id_range = None
        self.path = self.args.path
        if self.args.page_id_range is not None:
            match = re.match(r"\d+-\d+", self.args.page_id_range)
            if match:
                nums = [int(x) for x in match.group().split("-")]
                self.page_id_range = (nums[0], nums[1])
                # print(f"{self.page_id_range}")
        # Init session
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1)
        self.session.mount("http://", adapter)

    def execute(self):
        if self.args.op == Op.GetPage.name:
            self.testGetPage()
        elif self.args.op == Op.GetFileInfo.name:
            self.testGetFileInfo()
        elif self.args.op == Op.ListFiles.name:
            self.testListFiles()
        elif self.args.op == Op.PutPage.name:
            pass
        else:
            raise Exception(
                f"Unknown Op:{self.args.op} for {self.__class__.__name__}"
            )

    def validate_args(self):
        if self.args.worker_hosts is None:
            raise Exception(
                f"{self.__class__.__name__} requires list of worker_hosts!"
            )
        if self.args.op == Op.GetPage.name:
            required_args = [self.args.fileid, self.args.page_id_range]
            required_args_absence = any(arg is None for arg in required_args)
            if required_args_absence:
                raise Exception(
                    f"Missing args for {self.args.op}, required args:[fileid, page_id_range]"
                )

            if self.args.page_id_range is not None:
                match = re.match(r"\d+-\d+", self.args.page_id_range)
                if match:
                    nums = [int(x) for x in match.group().split("-")]
                    if nums[0] > nums[1]:
                        raise Exception("Invalid page_id_range")
                else:
                    raise Exception("Incorrect page_id_range param passed.")
        elif (
            self.args.op == Op.ListFiles.name
            or self.args.op == Op.GetFileInfo.name
        ):
            required_args = [self.args.path]
            required_args_absence = any(arg is None for arg in required_args)
            if required_args_absence:
                raise Exception(
                    f"Missing args for {self.args.op}, required args:[path]"
                )
        elif self.args.op == Op.PutPage.name:
            pass

    def testGetPage(self):
        page_idx = random.randint(self.page_id_range[0], self.page_id_range[1])
        try:
            response = self.session.get(
                FULL_PAGE_URL_FORMAT.format(
                    worker_host=self.worker_host,
                    http_port=ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
                    path_id=self.args.fileid,
                    file_path=self.args.path,
                    page_index=page_idx,
                )
            )
            response.raise_for_status()
            len(response.content)
            content_len = len(response.content)
            self.metrics.update(Metrics.TOTAL_OPS, 1)
            self.metrics.update(Metrics.TOTAL_BYTES, content_len)
        except Exception as e:
            raise Exception(
                f"Error GetPage, path:{self.path}: error {e}"
            ) from e

    def testListFiles(self):
        params = {"path": self.path}
        try:
            response = self.session.get(
                LIST_URL_FORMAT.format(
                    worker_host=self.worker_host,
                    http_port=ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
                ),
                params=params,
            )
            response.raise_for_status()
            # just read full content but do nothing
            json.loads(response.content)
            self.metrics.update(Metrics.TOTAL_OPS, 1)
        except Exception as e:
            raise Exception(
                f"Error ListFiles, path:{self.path}: error {e}"
            ) from e

    def testGetFileInfo(self):
        params = {"path": self.path}
        try:
            response = self.session.get(
                GET_FILE_STATUS_URL_FORMAT.format(
                    worker_host=self.worker_host,
                    http_port=ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE,
                ),
                params=params,
            )
            response.raise_for_status()
            json.loads(response.content)[0]
            self.metrics.update(Metrics.TOTAL_OPS, 1)
        except Exception as e:
            raise Exception(
                f"Error GetFileInfo, path:{self.path}: error {e}"
            ) from e

    def testPutPage(self):
        pass
