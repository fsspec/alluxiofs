# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
import logging
from io import BytesIO

import pycurl


def set_log_level(logger, test_options):
    if "log_level" in test_options:
        log_level = test_options["log_level"].upper()
        if log_level == "DEBUG":
            logger.setLevel(logging.DEBUG)
        elif log_level == "INFO":
            logger.setLevel(logging.INFO)
        elif log_level == "WARN" or log_level == "WARNING":
            logger.setLevel(logging.WARN)
        else:
            logger.warning(f"Unsupported log level: {log_level}")


def _c_send_get_request(url, headers):
    buffer = BytesIO()
    c = pycurl.Curl()
    headers = [f"{k}: {v}".encode("utf-8") for k, v in headers.items()]
    c.setopt(c.URL, url.encode("utf-8"))
    c.setopt(c.HTTPHEADER, headers)
    c.setopt(c.WRITEDATA, buffer)
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(c.CONNECTTIMEOUT, 10)
    c.setopt(c.TIMEOUT, 60)
    c.perform()
    status = c.getinfo(c.RESPONSE_CODE)
    c.close()

    if status == 104:
        raise ConnectionResetError("Connection reset by peer")
    elif status >= 400:
        raise RuntimeError(f"HTTP error: {status}")

    return buffer.getvalue()


def _c_send_get_request_stream(url, headers={}):
    buffer = BytesIO()
    c = pycurl.Curl()
    headers = [f"{k}: {v}".encode("utf-8") for k, v in headers.items()]
    c.setopt(c.URL, url.encode("utf-8"))
    c.setopt(c.HTTPHEADER, headers)
    c.setopt(c.WRITEDATA, buffer)
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(c.CONNECTTIMEOUT, 10)
    c.setopt(c.TIMEOUT, 60)
    c.perform()
    status = c.getinfo(c.RESPONSE_CODE)
    c.close()

    if status == 104:
        raise ConnectionResetError("Connection reset by peer")
    elif status >= 400:
        raise RuntimeError(f"HTTP error: {status}")

    return buffer
