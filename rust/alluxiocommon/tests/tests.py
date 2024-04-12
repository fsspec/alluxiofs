from alluxiocommon import _DataManager
from hashlib import md5
import time

KB = 1024
MB = 1024 * KB

FULL_PAGE_URL_FORMAT = (
    "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}"
)
PAGE_URL_FORMAT = "http://{worker_host}:{http_port}/v1/file/{path_id}/page/{page_index}?offset={page_offset}&length={page_length}"


class TestSuite:

    def __init__(self):
        self.page_size = 32 * MB
        self.worker_host = "172.31.45.56"
        self.worker_http_port = 28080
        self.dm = _DataManager(4)

    def multi_http_requests(self):
        offset, length = 0,40 * MB
        read_urls = []
        path_id = "addb83ae5f2025ec8f88b832c40177327ccd6aa2486712f34e68c3efbb2045e7"
        start = offset
        while start < offset + length:
            page_index = start // self.page_size
            inpage_off = start % self.page_size
            inpage_read_len = min(self.page_size - inpage_off, length - start)
            page_url = None
            if inpage_off == 0 and inpage_read_len == self.page_size:
                page_url = FULL_PAGE_URL_FORMAT.format(
                    worker_host=self.worker_host,
                    http_port=self.worker_http_port,
                    path_id=path_id,
                    page_index=page_index,
                )
            else:
                page_url = PAGE_URL_FORMAT.format(
                    worker_host=self.worker_host,
                    http_port=self.worker_http_port,
                    path_id=path_id,
                    page_index=page_index,
                    page_offset=inpage_off,
                    page_length=inpage_read_len,
                )
            read_urls.append(page_url)
            start += inpage_read_len
        data = self.dm.make_multi_http_req(read_urls)
        print(f"type:{type(data)}")
        md5_instance = md5()
        md5_instance.update(data)
        print(f"data len:{len(data)}, md5:{md5_instance.hexdigest()}")
        time.sleep(30)

def main():
    ts = TestSuite()
    for _ in range(1):
        ts.multi_http_requests()

if __name__ == "__main__":
    main()
