import os

from alluxiofs import AlluxioFileSystem


# Test multi-stream prefetch
def test_multi_stream_prefetch():
    if os.path.exists("/tmp/local_cache"):
        import shutil

        shutil.rmtree("/tmp/local_cache")
    alluxio_fs = AlluxioFileSystem(
        yaml_path="./config_template.yaml",
        local_cache_dir="/tmp/local_cache",
        local_cache_enabled=True,
    )
    file_name = "file:///home/yxd/alluxio/ufs/large_test.mcap"

    f1, r1 = alluxio_fs.open(file_name, "rb")
    f2, r2 = alluxio_fs.open(file_name, "rb")

    f1.seek(500 * 1024 * 1024)
    print("F1 seek to 50MB")
    print("===================================================")

    for i in range(50):
        f1.read(15 * 1024 * 1024)
        print(f"F1 read {i + 1} block: 16 MB")
        print("f1 window size:", r1.get_local_cache_prefetch_window_size())
        print("f2 window size:", r2.get_local_cache_prefetch_window_size())
        print()
        f2.read(15 * 1024 * 1024)
        print(f"F2 read {i + 1} block: 16 MB")
        print("f1 window size:", r1.get_local_cache_prefetch_window_size())
        print("f2 window size:", r2.get_local_cache_prefetch_window_size())
        print("=============================================")

    f1.close()
    f2.close()
