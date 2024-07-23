import fsspec

from alluxiofs import AlluxioFileSystem

# import pandas as pd
# import s3fs

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)
alluxio_fs = fsspec.filesystem(
    "alluxiofs", etcd_hosts="localhost", etcd_port=2379, target_protocol="s3"
)

# alluxio_fs.copy("s3://sibyltest/IO_test/sent_train_100_rename.csv","s3://sibyltest/IO_test/test_posix/sent_train_100_rename.csv")
# alluxio_fs.rm("s3://sibyltest/IO_test/test_posix/sent_train_100.csv")
# alluxio_fs.mkdir("newdir1234", False)
# print(alluxio_fs.ls("s3://sibyltest/IO_test"))
# alluxio_fs.rmdir("sibyltest/test_rmdir") # only this works...., won't work with s3:// prefix
# print(alluxio_fs.exists("s3://sibyltest/IO_test/sent_train_100.csv"))
# print(alluxio_fs.walk("s3://sibyltest/IO_test/"))
# alluxio_fs.rename("s3://sibyltest/IO_test/sent_train_100.csv", "s3://sibyltest/IO_test/sent_train_100_rename.csv")
# print(alluxio_fs.cat_file(path="s3://sibyltest/IO_test/sent_train_99.csv", start=0, end=217378))
# f = alluxio_fs.open("s3://sibyltest/IO_test/sent_train_2.csv", 'a')
# f.write("new line written by alluxiofs")
# f.close()

# with alluxio_fs.open('s3://sibyltest/IO_test/sent_train_1_loaded.csv', 'a') as f:
#     f.write("\n111 new line by alluxio\n")
# f.flush()

with alluxio_fs.open("s3://sibyltest/IO_test/sent_train_1.csv", "r") as f:

    print(f.read())

# alluxio_fs.chmod("sibyltest", 'public-read')
print(alluxio_fs.ls("s3://sibyltest/IO_test/"))
# print(alluxio_fs.isfile('s3://sibyltest/IO_test'))
