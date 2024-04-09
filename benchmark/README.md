# Alluxiofs Python Benchmark

This benchmark targets Alluxio Fsspec + Alluxio system cache performance understanding & optimization.
It's a light-weight benchmark that requires minimum dependency and mimic actual production traffic patterns.


## Alluxio FSSpec Individual Benchmark

Try out:
```commandline
python bench.py --etcd_hosts=localhost --numjobs=1 --runtime=10 --testsuite=FSSPEC --path=s3://ai-ref-arch/small-dataset --op=cat_file --bs=262144
```

Actual bench:
Substitute `path` to `s3://ai-ref-arch/imagenet-mini/val` or `s3://ai-ref-arch/imagenet-mini/train` for metadata `op` `ls` `info`
```commandline
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC --path=s3://ai-ref-arch/imagenet-mini/val --op=info
```

Substitute `path` to `s3://ai-ref-arch/10G-xgboost-data/` for data `op` `cat_file` `open_seq_read` `open_random_read`.
Tune your preferred buffer size.
```commandline
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC --path=s3://ai-ref-arch/10G-xgboost-data/ --op=open_seq_read --bs=262144
```

## Alluxio FSSpec Traffic Pattern Benchmark

Try out:
```commandline
python bench.py --etcd_hosts=localhost --numjobs=1 --runtime=10 --testsuite=FSSPEC_TRAFFIC --path=s3://ai-ref-arch/10G-xgboost-data/d9ef953e9a7347db8793f9e772357e68_000098.parquet --op=ray_xgboost_parquet
python bench.py --etcd_hosts=localhost --numjobs=1 --runtime=10 --testsuite=FSSPEC_TRAFFIC --path=s3://ai-ref-arch/imagenet-full-parquet/val/ab0845140d46e98353a12/imagenet_0.parquet --op=ray_pytorch_parquet
python bench.py --etcd_hosts=localhost --numjobs=1 --runtime=10 --testsuite=FSSPEC_TRAFFIC --path=s3://ai-ref-arch/imagenet-mini/val/n01440764/ --op=ray_pytorch_jpeg
```

Actual bench:
```commandline
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC_TRAFFIC --path=s3://ai-ref-arch/10G-xgboost-data/ --op=ray_xgboost_parquet
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC_TRAFFIC --path=s3://ai-ref-arch/imagenet-full-parquet/val/ab0845140d46e98353a12/ --op=ray_pytorch_parquet
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC_TRAFFIC --path=s3://ai-ref-arch/imagenet-full-parquet/train/d3defc403579d6fc0f5c9/ --op=ray_pytorch_parquet
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC_TRAFFIC --path=s3://ai-ref-arch/imagenet-mini/train/ --op=ray_pytorch_jpeg
```

Note that Alluxio FSSpec benchmarks with directory traverse logics. If the target directories are too big, then it may take a long time to traverse the dataset.
Recommend file/dir number > numjobs so that each process has enough file/dir to repeatedly running operations on top.
