# Alluxiofs Python Benchmark

This benchmark targets Alluxio Fsspec + Alluxio system cache performance understanding & optimization.
It's a light-weight benchmark that requires minimum dependency and mimic actual production traffic patterns.


## Alluxio FSSpec Individual Benchmark

Try out:
```commandline
python bench.py --etcd_hosts=localhost --numjobs=1 --runtime=10 --testsuite=FSSPEC --path=s3://ai-ref-arch/small-dataset --op=cat_file --bs=256KB
```

Actual bench:
Substitute `path` to `s3://ai-ref-arch/imagenet-mini/val` or `s3://ai-ref-arch/imagenet-mini/train` for metadata `op` `ls` `info`
```commandline
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC --path=s3://ai-ref-arch/imagenet-mini/val --op=info
```

Substitute `path` to `s3://ai-ref-arch/10G-xgboost-data/` for data `op` `cat_file` `open_seq_read` `open_random_read`.
Tune your preferred buffer size.
```commandline
python bench.py --etcd_hosts=localhost --numjobs=2 --runtime=20 --testsuite=FSSPEC --path=s3://ai-ref-arch/10G-xgboost-data/ --op=open_seq_read --bs=256KB
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


## Ray single node image loader micronbenchmark

#### Prerequisite
A running Ray cluster (single node is fine)
e.g. a single ray docker container  (current recommend Ray 2.9.3 version as latest version seems unstable(2024/05/09))

    docker run -it --rm rayproject/ray:2.9.3 /bin/bash
    [Note] on top of this there's a list of dependency install:
    pip install torch --no-cache-dir
    pip install torchvision
    pip install tensorflow
    pip install mosaicml-streaming
    pip install pydantic==1.10.12
    pip install protobuf==3.20.2
    pip install xgboost
    pip install xgboost_ray
    pip install s3fs
    sudo apt-get update
    sudo apt-get install -y awscli dstat vim screen
    sudo apt-get update
    sudo apt-get install -y awscli dstat vim screen iproute2
    pip install s3fs
    sudo apt-get install iproute2

[Internal] Use this ECR image for latest stable image with all dependencies installed:
`533267169037.dkr.ecr.us-east-1.amazonaws.com/alluxioray-lucy:lateststable`

Try out:
1) Start ray cluster (adjust relevant flags number accordingly)


    nohup ray start --head --memory=$((16 * 2**30)) --object-store-memory=$((4 * 2**30)) --dashboard-host=0.0.0.0 --metrics-export-port=8080 --block --num-cpus=14  2>&1 > /home/ray/ray.out &

make sure it is up by checking with `ray status`

2) Start the microbenchmark

- With Ray data + s3 parquet dataset:


    python3 benchmark/ray_single_node_image_loader_microbenchmark.py --parquet-data-root s3://ai-ref-arch/imagenet-mini-parquet/train/75e5c08ce5913cf8f513a/

- With Ray data + s3 parquet dataset on alluxio:


    1) on alluxio home, do load first:
    bin/alluxio job load --path s3://ai-ref-arch/imagenet-mini-parquet/train/75e5c08ce5913cf8f513a/ --submit
    2) run the microbench
    python3 benchmark/ray_single_node_image_loader_microbenchmark.py \
       --parquet-data-root s3://ai-ref-arch/imagenet-mini-parquet/train/75e5c08ce5913cf8f513a/ \
       --use-alluxio --alluxio-worker-hosts <hostname> \
       --alluxio-page-size <e.g. 32MB> \
       [--use-alluxiocommon (optional, to use RUST extension)]
