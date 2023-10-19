import ray
import fsspec
from alluxiofs import AlluxioFileSystem
import time
import argparse
import humanfriendly

def parse_args():
    parser = argparse.ArgumentParser(description="Benchmarking script for reading files with Alluxio")
    parser.add_argument("--etcd_host", type=str, default="localhost", required=False, help="The host address for etcd")
    parser.add_argument("--dataset", type=str, default="s3://ai-ref-arch/imagenet-mini/val", required=False, help="The path to the dataset in Alluxio")
    parser.add_argument('--materialize', type=bool, default=False, help='Materialize the dataset after reading')
    return parser.parse_args()

def main(args):
    start_time = time.time()

    fsspec.register_implementation("s3", AlluxioFileSystem, clobber=True) 
    alluxio = fsspec.filesystem('s3', etcd_host=args.etcd_host)

    ds = ray.data.read_images(args.dataset, filesystem=alluxio)
    if args.materialize:
        ds = ds.materialize()
    
    end_time = time.time()
    total_images = ds.count()
    total_time = end_time - start_time
    print(f"Total images processed: {total_images}")
    print(f"Total time taken: {total_time:.2f} seconds")
    if total_time > 0:
        images_per_second = total_images / total_time
        print(f"Images per second: {images_per_second:.2f}")

if __name__ == "__main__":
    args = parse_args()
    main(args)
