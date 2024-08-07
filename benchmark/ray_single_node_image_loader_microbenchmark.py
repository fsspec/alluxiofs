# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
"""
Runs single-node microbenchmarks for reading and preprocessing an image dataset
from local disk and (for supported data loaders) cloud storage. Pass a --data-root,
--parquet-data-root, --tf-data-root, and/or --mosaic-data-root pointing to the
dataset directory. Throughputs are written to `output.csv` in total images/s.
"""
import os
import time
from typing import Any
from typing import Callable
from typing import Iterator
from typing import TYPE_CHECKING

import fsspec
import numpy as np
import pandas as pd
import ray
import streaming
import tensorflow as tf
import torch
import torchvision
from datasets import load_dataset
from PIL import Image
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from streaming import LocalDataset
from streaming import StreamingDataset

from alluxiofs import AlluxioFileSystem

# HF Dataset.
# MosaicML StreamingDataset

if TYPE_CHECKING:
    import pyarrow

DEFAULT_IMAGE_SIZE = 224

# tf.data needs to resize all images to the same size when loading.
# This is the size of dog.jpg in s3://air-cuj-imagenet-1gb.
FULL_IMAGE_SIZE = (1213, 1546)


def iterate(dataset, label, batch_size, metrics, output_file=None):
    start = time.time()
    it = iter(dataset)
    num_rows = 0
    print_at = 1000
    for batch in it:
        if isinstance(batch, tuple) or isinstance(batch, list):
            batch = batch[0]
        else:
            batch = batch["image"]
        num_rows += batch.shape[0]
        if num_rows >= print_at:
            print(f"Read {num_rows} rows")
            print_at = ((num_rows // 1000) + 1) * 1000
    end = time.time()
    print(label, end - start, "epoch", i)

    tput = num_rows / (end - start)
    print(label, "tput", tput, "epoch", i)
    metrics[label] = tput

    if output_file is None:
        output_file = "output.csv"
    with open(output_file, "a+") as f:
        for label, tput in metrics.items():
            f.write(f"{label},{tput}\n")


def build_torch_dataset(
    root_dir, batch_size, shuffle=False, num_workers=None, transform=None
):
    if num_workers is None:
        num_workers = os.cpu_count()

    data = torchvision.datasets.ImageFolder(root_dir, transform=transform)
    data_loader = torch.utils.data.DataLoader(
        data,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        persistent_workers=True,
    )
    return data_loader


def parse_and_decode_tfrecord(example_serialized):
    feature_map = {
        "image/encoded": tf.io.FixedLenFeature(
            [], dtype=tf.string, default_value=""
        ),
        "image/class/label": tf.io.FixedLenFeature(
            [], dtype=tf.int64, default_value=-1
        ),
    }

    features = tf.io.parse_single_example(example_serialized, feature_map)
    label = tf.cast(features["image/class/label"], dtype=tf.int32)

    image_buffer = features["image/encoded"]
    image_buffer = tf.reshape(image_buffer, shape=[])
    image_buffer = tf.io.decode_jpeg(image_buffer, channels=3)
    return image_buffer, label


def tf_crop_and_flip(image_buffer, num_channels=3):
    """Crops the given image to a random part of the image, and randomly flips.

    We use the fused decode_and_crop op, which performs better than the two ops
    used separately in series, but note that this requires that the image be
    passed in as an un-decoded string Tensor.

    Args:
        image_buffer: scalar string Tensor representing the raw JPEG image buffer.
        bbox: 3-D float Tensor of bounding boxes arranged [1, num_boxes, coords]
            where each coordinate is [0, 1) and the coordinates are arranged as
            [ymin, xmin, ymax, xmax].
        num_channels: Integer depth of the image buffer for decoding.

    Returns:
        3-D tensor with cropped image.

    """
    # A large fraction of image datasets contain a human-annotated bounding box
    # delineating the region of the image containing the object of interest.    We
    # choose to create a new bounding box for the object which is a randomly
    # distorted version of the human-annotated bounding box that obeys an
    # allowed range of aspect ratios, sizes and overlap with the human-annotated
    # bounding box. If no box is supplied, then we assume the bounding box is
    # the entire image.
    shape = tf.shape(image_buffer)
    if len(shape) == num_channels + 1:
        shape = shape[1:]

    bbox = tf.constant(
        [0.0, 0.0, 1.0, 1.0], dtype=tf.float32, shape=[1, 1, 4]
    )  # From the entire image
    sample_distorted_bounding_box = tf.image.sample_distorted_bounding_box(
        shape,
        bounding_boxes=bbox,
        min_object_covered=0.1,
        aspect_ratio_range=[0.75, 1.33],
        area_range=[0.05, 1.0],
        max_attempts=100,
        use_image_if_no_bounding_boxes=True,
    )
    bbox_begin, bbox_size, _ = sample_distorted_bounding_box

    # Reassemble the bounding box in the format the crop op requires.
    offset_y, offset_x, _ = tf.unstack(bbox_begin)
    target_height, target_width, _ = tf.unstack(bbox_size)

    image_buffer = tf.image.crop_to_bounding_box(
        image_buffer,
        offset_height=offset_y,
        offset_width=offset_x,
        target_height=target_height,
        target_width=target_width,
    )
    # Flip to add a little more random distortion in.
    image_buffer = tf.image.random_flip_left_right(image_buffer)
    image_buffer = tf.compat.v1.image.resize(
        image_buffer,
        [DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE],
        method=tf.image.ResizeMethod.BILINEAR,
        align_corners=False,
    )
    return image_buffer


def build_tfrecords_tf_dataset(data_root, batch_size):
    filenames = [
        os.path.join(data_root, pathname) for pathname in os.listdir(data_root)
    ]
    ds = tf.data.Dataset.from_tensor_slices(filenames)
    ds = ds.interleave(tf.data.TFRecordDataset).map(
        parse_and_decode_tfrecord,
        num_parallel_calls=tf.data.experimental.AUTOTUNE,
    )
    ds = ds.map(lambda img, label: (tf_crop_and_flip(img), label))
    ds = ds.batch(batch_size)
    return ds


def decode_crop_and_flip_tf_record_batch(
    tf_record_batch: pd.DataFrame,
) -> pd.DataFrame:
    """
    This version of the preprocessor fuses the load step with the crop and flip
    step, which should have better performance (at the cost of re-executing the
    load step on each epoch):
    - the reference tf.data implementation can use the fused decode_and_crop op
    - ray.data doesn't have to materialize the intermediate decoded batch.
    """

    def process_images():
        for image_buffer in tf_record_batch["image/encoded"]:
            # Each image output is ~600KB.
            image_buffer = tf.reshape(image_buffer, shape=[])
            image_buffer = tf.io.decode_jpeg(image_buffer, channels=3)
            yield tf_crop_and_flip(image_buffer).numpy()

    labels = (tf_record_batch["image/class/label"]).astype("float32")
    df = pd.DataFrame.from_dict({"image": process_images(), "label": labels})

    return df


def get_transform(to_torch_tensor):
    # Note(swang): This is a different order from tf.data.
    # torch: decode -> randCrop+resize -> randFlip
    # tf.data: decode -> randCrop -> randFlip -> resize
    transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(
                antialias=True,
                size=DEFAULT_IMAGE_SIZE,
                scale=(0.05, 1.0),
                ratio=(0.75, 1.33),
            ),
            torchvision.transforms.RandomHorizontalFlip(),
        ]
        + ([torchvision.transforms.ToTensor()] if to_torch_tensor else [])
    )
    return transform


# Capture `transform`` in the map UDFs.
transform = get_transform(False)


def crop_and_flip_image(row):
    # Make sure to use torch.tensor here to avoid a copy from numpy.
    row["image"] = transform(
        torch.tensor(np.transpose(row["image"], axes=(2, 0, 1)))
    )
    return row


def crop_and_flip_image_batch(image_batch):
    image_batch["image"] = transform(
        # Make sure to use torch.tensor here to avoid a copy from numpy.
        # Original dims are (batch_size, channels, height, width).
        torch.tensor(np.transpose(image_batch["image"], axes=(0, 3, 1, 2)))
    )
    return image_batch


def decode_image_crop_and_flip(row):
    row["image"] = Image.frombytes(
        "RGB", (row["height"], row["width"]), row["image"]
    )
    # Convert back np to avoid storing a np.object array.
    return {"image": np.array(transform(row["image"]))}


class MdsDatasource(ray.data.datasource.FileBasedDatasource):
    _FILE_EXTENSION = "mds"

    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> Iterator[ray.data.block.Block]:
        file_info = streaming.base.format.base.reader.FileInfo(
            basename=os.path.basename(path),
            bytes=os.stat(path).st_size,
            hashes={},
        )
        reader = streaming.base.format.mds.MDSReader(
            dirname=os.path.dirname(path),
            split=None,
            column_encodings=["pil", "int"],
            column_names=["image", "label"],
            column_sizes=[None, 8],
            compression=None,
            hashes=[],
            raw_data=file_info,
            samples=-1,
            size_limit=None,
            zip_data=None,
        )

        i = 0
        while True:
            try:
                row = reader.decode_sample(reader.get_sample_data(i))
            except IndexError:
                break
            row["image"] = np.array(row["image"])
            builder = DelegatingBlockBuilder()
            builder.add(row)
            block = builder.build()
            yield block

            i += 1


class MosaicDataset(LocalDataset):
    def __init__(self, local: str, transforms: Callable) -> None:
        super().__init__(local=local)
        self.transforms = transforms

    def __getitem__(self, idx: int) -> Any:
        obj = super().__getitem__(idx)
        image = obj["image"]
        label = obj["label"]
        return self.transforms(image), label


class S3MosaicDataset(StreamingDataset):
    def __init__(
        self,
        s3_bucket: str,
        cache_dir: str,
        transforms: Callable,
        cache_limit=None,
    ) -> None:
        super().__init__(
            remote=s3_bucket, local=cache_dir, cache_limit=cache_limit
        )
        self.transforms = transforms

    def __getitem__(self, idx: int) -> Any:
        obj = super().__getitem__(idx)
        image = obj["image"]
        label = obj["label"]
        return self.transforms(image), label


def build_mosaic_dataloader(
    mosaic_data_root, batch_size, num_workers=None, tranform=None
):
    # MosaicML StreamingDataset.
    use_s3 = mosaic_data_root.startswith("s3://")

    if use_s3:
        MOSAIC_CACHE = "/tmp/mosaic_cache"
        try:
            import shutil

            shutil.rmtree(MOSAIC_CACHE)
        except FileNotFoundError:
            pass
        mosaic_ds = S3MosaicDataset(
            s3_bucket=args.mosaic_data_root,
            cache_dir=MOSAIC_CACHE,
            cache_limit="100gb",
            transforms=transform,
        )
        if args.torch_num_workers is None:
            mosaic_num_workers = os.cpu_count() * 4
        else:
            mosaic_num_workers = args.torch_num_workers
    else:
        mosaic_ds = MosaicDataset(
            args.mosaic_data_root, transforms=get_transform(True)
        )
        if args.torch_num_workers is None:
            mosaic_num_workers = os.cpu_count()
        else:
            mosaic_num_workers = args.torch_num_workers
    mosaic_dl = torch.utils.data.DataLoader(
        mosaic_ds, batch_size=args.batch_size, num_workers=mosaic_num_workers
    )
    return mosaic_dl


def build_hf_dataloader(
    data_root, batch_size, from_images, num_workers=None, transform=None
):
    if num_workers is None:
        num_workers = os.cpu_count()

    transform = get_transform(True)

    def transforms(examples):
        if from_images:
            examples["image"] = [
                transform(image.convert("RGB")) for image in examples["image"]
            ]
        else:
            examples["image"] = [
                transform(Image.frombytes("RGB", (height, width), image))
                for image, height, width in zip(
                    examples["image"], examples["height"], examples["width"]
                )
            ]
        return examples

    if from_images:
        dataset = load_dataset(
            "imagefolder",
            data_dir=data_root,
            split="train",
            num_proc=num_workers,
        )
    else:
        dataset = load_dataset("parquet", data_dir=data_root, split="train")
    if transform is not None:
        dataset = dataset.with_transform(transforms)

    def collate_fn(examples):
        images = []
        labels = []
        for example in examples:
            images.append((example["image"]))
            if "label" in example:
                labels.append(example["label"])
            else:
                labels.append(0)

        pixel_values = torch.stack(images)
        labels = torch.tensor(labels)
        return {"image": pixel_values, "label": labels}

    return torch.utils.data.DataLoader(
        dataset,
        collate_fn=collate_fn,
        batch_size=batch_size,
        num_workers=num_workers,
    )


def setup_alluxio(args):
    fsspec.register_implementation(
        "alluxiofs", AlluxioFileSystem, clobber=True
    )
    alluxio_kwargs = {}
    if args.alluxio_etcd_hosts and args.alluxio_worker_hosts:
        raise ValueError(
            "Either etcd_hosts or worker_hosts should be provided, not both."
        )
    if args.alluxio_etcd_hosts:
        alluxio_kwargs["etcd_hosts"] = args.alluxio_etcd_hosts
    if args.alluxio_worker_hosts:
        alluxio_kwargs["worker_hosts"] = args.alluxio_worker_hosts
    alluxio_kwargs["target_protocol"] = "s3"

    alluxio_options = {}
    if args.alluxio_page_size:
        alluxio_options[
            "alluxio.worker.page.store.page.size"
        ] = args.alluxio_page_size
    if args.use_alluxiocommon:
        alluxio_options["alluxio.common.extension.enable"] = "True"
    if args.alluxio_cluster_name:
        alluxio_options["alluxio.cluster.name"] = args.alluxio_cluster_name
    if alluxio_options:
        alluxio_kwargs["options"] = alluxio_options
    # alluxio_kwargs["test_options"] = {"log_level" : "DEBUG"}
    return fsspec.filesystem("alluxiofs", **alluxio_kwargs)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run single-node batch iteration benchmarks."
    )

    parser.add_argument(
        "--data-root",
        default=None,
        type=str,
        help=(
            "Directory path with raw images. Directory structure should be "
            '"<data_root>/train/<class>/<image file>"'
        ),
    )
    parser.add_argument(
        "--parquet-data-root",
        default=None,
        type=str,
        help=(
            "Directory path with Parquet files. Directory structure should be "
            '"<data_root>/*.parquet"'
        ),
    )
    parser.add_argument(
        "--mosaic-data-root",
        default=None,
        type=str,
        help=(
            "Directory path with MDS files. Directory structure should be "
            '"<data_root>/*.mds"'
        ),
    )
    parser.add_argument(
        "--tf-data-root",
        default=None,
        type=str,
        help=(
            "Directory path with TFRecords files. Directory structure should "
            'be "<data_root>/train/<tfrecords files>"'
        ),
    )
    parser.add_argument(
        "--batch-size",
        default=32,
        type=int,
        help="Batch size to use.",
    )
    parser.add_argument(
        "--num-epochs",
        default=3,
        type=int,
        help="Number of epochs to run. The throughput for the last epoch will be kept.",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        type=str,
        help="Output CSV path.",
    )
    parser.add_argument(
        "--torch-num-workers",
        default=None,
        type=int,
        help=(
            "Number of workers to pass to torch. By default # CPUs will be "
            "used, x4 for S3 datasets."
        ),
    )
    # Alluxio related
    parser.add_argument(
        "--use-alluxio",
        action="store_true",
        default=False,
        help="Whether to use Alluxio instead of original ufs filesystem for data loading.",
    )
    parser.add_argument(
        "--use-alluxiocommon",
        action="store_true",
        default=False,
        help="Whether to use AlluxioCommon native extension lib.",
    )
    parser.add_argument(
        "--alluxio-etcd-hosts",
        default=None,
        help="The ETCD host to connect to to get Alluxio workers connection info.",
    )
    parser.add_argument(
        "--alluxio-worker-hosts",
        default=None,
        help="The worker hostnames in host1,host2,host3 format. Either etcd_host or worker_hosts should be provided, not both.",
    )
    parser.add_argument(
        "--alluxio-page-size",
        default=None,
        help="The alluxio page size of Alluxio servers.",
    )
    parser.add_argument(
        "--alluxio-cluster-name",
        default=None,
        help="The alluxio cluster name of the Alluxio servers.",
    )
    args = parser.parse_args()

    metrics = {}

    if args.data_root is not None:
        # TF TEST
        """
        # tf.data, load images.
        tf_dataset = tf.keras.preprocessing.image_dataset_from_directory(
            args.data_root,
            batch_size=args.batch_size,
            image_size=(DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE),
        )
        for i in range(args.num_epochs):
            iterate(tf_dataset, "tf_data", args.batch_size, metrics, args.output_file)

        # tf.data, with transform.
        tf_dataset = tf.keras.preprocessing.image_dataset_from_directory(args.data_root)
        tf_dataset = tf_dataset.map(lambda img, label: (tf_crop_and_flip(img), label))
        tf_dataset.unbatch().batch(args.batch_size)
        for i in range(args.num_epochs):
            iterate(
                tf_dataset,
                "tf_data+transform",
                args.batch_size,
                metrics,
                args.output_file,
            )
        """

        # TORCH TEST
        # torch, load images.
        """
        torch_resize_transform = torchvision.transforms.Compose(
            [
                torchvision.transforms.Resize(
                    (DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE)
                ),
                torchvision.transforms.ToTensor(),
            ]
        )
        torch_dataset = build_torch_dataset(
            args.data_root,
            args.batch_size,
            num_workers=args.torch_num_workers,
            transform=torch_resize_transform,
        )
        for i in range(args.num_epochs):
            iterate(
                torch_dataset,
                "torch",
                args.batch_size,
                metrics,
                args.output_file,
            )

        # torch, with transform.
        torch_dataset = build_torch_dataset(
            args.data_root,
            args.batch_size,
            num_workers=args.torch_num_workers,
            transform=get_transform(True),
        )
        for i in range(args.num_epochs):
            iterate(
                torch_dataset,
                "torch+transform",
                args.batch_size,
                metrics,
                args.output_file,
            )

        # HuggingFace Datasets, load images.
        hf_dataset = build_hf_dataloader(
            args.data_root,
            args.batch_size,
            from_images=True,
            num_workers=args.torch_num_workers,
            transform=torch_resize_transform,
        )
        for i in range(args.num_epochs):
            iterate(
                hf_dataset,
                "HF",
                args.batch_size,
                metrics,
                args.output_file,
            )

        # HuggingFace Datasets, with transform.
        hf_dataset = build_hf_dataloader(
            args.data_root,
            args.batch_size,
            from_images=True,
            num_workers=args.torch_num_workers,
            transform=get_transform(True),
        )
        for i in range(args.num_epochs):
            iterate(
                hf_dataset,
                "HF+transform",
                args.batch_size,
                metrics,
                args.output_file,
            )
        """

        alluxio = setup_alluxio(args)
        ray_dataset = ray.data.read_images(
            args.data_root,
            mode="RGB",
            size=(DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE),
            filesystem=setup_alluxio(args) if args.use_alluxio else None,
        )
        for i in range(args.num_epochs):
            iterate(
                ray_dataset.iter_torch_batches(batch_size=args.batch_size),
                "ray_data",
                args.batch_size,
                metrics,
                args.output_file,
            )

        # ray.data, with transform.
        ray_dataset = ray.data.read_images(
            args.data_root,
            mode="RGB",
            filesystem=setup_alluxio(args) if args.use_alluxio else None,
        ).map(crop_and_flip_image)
        for i in range(args.num_epochs):
            iterate(
                ray_dataset.iter_batches(batch_size=args.batch_size),
                "ray_data+map_transform",
                args.batch_size,
                metrics,
                args.output_file,
            )

    if args.tf_data_root is not None:
        # TFRecords dataset.
        tf_dataset = build_tfrecords_tf_dataset(
            args.tf_data_root, args.batch_size
        )
        for i in range(args.num_epochs):
            iterate(
                tf_dataset,
                "tf_data_tfrecords+transform",
                args.batch_size,
                metrics,
                args.output_file,
            )

        # TFRecords dataset with Ray Data.
        ray_dataset = ray.data.read_tfrecords(
            args.tf_data_root,
            filesystem=setup_alluxio(args) if args.use_alluxio else None,
        )
        ray_dataset = ray_dataset.map_batches(
            decode_crop_and_flip_tf_record_batch,
            batch_size=args.batch_size,
            batch_format="pandas",
        )
        for i in range(args.num_epochs):
            iterate(
                ray_dataset.to_tf(
                    batch_size=args.batch_size,
                    feature_columns="image",
                    label_columns="label",
                ),
                "ray_data_tfrecords+transform",
                args.batch_size,
                metrics,
                args.output_file,
            )

    if args.parquet_data_root is not None:
        # HuggingFace Dataset, reading from parquet.
        """
        hf_dataset = build_hf_dataloader(
            args.parquet_data_root,
            args.batch_size,
            from_images=False,
            num_workers=args.torch_num_workers,
            transform=get_transform(True),
        )
        for i in range(args.num_epochs):
            iterate(
                hf_dataset,
                "HF_parquet+transform",
                args.batch_size,
                metrics,
                args.output_file,
            )
        """

        # Ray Data, reading from parquet.
        ray_dataset = ray.data.read_parquet(
            args.parquet_data_root,
            filesystem=setup_alluxio(args) if args.use_alluxio else None,
        )
        ray_dataset = ray_dataset.map(decode_image_crop_and_flip)
        for i in range(args.num_epochs):
            iterate(
                ray_dataset.iter_torch_batches(batch_size=args.batch_size),
                "ray_data_parquet+map_transform",
                args.batch_size,
                metrics,
                args.output_file,
            )

    if args.mosaic_data_root is not None:
        # MosaicML StreamingDataset.
        mosaic_dl = build_mosaic_dataloader(
            args.mosaic_data_root,
            batch_size=args.batch_size,
            num_workers=args.torch_num_workers,
            transform=get_transform(True),
        )
        for i in range(args.num_epochs):
            iterate(
                mosaic_dl,
                "mosaicml_mds",
                args.batch_size,
                metrics,
                args.output_file,
            )

    metrics_dict = {}
    with open("output.csv", "w+") as f:
        for label, tput in metrics.items():
            f.write(f"{label},{tput}\n")
