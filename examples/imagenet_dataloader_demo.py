"""
Demo: PyTorch DataLoader with AlluxioDataset and AlluxioIterableDataset.

Shows how to use Alluxio-backed datasets for image training with:
  - AlluxioDataset (map-style) + DistributedSampler for DDP
  - AlluxioIterableDataset (streaming) for large datasets
  - Throughput measurement (images/sec, MB/sec)
  - Cache warm vs cold comparison

Usage
-----
    # Map-style dataset (requires Alluxio + S3 with images)
    python examples/imagenet_dataloader_demo.py \\
        --worker-hosts host1,host2 \\
        --data-dir s3://my-bucket/imagenet/train \\
        --batch-size 64 --num-workers 8

    # Dry-run: generates synthetic PNG files in /tmp
    python examples/imagenet_dataloader_demo.py --dry-run --num-files 200
"""

import argparse
import io
import os
import sys
import tempfile
import time
from typing import Optional

_TORCH_AVAILABLE = False
try:
    import torch
    from torch.utils.data import DataLoader

    _TORCH_AVAILABLE = True
except ImportError:
    pass

_PIL_AVAILABLE = False
try:
    from PIL import Image
    import torchvision.transforms as T

    _PIL_AVAILABLE = True
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


def make_image_transform(image_size: int = 224):
    """Standard ImageNet preprocessing pipeline."""
    if not _PIL_AVAILABLE:
        # Fallback: return raw bytes (useful for throughput testing without PIL)
        return lambda raw: torch.frombuffer(bytearray(raw), dtype=torch.uint8)

    return T.Compose(
        [
            T.Resize(256),
            T.CenterCrop(image_size),
            T.ToTensor(),
            T.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )


def decode_image(raw: bytes):
    """Decode raw JPEG/PNG bytes → float32 tensor (C, H, W)."""
    if _PIL_AVAILABLE:
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        return make_image_transform()(img)
    return torch.zeros(3, 224, 224)


# ---------------------------------------------------------------------------
# Synthetic dataset for dry-run
# ---------------------------------------------------------------------------


def create_synthetic_dataset(tmpdir: str, n_files: int, image_size: int = 64) -> str:
    """Write n_files synthetic PNG images to tmpdir."""
    import random

    if not _PIL_AVAILABLE:
        # Write random bytes files if PIL not available
        for i in range(n_files):
            path = os.path.join(tmpdir, f"img_{i:05d}.bin")
            with open(path, "wb") as f:
                f.write(bytes(random.randint(0, 255) for _ in range(8192)))
        return tmpdir

    print(f"  Generating {n_files} synthetic images ({image_size}x{image_size})…")
    for i in range(n_files):
        arr = bytes(
            [random.randint(0, 255) for _ in range(image_size * image_size * 3)]
        )
        img = Image.frombytes("RGB", (image_size, image_size), arr)
        img.save(os.path.join(tmpdir, f"img_{i:05d}.png"))
    return tmpdir


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


def benchmark_dataloader(loader, max_batches: int = 50, desc: str = "") -> dict:
    """Iterate ``max_batches`` batches and measure throughput."""
    total_samples = 0
    total_bytes = 0
    t0 = time.perf_counter()
    n_batches = 0

    for batch in loader:
        if isinstance(batch, (list, tuple)):
            item = batch[0]
        else:
            item = batch
        if hasattr(item, "nbytes"):
            total_bytes += item.nbytes
        elif isinstance(item, bytes):
            total_bytes += len(item)
        if hasattr(item, "shape"):
            total_samples += item.shape[0]
        else:
            total_samples += 1
        n_batches += 1
        if n_batches >= max_batches:
            break

    elapsed = time.perf_counter() - t0
    return {
        "description": desc,
        "batches": n_batches,
        "samples": total_samples,
        "elapsed_s": elapsed,
        "images_per_sec": total_samples / elapsed if elapsed > 0 else 0,
        "mb_per_sec": total_bytes / 1e6 / elapsed if elapsed > 0 else 0,
    }


def print_results(results: list) -> None:
    print("\n=== Throughput Results ===")
    for r in results:
        print(
            f"  [{r['description']}] "
            f"{r['images_per_sec']:.1f} img/s  "
            f"{r['mb_per_sec']:.1f} MB/s  "
            f"(batches={r['batches']}, elapsed={r['elapsed_s']:.2f}s)"
        )
    print()


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="AlluxioDataset DataLoader throughput demo"
    )
    parser.add_argument("--worker-hosts", default="localhost")
    parser.add_argument("--data-dir", default="s3://my-bucket/imagenet/train")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--max-batches", type=int, default=20)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate synthetic data in /tmp; no Alluxio needed",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=100,
        help="Number of synthetic files for dry-run",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Use AlluxioIterableDataset instead of AlluxioDataset",
    )
    args = parser.parse_args()

    if not _TORCH_AVAILABLE:
        print("ERROR: torch is required. pip install torch")
        sys.exit(1)

    print("=== AlluxioDataset DataLoader Demo ===\n")

    tmpdir_ctx = None
    results = []

    try:
        if args.dry_run:
            # --- Dry-run: local filesystem ---
            print("[dry-run] Using local filesystem via fsspec memory:// backend")
            tmpdir_ctx = tempfile.TemporaryDirectory()
            data_dir = create_synthetic_dataset(
                tmpdir_ctx.name, args.num_files, image_size=64
            )

            # Use fsspec local filesystem
            import fsspec

            local_fs = fsspec.filesystem("file")

            from alluxiofs.torch_dataset import AlluxioDataset, AlluxioIterableDataset

            transform = decode_image if _PIL_AVAILABLE else None

            if args.streaming:
                print("\n[AlluxioIterableDataset] streaming mode")
                dataset = AlluxioIterableDataset(
                    local_fs,
                    data_dir,
                    transform=transform,
                    file_filter=lambda p: p.endswith((".png", ".bin")),
                )
                loader = DataLoader(dataset, batch_size=args.batch_size, num_workers=0)
            else:
                print("\n[AlluxioDataset] map-style mode")
                dataset = AlluxioDataset(
                    local_fs,
                    data_dir,
                    transform=transform,
                    file_filter=lambda p: p.endswith((".png", ".bin")),
                )
                print(f"  Dataset size: {len(dataset)} files")
                loader = DataLoader(
                    dataset,
                    batch_size=args.batch_size,
                    num_workers=min(args.num_workers, 2),
                    shuffle=True,
                )

            r = benchmark_dataloader(
                loader,
                max_batches=args.max_batches,
                desc="AlluxioDataset (dry-run, local)",
            )
            results.append(r)

        else:
            # --- Real Alluxio + S3 ---
            from alluxiofs import AlluxioFileSystem
            from alluxiofs.telemetry import instrument_alluxio_client
            from alluxiofs.torch_dataset import AlluxioDataset, AlluxioIterableDataset

            print(f"Connecting to Alluxio: {args.worker_hosts}")
            fs = AlluxioFileSystem(
                worker_hosts=args.worker_hosts,
                target_protocol="s3",
            )

            # Instrument the underlying AlluxioClient
            telem = instrument_alluxio_client(fs.alluxio)

            transform = decode_image

            if args.streaming:
                dataset = AlluxioIterableDataset(
                    fs,
                    args.data_dir,
                    transform=transform,
                    file_filter=lambda p: p.endswith((".jpg", ".jpeg", ".png")),
                )
                loader = DataLoader(
                    dataset,
                    batch_size=args.batch_size,
                    num_workers=args.num_workers,
                )
                desc = f"AlluxioIterableDataset (workers={args.num_workers})"
            else:
                dataset = AlluxioDataset(
                    fs,
                    args.data_dir,
                    transform=transform,
                    file_filter=lambda p: p.endswith((".jpg", ".jpeg", ".png")),
                )
                print(f"  Dataset size: {len(dataset)} files")

                loader = DataLoader(
                    dataset,
                    batch_size=args.batch_size,
                    num_workers=args.num_workers,
                    shuffle=True,
                    prefetch_factor=2 if args.num_workers > 0 else None,
                    persistent_workers=args.num_workers > 0,
                )
                desc = f"AlluxioDataset (workers={args.num_workers})"

            # Cold cache pass
            print(f"\n[Cold cache] {desc}")
            r_cold = benchmark_dataloader(
                loader, max_batches=args.max_batches, desc=f"{desc} cold"
            )
            results.append(r_cold)

            # Warm cache pass (Alluxio has now cached the pages)
            print(f"\n[Warm cache] {desc}")
            r_warm = benchmark_dataloader(
                loader, max_batches=args.max_batches, desc=f"{desc} warm"
            )
            results.append(r_warm)

            telem.print_summary()

    finally:
        if tmpdir_ctx:
            tmpdir_ctx.cleanup()

    print_results(results)
    print("Demo complete.")


if __name__ == "__main__":
    main()
