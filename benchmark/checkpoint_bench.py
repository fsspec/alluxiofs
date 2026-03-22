"""
Checkpoint I/O Benchmark: AlluxioCheckpointWriter vs torch.save to local/S3.

Measures wall-clock time and effective throughput for writing and reading back
a model state_dict of configurable size (default: ~7B-parameter GPT-style
model, ~14 GB in bfloat16).

Usage
-----
    # Benchmark against local disk (no Alluxio needed)
    python benchmark/checkpoint_bench.py --mode local --size-gb 1

    # Benchmark against Alluxio
    python benchmark/checkpoint_bench.py \\
        --mode alluxio \\
        --worker-hosts host1,host2 \\
        --checkpoint-dir s3://bucket/checkpoints/bench \\
        --size-gb 14 \\
        --num-shards 8

Output
------
    JSON results printed to stdout and optionally saved to --output-json.
    A matplotlib bar chart is saved as checkpoint_bench.png.
"""

import argparse
import io
import json
import os
import sys
import tempfile
import time
from typing import Dict, Optional

# Graceful import of optional dependencies
_TORCH_AVAILABLE = False
try:
    import torch

    _TORCH_AVAILABLE = True
except ImportError:
    pass


def _make_fake_state_dict(size_gb: float) -> dict:
    """
    Build a synthetic state_dict that serializes to approximately ``size_gb`` GB.

    Uses bfloat16 tensors to match typical LLM checkpoint dtype.
    """
    if not _TORCH_AVAILABLE:
        raise ImportError("torch is required. pip install torch")

    target_bytes = int(size_gb * 1e9)
    dtype = torch.bfloat16
    bytes_per_elem = 2  # bfloat16

    # Simulate a transformer with a mix of large weight matrices
    layer_shapes = {
        "embed_tokens.weight": (32000, 4096),  # token embedding
        "lm_head.weight": (32000, 4096),
    }
    # Fill the rest with transformer layer weights
    remaining_bytes = target_bytes - sum(
        r * c * bytes_per_elem for r, c in layer_shapes.values()
    )
    n_layers = max(1, remaining_bytes // (4 * 4096 * 4096 * bytes_per_elem))
    for i in range(int(n_layers)):
        layer_shapes[f"layers.{i}.self_attn.q_proj.weight"] = (4096, 4096)
        layer_shapes[f"layers.{i}.self_attn.k_proj.weight"] = (4096, 4096)
        layer_shapes[f"layers.{i}.self_attn.v_proj.weight"] = (4096, 4096)
        layer_shapes[f"layers.{i}.self_attn.o_proj.weight"] = (4096, 4096)

    state_dict = {}
    for name, shape in layer_shapes.items():
        state_dict[name] = torch.randn(shape, dtype=dtype)

    return state_dict


def _serialized_size_gb(state_dict: dict) -> float:
    buf = io.BytesIO()
    torch.save(state_dict, buf)
    return len(buf.getvalue()) / 1e9


# -------------------------------------------------------------------------
# Backends
# -------------------------------------------------------------------------


def bench_torch_save_local(state_dict: dict, tmpdir: str) -> Dict:
    """Baseline: torch.save to a single local file."""
    path = os.path.join(tmpdir, "checkpoint.pt")

    # Write
    t0 = time.perf_counter()
    torch.save(state_dict, path)
    write_s = time.perf_counter() - t0
    size_gb = os.path.getsize(path) / 1e9

    # Read
    t0 = time.perf_counter()
    _ = torch.load(path, map_location="cpu")
    read_s = time.perf_counter() - t0

    return {
        "backend": "torch.save (local disk)",
        "size_gb": size_gb,
        "write_s": write_s,
        "read_s": read_s,
        "write_throughput_gb_s": size_gb / write_s,
        "read_throughput_gb_s": size_gb / read_s,
    }


def bench_alluxio_checkpoint(
    state_dict: dict,
    worker_hosts: str,
    checkpoint_dir: str,
    num_shards: int,
    max_workers: int,
) -> Dict:
    """AlluxioCheckpointWriter: parallel sharded write + prefetch read."""
    from alluxiofs import AlluxioClient
    from alluxiofs.checkpoint import CheckpointReader, CheckpointWriter

    client = AlluxioClient(worker_hosts=worker_hosts)
    writer = CheckpointWriter(client, num_shards=num_shards, max_workers=max_workers)

    # Write
    write_stats = writer.save(state_dict, checkpoint_dir)

    # Read with prefetch
    reader = CheckpointReader(client, max_workers=max_workers)
    reader.prefetch(checkpoint_dir)  # start background fetch immediately
    time.sleep(0.05)  # simulate brief model init
    t0 = time.perf_counter()
    _ = reader.load(checkpoint_dir)
    read_s = time.perf_counter() - t0

    size_gb = write_stats["total_bytes"] / 1e9
    return {
        "backend": f"AlluxioCheckpointWriter (shards={num_shards})",
        "size_gb": size_gb,
        "write_s": write_stats["elapsed_s"],
        "write_serialize_s": write_stats["serialize_s"],
        "read_s": read_s,
        "write_throughput_gb_s": write_stats["throughput_gb_s"],
        "read_throughput_gb_s": size_gb / read_s,
    }


# -------------------------------------------------------------------------
# Plotting
# -------------------------------------------------------------------------


def plot_results(results: list, output_png: str = "checkpoint_bench.png"):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; skipping plot. pip install matplotlib")
        return

    labels = [r["backend"] for r in results]
    write_tp = [r["write_throughput_gb_s"] for r in results]
    read_tp = [r["read_throughput_gb_s"] for r in results]

    x = range(len(labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 5))
    bars_w = ax.bar([i - width / 2 for i in x], write_tp, width, label="Write GB/s")
    bars_r = ax.bar([i + width / 2 for i in x], read_tp, width, label="Read GB/s")
    ax.set_ylabel("Throughput (GB/s)")
    ax.set_title("Checkpoint I/O Throughput: Alluxio vs Baseline")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.legend()
    ax.bar_label(bars_w, fmt="%.2f", padding=3)
    ax.bar_label(bars_r, fmt="%.2f", padding=3)
    fig.tight_layout()
    plt.savefig(output_png, dpi=150)
    print(f"Saved chart to {output_png}")


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint I/O benchmark: Alluxio vs torch.save"
    )
    parser.add_argument(
        "--mode",
        choices=["local", "alluxio", "both"],
        default="local",
        help="Which backends to benchmark (default: local)",
    )
    parser.add_argument(
        "--size-gb",
        type=float,
        default=1.0,
        help="Approximate checkpoint size in GB (default: 1.0)",
    )
    parser.add_argument(
        "--worker-hosts",
        default="localhost",
        help="Comma-separated Alluxio worker hosts",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="s3://my-bucket/benchmarks/checkpoint",
        help="Alluxio checkpoint directory path",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=8,
        help="Number of parallel shards for AlluxioCheckpointWriter",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Thread pool size for parallel shard I/O",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Optional path to save results as JSON",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate a matplotlib bar chart",
    )
    args = parser.parse_args()

    if not _TORCH_AVAILABLE:
        print("ERROR: torch not installed. pip install torch")
        sys.exit(1)

    print(f"Building synthetic state_dict (~{args.size_gb} GB)…")
    state_dict = _make_fake_state_dict(args.size_gb)
    actual_gb = _serialized_size_gb(state_dict)
    print(f"  Serialized size: {actual_gb:.3f} GB")

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        if args.mode in ("local", "both"):
            print("\n[1/2] Benchmarking torch.save (local disk)…")
            r = bench_torch_save_local(state_dict, tmpdir)
            results.append(r)
            print(
                f"  Write: {r['write_s']:.2f}s  ({r['write_throughput_gb_s']:.3f} GB/s)"
            )
            print(
                f"  Read : {r['read_s']:.2f}s  ({r['read_throughput_gb_s']:.3f} GB/s)"
            )

        if args.mode in ("alluxio", "both"):
            print("\n[2/2] Benchmarking AlluxioCheckpointWriter…")
            r = bench_alluxio_checkpoint(
                state_dict,
                args.worker_hosts,
                args.checkpoint_dir,
                args.num_shards,
                args.max_workers,
            )
            results.append(r)
            print(
                f"  Write: {r['write_s']:.2f}s  ({r['write_throughput_gb_s']:.3f} GB/s)"
            )
            print(
                f"  Read : {r['read_s']:.2f}s  ({r['read_throughput_gb_s']:.3f} GB/s)"
            )

    print("\n=== Results (JSON) ===")
    print(json.dumps(results, indent=2))

    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output_json}")

    if args.plot:
        plot_results(results)


if __name__ == "__main__":
    main()
