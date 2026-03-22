"""
End-to-end demo: parallel LLM checkpoint write + prefetch read with Alluxio.

Simulates the checkpoint cycle of a GPT-2 style training loop:
  1. Forward + backward pass (simulated with a sleep)
  2. Checkpoint write: shard model state_dict in parallel to Alluxio
  3. Crash + resume: prefetch shards in background while model re-initializes
  4. Load state_dict from prefetched shards → resume training

Works without a GPU. Replace GPT2Config with any model for real training.

Usage
-----
    # Requires: pip install torch transformers alluxiofs
    python examples/llm_checkpoint_demo.py \\
        --worker-hosts host1,host2,host3 \\
        --checkpoint-dir s3://my-bucket/demo/gpt2_ckpt \\
        --num-shards 4 \\
        --steps 3

    # Quick local smoke-test (skips Alluxio, writes to /tmp)
    python examples/llm_checkpoint_demo.py --dry-run
"""

import argparse
import io
import os
import sys
import tempfile
import time

_TORCH_AVAILABLE = False
try:
    import torch
    import torch.nn as nn

    _TORCH_AVAILABLE = True
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Tiny GPT-2-style model (no external deps beyond torch)
# ---------------------------------------------------------------------------


def build_model(vocab_size: int = 50257, n_embd: int = 768, n_layer: int = 12):
    """
    Build a minimal transformer language model for demo purposes.

    For a real experiment, swap this with:
        from transformers import GPT2LMHeadModel, GPT2Config
        config = GPT2Config(n_embd=1024, n_layer=24, n_head=16)
        model = GPT2LMHeadModel(config)
    """
    if not _TORCH_AVAILABLE:
        raise ImportError("pip install torch")

    class TinyTransformer(nn.Module):
        def __init__(self):
            super().__init__()
            self.embed = nn.Embedding(vocab_size, n_embd)
            self.layers = nn.ModuleList(
                [
                    nn.TransformerEncoderLayer(
                        d_model=n_embd,
                        nhead=8,
                        dim_feedforward=n_embd * 4,
                        batch_first=True,
                    )
                    for _ in range(n_layer)
                ]
            )
            self.head = nn.Linear(n_embd, vocab_size, bias=False)

        def forward(self, x):
            x = self.embed(x)
            for layer in self.layers:
                x = layer(x)
            return self.head(x)

    return TinyTransformer()


# ---------------------------------------------------------------------------
# Alluxio checkpoint helpers
# ---------------------------------------------------------------------------


def checkpoint_write_alluxio(
    state_dict: dict,
    client,
    checkpoint_dir: str,
    num_shards: int,
    step: int,
) -> dict:
    from alluxiofs.checkpoint import CheckpointWriter

    writer = CheckpointWriter(client, num_shards=num_shards, max_workers=num_shards)
    stats = writer.save(
        state_dict,
        checkpoint_dir,
        metadata={"step": step, "framework": "pytorch"},
    )
    return stats


def checkpoint_load_alluxio(client, checkpoint_dir: str, num_shards: int) -> dict:
    from alluxiofs.checkpoint import CheckpointReader

    reader = CheckpointReader(client, max_workers=num_shards)
    # Kick off background prefetch immediately
    reader.prefetch(checkpoint_dir)

    # Simulate work happening in parallel (e.g., optimizer re-init)
    print("  [prefetch started] re-initializing model/optimizer…")
    time.sleep(0.5)

    # load() waits for prefetched shards → near-instant if prefetch completed
    state_dict = reader.load(checkpoint_dir)
    reader.close()
    return state_dict


# ---------------------------------------------------------------------------
# Dry-run (no Alluxio): torch.save / torch.load to tempdir
# ---------------------------------------------------------------------------


def checkpoint_write_local(state_dict: dict, tmpdir: str, step: int) -> dict:
    path = os.path.join(tmpdir, f"checkpoint_step{step}.pt")
    t0 = time.perf_counter()
    torch.save(state_dict, path)
    elapsed = time.perf_counter() - t0
    size = os.path.getsize(path)
    return {
        "path": path,
        "total_bytes": size,
        "elapsed_s": elapsed,
        "throughput_gb_s": size / elapsed / 1e9,
    }


def checkpoint_load_local(path: str) -> dict:
    return torch.load(path, map_location="cpu")


# ---------------------------------------------------------------------------
# Training loop simulation
# ---------------------------------------------------------------------------


def simulate_training_step(model, step: int, device: str = "cpu"):
    """One fake forward+backward pass (no real data needed for demo)."""
    model.train()
    x = torch.randint(0, 50257, (2, 16)).to(device)
    out = model(x)
    loss = out.mean()
    loss.backward()
    return float(loss.detach())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="LLM checkpoint write/read demo with Alluxio"
    )
    parser.add_argument(
        "--worker-hosts",
        default="localhost",
        help="Comma-separated Alluxio worker hosts",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="s3://my-bucket/demo/gpt2_ckpt",
        help="Base Alluxio path for checkpoints",
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=4,
        help="Number of parallel shards (default: 4)",
    )
    parser.add_argument(
        "--steps",
        type=int,
        default=3,
        help="Number of simulated training steps",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Alluxio; write to /tmp instead (smoke test)",
    )
    parser.add_argument(
        "--n-layer",
        type=int,
        default=4,
        help="Number of transformer layers in demo model (default: 4)",
    )
    args = parser.parse_args()

    if not _TORCH_AVAILABLE:
        print("ERROR: torch is required. pip install torch")
        sys.exit(1)

    print("=== LLM Checkpoint Demo (Alluxio parallel shard I/O) ===\n")
    print(f"Building model (n_layer={args.n_layer})…")
    model = build_model(n_layer=args.n_layer)
    n_params = sum(p.numel() for p in model.parameters())
    print(f"  Parameters: {n_params / 1e6:.1f}M\n")

    client = None
    tmpdir_ctx = None

    if args.dry_run:
        print("[dry-run] Using local /tmp instead of Alluxio\n")
        tmpdir_ctx = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_ctx.name
    else:
        from alluxiofs import AlluxioClient
        from alluxiofs.telemetry import instrument_alluxio_client

        client = AlluxioClient(worker_hosts=args.worker_hosts)
        telem = instrument_alluxio_client(client)
        print(f"Connected to Alluxio workers: {args.worker_hosts}\n")

    try:
        last_checkpoint_path = None
        for step in range(1, args.steps + 1):
            print(f"--- Step {step}/{args.steps} ---")

            # Simulate forward/backward
            loss = simulate_training_step(model, step)
            print(f"  Loss: {loss:.4f}")

            # Checkpoint every step in this demo
            t_ckpt = time.perf_counter()
            state_dict = model.state_dict()

            if args.dry_run:
                stats = checkpoint_write_local(state_dict, tmpdir, step)
                last_checkpoint_path = stats["path"]
                print(
                    f"  [local] wrote {stats['total_bytes'] / 1e6:.1f} MB "
                    f"in {stats['elapsed_s']:.3f}s "
                    f"({stats['throughput_gb_s']:.3f} GB/s)"
                )
            else:
                ckpt_dir = f"{args.checkpoint_dir}/step_{step:06d}"
                stats = checkpoint_write_alluxio(
                    state_dict, client, ckpt_dir, args.num_shards, step
                )
                last_checkpoint_path = ckpt_dir
                print(
                    f"  [alluxio] wrote {stats['total_bytes'] / 1e6:.1f} MB "
                    f"in {stats['elapsed_s']:.3f}s "
                    f"({stats['throughput_gb_s']:.3f} GB/s) "
                    f"across {stats['num_shards']} shards"
                )

            print(f"  Checkpoint overhead: {time.perf_counter() - t_ckpt:.3f}s\n")

        # --- Resume simulation ---
        print("=== Simulating crash + resume from last checkpoint ===\n")
        print(f"  Checkpoint path: {last_checkpoint_path}")
        model_fresh = build_model(n_layer=args.n_layer)

        t_load = time.perf_counter()
        if args.dry_run:
            loaded_state = checkpoint_load_local(last_checkpoint_path)
        else:
            loaded_state = checkpoint_load_alluxio(
                client, last_checkpoint_path, args.num_shards
            )
        model_fresh.load_state_dict(loaded_state)
        print(f"  Resume completed in {time.perf_counter() - t_load:.3f}s\n")

        # Verify: one more forward pass to confirm correctness
        loss_resumed = simulate_training_step(model_fresh, step=0)
        print(f"  First loss after resume: {loss_resumed:.4f}")

        if not args.dry_run:
            print()
            telem.print_summary()

    finally:
        if tmpdir_ctx:
            tmpdir_ctx.cleanup()

    print("\nDemo complete.")


if __name__ == "__main__":
    main()
