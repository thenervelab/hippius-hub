#!/usr/bin/env python3
"""Benchmark phase 1 of the upload pipeline: `chunk_and_hash_native` throughput.

Isolates the CPU-bound chunk+hash pass (FastCDC boundaries + whole-file SHA-256
+ per-chunk SHA-256) with zero network involvement, so pipeline changes can be
compared before/after without a live registry in the loop. Baseline profile
(py-spy 2026-07-16): this phase is 34% of upload wall clock, hashing 94% of the
phase, FastCDC 6%.

Production uploads stream this phase via `chunk_stream_native` (packs upload
while chunking runs), but both entry points drive the same Rust
`run_chunk_pipeline`, so the pipeline measured here is the one production uses.

The input file is deterministic: a seeded PRNG (seed 0xB0) generates the bytes,
so the same size always yields the same content, the same CDC boundaries, and
comparable runs. Because of that, an existing file of the right size at the
target path is reused as-is instead of being regenerated. If you pass --file
pointing at your own pre-existing file, cross-run comparisons are only valid
against that same file.

This measures phase-1 CPU over a page-cached file — disk I/O is deliberately
excluded — and `--runs 1` has no first-touch protection (the default 3-run
median absorbs it).

IMPORTANT: run against a release build (`maturin develop --release`). The sha2
asm paths and LTO are the thing being measured; a debug build measures nothing
meaningful. The script warns if throughput is low enough to suggest one.

Examples:

    # Default: 2 GiB input, 3 timed runs.
    python scripts/bench_phase1.py

    # Smaller input, more runs.
    python scripts/bench_phase1.py --size-mib 512 --runs 5

    # Reuse a specific file across invocations.
    python scripts/bench_phase1.py --file /tmp/bench-phase1.bin
"""
import argparse
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from hippius_hub.hippius_core import chunk_and_hash_native
except ImportError:
    print(
        "ERROR: hippius_core is not installed. Build it with `maturin develop --release`\n"
        "(release matters: a debug build gives meaningless numbers).",
        file=sys.stderr,
    )
    sys.exit(1)

from hippius_hub.constants import resolve_cdc_avg_size


MIB = 1024 * 1024
GEN_SEED = 0xB0
GEN_BLOCK = 1 * MIB

# Below this, even software SHA-256 on a decade-old core is unlikely; a debug
# build of the native module is the usual culprit (recorded software-SHA
# baselines sit at 113-147 MiB/s; hardware SHA at 500+ MiB/s).
SUSPICIOUS_MIBS = 80.0


def parse_args():
    p = argparse.ArgumentParser(
        description="Benchmark chunk_and_hash_native (upload phase-1) throughput.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("\n", 1)[1] if __doc__ else None,
    )
    p.add_argument("--size-mib", type=int, default=2048,
                   help="Input file size in MiB (default: 2048)")
    p.add_argument("--runs", type=int, default=3,
                   help="Number of timed runs (default: 3)")
    p.add_argument("--file", type=Path, default=None,
                   help="Input file path (default: <tempdir>/hippius-bench-phase1-<size>mib.bin); "
                        "reused if it already exists with the right size")
    args = p.parse_args()
    if args.size_mib < 1:
        p.error("--size-mib must be >= 1")
    if args.runs < 1:
        p.error("--runs must be >= 1")
    if args.file is None:
        args.file = Path(tempfile.gettempdir()) / f"hippius-bench-phase1-{args.size_mib}mib.bin"
    return args


def cpu_brand() -> str:
    """Best-effort CPU model string; sysctl on macOS, platform elsewhere."""
    if sys.platform == "darwin":
        try:
            out = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True, timeout=5,
            )
            if out.returncode == 0 and out.stdout.strip():
                return out.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass
    return platform.processor() or "unknown"


def ensure_input_file(path: Path, size_mib: int) -> None:
    """Generate the deterministic input file unless it already exists at full size.

    Content is a fixed function of GEN_SEED and size, so an existing file of the
    right size is byte-identical to a fresh one and reuse is valid.
    """
    size = size_mib * MIB
    if path.exists() and path.stat().st_size == size:
        print(f"Reusing existing input file: {path} ({size_mib} MiB)")
        return

    print(f"Generating {size_mib} MiB deterministic input at {path} ...", flush=True)
    rng = random.Random(GEN_SEED)
    t0 = time.perf_counter()
    with open(path, "wb") as f:
        remaining = size
        while remaining > 0:
            n = min(GEN_BLOCK, remaining)
            f.write(rng.randbytes(n))
            remaining -= n
    print(f"  generated in {time.perf_counter() - t0:.1f}s")


def run_bench(path: Path, size_mib: int, runs: int) -> list:
    avg_size = resolve_cdc_avg_size()
    print(f"CDC average chunk size: {avg_size // MIB} MiB")
    print(f"Timing {runs} run(s) of chunk_and_hash_native ...")

    results = []
    for i in range(1, runs + 1):
        t0 = time.perf_counter()
        _whole_hex, chunks = chunk_and_hash_native(str(path), avg_size)
        dt = time.perf_counter() - t0
        mibs = size_mib / dt
        results.append((dt, mibs))
        extra = f", {len(chunks)} chunks" if i == 1 else ""
        print(f"  run {i}: {dt:.2f}s  ({mibs:.1f} MiB/s{extra})")
    return results


def report(results: list, size_mib: int) -> float:
    median_mibs = statistics.median(m for _, m in results)
    median_dt = statistics.median(d for d, _ in results)
    print()
    print(f"Machine: {platform.platform()}")
    print(f"CPU:     {cpu_brand()} ({os.cpu_count()} logical cores)")
    print(f"Input:   {size_mib} MiB deterministic (seed {GEN_SEED:#x})")
    print(f"Median:  {median_dt:.2f}s  ({median_mibs:.1f} MiB/s)")

    if median_mibs < SUSPICIOUS_MIBS:
        print()
        print(f"WARNING: {median_mibs:.1f} MiB/s is below any recorded release-build baseline.")
        print("Likely a debug build of hippius_core — rebuild with `maturin develop --release`.")
    return median_mibs


def main():
    args = parse_args()
    ensure_input_file(args.file, args.size_mib)
    results = run_bench(args.file, args.size_mib, args.runs)
    report(results, args.size_mib)


if __name__ == "__main__":
    main()
