#!/usr/bin/env python3
"""Portable benchmark: time a single-file download with both
`huggingface_hub` and `hippius_hub` from any machine (laptop, VPS, CI).

Default: 5 GiB synthetic blob on Hippius vs a ~5 GB safetensors shard on HF.

Each client runs `--runs` times back-to-back. The local cache_dir is wiped
between runs so any speedup is server-side edge caching (ATS in front of
Harbor, CloudFront in front of HF).

Examples:

    # First run — seed the Hippius blob, then bench.
    export HIPPIUS_TEST_USER='robot$...'
    export HIPPIUS_TEST_PASS='...'
    python scripts/bench.py --seed --size 5

    # Subsequent runs — just bench (no upload).
    python scripts/bench.py --size 5

    # Larger blob, 3 pulls each.
    python scripts/bench.py --seed --size 10 --runs 3

    # Different HF reference file.
    python scripts/bench.py --hf-repo bigscience/bloom-560m --hf-file pytorch_model.bin

Auth: set `HIPPIUS_TEST_USER` + `HIPPIUS_TEST_PASS`, or `HIPPIUS_TEST_TOKEN`.
"""
import argparse
import atexit
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

# ---- HF cache isolation (must run before importing huggingface_hub) ---------
# `hf_hub_download(cache_dir=...)` only controls the Hub cache (the `models--*`
# tree). `hf_xet` keeps a separate **chunk cache** at `$HF_HOME/xet/` that the
# `cache_dir` argument doesn't touch. Without isolation, a "warm" HF run can
# silently hit local chunk cache populated by a previous bench invocation —
# turning a server-cache benchmark into a local-disk benchmark.
#
# Fix: route every HF cache (hub + xet + tokens) into a per-bench temp
# directory we control. Wiped between runs in `run_n_times`; full tree
# `rmtree`'d at process exit.
_BENCH_HF_HOME = tempfile.mkdtemp(prefix="bench-hf-home-")
os.environ["HF_HOME"] = _BENCH_HF_HOME
os.environ["HF_XET_CACHE_DIR"] = str(Path(_BENCH_HF_HOME) / "xet")
atexit.register(lambda: shutil.rmtree(_BENCH_HF_HOME, ignore_errors=True))
# -----------------------------------------------------------------------------

from huggingface_hub import hf_hub_download as hf_hub_download_real

from hippius_hub import auth, hf_hub_download as hippius_download, upload_file


DEFAULT_HF_REPO = "microsoft/Phi-3-mini-4k-instruct"
DEFAULT_HF_FILE = "model-00001-of-00002.safetensors"  # ~4.97 GB
DEFAULT_HIPPIUS_REPO = "test/bench"
DEFAULT_HIPPIUS_FILE = "model.bin"
GIB = 1024 * 1024 * 1024


def parse_args():
    p = argparse.ArgumentParser(
        description="Benchmark single-file download with huggingface_hub vs hippius_hub.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("\n", 1)[1] if __doc__ else None,
    )
    p.add_argument("--size", type=float, default=5.0,
                   help="Synthetic blob size in GiB (default: 5)")
    p.add_argument("--seed", action="store_true",
                   help="Upload a fresh synthetic blob to Hippius before benchmarking")
    p.add_argument("--runs", type=int, default=2,
                   help="Number of pulls per client (default: 2; 1 disables warm-cache observation)")
    p.add_argument("--hf-repo", default=DEFAULT_HF_REPO,
                   help=f"HF reference repo (default: {DEFAULT_HF_REPO})")
    p.add_argument("--hf-file", default=DEFAULT_HF_FILE,
                   help=f"HF reference file (default: {DEFAULT_HF_FILE})")
    p.add_argument("--hippius-repo", default=DEFAULT_HIPPIUS_REPO,
                   help=f"Hippius repo_id (default: {DEFAULT_HIPPIUS_REPO})")
    p.add_argument("--hippius-revision", default=None,
                   help="Hippius revision (default: '{size}gb', e.g. '5gb')")
    p.add_argument("--hippius-file", default=DEFAULT_HIPPIUS_FILE,
                   help=f"Filename inside the Hippius repo (default: {DEFAULT_HIPPIUS_FILE})")
    args = p.parse_args()
    if args.hippius_revision is None:
        args.hippius_revision = f"{int(args.size)}gb"
    args.size_bytes = int(args.size * GIB)
    if args.runs < 1:
        p.error("--runs must be >= 1")
    return args


def fmt_size(n: int) -> str:
    for u in ["B", "KiB", "MiB", "GiB"]:
        if n < 1024:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} TiB"


def throughput_mbs(byts: int, seconds: float) -> float:
    return (byts / 1024 / 1024) / seconds


def fmt_throughput(byts: int, seconds: float) -> str:
    return f"{throughput_mbs(byts, seconds):.1f} MiB/s"


def login_from_env():
    user = os.environ.get("HIPPIUS_TEST_USER")
    password = os.environ.get("HIPPIUS_TEST_PASS")
    token = os.environ.get("HIPPIUS_TEST_TOKEN")
    if not (token or (user and password)):
        print(
            "ERROR: set HIPPIUS_TEST_USER + HIPPIUS_TEST_PASS, or HIPPIUS_TEST_TOKEN",
            file=sys.stderr,
        )
        sys.exit(1)
    # Use an ephemeral token file so this script doesn't clobber a saved one.
    token_dir = tempfile.mkdtemp(prefix="bench-token-")
    auth.TOKEN_PATH = str(Path(token_dir) / "token")
    auth.login(username=user, password=password, token=token)
    return token_dir


def write_seed_blob(path: Path, size: int) -> None:
    """Write `size` bytes of OS-random content via a repeated 4 MiB block."""
    block = os.urandom(4 * 1024 * 1024)
    written = 0
    with open(path, "wb") as f:
        while written < size:
            n = min(len(block), size - written)
            f.write(block[:n])
            written += n


def seed_hippius(args) -> tuple:
    tmp_dir = tempfile.mkdtemp(prefix="bench-seed-")
    blob_path = Path(tmp_dir) / "blob.bin"
    print(f"=> Generating {fmt_size(args.size_bytes)} blob at {blob_path}", flush=True)
    t0 = time.perf_counter()
    write_seed_blob(blob_path, args.size_bytes)
    print(f"   generated in {time.perf_counter() - t0:.1f}s", flush=True)

    print(f"=> Uploading to {args.hippius_repo}:{args.hippius_revision}/{args.hippius_file}", flush=True)
    t0 = time.perf_counter()
    upload_file(
        path_or_fileobj=str(blob_path),
        path_in_repo=args.hippius_file,
        repo_id=args.hippius_repo,
        revision=args.hippius_revision,
        commit_message=f"bench seed ({fmt_size(args.size_bytes)})",
    )
    dt = time.perf_counter() - t0
    print(f"   uploaded in {dt:.1f}s ({fmt_throughput(args.size_bytes, dt)})", flush=True)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    return args.size_bytes, dt


def _wipe_hf_xet_cache():
    """Clear `hf_xet`'s block cache between runs. Without this the second pull
    would hit local chunks and report inflated "warm" throughput that has
    nothing to do with the CDN edge cache we actually want to benchmark."""
    xet_cache = os.environ.get("HF_XET_CACHE_DIR")
    if xet_cache:
        shutil.rmtree(xet_cache, ignore_errors=True)


def run_n_times(label: str, downloader, cache_root: Path, n: int) -> list:
    results = []
    for i in range(1, n + 1):
        cache_dir = cache_root / f"{label}-run{i}"
        tag = "cold" if i == 1 else f"warm-{i - 1}"
        print(f"=> {label} run {i}/{n} ({tag}) ...", flush=True)
        t0 = time.perf_counter()
        size = downloader(cache_dir)
        dt = time.perf_counter() - t0
        results.append({"run": i, "tag": tag, "size": size, "dt": dt})
        print(f"   {fmt_size(size)} in {dt:.2f}s ({fmt_throughput(size, dt)})", flush=True)
        shutil.rmtree(cache_dir, ignore_errors=True)
        # The HF Hub cache (cache_dir) and the xet block cache are separate
        # stores. Wipe the xet cache too so warm pulls measure server-side
        # caching, not local chunks left by run i.
        if label == "huggingface_hub":
            _wipe_hf_xet_cache()
    return results


def hf_downloader(args):
    def _dl(cache_dir):
        path = hf_hub_download_real(repo_id=args.hf_repo, filename=args.hf_file, cache_dir=str(cache_dir))
        return os.path.getsize(path)
    return _dl


def hippius_downloader(args):
    def _dl(cache_dir):
        path = hippius_download(
            repo_id=args.hippius_repo,
            filename=args.hippius_file,
            revision=args.hippius_revision,
            cache_dir=str(cache_dir),
        )
        return os.path.getsize(path)
    return _dl


def print_report(args, results: dict, seed_result):
    print()
    print("=" * 78)
    print(f"BENCHMARK — {fmt_size(args.size_bytes)} single-file download")
    print("=" * 78)
    print(f"  HF source:      {args.hf_repo}/{args.hf_file}")
    print(f"  Hippius source: {args.hippius_repo}:{args.hippius_revision}/{args.hippius_file}")
    print(f"  Runs per client: {args.runs} (local cache wiped between runs)")
    print()

    if seed_result is not None:
        size, dt = seed_result
        print(f"Seed step: uploaded {fmt_size(size)} to Hippius in "
              f"{dt:.1f}s ({fmt_throughput(size, dt)})")
        print()

    print(f"{'client':<18} {'run':>3} {'tag':>8} {'time':>10} {'throughput':>14}")
    print("-" * 78)
    for label in ("hippius_hub", "huggingface_hub"):
        for r in results[label]:
            print(f"{label:<18} {r['run']:>3} {r['tag']:>8} {r['dt']:>9.2f}s "
                  f"{fmt_throughput(r['size'], r['dt']):>14}")
    print()

    if args.runs >= 2:
        print("Cache warmth (last run vs first run):")
        print(f"{'client':<18} {'cold':>14} {'warm':>14} {'speedup':>10}")
        print("-" * 78)
        for label in ("hippius_hub", "huggingface_hub"):
            cold = results[label][0]
            warm = results[label][-1]
            cold_mbs = throughput_mbs(cold["size"], cold["dt"])
            warm_mbs = throughput_mbs(warm["size"], warm["dt"])
            print(f"{label:<18} {cold_mbs:>10.1f} MiB/s {warm_mbs:>10.1f} MiB/s "
                  f"{warm_mbs / cold_mbs:>8.2f}x")
        print()
        print("  (>1.1x = server edge cache is serving the second pull;")
        print("   ~1.0x = either the cache isn't helping, or the first pull already hit warm cache.)")
        print()

    hf_warm = results["huggingface_hub"][-1]
    hp_warm = results["hippius_hub"][-1]
    hf_mbs = throughput_mbs(hf_warm["size"], hf_warm["dt"])
    hp_mbs = throughput_mbs(hp_warm["size"], hp_warm["dt"])
    ratio = hp_mbs / hf_mbs
    final_label = "warm pull" if args.runs >= 2 else "single pull"
    print(f"On the {final_label}, hippius_hub vs huggingface_hub = {ratio:.2f}x "
          f"({'faster' if ratio > 1 else 'slower'})")
    print()
    print("Caveat: measures client + network + server end-to-end. HF.co fronts with")
    print("CloudFront; Hippius is one Harbor instance behind ATS. Geo-routing and")
    print("per-edge bandwidth differences are folded in.")
    print("=" * 78)


def main():
    args = parse_args()
    token_dir = login_from_env()
    try:
        seed_result = seed_hippius(args) if args.seed else None

        work = Path(tempfile.mkdtemp(prefix="bench-dl-"))
        try:
            results = {
                "huggingface_hub": run_n_times("huggingface_hub", hf_downloader(args), work, args.runs),
                "hippius_hub":     run_n_times("hippius_hub",     hippius_downloader(args), work, args.runs),
            }
            print_report(args, results, seed_result)
        finally:
            shutil.rmtree(work, ignore_errors=True)
    finally:
        shutil.rmtree(token_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
