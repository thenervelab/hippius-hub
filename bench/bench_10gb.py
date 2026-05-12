"""Benchmark: download a ~10 GiB blob with both `huggingface_hub` and
`hippius_hub` and compare wall-clock throughput.

HF reference: `EleutherAI/gpt-neo-2.7B/pytorch_model.bin` (9.94 GiB, public).
Hippius blob: `test/bench:10gb/pytorch_model.bin` (synthetic, 10 GiB).

If `SEED=1` is set, the script first generates a fresh 10 GiB synthetic
blob and uploads it to Hippius before running the download timings. The
seed step needs an extra 10 GB of free local disk.

Caveat: this benchmarks the *whole pipeline* (client + network + server +
CDN). HF.co fronts with CloudFront; registry.hippius.com is one Harbor
instance — differences in geo-routing and server bandwidth are folded
into the reported throughput.
"""
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from huggingface_hub import hf_hub_download as hf_hub_download_real

from hippius_hub import auth, hf_hub_download as hippius_download, upload_file


HF_REPO = os.environ.get("BENCH_HF_REPO", "EleutherAI/gpt-neo-2.7B")
HF_FILE = os.environ.get("BENCH_HF_FILE", "pytorch_model.bin")
HIPPIUS_REPO = os.environ.get("BENCH_HIPPIUS_REPO", "test/bench")
HIPPIUS_REVISION = os.environ.get("BENCH_HIPPIUS_REVISION", "10gb")
HIPPIUS_FILE = os.environ.get("BENCH_HIPPIUS_FILE", "pytorch_model.bin")
SEED_SIZE_BYTES = int(os.environ.get("BENCH_SEED_SIZE", 10 * 1024 * 1024 * 1024))
DO_SEED = os.environ.get("SEED", "").lower() in ("1", "true", "yes")
# Each client runs N times back-to-back with a fresh local cache_dir between
# runs. The local-disk wipe means we're measuring server-side caching
# (ATS in front of Harbor, CloudFront in front of HF) — not OS-page-cache reuse.
BENCH_RUNS = max(1, int(os.environ.get("BENCH_RUNS", 2)))


def fmt_size(n: int) -> str:
    for u in ["B", "KiB", "MiB", "GiB"]:
        if n < 1024:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} TiB"


def fmt_throughput(byts: int, seconds: float) -> str:
    return f"{(byts / 1024 / 1024) / seconds:.1f} MiB/s"


def write_seed_blob(path: Path, size: int) -> None:
    """Write `size` bytes of OS-random content. 4 MiB block repeated."""
    block = os.urandom(4 * 1024 * 1024)
    written = 0
    with open(path, "wb") as f:
        while written < size:
            n = min(len(block), size - written)
            f.write(block[:n])
            written += n


def seed_hippius_blob():
    """Upload a 10 GiB synthetic blob to test/bench:10gb. Returns (size, dt)."""
    tmp_dir = tempfile.mkdtemp(prefix="bench-seed-")
    blob_path = Path(tmp_dir) / "blob.bin"
    print(f"=> Generating {fmt_size(SEED_SIZE_BYTES)} blob at {blob_path}", flush=True)
    t0 = time.perf_counter()
    write_seed_blob(blob_path, SEED_SIZE_BYTES)
    print(f"   generated in {time.perf_counter() - t0:.1f}s", flush=True)

    print(f"=> Uploading to {HIPPIUS_REPO}:{HIPPIUS_REVISION}/{HIPPIUS_FILE}", flush=True)
    t0 = time.perf_counter()
    upload_file(
        path_or_fileobj=str(blob_path),
        path_in_repo=HIPPIUS_FILE,
        repo_id=HIPPIUS_REPO,
        revision=HIPPIUS_REVISION,
        commit_message="bench-10gb seed",
    )
    dt = time.perf_counter() - t0
    print(f"   uploaded in {dt:.1f}s ({fmt_throughput(SEED_SIZE_BYTES, dt)})", flush=True)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    return SEED_SIZE_BYTES, dt


def _do_hf_download(cache_dir: Path):
    path = hf_hub_download_real(repo_id=HF_REPO, filename=HF_FILE, cache_dir=str(cache_dir))
    return os.path.getsize(path)


def _do_hippius_download(cache_dir: Path):
    path = hippius_download(
        repo_id=HIPPIUS_REPO,
        filename=HIPPIUS_FILE,
        revision=HIPPIUS_REVISION,
        cache_dir=str(cache_dir),
    )
    return os.path.getsize(path)


def run_n_times(label: str, downloader, cache_root: Path, n: int) -> list:
    """Run `downloader` n times, wiping the local cache between runs so each
    network call goes through to the server. The cache-hit speedup we want to
    observe is in the server's edge cache (ATS / CloudFront), not local disk."""
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
    return results


def _throughput_mbs(byts: int, seconds: float) -> float:
    return (byts / 1024 / 1024) / seconds


def write_summary(results: dict, seed_result):
    lines = []
    lines.append("# Benchmark — ~10 GiB single-file download\n")
    lines.append(f"- HF source: `{HF_REPO}/{HF_FILE}`\n")
    lines.append(f"- Hippius source: `{HIPPIUS_REPO}:{HIPPIUS_REVISION}/{HIPPIUS_FILE}`\n")
    lines.append(f"- Each client run **{BENCH_RUNS}×** back-to-back; local cache wiped between runs "
                 "so any speedup is server-side edge caching (ATS / CloudFront).\n\n")

    if seed_result is not None:
        size, dt = seed_result
        lines.append(f"**Seed step**: uploaded {fmt_size(size)} to Hippius in "
                     f"{dt:.1f}s ({fmt_throughput(size, dt)})\n\n")

    lines.append("## Per-run timings\n\n")
    lines.append("| client | run | tag | time | throughput |\n")
    lines.append("|---|---:|:---:|---:|---:|\n")
    for label in ("hippius_hub", "huggingface_hub"):
        for r in results[label]:
            lines.append(
                f"| {label} | {r['run']} | {r['tag']} | {r['dt']:.2f}s "
                f"| {fmt_throughput(r['size'], r['dt'])} |\n"
            )

    if BENCH_RUNS >= 2:
        lines.append("\n## Cache warmth (last run vs first run)\n\n")
        lines.append("| client | cold throughput | warm throughput | warm speedup |\n")
        lines.append("|---|---:|---:|---:|\n")
        for label in ("hippius_hub", "huggingface_hub"):
            cold = results[label][0]
            warm = results[label][-1]
            cold_mbs = _throughput_mbs(cold["size"], cold["dt"])
            warm_mbs = _throughput_mbs(warm["size"], warm["dt"])
            lines.append(
                f"| {label} | {cold_mbs:.1f} MiB/s | {warm_mbs:.1f} MiB/s "
                f"| {warm_mbs / cold_mbs:.2f}× |\n"
            )
        lines.append(
            "\nA warm speedup > 1.1× means the server-side cache is serving the second pull; "
            "≈ 1.0× means the cache isn't helping (or first pull already hit a warm cache).\n"
        )

    lines.append("\n## Warm-vs-warm client comparison\n\n")
    hf_warm = results["huggingface_hub"][-1]
    hp_warm = results["hippius_hub"][-1]
    hf_mbs = _throughput_mbs(hf_warm["size"], hf_warm["dt"])
    hp_mbs = _throughput_mbs(hp_warm["size"], hp_warm["dt"])
    ratio = hp_mbs / hf_mbs
    lines.append(f"On the warm pull, **hippius_hub vs huggingface_hub** = {ratio:.2f}× "
                 f"({'faster' if ratio > 1 else 'slower'})\n")
    lines.append("\n> Caveat: this measures client + network + server end-to-end. "
                 "HF.co fronts with CloudFront; Hippius is one Harbor instance behind "
                 "ATS. Geo-routing and per-edge bandwidth differences are folded in.\n")

    text = "".join(lines)
    print()
    print(text)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as f:
            f.write(text)


def main():
    user = os.environ.get("HIPPIUS_TEST_USER")
    password = os.environ.get("HIPPIUS_TEST_PASS")
    token = os.environ.get("HIPPIUS_TEST_TOKEN")
    if not (token or (user and password)):
        print("ERROR: set HIPPIUS_TEST_USER+HIPPIUS_TEST_PASS or HIPPIUS_TEST_TOKEN", file=sys.stderr)
        sys.exit(1)

    with tempfile.TemporaryDirectory() as token_dir:
        auth.TOKEN_PATH = str(Path(token_dir) / "token")
        auth.login(username=user, password=password, token=token)

        seed_result = None
        if DO_SEED:
            seed_result = seed_hippius_blob()

        work = Path(tempfile.mkdtemp(prefix="bench-10gb-"))
        try:
            hf_runs = run_n_times("huggingface_hub", _do_hf_download, work, BENCH_RUNS)
            hippius_runs = run_n_times("hippius_hub", _do_hippius_download, work, BENCH_RUNS)
            results = {
                "huggingface_hub": hf_runs,
                "hippius_hub": hippius_runs,
            }
            write_summary(results, seed_result)
        finally:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
