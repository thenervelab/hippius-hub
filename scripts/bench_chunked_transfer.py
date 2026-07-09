"""Benchmark the chunked-artifact layout against the pre-chunking single-blob path.

Measures the two things chunking is meant to improve, both against the live
registry's `test/e2e-client` namespace — which changes nothing in production's
config or deployment (chunks are ordinary content-addressed blobs; the org-wide
trace confirmed chunking is a self-contained client change):

  1. **Transfer wall-clock** — upload + download of one large file, chunked (K
     parallel blobs) vs plain (single blob). The "faster transfer" axis. Since
     staging reuses prod's Harbor, chunked-ON vs chunked-OFF *is* the staging-vs-
     prod comparison.
  2. **Incremental re-upload** — after a small edit, how many chunks are NEW vs
     `HEAD`-deduped. The "upload only the bytes we're missing" axis — the reason
     chunking landed in v1, invisible to a single-blob layout (any edit re-uploads
     the whole file).

Auth + target come from the same env the e2e suite uses (`HIPPIUS_TEST_USER`/
`HIPPIUS_TEST_PASS` or `HIPPIUS_TEST_TOKEN`; `HIPPIUS_TEST_REPO`). Size and CDC
average are env-overridable so a run scales without code edits. Results print as a
Markdown table to stdout and, when running under Actions, to `$GITHUB_STEP_SUMMARY`.

    HIPPIUS_TEST_TOKEN=... python scripts/bench_chunked_transfer.py
"""
import os
import sys
import tempfile
import time
import uuid

from hippius_hub import hf_hub_download, hippius_hub_upload
from hippius_hub import auth
from hippius_hub._oci import fetch_manifest, group_files
from hippius_hub.auth import get_oci_bearer_token
from hippius_hub.constants import resolve_registry
from hippius_hub.file_download import _oci_repo_path

_MIB = 1024 * 1024


def _env_int(name: str, default_mib: int) -> int:
    """Read a size env var in MiB, returning bytes. Fails loudly on non-positive."""
    raw = os.environ.get(name)
    if not raw:
        return default_mib * _MIB
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer (MiB), got {value}")
    return value * _MIB


def _make_file(path: str, size: int) -> None:
    """Fill `path` with `size` bytes of pseudo-random data in 8 MiB blocks.

    os.urandom keeps chunks incompressible (a realistic model-weight proxy) so the
    benchmark isn't skewed by registry-side compression of a zero-filled file.
    """
    remaining = size
    with open(path, "wb") as f:
        while remaining > 0:
            block = os.urandom(min(8 * _MIB, remaining))
            f.write(block)
            remaining -= len(block)


def _mutate_middle(src: str, dst: str, patch: int) -> None:
    """Copy `src` to `dst`, overwriting `patch` bytes in the middle.

    A localized edit is the case chunking wins on: FastCDC re-syncs boundaries just
    past the change, so only the chunks overlapping the edit get new digests — the
    rest HEAD-dedup. A single-blob layout would re-transfer the entire file.
    """
    with open(src, "rb") as f:
        data = bytearray(f.read())
    start = max(0, (len(data) - patch) // 2)
    data[start:start + patch] = os.urandom(min(patch, len(data) - start))
    with open(dst, "wb") as f:
        f.write(data)


def _set_mode(chunked: bool, cdc_avg: int) -> None:
    """Point the resolvers at the chunked or the plain single-blob write path."""
    if chunked:
        os.environ["HIPPIUS_CHUNKED_WRITE"] = "1"
        os.environ["HIPPIUS_CHUNK_THRESHOLD"] = "1"  # force chunking regardless of size
        os.environ["HIPPIUS_CDC_AVG_SIZE"] = str(cdc_avg)
    else:
        os.environ["HIPPIUS_CHUNKED_WRITE"] = "0"
        os.environ.pop("HIPPIUS_CHUNK_THRESHOLD", None)


def _time(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def _chunk_digests(registry: str, oci_repo: str, revision: str, token: str) -> list:
    """The ordered chunk digests of a chunked upload, for the dedup comparison."""
    result = fetch_manifest(registry, oci_repo, revision, token)
    (group,) = [g for g in group_files(result.manifest) if g.title == "bench.bin"]
    return [c.digest for c in group.chunks]


def _upload_download(local: str, repo: str, revision: str, cache_dir: str, size: int) -> dict:
    """Upload then download `local`, returning wall-clock + throughput for each leg."""
    up = _time(lambda: hippius_hub_upload(repo_id=repo, local_path=local, revision=revision))
    down = _time(lambda: hf_hub_download(
        repo_id=repo, filename="bench.bin", revision=revision, cache_dir=cache_dir))
    mib = size / _MIB
    return {"up_s": up, "down_s": down, "up_mibps": mib / up, "down_mibps": mib / down}


def _login() -> None:
    """Authenticate from HIPPIUS_TEST_* into a temp token file (not the user's cache)."""
    user, password, token = (
        os.environ.get("HIPPIUS_TEST_USER"),
        os.environ.get("HIPPIUS_TEST_PASS"),
        os.environ.get("HIPPIUS_TEST_TOKEN"),
    )
    if not token and not (user and password):
        sys.exit("bench: set HIPPIUS_TEST_TOKEN or HIPPIUS_TEST_USER + HIPPIUS_TEST_PASS")
    auth.TOKEN_PATH = os.path.join(tempfile.mkdtemp(prefix="bench-tok-"), "token")
    auth.login(username=user, password=password, token=token)


def _emit(lines: list) -> None:
    """Print the report and mirror it to the Actions step summary when present."""
    report = "\n".join(lines)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(report + "\n")


def main() -> None:
    size = _env_int("BENCH_SIZE_MIB", 256)
    cdc_avg = _env_int("BENCH_CDC_AVG_MIB", 4)  # fastcdc AVERAGE_MAX; larger panics
    patch = _env_int("BENCH_PATCH_MIB", 4)
    repo = os.environ.get("HIPPIUS_TEST_REPO", "test/e2e-client")

    _login()
    registry = resolve_registry(None)
    oci_repo = _oci_repo_path(repo, None)
    token = get_oci_bearer_token(oci_repo)
    tag = uuid.uuid4().hex[:8]

    work = tempfile.mkdtemp(prefix="bench-chunked-")
    src = os.path.join(work, "bench.bin")
    _make_file(src, size)

    # --- axis 1: transfer wall-clock, plain vs chunked ---
    _set_mode(chunked=False, cdc_avg=cdc_avg)
    plain = _upload_download(src, repo, f"bench-{tag}-plain", os.path.join(work, "c-plain"), size)

    _set_mode(chunked=True, cdc_avg=cdc_avg)
    rev_v1 = f"bench-{tag}-chunked"
    chunked = _upload_download(src, repo, rev_v1, os.path.join(work, "c-chunked"), size)
    v1_chunks = _chunk_digests(registry, oci_repo, rev_v1, token)

    # --- axis 2: incremental re-upload (chunked only; a plain blob re-sends 100%) ---
    src_v2 = os.path.join(work, "bench_v2.bin")
    _mutate_middle(src, src_v2, patch)
    rev_v2 = f"bench-{tag}-chunked-v2"
    reup_s = _time(lambda: hippius_hub_upload(repo_id=repo, local_path=src_v2, revision=rev_v2))
    v2_chunks = _chunk_digests(registry, oci_repo, rev_v2, token)
    shared = set(v1_chunks) & set(v2_chunks)
    new_chunks = [d for d in v2_chunks if d not in shared]
    reused_pct = 100.0 * len(shared) / len(v2_chunks) if v2_chunks else 0.0

    _emit([
        f"## Chunked-layout benchmark ({size // _MIB} MiB file, {cdc_avg // _MIB} MiB CDC avg)",
        "",
        "### Transfer wall-clock",
        "| mode | upload | download | up MiB/s | down MiB/s | chunks |",
        "|------|-------:|---------:|--------:|----------:|-------:|",
        f"| plain (prod baseline) | {plain['up_s']:.1f}s | {plain['down_s']:.1f}s | "
        f"{plain['up_mibps']:.0f} | {plain['down_mibps']:.0f} | 1 |",
        f"| chunked (staging) | {chunked['up_s']:.1f}s | {chunked['down_s']:.1f}s | "
        f"{chunked['up_mibps']:.0f} | {chunked['down_mibps']:.0f} | {len(v1_chunks)} |",
        "",
        f"### Incremental re-upload after a {patch // _MIB} MiB edit",
        f"- re-upload wall-clock: **{reup_s:.1f}s** (plain would re-send the whole {size // _MIB} MiB)",
        f"- chunks reused via HEAD-dedup: **{len(shared)}/{len(v2_chunks)} ({reused_pct:.0f}%)**",
        f"- new chunks transferred: **{len(new_chunks)}**",
    ])


if __name__ == "__main__":
    main()
