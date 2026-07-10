"""Benchmark + Harbor-flow probe for the chunked-artifact layout.

Two parts, both against the `test/e2e-client` namespace (no production config or
deployment change — every blob is an ordinary content-addressed blob a later GC
reclaims):

1. **Transfer benchmark** — warmed, median-of-N (each run on FRESH random content
   so an upload actually moves bytes instead of dedup-skipping) upload+download of
   a large file, chunked vs plain, plus the incremental re-upload dedup win.

2. **Harbor-flow probe** — localizes the *upload* bottleneck the transfer numbers
   can't explain on their own:
   - per-request round-trip latency (HEAD dedup check, POST upload-init),
   - single-connection PUT throughput,
   - aggregate throughput swept over parallel-connection counts.
   Reading it: if aggregate throughput RISES with concurrency, the link is
   per-connection limited — raising `HIPPIUS_UPLOAD_WORKERS` genuinely helps. If it
   PLATEAUS, we're aggregate-bandwidth bound and more workers won't move it.

Env: `HIPPIUS_TEST_*` for auth; `BENCH_SIZE_MIB` (512), `BENCH_RUNS` (3),
`BENCH_PATCH_MIB` (4), `BENCH_CDC_AVG_MIB` (4), `BENCH_PROBE_MIB` (128),
`BENCH_HARBOR_PROBE` (1), `HIPPIUS_TEST_REPO` (test/e2e-client).

    HIPPIUS_TEST_TOKEN=... python scripts/bench_chunked_transfer.py
"""
import hashlib
import os
import shutil
import statistics
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

from hippius_hub import auth, hf_hub_download, hippius_hub_upload
from hippius_hub._oci import fetch_manifest, group_files
from hippius_hub.auth import get_oci_bearer_token
from hippius_hub.constants import resolve_registry
from hippius_hub.file_download import _oci_repo_path

_MIB = 1024 * 1024
_UPLOAD_TIMEOUT = httpx.Timeout(900.0)


def _env_int(name: str, default_mib: int) -> int:
    raw = os.environ.get(name)
    if not raw or not raw.strip():
        return default_mib * _MIB
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer (MiB), got {value}")
    return value * _MIB


def _env_count(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw or not raw.strip():
        return default
    return max(1, int(raw))


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if not raw or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _fill_random(path: str, size: int) -> None:
    """Write `size` bytes of incompressible random data in 8 MiB blocks."""
    remaining = size
    with open(path, "wb") as f:
        while remaining > 0:
            block = os.urandom(min(8 * _MIB, remaining))
            f.write(block)
            remaining -= len(block)


def _mutate_middle(src: str, dst: str, patch: int) -> None:
    """Copy `src` to `dst`, overwriting `patch` bytes in the middle (a localized edit)."""
    with open(src, "rb") as f:
        data = bytearray(f.read())
    start = max(0, (len(data) - patch) // 2)
    data[start:start + patch] = os.urandom(min(patch, len(data) - start))
    with open(dst, "wb") as f:
        f.write(data)


def _set_mode(mode: str, cdc_avg: int) -> None:
    """Point the uploader at one of: 'plain', 'v1' (chunk blobs), 'v2' (packs)."""
    if mode == "plain":
        os.environ["HIPPIUS_CHUNKED_WRITE"] = "0"
        os.environ.pop("HIPPIUS_CHUNK_THRESHOLD", None)
        os.environ.pop("HIPPIUS_CHUNKED_LAYOUT", None)
        return
    os.environ["HIPPIUS_CHUNKED_WRITE"] = "1"
    os.environ["HIPPIUS_CHUNK_THRESHOLD"] = "1"
    os.environ["HIPPIUS_CDC_AVG_SIZE"] = str(cdc_avg)
    os.environ["HIPPIUS_CHUNKED_LAYOUT"] = "v2" if mode == "v2" else "v1"


def _time(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def _stat(xs: list) -> str:
    """median (min–max) over the measured runs, formatted in seconds."""
    lo, hi, med = min(xs), max(xs), statistics.median(xs)
    return f"{med:.1f}s ({lo:.1f}–{hi:.1f})" if len(xs) > 1 else f"{med:.1f}s"


def _login() -> None:
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
    report = "\n".join(lines)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(report + "\n")


def _upload_download(local: str, repo: str, revision: str, cache_dir: str, size: int) -> dict:
    up = _time(lambda: hippius_hub_upload(repo_id=repo, local_path=local, revision=revision))
    down = _time(lambda: hf_hub_download(
        repo_id=repo, filename="bench.bin", revision=revision, cache_dir=cache_dir))
    return {"up": up, "down": down}


def _chunk_digests(registry: str, oci_repo: str, revision: str, token: str) -> list:
    result = fetch_manifest(registry, oci_repo, revision, token)
    match = [g for g in group_files(result.manifest) if g.title == "bench.bin"]
    if not match:
        raise RuntimeError(f"revision {revision!r} has no 'bench.bin' group")
    return [c.digest for c in match[0].chunks]


# ---------------- Harbor-flow probe (raw OCI blob-upload primitives) ----------------

def _put_blob(client: httpx.Client, registry: str, oci_repo: str, token: str, data: bytes) -> dict:
    """One monolithic OCI blob push (POST init + PUT-with-digest), timing each leg."""
    base = f"{registry}/v2/{oci_repo}/blobs"
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    headers = {"Authorization": f"Bearer {token}"}
    t0 = time.perf_counter()
    init = client.post(f"{base}/uploads/", headers={**headers, "Content-Length": "0"})
    init.raise_for_status()
    init_s = time.perf_counter() - t0
    loc = init.headers["Location"]
    if loc.startswith("/"):
        loc = f"{registry}{loc}"
    sep = "&" if "?" in loc else "?"
    t1 = time.perf_counter()
    put = client.put(
        f"{loc}{sep}digest={digest}",
        headers={**headers, "Content-Type": "application/octet-stream"},
        content=data,
    )
    put.raise_for_status()
    return {"init_s": init_s, "put_s": time.perf_counter() - t1}


def _probe_latency(client: httpx.Client, registry: str, oci_repo: str, token: str) -> dict:
    """Fixed per-request cost: a HEAD on an absent digest and a POST upload-init."""
    base = f"{registry}/v2/{oci_repo}/blobs"
    headers = {"Authorization": f"Bearer {token}"}
    absent = "sha256:" + "0" * 64
    head_s = _time(lambda: client.head(f"{base}/{absent}", headers=headers))
    init = _put_blob(client, registry, oci_repo, token, os.urandom(4096))
    return {"head_ms": head_s * 1000, "init_ms": init["init_s"] * 1000}


def _probe_srmu(client: httpx.Client, registry: str, oci_repo: str, token: str) -> int:
    """Does this registry support single-request monolithic upload (SRMU)?

    `POST /blobs/uploads/?digest=<d>` with the body in one request: 201 Created =
    supported (the single-POST optimization would cut a round-trip per chunk); 202
    Accepted = NOT supported (the registry wants POST-init then PUT, so single-POST
    would double-upload and hurt). SRMU is optional and being deprecated in the OCI
    spec, so this must be measured, not assumed. Fresh random body so it's a real
    create, not a dedup hit.
    """
    data = os.urandom(4096) + uuid.uuid4().bytes
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"}
    return client.post(f"{registry}/v2/{oci_repo}/blobs/uploads/?digest={digest}",
                       headers=headers, content=data).status_code


def _probe_concurrency(registry: str, oci_repo: str, token: str, total: int, workers: int) -> float:
    """Aggregate MiB/s pushing `total` bytes as `workers` fresh parallel blobs."""
    part = max(_MIB, total // workers)
    blobs = [os.urandom(part) + uuid.uuid4().bytes for _ in range(workers)]
    with httpx.Client(timeout=_UPLOAD_TIMEOUT, http2=False) as client:
        def _one(data: bytes) -> None:
            _put_blob(client, registry, oci_repo, token, data)
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(_one, blobs))
        elapsed = time.perf_counter() - start
    return (part * workers) / _MIB / elapsed


def _harbor_probe(registry: str, oci_repo: str, token: str, probe: int) -> list:
    """Latency + single-connection throughput + a concurrency sweep."""
    with httpx.Client(timeout=_UPLOAD_TIMEOUT) as client:
        lat = _probe_latency(client, registry, oci_repo, token)
        srmu = _probe_srmu(client, registry, oci_repo, token)
        single = _put_blob(client, registry, oci_repo, token, os.urandom(probe))
    single_mibps = probe / _MIB / single["put_s"]
    srmu_verdict = {
        201: "SUPPORTED — single-POST would cut a round-trip per chunk",
        202: "NOT supported (202) — single-POST would double-upload; don't build it",
    }.get(srmu, f"unexpected status {srmu}")
    levels = [1, 2, 4, 8, 16]
    sweep = [(w, _probe_concurrency(registry, oci_repo, token, probe, w)) for w in levels]
    scaling = sweep[-1][1] / sweep[0][1] if sweep[0][1] else 0.0
    verdict = (
        "per-connection limited — more upload workers help"
        if scaling >= 1.5 else
        "aggregate-bandwidth bound — more workers won't help"
    )
    lines = [
        "### Harbor-flow probe",
        f"- per-request latency: HEAD {lat['head_ms']:.0f} ms, POST-init {lat['init_ms']:.0f} ms",
        f"- single-POST (SRMU) capability: **{srmu} → {srmu_verdict}**",
        f"- single-connection PUT throughput: **{single_mibps:.1f} MiB/s**",
        "",
        "| parallel connections | aggregate MiB/s |",
        "|---------------------:|----------------:|",
    ]
    lines += [f"| {w} | {mibps:.1f} |" for w, mibps in sweep]
    lines += ["", f"- 1→16 scaling: **{scaling:.1f}×** → {verdict}"]
    return lines


# ---------------- main ----------------

def _transfer_rows(work: str, repo: str, tag: str, size: int, runs: int, cdc_avg: int) -> list:
    """Median-of-`runs` upload/download for plain, v1 (chunk blobs) and v2 (packs).

    Fresh random content each run so every upload actually transfers. v2's win is
    fewer round-trips (~size/64MiB pack PUTs vs ~size/4MiB chunk PUTs), so this is
    where the pack layout should show a lower upload wall-clock than v1.
    """
    modes = [("plain (prod baseline)", "plain", "cp"),
             ("v1 (chunk blobs)", "v1", "c1"),
             ("v2 (packs)", "v2", "c2")]
    times = {label: {"up": [], "down": []} for label, _m, _d in modes}
    src = os.path.join(work, "bench.bin")
    for r in range(runs):
        _fill_random(src, size)
        for label, mode, cache in modes:
            _set_mode(mode, cdc_avg)
            res = _upload_download(src, repo, f"bench-{tag}-{mode}-{r}", os.path.join(work, cache), size)
            times[label]["up"].append(res["up"])
            times[label]["down"].append(res["down"])
            shutil.rmtree(os.path.join(work, cache), ignore_errors=True)
    rows = [
        "### Transfer wall-clock (median of "
        f"{runs} run{'s' if runs > 1 else ''}, fresh content each)",
        "| mode | upload | download |",
        "|------|-------:|---------:|",
    ]
    rows += [f"| {label} | {_stat(times[label]['up'])} | {_stat(times[label]['down'])} |"
             for label, _m, _d in modes]
    return rows


def _incremental_rows(work, repo, tag, registry, oci_repo, token, size, patch, cdc_avg) -> list:
    """The dedup win: a localized edit re-uploads only the changed chunks."""
    _set_mode("v1", cdc_avg)  # dedup diff reads v1 chunk layers; v2 dedup is proven by e2e
    v1 = os.path.join(work, "v1", "bench.bin")
    os.makedirs(os.path.dirname(v1), exist_ok=True)
    _fill_random(v1, size)
    hippius_hub_upload(repo_id=repo, local_path=v1, revision=f"bench-{tag}-inc-v1")
    v1_chunks = _chunk_digests(registry, oci_repo, f"bench-{tag}-inc-v1", token)
    v2 = os.path.join(work, "v2", "bench.bin")
    os.makedirs(os.path.dirname(v2), exist_ok=True)
    _mutate_middle(v1, v2, patch)
    reup_s = _time(lambda: hippius_hub_upload(repo_id=repo, local_path=v2, revision=f"bench-{tag}-inc-v2"))
    v2_chunks = _chunk_digests(registry, oci_repo, f"bench-{tag}-inc-v2", token)
    shared = set(v1_chunks) & set(v2_chunks)
    reused_pct = 100.0 * len(shared) / len(v2_chunks) if v2_chunks else 0.0
    return [
        f"### Incremental re-upload after a {patch // _MIB} MiB edit",
        f"- re-upload wall-clock: **{reup_s:.1f}s** (plain re-sends the whole {size // _MIB} MiB)",
        f"- chunks reused via HEAD-dedup: **{len(shared)}/{len(v2_chunks)} ({reused_pct:.0f}%)**",
        f"- new chunks transferred: **{len(v2_chunks) - len(shared)}**",
    ]


def main() -> None:
    size = _env_int("BENCH_SIZE_MIB", 512)
    runs = _env_count("BENCH_RUNS", 3)
    cdc_avg = _env_int("BENCH_CDC_AVG_MIB", 4)
    patch = _env_int("BENCH_PATCH_MIB", 4)
    probe = _env_int("BENCH_PROBE_MIB", 128)
    do_probe = _env_flag("BENCH_HARBOR_PROBE", True)
    repo = os.environ.get("HIPPIUS_TEST_REPO", "test/e2e-client")

    _login()
    registry = resolve_registry(None)
    oci_repo = _oci_repo_path(repo, None)
    token = get_oci_bearer_token(oci_repo, push=True)
    tag = uuid.uuid4().hex[:8]
    work = tempfile.mkdtemp(prefix="bench-chunked-")

    # Warm the connection/token path with a tiny blob so run 1 isn't cold-skewed.
    with httpx.Client(timeout=_UPLOAD_TIMEOUT) as client:
        _put_blob(client, registry, oci_repo, token, os.urandom(8 * _MIB))

    out = [f"## Chunked-layout benchmark ({size // _MIB} MiB file, {cdc_avg // _MIB} MiB CDC avg)", ""]
    out += _transfer_rows(work, repo, tag, size, runs, cdc_avg) + [""]
    out += _incremental_rows(work, repo, tag, registry, oci_repo, token, size, patch, cdc_avg) + [""]
    if do_probe:
        out += _harbor_probe(registry, oci_repo, token, probe)
    _emit(out)


if __name__ == "__main__":
    main()
