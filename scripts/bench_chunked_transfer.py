"""Benchmark + Harbor-flow probe for the chunked-artifact layout.

Three parts, all against the `test/e2e-client` namespace (no production config or
deployment change — every blob is an ordinary content-addressed blob a later GC
reclaims):

1. **Single-file transfer benchmark** — warmed, median-of-N (each run on FRESH
   random content so an upload actually moves bytes instead of dedup-skipping)
   upload+download of one large file, plain vs v1 (chunk blobs) vs v2 (packs),
   plus the incremental re-upload dedup win.

2. **Folder (multi-file) tier** — the shape real clients use: an 80 GB model is
   uploaded as ~30 shards of 2–3 GB each via `upload_folder`, which runs files
   concurrently AND each file runs its packs concurrently. Peak upload memory is
   therefore `min(file_workers, shards) × min(pack_workers, packs/shard) ×
   pack_size` — ~4 GB at the production defaults (8×8×64 MiB). A GitHub-hosted
   runner can't hold that, so we don't move it: we run a SMALL folder that
   saturates the same worker pools, measure the real peak RSS (which validates
   the formula), and project it to the production config. That surfaces the 4 GB
   ceiling without needing 4 GB.

3. **Harbor-flow probe** — localizes the *upload* bottleneck the transfer numbers
   can't explain on their own: per-request latency, single-connection PUT
   throughput, and aggregate throughput swept over parallel-connection counts.
   If aggregate throughput RISES with concurrency, the link is per-connection
   limited — raising `HIPPIUS_UPLOAD_WORKERS` helps. If it PLATEAUS, we're
   aggregate-bandwidth bound and more workers won't move it.

Peak RSS is sampled on a background thread from `/proc/self/status` (Linux only —
where CI runs and where scale matters); other platforms report `n/a`. It is a
measured fact for the run's config; only the *memory* projection extrapolates
(pack buffers scale linearly with pack_size). Wall-clock does NOT extrapolate —
it is network-bound, so each number is reported at the size it was measured.

Env: `HIPPIUS_TEST_*` for auth; `BENCH_TIERS` (single,folder), `BENCH_SIZE_MIB`
(512), `BENCH_RUNS` (3), `BENCH_PATCH_MIB` (4), `BENCH_CDC_AVG_MIB` (4),
`BENCH_PACK_MIB` (64, the production pack size), `BENCH_UPLOAD_WORKERS` (8),
`BENCH_FOLDER_SHARDS` (4), `BENCH_SHARD_MIB` (128), `BENCH_FOLDER_FILE_WORKERS`
(8), `BENCH_PROBE_MIB` (128), `BENCH_HARBOR_PROBE` (1), `HIPPIUS_TEST_REPO`
(test/e2e-client). Dispatch a run with larger shards/size from a machine with
capacity to measure the true GB-scale shape.

    HIPPIUS_TEST_TOKEN=... python scripts/bench_chunked_transfer.py
"""
import hashlib
import math
import os
import shutil
import statistics
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

from hippius_hub import auth, hf_hub_download, hippius_hub_upload, upload_folder
from hippius_hub._oci import fetch_manifest, group_files
from hippius_hub.auth import get_oci_bearer_token
from hippius_hub.constants import resolve_max_inflight_packs, resolve_registry
from hippius_hub.file_download import _oci_repo_path

_MIB = 1024 * 1024
_UPLOAD_TIMEOUT = httpx.Timeout(900.0)
# Production defaults the memory projection extrapolates to: an 80 GB model is
# ~30 shards, uploaded 8-files-at-once, each file packing 8-at-once at 64 MiB.
_PROD_FILE_WORKERS = 8
_PROD_PACK_WORKERS = 8
_PROD_SHARDS = 30
_PROD_SHARD_BYTES = 3 * 1024 * _MIB  # 3 GiB, the upper end of the 2–3 GB shard range


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


def _env_tiers() -> set:
    raw = os.environ.get("BENCH_TIERS")
    if not raw or not raw.strip():
        return {"single", "folder"}
    return {t.strip().lower() for t in raw.split(",") if t.strip()}


# ---------------- peak-RSS sampler (Linux /proc; n/a elsewhere) ----------------

def _read_vmrss_kib():
    """Current resident set size in KiB from /proc/self/status, or None off-Linux."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


class _PeakRSS:
    """Sample VmRSS on a daemon thread; expose the peak MiB seen inside the block.

    The kernel's VmHWM high-water mark is monotonic over the whole process life and
    cannot be reset, so it can't isolate one operation's peak across sequential
    modes. Sampling VmRSS during the block gives a per-operation peak instead.
    """

    def __init__(self, interval: float = 0.05):
        self._interval = interval
        self._peak_kib = 0
        self._stop = threading.Event()
        self._thread = None
        self._supported = _read_vmrss_kib() is not None

    def __enter__(self):
        if self._supported:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            cur = _read_vmrss_kib()
            if cur and cur > self._peak_kib:
                self._peak_kib = cur
            self._stop.wait(self._interval)

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    @property
    def peak_mib(self):
        if not self._supported or not self._peak_kib:
            return None
        return self._peak_kib / 1024


def _fmt_mib(mib) -> str:
    return f"{mib:.0f} MiB" if mib is not None else "n/a"


def _current_rss_mib():
    kib = _read_vmrss_kib()
    return kib / 1024 if kib is not None else None


def _fill_random(path: str, size: int) -> None:
    """Write `size` bytes of incompressible random data in 8 MiB blocks."""
    remaining = size
    with open(path, "wb") as f:
        while remaining > 0:
            block = os.urandom(min(8 * _MIB, remaining))
            f.write(block)
            remaining -= len(block)


def _mutate_middle(src: str, dst: str, patch: int) -> None:
    """Copy `src` to `dst`, overwriting `patch` bytes in the middle (a localized edit).

    Streams via copyfile + a seek/overwrite so a multi-GB source never lands in
    memory as a whole-file `bytearray` — the harness must not OOM before the
    client it measures does.
    """
    shutil.copyfile(src, dst)
    size = os.path.getsize(dst)
    start = max(0, (size - patch) // 2)
    with open(dst, "r+b") as f:
        f.seek(start)
        f.write(os.urandom(min(patch, size - start)))


def _set_mode(mode: str, cdc_avg: int, pack_size: int, upload_workers: int) -> None:
    """Point the uploader at one of: 'plain', 'v1' (chunk blobs), 'v2' (packs)."""
    if mode == "plain":
        os.environ["HIPPIUS_CHUNKED_WRITE"] = "0"
        os.environ.pop("HIPPIUS_CHUNK_THRESHOLD", None)
        os.environ.pop("HIPPIUS_CHUNKED_LAYOUT", None)
        return
    os.environ["HIPPIUS_CHUNKED_WRITE"] = "1"
    os.environ["HIPPIUS_CHUNK_THRESHOLD"] = "1"
    os.environ["HIPPIUS_CDC_AVG_SIZE"] = str(cdc_avg)
    os.environ["HIPPIUS_PACK_SIZE"] = str(pack_size)
    os.environ["HIPPIUS_UPLOAD_WORKERS"] = str(upload_workers)
    os.environ["HIPPIUS_CHUNKED_LAYOUT"] = "v2" if mode == "v2" else "v1"


def _time(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def _timed_peak(fn):
    """Return (elapsed_s, peak_mib_or_None) for `fn`, sampling RSS across the call."""
    with _PeakRSS() as rss:
        elapsed = _time(fn)
    return elapsed, rss.peak_mib


def _stat(xs: list) -> str:
    """median (min–max) over the measured runs, formatted in seconds."""
    lo, hi, med = min(xs), max(xs), statistics.median(xs)
    return f"{med:.1f}s ({lo:.1f}–{hi:.1f})" if len(xs) > 1 else f"{med:.1f}s"


def _peak_stat(xs: list) -> str:
    present = [x for x in xs if x is not None]
    return _fmt_mib(max(present)) if present else "n/a"


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


def _upload_download(local: str, repo: str, revision: str, cache_dir: str) -> dict:
    up, peak = _timed_peak(lambda: hippius_hub_upload(repo_id=repo, local_path=local, revision=revision))
    down = _time(lambda: hf_hub_download(
        repo_id=repo, filename="bench.bin", revision=revision, cache_dir=cache_dir))
    return {"up": up, "down": down, "peak": peak}


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


# ---------------- single-file transfer tier ----------------

def _transfer_rows(work, repo, tag, size, runs, cdc_avg, pack_size, upload_workers) -> list:
    """Median-of-`runs` upload/download for plain, v1 (chunk blobs) and v2 (packs).

    Fresh random content each run so every upload actually transfers. v2's win is
    fewer round-trips (~size/pack_size pack PUTs vs ~size/4MiB chunk PUTs), so this
    is where the pack layout should show a lower upload wall-clock than v1. Peak RSS
    is reported alongside so a per-stream memory regression shows up next to speed.
    """
    modes = [("plain (prod baseline)", "plain", "cp"),
             ("v1 (chunk blobs)", "v1", "c1"),
             ("v2 (packs)", "v2", "c2")]
    times = {label: {"up": [], "down": [], "peak": []} for label, _m, _d in modes}
    src = os.path.join(work, "bench.bin")
    for r in range(runs):
        _fill_random(src, size)
        for label, mode, cache in modes:
            _set_mode(mode, cdc_avg, pack_size, upload_workers)
            res = _upload_download(src, repo, f"bench-{tag}-{mode}-{r}", os.path.join(work, cache))
            times[label]["up"].append(res["up"])
            times[label]["down"].append(res["down"])
            times[label]["peak"].append(res["peak"])
            shutil.rmtree(os.path.join(work, cache), ignore_errors=True)
    rows = [
        "### Single-file transfer wall-clock (median of "
        f"{runs} run{'s' if runs > 1 else ''}, fresh content each)",
        "| mode | upload | download | peak RSS |",
        "|------|-------:|---------:|---------:|",
    ]
    rows += [f"| {label} | {_stat(times[label]['up'])} | {_stat(times[label]['down'])} "
             f"| {_peak_stat(times[label]['peak'])} |"
             for label, _m, _d in modes]
    return rows


def _incremental_rows(work, repo, tag, registry, oci_repo, token, size, patch, cdc_avg, pack_size, workers) -> list:
    """The dedup win: a localized edit re-uploads only the changed chunks."""
    _set_mode("v1", cdc_avg, pack_size, workers)  # dedup diff reads v1 chunk layers; v2 dedup is proven by e2e
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


# ---------------- folder (multi-file) tier ----------------

def _packs_per_shard(shard: int, pack_size: int) -> int:
    return max(1, math.ceil(shard / pack_size))


def _inflight_ceiling(cap, file_workers, shards, pack_workers, packs_per_shard, pack_size) -> int:
    """Bytes of pack buffers resident when every admitted worker holds one full pack.

    A folder upload runs `file_workers` files at once, each running `pack_workers`
    packs at once, but the shared in-flight cap (`resolve_max_inflight_packs`) bounds
    the cross-file total — so effective concurrency is `min(cap, nested)`, and each
    admitted pack is one `pack_size` buffer. The cap is what stops the nested product
    (~64 at the defaults) from multiplying resident memory.
    """
    nested = min(file_workers, shards) * min(pack_workers, packs_per_shard)
    return min(cap, nested) * pack_size


def _mult_str(multiplier) -> str:
    return f"{multiplier:.1f}" if multiplier is not None else "n/a"


def _folder_rows(work, repo, tag, shards, shard, cdc_avg, pack_size, file_workers, pack_workers, baseline) -> list:
    """Upload a folder of shards (the 80 GB-model shape) and measure peak memory.

    The run is deliberately small so a hosted runner survives it, but it saturates
    the SAME worker pools and shares the SAME in-flight cap as production, so the
    measured peak RSS validates `_inflight_ceiling`. The per-pack RSS multiplier
    (real resident bytes exceed one `pack_size` per pack — disk read buffer + HTTP
    body + per-thread overhead) is derived from THIS run's measured peak rather than
    hardcoded, so the production projection tracks reality and auto-corrects when an
    allocation is removed (e.g. the Bytes-body fix) instead of going stale.
    """
    _set_mode("v2", cdc_avg, pack_size, pack_workers)
    cap = resolve_max_inflight_packs()  # reads HIPPIUS_UPLOAD_WORKERS that _set_mode just set
    src_dir = os.path.join(work, "folder-src")
    os.makedirs(src_dir, exist_ok=True)
    for i in range(shards):
        _fill_random(os.path.join(src_dir, f"model-{i:05d}.bin"), shard)

    rev = f"bench-{tag}-folder"
    up, peak = _timed_peak(lambda: upload_folder(
        repo_id=repo, folder_path=src_dir, revision=rev, max_workers=file_workers))

    # Correctness spot-check: one shard round-trips (proves the folder manifest is
    # readable, not just writable) — a cheap guard that the fanout didn't corrupt.
    cache = os.path.join(work, "folder-cache")
    os.makedirs(cache, exist_ok=True)
    got = hf_hub_download(repo_id=repo, filename="model-00000.bin", revision=rev, cache_dir=cache)
    ok = os.path.getsize(got) == shard
    shutil.rmtree(cache, ignore_errors=True)

    pps = _packs_per_shard(shard, pack_size)
    ceil_mib = _inflight_ceiling(cap, file_workers, shards, pack_workers, pps, pack_size) / _MIB
    prod_pps = _packs_per_shard(_PROD_SHARD_BYTES, pack_size)
    prod_ceil_mib = _inflight_ceiling(cap, _PROD_FILE_WORKERS, _PROD_SHARDS, _PROD_PACK_WORKERS,
                                      prod_pps, pack_size) / _MIB
    # Observed resident cost per unit of pack buffer, isolated from the baseline.
    delta = max(0.0, peak - baseline) if (peak is not None and baseline is not None) else None
    multiplier = (delta / ceil_mib) if (delta is not None and ceil_mib > 0) else None
    if multiplier is not None:
        projected = baseline + prod_ceil_mib * multiplier
        projected_str = (f"~{_fmt_mib(projected)}** (baseline {_fmt_mib(baseline)} + "
                         f"{_fmt_mib(prod_ceil_mib)} pack buffers × {_mult_str(multiplier)} observed/pack")
    else:
        projected_str = (f"~{_fmt_mib(prod_ceil_mib)}** of concurrent pack buffers "
                         "(per-pack RSS multiplier unmeasured off-Linux")
    total_mib = shards * shard // _MIB
    return [
        "### Folder tier (multi-file — the 80 GB-model shape)",
        f"- uploaded **{shards} shard(s) × {shard // _MIB} MiB = {total_mib} MiB** via `upload_folder` "
        f"({file_workers} files × {pack_workers} packs, cap {cap} in-flight, {pack_size // _MIB} MiB packs)",
        f"- upload wall-clock: **{up:.1f}s**; one-shard round-trip: **{'OK' if ok else 'MISMATCH'}**",
        f"- measured peak RSS: **{_fmt_mib(peak)}** (baseline {_fmt_mib(baseline)}; config ceiling "
        f"{_fmt_mib(ceil_mib)} of pack buffers → observed **×{_mult_str(multiplier)}** resident per pack)",
        f"- **projected peak at production** (~{_PROD_SHARDS} × {_PROD_SHARD_BYTES // _MIB} MiB shards, "
        f"{_PROD_FILE_WORKERS}×{_PROD_PACK_WORKERS} workers, cap {cap}, {pack_size // _MIB} MiB packs): "
        f"**{projected_str}) — bounded by the in-flight cap, not the file×pack product",
    ]


# ---------------- main ----------------

def main() -> None:
    tiers = _env_tiers()
    size = _env_int("BENCH_SIZE_MIB", 512)
    runs = _env_count("BENCH_RUNS", 3)
    cdc_avg = _env_int("BENCH_CDC_AVG_MIB", 4)
    pack_size = _env_int("BENCH_PACK_MIB", 64)
    upload_workers = _env_count("BENCH_UPLOAD_WORKERS", _PROD_PACK_WORKERS)
    patch = _env_int("BENCH_PATCH_MIB", 4)
    probe = _env_int("BENCH_PROBE_MIB", 128)
    shards = _env_count("BENCH_FOLDER_SHARDS", 4)
    shard = _env_int("BENCH_SHARD_MIB", 128)
    file_workers = _env_count("BENCH_FOLDER_FILE_WORKERS", _PROD_FILE_WORKERS)
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
    # Baseline RSS AFTER warm-up (interpreter + libs + connection loaded), so the
    # folder tier's per-pack multiplier isolates pack-buffer growth from fixed cost.
    baseline = _current_rss_mib()

    out = [f"## Chunked-layout benchmark ({size // _MIB} MiB file, "
           f"{cdc_avg // _MIB} MiB CDC avg, {pack_size // _MIB} MiB packs)", ""]
    if "single" in tiers:
        out += _transfer_rows(work, repo, tag, size, runs, cdc_avg, pack_size, upload_workers) + [""]
        out += _incremental_rows(work, repo, tag, registry, oci_repo, token,
                                 size, patch, cdc_avg, pack_size, upload_workers) + [""]
    if "folder" in tiers:
        out += _folder_rows(work, repo, tag, shards, shard, cdc_avg,
                            pack_size, file_workers, upload_workers, baseline) + [""]
    if do_probe:
        out += _harbor_probe(registry, oci_repo, token, probe)
    _emit(out)


if __name__ == "__main__":
    main()
