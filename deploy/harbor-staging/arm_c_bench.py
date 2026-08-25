#!/usr/bin/env python3
"""1 GiB upload against in-cluster Harbor staging. Never talks to prod."""

from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

import httpx

ENDPOINT = os.environ.get(
    "HARBOR_ENDPOINT", "http://harbor.harbor-staging.svc.cluster.local"
).rstrip("/")
ADMIN_USER = os.environ.get("HARBOR_ADMIN_USER", "admin")
ADMIN_PASS = os.environ["HARBOR_ADMIN_PASSWORD"]
PROJECT = os.environ.get("HARBOR_PROJECT", "arm-c")
SIZE_MIB = int(os.environ.get("BENCH_SIZE_MIB", "1024"))
RUNS = int(os.environ.get("BENCH_RUNS", "3"))
FILE_PATH = Path(os.environ.get("BENCH_FILE", "/data/bench.bin"))


def wait_healthy() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            r = httpx.get(f"{ENDPOINT}/api/v2.0/health", timeout=5.0)
            if r.status_code == 200:
                print("harbor healthy", r.text[:200], flush=True)
                return
        except httpx.HTTPError as e:
            print("waiting for harbor:", e, flush=True)
        time.sleep(3)
    sys.exit("harbor never became healthy")


def ensure_project() -> None:
    """Basic auth only. A shared cookie jar picks up Harbor's `sid` and then
    CSRF-blocks POST /projects (cookie session without X-Harbor-CSRF-Token)."""
    auth = (ADMIN_USER, ADMIN_PASS)
    r = httpx.get(
        f"{ENDPOINT}/api/v2.0/projects/{PROJECT}",
        auth=auth,
        timeout=30.0,
    )
    if r.status_code == 200:
        print(f"project {PROJECT} exists")
        return
    r = httpx.post(
        f"{ENDPOINT}/api/v2.0/projects",
        auth=auth,
        json={"project_name": PROJECT, "public": True},
        timeout=30.0,
    )
    print(f"create project {r.status_code} {r.text[:300]!r}", flush=True)
    if r.status_code not in (201, 409):
        r.raise_for_status()
    print(f"project {PROJECT} ready ({r.status_code})")


def ensure_file() -> None:
    want = SIZE_MIB * 1024 * 1024
    if FILE_PATH.exists() and FILE_PATH.stat().st_size == want:
        print(f"reusing {FILE_PATH}")
        return
    FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"writing {SIZE_MIB} MiB at {FILE_PATH}", flush=True)
    t0 = time.perf_counter()
    with open(FILE_PATH, "wb") as f:
        leftover = want
        # Fresh urandom every block. Reusing one 8 MiB buffer made FastCDC
        # emit identical 64 MiB packs; Harbor then PUTs the same digest ~14x.
        while leftover:
            n = min(8 * 1024 * 1024, leftover)
            f.write(os.urandom(n))
            leftover -= n
    print(f"  wrote in {time.perf_counter() - t0:.1f}s", flush=True)


def main() -> None:
    wait_healthy()
    ensure_project()
    ensure_file()

    os.environ["HOME"] = "/tmp/arm-c-home"
    os.makedirs("/tmp/arm-c-home", exist_ok=True)
    os.environ["HIPPIUS_CHUNK_THRESHOLD"] = "1"
    # 1 GiB / 64 MiB = 16 packs. Default workers=8 is two Harbor digest-PUT
    # waves (~3s p50 each). 16 in flight is one wave. Cap must rise with the
    # pool or HIPPIUS_MAX_INFLIGHT_PACKS stays at 8 and this is a no-op.
    os.environ["HIPPIUS_UPLOAD_WORKERS"] = "16"
    os.environ["HIPPIUS_MAX_INFLIGHT_PACKS"] = "16"
    print(
        "upload workers=16 inflight_packs=16 "
        "(one wave of 16 x 64 MiB digest PUTs)",
        flush=True,
    )

    import base64

    from hippius_hub import file_upload

    # Explicit token so we don't trip the off-origin ambient-credential guard
    # (staging is not registry.hippius.com).
    basic = base64.b64encode(f"{ADMIN_USER}:{ADMIN_PASS}".encode()).decode()
    token = f"Basic {basic}"

    times: list[float] = []
    for i in range(1, RUNS + 1):
        repo = f"{PROJECT}/bench-{uuid.uuid4().hex[:8]}"
        print(f"run {i}: upload to {repo} via {ENDPOINT}", flush=True)
        t0 = time.perf_counter()
        file_upload.upload_file(
            path_or_fileobj=str(FILE_PATH),
            path_in_repo="bench.bin",
            repo_id=repo,
            token=token,
            endpoint=ENDPOINT,
        )
        dt = time.perf_counter() - t0
        mibs = SIZE_MIB / dt
        times.append(dt)
        print(f"  run {i}: {dt:.2f}s ({mibs:.1f} MiB/s)", flush=True)

    times.sort()
    med = times[len(times) // 2]
    print(
        f"median: {med:.2f}s ({SIZE_MIB / med:.1f} MiB/s) "
        f"over {RUNS} runs of {SIZE_MIB} MiB",
        flush=True,
    )


if __name__ == "__main__":
    main()
