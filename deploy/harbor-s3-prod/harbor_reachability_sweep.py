#!/usr/bin/env python3
"""Prove every artifact still resolves after the storage-driver flip.

The filesystem-to-S3 cut fails in exactly one way: some artifacts resolve and
some 404, silently, until a user hits one. Sampling a handful of blobs does not
catch it. This checks both link classes for every artifact Harbor knows about:

    GET /v2/<repository>/manifests/<artifact digest>    -> _manifests/revisions
    GET /v2/<repository>/blobs/<one layer digest>       -> _layers
        with Range: bytes=0-0

Both are GETs, not HEADs, on purpose. The registry runs
`storage.cache.layerinfo: redis` and that cache is *repository-scoped* — its keys
are `repository::<repo>::blobs::<digest>` — so a HEAD can be answered from redis
without reading the link object or touching S3 at all. Redis is a separate
StatefulSet and survives the registry restart, so a HEAD-based sweep would report
PASS for a repository whose links never landed. A one-byte ranged GET has to
resolve the link and read the object.

Flush that cache before running (`redis-cli -n 2 FLUSHDB` on harbor-redis-0) so
nothing is answered from a pre-flip descriptor.

Near-zero bytes are transferred, so the whole sweep is minutes against a frozen
registry. Run it while Harbor is still `read_only`: a failure then means a cheap
rollback rather than reverse-copying an S3 write window.

Exits non-zero on the first class of failure, with the failing repositories
listed. Reads Harbor's database read-only and talks to Harbor over its normal
authenticated API — it never touches storage directly.
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    )
)
PROGRESS_EVERY = 2000

# One row per artifact: its manifest digest plus one real layer digest. The
# `<> a.digest` guard skips the manifest's own entry, which Harbor records among
# a repository's blobs but which lives under _manifests/revisions, not _layers.
ARTIFACT_SQL = """
SELECT a.repository_name, a.digest, MIN(ab.digest_blob)
FROM artifact a
LEFT JOIN artifact_blob ab
       ON ab.digest_af = a.digest AND ab.digest_blob <> a.digest
GROUP BY a.repository_name, a.digest
"""


def db_dsn() -> str:
    """Resolve the Harbor database connection, preferring an explicit DSN.

    Falling back to libpq's own environment keeps a generated password out of a
    hand-built URL, where a special character would silently mis-parse.
    """
    dsn = os.environ.get("HARBOR_DB_DSN", "")
    if dsn:
        return dsn
    if os.environ.get("PGHOST") and os.environ.get("PGDATABASE"):
        return ""

    sys.exit("need HARBOR_DB_DSN, or PGHOST + PGDATABASE (with PGUSER / PGPASSWORD)")


def iter_artifacts(
    dsn: str, batch: int = 5000
) -> Iterator[tuple[str, str, str | None]]:
    """Stream (repository, manifest digest, one layer digest) for every artifact."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor(name="artifacts") as cur:
            cur.itersize = batch
            cur.execute(ARTIFACT_SQL)
            yield from cur


class TokenCache:
    """Per-repository pull tokens, fetched once and reused across probes."""

    def __init__(self, client: Any, registry: str, basic: str) -> None:
        self._client = client
        self._registry = registry
        self._basic = basic
        self._lock = threading.Lock()
        self._tokens: dict[str, str] = {}

    def get(self, repository: str) -> str:
        with self._lock:
            hit = self._tokens.get(repository)
        if hit is not None:
            return hit

        scope = f"repository:{repository}:pull"
        resp = self._client.get(
            f"{self._registry}/service/token",
            params={"service": "harbor-registry", "scope": scope},
            headers={"Authorization": f"Basic {self._basic}"},
        )
        resp.raise_for_status()
        token = resp.json()["token"]

        with self._lock:
            self._tokens[repository] = token

        return token


class Progress:
    """Thread-safe counter with periodic output."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._n = 0
        self._t0 = time.perf_counter()

    def bump(self) -> None:
        with self._lock:
            self._n += 1
            n = self._n
        if n % PROGRESS_EVERY == 0:
            rate = n / max(time.perf_counter() - self._t0, 1e-6)
            print(f"  checked {n} artifacts ({rate:.0f}/s)", flush=True)

    @property
    def total(self) -> int:
        return self._n


def fetch(
    client: Any,
    url: str,
    token: str,
    *,
    accept: str | None = None,
    first_byte_only: bool = False,
) -> int:
    """GET `url` with a bearer token, returning the status code.

    `first_byte_only` sends `Range: bytes=0-0`, which keeps a multi-gigabyte
    layer to one byte on the wire while still forcing a real storage read.
    """
    headers = {"Authorization": f"Bearer {token}"}
    if accept:
        headers["Accept"] = accept
    if first_byte_only:
        headers["Range"] = "bytes=0-0"

    resp = client.get(url, headers=headers, follow_redirects=True)
    resp.close()

    return resp.status_code


def probe(
    client: Any,
    registry: str,
    tokens: TokenCache,
    row: tuple[str, str, str | None],
) -> tuple[str, str] | None:
    """Check both link classes for one artifact.

    Returns:
        None when the artifact resolves, else a (subject, reason) failure pair.
    """
    repository, digest, layer = row
    subject = f"{repository}@{digest}"

    try:
        token = tokens.get(repository)
    except Exception as exc:  # noqa: BLE001 - an auth failure is a real finding
        return subject, f"token: {exc}"

    status = fetch(
        client,
        f"{registry}/v2/{repository}/manifests/{digest}",
        token,
        accept=MANIFEST_ACCEPT,
    )
    if status != 200:
        return subject, f"manifest GET {status} (missing _manifests/revisions link)"

    # An artifact whose every blob row is its own manifest digest has no layer
    # to probe; the manifest check above is the whole of its reachability.
    if layer is None:
        return None

    status = fetch(
        client,
        f"{registry}/v2/{repository}/blobs/{layer}",
        token,
        first_byte_only=True,
    )
    # 206 is the ranged success; 200 means the registry ignored Range and sent
    # the whole layer, which still proves the bytes are there.
    if status not in (200, 206):
        return (
            subject,
            f"blob GET {status} for {layer} (missing _layers link or object)",
        )

    return None


def run_sweep(dsn: str, registry: str, basic: str, workers: int, limit: int) -> int:
    """Probe every artifact and report the failures."""
    import httpx

    failures: list[tuple[str, str]] = []
    lock = threading.Lock()
    progress = Progress()

    limits = httpx.Limits(
        max_connections=workers * 2, max_keepalive_connections=workers
    )
    with httpx.Client(timeout=30.0, limits=limits) as client:
        tokens = TokenCache(client, registry, basic)

        def work(row: tuple[str, str, str | None]) -> None:
            result = probe(client, registry, tokens, row)
            progress.bump()
            if result is not None:
                with lock:
                    failures.append(result)

        rows = iter_artifacts(dsn)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for _ in pool.map(work, rows):
                pass

    print(f"\nswept {progress.total} artifacts")
    if failures:
        print(f"FAIL  {len(failures)} artifacts do not resolve", file=sys.stderr)
        for subject, reason in failures[:limit]:
            print(f"  {subject}: {reason}", file=sys.stderr)
        if len(failures) > limit:
            print(f"  ... and {len(failures) - limit} more", file=sys.stderr)
        return 1

    print("PASS  every artifact resolves")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--show", type=int, default=40, help="failures to print")

    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()

    registry = os.environ.get("HARBOR_ENDPOINT", "").rstrip("/")
    admin = os.environ.get("HARBOR_ADMIN_PASSWORD", "")
    user = os.environ.get("HARBOR_ADMIN_USER", "admin")

    if not registry or not admin:
        sys.exit("need HARBOR_ENDPOINT and HARBOR_ADMIN_PASSWORD")

    dsn = db_dsn()

    basic = base64.b64encode(f"{user}:{admin}".encode()).decode()
    print(f"registry {registry}", flush=True)

    return run_sweep(dsn, registry, basic, args.workers, args.show)


if __name__ == "__main__":
    sys.exit(main())
