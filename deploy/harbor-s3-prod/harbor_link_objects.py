#!/usr/bin/env python3
"""Rebuild Harbor's per-repository link objects in the S3 bucket.

`copy-blobs-job.yaml` moves `blobs/**/data`. That is not enough on its own:
distribution also keeps a link file per (repository, digest) under
`repositories/`, and resolves `GET /v2/<repo>/blobs/<digest>` — the endpoint
`hippius-hub` downloads from and `docker pull` uses — through it. Without the
link the bytes are in the bucket and unreachable (`BLOB_UNKNOWN`).

Postgres does hold tags, so `_manifests/tags/` is genuinely dead on this Harbor
(verified empty on every prod repository). `_layers/` and
`_manifests/revisions/` are not, and Harbor has no tool to rebuild them.

Every link is derivable from Harbor's own database, so this generates the exact
live set rather than walking JuiceFS — the walk is prohibitively slow, and the
disk carries ~6k repository directories the database has already forgotten.

Derivation (verified against the live filesystem, 2026-08-27):

    _layers    every distinct (repository_name, digest_blob) in
               artifact x artifact_blob, EXCLUDING any digest_blob that is
               itself an artifact digest in that repository — Harbor records
               the manifest among a repository's blobs, but distribution
               stores manifests under _manifests/revisions, never _layers.
    revisions  every (repository_name, artifact.digest).
    body       "sha256:<hex>", 71 bytes, no trailing newline.

Modes: `plan` counts and samples without writing, `apply` writes (PUT is
idempotent, so re-running after a failure is safe), `verify` HEADs a sample and
checks size. Reads Harbor's database read-only.
"""

from __future__ import annotations

import argparse
import os
import random
import re
import sys
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

REPO_PREFIX = "docker/registry/v2/repositories"
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
LINK_SIZE = 71
DEFAULT_ENDPOINT = "http://gateway.hippius-s3-prod.svc.cluster.local:8080"
PROGRESS_EVERY = 5000

# Kept in step with hippius_s3_contract.py. tests/test_harbor_s3_guards.py
# asserts the two copies agree, so a guard cannot drift on one script only.
FORBIDDEN_BUCKETS = frozenset({"hippius-juicefs-data"})


def bucket_forbidden(name: str) -> str | None:
    """Return a refusal reason if `name` is (or looks like) the JuiceFS backend."""
    lowered = name.strip().lower()
    if lowered in FORBIDDEN_BUCKETS:
        return f"bucket {name!r} is the JuiceFS backend — refuse"
    if "juicefs" in lowered:
        return f"bucket {name!r} looks like JuiceFS — refuse"
    return None


def endpoint_forbidden(url: str) -> str | None:
    """Return a refusal reason if `url` is not a hippius-s3 gateway."""
    lowered = url.lower()
    if "minio" in lowered:
        return f"endpoint {url!r} is MinIO — this gate is hippius-s3"
    if "hippius-s3" not in lowered and "s3.hippius.com" not in lowered:
        return f"endpoint {url!r} is not hippius-s3"
    return None


# --- key derivation -------------------------------------------------------


def link_key(repository: str, digest: str, *, manifest: bool) -> str:
    """Build the storage key for one link object.

    Args:
        repository: Harbor `repository_name`, e.g. `library/swe-test`.
        digest: Full digest string, e.g. `sha256:<64 hex>`.
        manifest: True for a `_manifests/revisions` link, False for `_layers`.

    Returns:
        The key, with `rootdirectory` omitted to match the registry config.

    Raises:
        ValueError: If the digest is not a well-formed lowercase sha256 digest.
    """
    if not DIGEST_RE.match(digest):
        raise ValueError(f"refusing to build a key for malformed digest {digest!r}")

    hexpart = digest.split(":", 1)[1]
    kind = "_manifests/revisions" if manifest else "_layers"

    return f"{REPO_PREFIX}/{repository}/{kind}/sha256/{hexpart}/link"


def link_body(digest: str) -> bytes:
    """Return the link file's contents: the digest, no trailing newline."""
    return digest.encode("ascii")


# --- database -------------------------------------------------------------

LAYER_LINK_SQL = """
SELECT DISTINCT a.repository_name, ab.digest_blob
FROM artifact a
JOIN artifact_blob ab ON ab.digest_af = a.digest
WHERE NOT EXISTS (
    SELECT 1 FROM artifact m
    WHERE m.repository_name = a.repository_name
      AND m.digest = ab.digest_blob
)
"""

REVISION_LINK_SQL = "SELECT DISTINCT repository_name, digest FROM artifact"


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


def read_only_connection(dsn: str) -> Any:
    """Open a connection that cannot write to Harbor's database.

    Not autocommit: a server-side cursor is a DECLARE, which Postgres only
    accepts inside a transaction block. `read_only` has to be set before the
    first statement opens that transaction.
    """
    import psycopg

    conn = psycopg.connect(dsn)
    conn.read_only = True

    return conn


def iter_links(dsn: str, batch: int = 10000) -> Iterator[tuple[str, str, bool]]:
    """Stream every (repository, digest, is_manifest) link the registry needs.

    Uses server-side cursors so a 150k-row result never lands in memory at once.
    """
    with read_only_connection(dsn) as conn:
        for sql, manifest in ((LAYER_LINK_SQL, False), (REVISION_LINK_SQL, True)):
            with conn.cursor(name=f"links_{int(manifest)}") as cur:
                cur.itersize = batch
                cur.execute(sql)
                for repository, digest in cur:
                    yield repository, digest, manifest


# --- S3 -------------------------------------------------------------------


def s3_client(endpoint: str, access: str, secret: str) -> Any:
    """Build a path-style SigV4 client aimed at the hippius-s3 gateway."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            # Link objects are 71 bytes; a stalled connection is never worth
            # waiting on, but the gateway does shed load under the copy Job.
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=10,
            read_timeout=60,
            max_pool_connections=64,
        ),
    )


class Counter:
    """Thread-safe tally with periodic progress output."""

    def __init__(self, label: str) -> None:
        self._label = label
        self._lock = threading.Lock()
        self._n = 0
        self._t0 = time.perf_counter()

    def bump(self) -> int:
        with self._lock:
            self._n += 1
            n = self._n
        if n % PROGRESS_EVERY == 0:
            rate = n / max(time.perf_counter() - self._t0, 1e-6)
            print(f"  {self._label} {n} ({rate:.0f}/s)", flush=True)
        return n

    @property
    def total(self) -> int:
        return self._n


def put_link(
    client: Any, bucket: str, repository: str, digest: str, manifest: bool
) -> None:
    """PUT one link object. Idempotent — re-running after a failure is safe."""
    client.put_object(
        Bucket=bucket,
        Key=link_key(repository, digest, manifest=manifest),
        Body=link_body(digest),
    )


# --- modes ----------------------------------------------------------------


def run_plan(dsn: str, sample: int) -> int:
    """Count the link set and print a sample of keys, writing nothing."""
    layers = 0
    revisions = 0
    shown: list[str] = []

    for repository, digest, manifest in iter_links(dsn):
        if manifest:
            revisions += 1
        else:
            layers += 1
        if len(shown) < sample:
            shown.append(link_key(repository, digest, manifest=manifest))

    print(f"_layers links            {layers}")
    print(f"_manifests/revisions     {revisions}")
    print(f"total link objects       {layers + revisions}")
    print(f"bytes (at {LINK_SIZE} B each)   {(layers + revisions) * LINK_SIZE}")
    print("\nsample keys:")
    for key in shown:
        print(f"  {key}")

    return 0


def run_apply(dsn: str, client: Any, bucket: str, workers: int) -> int:
    """Write every link object, reporting failures without aborting the run."""
    done = Counter("written")
    failures: list[tuple[str, str]] = []
    lock = threading.Lock()
    started = time.perf_counter()

    def work(item: tuple[str, str, bool]) -> None:
        repository, digest, manifest = item
        try:
            put_link(client, bucket, repository, digest, manifest)
            done.bump()
        except Exception as exc:  # noqa: BLE001 - reported, run continues
            with lock:
                failures.append((f"{repository}@{digest}", str(exc)))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for _ in pool.map(work, iter_links(dsn)):
            pass

    elapsed = time.perf_counter() - started
    rate = done.total / max(elapsed, 1e-6)
    print(
        f"\nwrote {done.total} link objects in {elapsed:.1f}s ({rate:.0f}/s, {workers} workers)"
    )

    if failures:
        print(f"FAIL  {len(failures)} link objects did not write", file=sys.stderr)
        for name, err in failures[:20]:
            print(f"  {name}: {err}", file=sys.stderr)
        print("re-run `apply` — PUT is idempotent", file=sys.stderr)
        return 1

    return 0


def run_verify(dsn: str, client: Any, bucket: str, sample: int, workers: int) -> int:
    """HEAD a random sample of link objects and check they exist at 71 bytes."""
    population = list(iter_links(dsn))
    if not population:
        print("FAIL  the derivation returned no links at all", file=sys.stderr)
        return 1

    picks = random.sample(population, min(sample, len(population)))
    bad: list[str] = []
    lock = threading.Lock()

    def check(item: tuple[str, str, bool]) -> None:
        repository, digest, manifest = item
        key = link_key(repository, digest, manifest=manifest)
        try:
            head = client.head_object(Bucket=bucket, Key=key)
            size = int(head["ContentLength"])
            if size != LINK_SIZE:
                with lock:
                    bad.append(f"{key}: size {size} != {LINK_SIZE}")
        except Exception as exc:  # noqa: BLE001 - collected into the report
            with lock:
                bad.append(f"{key}: {exc}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for _ in pool.map(check, picks):
            pass

    print(f"checked {len(picks)} of {len(population)} link objects")
    if bad:
        print(f"FAIL  {len(bad)} missing or wrong-sized", file=sys.stderr)
        for line in bad[:20]:
            print(f"  {line}", file=sys.stderr)
        return 1

    print("PASS  sampled link objects are present")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=("plan", "apply", "verify"))
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--sample", type=int, default=200)

    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()

    dsn = db_dsn()

    if args.mode == "plan":
        return run_plan(dsn, args.sample)

    bucket = os.environ.get("HARBOR_S3_BUCKET", "")
    access = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    endpoint = os.environ.get("HARBOR_S3_ENDPOINT", DEFAULT_ENDPOINT).rstrip("/")

    if not bucket or not access or not secret:
        sys.exit("need HARBOR_S3_BUCKET and AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")

    err = bucket_forbidden(bucket) or endpoint_forbidden(endpoint)
    if err:
        sys.exit(err)

    print(f"endpoint {endpoint}", flush=True)
    print(f"bucket   {bucket}", flush=True)

    client = s3_client(endpoint, access, secret)
    client.head_bucket(Bucket=bucket)

    if args.mode == "apply":
        return run_apply(dsn, client, bucket, args.workers)

    return run_verify(dsn, client, bucket, args.sample, args.workers)


if __name__ == "__main__":
    sys.exit(main())
