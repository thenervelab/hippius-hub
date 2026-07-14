"""Client-side pieces of the chunk-index service (Track C).

Two things live here, both halves of the frozen contract with the index service
(which lives in ``hcfs``):

* The **sampling rule** (``is_key_chunk`` — master plan §6.4, frozen contract ②):
  which chunks are "key chunks" eligible for a global dedup query. It MUST compute
  identically to the service: if the two sides disagree — on which bytes, or on
  endianness — deduplication degrades *silently*, with no error and no way to tell.
  That is why the rule is pinned to the byte here and shipped with a shared
  test-vector fixture (``tests/fixtures/key_chunk_vectors.json``) that BOTH this
  repo's tests and the service's tests read, checksum-pinned so an edit to either
  copy fails CI at once.

* The **query/announce HTTP client** (C3 — master plan §6.3, frozen contract ①):
  ``query_chunks`` asks the index which key chunks it already holds (and where);
  ``announce_chunks`` tells it about chunks we just stored. Both are *fail-open* by
  construction (§6.6): the index is a cache, never a source of truth, so any error,
  timeout, or malformed response costs dedup, NEVER correctness and NEVER the upload.
  The client is inert until ``HIPPIUS_DEDUP_INDEX_URL`` is set
  (``constants.resolve_dedup_index_url``), so it ships safely before the service and
  the P3 registry-safety work exist. Wiring it into the upload path — and the OCI
  ``?mount=`` fast-path it enables — is a separate, later step.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from . import _http
from .auth import resolve_auth_header

_log = logging.getLogger(__name__)

# One chunk in this many is a "key chunk" queried against the global index. At a
# 256 KiB chunk size that is ~one query per 256 MiB of upload — a few thousand for a
# 200 GB push. The value is arbitrary but MUST match the service: change it and you
# change which chunks are sampled, silently lowering the hit rate if the two sides
# ever disagree.
KEY_CHUNK_SAMPLE_RATE = 1024

# The query endpoint accepts at most this many chunk hashes per call (§6.3); we batch
# larger requests. Kept in lockstep with the service's declared limit.
MAX_QUERY_BATCH = 256

# Aggressive on purpose (§6.6): "a slow index must not slow the upload." A couple of
# seconds, then we treat the batch as a miss and upload those chunks ourselves.
QUERY_TIMEOUT_SECS = 2.0
ANNOUNCE_TIMEOUT_SECS = 2.0


def is_key_chunk(digest: bytes) -> bool:
    """Whether a chunk's digest makes it a *key chunk* — eligible for a global dedup
    query (§6.4). Operates on the RAW 32-byte SHA-256 digest (not hex, not base64).

    A chunk is a key chunk iff the little-endian ``u64`` of its last 8 bytes is a
    multiple of ``KEY_CHUNK_SAMPLE_RATE``. The endianness is load-bearing — the
    client and the index service must agree to the byte — so it is pinned as
    LITTLE-endian over ``digest[24:32]`` and proven against the shared fixture.

    NOTE: the full §6.4 rule also makes a file's FIRST chunk a key chunk. That clause
    is *positional* — the caller adds it (``i == 0 or is_key_chunk(d)``) — and cannot
    be derived from the digest alone, so it is not this function's concern.
    """
    if len(digest) != 32:
        raise ValueError(f"expected a 32-byte SHA-256 digest, got {len(digest)} bytes")
    tail = int.from_bytes(digest[24:32], "little")
    return tail % KEY_CHUNK_SAMPLE_RATE == 0


@dataclass(frozen=True)
class ChunkRef:
    """A chunk's placement within a pack: bare-hex digest, byte offset, byte size.

    Used both for the neighbourhood returned by a query hit and for the chunk list
    sent on announce — they are the same shape on the wire.
    """

    digest: str
    offset: int
    size: int


@dataclass(frozen=True)
class IndexHit:
    """A query hit for one key chunk: the pack that holds it, the repo to mount that
    pack FROM (``?mount=<pack>&from=<repo>``), and the pack's whole neighbourhood —
    every chunk in that pack, which is what makes 1/1024 sampling recover most of the
    available dedup (§6.3). ``chunks`` may be empty if the service omitted it.
    """

    pack: str
    repo: str
    chunks: Tuple[ChunkRef, ...]


def query_chunks(
    index_url: Optional[str], token, digests: Iterable[str]
) -> Dict[str, IndexHit]:
    """Ask the index which of ``digests`` (bare-hex key-chunk hashes) it already holds.

    Returns ``{queried_hex: IndexHit}`` for every hit. FAIL-OPEN (§6.6): a missing URL,
    a timeout, a non-2xx, or a malformed body for a batch simply contributes no hits —
    this function never raises, so the caller uploads any chunk it did not get a hit
    for, exactly as it does today. Sends at most ``MAX_QUERY_BATCH`` hashes per request.

    ``digests`` are bare hex (no ``sha256:`` prefix), matching the wire contract; the
    caller owns any conversion from an internal ``sha256:...`` form.
    """
    digest_list: List[str] = list(digests)
    if not index_url or not digest_list:
        return {}

    url = index_url.rstrip("/") + "/v1/chunks/query"
    headers = _auth_headers(token)
    hits: Dict[str, IndexHit] = {}

    for start in range(0, len(digest_list), MAX_QUERY_BATCH):
        batch = digest_list[start : start + MAX_QUERY_BATCH]
        try:
            resp = _http.client().post(
                url, json={"chunks": batch}, headers=headers, timeout=QUERY_TIMEOUT_SECS
            )
            resp.raise_for_status()
            hits.update(_parse_hits(resp.json()))
        except Exception:
            # The index is a cache: never let a query error, timeout, or bad response
            # fail (or slow, beyond the timeout) an upload. A failed batch is a miss.
            _log.debug("dedup-index query batch failed; treating as miss", exc_info=True)
            continue
    return hits


def announce_chunks(
    index_url: Optional[str], token, repo: str, pack: str, chunks: Iterable[ChunkRef]
) -> None:
    """Tell the index about chunks we just stored — fire-and-forget (§6.3/§6.6).

    Sends ALL of ``chunks`` (not just key chunks): announce populates the neighbourhood
    that a future query hit returns. NEVER raises — a failure costs future dedup, never
    this push. NEVER sends a ``visibility`` field: the service resolves visibility from
    the registry itself (§6.5), so a buggy or modified client cannot leak a private
    repo's chunks into the global pool. A missing URL makes this a no-op.
    """
    if not index_url:
        return

    url = index_url.rstrip("/") + "/v1/chunks/announce"
    body = {
        "repo": repo,
        "pack": pack,
        "chunks": [{"digest": c.digest, "offset": c.offset, "size": c.size} for c in chunks],
    }
    try:
        _http.client().post(
            url, json=body, headers=_auth_headers(token), timeout=ANNOUNCE_TIMEOUT_SECS
        )
    except Exception:
        _log.debug("dedup-index announce failed; ignoring (costs future dedup only)", exc_info=True)


def _auth_headers(token) -> Dict[str, str]:
    header = resolve_auth_header(token)
    return {"Authorization": header} if header else {}


def _parse_hits(payload) -> Dict[str, IndexHit]:
    """Parse a ``/v1/chunks/query`` response into ``{hex: IndexHit}``, defensively.

    A malformed individual hit is skipped rather than discarding the whole batch —
    a partial answer from the index is still useful, and robustness here costs nothing.
    """
    out: Dict[str, IndexHit] = {}
    hits = payload.get("hits") if isinstance(payload, dict) else None
    if not isinstance(hits, dict):
        return out
    for queried_hex, hit in hits.items():
        try:
            pack = hit["pack"]
            repo = hit["repo"]
            if not (isinstance(pack, str) and isinstance(repo, str)):
                continue
            chunks = tuple(
                ChunkRef(c["digest"], int(c["offset"]), int(c["size"]))
                for c in hit.get("chunks", [])
            )
            out[queried_hex] = IndexHit(pack=pack, repo=repo, chunks=chunks)
        except (KeyError, TypeError, ValueError):
            continue
    return out
