"""Client-side pieces of the chunk-index service (Track C).

For now this holds only the **sampling rule** — the frozen contract (master plan
§6.4) that decides which chunks are "key chunks" eligible for a global dedup query.
It MUST compute identically to the index service (in ``hcfs``): if the two sides
disagree — on which bytes, or on endianness — deduplication degrades *silently*,
with no error and no way to tell from the outside. That is why the rule is pinned to
the byte here and shipped with a shared test-vector fixture
(``tests/fixtures/key_chunk_vectors.json``) that BOTH this repo's tests and the
service's tests read, checksum-pinned so an edit to either copy fails CI at once.

The index query/announce HTTP client (C3) will live here too, once its API is frozen.
"""
from __future__ import annotations

# One chunk in this many is a "key chunk" queried against the global index. At a
# 256 KiB chunk size that is ~one query per 256 MiB of upload — a few thousand for a
# 200 GB push. The value is arbitrary but MUST match the service: change it and you
# change which chunks are sampled, silently lowering the hit rate if the two sides
# ever disagree.
KEY_CHUNK_SAMPLE_RATE = 1024


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
