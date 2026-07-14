#!/usr/bin/env python3
"""Generate the shared key-chunk sampling test vectors (frozen contract §6.4).

Writes ``tests/fixtures/key_chunk_vectors.json`` — a deterministic list of 32-byte
SHA-256 digests (hex) and whether each is a key chunk under the little-endian
``tail % 1024 == 0`` rule. The SAME file is committed to BOTH ``hippius-hub`` and
``hcfs``; each side's tests assert its SHA-256 against a pinned constant, so an edit
to either copy turns both builds red — the only mechanism that makes a divergent
sampling rule loud instead of silent.

Regenerating this file changes that hash: if you ever do, update the pinned constant
in BOTH repos deliberately, and keep the two copies byte-identical.

Run: ``python scripts/gen_key_chunk_vectors.py`` (then re-pin the SHA-256 in the test).
"""
from __future__ import annotations

import hashlib
import json
import os

SAMPLE_RATE = 1024


def _digest(filler_byte: int, tail: bytes) -> str:
    """A 32-byte digest: 24 filler bytes (ignored by the rule, varied so vectors are
    distinct) followed by the 8 tail bytes the rule actually reads."""
    return (bytes([filler_byte]) * 24 + tail).hex()


def _le(value: int) -> bytes:
    return value.to_bytes(8, "little")


def build_vectors():
    vectors = []

    def add(filler: int, tail: bytes, note: str) -> None:
        tail_le = int.from_bytes(tail, "little")
        vectors.append(
            {
                "digest": _digest(filler, tail),
                "is_key": tail_le % SAMPLE_RATE == 0,
                "note": note,
            }
        )

    # Plain hits: LE tail is a multiple of 1024.
    add(0x11, _le(0), "tail 0 -> hit")
    add(0x22, _le(1024), "tail 1024 -> hit")
    add(0x33, _le(1024 * 7), "tail 7168 -> hit")
    # Plain misses.
    add(0x44, _le(1), "tail 1 -> miss")
    add(0x55, _le(1023), "tail 1023 -> miss")
    add(0x66, _le(512), "tail 512 -> miss")
    # Endianness traps — the whole reason this fixture exists.
    #   00..00 01: LE = 2**56 (multiple of 1024) -> HIT; BE = 1 -> a big-endian impl
    #   would call it a MISS. Expected True catches that bug.
    add(0x77, bytes([0, 0, 0, 0, 0, 0, 0, 1]), "endianness trap: LE hit, BE miss")
    #   01 00..00: LE = 1 -> MISS; BE = 2**56 -> a big-endian impl would call it a
    #   HIT. Expected False catches that bug.
    add(0x88, bytes([1, 0, 0, 0, 0, 0, 0, 0]), "endianness trap: LE miss, BE hit")
    # Extremes.
    add(0x00, bytes(8), "all-zero tail -> hit")
    add(0xFF, bytes([0xFF] * 8), "all-0xFF tail (2**64-1) -> miss")
    return vectors


def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    out = os.path.normpath(os.path.join(here, "..", "tests", "fixtures", "key_chunk_vectors.json"))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    payload = {
        "_comment": (
            "Shared key-chunk sampling vectors (frozen contract, storage-efficiency "
            "master plan §6.4). Committed byte-identically to BOTH hippius-hub and "
            "hcfs; each side pins this file's SHA-256. Regenerate with "
            "scripts/gen_key_chunk_vectors.py."
        ),
        "sample_rate": SAMPLE_RATE,
        "rule": "int.from_bytes(digest[24:32], 'little') % sample_rate == 0",
        "vectors": build_vectors(),
    }
    raw = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with open(out, "wb") as f:
        f.write(raw)
    print(f"wrote {out} ({len(payload['vectors'])} vectors)")
    print(f"sha256 {hashlib.sha256(raw).hexdigest()}")


if __name__ == "__main__":
    main()
