"""Contract ② — the key-chunk sampling rule (master plan §6.4).

The client and the index service (``hcfs``) MUST compute ``is_key_chunk``
identically, or deduplication degrades *silently* — no error, no alert, just fewer
hits forever. This pins our implementation against the SHARED fixture and pins the
fixture's SHA-256 so an edit to either repo's copy fails CI. The same constant is
pinned on the ``hcfs`` side; the two copies must stay byte-identical.
"""
from __future__ import annotations

import hashlib
import json
import os

import pytest

from hippius_hub._dedup_index import KEY_CHUNK_SAMPLE_RATE, is_key_chunk

_FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "key_chunk_vectors.json")

# The shared fixture's SHA-256. The IDENTICAL constant is pinned in hcfs; editing the
# fixture in either repo without updating BOTH constants turns the builds red — the
# only mechanism that makes a divergent sampling rule loud instead of silent. Regen
# with scripts/gen_key_chunk_vectors.py, which prints the new hash.
EXPECTED_FIXTURE_SHA256 = "ecbffe83b6ea798ed577d9ed34c785ac58e0ea75cde285f48d63d2c668877bb0"


def _load():
    with open(_FIXTURE, "rb") as f:
        raw = f.read()
    return raw, json.loads(raw)


def test_fixture_is_pinned_by_checksum():
    raw, _ = _load()
    got = hashlib.sha256(raw).hexdigest()
    assert got == EXPECTED_FIXTURE_SHA256, (
        "key_chunk_vectors.json changed. If deliberate, regenerate it "
        "(scripts/gen_key_chunk_vectors.py) and update EXPECTED_FIXTURE_SHA256 here "
        "AND the matching constant in hcfs — the two copies must stay byte-identical."
    )


def test_is_key_chunk_matches_every_vector():
    _, payload = _load()
    assert payload["sample_rate"] == KEY_CHUNK_SAMPLE_RATE
    assert payload["vectors"], "fixture must not be empty"
    for v in payload["vectors"]:
        digest = bytes.fromhex(v["digest"])
        assert is_key_chunk(digest) == v["is_key"], f"mismatch on {v['note']}: {v['digest']}"


def test_endianness_trap_actually_distinguishes_endianness():
    # Prove the LE-vs-BE choice has teeth: on the trap vectors a big-endian
    # implementation would DISAGREE with is_key_chunk — so the fixture catches a
    # service that got the endianness wrong, rather than silently under-deduping.
    _, payload = _load()
    traps = [v for v in payload["vectors"] if "endianness trap" in v["note"]]
    assert traps, "fixture must include endianness-trap vectors"
    for v in traps:
        digest = bytes.fromhex(v["digest"])
        le_hit = int.from_bytes(digest[24:32], "little") % KEY_CHUNK_SAMPLE_RATE == 0
        be_hit = int.from_bytes(digest[24:32], "big") % KEY_CHUNK_SAMPLE_RATE == 0
        assert le_hit != be_hit, f"trap vector must distinguish endianness: {v['note']}"
        assert is_key_chunk(digest) == le_hit  # we follow the little-endian rule


def test_rejects_a_non_32_byte_digest():
    with pytest.raises(ValueError):
        is_key_chunk(b"\x00" * 31)
    with pytest.raises(ValueError):
        is_key_chunk(b"\x00" * 33)
