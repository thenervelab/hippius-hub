"""Pure packing planner for chunked-v2 (hippius_hub._packing).

Property: whatever the mix of new/reused chunks, `plan_packs` preserves file
order, covers every chunk exactly once, assigns new-chunk byte ranges that
exactly reconstruct the new chunks in order, and closes packs at the size bound.
Then resolve → serialize → parse round-trips the pointer.
"""
import hashlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hippius_hub._oci import parse_pointer_v2
from hippius_hub._packing import (
    plan_packs,
    pointer_v2_bytes,
    resolve_pointer_chunks,
)


def _digest(seed: bytes) -> str:
    return "sha256:" + hashlib.sha256(seed).hexdigest()


def _file(sizes):
    """(chunk_digest, size, file_offset) list from chunk sizes, cumulative offsets."""
    chunks, off = [], 0
    for i, s in enumerate(sizes):
        chunks.append((_digest(f"c{i}".encode()), s, off))
        off += s
    return chunks


# ---- property test ----

_sizes = st.lists(st.integers(min_value=1, max_value=4000), min_size=0, max_size=40)


@given(_sizes, st.integers(min_value=1, max_value=8000), st.data())
def test_plan_packs_invariants(sizes, pack_size, data):
    chunks = _file(sizes)
    # Mark a random subset of chunks as already present (reused).
    reused_idx = data.draw(st.sets(st.integers(0, len(chunks) - 1), max_size=len(chunks))) if chunks else set()
    dedup = {chunks[i][0]: (_digest(f"pack{i}".encode()), 100 + i) for i in reused_idx}

    plan = plan_packs(chunks, dedup, pack_size)

    # order + exact coverage
    assert len(plan.planned) == len(chunks)
    assert [p.chunk_digest for p in plan.planned] == [c[0] for c in chunks]

    # reused vs new classification matches the index
    for c, p in zip(chunks, plan.planned):
        if c[0] in dedup:
            assert p.new_pack_index is None and p.pack_digest == dedup[c[0]][0]
        else:
            assert p.new_pack_index is not None and p.pack_digest is None

    # new packs' ranges exactly reconstruct the new chunks (file_offset, size) in order
    new_ranges = [r for np in plan.new_packs for r in np.ranges]
    expected = [(c[2], c[1]) for c in chunks if c[0] not in dedup]
    assert new_ranges == expected

    # within-pack offsets are cumulative from 0; non-final packs reached the bound
    for i, np in enumerate(plan.new_packs):
        running = 0
        for off, size in np.ranges:
            running += size
        assert np.size == sum(s for _o, s in np.ranges)
        if i + 1 < len(plan.new_packs):
            assert np.size >= pack_size, "a closed non-final pack must reach pack_size"
    for p in plan.planned:
        if p.new_pack_index is not None:
            np = plan.new_packs[p.new_pack_index]
            # offset within pack equals sum of preceding ranges up to this chunk
            assert 0 <= p.pack_offset < np.size or (np.size == 0)

    # round-trip: resolve with fake uploaded digests → serialize → parse
    fake = [_digest(f"up{i}".encode()) for i in range(len(plan.new_packs))]
    resolved = resolve_pointer_chunks(plan, fake)
    assert len(resolved) == len(chunks)
    if resolved:
        whole = _digest(b"whole")[7:]
        blob = pointer_v2_bytes(whole, sum(sizes), resolved)
        parsed = parse_pointer_v2(blob)
        assert tuple((r.chunk_digest, r.size, r.pack_digest, r.pack_offset) for r in parsed) == resolved


# ---- explicit edges ----

def test_all_new_single_pack_when_under_size():
    chunks = _file([10, 20, 30])
    plan = plan_packs(chunks, {}, pack_size=1000)
    assert len(plan.new_packs) == 1
    assert plan.new_packs[0].ranges == ((0, 10), (10, 20), (30, 30))
    assert [p.pack_offset for p in plan.planned] == [0, 10, 30]


def test_pack_closes_at_size_bound():
    chunks = _file([60, 60, 60])  # pack_size 100 → close after 2nd (120>=100), 3rd new pack
    plan = plan_packs(chunks, {}, pack_size=100)
    assert len(plan.new_packs) == 2
    assert plan.new_packs[0].ranges == ((0, 60), (60, 60))
    assert plan.new_packs[1].ranges == ((120, 60),)
    assert plan.planned[2].new_pack_index == 1 and plan.planned[2].pack_offset == 0


def test_reused_chunks_transfer_nothing():
    chunks = _file([10, 20, 30])
    dedup = {chunks[1][0]: ("sha256:" + "a" * 64, 5)}  # middle chunk already present
    plan = plan_packs(chunks, dedup, pack_size=1000)
    # only 2 new chunks packed; the reused one is not in any pack range
    new_ranges = [r for np in plan.new_packs for r in np.ranges]
    assert new_ranges == [(0, 10), (30, 30)]
    assert plan.planned[1].pack_digest == "sha256:" + "a" * 64
    assert plan.planned[1].new_pack_index is None


def test_resolve_rejects_wrong_digest_count():
    plan = plan_packs(_file([10]), {}, pack_size=1000)
    with pytest.raises(ValueError):
        resolve_pointer_chunks(plan, [])  # 1 new pack, 0 digests
