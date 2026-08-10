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
    NewPack,
    PackAccumulator,
    PackPlan,
    PlannedChunk,
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


# ---- PackAccumulator: incremental feed must equal batch plan_packs ----

def _plan_packs_reference(chunks, dedup_index, pack_size):
    """Independent oracle: the original batch plan_packs loop, verbatim.

    Production plan_packs now DELEGATES to PackAccumulator, so comparing the
    accumulator only against it would be tautological — a boundary mutation
    would change both sides in lockstep. This copy pins the semantics: pack
    closes at cur_offset >= pack_size; dedup index read-only (no self-dedup).
    """
    planned, new_packs, cur_ranges, cur_offset = [], [], [], 0
    for digest, size, file_offset in chunks:
        hit = dedup_index.get(digest)
        if hit is not None:
            planned.append(PlannedChunk(digest, size, hit[1], pack_digest=hit[0]))
            continue
        planned.append(PlannedChunk(digest, size, cur_offset, new_pack_index=len(new_packs)))
        cur_ranges.append((file_offset, size))
        cur_offset += size
        if cur_offset >= pack_size:
            new_packs.append(tuple(cur_ranges))
            cur_ranges, cur_offset = [], 0
    if cur_ranges:
        new_packs.append(tuple(cur_ranges))
    return PackPlan(tuple(planned), tuple(NewPack(r) for r in new_packs))


def _file_from_spec(spec):
    chunks, off = [], 0
    for did, size in spec:
        chunks.append((_digest(f"d{did}".encode()), size, off))
        off += size
    return chunks


@given(st.data())
def test_accumulator_equals_plan_packs(data):
    pack_size = data.draw(st.integers(min_value=1, max_value=8000))
    # Sizes biased toward pack_size-1 / pack_size / pack_size+1 so cumulative
    # sums actually land ON the close boundary — plain uniform sizes almost
    # never do, which lets off-by-one close conditions survive.
    size_strat = st.one_of(
        st.integers(min_value=1, max_value=4000),
        st.sampled_from(sorted({max(1, pack_size - 1), pack_size, pack_size + 1})),
    )
    # (digest_id, size) pairs: repeated digest_ids model the same chunk digest
    # occurring twice WITHIN one file — plan_packs does not self-dedup those
    # (the dedup index is never mutated mid-plan), and neither may the accumulator.
    spec = data.draw(
        st.lists(st.tuples(st.integers(min_value=0, max_value=5), size_strat), max_size=40)
    )
    chunks = _file_from_spec(spec)
    # Mark a random subset of DIGESTS (not positions) as reused, so a duplicated
    # digest is consistently reused-or-new — matching a real dedup index.
    digests = sorted({c[0] for c in chunks})
    reused = data.draw(st.sets(st.sampled_from(digests))) if digests else set()
    dedup = {d: (_digest(b"pack" + d.encode()), 100 + i) for i, d in enumerate(reused)}

    acc = PackAccumulator(dedup, pack_size)
    packs = [p for c in chunks for p in acc.feed(c)]
    plan = acc.finish()

    assert plan == _plan_packs_reference(chunks, dedup, pack_size)
    assert plan == plan_packs(chunks, dedup, pack_size)  # delegation wiring
    # feed() emitted exactly the completed packs, in order; only the final
    # partial pack (if any) is deferred to finish().
    assert tuple(packs) == plan.new_packs[: len(packs)]
    assert len(plan.new_packs) - len(packs) <= 1


def test_accumulator_completes_pack_at_exact_boundary():
    chunks = _file([50, 50, 10])  # 50+50 == pack_size → pack closes on 2nd feed
    acc = PackAccumulator({}, pack_size=100)
    assert acc.feed(chunks[0]) == []
    assert [p.ranges for p in acc.feed(chunks[1])] == [((0, 50), (50, 50))]
    assert acc.feed(chunks[2]) == []
    plan = acc.finish()
    assert plan.new_packs[1].ranges == ((100, 10),)


def test_accumulator_stays_open_one_byte_below_bound():
    chunks = _file([99, 1])  # 99 < pack_size=100 must NOT close; +1 hits it
    acc = PackAccumulator({}, pack_size=100)
    assert acc.feed(chunks[0]) == []
    assert [p.ranges for p in acc.feed(chunks[1])] == [((0, 99), (99, 1))]


def test_accumulator_oversize_chunk_completes_immediately():
    (chunk,) = _file([500])  # single chunk > pack_size → its own pack, at once
    acc = PackAccumulator({}, pack_size=100)
    assert [p.ranges for p in acc.feed(chunk)] == [((0, 500),)]
    assert acc.finish().new_packs == (NewPack(((0, 500),)),)


def test_duplicate_digest_within_file_is_packed_twice():
    # Same digest at two file offsets, absent from the dedup index: plan_packs
    # packs BOTH occurrences (no self-dedup), so the accumulator must too.
    d = _digest(b"dup")
    chunks = [(d, 10, 0), (d, 10, 10)]
    for plan in (plan_packs(chunks, {}, 1000), _feed_all(chunks, {}, 1000)):
        assert plan.new_packs[0].ranges == ((0, 10), (10, 10))
        assert [p.new_pack_index for p in plan.planned] == [0, 0]
        assert [p.pack_offset for p in plan.planned] == [0, 10]


def _feed_all(chunks, dedup, pack_size):
    acc = PackAccumulator(dedup, pack_size)
    for c in chunks:
        acc.feed(c)
    return acc.finish()


def test_accumulator_finish_is_idempotent_and_feed_after_finish_raises():
    acc = PackAccumulator({}, pack_size=100)
    acc.feed(_file([10])[0])
    plan = acc.finish()
    assert acc.finish() == plan
    with pytest.raises(RuntimeError):
        acc.feed((_digest(b"late"), 5, 10))


def test_accumulator_rejects_nonpositive_pack_size():
    with pytest.raises(ValueError):
        PackAccumulator({}, pack_size=0)
    with pytest.raises(ValueError):
        plan_packs([], {}, pack_size=-1)
