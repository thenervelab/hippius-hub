"""Pure packing logic for the chunked-v2 layout (no I/O, no network).

Given a file's CDC chunks (in file order) and a dedup index of chunks already
present in prior revisions' packs, decide which chunks are *reused* (referenced by
range into an existing pack, zero bytes transferred) and which are *new* (packed,
in file order, into ~64 MiB pack blobs to upload). The uploader executes the plan
and computes each new pack's content digest; `resolve_pointer_chunks` then fills
those digests back into the file-ordered chunk list that becomes the pointer blob.

Kept pure so the packing invariants (order preservation, exact coverage, pack-size
bounds) are property-tested without a registry — see tests/test_packing.py.
"""
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .constants import CHUNKED_LAYOUT_V2

# (pack_digest, pack_offset) for a chunk already stored in some prior pack.
DedupEntry = Tuple[str, int]


@dataclass(frozen=True)
class NewPack:
    """A pack to build and upload: file byte-ranges to concatenate, in pack order.

    `ranges` are (file_offset, size) read straight from the source file; their
    concatenation IS the pack blob, and its sha256 becomes the pack digest.
    """

    ranges: Tuple[Tuple[int, int], ...]

    @property
    def size(self) -> int:
        return sum(size for _off, size in self.ranges)


@dataclass(frozen=True)
class PlannedChunk:
    """One chunk in file order, resolved to a location once packs are uploaded.

    A *reused* chunk has `pack_digest` set (from the dedup index). A *new* chunk has
    `new_pack_index` set (an index into `PackPlan.new_packs`) and `pack_digest is
    None` until `resolve_pointer_chunks` substitutes the uploaded pack's digest.
    `pack_offset` is the byte offset of the chunk *within its pack* either way.
    """

    chunk_digest: str
    size: int
    pack_offset: int
    pack_digest: Optional[str] = None
    new_pack_index: Optional[int] = None


@dataclass(frozen=True)
class PackPlan:
    planned: Tuple[PlannedChunk, ...]
    new_packs: Tuple[NewPack, ...]


class PackAccumulator:
    """Incremental `plan_packs`: feed chunks in file order, packs complete as
    ~`pack_size` of NEW bytes accumulate, so uploads start before chunking ends.

    Invariant: for any chunk sequence, feeding all chunks then `finish()` yields
    a `PackPlan` identical to `plan_packs(chunks, dedup_index, pack_size)` —
    `plan_packs` is literally implemented that way, and the equivalence is
    property-tested in tests/test_packing.py. The dedup index is read, never
    mutated: a digest appearing twice in one file (and absent from the index)
    is packed twice, exactly as `plan_packs` always did.

    `finish()` flushes the final partial pack and is idempotent; `feed()` after
    `finish()` raises RuntimeError (the plan is already sealed).

    Not thread-safe; feed from one thread.
    """

    def __init__(self, dedup_index: Dict[str, DedupEntry], pack_size: int) -> None:
        if pack_size <= 0:
            raise ValueError(f"pack_size must be positive, got {pack_size}")
        self._dedup_index = dedup_index
        self._pack_size = pack_size
        self._planned: List[PlannedChunk] = []
        self._new_packs: List[NewPack] = []
        self._cur_ranges: List[Tuple[int, int]] = []
        self._cur_offset = 0  # byte offset within the pack currently being built
        self._plan: Optional[PackPlan] = None

    def feed(self, chunk: Tuple[str, int, int]) -> List[NewPack]:
        """Plan one (chunk_digest, size, file_offset); return packs just completed.

        A pack closes once it reaches `pack_size`, so it may overshoot by at most
        the last chunk added (bounded by fastcdc's 16 MiB max chunk); packs have
        no minimum. At most one pack completes per feed.
        """
        if self._plan is not None:
            raise RuntimeError("feed() after finish(): the pack plan is sealed")
        digest, size, file_offset = chunk
        hit = self._dedup_index.get(digest)
        if hit is not None:
            pack_digest, pack_offset = hit
            self._planned.append(
                PlannedChunk(digest, size, pack_offset, pack_digest=pack_digest)
            )
            return []
        # New chunk → append to the pack currently open. Its eventual index is
        # `len(self._new_packs)` because the open pack is appended on close.
        self._planned.append(
            PlannedChunk(digest, size, self._cur_offset, new_pack_index=len(self._new_packs))
        )
        self._cur_ranges.append((file_offset, size))
        self._cur_offset += size
        if self._cur_offset >= self._pack_size:
            return [self._close()]
        return []

    def _close(self) -> NewPack:
        pack = NewPack(tuple(self._cur_ranges))
        self._new_packs.append(pack)
        self._cur_ranges = []
        self._cur_offset = 0
        return pack

    def finish(self) -> PackPlan:
        """Flush the final partial pack (if any) and return the sealed plan.

        Prefix property: the packs returned by `feed()` form a prefix of
        `finish().new_packs`, in order; any final partial pack is
        `new_packs[-1]` and was never returned by `feed()` — so a streaming
        caller that already handled `n_emitted` packs from `feed()` must
        submit exactly `new_packs[n_emitted:]` after `finish()`.
        """
        if self._plan is None:
            if self._cur_ranges:
                self._close()
            self._plan = PackPlan(tuple(self._planned), tuple(self._new_packs))
        return self._plan


def plan_packs(
    chunks: List[Tuple[str, int, int]],
    dedup_index: Dict[str, DedupEntry],
    pack_size: int,
) -> PackPlan:
    """Partition file-ordered `chunks` into reused refs and new packs.

    `chunks` is (chunk_digest, size, file_offset) in file order. `dedup_index` maps
    a chunk digest to the (pack_digest, pack_offset) where it already lives.
    Thin batch wrapper over `PackAccumulator` — see its docstring for the pack
    boundary and self-dedup semantics.
    """
    acc = PackAccumulator(dedup_index, pack_size)
    for chunk in chunks:
        acc.feed(chunk)
    return acc.finish()


def resolve_pointer_chunks(
    plan: PackPlan,
    new_pack_digests: List[str],
) -> Tuple[Tuple[str, int, str, int], ...]:
    """Fill uploaded pack digests into the plan, yielding the pointer's chunk list.

    `new_pack_digests[i]` is the content digest of `plan.new_packs[i]` after upload.
    Returns (chunk_digest, size, pack_digest, pack_offset) in file order — exactly
    the tuples the v2 pointer blob serializes.
    """
    if len(new_pack_digests) != len(plan.new_packs):
        raise ValueError(
            f"expected {len(plan.new_packs)} uploaded pack digests, "
            f"got {len(new_pack_digests)}"
        )
    out: List[Tuple[str, int, str, int]] = []
    for pc in plan.planned:
        pack_digest = pc.pack_digest if pc.new_pack_index is None else new_pack_digests[pc.new_pack_index]
        out.append((pc.chunk_digest, pc.size, pack_digest, pc.pack_offset))
    return tuple(out)


def pointer_v2_bytes(
    whole_hex: str,
    file_size: int,
    pointer_chunks: Tuple[Tuple[str, int, str, int], ...],
) -> bytes:
    """Serialize the deterministic v2 pointer blob.

    Canonical JSON (sorted keys, no whitespace, no timestamps): a *fresh* upload of
    identical bytes packs identically → identical pointer → pointer-level dedup. A
    re-upload references old packs, so its pointer differs — chunk-level dedup, the
    one that matters, is preserved regardless. `parse_pointer_v2` is the inverse.
    """
    doc = {
        "version": CHUNKED_LAYOUT_V2,
        "file": {"size": file_size, "digest": f"sha256:{whole_hex}"},
        "chunks": [
            {"digest": cd, "size": sz, "pack": pk, "offset": off}
            for cd, sz, pk, off in pointer_chunks
        ],
    }
    return json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")
