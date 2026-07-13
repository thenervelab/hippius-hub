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


def plan_packs(
    chunks: List[Tuple[str, int, int]],
    dedup_index: Dict[str, DedupEntry],
    pack_size: int,
) -> PackPlan:
    """Partition file-ordered `chunks` into reused refs and new packs.

    `chunks` is (chunk_digest, size, file_offset) in file order. `dedup_index` maps
    a chunk digest to the (pack_digest, pack_offset) where it already lives. A pack
    closes once it reaches `pack_size`, so it may overshoot by at most the last
    chunk added (bounded by fastcdc's 16 MiB max chunk); packs have no minimum.
    """
    if pack_size <= 0:
        raise ValueError(f"pack_size must be positive, got {pack_size}")
    planned: List[PlannedChunk] = []
    new_packs: List[Tuple[Tuple[int, int], ...]] = []
    cur_ranges: List[Tuple[int, int]] = []
    cur_offset = 0  # byte offset within the pack currently being built

    def close() -> None:
        nonlocal cur_ranges, cur_offset
        if cur_ranges:
            new_packs.append(tuple(cur_ranges))
            cur_ranges = []
            cur_offset = 0

    for digest, size, file_offset in chunks:
        hit = dedup_index.get(digest)
        if hit is not None:
            pack_digest, pack_offset = hit
            planned.append(PlannedChunk(digest, size, pack_offset, pack_digest=pack_digest))
            continue
        # New chunk → append to the pack currently open. Its eventual index is
        # `len(new_packs)` because the open pack is appended on close().
        planned.append(PlannedChunk(digest, size, cur_offset, new_pack_index=len(new_packs)))
        cur_ranges.append((file_offset, size))
        cur_offset += size
        if cur_offset >= pack_size:
            close()
    close()
    return PackPlan(tuple(planned), tuple(NewPack(r) for r in new_packs))


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
