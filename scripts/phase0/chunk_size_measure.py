#!/usr/bin/env python3
"""C5 — measure what a smaller CDC chunk size actually buys, before committing to it.

Decision 3 in the master plan (dropping HIPPIUS_CDC_AVG_SIZE from 4 MiB to 256 KiB)
is IRREVERSIBLE: chunks at the new size never dedup against chunks already stored at
the old size. So measure first. This chunks a real file with the SHIPPED chunker at
4 MiB / 1 MiB / 256 KiB and reports the three numbers that decide it:

  * dedup ratio      — how much of an updated file re-uploads (needs two versions)
  * chunk count      — index/query cost scales with it
  * pointer size     — A.1's missing column: the pointer is downloaded on EVERY
                       fetch of the file, and it grows as chunks shrink. This is the
                       cost that argues 256 KiB over the theoretical 64 KiB optimum.

Usage:
  python scripts/phase0/chunk_size_measure.py FILE                 # count + pointer size
  python scripts/phase0/chunk_size_measure.py FILE_V17 FILE_V18    # + dedup ratio v17->v18
  python scripts/phase0/chunk_size_measure.py --self-test          # synthetic, no corpus

Needs the built extension (`maturin develop`) — it drives the real FastCDC chunker.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile

# The chunk sizes to sweep. 4 MiB is today's default (fastcdc's AVERAGE_MAX); 256 KiB
# is the plan's recommendation; 1 MiB is the midpoint. All inside the chunker's
# accepted [256 B, 4 MiB] range.
AVG_SIZES = [4 * 1024 * 1024, 1024 * 1024, 256 * 1024]
PACK_SIZE = 64 * 1024 * 1024  # for the pointer's pack assignment; does not affect size

_PLACEHOLDER_PACK = "sha256:" + "0" * 64  # same length as a real pack digest


def _chunks(path: str, avg: int):
    """(whole_hex, [(chunk_sha_with_prefix, size, offset)]) via the shipped chunker."""
    from hippius_hub.hippius_core import chunk_and_hash_native

    whole_hex, metas = chunk_and_hash_native(path, avg)
    return whole_hex, [(f"sha256:{h}", size, offset) for h, offset, size in metas]


def _pointer_size(path: str, avg: int):
    """(chunk_count, serialized_pointer_bytes) for `path` chunked at `avg`."""
    from hippius_hub._packing import plan_packs, pointer_v2_bytes, resolve_pointer_chunks

    file_size = os.path.getsize(path)
    whole_hex, chunks = _chunks(path, avg)
    plan = plan_packs(chunks, {}, PACK_SIZE)  # empty index → every chunk is new
    pointer_chunks = resolve_pointer_chunks(plan, [_PLACEHOLDER_PACK] * len(plan.new_packs))
    pointer = pointer_v2_bytes(whole_hex, file_size, pointer_chunks)
    return len(chunks), len(pointer)


def _dedup_ratio(file_a: str, file_b: str, avg: int) -> float:
    """Fraction of file B that must be uploaded given file A already exists, at `avg`
    — i.e. novel bytes of B / size of B (lower = more reuse)."""
    _, ca = _chunks(file_a, avg)
    _, cb = _chunks(file_b, avg)
    have = {d for d, _s, _o in ca}
    novel = sum(s for d, s, _o in cb if d not in have)
    total_b = sum(s for _d, s, _o in cb)
    return novel / total_b if total_b else 0.0


def _human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def measure(file_a: str, file_b: str | None) -> list:
    rows = []
    for avg in AVG_SIZES:
        count, ptr = _pointer_size(file_a, avg)
        ratio = _dedup_ratio(file_a, file_b, avg) if file_b else None
        rows.append((avg, count, ptr, ratio))
    return rows


def _print(rows, two_files: bool) -> None:
    hdr = f"{'avg chunk':>10} {'chunks':>9} {'pointer':>12} {'ptr/chunk':>10}"
    if two_files:
        hdr += f" {'push (B/B)':>12}"
    print(hdr)
    print("-" * len(hdr))
    for avg, count, ptr, ratio in rows:
        line = f"{_human(avg):>10} {count:>9} {_human(ptr):>12} {ptr / max(count, 1):>9.0f}B"
        if two_files:
            line += f" {ratio * 100:>10.1f}%"
        print(line)


def self_test() -> int:
    print("SELF-TEST — 16 MiB synthetic file, sweeping chunk size.\n")
    tmp = os.path.join(tempfile.mkdtemp(prefix="c5_measure_"), "blob.bin")
    with open(tmp, "wb") as f:
        f.write(os.urandom(16 * 1024 * 1024))
    rows = measure(tmp, None)
    _print(rows, two_files=False)
    counts = [c for _a, c, _p, _r in rows]  # order: 4 MiB, 1 MiB, 256 KiB
    ptrs = [p for _a, _c, p, _r in rows]
    ok = counts[0] < counts[1] < counts[2] and ptrs[0] < ptrs[1] < ptrs[2]
    print(f"\nchunks grow and pointer grows as chunks shrink -> {'PASS' if ok else 'FAIL'}")
    print("(the pointer-size column is the whole point of this tool — A.1's missing number)")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="C5 chunk-size measurement.")
    ap.add_argument("file_a", nargs="?")
    ap.add_argument("file_b", nargs="?", help="a second version, for the dedup ratio")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    if not args.file_a:
        ap.error("provide FILE (and optionally a second version), or --self-test")
    print(f"Chunk-size sweep: {args.file_a}" + (f"  ->  {args.file_b}" if args.file_b else "") + "\n")
    _print(measure(args.file_a, args.file_b), two_files=bool(args.file_b))
    return 0


if __name__ == "__main__":
    sys.exit(main())
