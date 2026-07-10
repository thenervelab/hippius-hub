# Chunked-v2 Pack Layout — Design (Xet-style packs over OCI)

**Status: DESIGN — not implemented.** Follow-up to
`2026-07-09-chunked-artifact-layout.md` (chunked-v1, live on staging). Do not
build until the Harbor writeback infra change lands and is measured — see
§Sequencing.

**Goal:** keep chunked-v1's 4 MiB dedup granularity but transfer in ~64 MiB
**pack** blobs, cutting per-file upload round-trips ~15× and restoring the
~1.2 TB/revision manifest budget — the two costs v1 paid for `fastcdc`'s 4 MiB
average cap.

## Why (evidence)

Measured on staging (2026-07-09/10, Harbor-flow probe): each blob upload costs
3 round-trips (HEAD ~0.6 s + POST-init ~1.5 s + PUT), and Harbor does NOT
support OCI single-POST (returns 202), so the round-trips cannot be collapsed
per blob. At v1's 4 MiB chunks, a 512 MiB file pays ~100 × 3 round-trips
(~3.5 min of pure latency at measured RTTs); a 68 GB 34B model pays ~17,000
chunks' worth. The bytes themselves are a separate, infra-level bottleneck
(JuiceFS writeback — separate plan); packs attack the *round-trip* term.

This is exactly HF-Xet's answer, verified against their published design
([Rearchitecting Uploads/Downloads](https://huggingface.co/blog/rearchitecting-uploads-and-downloads),
[From Chunks to Blocks](https://huggingface.co/blog/from-chunks-to-blocks),
[Xet protocol spec](https://huggingface.co/docs/xet/en/index)): dedup at small
CDC chunks (~64 KiB), but **never transfer chunks individually** — new chunks
are packed into content-addressed **xorbs (≤ 64 MiB)**, and unchanged chunks
are referenced *by range into the xorbs they already live in*. Blocks cut their
metadata entries ~1000×; their headline win (gemma-2: 191 GB → 97 GB
transferred) comes from dedup, at ~50 MB/s per-client streams. We adapt the
xorb pattern to plain OCI blobs — no CAS service; the previous revision's
pointer is the dedup index, which covers our dominant workload (re-upload of a
changed model to the same repo).

## Design

### Wire format

- **Layout value:** `com.hippius.layout: chunked-v2`. New value → every
  deployed guard-bearing reader (v1, on staging now) refuses it LOUDLY with an
  upgrade hint — the forward-compat floor shipped with v1 does its job.
  `KNOWN_LAYOUTS` gains `chunked-v2` in the same commit that teaches the
  reader to parse it.
- **Media types:** titled pointer layer `application/vnd.hippius.pointer.v2`;
  untitled pack layers `application/vnd.hippius.pack.v1` (a pack is just bytes;
  its format never changes — the *pointer* schema is what versions).
  `artifactType: application/vnd.hippius.chunked.v2`.
- **Layers:** one titled pointer layer per chunked file, plus the **union of
  all pack blobs the file references** — new packs AND old packs reused from
  prior revisions — as untitled layers. Listing reused packs is load-bearing:
  a blob is GC-safe and pullable only while some manifest references it.
  Positional pointer→chunk association (v1) is gone; association lives in the
  pointer blob.
- **Pointer layer annotations** (unchanged from v1 where possible): title,
  `com.hippius.file.size`, `com.hippius.file.digest`, `com.hippius.chunk.count`
  — so `siblings`/`list_repo_files` stay annotation-only (no pointer fetch on
  the metadata path).

### Pointer blob v2 (fetched by the reader — the one new read round-trip)

```json
{
  "version": "chunked-v2",
  "file": {"size": 68719476736, "digest": "sha256:<whole-file>"},
  "chunks": [
    {"digest": "sha256:<c0>", "size": 4194304,
     "pack": "sha256:<P0>", "offset": 0},
    {"digest": "sha256:<c1>", "size": 3145728,
     "pack": "sha256:<P0>", "offset": 4194304}
  ]
}
```

Canonical JSON (sorted keys, no whitespace, no timestamps). Chunk order =
file order. Integrity is free: the pointer layer's digest content-addresses
these bytes, so a tampered pointer fails the blob digest check.

**Determinism caveat (weaker than v1, deliberately):** a *fresh* upload packs
chunks greedily in file order → deterministic → identical files still produce
identical packs and pointers. But a *re-upload after an edit* references old
packs, so its pointer differs from a fresh upload's pointer for the same
bytes. Pointer-level dedup is lost in that case; chunk-level dedup — the one
that matters — is precisely what the layout exists to provide.

### Upload algorithm

1. **Chunk** (unchanged): `chunk_and_hash` — FastCDC, 4 MiB average. The CDC
   parameters remain the v1 wire contract; v2 changes packaging, not
   boundaries, so v1↔v2 chunk digests agree and cross-layout dedup of chunk
   *content* is possible at repack time.
2. **Build the dedup index:** fetch the current revision's manifest (already
   done for merge) and the pointer blobs of chunked files in it → map
   `chunk digest → (pack, offset, size)`. v1 files contribute too: a v1 chunk
   layer is a pack of one (`pack = chunk digest, offset 0`). No global/HEAD
   dedup in this version — local (same repo+revision history) covers the
   re-upload workload; global is a documented follow-up.
3. **Partition:** chunks found in the index are **reused** (zero bytes, zero
   requests); the rest are **new**.
4. **Pack:** concatenate new chunks in file order into packs, closing a pack
   when it reaches the 64 MiB target (a pack may overshoot by at most one
   chunk, ≤ 16 MiB — packs have no minimum; a 1-chunk edit yields one small
   pack). Pack digest = sha256 of the pack bytes (ordinary content-addressed
   blob).
5. **Upload packs** via the existing per-blob flow (HEAD + POST + PUT) — now
   3 round-trips per ~64 MiB instead of per ~4 MiB. Parallel across packs with
   the existing worker pool.
6. **Pointer + manifest:** upload pointer blob; assemble layers = pointer +
   union of referenced pack digests; annotate `chunked-v2`; PUT with If-Match
   (unchanged).

**Repack policy (bounds fragmentation):** after many edits a file's pointer
references many packs it barely uses, and dead chunks accumulate inside old
packs. Rule: while building the index, compute each old pack's **live
fraction** for this file; if `< REPACK_THRESHOLD` (proposed: 25%), treat its
still-needed chunks as *new* (they get re-packed and re-uploaded). Bounds
read scatter and storage amplification at a small, occasional upload cost.
Old packs stop being referenced once no manifest lists them → Harbor GC
reclaims them.

### Download algorithm

1. `group_files` sees a `pointer.v2` titled layer → fetch + parse the pointer
   blob (1 GET per chunked file), verify against the layer digest.
2. **Plan coalesced reads:** group chunks by pack; adjacent chunks in the same
   pack are contiguous, so a fresh-uploaded file collapses to ~N_packs large
   reads. Heuristic per pack: if the file uses ≥ 50% of the pack, GET the
   whole pack (a clean 200 — cache/ATS friendly) and slice locally; otherwise
   ranged GETs per contiguous run.
3. **Verify:** per-chunk sha256 after slicing (digests+sizes come from the
   pointer) and the unconditional whole-file `sha256(concat)` — both v1
   invariants preserved.

**ATS/206 note:** v1 deliberately made every transfer a full `200`. v2's
partial-pack reads reintroduce Range requests on the download path. Mitigants:
the ≥50% heuristic makes full-pack 200s the common case (all fresh uploads);
the warmer warms full packs, and ATS serves ranges *out of a cached full
object* (`cache.range.lookup=1`) — the historical bug is about caching 206
*responses*, not serving ranges from cached 200s. Verify on staging e2e before
enabling v2 writes (Phase gate below).

### Data-structure & error plan (Rust extension work)

- `pack_and_upload_native(path, ranges: Vec<(offset, len)>, url, token)` —
  streaming scatter-gather: reads each (possibly non-contiguous) chunk range
  in order, feeding one PUT body; O(1) memory (no 64 MiB buffering), sha256
  computed in-stream. Ownership mirrors `upload_blob_range_async` (re-open +
  re-seek per retry attempt so the body is fresh). Errors: reuse
  `CoreError` — `Integrity` for digest mismatch (permanent), existing
  retryable classification for transport.
- `download_packs_native` — extends `chunk_fetcher::ChunkAssembler` plans
  with `(url, http_range, [(chunk_digest, chunk_size, file_offset)])` entries:
  one fetched span verifies and scatters multiple chunks to their file
  offsets. Same semaphore bound, same abort-all-on-first-error, same
  `Integrity` classification.
- Pure packing/coalescing planners (chunk list → pack assignments; pointer →
  read plan) live as pure functions with `proptest!` invariants: pack
  round-trip (`unpack(pack(chunks)) == chunks`), plan covers every byte
  exactly once, coalescing preserves order, repack threshold monotonicity.

### Compatibility & rollout

- **Old readers:** pre-v1 clients (≤ v0.5.1) have no guard — but they also
  never see v2 artifacts unless someone writes them; the same opt-in
  discipline as v1 applies. v1 readers (guard-bearing) refuse v2 loudly.
- **v2 reader reads everything:** plain, chunked-v1 (positional path kept),
  chunked-v2.
- **Write gate:** `HIPPIUS_CHUNKED_LAYOUT=v1|v2` (default `v1`) alongside the
  existing `HIPPIUS_CHUNKED_WRITE` gate. Flip the default to v2 one release
  after the v2 reader is the deployed floor — the identical playbook that v1
  is executing now.
- **No migration:** existing v1 artifacts stay readable forever; repack-on-
  next-upload naturally converts hot files to v2 (their v1 chunks read as
  packs-of-one in the dedup index, so conversion re-uses transferred bytes
  where pack composition allows).

### Expected effect (at measured latencies)

| | v1 (4 MiB chunks) | v2 (64 MiB packs) |
|---|---|---|
| 512 MiB fresh upload — blob round-trips | ~100 × 3 | **~8 × 3** |
| latency overhead (HEAD .6s + POST 1.5s serialized 8-wide) | ~3.5 min | **~15 s** |
| 68 GB fresh upload — blobs | ~17,000 | **~1,090** |
| manifest budget @ ~110 B/layer ≤ 2 MiB | ~75 GB/revision | **~1.2 TB/revision** |
| re-upload after 4 MiB edit | ~3 chunk blobs | 1 small pack blob + pointer |
| dedup granularity | 4 MiB | **4 MiB (unchanged)** |

Bytes-on-the-wire throughput is governed by the Harbor/JuiceFS infra work —
packs do not fix that and are not claimed to.

## Sequencing (hard gate)

1. **Land the infra change first** (JuiceFS writeback + registry resources —
   separate plan/PR in the infra repo). Re-run the staging benchmark.
2. **Re-measure the round-trip share.** Post-writeback, if latency overhead is
   again the dominant term of a large fresh upload (expected), v2 is justified
   by data; if bytes still dominate, revisit.
3. Build v2 behind the layout gate; staging e2e + benchmark (including the
   ATS/Range verification); then the standard reader-first rollout.

## Open decisions

1. **REPACK_THRESHOLD** — proposed 25% live-fraction; pick with a
   fragmentation simulation (proptest over edit sequences) during review.
2. **Full-pack-vs-range download heuristic** — proposed ≥ 50% usage → full
   GET; tune against ATS behavior on staging.
3. **Pack target size** — 64 MiB (HF's block size; ~16 chunks). Bigger packs
   → fewer round-trips but coarser reuse-listing and bigger overshoot.
4. **Global dedup (cross-repo HEAD of candidate packs)** — deferred; local
   index covers the dominant workload without a CAS.
