# Chunked Artifact Layout — Design & Implementation Plan

> **For Claude:** Design doc + phased implementation plan. Review the design
> (§Design) before writing code. Supersedes the receiver approach — see
> §Supersedes. Revised 2026-07-09 after a 3-lens adversarial review (OCI/Harbor
> validity, client-code breakage, industry-standard alignment) — see §Review —
> and again 2026-07-09 to adopt content-defined chunking (FastCDC) in v1 for
> cross-version dedup (§Chunking strategy).

**Goal:** Parallelize large-file upload **and** download, **and dedup
cross-version re-uploads**, by representing a large file as K content-addressed
**content-defined** chunk blobs under one OCI artifact, uploaded/pulled
concurrently as ordinary whole blobs and `HEAD`-skipped when already present —
eliminating the in-cluster staged receiver and its entire
data-plane/scaling/ops surface.

**Architecture (Git-LFS/Xet "pointer" shape — "Option B"):** A file ≥ threshold
is split into K **content-defined** chunk blobs (FastCDC, ~64 MiB average —
HF-Xet's block/transfer size), each a normal digest-verified OCI blob pushed
directly to Harbor in parallel
(`docker push` semantics). Content-defined boundaries re-sync just past an
insert/delete, so a re-uploaded, slightly-changed file dedups every unchanged
chunk (`HEAD`-skip) instead of re-sending the shifted tail. The manifest
holds **one titled "pointer" layer per file** (carrying the whole-file
size+digest and the ordered chunk list) plus the K chunks as **untitled** layers.
Download reads the pointer, pulls the K chunks in parallel, concatenates in
order, and verifies the whole-file digest. Small files stay one plain titled blob
(K=1), byte-identical to today. The artifact is typed as an **OCI 1.1 artifact**
(not an image) so Trivy skips it and `docker pull` refuses it.

**Tech stack:** hippius-hub Python client + its Rust extension (`hippius_core`).
No server component. Harbor + JuiceFS unchanged.

---

## Why this is the right design (evidence)

Traced 2026-07-09 across the `thenervelab` org:
- **The `one titled layer == one file` contract is consumed in exactly one
  place — this client** (`_repo_ops.py:214` siblings; `_oci.py`
  `iter_titled_layers`; `file_download.py` resolve-by-title). Org-wide code
  search: `rfilename` → only hippius-hub. So the layout is ours to change.
- `hippius-console` renders **artifact-level** `digest`/`size` only (Harbor
  facade `RegistryArtifact`); `api.hippius.com/api/registry` confirmed live as a
  Harbor artifact-level API (`/repositories/` → 401 user-scoped, no per-file
  shape). `hippius-indexer`/`-api` index **Arion/IPFS** manifests (already
  chunk-aware). `hippius-frontend` is the marketing site.
- **Battle-tested precedent:** OCI chunked *upload* is strictly sequential and
  broken across registries — real clients push each blob whole and parallelize
  across **independent blobs** (docker's `max-concurrent-uploads`, default 5).
  Object stores use a part-number model with direct-to-storage. **Git-LFS and
  HF-Xet both represent a file as a pointer to content-addressed chunks** — this
  design is that shape (a pointer layer), adapted to OCI.

Net: chunking is a **self-contained client change**, not a multi-service
migration.

## Supersedes

- The staged receiver (`receiver/`, `Dockerfile.receiver`, `deploy/receiver/`)
  and the client multipart path (`src/multipart_uploader.rs`,
  `_should_use_multipart`, `HIPPIUS_RECEIVER_URL`, the receiver diagnostics).
- Plans `2026-07-09-parallel-blob-upload.md` and
  `2026-07-09-receiver-staging-rollout.md`.
- **PR #33** — close as superseded once Phase 3 lands (keep as a fallback only
  if a hard single-blob consumer ever surfaces).

---

## Design

### Chunking strategy — content-defined (FastCDC) for v1

Split with **FastCDC** (rolling Gear-hash), **~64 MiB average** (min 16 MiB, max
256 MiB — standard normalized-chunking ratios) — matching **HF-Xet's block/xorb
transfer size** (see the two-size note below). Threshold to chunk: the current
256 MiB threshold; below it, one plain blob (K=1). Per-chunk `HEAD` before PUT
skips already-present chunks — this *is* the "upload only missing bytes" dedup,
and it gives **upload resumability for free** (a re-run skips uploaded chunks,
like S3's skip-uploaded-parts).

**Which HF number this is (and which we can't use).** HF-Xet is two-tier: it
dedups at a ~**64 KiB** CDC chunk *inside a custom CAS*, then aggregates those
into ~**64 MiB** block/xorb units that are what actually transfer and store. We
are single-tier — our OCI blob is *both* the dedup unit and the transfer unit — so
the number that maps to our blob is HF's **64 MiB transfer unit**, not its 64 KiB
dedup unit. The 64 KiB unit is unusable as an OCI blob (see §Scope boundary) and
is exactly the deferred CAS project. Consequence: our dedup granularity is 64 MiB
(coarse) — a changed region re-sends up to one 64 MiB chunk. If dedup *quality*
ever outweighs Harbor lightness, drop the average to 8–16 MiB (the only knob);
CDC at any of these beats fixed-size, which re-sends the whole shifted tail.

**Why CDC in v1, not deferred.** The dominant real workload is re-uploading a
slightly-changed model. Content-defined boundaries re-sync just past an
insert/delete, so unchanged regions keep identical chunk digests and `HEAD`-skip;
only the genuinely-changed chunks transfer. Fixed-size boundaries would shift on
any length-changing edit (add/remove a layer, quantization change, header/repack)
and re-send the whole tail. Dedup itself (`HEAD`-before-PUT) is the same either
way — CDC is what makes it *survive* length-shifting edits. (In-place overwrites
that don't change file length already dedup even under fixed-size; CDC covers the
length-changing cases fixed-size loses.)

**Scope boundary — what we are NOT building.** HF-Xet's ~64 KiB chunks + custom
CAS + xorb/block aggregation. At 64 KiB a 20 GB file is ~300k chunks — untenable
as OCI blobs (blows the 4 MiB manifest cap, floods `HEAD`, buries Harbor GC and
`project_blob` quota). Fine-grained dedup requires an indirection layer OCI does
not provide (a CAS) — that indirection is HF's entire engineering cost. We stay
at **OCI-native granularity**: each CDC chunk is one ordinary blob, dedup is OCI
`HEAD`-before-PUT, no new service. At ~64 MiB average a 20 GB file is ~320 blobs —
light on Harbor GC/quota and the manifest budget. Coarser than Xet's 64 KiB dedup
unit, so a scattered edit re-sends its whole ~64 MiB chunk — acceptable because
model fine-tunes change whole tensors (MB–GB), and CDC still deduplicates every
*unchanged* 64 MiB region after a length shift, which fixed-size cannot.

**Determinism is a dedup correctness invariant.** The FastCDC parameters
(average/min/max size, Gear mask, seed) are **pinned constants and part of the
wire format**: identical content + identical params ⇒ identical boundaries ⇒
identical digests ⇒ dedup. Changing any of them silently re-cuts every file and
breaks cross-version dedup, so they are versioned with the layout — a change means
`chunked-v1` → `chunked-v2`, never an in-place tweak.

**Dependency:** adds the `fastcdc` crate (Rust) — the standard, well-tested
FastCDC implementation. Justified: a rolling-hash chunker is a correctness- and
determinism-critical primitive we must not hand-roll.

### Manifest layout — pointer + untitled chunks, typed as an OCI artifact

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "artifactType": "application/vnd.hippius.chunked.v1",
  "config": {
    "mediaType": "application/vnd.oci.empty.v1+json",
    "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
    "size": 2
  },
  "annotations": { "com.hippius.layout": "chunked-v1" },
  "layers": [
    { "mediaType": "application/vnd.hippius.pointer.v1",
      "digest": "sha256:<pointer-blob>", "size": 220,
      "annotations": {
        "org.opencontainers.image.title": "model.safetensors",
        "com.hippius.file.size": "160000000",
        "com.hippius.file.digest": "sha256:<whole-file>",
        "com.hippius.chunk.count": "3" } },
    { "mediaType": "application/vnd.hippius.chunk.v1", "digest": "sha256:<chunk0>", "size": 67108864 },
    { "mediaType": "application/vnd.hippius.chunk.v1", "digest": "sha256:<chunk1>", "size": 71303168 },
    { "mediaType": "application/vnd.hippius.chunk.v1", "digest": "sha256:<chunk2>", "size": 21587968 },
    { "mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "sha256:<config.json>", "size": 1234,
      "annotations": { "org.opencontainers.image.title": "config.json" } }
  ]
}
```

- **One titled layer per file.** A chunked file = one **pointer** layer
  (`application/vnd.hippius.pointer.v1`, titled with the filename) whose small
  blob body is the ordered chunk-digest list; file-level metadata
  (`com.hippius.file.size`/`digest`/`chunk.count`) lives on the pointer, once.
  The K chunk layers are **untitled** (no `title` annotation).
- **Small files (K=1)** stay exactly as today: one plain
  `application/octet-stream` titled layer, **no** chunk/pointer mediaType or
  annotations — byte-identical, so cross-dedup with pre-chunk uploads holds. The
  uploader **must branch on the threshold before choosing layout.**
- **OCI-artifact typing** (`artifactType` + the 2-byte empty config, size **2**,
  never size 0): Harbor classifies it as a generic artifact → Trivy cleanly
  *skips* it instead of erroring on unextractable chunk layers, and `docker pull`
  refuses it (a migration guard). **Confirmed available: the deployed Harbor is
  v2.15.0** (all components; verified from the pod image tags 2026-07-09), well
  above the ≥ 2.9/2.10 floor for OCI 1.1 `artifactType` + Referrers — the config-
  mediaType fallback is not needed.
- **`com.hippius.layout: chunked-v1`** is set on the manifest **only when at
  least one file in it is chunked**, so a repo of only small files doesn't trip
  the Phase 0 guard.

**Grouping (read side):** `iter_titled_layers` already yields exactly one titled
layer per file (pointer or plain) — untitled chunk layers are skipped naturally,
so there is **no duplicate-title logic**. For a pointer layer, read
`file.size`/`file.digest` from its annotations and the chunk list from its body;
for a plain layer, use its own `size`/`digest`.

### Manifest size ceiling (hard registry limit)

CNCF Distribution hard-caps the manifest PUT body at **4 MiB**
(`maxManifestBodySize`), returning HTTP 400 on the *final* manifest PUT after all
blobs are already uploaded. Because **all files share one manifest**, the budget
is per-artifact, not per-file. Controls:
- **Cap on aggregate manifest bytes**, target ≤ 2 MiB of layer JSON (headroom for
  Harbor core re-parsing) — **not** a per-file chunk count. Option B already
  minimizes this: untitled chunk layers carry only mediaType+digest+size
  (~110 B), and file metadata sits once on the pointer.
- With ~110 B/chunk, ≤ 2 MiB ≈ ~19k chunks ≈ ~1.2 TB per **revision manifest** at
  64 MiB average. A folder upload merges every file's pointer+chunk layers into
  that one manifest, so ~1.2 TB is the *combined* budget across all files in the
  revision, not a per-file limit — a repo of many large files reaches the cap at a
  total size below it. Still covers every model today. (Dropping the average to 8–16 MiB for finer dedup
  would lower this ceiling proportionally; a real CDC trade if that knob is ever
  turned.) For artifacts past the ceiling, fan out via an
  **OCI image index / Referrers**
  (a top manifest referencing per-file sub-manifests) — the only OCI-native way
  past 4 MiB. (v1 may simply cap+error with a clear message; index fan-out is a
  documented follow-up.)

### Integrity & failure handling

- **Integrity, no weaker than today:** Harbor verifies **each chunk's** digest
  inline on push; pull verifies each chunk digest; the client verifies
  `sha256(concat) == com.hippius.file.digest` after assembly, and verifies the
  pointer blob against its own digest.
- **Atomicity:** download writes to a temp file and atomic-renames; the manifest
  makes the whole fileset visible atomically (one PUT).
- **Orphaned chunks:** a crash after pushing chunks but before the manifest PUT
  leaves unreferenced chunk blobs. OCI has no abort primitive → they linger until
  **Harbor GC** reclaims them (GC only sweeps blobs referenced by no manifest, so
  this is safe). **Confirm Harbor GC is actually scheduled** (it is stop-the-world
  and sometimes disabled). The `HEAD`-before-`PUT` path makes a retried upload
  cheap (skips already-present chunks).

### Harbor deduplication & caching interaction (verified against `harbor-warmer`)

Both mechanisms operate on **content-addressed blobs** (keyed by `sha256`, pulled
at `/v2/<repo>/blobs/<digest>`, immutable). A chunk *is* a content-addressed
blob, so the machinery is unchanged — only granularity changes.

**Deduplication (Harbor, blob-level):**
- A byte-identical file → identical chunk digests → **still fully dedups**.
- Partially-identical files dedup at *chunk* granularity **including after an
  unaligned insert/delete** — FastCDC re-syncs boundaries just past the change, so
  unchanged regions keep identical chunk digests and `HEAD`-skip. This is the core
  win for re-uploading a slightly-changed model: only the changed chunks (a few
  ~64 MiB blobs) transfer; everything else is already present.
- Transition nuance: the *same* large file stored old-way (one blob) and new-way
  (K chunks) won't cross-dedup — different digests. Transient; only large files
  change representation.

**Caching (ATS edge, per-blob-URL):** confirmed from `harbor-warmer` — the warmer
polls Harbor per artifact, fetches the manifest, and **already loops each layer
blob**, warming it into the EU/US ATS edges with a full GET.
- **The warmer needs no change** — it keys on the artifact (manifest) digest and
  discovers layers dynamically; K chunk layers warm like K image layers.
- **Chunking retires a real workaround.** ATS 9.2.3 won't store `206 Partial
  Content` for large blobs, so today a multi-GB blob needs a full-blob warm-GET
  plus `cache.range.lookup=1`. **~64 MiB chunks are each a clean `200 OK`** the
  client pulls in parallel — no Range gymnastics, no 206 bug.

**Costs bounded by chunk size (why ~64 MiB, not 64 KiB):** more blobs → more
warmer tasks + Harbor blob-metadata/GC rows + edge-cache objects; parallel
cache-miss pulls burst concurrent JuiceFS reads (same total bytes). ~64 MiB (HF's
transfer unit) keeps a 20 GB file at ~320 blobs — not the ~300k that 64 KiB
Xet-style chunks would demand, which OCI blob-per-chunk cannot carry.

### Guarantee preservation & existing-data compatibility (MUST NOT break client data)

Existing stored artifacts are **never rewritten or migrated** — they remain
byte-for-byte as they are and stay fully readable. The new layout applies only to
*new* uploads of large files. Every current dedup and caching guarantee is
preserved; two requirements make that hold.

**Existing data stays readable, unchanged:**
- Old artifacts are one plain titled blob per file. `group_files` treats a plain
  titled layer (no pointer/chunk mediaType) as K=1 → resolved and downloaded
  exactly as today. No re-push, no digest change, no manifest rewrite.
- **Requirement — keep the legacy Range downloader.** Today `src/chunked_downloader.rs`
  already parallelizes a *single whole-file blob* download via N HTTP `Range`
  requests (206 slices from the ATS-cached full body). Pre-chunk large artifacts
  depend on this path, so it **must be retained** (Phase 4 must not delete it).
  New chunked artifacts use the chunk-parallel path; legacy single-blob artifacts
  keep the Range-parallel path. Both coexist.

**Deduplication guarantees — preserved:**
- Identical file → identical CDC chunks → all `HEAD`-skip (whole-file dedup, now
  at chunk granularity). Cross-repo/revision sharing and quota-by-unique-blob hold
  (Harbor keys `project_blob` by digest; shared chunks counted once).
- **Requirement — deterministic chunking and pointer blob.** Chunk boundaries are
  fixed by the pinned FastCDC params (see §Chunking strategy), and the pointer body
  must contain *only* the ordered chunk-digest+size list + whole-file size/digest —
  **no timestamps or per-upload metadata** — so two identical files produce the
  same chunks *and* the same pointer digest, deduping at both levels.
- Transient caveat: a large file stored old-way (one blob) and re-uploaded
  new-way (chunks) won't cross-dedup (different digests) until the old artifact
  ages out. Bounded, large-files-only, not a steady-state loss.

**Caching guarantees — preserved, and one fragile dependency retired:**
- Immutable content-addressed cache and `harbor-warmer` warm-on-publish are
  unchanged (the warmer loops layers → warms every chunk + the pointer).
- Parallel edge-speed download is preserved: new artifacts pull K full-`200`
  chunk blobs in parallel — which **removes** the ATS-`206`/Range coupling (and
  the `require_partial_content` fragility) that the whole-file path needs today.

**Integrity guarantee — preserved (trust boundary noted):** today Harbor attests
one blob == whole-file hash; with chunking Harbor attests each *chunk*, and the
*client* attests the assembly (`sha256(concat) == com.hippius.file.digest`).
End-to-end identical for a correct client.

**Registry-frontend metadata compatibility — no breaking change (verified in
`hippius-console`).** The console + `api.hippius.com/api/registry` read Harbor's
**artifact-level** `RegistryArtifact` (digest, size, tags, annotations, `type`,
`media_type`, `artifact_type`) — never per-file layers. `RegistryArtifactView.tsx`
renders these display-only with guards (`data.type || "-"`, optional media types,
`typeof size === "number"`); it does not branch, filter, or build any command
from them, there is no artifact-type list filter, and the push snippet is generic
static onboarding (not per-artifact). Chunking preserves artifact-level `size`
(same total bytes), `tags`, and `digest` semantics and only *adds* an optional
`artifactType` + the `com.hippius.layout` annotation. Note today's uploader
**already** emits `application/vnd.oci.empty.v1+json` config +
`application/octet-stream` layers (`file_upload.py:258,386`) — so current
artifacts are already non-image generic artifacts, and `artifactType` is an
*additive, explicit* classification, not a category flip. The only visible effect
is cosmetic label text in the artifact detail pane; an optional console tweak
could render "chunked — N files" but is not required.

### Backward compatibility & migration

- **Old artifacts** (one plain titled layer/file) stay readable — treated as K=1.
  No data migration.
- **Failure mode is now LOUD (the Option-B payoff).** A client that predates
  chunked support, reading a chunked manifest, resolves the file to its one titled
  **pointer** layer and writes the tiny pointer blob (~few hundred bytes) as the
  file — obviously wrong (size/digest mismatch), not a plausible 64 MiB prefix
  that passes `verify_hash`. Make the pointer self-identifying (a `version` field,
  LFS-style) so the wrong output is diagnosable.
- **Layered safeguards (no single honor-system guard):**
  1. **Phase 0 guard:** current client rejects a manifest with an unknown
     `com.hippius.layout`. Bounds the pre-guard window.
  2. **`artifactType`** makes `docker pull` / image tooling refuse the artifact
     outright (closes the third-party path).
  3. **Pointer loud-fail** (above) is the backstop for any reader that slips
     through.
  4. **Gate chunked writes** behind a deployed read-capable client-version floor
     (Phase 5). Note the residual: a user *downgrading* after chunked writes exist
     hits the loud-fail, not silent corruption. `oras pull` of these artifacts is
     **unsupported by design** — document it.

---

## Phase 0 — Ship the compatibility guard (before anything else)

**Files:** `hippius_hub/_oci.py`, new `tests/test_layout_guard.py`.

On every manifest fetch, if `annotations["com.hippius.layout"]` is present and not
in the known set (empty now → any value errors; `{"chunked-v1"}` once Phase 1
lands), raise `UnsupportedLayoutError("upgrade hippius-hub to read this
artifact")`. Release as its own version so the floor exists before any writes.

**Test:** unknown `com.hippius.layout` raises; absent annotation unaffected.

## Phase 1 — Manifest model + pointer grouping (pure, read side)

**Files:** `hippius_hub/_oci.py` (add `group_files(manifest) -> list[FileGroup]`
`{title, size, digest, chunks: list[str]}`, reading the pointer blob for chunked
files); route `_repo_ops.py:214` siblings, `layer_titles`, and **every**
`layer_titles`/`iter_titled_layers` caller (`list_repo_files` `:271`,
`file_exists` `:350`, snapshot enumeration) through it; `hypothesis` property test.

**Property:** `group_files` returns exactly one entry per logical file with
correct whole-file size/digest and ordered chunk digests; round-trips with the
Phase 3 uploader. Fixtures: K=1 plain, K=3 pointer, mixed, **0-byte file**
(K=1 empty-sha256 blob), size exactly divisible by 64 MiB (no empty trailing
chunk), and a malformed pointer (missing/duplicate index) → error.

## Phase 2 — Chunked download (parallel pull + concat + verify)

**Files:** `hippius_hub/file_download.py` (resolve a file → its `FileGroup`; if
chunked, drive the parallel pull); Rust `src/lib.rs` + `src/`
(`download_chunks_native(urls, chunk_digests, dest, file_digest)` — pull K blobs
concurrently, write in order to a temp file, verify each chunk digest and the
whole-file digest, atomic-rename). The path-traversal guard (`_safe_join`) is
unchanged (operates on the title string). `snapshot_download`/`list_repo_files`
enumerate via `group_files` (one entry per file) — **fixes the K-duplicate
double-count**.

**Legacy path retained (existing client data):** route by `FileGroup` kind —
chunked → new chunk-parallel path; plain → the **existing
`src/chunked_downloader.rs` Range-parallel download, unchanged**. Every artifact
uploaded before this change (one whole-file blob per file) downloads exactly as
today; only new chunked artifacts take the new path.

**Test:** httpx-mock a chunked file; assert parallel GETs, correct concat,
whole-file digest verification, corrupted-chunk fails loudly; and a plain
single-blob artifact still downloads via the Range path (existing behavior).

## Phase 3 — Chunked upload (split + parallel blob PUT + pointer manifest)

**Files:** `hippius_hub/file_upload.py`; Rust `src/lib.rs` + a new
`upload_chunks_native` (split ≥ threshold file with **FastCDC** at the pinned
params, sha256 each chunk, `HEAD`-dedup, PUT missing chunks concurrently straight
to Harbor, return chunk descriptors + whole-file digest). Build the pointer blob +
manifest layers. The chunk splitter is the **only** part CDC changes — Phases 0–2
(guard, pointer grouping, download) are boundary-agnostic and stand as written.

- **Layout branch:** file `< threshold` → one plain titled octet-stream layer
  (unchanged). `≥ threshold` → pointer + untitled chunks + `artifactType` + empty
  config; set `com.hippius.layout` on the manifest only if ≥ 1 chunked file.
- **CRITICAL — group-aware manifest merge.** Rewrite `_merge_layers`
  (`file_upload.py:267`): it is currently title-keyed and **collapses a chunked
  file's layers to one**, and worse, committing an *unrelated* file into a repo
  that holds a chunked file rewrites that file down to a single layer (silent data
  loss). New merge: partition existing layers into file-groups (by titled layer +
  its trailing untitled chunk layers), drop only groups whose title is being
  replaced/deleted, **keep every layer of every surviving group**, append all new
  layers. Regression test: upload `small.txt` into a repo holding chunked
  `big.bin` → `big.bin`'s chunks all survive intact.

**Test:** real-fixture round-trip through the public upload path (lowered
threshold): upload → manifest has pointer + K chunk layers → download → bytes and
whole-file digest match. `HEAD`-dedup skips present chunks on re-upload. `proptest`
on the FastCDC splitter: (a) **partition** — `concat(chunks) == input`, no gap or
overlap; (b) **determinism** — same bytes ⇒ same boundaries; (c) **size bounds** —
every chunk in `[min, max]` except a possible short final chunk; (d) **shift
locality** (the CDC payoff) — inserting one byte re-cuts only chunks near the
insert, leaving the digests of chunks before it and well after it unchanged, so
`HEAD`-dedup still skips them.

## Phase 4 — Delete the receiver and multipart path (surgical)

**Remove:** `receiver/` (whole crate), `Dockerfile.receiver`, `.dockerignore`
receiver bits, `deploy/receiver/`, `src/multipart_uploader.rs`,
`_should_use_multipart`, `resolve_receiver_url` + `_LOOPBACK_HOSTS`,
`HIPPIUS_RECEIVER_URL`, `upload_blob_multipart_native` (Rust `#[pyfunction]` +
registration) **and its import at `file_upload.py:39` (same commit, or the module
fails to import)**. Drop the `receiver` workspace member from `Cargo.toml`.

**Keep / do NOT delete:**
- `src/chunked_downloader.rs` — the **Range-parallel download path for existing
  single-blob artifacts**. Deleting it breaks parallel downloads of all pre-chunk
  client data. It is the *download* parallelizer, unrelated to the *upload*
  receiver/multipart being removed.
- `src/diagnostics.rs` **`probe_blob` / `diagnose_blob_native` / the `diagnose`
  CLI** — the download diagnostic, unrelated to the receiver. Remove only
  `upload_probe` / `diagnose_upload_native` / the `diagnose-upload` CLI.
- `DEFAULT_MULTIPART_PART_SIZE` / `DEFAULT_MULTIPART_THRESHOLD` — **rename** to
  chunk config (`DEFAULT_CHUNK_SIZE` / `DEFAULT_CHUNK_THRESHOLD`), reused by
  Phase 3. Do not delete.

**Remove atomically** the now-dead tests (`test_multipart_routing.py`,
`test_multipart_config.py` receiver bits, `test_diagnose_upload.py` if the CLI
goes). Update `Cargo.toml`, `README`, close PR #33, run the full clippy/test/deny
gate.

## Phase 5 — Rollout

1. The forward-compat guard (`_oci._guard_layout` + `KNOWN_LAYOUTS`) ships together
   with read + write in this PR — there is no earlier release carrying it as an
   empty floor, so no already-deployed reader (≤ v0.5.1) refuses a chunked
   artifact. This backward gap is the reason writes are opt-in below.
2. Release read + write as the new client, chunked writes **off** by default.
3. **Enable chunked writes** only once the read-capable (guard-bearing) client is
   the deployed floor. The gate is `HIPPIUS_CHUNKED_WRITE`
   (`resolve_chunked_write_enabled`, **default off / opt-in this release**): an old
   reader lacking the guard does NOT fail loudly — it matches the pointer layer by
   its title and silently writes the ~200-byte pointer blob as the file — so the
   default stays off until the guard-bearing reader is universal. A producer sets
   `HIPPIUS_CHUNKED_WRITE=1` to opt in (e.g. staging e2e); a later release flips the
   default on once the fleet is upgraded.
4. Measure large-file upload wall-clock (K-way parallel) and download wall-clock
   (new parallelism) vs the pre-chunk baseline. No Harbor/JuiceFS change; scaling
   is Harbor's existing horizontal core replicas.

---

## Implementation status (landed on `feat/chunked-artifact-layout`)

All phases implemented and tested. Deviations from the plan as written, all
deliberate:

- **`group_files` is pure over the manifest** — chunk digests/sizes come from the
  untitled chunk layers (positional, OCI-order-preserved), not by fetching the
  pointer blob. The pointer blob still exists (deterministic, self-identifying)
  for pointer-level dedup and old-client loud-fail, but readers don't fetch it, so
  the read side stays a single round-trip.
- **CDC chunk size is 64 MiB** (HF-Xet's transfer/block unit), not the earlier
  8 MiB — matches the resolved Open Decision #4.
- **Config constants were added, not renamed.** `DEFAULT_CHUNK_THRESHOLD` /
  `DEFAULT_CDC_AVG_SIZE` are new (the download `DEFAULT_CHUNK_SIZE` is a distinct
  Range-size knob); the old `DEFAULT_MULTIPART_*` were deleted with the receiver.
- **Manifest-size ceiling**: the v1 cap+error IS implemented — `_assemble_manifest`
  measures the serialized manifest and raises `ManifestTooLargeError` before the
  PUT once it exceeds the 4 MiB registry cap (`MAX_MANIFEST_BYTES`), so an artifact
  with too many chunks fails with a clear message instead of the registry's opaque
  400 after all blobs are uploaded. Only the *Referrers/index fan-out* (Open
  Decision #2's expensive half) is deferred; the ~1.2 TB budget (per revision
  manifest, all files in the revision combined) at 64 MiB covers every model today.
- New Rust: `src/chunk_fetcher.rs` (parallel chunk pull), `chunk_and_hash` +
  `upload_blob_range_async` in `uploader.rs`, `CoreError::Integrity`. New Python
  errors: `UnsupportedLayoutError`, `MalformedManifestError`, `ManifestTooLargeError`.

**Review-fix hardening (PR #34 code review, folded into the same commit):**
- **Bounded download concurrency.** `download_chunks_native` caps in-flight chunk
  fetches with a `tokio::Semaphore(max_concurrent)`. Without it, `pool_max_idle_per_host`
  bounds only the *idle* pool, so a many-thousand-chunk file would open one socket
  per chunk and exhaust FDs / ephemeral ports / trip Harbor 429s. "K parallel pulls"
  is therefore *K bounded by `max_concurrent`*.
- **Whole-file verify is unconditional on chunked assembly** (decoupled from the
  opt-in `HIPPIUS_VERIFY_HASH`): per-chunk digests prove each chunk's bytes but not
  its *position*, so the `sha256(concat)` pass is the only check on correct
  ordering and always runs. This matches §Integrity's stated guarantee.
- **`chunk.count >= 1` guard.** `group_files` rejects a `count == 0` pointer
  (`MalformedManifestError`) — it would otherwise collapse to a plain file whose
  whole-file blob was never uploaded.
- **Chunked writes default OFF (opt-in this release).** The forward-compat guard
  ships in this same release, so no deployed reader (≤ v0.5.1) carries it: an
  un-upgraded reader would silently write the pointer blob as the file, not fail
  loudly. `resolve_chunked_write_enabled` defaults off; a producer sets
  `HIPPIUS_CHUNKED_WRITE=1` to opt in. A later release flips the default on once the
  guard-bearing reader is universally deployed.
- **`group_files` skips foreign untitled layers.** An untitled layer of a non-chunk
  media type (a co-located `docker`/`oras` push) is skipped — a foreign manifest
  degrades to its titled subset instead of hard-failing every read API. A stray
  untitled *chunk* layer still raises: that is our-layout corruption, not third-party
  content.

Open Decision #5 (**Harbor GC scheduled?**) remains an ops confirmation, not a
code item — orphaned chunks from a failed upload are only reclaimed if GC runs.

## Review (2026-07-09, 3-lens adversarial) — findings incorporated

- **Option A → Option B (pointer).** All three lenses flagged that K layers
  sharing a title caused silent corruption (title→file collision in `oras`/`crane`
  and in our own `_merge_layers`/`layer_titles`) and a *silent* migration failure.
  Option B (titled pointer + untitled chunks) fixes all of it and makes migration
  loud. Untitled chunk layers are GC-safe (all manifest `layers` are marked).
- **OCI-artifact typing** (`artifactType` + 2-byte empty config): fixes a Trivy
  scan-error regression on custom layer mediaTypes and refuses `docker pull`.
- **4 MiB manifest cap:** the earlier `MAX_CHUNKS=100_000` was ~10× over; now a
  per-artifact byte budget + Referrers fan-out for the tail.
- **Group-aware merge:** the `_merge_layers` data-loss bug is now a first-class
  Phase 3 task with a regression test.
- **Surgical Phase 4:** keep `probe_blob`/`diagnose`; rename (not delete) the size
  constants; remove the Rust import + dead tests atomically.
- **Corrected claims:** the "matches Xet's block size" line was wrong (Xet dedups
  at ~64 KiB). Superseded by the CDC revision below.
- **Revision 2026-07-09 (CDC in v1):** dropped fixed-size for FastCDC ~64 MiB
  average (HF's block/transfer unit) so cross-version dedup survives
  length-shifting edits. Fixed-size only deduped
  byte-aligned/in-place edits; the common re-upload (add/remove a layer, repack,
  quantization change) shifts every later boundary and defeated it. Coarse CDC
  fixes this on the existing Harbor stack (blob-per-chunk, `HEAD`-before-PUT);
  fine-grained Xet-style dedup (custom CAS) stays out of scope.

## Open design decisions to confirm

1. ~~**Harbor version** — confirm ≥ 2.9/2.10 for `artifactType`.~~ **RESOLVED
   2026-07-09: Harbor v2.15.0** (pod image tags, all components). OCI 1.1
   `artifactType` + Referrers fully supported; no fallback needed.
2. ~~**Manifest-size ceiling behavior**~~ **RESOLVED: v1 cap+error implemented**
   (`_assemble_manifest` → `ManifestTooLargeError` above `MAX_MANIFEST_BYTES`).
   The Referrers/index fan-out for genuinely huge artifacts remains the deferred
   follow-up.
3. **Client-version floor mechanism** — how the read-capable floor is asserted
   before enabling writes (a Harbor webhook on `com.hippius.layout` is the only
   true server-side enforcement; otherwise the pointer loud-fail + `artifactType`
   are the safety net).
4. ~~**CDC later?**~~ **RESOLVED 2026-07-09: coarse CDC (FastCDC ~64 MiB average,
   HF's block/transfer size) is in v1.** Cross-version dedup of re-uploaded,
   slightly-changed models is the
   dominant workload and CDC is a localized change (splitter only). The remaining
   deferral is **fine-grained Xet-style dedup** (~64 KiB chunks + a custom CAS) —
   that needs an indirection layer OCI cannot carry as blob-per-chunk, so it is a
   separate future project, not a parameter tweak.
5. **Harbor GC scheduled?** — confirm GC runs so orphaned chunks from failed
   uploads are reclaimed.
