# Parallel Blob Upload Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Parallelize the single large-blob upload (the irreducible case) by fanning out N concurrent part streams over the WAN into an in-cluster staged receiver that re-emits one native OCI blob PUT to Harbor — keeping `docker pull`, Trivy scan, quota, GC, and the model-index pipeline byte-for-byte intact.

**Architecture:** "Parallelize the WAN, serialize the LAN." The client splits a blob into parts and PUTs them concurrently to a new stateless-per-upload receiver service. The receiver stages parts on local NVMe, and on completion streams one in-order monolithic `PUT …?digest=sha256:…` into `harbor-registry` over the fast LAN. Harbor hashes/verifies/registers natively — zero storage-layout coupling. This is the **staged** variant (buffer-then-forward), matching the battle-tested pattern of S3 multipart upload and Hugging Face's CAS-in-the-middle topology; the streaming "frontier-feeder" overlap from the design doc is deliberately deferred until measurement proves the LAN tail matters.

**Tech Stack:** Rust `hippius_core` (pyo3 0.22, tokio, reqwest, existing 32-way parallel machinery) · Python `hippius_hub` (routing, threshold) · new Rust receiver service (axum/tokio) · Harbor/distribution filesystem driver over JuiceFS · k8s.

**Prior-art basis:** `Downloads/2026-07-07-hub-blob-receiver-design.md` (Option A) + review comparison to HF Xet (`blog/rearchitecting-uploads-and-downloads`, `blog/from-chunks-to-blocks`). Both confirm a server-side broker between client and store is the standard shape; S3 MPU + HF xorbs confirm "store parts, assemble at completion" over stream-reassembly.

---

## Ground truth (verified against code this session)

- Downloads already fan out **32-way** ranged GETs (`src/chunked_downloader.rs:18` `MAX_CONCURRENT_DOWNLOADS = 32`, 100 MB chunks). This is the machinery to reverse.
- Uploads are **single-stream**: `upload_blob_async` → one `FramedRead` → `Body::wrap_stream` → one `PUT ?digest=` (`src/uploader.rs:79`, `hippius_hub/file_upload.py:75`).
- pyo3 surface (`src/lib.rs`): `hash_file_native`, `upload_blob_native`, `download_file_native`. We add `upload_blob_multipart_native`.
- Registry host is `registry.hippius.com` (Harbor); `api.hippius.com` is control-plane only (`hippius_hub/constants.py:10-15`).
- Retry/backoff classifier `CoreError::is_retryable` is shared between up/down paths (`src/error.rs`) — reuse it.
- Cluster reality (design doc, verified `c-m-ff5bhnvp`): Harbor is `storage: filesystem` over JuiceFS; commit = `rename()` (~0.1s, flat with size); bottleneck is the client uplink.

---

## Deployment gate (does NOT block coding)

Before **rolling out**, the team runs the Phase 0 harness in-cluster to obtain the one make-or-break number:

> **max sustained single-stream ingest into `harbor-registry` (inline SHA-256 + JuiceFS write)** vs **N-way WAN aggregate a fast client can deliver.**

If Harbor's single-upload ingest < the WAN aggregate, the receiver's one LAN stream is the new ceiling and the win shrinks — revisit before rollout. Also measure receiver NVMe/NIC I/O **under concurrency** (HF's postmortem: concurrent multi-GB uploads hit network + disk I/O limits, not CPU). All code below is written and unit/integration-tested against mocks regardless of this number.

---

## Phase 1 — Receiver HTTP contract (the seam both sides code against)

No code; this is the shared interface. Client (Phase 2) and receiver (Phase 3) both implement it. Kept minimal and OCI-adjacent.

```
POST   /v2/{repo}/blobs/uploads/multipart
       Authorization: Bearer <oci-token>
       body: {"digest":"sha256:…","size":<u64>,"part_size":<u64>}
       → 201 {"upload_id":"…","part_size":<u64>,"expires_at":"…"}
       (receiver validates auth against Harbor; MAY clamp part_size)
       NOTE: num_parts is NOT returned — the client derives it as
       ceil(size / part_size) from the authoritative part_size, so there is
       one source of truth for the part count and no field to disagree on.

PUT    /v2/{repo}/blobs/uploads/multipart/{upload_id}/parts/{part_number}
       Authorization: Bearer <oci-token>
       Content-Range: bytes <start>-<end>/<size>
       body: <raw part bytes>
       → 204   (idempotent: re-PUT of an already-received part is a no-op 204)

POST   /v2/{repo}/blobs/uploads/multipart/{upload_id}/complete
       Authorization: Bearer <oci-token>
       → 201 Location: /v2/{repo}/blobs/sha256:…   (receiver has streamed the
         monolithic PUT to Harbor; Harbor verified the digest)
       → 409 if parts missing; body lists missing part numbers (client re-PUTs)

DELETE /v2/{repo}/blobs/uploads/multipart/{upload_id}    (abort; GC scratch)
```

Design invariants:
- **Idempotent parts** — a part PUT is keyed by `(upload_id, part_number)`; re-sending is safe. This is what lets the client retry a failed part and survive a receiver restart (re-PUT surviving parts to a fresh upload).
- **Receiver never trusts the client digest** — it forwards to Harbor, which hashes inline and rejects on mismatch. The digest in `initiate` is only used to build the Harbor `?digest=` finalize URL and dedup HEAD.
- **Part size is receiver-chosen** (no S3 5 MB floor — parts hit the receiver, not S3). Default 64 MB; overridable.

---

## Phase 2 — Client multipart path (in `hippius-hub`, this repo)

### Task 2.1: Part-plan math (pure functions, TDD)

**Files:**
- Create: `src/multipart_uploader.rs`
- Wire: `src/lib.rs` (add `mod multipart_uploader;`)

**Step 1: Write failing tests** — mirror the existing `chunk_bounds`/`num_chunks` suite in `chunked_downloader.rs`, reusing the same coverage/contiguity invariants. Part math is the same math as chunk math, so:

```rust
// in src/multipart_uploader.rs #[cfg(test)] mod tests
#[test] fn num_parts_matches_num_chunks_semantics() {
    assert_eq!(num_parts(0, 64), 0);
    assert_eq!(num_parts(1, 64), 1);
    assert_eq!(num_parts(64, 64), 1);
    assert_eq!(num_parts(65, 64), 2);
}
#[test] fn part_bounds_last_truncates_at_eof() {
    assert_eq!(part_bounds(1024, 1000, 1), (1000, 1023));
}
```
Plus a `proptest!` block asserting coverage (Σ part sizes = total), contiguity (no gaps/overlaps), and full span (first starts 0, last ends size-1) — identical properties to the downloader's proptest, because the two must agree byte-for-byte for reassembly to be correct.

**Step 2:** `cargo test -p hippius_core multipart_uploader::tests` → FAIL (undefined).

**Step 3: Implement** `num_parts` and `part_bounds`. These are byte-for-byte the same as `num_chunks`/`chunk_bounds`. **DRY decision:** extract the shared math into a small `pub(crate) mod part_math` used by both modules rather than copy-paste — a divergence between up-plan and down-verify math would silently corrupt reassembly. (Check `mcp__illu__references` on `chunk_bounds`/`num_chunks` before extracting to catch all call sites + tests.)

**Step 4:** tests pass. **Step 5:** commit `feat(rust): part-plan math shared with chunk math`.

### Task 2.2: Concurrent part-PUT engine

**Files:** `src/multipart_uploader.rs`; reuse `CoreError`, `upload_client()` pattern from `uploader.rs`.

Mirror `ChunkedDownloader::download`'s structure in reverse:
- `initiate` → POST multipart, parse `upload_id` + `part_size`.
- Fan out parts via `FuturesUnordered` + eager `tokio::spawn` + `AbortHandle` collection (copy the exact cancellation-safety pattern from `chunked_downloader.rs:181-242` — dropping the stream must abort survivors).
- Each part task: open own file handle, `seek(start)`, stream `end-start+1` bytes as a `PUT` with `Content-Range`; reuse `download_chunk_with_retry`'s exponential-backoff + `is_retryable` loop.
- On all-parts-ok → POST `complete`; on 409 → re-PUT the listed missing parts, then re-complete (bounded retries).
- Progress bar identical style to `uploader.rs`.

Tests: unit-test the orchestrator against a **mock receiver** (a tiny `tokio` test server or `wiremock`-style httptest) asserting: N parts arrive with correct `Content-Range`; a transient 503 on one part triggers exactly one retry; a 409-with-missing on complete triggers re-PUT of only the missing parts. Add `httptest` or `wiremock` to `[dev-dependencies]`.

Commit per green test.

### Task 2.3: pyo3 export

**Files:** `src/lib.rs`, `src/multipart_uploader.rs`

Add `#[pyfunction] upload_blob_multipart_native(base_url, repo, digest, size, path, part_size, auth_token, concurrency)` on the shared runtime (mirror how `upload_blob_native` is wired). Return `PyResult<()>`; map `CoreError` via existing `core_err_to_py`. Add a `runtime_tests`-style pin.

### Task 2.4: Python routing + threshold

**Files:** `hippius_hub/file_upload.py`, `hippius_hub/constants.py`

- Add `resolve_multipart_threshold()` (env `HIPPIUS_MULTIPART_THRESHOLD`, default from Phase 0 measurement; start 256 MB) and `resolve_receiver_url()` (env `HIPPIUS_RECEIVER_URL`; when unset, multipart is **off** and behavior is byte-for-byte today's single PUT).
- In `_ensure_blob_uploaded`: after the existing dedup HEAD, if `file_size >= threshold and receiver_url` → call `upload_blob_multipart_native` against the receiver; else the current `upload_blob_native`. The manifest PUT path is unchanged.
- Tests: `tests/` — assert routing picks multipart only above threshold and only when receiver configured; assert a sub-threshold file still takes the legacy path (no behavior change when the feature is off).

Commit.

---

## Phase 3 — Staged receiver service (in-repo Cargo workspace member)

> **Placement (decided):** the receiver is part of the hub and lives **in this repo** as a Cargo **workspace member** — `receiver/` (crate `hub-blob-receiver`, a `[[bin]]`). Rust + axum + tokio.
>
> Structure:
> - Root `Cargo.toml` gains `[workspace] members = ["receiver"]`; the root package stays the `hippius_core` pyo3 lib, so `[tool.maturin]` and the wheel build are **unchanged** (verify with `uv build` after adding the workspace table).
> - The receiver does **not** depend on `hippius_core` (that would drag pyo3/Python linkage into a plain binary). If real code sharing emerges (e.g. `CoreError`), extract a pyo3-free `hippius-blob-core` crate that both depend on — not before.
> - Receiver gets its own strict `[lints.clippy]` block mirroring the root crate's (don't refactor the root manifest into `[workspace.lints]` — keep blast radius off the released package).
> - `Dockerfile.receiver` at repo root (or `receiver/Dockerfile`) builds `cargo build -p hub-blob-receiver --release`.
> - CI note: root becoming a workspace root means `cargo test`/`cargo clippy --all` now also cover the receiver — desirable, but expect longer CI and update the workflow if it pins a single package.

### Task 3.1: Upload session store + part sink
- In-memory `DashMap<upload_id, UploadSession>`; session holds repo, digest, size, part plan, and a bounded NVMe scratch dir (`emptyDir`). Parts written to `scratch/{upload_id}/{part_number}`.
- Global concurrency cap (`maxConcurrentUploads`) + per-session backpressure; reject new initiates with 429 when saturated (client already treats 429 as retryable).
- Bounded scratch: `maxConcurrentUploads × size` cap; refuse/GC on overflow.

### Task 3.2: Auth passthrough
- `initiate` validates the caller's bearer token against Harbor (cheap: HEAD the repo or reuse Harbor token check) before allocating scratch. Receiver holds a robot credential for the LAN leg but authorizes the client with the client's own token — never a privilege upgrade.

### Task 3.3: Complete → monolithic PUT to Harbor
- On `complete`: verify all parts present (else 409 + missing list). Open the OCI upload against Harbor (`POST /v2/{repo}/blobs/uploads/`), then stream the reassembled bytes **in part order from disk** as one `PUT {location}?digest=<digest>` — a `futures::stream` concatenating each part file. Harbor hashes inline; on 201 return its `Location`. Then delete scratch.
- Integration test: a **mock Harbor** (httptest) asserting exactly one monolithic PUT arrives with the concatenated bytes in order and the `?digest=` query; assert digest-mismatch (mock returns 400) surfaces as a receiver 400 to the client, scratch GC'd.

### Task 3.4: Resumability + lifecycle
- Idempotent part PUT (Task 3.1 keying makes re-PUT a 204 no-op).
- `DELETE` aborts + GCs. Background sweeper GCs sessions past `expires_at`.
- Graceful drain on SIGTERM (finish in-flight completes; reject new initiates) + a PodDisruptionBudget in Phase 4.

---

## Phase 4 — Wiring, e2e, deploy (team-run gate)

### Task 4.1: Phase 0 harness (build now; team runs in-cluster)
**Files:** `src/diagnostics.rs`, `hippius_hub/diagnose.py`
- Add an upload probe: single-stream PUT throughput into `harbor-registry` vs N-way-into-receiver aggregate, plus receiver scratch I/O under concurrency. Emit into the existing `DiagnosticReport` JSON. This produces the deployment-gate number.

### Task 4.2: k8s manifests + image
- **Code + `Dockerfile.receiver` live in this repo.** k8s manifests go under `deploy/receiver/` here (self-contained + reviewable); the **infra/GitOps repo references or vendors them** to apply to the cluster. Confirm with the infra owner whether manifests should be authored here and synced, or authored in the infra repo against this image — default: author here, infra consumes.
- Deployment (no PVC — network to `harbor-registry` + robot cred only), Service, PDB, HPA on concurrency, NVMe `emptyDir` sized `maxConcurrentUploads × N × partSize`, resource limits from Task 4.1 I/O numbers.
- Image published to the same registry as the other hub services (`ghcr.io/thenervelab/…`).

### Task 4.3: End-to-end test
- Real client → receiver → Harbor round-trip on a multi-GB fixture; assert `docker pull` of the pushed blob succeeds and digest matches (proves native registration held).

---

## Sequencing for execution

1. **Task 2.1** (part-plan math — pure, TDD, in-repo) ← start here
2. Task 2.2 → 2.3 → 2.4 (client path, mock receiver)
3. Task 4.1 (harness — unblock the team's measurement early)
4. Phase 3 (receiver) against the same contract
5. Phase 4.2–4.3 (deploy + e2e) — after the gate number is in

Client (Phase 2) and receiver (Phase 3) are independent once the Phase 1 contract is fixed; they can be built in parallel.

## Non-goals (explicit)

- No Xet-style sub-file/global dedup (separate later bet; marginal for novel large shards).
- No storage-driver change; Harbor stays filesystem/JuiceFS.
- No `upload_file`/`upload_folder` signature change; multipart is internal + env-gated.
- No frontier-feeder overlap in v1 (add only if Task 4.1 shows the LAN tail is material).
