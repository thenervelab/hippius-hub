# hippius_core Review Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this
> plan task-by-task. Before the first edit of any session, call
> `mcp__hippius-mem__recall` about the task at hand. Any subagent prompt must include:
> "Call `mcp__hippius-mem__recall` about the task before making changes, and
> `mcp__hippius-mem__remember` any durable decision/gotcha you discover."

**Goal:** Land the verified findings of the 2026-08 hippius_core review: hygiene fixes
(Phase A), the upload hashing/pipeline redesign that removes the measured serial
multi-pass SHA-256 bottleneck (Phase B), incremental download verification (Phase C),
and module restructuring (Phase D).

**Architecture:** Python stays the control plane; Rust stays transport + CPU. Phase B
parallelizes per-chunk SHA-256 off the CDC critical path with a scoped thread pool
(no new dependencies), then converts the chunk→pack→upload flow from
phase-then-phase to a streaming pipeline via a pull-based `ChunkStream` pyclass and
an incremental Python `PackAccumulator`. Phase C reuses the pack path's proven
incremental-hash machinery for the legacy Range download.

**Tech Stack:** Rust (pyo3 0.29, tokio, reqwest, sha2, fastcdc, thiserror), Python
3.10+ driver, maturin build, proptest + pytest.

---

## Context: verified findings this plan addresses

Every claim below was verified against the tree at commit `bca88df` (2026-08-10):

| ID | Finding | Anchor |
|----|---------|--------|
| P0 | Phase-1 upload is one serial pass doing whole-file + per-chunk SHA on one core; pack upload re-reads and re-hashes. Measured: 34% of upload wall clock, hashing 94% of it, FastCDC 6% | `src/uploader.rs:69-97`, `src/uploader.rs:902-959` |
| P0/P1 | Full second disk read on chunked-v2 upload (`chunk_and_hash` then per-pack `read_ranges`) | `src/uploader.rs:61`, `src/uploader.rs:940` |
| P1 | Range download re-reads the whole file to verify; pack path already hashes incrementally | `src/chunked_downloader.rs:296-311`, `src/chunk_fetcher.rs:328-333` |
| P2 | Per-spawn `String`/`Vec` clones on the pack fan-out; hex-string digest compares | `src/chunk_fetcher.rs:342-349,589,631` |
| Q3 | `DiagError` is Debug-only, surfaced as `format!("{e:?}")` | `src/diagnostics.rs:46-55`, `src/lib.rs:359` |
| Q3b | `JoinError` mapped to `CoreError::Io` at 6 call sites despite a dedicated `JoinFailed` variant; Content-Range mismatch mapped to `Io(InvalidData)` | `src/chunk_fetcher.rs:488,595,666`, `src/chunked_downloader.rs:397,482,639-645`, `src/uploader.rs:143,922` |
| Q4 | Emoji glyphs in progress messages (house style: none) | all three transfer modules |
| Q5 | No `rustfmt.toml`; no `unsafe_code = "forbid"` | repo root, `Cargo.toml` |
| Q8 | `force_retryable` fabricates a 503 | `src/uploader.rs:571-577` |
| Q2 | Digests are hex `String` end-to-end | `src/chunk_fetcher.rs:251`, `src/uploader.rs:34` |
| Q1 | Three 1.3–1.6k-line modules | `src/uploader.rs`, `src/chunk_fetcher.rs`, `src/chunked_downloader.rs` |

## Hard constraints (violating any of these is a regression)

1. **FastCDC boundaries are the wire contract.** Identical bytes must produce
   identical chunk boundaries and digests across versions or cross-revision dedup
   breaks. Nothing in this plan may alter `StreamCDC` parameters, iteration order of
   the emitted chunk list, or the hex digests returned to Python.
2. **Do NOT parallelize FastCDC itself.** Measured at 6% of phase-1; the rolling
   gear-hash is sequential by design (team memory `mem_01KXP6BQ...`).
3. **Fresh OCI session per retry attempt.** A retried PUT/PATCH must never reuse a
   consumed session (PR #48 regression: "upload resumed at wrong offset"). The
   session POST stays inside the retried unit (`try_upload_blob_once` /
   `try_pack_upload_once`).
4. **All three SHA-256 passes are protocol-required** (whole-file digest, per-chunk
   dedup digests, per-pack OCI blob digest). The fix is overlap, not omission.
5. **Retry classification changes must be deliberate and test-pinned.** `Io` is
   retryable, `JoinFailed` is not, `BadResponse` is (`src/error.rs:214-255`). Any
   variant remap below states its intended classification.
6. **Never call native entry points from inside a tokio task** (nested `block_on`
   deadlocks). Preserved by keeping all new FFI entry points on the existing
   `py.detach` + `shared_runtime`/plain-thread patterns.
7. **Bounded memory.** The pack gate exists because inflight × 64 MiB is real.
   Every new queue/channel in this plan states its byte bound.

## Non-goals (rejected, do not implement)

- Rewriting or parallelizing FastCDC; HTTP/2; dropping sha2 asm.
- Progress-trait injection / removing indicatif (bars already self-hide off-TTY;
  only the emoji text changes).
- `#[pyclass]` option bags (Q6) — YAGNI until another knob lands.
- Multi-crate workspace split.
- B4 zero-re-read packing (pack bytes handed straight from the chunker) — deferred
  until B3 measurement shows the overlapped re-read still matters.

## Compatibility gates (the stack contract — checked 2026-08-10)

No sibling repo imports `hippius_hub` as a Python library (only hippius-console
references it, as CLI snippets in UI copy). The external contract is therefore:

1. **Registry wire format** — pointer.v2 bytes, pack layout, manifest shape, CDC
   boundaries/digests. Guarded by: the B1 serial-vs-parallel equivalence proptest
   (boundaries + digests bit-identical), the untouched `_packing.py` serializers,
   and the staging chunked-v2 roundtrip in `e2e.yml`
   (`tests/test_live_chunked_v2_roundtrip.py`).
2. **CLI commands, flags, and `HIPPIUS_*` env vars** — no task in this plan may
   change, rename, or re-default any of them (A8 is docs-only).
3. **PyPI wheel behavior** — abi3-py38 config and wheel targets untouched;
   `production-smoke-tests.yml` (hourly, latest wheel) is the backstop after
   release.
4. **Cross-version artifacts** — NOT covered by existing CI. Before merging the
   Phase B PR, run a cross-client check locally: upload a multi-pack file to the
   staging test namespace with the working tree (`maturin develop`), download it
   with the latest PyPI wheel in a separate venv, byte-compare; then the reverse
   (upload with wheel, download with working tree). Both directions must be
   byte-identical. Record the result in the PR description.
5. **Error-message text is not parsed downstream** (verified: no consumer greps
   our messages), so A3/A5 message changes are safe — but `smoke/` and `tests/`
   may pin strings; fix expectations in the same commit as the change.

Additional standing rule for every task: `pytest -v` must pass with the SAME
test count or greater (a silently skipped/deleted test is a red flag), and the
e2e workflow must be green on the PR before merge.

## Global verification gates (run for every task unless stated otherwise)

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

For tasks touching Python or the FFI surface, additionally:

```bash
maturin develop --release   # rebuild the extension the venv imports
pytest -q
```

Branch strategy: one branch + PR per phase (`chore/core-hygiene`,
`feat/upload-hash-pipeline`, `feat/range-incremental-verify`,
`refactor/core-module-split`). Never push to main. Execute in a dedicated worktree.

---

# Phase A — Hygiene (branch `chore/core-hygiene`)

### Task A1: rustfmt.toml + mechanical format

**Files:**
- Create: `rustfmt.toml`
- Modify: whatever `cargo fmt` touches (mechanical only)

**Step 1:** Create `rustfmt.toml`:

```toml
edition = "2021"
use_small_heuristics = "Default"
```

**Step 2:** Preview the mechanical diff size: `cargo fmt --all -- --check | head -50`.

**Step 3:** Apply: `cargo fmt --all`. Re-run clippy + `cargo test` (formatting must
not change behavior; if any test fails, stop — something else is wrong).

**Step 4:** Commit config + reformat together:
`style: add rustfmt.toml and apply mechanical format`

### Task A2: forbid unsafe structurally

**Files:**
- Modify: `Cargo.toml`

**Step 1:** Add above the existing `[lints.clippy]` table:

```toml
[lints.rust]
unsafe_code = "forbid"
```

**Step 2:** `cargo clippy --all-targets --all-features -- -D warnings` — must pass
(the tree has zero `unsafe` today; this makes it structural).

**Step 3:** Commit: `chore: forbid unsafe_code at the lint level`

### Task A3: strip emoji from progress messages

**Files:**
- Modify: `src/chunked_downloader.rs:179,293,305,308`, `src/chunk_fetcher.rs:326,413`,
  `src/uploader.rs:482,500,501,508,514,854,872-874`

**Step 1:** Find every non-ASCII byte in source: `rg -n "[^\x00-\x7F]" src/`.
Expected: only the progress-message string literals listed above.

**Step 2:** Replace each message with plain ASCII, preserving the `{basename}`
interpolation:

| Old (emoji prefix) | New |
|--------------------|-----|
| download start | `"Downloading"` / `"Downloading packs"` |
| download done | `"Download complete"` / `"Packs complete"` |
| verify start/done | `"Verifying SHA256"` / `"Verified"` |
| upload start | `format!("Uploading {basename}")` |
| stalled | `format!("{basename}: stalled")` |
| failed | `format!("{basename}: failed")` |
| uploaded | `format!("{basename}: uploaded")` |

**Step 3:** Check nothing asserts on the old strings:
`rg -n "Uploading|stalled|📤|✅" src/ tests/ hippius_hub/ smoke/` — fix any test
that pinned the emoji text (there is a progress-message channel test around
`src/chunk_fetcher.rs:972`; update expectations if it matches literals).

**Step 4:** `rg -n "[^\x00-\x7F]" src/` returns nothing. Run gates. Commit:
`style: replace emoji progress messages with plain ASCII`

### Task A4: DiagError gets Display + Error; consistent Python surface

**Files:**
- Modify: `src/diagnostics.rs:46-68`, `src/lib.rs:359`

**Step 1 (failing test first):** In `src/diagnostics.rs` tests, add:

```rust
#[test]
fn diag_error_displays_cause_not_debug() {
    let e = DiagError::Url("blob URL has no host".to_string());
    assert_eq!(e.to_string(), "invalid diagnostics URL: blob URL has no host");
    let io = DiagError::Io(std::io::Error::other("boom"));
    assert!(std::error::Error::source(&io).is_some());
}
```

Run `cargo test diag_error_displays` — FAILS (no `Display`).

**Step 2:** Replace the hand-rolled enum + `From` impls with thiserror (already a
dependency), deleting the `#[expect(dead_code)]` block:

```rust
#[derive(Debug, thiserror::Error)]
pub enum DiagError {
    #[error("invalid diagnostics URL: {0}")]
    Url(String),
    #[error("diagnostics I/O error")]
    Io(#[from] std::io::Error),
    #[error("diagnostics HTTP error")]
    Reqwest(#[from] reqwest::Error),
}
```

**Step 3:** In `src/lib.rs:359` replace `format!("{e:?}")` with the same
source-chain rendering `core_err_to_py` uses (extract that chain-walking into a
small helper if it is currently inline, and call it for both error types so Python
always sees `message\ncaused by: ...`).

**Step 4:** Gates pass. Commit:
`fix: give DiagError a Display/Error impl and source-chain Python messages`

### Task A5: typed session restart instead of synthetic 503

**Files:**
- Modify: `src/error.rs`, `src/uploader.rs:571-577` (+ the three `force_retryable`
  call sites and the test at `src/uploader.rs:1498`)

**Step 1 (failing test):** In `src/error.rs` tests:

```rust
#[test]
fn session_restart_is_retryable_and_chains_cause() {
    let cause = CoreError::ServerError(416, "range not satisfiable".into());
    let e = CoreError::SessionRestart { source: Box::new(cause) };
    assert!(e.is_retryable());
    assert!(std::error::Error::source(&e).is_some());
}
```

**Step 2:** Add the variant (crate-internal matches are exhaustive, so the compiler
lists every site to update — `is_retryable`, plus any match in `lib.rs`):

```rust
/// The current OCI upload session is unrecoverable (expired 404, offset
/// desync the intra-session GET could not resolve, or a 416 after
/// resume). Retryable BY DESIGN: the recovery is a fresh session from
/// offset 0 in the outer retry loop — the fabricated-503 workaround this
/// variant replaces.
#[error("upload session unrecoverable; restarting")]
SessionRestart {
    #[source]
    source: Box<CoreError>,
},
```

Add `CoreError::SessionRestart { .. } => true` to `is_retryable`.

**Step 3:** `force_retryable` becomes:

```rust
fn force_retryable(e: CoreError) -> CoreError {
    if e.is_retryable() {
        e
    } else {
        CoreError::SessionRestart { source: Box::new(e) }
    }
}
```

Update `force_retryable_maps_416_to_a_retryable_error` to assert the new variant
and that the 416 cause is reachable via `source()`.

**Step 4:** Gates + `pytest -q` (Python matches on message text? check
`rg -n "unrecoverable|restarting" hippius_hub/ tests/` — none expected). Commit:
`fix: replace force_retryable's synthetic 503 with a typed SessionRestart variant`

### Task A6: route JoinError and Content-Range errors to honest variants

**Files:**
- Modify: `src/uploader.rs:143,922`, `src/chunk_fetcher.rs:488,595,666`,
  `src/chunked_downloader.rs:397,482,639-645`

Two deliberate semantic changes — pin each with a test:

**Step 1:** `spawn_blocking`/join failures currently become `Io(other)` (retryable).
A panicked hash closure reproduces on retry; map to
`CoreError::JoinFailed { index: None, source: join_err }` (non-retryable) at all six
sites. Test: a `spawn_blocking` closure that panics surfaces `JoinFailed`, and
`is_retryable()` is false.

**Step 2:** Content-Range validation failures (`src/chunked_downloader.rs:482,639-645`)
currently `Io(InvalidData)` (retryable). Map to `CoreError::BadResponse(...)` —
still retryable (an LB mid-rollout can emit a malformed header once), so retry
behavior is unchanged while the type stops lying. Update the existing
content-range tests to expect `BadResponse`.

**Step 3:** Gates. Commit:
`fix: stop laundering join panics and bad Content-Range through CoreError::Io`

### Task A7: Sha256Digest newtype on the pack-download verify path

**Files:**
- Create: `src/digest.rs`
- Modify: `src/lib.rs` (module decl + the `PackChunkTarget` construction at
  `src/lib.rs:461-465`), `src/chunk_fetcher.rs:251,349,631`

Scope discipline: this task converts ONLY the download verify path (the hot
per-chunk compare). The uploader's internal digests convert in Phase B where that
code is rewritten anyway. Hex stays at the FFI boundary.

**Step 1 (failing tests):** `src/digest.rs` with tests:

```rust
//! Binary SHA-256 digest newtype. Hex only at the FFI/display boundary.

use crate::error::CoreError;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Parse `"<64 hex chars>"` (no `sha256:` prefix). Typed error so a
    /// non-digest string can never masquerade as one past this point.
    pub fn from_hex(s: &str) -> Result<Self, CoreError> {
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out)
            .map_err(|e| CoreError::InvalidArgument(format!("bad sha256 hex {s:?}: {e}")))?;
        Ok(Self(out))
    }
}

impl From<[u8; 32]> for Sha256Digest {
    fn from(raw: [u8; 32]) -> Self {
        Self(raw)
    }
}

impl std::fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl std::fmt::Debug for Sha256Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sha256Digest({self})")
    }
}
```

Tests: round-trip `from_hex`/`Display`; `from_hex` rejects short/odd/non-hex input;
`Sha256Digest::from(<sha2 digest array>)` equals `from_hex(hex::encode(...))`.

**Step 2:** `PackChunkTarget.expected_sha256: Sha256Digest`. The FFI construction
in `lib.rs` parses incoming hex with `from_hex` (error → `RuntimeError` via
`core_err_to_py`, keeping the single native error surface). `verify_and_scatter` compares
`Sha256Digest::from(<[u8;32]>::from(Sha256::digest(slice))) != *expected` — no hex
allocation per chunk; hex only inside the failure `format!`.

**Step 3:** Gates + `maturin develop --release && pytest -q` (an invalid-hex digest
from Python must now fail fast with a clear message — add a Python test if none
covers it). Commit: `refactor: binary Sha256Digest newtype on the pack verify path`

### Task A8: document the concurrency-knob reality

**Files:**
- Modify: `README.md` (env-var table rows for `HIPPIUS_UPLOAD_WORKERS` /
  `HIPPIUS_MAX_INFLIGHT_PACKS`), `docs/diagnosing-speed.md`, `src/lib.rs` (module
  doc comment)

**Step 1:** State in both docs: effective single-file pack concurrency is
`min(n_packs, HIPPIUS_UPLOAD_WORKERS, HIPPIUS_MAX_INFLIGHT_PACKS)`; raising
`MAX_INFLIGHT_PACKS` alone is a no-op (the per-file pool stays at 8); pack count
caps useful workers (1 GiB / 64 MiB = 16); resident memory ≈ inflight × 64 MiB.
(Team memory `mem_01KZDQG1...`; also note the constants.py "~0.9x scaling" bench is
confounded and must not justify tuning.)

**Step 2:** Add to the `src/lib.rs` crate doc: "Native entry points call
`block_on` on the shared runtime; never invoke them from inside a tokio task —
that nests `block_on` and deadlocks."

**Step 3:** Commit: `docs: document effective upload concurrency and the block_on rule`

### Task A9: thiserror 2.x bump

**Files:**
- Modify: `Cargo.toml`, `Cargo.lock`

**Step 1:** Look up the current stable (`cargo search thiserror` or docs.rs — do
NOT assume from memory). Bump `thiserror = "2"`.

**Step 2:** `cargo build` — thiserror 2 is a near-drop-in; fix any fallout (the
known break: `#[error]` attribute parsing edge cases). Gates. Commit:
`chore: bump thiserror to 2.x`

---

# Phase B — Upload hashing pipeline (branch `feat/upload-hash-pipeline`)

This phase IS the design doc that team memory references as missing
(`docs/plans/2026-07-16-upload-hashing-pipeline-design.md` was never committed).

**Measured baseline (team memory, py-spy, ~3 GB upload):** phase-1
`chunk_and_hash_native` = 34% of wall clock, single-threaded; hashing 94% of
phase 1. Separately: 81% of upload wall clock sits in two zero-network windows
(before first byte ≈ phase 1; after last byte unattributed).

**Design.** Today (serial): `[CDC → whole.update → chunk Sha256] per chunk`, then
`plan_packs`, then packs upload (re-read + pack hash + PUT).

Target:

```
B1:  CDC + whole-file hash (producer thread)
        └─ bounded queue ─→ N hash workers (per-chunk SHA)      | phase 1 ≈ max(whole-hash, chunk-hash) + CDC
B3:  ...same pipeline, chunks stream to Python in file order
        └─ PackAccumulator fills packs incrementally
             └─ pack upload submitted the moment a pack fills   | phase 2 overlaps phase 1
```

### Task B0: phase-1 benchmark harness (baseline BEFORE any change)

**Files:**
- Create: `scripts/bench_phase1.py`

**Step 1:** Script: generate a deterministic pseudo-random file (seeded, 2 GiB,
written once to a temp dir; size overridable via argv), then time 3 runs of
`hippius_core.chunk_and_hash_native(path, resolve_cdc_avg_size())` and print
median seconds and MiB/s. No asserts — a measurement tool, isolated phase-1 CPU
(report Phase B.5: never bench only end-to-end against a live registry).

**Step 2:** `maturin develop --release && python scripts/bench_phase1.py` — record
the baseline number in the commit message.

**Step 3:** Commit: `bench: phase-1 chunk+hash throughput harness (baseline: X MiB/s)`

### Task B1: hash chunks off the CDC critical path

**Files:**
- Modify: `src/uploader.rs:69-97` (`chunk_and_hash_reader`)
- Test: same file's `#[cfg(test)]` block

**Memory bound:** queue depth 4 + ≤4 workers in flight, chunk max = 4×avg
(16 MiB at the 4 MiB default) → ≤ 128 MiB transient worst case, ~32 MiB typical.
State this in a comment on the constants.

**Step 1 (equivalence oracle first):** Move the current serial loop body into a
test-only reference:

```rust
#[cfg(test)]
fn chunk_and_hash_reader_serial<R: std::io::Read>(
    source: R,
    avg_size: u64,
) -> Result<(String, ChunkList), CoreError> {
    /* the exact current implementation */
}
```

Add a proptest pinning equivalence (this also re-pins constraint 1, boundary
determinism):

```rust
proptest! {
    #[test]
    fn parallel_chunk_hash_matches_serial(
        data in proptest::collection::vec(any::<u8>(), 0..262_144),
        avg in 256u64..8192,
    ) {
        let serial = chunk_and_hash_reader_serial(std::io::Cursor::new(&data), avg)?;
        let parallel = chunk_and_hash_reader(std::io::Cursor::new(&data), avg)?;
        prop_assert_eq!(serial, parallel);
    }
}
```

Run: passes trivially (both are the serial impl right now). This is the safety
net for step 2.

**Step 2:** Rewrite `chunk_and_hash_reader` (validation and `StreamCDC` setup
unchanged):

```rust
const MAX_HASH_WORKERS: usize = 4;
const HASH_QUEUE_DEPTH: usize = 4;

fn chunk_and_hash_reader<R: std::io::Read>(
    source: R,
    avg_size: u64,
) -> Result<(String, ChunkList), CoreError> {
    // ... existing validation, to_u32, StreamCDC::new ...

    let workers = std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1))
        .unwrap_or(1)
        .clamp(1, MAX_HASH_WORKERS);

    std::thread::scope(|scope| {
        // (index, chunk bytes) in; (index, digest) out. sync_channel bounds
        // transient memory: (HASH_QUEUE_DEPTH + workers) × max_chunk.
        let (work_tx, work_rx) =
            std::sync::mpsc::sync_channel::<(usize, Vec<u8>)>(HASH_QUEUE_DEPTH);
        let work_rx = std::sync::Arc::new(std::sync::Mutex::new(work_rx));
        let (done_tx, done_rx) = std::sync::mpsc::channel::<(usize, [u8; 32])>();

        for _ in 0..workers {
            let rx = std::sync::Arc::clone(&work_rx);
            let tx = done_tx.clone();
            scope.spawn(move || loop {
                // Hold the lock only for the recv; hashing runs unlocked so
                // workers overlap. Poisoning is unreachable (workers do not
                // panic) but treated as shutdown, not unwrap.
                let job = match rx.lock() {
                    Ok(guard) => guard.recv(),
                    Err(_) => break,
                };
                let Ok((idx, data)) = job else { break };
                let digest: [u8; 32] = sha2::Sha256::digest(&data).into();
                if tx.send((idx, digest)).is_err() {
                    break;
                }
            });
        }
        drop(done_tx); // collectors below see EOF once workers exit

        // Producer: CDC + whole-file hash stay on this thread (constraint 2).
        let mut whole = Sha256::new();
        let mut metas: Vec<(u64, u64)> = Vec::new();
        let mut produce_err: Option<CoreError> = None;
        for (idx, result) in chunker.enumerate() {
            match result {
                Ok(cd) => {
                    whole.update(&cd.data);
                    metas.push((cd.offset, cd.length as u64));
                    if work_tx.send((idx, cd.data)).is_err() {
                        break; // all workers gone — join below surfaces state
                    }
                }
                Err(e) => {
                    produce_err = Some(CoreError::Io(std::io::Error::other(e)));
                    break;
                }
            }
        }
        drop(work_tx); // workers drain the queue then exit

        let mut digests: Vec<Option<[u8; 32]>> = vec![None; metas.len()];
        for (idx, digest) in done_rx {
            if let Some(slot) = digests.get_mut(idx) {
                *slot = Some(digest);
            }
        }
        if let Some(e) = produce_err {
            return Err(e);
        }

        let chunks = metas
            .into_iter()
            .zip(digests)
            .map(|((offset, len), digest)| {
                digest
                    .map(|d| (hex::encode(d), offset, len))
                    .ok_or_else(|| {
                        CoreError::Integrity("chunk hash worker exited early".to_string())
                    })
            })
            .collect::<Result<ChunkList, CoreError>>()?;
        Ok((hex::encode(whole.finalize()), chunks))
    })
}
```

**Step 3:** `cargo test uploader` — the proptest and every existing
`chunk_and_hash` test (empty input, digest-vs-reference at
`src/uploader.rs:1090,1136`) must pass unchanged.

**Step 4 (verify the test catches failures):** Temporarily corrupt one digest in
the parallel path (e.g. hash `&data[..data.len()-1]`), confirm the proptest fails,
revert.

**Step 5:** Benchmark: `python scripts/bench_phase1.py`. Expected: ≥1.5x on a
multi-core box (phase 1 collapses to whole-file hash + CDC). Record before/after
in the commit message. Commit:
`perf: hash chunks on a scoped worker pool off the CDC critical path (X→Y MiB/s)`

### Task B2: SHA backend spike (timeboxed, decision-gated — may conclude "no")

**Files:**
- Create: nothing permanent unless adopted

**Step 1:** Extend `scripts/bench_phase1.py` (or a scratch Rust bin in the
scratchpad, not the repo) to isolate single-stream SHA-256 throughput. Compare
`sha2` (asm) against `ring::digest` on the actual wheel targets available to you
(Apple Silicon at minimum; Linux x86_64 via CI or a Docker run). Team memory
baseline: ring ≈ 1.5x on a no-SHA-NI Xeon; on M1 sha2+asm already hits the ARMv8
crypto ceiling (~2 GiB/s), so expect ring ≈ parity there.

**Step 2 — decision gate:** Adopt ring ONLY if all three hold: (a) ≥1.3x phase-1
end-to-end on at least one shipped wheel target, (b) wheels still build on every
target in CI (ring carries its own asm/build story), (c) digests bit-identical.
Otherwise write the numbers into the PR description and close the spike with no
code change. Note: after B1, single-stream SHA only bounds the whole-file-hash
floor — re-check whether it is still the binding constraint before spending here.

### Task B3: stream chunks to Python; overlap packing/upload with hashing

This is the largest task. Sub-plan strictly in order; each sub-step is a commit.

**Files:**
- Modify: `src/uploader.rs` (pipeline core emits ordered chunks incrementally),
  `src/lib.rs` (new `ChunkStream` pyclass + entry point)
- Modify: `hippius_hub/_packing.py` (add `PackAccumulator`),
  `hippius_hub/file_upload.py:286-365` (`_upload_file_chunked_v2`)
- Test: `tests/test_packing.py`, the existing upload tests, plus a new
  stream-integration test

**B3.1 — Rust: ordered incremental emission.**

Refactor the B1 pipeline so the collector can emit `(hex, offset, len)` triples in
file order as digests resolve, instead of only at EOF:

- Producer thread (CDC + whole hash + feed) and workers exactly as B1.
- Collector keeps `next_emit: usize` and a `BTreeMap<usize, [u8; 32]>` of
  out-of-order completions; while `next_emit` is present, pop and push onto the
  current batch; flush the batch to an output `sync_channel` (capacity 2) every
  `CHUNK_BATCH` chunks (default 64 — metadata only, bytes are long dropped, so
  the queue is KiB-scale).
- `chunk_and_hash_reader` becomes a thin consumer of this (collect all batches),
  so the B1 proptest keeps guarding the shared pipeline.

**B3.2 — Rust: `ChunkStream` pyclass.**

```rust
/// Pull-based streaming FFI for the chunked-v2 upload: Python drives, so no
/// Rust→Python reentrancy and the GIL is held only between batches.
#[pyclass]
struct ChunkStream { /* Mutex<Option<Receiver<...>>>, Mutex<Option<JoinHandle<...>>> */ }

#[pymethods]
impl ChunkStream {
    /// Next file-ordered batch of (sha256_hex, offset, length), or None at EOF.
    /// Blocks off-GIL (py.detach) in a recv_timeout loop that polls Ctrl-C the
    /// same way run_interruptible does (SIGNAL_POLL_INTERVAL / poll_ctrl_c).
    fn next_batch(&self, py: Python<'_>) -> PyResult<Option<Vec<(String, u64, u64)>>>;

    /// Whole-file sha256 hex. Errors if called before next_batch returned None.
    /// Joins the producer thread; a pipeline error raises here (or on the
    /// next_batch that first observes it).
    fn finish(&self, py: Python<'_>) -> PyResult<String>;
}

#[pyfunction]
fn chunk_stream_native(path: String, avg_size: u64) -> PyResult<ChunkStream>;
```

The producer runs on a plain `std::thread` (pure CPU+file I/O, no tokio — same
reasoning as `chunk_and_hash_native`'s `py.detach` at `src/lib.rs:265`).
Dropping the `ChunkStream` mid-iteration drops the receiver; the producer sees the
closed channel and exits (test this — no leaked thread, no deadlock).

Rust tests: batches concatenated == `chunk_and_hash` output (same file);
early-drop terminates the producer; error in the reader surfaces on `next_batch`.

**B3.3 — Python: incremental `PackAccumulator` (TDD, property-tested).**

In `hippius_hub/_packing.py`:

```python
class PackAccumulator:
    """Incremental plan_packs: feed chunks in file order, packs complete as
    ~pack_size of NEW bytes accumulate, so uploads start before chunking ends.

    Invariant: for any chunk sequence, feeding all chunks then finish() yields
    a PackPlan identical to plan_packs(chunks, dedup_index, pack_size)."""

    def __init__(self, dedup_index, pack_size): ...
    def feed(self, chunk) -> list[NewPack]:  # zero or more packs just completed
    def finish(self) -> PackPlan:            # flushes the final partial pack
```

Write the equivalence test FIRST (mirror the existing `tests/test_packing.py`
style; randomized chunk lists + dedup indices over fixed seeds — use `hypothesis`
only if it is already a test dependency, otherwise seeded `random`):

```python
def test_accumulator_equals_plan_packs(seeded_cases):
    for chunks, index, pack_size in seeded_cases:
        acc = PackAccumulator(index, pack_size)
        packs = [p for c in chunks for p in acc.feed(c)]
        plan = acc.finish()
        assert plan == plan_packs(chunks, index, pack_size)
        assert tuple(packs) == plan.new_packs[: len(packs)]
```

Run (fails: class missing) → implement by refactoring `plan_packs`' loop body into
the class and reimplementing `plan_packs` ON TOP of `PackAccumulator`
(replace, don't duplicate — one packing implementation). All existing packing
property tests keep passing.

**B3.4 — Python: streaming `_upload_file_chunked_v2`.**

Rewrite `hippius_hub/file_upload.py:303-328`:

```python
stream = chunk_stream_native(abs_path, resolve_cdc_avg_size())
acc = PackAccumulator(dedup_index, resolve_pack_size())
futures: list = []
with ThreadPoolExecutor(max_workers=resolve_upload_workers()) as executor:
    while (batch := stream.next_batch()) is not None:
        for h, offset, size in batch:
            for new_pack in acc.feed((f"sha256:{h}", size, offset)):
                futures.append(executor.submit(_upload_pack, new_pack))
    plan = acc.finish()
    if len(futures) < len(plan.new_packs):        # final partial pack
        for new_pack in plan.new_packs[len(futures):]:
            futures.append(executor.submit(_upload_pack, new_pack))
    whole_hex = stream.finish()
    new_pack_digests = [f.result() for f in futures]  # order == plan.new_packs
```

Everything from `resolve_pointer_chunks` down is unchanged (pointer still written
last — crash safety preserved). The pack gate still brackets the native call
inside `_upload_pack`, so the memory ceiling is untouched.

Then check remaining callers of `chunk_and_hash_native`
(`rg -n "chunk_and_hash_native" hippius_hub/ tests/ smoke/`): if the v2 path was
its only production caller, remove the pyfunction and port its tests to the
stream (replace, don't deprecate); keep it only if the v1/legacy path still calls it.

**B3.5 — Integration + failure tests.** Extend the existing mock-registry upload
tests (find them: `rg -ln "pack_upload_native|_upload_file_chunked_v2" tests/`):
(a) multi-pack file uploads correctly with digests/pointer identical to the
pre-change fixture expectations; (b) a pack-upload failure mid-stream propagates
and does not hang the stream (executor shutdown joins, `ChunkStream` drop
terminates the producer); (c) Ctrl-C path — at minimum the Rust-side early-drop
test from B3.2 plus a Python test that abandoning iteration doesn't leak.

**B3.6 — Measure and record.** `scripts/bench_phase1.py` (unchanged number
expected) plus an end-to-end wall-clock comparison of `_upload_file_chunked_v2`
against a local mock registry on a ≥2 GiB file, before/after branch. Record both
in the PR. Then `mcp__hippius-mem__remember` the design decision (pull-based
ChunkStream + PackAccumulator, expected overlap win, and that
`docs/plans/2026-08-10-core-review-remediation.md` Phase B supersedes the
never-committed 2026-07-16 design doc).

---

# Phase C — Download polish (branch `feat/range-incremental-verify`)

### Task C1: incremental whole-file hash for the Range path

**Files:**
- Create: `src/incremental_hash.rs`
- Modify: `src/chunk_fetcher.rs` (delete the moved code, import), `src/lib.rs`
  (module decl), `src/chunked_downloader.rs` (wire in; `download` at the
  verify branch `:296-311`)

**Step 1 (pure move):** Extract `spawn_incremental_hasher`, `incremental_hash`,
and the `HashSignal`/`HasherTask`/`IncrementalHash` types
(`src/chunk_fetcher.rs:213-226` + their fn bodies) into `src/incremental_hash.rs`
as `pub(crate)`, together with their unit tests. No behavior change; gates pass.
Commit: `refactor: extract the incremental whole-file hasher into its own module`

**Step 2 (failing test):** In `chunked_downloader` tests, mirror the pack path's
incremental-hash test: a multi-chunk download with `verify_hash=true` must produce
the correct digest WITHOUT a second full read. Assert via the hasher-used path
(e.g. expose whether fallback ran, or assert on the existing message channel) —
follow whatever the chunk_fetcher tests assert today.

**Step 3:** Wire it: in `download`, when `verify_hash` is true, call
`spawn_incremental_hasher(dest, content_length, true)` before spawning chunk
tasks; each chunk task sends its `vec![(start, len)]` extent on completion (the
Range bounds already tile the file exactly — that is the hasher's invariant); after
the join loop, drop the sender, await the task; `Some(hex)` → return it, `None`
(hasher could not cover the file) → existing `compute_sha256` full-read fallback.
Correctness never depends on the incremental path — identical to the pack path's
best-effort contract.

**Step 4:** Gates; run the downloader test suite specifically
(`cargo test chunked_downloader`). Commit:
`perf: overlap Range-path whole-file verify with the download (mirror pack path)`

### Task C2: cut per-task clones on the fan-outs

**Files:**
- Modify: `src/chunked_downloader.rs` (per-chunk task spawn ~`:201-253`),
  `src/chunk_fetcher.rs:342-352,589`

**Step 1:** Downloader: store `url: Arc<str>`, `auth_token: Option<Arc<str>>`,
`dest: Arc<Path>` once in the struct/plan; tasks clone the `Arc`s (pointer bump)
instead of `String`/`PathBuf`. Pack fetcher: move each `PackPlanEntry` into its
task instead of cloning `url` + every target's `expected_sha256` (restructure the
loop to consume `packs` by value, or wrap entries in `Arc`); drop the
`targets.to_vec()` at `:589` by passing the owned/Arc'd slice through.

**Step 2:** No behavior change — full gates + existing tests only. Commit:
`perf: share fan-out state via Arc instead of per-task String clones`

### Task C3 (OPTIONAL — skip unless C-profiling shows syscall cost): positioned writes

`FileExt::write_at`/`read_at` on Unix (seek+write fallback behind `#[cfg]` for
Windows) for `verify_and_scatter` and `read_ranges`. Only do this with a
measurement in hand; otherwise record "skipped, no evidence" in the PR and move on.

---

# Phase D — Structure (branch `refactor/core-module-split`)

Mechanical only; every task is a pure move with gates green. Do this LAST so
Phases B/C don't churn through renamed files.

### Task D1: split `src/uploader.rs`

`src/uploader/` directory: `mod.rs` (re-exports keep `crate::uploader::*` paths
stable), `cdc.rs` (chunk+hash pipeline, ChunkStream producer), `client.rs`
(upload_client + consts), `watchdog.rs` (`DoneOnEof`, `send_streaming_watchdogged`,
stall consts), `blob.rs` (session/PATCH/close/put_streaming), `pack.rs`
(`pack_upload_async`, `read_ranges`, frames). Tests move with their subjects.
One commit; `cargo test` count identical before/after.

### Task D2: split `src/chunk_fetcher.rs` and `src/chunked_downloader.rs`

Same recipe: fetcher → `client.rs` / `assemble.rs` / `scatter.rs`; downloader →
`plan.rs` (num_chunks/bounds math) / `download.rs` / `verify.rs`.

Additionally (function-level, not just file-level): split `PackAssembler::assemble`
and `ChunkedDownloader::download` below 100 lines (extract setup/teardown helpers),
then remove the two `#[expect(clippy::too_many_lines)]` attributes A1 added —
`unfulfilled_lint_expectations` will enforce the removal.

### Task D3: consolidate transport constants

Single `src/transport.rs` holding the shared named constants
(`CONNECT_TIMEOUT_SECS`, `VERIFY_READ_BUFFER`/`HASH_READ_BUFFER`,
`CHUNK_REQUEST_TIMEOUT`) with the existing per-side doc comments preserved; both
clients import. Do NOT merge the clients themselves — the upload/download timeout
philosophies differ deliberately (`src/uploader.rs:269-281`).

---

## Definition of done (whole plan)

- All gates green on every commit; zero new dependencies except (conditionally) ring.
- Bench evidence attached to the Phase B PR: phase-1 MiB/s before/after B1, and
  end-to-end wall clock before/after B3 on a ≥2 GiB file.
- Adversarial self-review before each PR (per house rules); PR descriptions state
  what the code does now, plain language.
- Durable learnings recorded in hippius-mem: the B3 design decision, the B2
  bench verdict, and any gotcha discovered (one fact per note).
