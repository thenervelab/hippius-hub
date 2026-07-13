# Connection & Transport Audit Remediation Plan

> **For Claude:** REQUIRED SUB-SKILL: use `superpowers:test-driven-development` for every code task (write the failing test first). Rust changes: `cargo test` + `cargo clippy --all-targets -- -D warnings` green before each commit. Python: `pytest -m 'not expensive and not e2e' -q` + `ruff check` + `ty check` green before each commit. illu MCP is unavailable this session — use Read/Grep; the illu quality-gate is substituted by clippy+test+ruff+ty.

**Goal:** Remediate all 20 surviving findings from the 2026-07-11 connection/perf audit — closing the upload/download stall-timeout gaps, adding cancellation, fixing correctness/robustness edges, and landing the perf micro-wins — each with tests, and benchmarks for the perf items.

**Architecture:** Two planes. The Rust data plane (`hippius_core`, `src/*.rs`) does byte transfer over process-global reqwest clients; the Python control plane (`hippius_hub/*.py`) does OCI/Harbor metadata over a pooled `httpx.Client`. The audit's core theme is *upload/download plane divergence*: the download plane is well-guarded, the upload plane is not. Fixes restore parity.

**Tech stack:** Rust (tokio, reqwest 0.12, pyo3 0.29, tokio-util, sha2), Python 3.9+ (httpx, pytest, respx, hypothesis), maturin bridge.

**Key verified design facts (do not re-derive):**
- reqwest 0.12 `read_timeout` is **`ClientBuilder`-only**, covers **reads only**, resets per successful read. No per-request variant. → thread via **first-caller-wins** on the process-global client (mirror `chunk_fetcher::global_pack_gate`).
- read_timeout does **not** bound a PUT request-body **write** stall (zero-window). → upload needs an **idle progress watchdog** on the body stream, not read_timeout alone.
- `RequestBuilder::timeout` = total deadline (keep the existing per-chunk 5-min one).
- The stale `constants.py:176` "reqwest 0.11 only offers a total request timeout" premise is false and must be corrected.

**Branch:** `perf/connection-audit-remediation-2026-07` (off `staging`).

---

## Phase ordering (value/risk, minimizes file re-churn)

| Phase | Findings | Files | Risk |
|-------|----------|-------|------|
| 1 | L4 jitter | `src/retry.rs` | low |
| 2 | M4, L9 download read-timeout + env plumbing | `chunk_fetcher.rs`, `chunked_downloader.rs`, `lib.rs`, `constants.py`, `file_download.py` | med |
| 3 | H1 upload stall guard | `uploader.rs`, `error.rs` | med-high |
| 4 | L1, L5 download correctness | `chunked_downloader.rs` | med |
| 5 | L12, L13, L14, L15, L16 memory/perf | `chunk_fetcher.rs`, `chunked_downloader.rs`, `uploader.rs` | low-med |
| 6 | L2 plain-blob retry session | `uploader.rs`, `lib.rs`, `file_upload.py` | med |
| 7 | L7 diagnostics DNS | `diagnostics.rs` | low |
| 8 | M1 cancellation (Ctrl-C) | `lib.rs`, `chunked_downloader.rs`, `chunk_fetcher.rs`, `uploader.rs` | HIGH |
| 9 | M2 + critic#3 token refresh + endpoint | `auth.py`, `_oci.py`, `file_upload.py`, `file_download.py`, `diagnose.py` | med |
| 10 | L3 control-plane retry | `_http.py` (new helper), `_oci.py`, `auth.py`, `file_upload.py` | med |
| 11 | M3 fail-fast/cancel | `_snapshot_download.py`, `file_upload.py` | low |
| 12 | L6, L8, L10, L11 timeouts+logic | `_harbor.py`, `cli.py`, `file_upload.py` | low |
| 13 | Benchmarks, e2e, CHANGELOG, release notes | `scripts/`, `tests/`, `CHANGELOG.md` | low |

Commit per phase (or per finding where a phase groups several). Never squash correctness with perf.

---

## Design decisions locked (FFI-affecting — flagged per global rule)

1. **H1 upload write-stall → idle progress watchdog** (not size-scaled total timeout). A size-scaled total timeout would take hours to trip on a multi-GB blob, so it does not prevent the folder-wedge; the watchdog trips on ~30s of no write progress. `.read_timeout(30s)` is added too (covers the zero-body init POST + response wait).
2. **M4/L9 → thread `connect_timeout_secs` + `read_timeout_secs` through `download_file_native` / `download_packs_native`** (matching the existing `diagnose_blob_native` signature), first-caller-wins into the client. Default read_timeout = 30s (fixes the 20-min slow-loris by default, not opt-in).
3. **L2 → `upload_blob_native` takes `uploads_url` + `digest`** and does POST-init + PUT per retry attempt (symmetry with `pack_upload_native`), instead of Python doing the POST and Rust re-PUTting a consumed session.
4. **M1 → `tokio_util::sync::CancellationToken` threaded into every orchestration; a driver task periodically re-acquires the GIL (`Python::attach`) and calls `check_signals`**, cancelling the token + aborting the `FuturesUnordered` on `KeyboardInterrupt`. Highest-risk; lands last of the Rust work behind its own tests.

---

## Phase 1 — L4: backoff jitter collapse (`src/retry.rs`)

**Bug:** `entropy = subsec_nanos()` (ns) reduced `% cap_ms` (ms) — on µs-resolution clocks nanos is always ×1000, every cap is ×200, so attempt-1 delay is always 0 → lockstep retry storm.

**Files:** Modify `src/retry.rs:44-46`; test in same file's `#[cfg(test)]`.

- **Step 1 (failing test):** add `jitter_spreads_on_microsecond_clock` — feed `jittered_backoff(1, e)` for `e in (1000..=200_000).step_by(1000)` (µs-quantized nanos), assert the set of distinct delays > 1 (today it's `{0}`). Run: `cargo test -p hippius_core retry:: -- --nocapture` → FAIL.
- **Step 2 (fix):** compute in nanoseconds — `Duration::from_nanos(entropy % (cap_ms.saturating_mul(1_000_000)).max(1))`. Keep `always_within_cap` proptest (still must hold — the `< cap` invariant). Update the module doc to note the ns reduction.
- **Step 3:** `cargo test -p hippius_core` → PASS; `cargo clippy` clean.
- **Step 4 (commit):** `fix(retry): compute full-jitter in nanoseconds so it doesn't collapse to 0 on µs clocks`.

## Phase 2 — M4/L9: download read-timeout + honor env knobs

**Bug:** `download_client()` has no `read_timeout`; a dribbling chunk runs the 5-min total timeout ×3 retries ≈ 20 min. `HIPPIUS_READ_TIMEOUT`/`HIPPIUS_CONNECT_TIMEOUT` reach only `diagnose`.

**Files:** `src/chunk_fetcher.rs` (`download_client`, `PackAssembler::new`), `src/chunked_downloader.rs` (its client + `ChunkedDownloader::new`), `src/lib.rs` (`download_file_native`, `download_packs_native` signatures), `hippius_hub/constants.py` (fix stale docstring; keep `resolve_read_timeout`/`resolve_connect_timeout`), `hippius_hub/file_download.py` (pass them at the 3 call sites), tests in each Rust module + `tests/`.

- **Design:** `download_client(read_timeout: Duration, connect_timeout: Duration)` — first-caller-wins `OnceLock`; default read_timeout 30s when Python passes `None`. Add `.read_timeout(read_timeout)` to both download builders.
- **Step 1 (failing test):** Rust `tokio::test` `download_errors_on_stalled_read` — a localhost `TcpListener` that accepts, sends headers + 1 byte, then sleeps > read_timeout; assert `download` returns `Err` within ~read_timeout+ε (set a 2s read_timeout in the test). FAIL today (hangs / only the 5-min total bounds it).
- **Step 2 (fix):** thread the timeouts; correct `constants.py` docstring (state reqwest 0.12 supports per-read stall detection; the knob now reaches real transfers). Default read_timeout applied when unset.
- **Step 3:** Rust test passes; add Python `tests/test_download_read_timeout.py` asserting `file_download` forwards `resolve_read_timeout()`/`resolve_connect_timeout()` into `download_file_native` (monkeypatch the native fn, assert kwargs). `pytest` green.
- **Step 4 (commit):** `perf(download): add read_timeout + honor HIPPIUS_READ/CONNECT_TIMEOUT on real transfers`.

## Phase 3 — H1: upload stall guard (`src/uploader.rs`, `src/error.rs`)

**Bug:** `upload_client()` has neither `.timeout()` nor `.read_timeout()`; a stalled-but-TCP-alive peer hangs `send().await` forever and drains `_pack_upload_gate` → whole `upload_folder` wedges.

- **Design:**
  - Add `.read_timeout(30s)` to `upload_client()` (covers init POST + response waits).
  - `put_streaming`: wrap the body stream so each yielded chunk stamps `Arc<Mutex<Instant>>` (lock only briefly — never across await, `await_holding_lock` is denied). Spawn a watchdog; `tokio::select!` the `send()` future against a loop that trips when `last_progress.elapsed() > WRITE_STALL_TIMEOUT` (30s). On trip → drop the send future (cancels request) and return a **retryable** `CoreError` so the existing retry loop re-attempts.
  - `error.rs`: add `#[error("upload stalled: no write progress for {0:?}")] Stall(Duration)` variant, `is_retryable()` → true.
- **Step 1 (failing test):** `upload_aborts_on_write_stall` — localhost server that reads the request line + a few KB then stops reading (fills the socket buffer); assert the PUT returns a retryable `Stall` within ~WRITE_STALL_TIMEOUT (use a 2s override in test). FAIL today (hangs).
- **Step 2 (fix):** implement watchdog + read_timeout + error variant.
- **Step 3:** `cargo test` + `cargo clippy` green; confirm `upload_retry_handles_5xx` still passes.
- **Step 4 (commit):** `fix(upload): idle write-stall watchdog + read_timeout so a stalled registry can't wedge a folder upload`.

## Phase 4 — L1/L5: legacy Range correctness (`src/chunked_downloader.rs`)

- **L1:** parse & validate `Content-Range` on a 206 before writing (`bytes {start}-{end}/*`); mismatch → retryable `CoreError`. Add a `parse_content_range` pure fn + **proptest** (round-trip: format then parse == identity; reject malformed).
- **L5:** accept `200 OK` when the request covers the whole object (single chunk / `start==0 && end==len-1`); stream the length-bounded body.
- **Steps:** failing unit tests (206 with wrong offset → error; 200 for whole-file range → success) → implement → proptest for the parser → green → commit `fix(download): validate Content-Range and accept whole-file 200 on the legacy path`.

## Phase 5 — L12/L13/L14/L15/L16: memory & perf

- **L12** (`chunk_fetcher.rs:396`): stream the pack body with a running byte counter; abort once received > `pack_size` (mirror `try_download_chunk_to_offset`) instead of `res.bytes()` unbounded. Test: mock oversized body → bounded rejection.
- **L13** (`chunked_downloader.rs:220`): bound live spawned tasks to `MAX_INFLIGHT_CHUNKS` (drain-as-permits-free); drop the `num_chunks` pre-size; fix the stale "semaphore bounds this" comment. Preserve the audit-D4 abort_handles cancellation intent. Test: task-count bound under a small chunk size.
- **L14** (`chunk_fetcher.rs:419`, and `pack_upload_async:343`): move slice/body SHA-256 onto `spawn_blocking`.
- **L15** (`chunk_fetcher.rs:174`, `chunked_downloader.rs:192`): drop `sync_all()` after `set_len()` (durability is discarded; `truncate(true)` on open). **Bench:** offline micro-bench timing 10k-file preallocation with/without fsync.
- **L16** (`uploader.rs:119`): use `VERIFY_READ_BUFFER` (8 MiB) in `hash_file_async`. **Bench:** offline micro-bench hashing a 1 GiB temp file 64 KiB vs 8 MiB.
- Commit: `perf(transport): bound pack body, offload hashing, drop dead fsync, widen hash buffer` (+ `bench(transport): hash-buffer and preallocation micro-benchmarks`).

## Phase 6 — L2: plain-blob retry session (`uploader.rs`, `lib.rs`, `file_upload.py`)

Make the plain path symmetric with the pack path: `upload_blob_native(uploads_url, path, digest, auth_token)` does POST-init + PUT-with-digest inside `try_upload_blob_once`, so each retry re-inits. Update `file_upload.py:84-92` to pass `uploads_url` + `digest` instead of pre-POSTing. Test: retry after a mid-PUT reset re-inits and succeeds (mock). Commit `fix(upload): re-init the OCI upload session per retry on the plain path`.

## Phase 7 — L7: diagnostics DNS (`src/diagnostics.rs`)

Wrap `lookup_host` in `tokio::time::timeout(connect_timeout, …)`; on multi-address results, try addresses in order (happy-eyeballs-lite) so a dead first IPv6 doesn't produce a false-negative verdict. Record a DNS-phase error. Test: unresolvable host → bounded DNS error. Commit `fix(diagnose): bound DNS resolution and try all resolved addresses`.

## Phase 8 — M1: cancellation (Ctrl-C) — HIGH RISK, isolated

Thread `CancellationToken` into `ChunkedDownloader::download`, `PackAssembler::assemble`, and the uploader orchestration; each per-chunk/pack task `select!`s against `token.cancelled()`. In each `lib.rs` entry point, spawn a driver that every ~100ms re-acquires the GIL (`Python::attach`) and calls `check_signals()`; on `Err` (KeyboardInterrupt) cancel the token, abort the futures, and return the `PyErr`. Confirm temp-file cleanup still runs on the cancel path. Tests: a cancellation-token unit per orchestration (token pre-cancelled → returns promptly, no partial file left). Commit `feat(transport): make native download/upload interruptible by Ctrl-C`.

## Phase 9 — M2 + critic#3: token refresh + endpoint

- **M2:** on a 401 from a blob/manifest PUT (Python side, and surfaced from Rust), call `clear_oci_token_cache()` + re-fetch (`use_cache=False`) and retry once with the fresh token. Wire into `file_upload.py` (manifest PUT + blob paths) and `file_download.py`.
- **critic#3:** `diagnose.py:100` pass `endpoint=endpoint` into `get_oci_bearer_token`.
- Tests: respx 401→refresh→200 for upload + download; diagnose custom-endpoint token uses the custom registry. Commit `fix(auth): refresh the OCI token on 401 mid-operation; thread endpoint through diagnose`.

## Phase 10 — L3: control-plane retry

Add a shared retry helper (in `_http.py`) mirroring `CoreError::is_retryable` (connection errors + 408/429/5xx, jittered capped backoff). Wrap `fetch_manifest` (`_oci.py`), the token fetch (`auth.py:429`), blob HEAD/init. For the dedup-index GET fan-out (`file_upload.py:243`), catch per-pointer failures and **drop that pointer's chunks** (fail-open on an optimization read). Tests: respx 503→retry→200; a failing pointer GET degrades instead of aborting. Commit `fix(http): retry transient control-plane failures; dedup index fails open`.

## Phase 11 — M3: fail-fast / cancel queued work

`_snapshot_download.py:197` and `file_upload.py:1007`: on the first raised `future.result()`, `executor.shutdown(wait=False, cancel_futures=True)` (try/finally). Tests: injected failure cancels queued futures (assert not all ran). Commit `fix(transfer): fail-fast and cancel queued work on first error / Ctrl-C`.

## Phase 12 — L6/L8/L10/L11: timeouts + logic

- **L6:** `_harbor.py:55/89/138/183` pass `timeout=DEFAULT_HTTP_TIMEOUT`.
- **L8:** `cli.py:190` `subprocess.run(..., timeout=60)` + `TimeoutExpired` guard.
- **L10:** `file_upload.py:985` guard the dedup-index build on `any(size >= resolve_chunk_threshold())` (mirror `upload_file`).
- **L11:** `cli.py:465/491` `{(m.get('format') or '—'):12}`.
- Tests per item (respx timeout kwarg assertions; a `format: null` row lists without crashing). Commit `fix(cli/harbor): explicit timeouts, null-format guard, gated dedup index`.

## Phase 13 — benchmarks, e2e, docs

- Offline micro-benches from Phase 5 wired as `scripts/bench_micro.py` (hash-buffer, preallocation-fsync) with before/after asserted deltas.
- e2e (gated `@pytest.mark.e2e`): a real stalled-read/timeout smoke where feasible; the token-refresh path against `test/e2e-client`. These run in CI (`e2e.yml`) with `HIPPIUS_TEST_*`.
- Run: full offline `pytest`, `cargo test`, `cargo clippy -D warnings`, `ruff`, `ty`. Update `CHANGELOG.md` + bump notes.
- Final commit `docs/bench: micro-benchmarks, e2e coverage, changelog for the connection-audit remediation`.

---

## Verification gates (every phase)
- Rust: `cargo test -p hippius_core` + `cargo clippy --all-targets --all-features -- -D warnings`.
- Python: `.venv/bin/python -m pytest -m 'not expensive and not e2e' -q` + `ruff check` + `ty check`.
- Rebuild the extension when Rust changes affect Python tests: `maturin develop` (or the project's build step) so `pytest` exercises the new `.so`.
- No `unsafe` is introduced (crate has none) — miri N/A unless that changes.
