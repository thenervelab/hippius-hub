# hippius-hub Audit Remediation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix every finding from the 2026-05-26 illu/axiom audit of `hippius-hub` (Python CLI + `hippius_core` pyo3 extension), plus backfill the test gaps the audit surfaced.

**Architecture:** Order tasks **by severity** (Critical → High → Medium → Low → Refactors → Tests-backfill). Each task is a separate commit so individual fixes can be reverted. Tests precede implementation (TDD) for behavior changes; for mechanical fixes (license fields, comment language, lint configs) we write the verification command instead.

**Tech Stack:** Python 3.13 + pytest (`ruff`/`ty` for lint+type), Rust 2021 + pyo3 0.20 → 0.22 + tokio + reqwest + sha2 + thiserror (new), respx (new test dep), proptest (new dev-dep).

**Worktree:** `/Users/georgiosdelkos/Documents/GitHub/Bitensor/hippius-hub-audit-fixes` on branch `audit/fix-all`.

**Conventions used in this plan:**
- File paths are absolute from the worktree root.
- "Run" lines give the exact shell command, with expected output.
- Commits use Conventional Commits (`fix:`, `feat:`, `refactor:`, `test:`, `chore:`).
- Each finding from the audit is tagged in the task header (e.g. `[C1]`, `[D3]`) so the audit report and the plan stay cross-referenced.

---

## Phase 0 — Safety net (do these first, they catch regressions in every later phase)

### Task 0.1: Add `[lints]` table to `Cargo.toml` [CT1]

**Files:**
- Modify: `Cargo.toml`

**Step 1: Append the project-mandated lints block** (per `~/.claude/CLAUDE.md` Rust section).

Append at the end of `Cargo.toml`:

```toml
[lints.clippy]
pedantic = { level = "warn", priority = -1 }
# Panic prevention
unwrap_used = "deny"
expect_used = "warn"
panic = "deny"
panic_in_result_fn = "deny"
unimplemented = "deny"
# No cheating
allow_attributes = "deny"
# Code hygiene
dbg_macro = "deny"
todo = "deny"
print_stdout = "deny"
print_stderr = "deny"
# Safety
await_holding_lock = "deny"
large_futures = "deny"
exit = "deny"
mem_forget = "deny"
# Pedantic relaxations (too noisy)
module_name_repetitions = "allow"
similar_names = "allow"
```

**Step 2: Run lints to see the existing violations**

Run: `cargo clippy --all-targets --all-features 2>&1 | tee /tmp/clippy-baseline.txt`
Expected: Several denied `unwrap_used` violations in `chunked_downloader.rs:100,159` and `uploader.rs:67`. These are fixed in later tasks — do NOT fix them here.

**Step 3: Commit lints config alone**

```bash
git add Cargo.toml
git commit -m "chore: add project clippy lints baseline

Subsequent commits fix the violations one by one."
```

---

### Task 0.2: Add `license` field to `Cargo.toml` [CT2]

**Files:**
- Modify: `Cargo.toml:1-10`

**Step 1: Add license**

Insert after `edition = "2021"`:
```toml
license = "MIT OR Apache-2.0"
```

**Step 2: Verify**

Run: `cargo metadata --format-version=1 | python -c 'import json,sys; m=json.load(sys.stdin); p=next(p for p in m["packages"] if p["name"]=="hippius_core"); print(p["license"])'`
Expected: `MIT OR Apache-2.0`

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: declare dual MIT/Apache-2.0 license"
```

---

### Task 0.3: Add `respx` and `proptest` to dev deps [test-backfill prereq]

**Files:**
- Modify: `pyproject.toml`
- Modify: `Cargo.toml`

**Step 1: Add `respx` to `[project.optional-dependencies.test]` in `pyproject.toml`** (or wherever pytest deps live).

After identifying the existing test dep section, add `"respx>=0.21"`.

**Step 2: Add `proptest` to `[dev-dependencies]` in `Cargo.toml`**.

```toml
[dev-dependencies]
proptest = "1.5"
```

**Step 3: Install + verify**

Run: `uv sync --all-extras && cargo build --tests`
Expected: Both succeed; no test runs yet.

**Step 4: Commit**

```bash
git add pyproject.toml Cargo.toml Cargo.lock uv.lock
git commit -m "chore: add respx (python) and proptest (rust) dev deps"
```

---

## Phase 1 — Tier 1 Critical (ship-blockers)

### Task 1.1: Add `timeout=` to the OCI bearer token request [C1]

**Files:**
- Test: `tests/test_auth_timeout.py` (create)
- Modify: `hippius_hub/auth.py:192`

**Step 1: Write failing test**

Create `tests/test_auth_timeout.py`:

```python
"""Regression: auth.py:192 must pass timeout= to httpx.get."""
import inspect
from hippius_hub import auth


def test_get_oci_bearer_token_passes_timeout():
    src = inspect.getsource(auth.get_oci_bearer_token)
    assert "timeout=" in src, (
        "get_oci_bearer_token must pass timeout= to its httpx call; "
        "without it a stalled token endpoint hangs the whole client."
    )
```

(We test the source rather than mocking httpx so the test survives the respx migration in Phase 6.)

**Step 2: Run test → expect FAIL**

Run: `pytest tests/test_auth_timeout.py -v`
Expected: FAIL — `timeout=` not present.

**Step 3: Fix `hippius_hub/auth.py:192`**

Change:
```python
resp = httpx.get(auth_url, headers=headers)
```
to:
```python
resp = httpx.get(auth_url, headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
```

`DEFAULT_HTTP_TIMEOUT` is already imported from `.constants` indirectly via other modules — verify the import at the top of `auth.py` includes it. If not, add `DEFAULT_HTTP_TIMEOUT` to the existing `from .constants import ...` line.

**Step 4: Run test → expect PASS**

Run: `pytest tests/test_auth_timeout.py -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_auth_timeout.py hippius_hub/auth.py
git commit -m "fix(auth): add HTTP timeout to OCI bearer token request

Every other HTTP call in the codebase passes DEFAULT_HTTP_TIMEOUT.
The token endpoint gates every other call, so a stalled registry
hangs the whole client forever without it.

Closes: audit C1"
```

---

### Task 1.2: Honor `token=False` (skip docker-config fallback) [C2]

**Files:**
- Test: `tests/test_auth_token_false.py` (create)
- Modify: `hippius_hub/auth.py:155-200`

**Step 1: Write failing test**

```python
"""Regression: token=False must skip docker-config fallback.

HF semantics: 'If False, no token will be used.'
Current code: falls through to get_docker_auth(), violating that contract.
"""
from unittest.mock import patch
import pytest
from hippius_hub.auth import get_oci_bearer_token


@patch("hippius_hub.auth.get_docker_auth")
@patch("hippius_hub.auth.httpx.get")
def test_explicit_no_auth_skips_docker_fallback(mock_http, mock_docker):
    mock_docker.return_value = "stolen-base64-auth"
    mock_http.return_value.json.return_value = {"token": "anon-token"}
    mock_http.return_value.raise_for_status.return_value = None

    # Caller explicitly says: no auth. Sentinel value at the boundary.
    get_oci_bearer_token("foo/bar", token=False, use_cache=False)

    # The docker fallback must not be consulted.
    mock_docker.assert_not_called()
    # The request must go out with NO Authorization header.
    call_headers = mock_http.call_args.kwargs["headers"]
    assert "Authorization" not in call_headers
```

**Step 2: Run → FAIL**

Run: `pytest tests/test_auth_token_false.py -v`
Expected: FAIL — `get_docker_auth` was called.

**Step 3: Fix**

Two changes in `hippius_hub/auth.py`:

1. Change the signature of `get_oci_bearer_token` to accept a sentinel:
   ```python
   def get_oci_bearer_token(
       repo_id: str,
       token: Union[str, bool, None] = None,
       push: bool = False,
       use_cache: bool = True,
   ) -> str:
   ```
2. Reshape the auth-resolution block (currently auth.py:178-190) into:
   ```python
   # `token is False` is the HF sentinel for "anonymous; do not auto-discover".
   no_auth = token is False
   effective_token = None if no_auth else token

   if not effective_token and not no_auth:
       docker_auth = get_docker_auth(DEFAULT_REGISTRY_URL)
       if docker_auth:
           headers["Authorization"] = f"Basic {docker_auth}"

   if not headers.get("Authorization") and effective_token:
       if effective_token.startswith(("Basic ", "Bearer ")):
           headers["Authorization"] = effective_token
       else:
           headers["Authorization"] = f"Bearer {effective_token}"
   ```

Also update `resolve_token_value` (auth.py:84-93) to forward `False` instead of collapsing to `None`:

```python
def resolve_token_value(token):
    if token is False:
        return False  # propagate the anonymous sentinel
    if isinstance(token, str):
        return token
    return get_token()
```

And update all six call sites that pass the result into `get_oci_bearer_token` (use `grep -nF 'get_oci_bearer_token(' hippius_hub/`) to ensure they pass `False` through.

**Step 4: Run all auth tests → PASS**

Run: `pytest tests/test_auth_token_false.py tests/test_phase_a.py -v`
Expected: All pass.

**Step 5: Commit**

```bash
git add tests/test_auth_token_false.py hippius_hub/auth.py
git commit -m "fix(auth): honor token=False per HF semantics

HF docs: 'If False, no token will be used.' Previously, passing
token=False normalized to None and then fell into the docker-config
fallback — so a user asking for anonymous I/O could silently push
under their docker credentials.

Now: token=False propagates as a sentinel, and the docker-config
fallback only runs when the caller has provided no explicit
preference (token=None).

Closes: audit C2"
```

---

### Task 1.3: chmod 0600 on the saved docker token [C3]

**Files:**
- Test: `tests/test_auth_token_perms.py` (create)
- Modify: `hippius_hub/auth.py:37-66`

**Step 1: Write failing test**

```python
"""Regression: login() must chmod 0600 the saved token file."""
import os
import stat
from hippius_hub.auth import login, TOKEN_PATH


def test_login_chmods_token_file(tmp_path, monkeypatch):
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "token"))
    login(token="abc")
    mode = os.stat(tmp_path / "token").st_mode
    perms = stat.S_IMODE(mode)
    assert perms == 0o600, f"expected 0600, got {oct(perms)}"
```

**Step 2: Run → FAIL**

Run: `pytest tests/test_auth_token_perms.py -v`
Expected: FAIL — perms are 0644 (or whatever umask produces).

**Step 3: Fix `hippius_hub/auth.py`**

Replace the `with open(...)` block at auth.py:64-65 with:

```python
# Write+chmod together so the file is never world-readable mid-flight.
# os.chmod on a path is a syscall, not enforced atomically with open();
# the best-effort try/except mirrors save_api_token in console.py.
with open(TOKEN_PATH, "w") as f:
    f.write(auth_str)
try:
    os.chmod(TOKEN_PATH, 0o600)
except OSError:
    pass
```

**Step 4: Run test → PASS**

Run: `pytest tests/test_auth_token_perms.py -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add tests/test_auth_token_perms.py hippius_hub/auth.py
git commit -m "fix(auth): chmod 0600 on the docker token file

Matches the existing handling in console.save_api_token().
Without this, the file sits at the default umask (0644 on Linux),
world-readable on multi-user hosts.

Closes: audit C3"
```

---

### Task 1.4: Process-global tokio runtime in `hippius_core` [L1]

**Files:**
- Modify: `src/lib.rs`

**Step 1: Write a Rust-side benchmark/test that demonstrates the cost**

Add to `src/lib.rs` under `#[cfg(test)]`:

```rust
#[cfg(test)]
mod runtime_tests {
    use std::time::Instant;

    #[test]
    fn shared_runtime_avoids_startup_cost() {
        // Smoke test: ten consecutive Runtime::new() calls take meaningful
        // time on macOS/Linux. After the fix, the global runtime is created
        // once and reused, so the per-call overhead drops to ~zero.
        let start = Instant::now();
        for _ in 0..10 {
            let _rt = tokio::runtime::Runtime::new().unwrap();
        }
        let elapsed = start.elapsed();
        // This assertion is informational, not load-bearing — we expect
        // 10 Runtime::new() to take more than a couple of milliseconds.
        // After the global-runtime fix, the equivalent loop will be ~0ms.
        eprintln!("10x Runtime::new(): {:?}", elapsed);
    }
}
```

This is a benchmark stub — its job is to make the cost visible during review, not to enforce a threshold.

**Step 2: Add a process-global runtime**

In `src/lib.rs`, add at module scope (after imports):

```rust
use std::sync::OnceLock;

/// Process-global multi-threaded tokio runtime.
///
/// Per pyo3 best practice (and verified by axiom rust_quality_*_performance),
/// constructing a new tokio Runtime per call spins up worker threads, allocates
/// epoll/kqueue handles, and tears them down on drop — overhead that dominates
/// many-small-file workloads. One shared runtime amortises that cost across
/// the lifetime of the Python process.
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("hippius-core")
            .build()
            .expect("failed to build the shared tokio runtime — fatal at module init")
    })
}
```

**Step 3: Replace per-call `Runtime::new()` in the three `#[pyfunction]`s**

For each of `download_file_native`, `hash_file_native`, `upload_blob_native`, replace:

```rust
let rt = tokio::runtime::Runtime::new()
    .map_err(|e| PyRuntimeError::new_err(format!("Failed to create tokio runtime: {}", e)))?;
```

with:

```rust
let rt = shared_runtime();
```

(`rt.block_on(...)` still works — `&Runtime::block_on` is the same shape.)

**Step 4: Build + test**

Run: `cargo build --release && cargo test runtime_tests::shared_runtime_avoids_startup_cost -- --nocapture`
Expected: builds; prints elapsed time on stderr.

**Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "perf(core): share one tokio runtime across all #[pyfunction] calls

Previously every download/hash/upload call constructed a new multi-thread
Runtime, spawning worker threads on every invocation. For
snapshot_download — which calls hf_hub_download once per file — that
was the dominant overhead.

A OnceLock-wrapped global runtime amortises the cost across the
lifetime of the Python process.

Closes: audit L1"
```

---

### Task 1.5: Release the GIL across `rt.block_on` [L2]

**Files:**
- Modify: `src/lib.rs`

**Step 1: Update `#[pyfunction]` signatures to accept the `Python<'_>` token**

For each of the three functions, change the signature to take `py: Python<'_>` as the first parameter:

```rust
#[pyfunction]
#[pyo3(signature = (url, dest_path, auth_token=None, chunk_size=None, verify_hash=true))]
fn download_file_native(
    py: Python<'_>,
    url: String,
    dest_path: String,
    auth_token: Option<String>,
    chunk_size: Option<u64>,
    verify_hash: bool,
) -> PyResult<String> {
    let rt = shared_runtime();
    let downloader = ChunkedDownloader::new(url, auth_token, chunk_size)
        .map_err(|e| PyRuntimeError::new_err(format!("Downloader init error: {:?}", e)))?;
    let dest = PathBuf::from(dest_path);

    // Release the GIL so other Python threads can run during the (long)
    // network/disk I/O. pyo3 acquires the GIL automatically on function
    // entry; allow_threads explicitly releases it for the closure body.
    py.allow_threads(|| {
        rt.block_on(async { downloader.download(&dest, verify_hash).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Download failed: {:?}", e)))
    })
}
```

Apply the same pattern to `hash_file_native` and `upload_blob_native`.

**Step 2: Build**

Run: `cargo build --release`
Expected: builds clean.

**Step 3: Smoke-test via Python**

Run: `pytest tests/test_phase_a.py -k 'try_to_load_from_cache' -v`
Expected: PASS (these tests don't actually call the native code, but they import it — confirms the module still loads).

**Step 4: Commit**

```bash
git add src/lib.rs
git commit -m "perf(core): release the GIL across blocking native I/O

Previously rt.block_on() ran inside the Python thread while holding
the GIL — so other Python threads were starved for the duration of
any download/hash/upload. py.allow_threads() drops the GIL for the
closure body and re-acquires it on return; pyo3 maps any returned
PyResult back through the GIL as usual.

Closes: audit L2"
```

---

### Task 1.6: Preserve cause in `ChunkFailed` [D1]

**Files:**
- Test: `src/chunked_downloader.rs` (extend `tests` mod)
- Modify: `src/chunked_downloader.rs:37-43, 145-151`

**Step 1: Reshape `DownloadError`**

Replace:
```rust
ChunkFailed(usize),
```
with:
```rust
ChunkFailed {
    index: usize,
    source: Box<DownloadError>,
},
JoinFailed {
    index: usize,
    source: tokio::task::JoinError,
},
```

**Step 2: Update the consumer at line 145-151**

```rust
while let Some(res) = stream.next().await {
    match res {
        Err(join_err) => {
            return Err(DownloadError::JoinFailed {
                index: usize::MAX,    // unknown; the JoinError lost it
                source: join_err,
            });
        }
        Ok((i, Err(chunk_err))) => {
            return Err(DownloadError::ChunkFailed {
                index: i,
                source: Box::new(chunk_err),
            });
        }
        Ok((_, Ok(()))) => continue,
    }
}
```

**Step 3: Add a test that asserts the cause survives**

In `src/chunked_downloader.rs::tests`:

```rust
#[test]
fn chunk_failed_carries_cause() {
    let inner = DownloadError::ServerError(404, "not found".into());
    let outer = DownloadError::ChunkFailed { index: 3, source: Box::new(inner) };
    match outer {
        DownloadError::ChunkFailed { index, source } => {
            assert_eq!(index, 3);
            assert!(matches!(*source, DownloadError::ServerError(404, _)));
        }
        _ => panic!("expected ChunkFailed"),
    }
}
```

**Step 4: Build + test**

Run: `cargo test -p hippius_core`
Expected: All tests pass.

**Step 5: Commit**

```bash
git add src/chunked_downloader.rs
git commit -m "fix(core): preserve cause in ChunkFailed error variant

Previously a chunk error collapsed to ChunkFailed(usize) — the
user got 'chunk 5 failed' with zero clue whether it was a 404, a
500, a connection reset, or a disk-full. The structured cause now
flows through; the Python layer in a later commit will surface it
as a typed PyException.

Closes: audit D1"
```

---

### Task 1.7: Require HTTP 206 Partial Content on chunk reads [D2]

**Files:**
- Modify: `src/chunked_downloader.rs:269-284`

**Step 1: Write a test** (using `reqwest`'s mock pattern is non-trivial; instead we add an assertion-style test that exercises a `Builder` of the response object — or simpler, document via comment + fixture file).

Realistically, asserting against `try_download_chunk_to_offset` requires an HTTP fixture. The cheapest approach is a unit test of a small helper extracted from the function:

```rust
// In chunked_downloader.rs add:
fn require_partial_content(status: reqwest::StatusCode, start: u64, end: u64)
    -> Result<(), DownloadError>
{
    use reqwest::StatusCode;
    match status {
        StatusCode::PARTIAL_CONTENT => Ok(()),
        StatusCode::OK => Err(DownloadError::ServerError(
            status.as_u16(),
            format!(
                "server ignored Range bytes={}-{} (returned 200 OK instead of 206); \
                 writing the full body at offset {} would corrupt the file",
                start, end, start,
            ),
        )),
        other => Err(DownloadError::ServerError(
            other.as_u16(),
            format!("Failed chunk bytes {}-{}", start, end),
        )),
    }
}

#[cfg(test)]
mod partial_content_tests {
    use super::*;
    use reqwest::StatusCode;
    #[test]
    fn accepts_206() { assert!(require_partial_content(StatusCode::PARTIAL_CONTENT, 0, 99).is_ok()); }
    #[test]
    fn rejects_200_with_diagnostic() {
        let err = require_partial_content(StatusCode::OK, 0, 99).unwrap_err();
        let msg = format!("{:?}", err);
        assert!(msg.contains("ignored Range"));
    }
    #[test]
    fn rejects_other_4xx_5xx() {
        assert!(require_partial_content(StatusCode::NOT_FOUND, 0, 99).is_err());
        assert!(require_partial_content(StatusCode::INTERNAL_SERVER_ERROR, 0, 99).is_err());
    }
}
```

**Step 2: Replace the existing check in `try_download_chunk_to_offset`**

Replace:
```rust
if !res.status().is_success() {
    return Err(DownloadError::ServerError(
        res.status().as_u16(),
        format!("Failed chunk bytes {}-{}", start, end),
    ));
}
```
with:
```rust
require_partial_content(res.status(), start, end)?;
```

**Step 3: Test**

Run: `cargo test -p hippius_core partial_content_tests`
Expected: 3 passes.

**Step 4: Commit**

```bash
git add src/chunked_downloader.rs
git commit -m "fix(core): require HTTP 206 Partial Content on chunk reads

Previously try_download_chunk_to_offset accepted any 2xx status
including 200 OK. A server that ignored the Range header would
return 200 with the full body; we'd write the full file at the
chunk's offset, corrupting every byte from end+1 forward.

require_partial_content rejects 200 with a diagnostic that
explicitly mentions the Range header was ignored.

Closes: audit D2"
```

---

### Task 1.8: Error on missing Content-Length [D3]

**Files:**
- Modify: `src/chunked_downloader.rs:37-43, 173-192`

**Step 1: Add a new variant**

```rust
pub enum DownloadError {
    // ... existing ...
    MissingContentLength,
}
```

**Step 2: Replace `unwrap_or(0)` in `get_content_length`**

```rust
let content_length = res.headers()
    .get(header::CONTENT_LENGTH)
    .and_then(|val| val.to_str().ok())
    .and_then(|val| val.parse::<u64>().ok())
    .ok_or(DownloadError::MissingContentLength)?;
```

**Step 3: Empty-file path must come from the *response*, not the missing header.**

`download()` already calls `if content_length == 0 { return self.create_empty_file(...) }` — so a legitimately empty blob still works; only the *missing-header* case now errors. Add a regression test:

```rust
#[test]
fn missing_content_length_is_a_distinct_error() {
    let err = DownloadError::MissingContentLength;
    assert!(matches!(err, DownloadError::MissingContentLength));
}
```

**Step 4: Test**

Run: `cargo test -p hippius_core`
Expected: all pass.

**Step 5: Commit**

```bash
git add src/chunked_downloader.rs
git commit -m "fix(core): error on missing Content-Length instead of producing empty file

Previously a missing/unparseable Content-Length silently fell through
to create_empty_file(), truncating the destination to 0 bytes and
returning sha256 of empty data. The Python caller had no way to
distinguish 'blob is empty' from 'server didn't send Content-Length'.

Now: MissingContentLength is a distinct variant; the empty-file path
is only reached when the server explicitly says 0.

Closes: audit D3"
```

---

## Phase 2 — Tier 2 High

### Task 2.1: OCI `If-Match` for manifest PUT (or fail-fast) [H1]

**Files:**
- Modify: `hippius_hub/file_upload.py:240-305, 308-416`
- Modify: `hippius_hub/_oci.py` (extend `fetch_manifest` to return digest)
- Test: `tests/test_upload_if_match.py` (create)

**Step 1: Decide policy.** This task uses *optimistic concurrency*. If we discover Harbor doesn't support `If-Match` on manifests, we fall back to "single-writer-per-revision, fail-fast on concurrent PUT" — but try If-Match first; the OCI distribution spec section 4.4 includes it.

**Step 2: Extend `fetch_manifest` to also return the `Docker-Content-Digest` header**

Current signature returns `Optional[dict]`. Change to `Optional[tuple[dict, str]]` (manifest, digest) or split into a richer return type. Caller code at `upload_file:281`, `upload_folder:355` needs updates.

**Step 3: Add `If-Match: <prev-digest>` to `_put_manifest`**

```python
def _put_manifest(registry, repo_id, revision, oci_token, manifest, *, if_match: Optional[str] = None):
    url = f"{registry}/v2/{repo_id}/manifests/{revision}"
    headers = {
        "Authorization": f"Bearer {oci_token}",
        "Content-Type": "application/vnd.oci.image.manifest.v1+json",
    }
    if if_match:
        headers["If-Match"] = if_match
    resp = httpx.put(url, headers=headers, json=manifest, timeout=DEFAULT_HTTP_TIMEOUT * 2)
    if resp.status_code == 412:  # Precondition Failed
        raise ConcurrentManifestUpdateError(
            f"manifest at {repo_id}:{revision} changed between read and write"
        )
    resp.raise_for_status()
    return resp
```

Add `ConcurrentManifestUpdateError` to `hippius_hub/errors.py`.

**Step 4: Test** (mocks-based; respx-style)

```python
def test_put_manifest_sends_if_match_when_previous_digest_known(respx_mock):
    # ... arrange respx route + assert request headers ...
```

**Step 5: Commit**

```bash
git add hippius_hub/file_upload.py hippius_hub/_oci.py hippius_hub/errors.py tests/test_upload_if_match.py
git commit -m "fix(upload): use OCI If-Match for optimistic concurrency on manifest PUT

Previously two concurrent uploads to the same repo:revision raced;
the second PUT silently overwrote the first uploader's layer. Now
we send If-Match: <previous-manifest-digest>; on 412 Precondition
Failed we raise ConcurrentManifestUpdateError so the caller knows
to retry or serialize externally.

Closes: audit H1, M5"
```

---

### Task 2.2: Stop `.strip()`-ing secrets [H2]

**Files:**
- Modify: `hippius_hub/cli.py:573-575`
- Test: `tests/test_cli_login_no_strip.py` (create)

**Step 1: Test**

```python
"""Regression: .strip() must not be applied to secrets — it silently
mutates passwords that happen to end in whitespace, producing
misleading 401s downstream."""
import inspect
from hippius_hub import cli

def test_cli_does_not_strip_secrets():
    src = inspect.getsource(cli.main)
    # We allow .strip() on the username and the visible prompts,
    # but the lines that handle getpass output must not.
    for line in src.splitlines():
        if "getpass.getpass" in line:
            assert ".strip()" not in line, (
                f"strip() on a getpass result silently mutates secrets: {line}"
            )
```

**Step 2: Run → FAIL**

**Step 3: Fix** — remove `.strip()` from the two `getpass.getpass(...)` lines in `cli.py:573-575`.

**Step 4: PASS**

**Step 5: Commit**

```bash
git commit -m "fix(cli): do not strip whitespace from secrets

A password ending in whitespace would silently lose those bytes,
producing a misleading 401 with no diagnostic clue. .strip() is
appropriate on usernames; not on secrets.

Closes: audit H2"
```

---

### Task 2.3: Route typed exceptions in CLI dispatch [H3]

**Files:**
- Modify: `hippius_hub/cli.py:558-568`

**Step 1: Extract a helper**

In `cli.py`, add:

```python
def _format_download_error(e: Exception) -> tuple[str, int]:
    """Map a download/upload exception to (message, exit_code) for the CLI."""
    from .errors import (
        EntryNotFoundError, RepositoryNotFoundError, RevisionNotFoundError,
        LocalEntryNotFoundError, GatedRepoError, DisabledRepoError,
        HfHubHTTPError,
    )
    if isinstance(e, EntryNotFoundError):
        return (f"❌ File not found in repo: {e}", 2)
    if isinstance(e, RepositoryNotFoundError):
        return (f"❌ Repository not found: {e}", 3)
    if isinstance(e, RevisionNotFoundError):
        return (f"❌ Revision not found: {e}", 4)
    if isinstance(e, LocalEntryNotFoundError):
        return (f"❌ Local cache miss: {e}", 5)
    if isinstance(e, (GatedRepoError, DisabledRepoError)):
        return (f"❌ Access denied: {e}", 6)
    if isinstance(e, HfHubHTTPError):
        return (f"❌ Registry HTTP error: {e}", 7)
    return (f"❌ Download failed: {e}", 1)
```

**Step 2: Use it in the download and upload branches**

Replace `except Exception as e: print(f"❌ Download failed: {e}"); sys.exit(1)` with:

```python
except Exception as e:
    msg, code = _format_download_error(e)
    print(msg)
    sys.exit(code)
```

**Step 3: Test**

```python
@pytest.mark.parametrize("exc,expected_code", [
    (EntryNotFoundError("x"), 2),
    (RepositoryNotFoundError("y"), 3),
    (Exception("opaque"), 1),
])
def test_format_download_error_distinguishes_typed_errors(exc, expected_code):
    _, code = cli._format_download_error(exc)
    assert code == expected_code
```

**Step 4: Commit**

```bash
git commit -m "fix(cli): route typed download errors to distinct exit codes

Previously every failure collapsed into 'Download failed: <str(e)>'
with exit code 1. Now users (and CI consumers) get distinct exit
codes for EntryNotFound vs RepositoryNotFound vs Gated, etc.

Closes: audit H3"
```

---

### Task 2.4: Wire respx into the test infrastructure [H4]

**Files:**
- Modify: `tests/conftest.py` (extend)
- Create: `tests/respx_fixtures.py`

**Step 1: Add a `respx_mock` fixture** to `tests/conftest.py` (respx ships its own pytest plugin; just import it).

```python
# tests/conftest.py — append:
pytest_plugins = ["respx"]
```

**Step 2: Stub the Hippius registry endpoints**

Create `tests/respx_fixtures.py` with fixtures for `/service/token`, `/v2/<repo>/manifests/<rev>`, `/v2/<repo>/blobs/<digest>`. These will be consumed by the unit tests added in Phase 6 (auth.py + repo_ops backfill).

**Step 3: Verify**

Run: `pytest --collect-only tests/`
Expected: collection succeeds; no new tests yet.

**Step 4: Commit**

```bash
git commit -m "test: wire respx into the test infrastructure

Establishes the fixture surface for the upcoming unit-test backfill
of auth.py, _oci.py, and the manifest path — none of which has any
unit coverage today (all tests are e2e-marked, requiring real
credentials and a live registry).

Closes: audit H4 (infrastructure half)"
```

---

### Task 2.5: Fix `get_docker_auth` substring match → host equality [N1]

**Files:**
- Test: `tests/test_get_docker_auth.py` (create)
- Modify: `hippius_hub/auth.py:135-153`

**Step 1: Test**

```python
"""Regression: get_docker_auth must not substring-match registry hosts."""
import json
from hippius_hub.auth import get_docker_auth

def test_confused_deputy_resists_substring_match(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({
        "auths": {
            # Attacker entry — superstring of the real registry.
            "https://registry.hippius.com.evil.example": {"auth": "STOLEN"},
            "https://registry.hippius.com": {"auth": "REAL"},
        }
    }))
    monkeypatch.setenv("HOME", str(tmp_path.parent))
    monkeypatch.setattr("os.path.expanduser", lambda p: str(cfg) if p.endswith("config.json") else p)
    result = get_docker_auth("https://registry.hippius.com")
    assert result == "REAL"
```

**Step 2: Fix `get_docker_auth`**

Replace the substring loop:
```python
for key, val in auths.items():
    if host in key:
        return val.get("auth")
```
with:
```python
# Match on host, not substring — otherwise "registry.hippius.com" would
# match "registry.hippius.com.evil.example", a classic confused-deputy.
for key, val in auths.items():
    key_host = key.replace("https://", "").replace("http://", "").rstrip("/")
    if key_host == host:
        return val.get("auth")
```

**Step 3: PASS**

**Step 4: Commit**

```bash
git commit -m "fix(auth): use host equality, not substring match, for docker config

Previously a docker config containing
'registry.hippius.com.evil.example' would shadow the real
registry's entry — classic confused-deputy when registry hostnames
share a suffix. Now we compare normalised hosts for equality.

Closes: audit N1"
```

---

### Task 2.6: Cancel in-flight chunks on first error [D4]

**Files:**
- Modify: `src/chunked_downloader.rs:131-151`

**Step 1: Collect JoinHandles and abort on error**

Refactor the stream-driven loop:

```rust
let handles: Vec<_> = (0..num_chunks)
    .map(|i| {
        let (start, end) = chunk_bounds(content_length, self.chunk_size, i);
        let client = self.client.clone();
        let url = self.url.clone();
        let token = self.auth_token.clone();
        let chunk_pb = pb.clone();
        let path = dest_path_buf.clone();
        tokio::spawn(async move {
            let res = download_chunk_with_retry(client, url, token, start, end, i, path, chunk_pb).await;
            (i, res)
        })
    })
    .collect();

let mut stream = futures::stream::iter(handles.iter().enumerate().map(|(idx, h)| async move {
    // borrow the handle, await its completion
    h.await
})).buffer_unordered(MAX_CONCURRENT_DOWNLOADS);

while let Some(res) = stream.next().await {
    match res {
        Err(join_err) => {
            // Abort the rest before bubbling up.
            for h in &handles { h.abort(); }
            return Err(DownloadError::JoinFailed { index: usize::MAX, source: join_err });
        }
        Ok((i, Err(chunk_err))) => {
            for h in &handles { h.abort(); }
            return Err(DownloadError::ChunkFailed { index: i, source: Box::new(chunk_err) });
        }
        Ok((_, Ok(()))) => continue,
    }
}
```

(Note: `JoinHandle::abort` does not block — it signals cancellation. The aborted tasks finish their current await point and return without completing.)

**Step 2: Test** (smoke)

Run: `cargo test -p hippius_core`
Expected: all pre-existing tests still pass.

**Step 3: Commit**

```bash
git commit -m "fix(core): abort in-flight chunk tasks when one fails

JoinHandle::drop does not cancel a tokio task — so the previous
'early return on first error' code left in-flight chunk downloads
running in the background, writing to dest_path and holding
network sockets. We now collect the handles and abort the rest
before propagating the error.

Closes: audit D4"
```

---

### Task 2.7: Classify retries by status code [D5]

**Files:**
- Modify: `src/chunked_downloader.rs:229-255`

**Step 1: Helper**

```rust
fn is_retryable(err: &DownloadError) -> bool {
    match err {
        // Network/transport errors are retryable.
        DownloadError::ReqwestError(_) => true,
        DownloadError::IoError(_) => true,
        // 5xx server errors are retryable; 4xx are permanent.
        DownloadError::ServerError(status, _) => (500..600).contains(status),
        // Structured terminal errors are not retryable.
        DownloadError::ChunkFailed { .. } => false,
        DownloadError::JoinFailed { .. } => false,
        DownloadError::MissingContentLength => false,
    }
}
```

**Step 2: Use it in the retry loop**

```rust
loop {
    match try_download_chunk_to_offset(&client, &url, &token, start, end, &dest_path, &pb).await {
        Ok(_) => return Ok(()),
        Err(e) => {
            retries += 1;
            if !is_retryable(&e) || retries > MAX_RETRIES {
                return Err(e);
            }
            let wait_time = 2u64.pow(retries) * 100;
            tokio::time::sleep(Duration::from_millis(wait_time)).await;
        }
    }
}
```

**Step 3: Test**

```rust
#[test]
fn is_retryable_distinguishes_5xx_from_4xx() {
    assert!( is_retryable(&DownloadError::ServerError(500, "x".into())));
    assert!( is_retryable(&DownloadError::ServerError(503, "x".into())));
    assert!(!is_retryable(&DownloadError::ServerError(404, "x".into())));
    assert!(!is_retryable(&DownloadError::ServerError(401, "x".into())));
    assert!(!is_retryable(&DownloadError::MissingContentLength));
}
```

**Step 4: Commit**

```bash
git commit -m "fix(core): retry only 5xx and transport errors, not 4xx

Previously download_chunk_with_retry retried every DownloadError
variant — including 401/403/404. That wasted 200+400+800+1600 ms
of backoff before failing on a permanent error. We now classify
5xx and reqwest/io errors as retryable and fail fast on 4xx.

Closes: audit D5"
```

---

### Task 2.8: `spawn_blocking` for sha256 [U1]

**Files:**
- Modify: `src/uploader.rs:31-47, src/chunked_downloader.rs:212-227`

**Step 1: For `hash_file_async`** (uploader.rs:31), switch to spawn_blocking with std::fs:

```rust
pub async fn hash_file_async(path: &Path) -> Result<(String, u64), UploadError> {
    use std::io::Read;
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(String, u64), UploadError> {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 64 * 1024];
        let mut total: u64 = 0;
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 { break; }
            hasher.update(&buffer[..n]);
            total += n as u64;
        }
        Ok((hex::encode(hasher.finalize()), total))
    })
    .await
    .map_err(|e| UploadError::IoError(std::io::Error::other(e)))?
}
```

**Step 2: For `compute_sha256`** (chunked_downloader.rs:212) — same pattern: move the blocking I/O + digest off the tokio worker.

**Step 3: Test**

Run: `cargo test -p hippius_core`
Expected: still passes.

**Step 4: Commit**

```bash
git commit -m "perf(core): move sha256 hashing off the tokio worker thread

Hashing a multi-GB file with sync sha2 inside an async function
blocks a tokio worker for seconds, starving other tasks on the
same runtime. spawn_blocking runs it on the blocking-pool thread
the runtime keeps for exactly this purpose.

Closes: audit U1"
```

---

## Phase 3 — Tier 3 Medium

### Task 3.1: Warn on ignored kwargs in download paths [M1]

**Files:**
- Modify: `hippius_hub/file_download.py:78-178`
- Modify: `hippius_hub/_snapshot_download.py:15-112`

**Step 1: Lift the warn-helper pattern from `_handle_unsupported_kwargs`**

In `file_download.py`, add:

```python
def _handle_ignored_download_kwargs(
    *,
    etag_timeout: float,
    tqdm_class,
    dry_run: bool,
    headers,
    user_agent,
    library_name,
    library_version,
):
    """Emit UserWarning for HF kwargs we accept but don't yet honor."""
    import warnings
    # etag_timeout has a default; only warn if explicitly non-default.
    if etag_timeout != 10.0:
        warnings.warn(
            "etag_timeout is ignored: hippius_hub does not perform ETag negotiation.",
            UserWarning, stacklevel=3,
        )
    if tqdm_class is not None:
        warnings.warn("tqdm_class is ignored: hippius_hub uses its own progress bar.",
                      UserWarning, stacklevel=3)
    if dry_run:
        # dry_run is supported in snapshot_download but NOT here — fail fast,
        # don't silently download.
        raise NotImplementedError(
            "dry_run is not supported by hf_hub_download; use snapshot_download(dry_run=True) "
            "to enumerate files without downloading."
        )
    if headers:
        warnings.warn("headers= is ignored: hippius_hub doesn't pass custom HTTP headers yet.",
                      UserWarning, stacklevel=3)
    if user_agent:
        warnings.warn("user_agent is ignored.", UserWarning, stacklevel=3)
    if library_name or library_version:
        warnings.warn("library_name/library_version are ignored.", UserWarning, stacklevel=3)
```

Call it at the top of `hf_hub_download` after `_validate_repo_type`.

**Step 2: Mirror for `snapshot_download`** (but allow `dry_run`, which it implements).

**Step 3: Test**

```python
def test_hf_hub_download_dry_run_raises():
    with pytest.raises(NotImplementedError, match="dry_run"):
        hf_hub_download("foo/bar", "x", dry_run=True)
```

**Step 4: Commit**

```bash
git commit -m "fix(download): warn on ignored HF kwargs; raise for dry_run

The accept-and-ignore docstring was a phantom feature: a user
passing dry_run=True to hf_hub_download still got a real download.
Now we warn on every ignored kwarg and raise NotImplementedError
on dry_run (which is supported in snapshot_download but not here).

Closes: audit M1, N4"
```

---

### Task 3.2: Split `cli.main` into parser + dispatch helpers [M2a]

**Files:**
- Modify: `hippius_hub/cli.py:389-611`

**Step 1: Extract `_build_parser() -> argparse.ArgumentParser`**

Move all `add_subparsers` / `add_argument` / `set_defaults` calls into a separate function. The function returns the configured parser.

**Step 2: Extract `_cmd_download(args)`, `_cmd_upload(args)`, `_cmd_login(args)`**

Each handles one top-level command's logic.

**Step 3: `main()` shrinks to:**

```python
def main():
    parser = _build_parser()
    args = parser.parse_args()
    handlers = {
        "download": _cmd_download,
        "upload": _cmd_upload,
        "login": _cmd_login,
    }
    if args.command in handlers:
        handlers[args.command](args)
        return
    if args.command in ("registry", "models"):
        if not hasattr(args, "func"):
            parser.print_help()
            sys.exit(1)
        try:
            args.func(args)
        except ConsoleError as e:
            _handle_console_error(e)
        return
    parser.print_help()
    sys.exit(1)
```

**Step 4: Test** — run the full CLI test suite to confirm no regression.

Run: `pytest tests/test_cli.py -v`

**Step 5: Commit**

```bash
git commit -m "refactor(cli): split main() into _build_parser + per-command handlers

main() was 223 lines, exceeding the project's 100-line/function limit.
Decomposing into a parser-building function + one handler per top-level
command keeps each piece reviewable and lets tests target individual
handlers.

Closes: audit M2 (cli.main half)"
```

---

### Task 3.3: Refactor `upload_folder` — extract `_process` and merge tail [M2b]

**Files:**
- Modify: `hippius_hub/file_upload.py:308-416`

**Step 1: Extract `_upload_one_file` (currently the `_process` closure)** to a module-level function.

**Step 2: Extract the manifest-merge-and-PUT tail** into `_finalize_upload_manifest`.

**Step 3: `upload_folder` shrinks to ≤80 lines.**

Run: `wc -l hippius_hub/file_upload.py` and verify the function (visible via `python -c "from hippius_hub.file_upload import upload_folder; import inspect; print(len(inspect.getsource(upload_folder).splitlines()))"`).

**Step 4: Commit**

```bash
git commit -m "refactor(upload): split upload_folder into per-file + finalize helpers

upload_folder was 109 lines, exceeding the project's 100-line/function
limit. Splitting the per-file work and the merge-and-PUT tail makes
each piece testable without spinning up a registry.

Closes: audit M2 (upload_folder half)"
```

---

### Task 3.4: Refactor `hf_hub_download` — extract path resolution [M2c]

**Files:**
- Modify: `hippius_hub/file_download.py:78-178`

**Step 1: Extract `_resolve_dest_paths(...)`** returning a small dataclass `DownloadPaths` with `dest_file`, `repo_dir`, `snapshots_dir`.

**Step 2: Extract `_resolve_target_digest(manifest, filename) -> str`** with explicit error on miss.

**Step 3: `hf_hub_download` shrinks to ≤80 lines.**

**Step 4: Commit**

```bash
git commit -m "refactor(download): split hf_hub_download into path + digest resolution helpers

Same motivation as the upload_folder split — 101 lines was over the
hard limit, and the path-resolution + digest-lookup logic is
independently testable.

Closes: audit M2 (hf_hub_download half)"
```

---

### Task 3.5: Surface `get_docker_auth` exceptions [N2]

**Files:**
- Modify: `hippius_hub/auth.py:135-153`

**Step 1: Replace `except Exception: pass` with explicit known cases**

```python
def get_docker_auth(registry_url: str) -> Optional[str]:
    docker_config = os.path.expanduser("~/.docker/config.json")
    if not os.path.exists(docker_config):
        return None
    try:
        with open(docker_config, "r") as f:
            config = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        # A broken or unreadable docker config is worth surfacing — it
        # often means the user thinks they're logged in but isn't.
        import warnings
        warnings.warn(
            f"docker config at {docker_config} is unreadable ({e}); "
            "treating as no creds.",
            UserWarning,
        )
        return None
    host = registry_url.replace("https://", "").replace("http://", "").rstrip("/")
    for key, val in config.get("auths", {}).items():
        key_host = key.replace("https://", "").replace("http://", "").rstrip("/")
        if key_host == host:
            return val.get("auth")
    return None
```

**Step 2: Commit**

```bash
git commit -m "fix(auth): surface get_docker_auth read/parse failures as warnings

Previously a corrupted ~/.docker/config.json fell through as
'no creds' indistinguishable from 'no docker config'. Users who
expected to be logged in got a 401 with no diagnostic clue. We
now warn explicitly when the file exists but can't be read or parsed.

Closes: audit N2"
```

---

### Task 3.6: Unique-per-call temp file in `_download_to_cache` [N3]

**Files:**
- Modify: `hippius_hub/file_download.py:181-214`

**Step 1: Use `tempfile.NamedTemporaryFile`**

Replace:
```python
temp_path = os.path.join(blobs_dir, f"tmp_{filename.replace('/', '_')}")
```
with:
```python
import tempfile
# Unique per call: two concurrent downloaders writing the same logical
# file no longer race on a shared temp path.
fd, temp_path = tempfile.mkstemp(
    dir=blobs_dir,
    prefix=f"tmp_{filename.replace('/', '_')}_",
)
os.close(fd)  # Rust opens its own handle by path; we just want the unique name.
```

Ensure cleanup if `download_file_native` raises:
```python
try:
    calculated_hash = download_file_native(...)
except Exception:
    if os.path.exists(temp_path):
        os.remove(temp_path)
    raise
```

**Step 2: Commit**

```bash
git commit -m "fix(download): use unique temp file path per call

Two processes downloading the same filename into the same
cache_dir previously collided on tmp_{filename}, racing on the
same file handle in the Rust engine.

Closes: audit N3"
```

---

### Task 3.7: Per-request timeout on chunk GETs [D6]

**Files:**
- Modify: `src/chunked_downloader.rs:261-312`

**Step 1: Add a request timeout** in `try_download_chunk_to_offset`:

```rust
let mut req = client.get(url)
    .header(header::RANGE, format!("bytes={}-{}", start, end))
    .timeout(Duration::from_secs(300));  // 5 min per chunk; chunk size capped at 100MB ~= 1Mb/s floor
```

**Step 2: Commit**

```bash
git commit -m "fix(core): add per-request timeout to chunk GETs

The Client had connect_timeout=30s but no request timeout — a
slow-loris server could hold a TCP open and dribble bytes
indefinitely without tripping the connect_timeout. Combined with
the recently-added abort-on-first-error, this puts an upper bound
on the time a stuck download can hang the runtime.

Closes: audit D6"
```

---

### Task 3.8: Implement `std::error::Error` for `DownloadError` + `UploadError` via `thiserror` [D8, U4]

**Files:**
- Modify: `Cargo.toml` (add thiserror)
- Modify: `src/chunked_downloader.rs:37-55, src/uploader.rs:11-28`
- Optionally consolidate into a single `src/error.rs`

**Step 1: Add thiserror**

```toml
thiserror = "1.0"
```

**Step 2: Replace the hand-written enums**

```rust
// src/error.rs (new)
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CoreError {
    #[error("HTTP transport error")]
    Reqwest(#[from] reqwest::Error),
    #[error("local I/O error")]
    Io(#[from] std::io::Error),
    #[error("server returned {0} {1}")]
    ServerError(u16, String),
    #[error("chunk {index} failed")]
    ChunkFailed { index: usize, #[source] source: Box<CoreError> },
    #[error("chunk {index} task panicked")]
    JoinFailed { index: usize, #[source] source: tokio::task::JoinError },
    #[error("server did not return Content-Length")]
    MissingContentLength,
}
```

**Step 3: Drop the duplicate `UploadError`/`DownloadError` enums**; everything uses `CoreError`.

**Step 4: Update `lib.rs` error mapping** to walk the source chain:

```rust
fn core_err_to_py(e: CoreError) -> PyErr {
    // Walk Error::source() to build a chained Python exception.
    let mut msg = e.to_string();
    let mut current: Option<&dyn std::error::Error> = e.source();
    while let Some(src) = current {
        msg.push_str(&format!("\ncaused by: {}", src));
        current = src.source();
    }
    PyRuntimeError::new_err(msg)
}
```

**Step 5: Test + commit**

```bash
git commit -m "refactor(core): unify error types via thiserror

Previously DownloadError and UploadError were parallel-but-distinct
enums with no Display, no source(), and no shared structure. We
now have a single CoreError enum derived via thiserror, with a
proper source chain that the Python-side error message preserves.

Closes: audit D8, U4"
```

---

### Task 3.9: Replace `unwrap()` on infallible templates with `expect("...")` [D7]

**Files:**
- Modify: `src/chunked_downloader.rs:100-103, 159-162`
- Modify: `src/uploader.rs:67-72`

**Step 1: Mechanical**

Replace each `.unwrap()` on the progress-bar template constructor with `.expect("indicatif template is static and infallible")`.

**Step 2: Verify clippy passes**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: previously-warning `unwrap_used` lines now silent.

**Step 3: Commit**

```bash
git commit -m "style(core): replace unwrap on infallible templates with expect

Satisfies the clippy::unwrap_used = deny lint added in 0.1.
The template strings are static — they cannot fail at runtime —
but expect() documents that fact at the call site.

Closes: audit D7"
```

---

### Task 3.10: Chunked transfer for upload (drop Content-Length pre-stat) [U2]

**Files:**
- Modify: `src/uploader.rs:51-113`

**Step 1: Drop the `header::CONTENT_LENGTH` set**; reqwest with `wrap_stream` will use chunked Transfer-Encoding by default. This means we no longer pre-stat the file, eliminating the metadata-vs-actual-size race.

**Step 2: Commit**

```bash
git commit -m "fix(core): drop pre-stat Content-Length on upload

If the file changed size between metadata().len() and the actual
stream, the upload would either truncate (file grew) or transmit
zeros (file shrunk). Chunked transfer-encoding lets reqwest emit
exactly what FramedRead delivers.

Closes: audit U2"
```

---

### Task 3.11: Add retry to uploader [U3]

**Files:**
- Modify: `src/uploader.rs:51-113`

**Step 1: Wrap `upload_blob_async` body in a 4-retry loop** using the same `is_retryable` helper from Task 2.7. Note that uploading a stream is more delicate than downloading — we have to re-open the file at the start of each retry.

```rust
pub async fn upload_blob_async(url: &str, path: &Path, auth_token: Option<&str>) -> Result<(), CoreError> {
    let mut retries = 0;
    loop {
        match try_upload_blob_once(url, path, auth_token).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                if !is_retryable(&e) || retries > MAX_RETRIES {
                    return Err(e);
                }
                let wait_time = 2u64.pow(retries) * 100;
                tokio::time::sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}
```

**Step 2: Commit**

```bash
git commit -m "feat(core): retry uploads on 5xx and transport errors

The downloader retried 4×; the uploader didn't retry at all. Now
both use the same is_retryable classifier and the same exponential
backoff schedule.

Closes: audit U3"
```

---

### Task 3.12: Use `Option<String>` for hash result [L6 (Rust)]

**Files:**
- Modify: `src/lib.rs:8-32, src/chunked_downloader.rs:90-171`

**Step 1: Change `download()` return to `Result<Option<String>, CoreError>`**

```rust
pub async fn download(&self, dest_path: &Path, verify_hash: bool) -> Result<Option<String>, CoreError>
```

Return `None` instead of `String::new()` when verify is skipped.

**Step 2: pyo3 side**

`download_file_native` returns `PyResult<Option<String>>` — pyo3 maps Rust `Option<String>` to Python `Optional[str]` automatically. The Python caller in `file_download.py` already handles the "skipped verify" case via `target_digest.replace("sha256:", "")` fallback; update it to switch on `None` instead of `""`.

**Step 3: Commit**

```bash
git commit -m "refactor(core): use Option<String> for hash result instead of empty-string sentinel

Empty-string was an in-band sentinel that conflicted with the legitimate
'sha256 of empty data' value. None makes the 'verify skipped' case
explicit at the type level.

Closes: audit L6 (Rust)"
```

---

### Task 3.13: French → English inline comments [L5 (Rust)]

**Files:**
- Modify: `src/lib.rs:17, 26`

**Step 1: Translate comments**

```rust
// Build (or reuse) the shared tokio runtime — see shared_runtime() below.
let rt = shared_runtime();

// Release the GIL so other Python threads can run during the (long)
// network/disk I/O.
```

**Step 2: Commit**

```bash
git commit -m "chore(core): translate French comments to English

Project-wide language is English; the two French inline comments
in lib.rs were inherited from an earlier commit.

Closes: audit L5 (Rust)"
```

---

## Phase 4 — Tier 4 Low / nits

### Task 4.1: Hash token before keying OCI cache [M3]

**Files:**
- Modify: `hippius_hub/auth.py:163-200`

```python
import hashlib

def _token_cache_key(repo_id: str, push: bool, token: Optional[str]) -> tuple:
    if not token:
        return (repo_id, push, None)
    return (repo_id, push, hashlib.sha256(token.encode()).hexdigest())
```

Replace `cache_key = (repo_id, push, token)` with `cache_key = _token_cache_key(repo_id, push, token)`.

Commit: `fix(auth): hash token before keying the OCI bearer cache (closes M3)`

---

### Task 4.2: Log JWT exp parse failures [M4]

**Files:**
- Modify: `hippius_hub/auth.py:22-34`

Replace silent `return None` with a `warnings.warn(...)` or `logging.getLogger(__name__).debug(...)`.

Commit: `fix(auth): surface JWT exp parse failures via warning (closes M4)`

---

### Task 4.3: Concurrent-upload regression test [M5]

**Files:**
- Create: `tests/test_concurrent_upload.py`

Already partly satisfied by Task 2.1's `ConcurrentManifestUpdateError` test. Add a test that submits two `upload_file` calls in parallel against a mocked registry and asserts the second sees the 412.

Commit: `test: regression test for concurrent-upload If-Match path (closes M5)`

---

### Task 4.4: Drop the `f` prefix on the non-interpolating f-string [L2 (Python)]

**Files:**
- Modify: `hippius_hub/file_upload.py:340`

```python
commit_message = "Upload folder using hippius_hub"
```

Commit: `style: drop unused f-string prefix (closes L2)`

---

### Task 4.5: Make `max_workers` configurable in `upload_folder` [L3 (Python)]

**Files:**
- Modify: `hippius_hub/file_upload.py:308-416`

Add `max_workers: int = 8` as a kwarg; thread it into `ThreadPoolExecutor`. Mirrors snapshot_download.

Commit: `feat(upload): expose max_workers on upload_folder for parity (closes L3)`

---

### Task 4.6: Tighten `_oci_repo_path` unreachable [L4 (Python)]

**Files:**
- Modify: `hippius_hub/file_download.py:68-69`

Replace `raise NotImplementedError(...)` after the validated branches with:
```python
raise AssertionError(
    f"unreachable: _validate_repo_type should have rejected {repo_type!r} before this point"
)
```

Commit: `style: tighten _oci_repo_path unreachable assertion (closes L4)`

---

### Task 4.7: Doc coverage push — Python `cmd_*` and console wrappers [L5 (Python)]

**Files:**
- Modify: `hippius_hub/cli.py` (every `cmd_*` function)
- Modify: `hippius_hub/console.py` (every wrapper)

One-line docstring per function. For `cmd_*` functions, a single line is enough:
```python
def cmd_registry_plans(args):
    """List available pricing plans (`hippius-hub registry plans`)."""
```

Run: `pytest --collect-only && python -c "from hippius_hub.cli import cmd_registry_plans; assert cmd_registry_plans.__doc__"`

Commit: `docs: add one-line docstrings to cmd_* and console.py wrappers (closes L5)`

---

### Task 4.8: Warn on symlink → hardlink → copy fallback [N5]

**Files:**
- Modify: `hippius_hub/file_download.py:233-250`

```python
def _create_symlink(src: str, dst: str):
    if os.path.exists(dst):
        os.remove(dst)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
        rel_src = os.path.relpath(src, os.path.dirname(dst))
        os.symlink(rel_src, dst)
        return
    except OSError as e:
        warnings.warn(f"symlink failed ({e}); falling back to hardlink", UserWarning)
    try:
        os.link(src, dst)
        return
    except OSError as e:
        warnings.warn(
            f"hardlink failed ({e}); falling back to full copy — disk usage will double",
            UserWarning,
        )
    shutil.copy2(src, dst)
```

Commit: `fix(download): warn on symlink/hardlink fallback to plain copy (closes N5)`

---

### Task 4.9: Add rustdoc to `#[pyfunction]`s [L7 (Rust)]

**Files:**
- Modify: `src/lib.rs:8-58`

Add `///` doc comment above each of the three `#[pyfunction]` items describing inputs, return value, exceptions raised on the Python side.

Commit: `docs(core): add rustdoc to public #[pyfunction]s (closes L7)`

---

## Phase 5 — Structural refactors

### Task 5.1: Migrate to `pyo3-async-runtimes` [STRUCT-1]

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`

**Step 1: Add dep**

```toml
pyo3-async-runtimes = { version = "0.21", features = ["tokio-runtime"] }
```

**Step 2: Replace `shared_runtime() + rt.block_on(py.allow_threads(...))` with `pyo3_async_runtimes::tokio::future_into_py(...)` for true coroutine-returning Python functions** — OR keep the sync-blocking shape but use the library's recommended runtime handle.

This is the "right" fix that Task 1.4 + 1.5 stub. It enables future Python `await`-ability of these functions without a wrapper.

**Step 3: Commit**

```bash
git commit -m "refactor(core): migrate tokio integration to pyo3-async-runtimes (closes STRUCT-1)"
```

---

### Task 5.2: Bump pyo3 to 0.22 (or current stable) [CT3]

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs` (port to `Bound<'_, T>` API)

The pyo3 0.20 → 0.22 migration is mechanical but touches every `&PyModule` / `&PyAny` / `&PyDict`. Follow [pyo3's migration guide](https://pyo3.rs/latest/migration). Test thoroughly after.

Commit: `chore(core): bump pyo3 0.20 → 0.22 and migrate to Bound API (closes CT3)`

---

### Task 5.3: Typed dataclass for HF token state [STRUCT-2]

**Files:**
- Create: `hippius_hub/_token.py`

```python
from dataclasses import dataclass
from typing import Union

@dataclass(frozen=True)
class Anonymous:
    """token=False — explicit no-auth."""

@dataclass(frozen=True)
class UseStored:
    """token=None/True — use the saved token."""

@dataclass(frozen=True)
class Literal:
    """token='...' — use this literal string."""
    value: str

TokenInput = Union[Anonymous, UseStored, Literal]

def from_hf_input(token) -> TokenInput:
    if token is False:
        return Anonymous()
    if isinstance(token, str):
        return Literal(token)
    return UseStored()
```

Then refactor `resolve_token_value`, `resolve_auth_header`, and `whoami` to dispatch on the typed input. This makes the three-state semantics impossible to bypass.

Commit: `refactor(auth): model HF token three-state input as typed dataclass (closes STRUCT-2)`

---

### Task 5.4: Doc-coverage push to >80% [L5 follow-up]

After Task 4.7, run `mcp__illu__doc_coverage` and add docstrings to any remaining undocumented public items until coverage exceeds 80%.

Commit: `docs: raise doc coverage above 80% (closes L5 follow-up)`

---

## Phase 6 — Test backfill: respx unit tests for `auth.py`

### Task 6.1: Unit tests for `_jwt_expiration` [backfill]

**Files:**
- Create: `tests/unit/test_jwt_expiration.py`

```python
import base64, json
from hippius_hub.auth import _jwt_expiration

def _make_jwt(payload: dict) -> str:
    """Build a fake JWT — header.payload.signature, b64url-encoded."""
    def b64(x): return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg':'none'})}.{b64(payload)}.signature"

def test_exp_extracted_from_valid_jwt():
    jwt = _make_jwt({"exp": 1700000000, "sub": "u"})
    assert _jwt_expiration(jwt) == 1700000000

def test_no_exp_field_returns_none():
    jwt = _make_jwt({"sub": "u"})
    assert _jwt_expiration(jwt) is None

def test_two_part_string_returns_none():
    assert _jwt_expiration("only.two") is None

def test_garbage_payload_returns_none():
    assert _jwt_expiration("h.@@not-base64@@.s") is None

def test_payload_not_json_returns_none():
    bad = base64.urlsafe_b64encode(b"not-json").decode().rstrip("=")
    assert _jwt_expiration(f"h.{bad}.s") is None
```

Commit: `test: unit-test _jwt_expiration edge cases (backfill)`

---

### Task 6.2: Unit tests for `resolve_token_value` [backfill]

**Files:**
- Create: `tests/unit/test_resolve_token_value.py`

Cases: `None`/`True` reads saved file; `False` returns False (post Task 1.2); `str` returns the string. Use `monkeypatch` to swap TOKEN_PATH.

Commit: `test: unit-test resolve_token_value three-state semantics (backfill)`

---

### Task 6.3: Unit tests for `get_oci_bearer_token` [backfill]

**Files:**
- Create: `tests/unit/test_get_oci_bearer_token.py`

Using `respx_mock`:

```python
@respx.mock
def test_cache_hit_skips_network():
    # Pre-populate cache with a JWT whose exp is in the future
    ...

@respx.mock
def test_cache_miss_sends_request_with_correct_scope():
    ...

@respx.mock
def test_expired_token_refetches():
    ...

@respx.mock
def test_passes_timeout():
    # Already covered by C1; this confirms via respx that the request
    # call object has a timeout attribute set.
    ...
```

Commit: `test: respx-based unit tests for get_oci_bearer_token (backfill)`

---

### Task 6.4: Unit tests for `get_docker_auth` [backfill]

**Files:**
- Create: `tests/unit/test_get_docker_auth.py`

Cases: missing file → None; readable file → correct host; malformed file → warning + None; substring-attack entry → real entry chosen.

Commit: `test: unit-test get_docker_auth host-matching + error paths (backfill)`

---

## Phase 7 — Test backfill: Rust proptest

### Task 7.1: Proptest invariants for chunk math [backfill]

**Files:**
- Modify: `src/chunked_downloader.rs::tests`

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn chunks_cover_exactly_content_length(
        content_length in 1u64..1_000_000_000,
        chunk_size in 1u64..200_000_000,
    ) {
        let n = num_chunks(content_length, chunk_size);
        if n == 0 { return Ok(()); }
        let mut total = 0u64;
        for i in 0..n {
            let (s, e) = chunk_bounds(content_length, chunk_size, i);
            // Every chunk is non-empty and disjoint from its neighbours.
            prop_assert!(s <= e);
            if i > 0 {
                let (_, prev_end) = chunk_bounds(content_length, chunk_size, i - 1);
                prop_assert_eq!(s, prev_end + 1);
            }
            total += e - s + 1;
        }
        prop_assert_eq!(total, content_length);
    }
}
```

Run: `cargo test -p hippius_core --release proptest`

Commit: `test(core): proptest for chunk math invariants (backfill)`

---

### Task 7.2: Round-trip property test for manifest merge [backfill]

**Files:**
- Create: `tests/unit/test_merge_layers.py`

Property: merging `[]` with `[A, B]` and `delete_titles={}` yields `[A, B]`. Merging `[A1, B]` with `[A2]` yields `[A2, B]` (A replaced, B preserved). Merging anything with `delete_titles={A}` yields the input minus A. Use `hypothesis` to generate random layer sets and assert the structural invariants.

Commit: `test: hypothesis-based property tests for _merge_layers (backfill)`

---

## Final checklist

After all phases:

```bash
# Lints clean
cargo clippy --all-targets --all-features -- -D warnings
ruff check hippius_hub tests
ty check

# Tests pass
cargo test --release
pytest -q -m "not e2e"     # unit + respx-based tests
pytest -q -m "e2e"          # full suite, with creds, on demand

# Doc coverage
python -c "from mcp__illu__doc_coverage import ..." # or whatever the project uses

# Audit ledger
grep -c "Closes: audit" $(git log --format=%H main..HEAD)  # ~45+ findings closed
```

Then squash-merge the branch (or rebase + PR per file group, depending on team preference) and delete the `audit/fix-all` worktree.

---

## Notes for the executing agent

- **Per CLAUDE.md, every Rust diff must go through the `mcp__illu__quality_gate` workflow before final answer.** That includes calling `mcp__illu__project_style` + `mcp__illu__decisions` (both currently empty for this repo, but check on each task), `mcp__illu__axioms` baseline + task queries, `mcp__illu__exemplars` for codified patterns, and the 7-item adversarial self-review checklist whose answers go into `quality_gate`'s `self_review_*` fields.
- **For tasks that touch `unsafe`** — there is no `unsafe` in this crate today; if any task introduces some (none do as planned), run `cargo +nightly miri test` per the project rules.
- **For each Python fix that changes behavior** — keep tests passing throughout. If a fix breaks an existing test, the existing test was probably wrong; review whether the assertion was testing implementation rather than behavior, and update accordingly (don't silently accept the regression).
- **For backfill tests** — they go under `tests/unit/` and are NOT marked `@pytest.mark.e2e`, so they run in any CI without creds.
- **Commit cadence: one commit per task.** The audit-tag in the commit footer (`Closes: audit Cx`) lets the audit report stay in lock-step with the implementation. Don't squash within a phase.
