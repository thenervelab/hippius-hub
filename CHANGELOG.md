# Changelog

All notable changes to `hippius_hub` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`hippius-hub delete <project>/<repo>` CLI command.** Deletes a repository and
  all its revisions through Harbor's native repository DELETE (the same path as
  the `delete_repo` library function) — the only delete that removes the
  repository record, not just its artifacts. Prompts for confirmation before
  deleting; `-y`/`--yes` skips the prompt for scripts/CI, `--missing-ok` exits 0
  when the repository is already gone. Previously repo deletion was only reachable
  as a Python API call, so users scripting against the registry console endpoint
  were left with empty, artifact-less repositories.

### Fixed

- **`hippius-hub delete` now returns actionable exit codes for HTTP failures.**
  `delete_repo` reaches Harbor over httpx and raises `httpx.HTTPStatusError`
  directly, which the CLI's error mapper did not recognize — every delete HTTP
  failure collapsed to the generic exit 1. A 403 (the token lacks
  push-delete/admin) now maps to access-denied (14) and a 404 (no such repo) to
  not-found (11), matching the codes their huggingface_hub-typed siblings get.
  A missing local credential now raises `LocalTokenNotFoundError` (as
  `create_repo` already did) and the CLI reports "not logged in" (14) instead of
  the misleading "repository not found" (11) the previous `RepositoryNotFoundError`
  produced.

## [0.6.0] — 2026-07-13

### Changed (behavioral default — read before upgrading producers)

- **Chunked-v2 writes are now ON by default** (`HIPPIUS_CHUNKED_WRITE`, was
  opt-in). A large file (≥ `HIPPIUS_CHUNK_THRESHOLD`) uploaded by a 0.6.0 client
  is stored in the chunked-v2 layout. **Consumers must be on ≥ 0.6.0 to read it**
  — an older client (≤ v0.5.1) has no layout guard and silently writes the
  pointer blob as the file. Upgrade readers to ≥ 0.6.0 before pushing large
  files, or set `HIPPIUS_CHUNKED_WRITE=0` to keep the pre-chunking single-blob
  layout. Small files and every pre-existing artifact are unchanged.
- **Whole-file hash verification is now ON by default** (`HIPPIUS_VERIFY_HASH`).
  The plain/Range download path now verifies the downloaded bytes against the
  content-addressed digest before caching them, matching the chunked path (which
  always verified). Set `HIPPIUS_VERIFY_HASH=0` to restore transport-only checks.
- Both boolean gates above now **reject an unrecognized value** (e.g.
  `HIPPIUS_CHUNKED_WRITE=enabled`) with a `ValueError` instead of silently
  falling back to a default — a typo on a layout-changing gate surfaces
  immediately. Accepted spellings: `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`.

### Added

- **Resumable plain-blob uploads.** A large file below the chunk threshold now
  streams to the registry in bounded OCI `PATCH` chunks (`HIPPIUS_UPLOAD_CHUNK_SIZE`,
  default 16 MiB); on any transient failure the client `GET`s the registry's
  committed offset and resumes from there, so a mid-upload disconnect costs at most
  one chunk of re-send instead of the whole layer. Falls back to the monolithic
  streaming `PUT` if a registry rejects `PATCH` (405/501); the chunked-v2 pack path
  is unchanged.
- **Chunked-v2 (pack) layout for large files.** Files at or above
  `HIPPIUS_CHUNK_THRESHOLD` (256 MiB) are stored as content-defined chunks
  (FastCDC, ~4 MiB average) packed into ~64 MiB content-addressed *pack* blobs —
  a titled `pointer.v2` layer (mapping each chunk to its pack, offset, and size)
  plus the untitled pack blobs it references, typed with `artifactType` and a
  `com.hippius.layout: chunked-v2` annotation. A re-uploaded slightly-changed
  model references unchanged chunks by range into existing packs and uploads only
  the packs holding new chunks; downloads fetch each pack once and slice its
  chunks to their file offsets. Concurrent pack uploads are bounded across all
  files by a shared cap so folder uploads don't multiply resident memory. Small
  files and every pre-existing artifact are unchanged (one plain blob).
- New Rust extension functions: `chunk_and_hash_native`, `pack_upload_native`,
  `download_packs_native`.
- New env vars: `HIPPIUS_CHUNK_THRESHOLD`, `HIPPIUS_CDC_AVG_SIZE`,
  `HIPPIUS_PACK_SIZE`, `HIPPIUS_MAX_INFLIGHT_PACKS`, `HIPPIUS_BLOB_REUPLOAD_RETRIES`,
  and `HIPPIUS_CHUNKED_WRITE` (default on; set `0` to keep the single-blob layout
  for large files).
- Forward-compatibility guard: a manifest with an unknown `com.hippius.layout`
  is refused with `UnsupportedLayoutError` (new) instead of misread. Malformed
  chunked manifests raise `MalformedManifestError` (new).

### Removed

- The in-cluster staged blob **receiver** (`receiver/` crate,
  `Dockerfile.receiver`, `deploy/receiver/`) and the client multipart upload
  route it fronted (`upload_blob_multipart_native`, `HIPPIUS_RECEIVER_URL`,
  `HIPPIUS_MULTIPART_*`, the `diagnose-upload` CLI and its upload throughput
  probe). Chunking pushes chunk blobs straight to Harbor, so the receiver is
  superseded. The download `diagnose` command and its probe are unchanged.

### Changed

- **Uploads no longer carry a fixed 1-hour total-request timeout.** A large
  upload on a slow link was aborted at the 1h mark and — because a timeout is
  retryable — re-streamed from the start up to the retry budget (~4h of dead
  transfer). A dead or stalled peer is now detected by connect-timeout + TCP
  keepalive instead, without ever capping an honest transfer.
- **`HIPPIUS_VERIFY_HASH=1` now raises on a mismatched plain (non-chunked)
  download** (`OSError`, matching huggingface_hub) instead of silently caching
  the content. Previously the computed digest was only used to name the cache
  blob and was never compared to the manifest digest, so the check was a no-op
  on the primary (non-chunked) workload.
- The legacy Range download shares a process-global in-flight cap, so a snapshot
  of many large files opens at most ~32 concurrent registry connections rather
  than `snapshot_workers × 32`.
- `HIPPIUS_READ_TIMEOUT` is now applied to the `diagnose` transfer probe (it was
  accepted and displayed but never enforced).
- Transport retry backoff is now jittered (was a deterministic `2^n · 100ms`), so
  concurrent chunk/pack/upload retries under a registry `429`/`503` no longer
  collide in lockstep.

### Fixed

- **Chunked-v2 upload self-heals a blob the registry silently lost.** When a
  manifest PUT keeps failing `MANIFEST_BLOB_UNKNOWN` after the commit-visibility
  retry budget is spent, the referenced blob is durably gone — a registry-side GC
  reaped an untagged blob, or a commit that returned `2xx` never landed under
  storage pressure — not merely lagging, so re-awaiting it can't help. Both
  `upload_file` and `upload_folder` now re-run the whole upload (re-PUTting every
  referenced blob) up to `HIPPIUS_BLOB_REUPLOAD_RETRIES` (default 2) times before
  surfacing the new typed `ManifestBlobUnknownError`, instead of an opaque
  `HTTPStatusError` they retried in vain.
- **Connection & transport hardening.** A top-to-bottom audit of the Rust data
  plane and Python control plane:
  - Uploads now abort a stalled body write: a peer that completes TCP+TLS then
    stops draining the socket (invisible to the connect timeout and TCP
    keep-alive) is cut by an idle-progress watchdog and retried, instead of
    hanging forever and wedging a whole folder upload. The watchdog covers both
    the whole-file PUT and the chunked-write pack PUT, keys "body fully sent" off
    the stream reaching end-of-input (so a file rewritten mid-upload can't
    false-trip it), and the pack upload-init POST is bounded too.
  - A `206`'s `Content-Range` is validated to cover exactly the requested bytes,
    so a range-aliasing proxy can no longer silently write mis-placed bytes into
    a file cached under the correct-looking digest.
  - A whole-file `200 OK` to a single-chunk small-file download (the range covers
    the entire object) is now accepted as RFC-legal instead of rejected; a
    multi-chunk range-ignored `200` is still refused.
  - Transient control-plane failures (manifest fetch, token fetch — `408`/`429`/
    `5xx` and connection blips) are retried with jittered backoff like the data
    plane, instead of aborting the whole operation on one registry hiccup.
  - Retry backoff jitter no longer collapses to `0` on microsecond-resolution
    clocks (which re-created the lockstep retry storm it exists to prevent).
  - `snapshot_download` and `upload_folder` fail fast — the first error (or
    Ctrl-C) cancels queued transfers instead of draining the whole repo first.
  - `HIPPIUS_CONNECT_TIMEOUT` / `HIPPIUS_READ_TIMEOUT` now reach real transfers,
    not only `diagnose`; setting `HIPPIUS_READ_TIMEOUT` opts real downloads into
    per-read stall detection.
  - `diagnose` bounds DNS resolution and tries every resolved address (a dead
    first IPv6 no longer produces a false-negative), and its token fetch honors
    `--endpoint`.
  - `models list`/`show` no longer crash on a null `format` field; the Harbor
    whoami/create/delete admin calls use the same 30s timeout as their siblings;
    a wedged `docker login` is bounded to 60s; a folder of only-small files no
    longer builds the chunked-v2 dedup index it never consults.
  - The pack fetch is bounded against an over-sending server; the upload hash
    uses an 8 MiB read buffer (~128× fewer read syscalls); a dead per-download
    `fsync` of the pre-allocation was removed.
- **Connection & transport hardening — follow-up.** The deferred cluster from the
  same audit:
  - Native downloads and uploads are now interruptible by Ctrl-C: a `SIGINT`
    during a long transfer is checked ~10×/s and cancels the in-flight work
    instead of hanging until the transfer finishes.
  - A bearer token that expires mid-operation on a long transfer is refreshed and
    the upload/download retried once, instead of failing on the `401`.
  - Each download body read has a default-on 30s idle timeout (overridable by
    `HIPPIUS_READ_TIMEOUT`), so a peer that dribbles then stalls mid-body is cut
    promptly rather than only after the 5-minute per-chunk total timeout.
  - The plain blob upload re-initiates its OCI upload session on every retry, so a
    transient failure no longer re-PUTs a session the failed attempt consumed.
  - The legacy Range downloader bounds its live chunk tasks to a spawn window
    (drain-as-they-land) instead of eager-spawning one task per chunk; the pack
    verify/scatter and pack-body hashing moved off the async runtime onto the
    blocking pool.
- A `206 Partial Content` whose body is shorter — or longer — than the requested
  byte range is now rejected and retried instead of being written as a
  truncated/mis-sized chunk and cached under the correct-looking digest. An
  over-length body is bounded so its surplus cannot corrupt the adjacent chunk.
- `list_repo_refs` (and `HfApi(endpoint=...).list_repo_refs`) minted its auth
  token against the default registry, returning `401` on every custom endpoint;
  it now honors `endpoint=`.
- The upload client now sets a connect-timeout (a stalled handshake was
  previously unbounded).
- `diagnose` no longer hangs indefinitely on a stalled server (read-timeout plus
  a bounded raw connect), and one failed parallel range no longer discards the
  whole report. The size-probe HEAD also gained a request timeout.
- A missing upload-init `Location` header now raises a clear error instead of a
  `TypeError`.
- A non-numeric JWT `exp` claim is rejected instead of poisoning the OCI token
  cache with a persistent `TypeError`.
- `HippiusApi` no longer drops the constructor token when a call passes
  `token=None` explicitly.
- An interrupted cache download (`KeyboardInterrupt`/`SystemExit`) no longer
  leaves a temp blob behind.
- A pointer referencing a pack absent from the manifest raises
  `MalformedManifestError` instead of a bare `KeyError`.

## [0.5.0] — 2026-05-27

A consolidation release that lands the 45-finding security/correctness audit
remediation, plus four rounds of post-review hardening (race fixes, supply-chain
gates, clippy enforcement, ~400 LOC of new respx coverage for `console.py` and
`_harbor.py`).

The version jumps from `0.4.x` to `0.5.0` because this release contains breaking
changes — most are intentional security/correctness fixes, but downstream users
should expect to update calling code. See **Migration** below.

### Breaking changes

#### Critical (silent break — code may keep running but produce wrong results)

- **`download_file_native` (Rust extension) return type: `str` → `Optional[str]`.**
  - Where: `src/lib.rs:88-119` and `src/chunked_downloader.rs:download()`.
  - Was: returned `""` as an in-band sentinel for "verify skipped"; a 64-hex
    digest otherwise.
  - Is: returns `None` for verify-skipped, a 64-hex digest otherwise. The
    sentinel is now in the type, not the value.
  - Migration: callers consuming the function directly must dispatch on
    `is None` instead of equality with `""`. Example:
    ```python
    # Before
    digest = download_file_native(...)
    if digest == "":          # skipped
        digest = fallback_digest
    # After
    digest = download_file_native(...)
    if digest is None:        # skipped
        digest = fallback_digest
    ```
  - The shipped Python wrappers (`hf_hub_download`, `snapshot_download`,
    `_download_to_cache`) are already updated.

- **`auth.resolve_token_value(token)` semantics on `False`: `None` → `False`.**
  - Where: `hippius_hub/auth.py:128-152`.
  - Was: any `False` input was collapsed to `None`.
  - Is: `False` is forwarded verbatim so downstream paths can distinguish
    "no caller preference" (`None`, may consult docker config) from "explicit
    anonymous" (`False`, must NOT consult docker config).
  - Migration: callers comparing the return value with `is None` should treat
    both `None` and `False` as "no value":
    ```python
    value = resolve_token_value(token)
    if value is None or value is False:
        ...
    ```
  - This is what `resolve_auth_header` does internally
    (`hippius_hub/auth.py:165`).

#### Major (visible break)

- **CLI exit codes: `1` everywhere → typed `10`–`18`.**
  - Where: `hippius_hub/cli.py:_format_download_error` and the registry/models
    handlers.
  - New code table:
    | Code | Meaning                               |
    | ---- | ------------------------------------- |
    | `1`  | generic failure (unknown exception)   |
    | `2`  | argparse usage error (unchanged)      |
    | `10` | file not found in repo                |
    | `11` | repository not found                  |
    | `12` | revision not found                    |
    | `13` | local cache miss                      |
    | `14` | access denied (gated/disabled repo)   |
    | `15` | concurrent manifest write (412)       |
    | `16` | registry HTTP error (fallthrough)     |
    | `17` | registry namespace not available      |
    | `18` | malformed `<project>/<repo>` argument |
  - Migration: shell wrappers branching on `$?` should switch from
    `if [ $? -eq 1 ]` (catches everything) to specific codes. Scripts checking
    `-eq 2` for "namespace taken" or "bad repo format" must move to `17` / `18`.

- **`hf_hub_download(..., dry_run=True)` now raises `NotImplementedError`.**
  - Where: `hippius_hub/file_download.py:_handle_ignored_download_kwargs`.
  - Was: kwarg silently ignored; the full download proceeded anyway,
    defeating the caller's intent.
  - Is: raises immediately with a message pointing at `snapshot_download(...)`.
  - Migration: pass `dry_run=True` to `snapshot_download` instead, or drop the
    kwarg from `hf_hub_download` calls.

- **`snapshot_download(..., dry_run=True)` is now a true I/O short-circuit.**
  - Where: `hippius_hub/_snapshot_download.py`.
  - Was: still fetched the manifest (network round-trip) and applied
    `allow_patterns` / `ignore_patterns` filters against it before returning
    the snapshot directory.
  - Is: returns the snapshot directory path BEFORE any HTTP call (no token
    service, no manifest GET). `allow_patterns` / `ignore_patterns` are NOT
    applied under `dry_run`.
  - Migration: callers that need the would-be-downloaded filename list should
    invoke `snapshot_download` twice — once with `dry_run=True` to get the
    target directory, once without to enumerate files.

- **Concurrent manifest PUTs raise `ConcurrentManifestUpdateError` (HTTP 412).**
  - Where: `hippius_hub/file_upload.py:_put_manifest`,
    `hippius_hub/errors.py:ConcurrentManifestUpdateError` (new).
  - Was: silent last-writer-wins. Two writers racing on the same
    `repo_id:revision` would both succeed; the second silently dropped the
    first's layer.
  - Is: writers send `If-Match: <prior-manifest-digest>` on every PUT. A
    racing writer that advanced the manifest first causes the second's PUT to
    return 412, which surfaces as `ConcurrentManifestUpdateError` (a subclass
    of `HfHubHTTPError`, so existing `except HfHubHTTPError:` handlers still
    catch it).
  - Migration: callers can either accept the existing `HfHubHTTPError` catch,
    or specifically catch `ConcurrentManifestUpdateError` for retry-with-merge
    logic.

- **`hippius_hub._oci.fetch_manifest` return type: `Optional[dict]` →
  `Optional[ManifestResult]`.**
  - Where: `hippius_hub/_oci.py`.
  - Migration: callers must use `result.manifest` to access the dict body
    and `result.digest` for the `Docker-Content-Digest` header. The module
    is underscore-private; only internal callers should be affected.

- **`requires-python` bumped `>=3.8` → `>=3.10`.**
  - Where: `pyproject.toml`.
  - Driven by the `huggingface_hub>=1.0,<2.0` dependency floor. Users on 3.8
    or 3.9 must upgrade their interpreter.

- **`pyo3` `0.20` → `0.22`.**
  - Where: `Cargo.toml`.
  - Affects Rust crate consumers of `hippius_core` (not Python wheel users,
    which are insulated by the abi3 contract). The migration is the standard
    pyo3 0.22 `Bound<'_, T>` API — see pyo3's own changelog.

- **`reqwest` `0.11` → `0.12`.**
  - Where: `Cargo.toml`.
  - Closes RUSTSEC-2026-0104 (rustls-webpki CRL panic, transitively via
    reqwest 0.11's older rustls). Affects Rust crate consumers that read
    hippius_core's re-exported types.

#### Minor (edge-case-only)

- **`_create_symlink` cache materialization now emits `UserWarning` on each
  fallback step** (symlink failed → hardlink failed → copy). The final
  on-disk state is unchanged when at least one method succeeds; users with
  `-W error` filters will see exceptions where they previously got working
  copies. Pinned by `tests/test_create_symlink_warns.py`.

- **`auth.login` and `console.save_api_token` now write tokens via an atomic
  rename pattern** (`tempfile.mkstemp` + `os.fchmod(0o600)` + `os.replace`)
  instead of write-then-chmod. The on-disk format is unchanged, but the
  inode changes on every save — anyone monitoring the token file via
  `inotify`/`fsevents` sees one rename event instead of an open-write-chmod
  sequence. The new atomic path closes a microsecond-scale window where the
  file was world-readable.

- **`auth.get_docker_auth(registry_url)` host matching: substring → exact.**
  - Was: `if host in key:` (could match `registry.hippius.com` against a
    docker config entry for `registry.hippius.com.evil.example`).
  - Is: `if key_host == host:`.
  - Behavior is the same in practice for any real registry URL; a docker
    config entry that previously matched substring-only will silently stop
    matching. Security improvement; flag for completeness.

- **`upload_folder(..., max_workers=8)` parameter added** (default unchanged
  for existing callers).

- **CLI no longer strips whitespace from interactively-entered passwords.**
  Fixes a silent-corruption bug for passwords with trailing whitespace.

### Added

- **OCI `If-Match` header on manifest PUT** (`hippius_hub/file_upload.py`) —
  the registry-side closure of the audit H1 finding.
- **`ConcurrentManifestUpdateError`** — typed exception in `hippius_hub.errors`.
- **`TokenInput` typed dispatch** (`hippius_hub/_token.py`) — HF's three-state
  `token=None|True|False|str` argument is now resolved through a tagged-union
  dataclass instead of scattered `isinstance` / `is False` checks.
- **`upload_folder(max_workers=...)`** — caller-controllable parallelism on
  folder uploads.
- **45 Rust unit tests** (was 18) — chunk-math proptests, error-chain shape,
  retry classifier, shared-runtime singleton, 206-Partial-Content guard,
  AbortHandle semantics.
- **Behavioral end-to-end tests** for the Rust extension via real localhost
  HTTP servers: `test_chunked_download_partial_content.py` (D2),
  `test_chunked_download_abort.py` (D4), `test_uploader_retry.py` (U3),
  `test_download_verify_skip.py` (L6).
- **149+ Python tests** (was ~70) covering token-cache key separation,
  anonymous downloads, concurrent uploads, dry-run short-circuit, atomic
  token writes, atomic symlink replacement, the full CLI exit-code matrix
  via subprocess, and the previously-untested `console.py` (28 functions,
  38 tests) and `_harbor.py` (10 functions, 21 tests).
- **CI supply-chain gates**:
  - `cargo deny check` (advisories + licenses + bans + sources) — see
    `deny.toml` for the policy.
  - `pip-audit --strict` — Python CVE check.
  - `cargo clippy --all-targets --all-features -- -D warnings` — promotes
    the existing `[lints.clippy]` deny set (unwrap_used, panic, print_stdout,
    print_stderr, etc.) from documentation to enforcement.
  - **Dependabot** weekly grouped updates for `github-actions`, `pip`, and
    `cargo` ecosystems with a 7-day cooldown.
- **Workflow hardening**: SHA-pinned actions (not tags), least-privilege
  `permissions: contents: read`, `concurrency` group preventing CI-on-CI
  contention on `test/e2e-client`.

### Fixed

- **C1**: `auth.get_oci_bearer_token` now passes `timeout=DEFAULT_HTTP_TIMEOUT`
  on the token-service GET (previously no timeout → potential indefinite
  hang on a slow registry).
- **C2**: `token=False` is now honored as the HF anonymous sentinel
  end-to-end; `~/.docker/config.json` fallback is correctly gated.
- **C3**: Token file is now written via atomic-rename so it is never
  observable at a mode other than `0o600`. The previous write-then-chmod
  window has been closed.
- **D2**: `chunked_downloader.rs` now rejects HTTP 200 OK as a response to a
  `Range` request — without this, a broken proxy or registry that ignored
  the Range header could silently corrupt files.
- **D4**: Failed chunk requests now abort all in-flight sibling chunks via
  `AbortHandle`; the previous `buffer_unordered` early-return left
  background tasks running.
- **D6**: Per-chunk request timeout (5 minutes) added; closes the
  slow-loris hang vector where a fast handshake then dribbled bytes could
  hold a connection open indefinitely without tripping `connect_timeout`.
- **D8**: Local `DownloadError` / `UploadError` enums unified into a single
  `thiserror`-derived `CoreError` with `#[non_exhaustive]` and `#[source]`
  chains; Python now sees the full `caused by:` tail instead of a flattened
  Debug string.
- **H1**: Concurrent uploads to the same `repo_id:revision` no longer
  silently clobber each other; covered above under Breaking changes.
- **H2**: CLI `login` no longer `.strip()`s tokens with embedded whitespace.
- **L6**: `download_file_native` no longer uses `""` as an in-band "skipped
  verify" sentinel; covered above under Breaking changes.
- **M3**: OCI bearer-token cache key now hashes the token value, so two
  users hitting the same repo with different tokens get distinct cache
  entries (previously the second user got the first user's JWT).
- **M4**: Malformed JWT payloads now emit a typed `UserWarning` instead of
  silently bypassing the cache. Non-object payloads (`null`, `42`, `[]`)
  no longer crash with `AttributeError`.
- **STRUCT-1**: All Rust async calls now share a single tokio runtime
  (`pyo3_async_runtimes::tokio::get_runtime`) instead of building one per
  call; closes a thread-leak vector.
- **`_create_symlink` TOCTOU**: `if exists(): remove() ... symlink()` race
  replaced with atomic-rename pattern. Eight-thread concurrent test in
  `tests/test_create_symlink_atomic.py` pins the fix.

### Security

- **RUSTSEC-2026-0104** (rustls-webpki CRL panic) closed via the reqwest
  0.12 upgrade. Was not exploitable in our code path (we don't parse CRLs),
  but the dependency is gone now.
- **`auth.login` write-then-chmod TOCTOU** closed; token file is never
  observable at a non-`0o600` mode.
- **`_create_symlink` TOCTOU** closed; concurrent downloaders no longer
  race on the snapshot symlink.
- **OCI token cache key** now hashes the token before use; raw bearer JWTs
  no longer appear in the in-memory cache as part of the key.

### Documented deferrals

- **RUSTSEC-2025-0020** (pyo3 0.22 `PyString::from_object`): ignored in
  `deny.toml` because we don't call the affected function. Remove when
  upgrading pyo3 to ≥0.24.1.
- **RUSTSEC-2025-0119** (`number_prefix` unmaintained): transitive via
  `indicatif`; no safe upgrade until upstream migrates to `unit-prefix`.

---

## [0.4.7] and earlier

Pre-audit history; see `git log`.
