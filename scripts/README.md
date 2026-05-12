# `bench.py` — hippius_hub vs huggingface_hub throughput

Portable benchmark that times a single-file download with both clients from the same host and prints a per-run + cache-warmth + cross-client comparison table.

## TL;DR

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # → hippius_hub==0.3.0 + its deps

export HIPPIUS_TEST_USER='robot$...'
export HIPPIUS_TEST_PASS='...'

.venv/bin/python bench.py --seed --size 5    # first run, uploads blob then benches
.venv/bin/python bench.py --size 5           # subsequent runs (blob already on registry)
```

## What it does, phase by phase

The script runs sequentially through these phases:

### 1. Auth (`hippius_hub.auth.login`)

Reads `HIPPIUS_TEST_USER` + `HIPPIUS_TEST_PASS` (or `HIPPIUS_TEST_TOKEN`) from env and writes the resulting auth header to an **ephemeral token file** (`tempfile.mkdtemp(...)`) so the bench doesn't clobber any `~/.cache/hippius/hub/token` the host might already have. The temp dir is deleted in `main()`'s `finally:`.

### 2. (Optional) Seed — only if `--seed` is passed

Uploads a synthetic blob to the Hippius registry so the bench has something to download. Three sub-steps:

1. **Generate `--size` GiB of bytes locally.** Done via `os.urandom(4 MiB)` repeated to fill the target size — high-entropy enough that the registry won't compress it on the wire, fast because we generate one block and `f.write(block[:n])` it in a loop.
2. **Upload via `hippius_hub.upload_file`.** Internally this:
   - Calls the Rust `hash_file_native` to SHA256 the file (returns hash + size).
   - `HEAD /v2/{repo}/blobs/sha256:{digest}` — skip-if-exists check.
   - `POST /v2/{repo}/blobs/uploads/` to initiate, gets a `Location` URL back.
   - Streams the file via Rust `upload_blob_native` (16-way parallel chunked PUT) and finishes with `?digest=sha256:...`.
   - Fetches the existing OCI manifest at the revision (or builds an empty one), merges the new layer in, `PUT /v2/{repo}/manifests/{revision}` with the updated manifest.
   - Returns a `huggingface_hub.CommitInfo` instance.

This step is **skipped on subsequent runs** — once `test/bench:{N}gb/model.bin` exists on the registry, the blob persists. Re-seed only when you want fresh bytes.

### 3. Benchmark loop

For each client (`huggingface_hub` first, then `hippius_hub`) the script runs `--runs` downloads (default 2) into a **fresh `cache_dir`** per run. After each download the cache dir is `shutil.rmtree`'d. The local-disk wipe matters: it means any speedup observed on the second pull is the **server's edge cache** (ATS in front of Harbor / CloudFront in front of HF), not OS page cache or local FS reuse.

#### huggingface_hub side

```python
from huggingface_hub import hf_hub_download as hf_hub_download_real

hf_hub_download_real(
    repo_id="microsoft/Phi-3-mini-4k-instruct",
    filename="model-00001-of-00002.safetensors",
    cache_dir=str(cache_dir),
)
```

HF 1.x uses `hf_xet` under the hood — HF's own Rust-based parallel-chunked downloader. The file is fetched from `huggingface.co` through CloudFront and lands in HF's standard cache layout (`{cache_dir}/models--microsoft--Phi-3-mini-4k-instruct/snapshots/{commit_sha}/model-00001-of-00002.safetensors` as a symlink into `blobs/`). We then `os.path.getsize(path)` to record the bytes pulled.

#### hippius_hub side

```python
from hippius_hub import hf_hub_download as hippius_download

hippius_download(
    repo_id="test/bench",
    filename="model.bin",
    revision="5gb",
    cache_dir=str(cache_dir),
)
```

Identical Python signature to HF's. Internally:

1. **Auth resolution** — pulls the saved Basic/Bearer auth header, exchanges it for an OCI bearer token via `registry.hippius.com/service/token?service=harbor-registry&scope=repository:test/bench:pull` (cached for the JWT's `exp` claim with 30s leeway).
2. **Manifest fetch** — `GET /v2/test/bench/manifests/5gb` returns the OCI manifest JSON. The script walks `manifest.layers[]` looking for the one with `annotations["org.opencontainers.image.title"] == "model.bin"`. That layer's `digest` is the blob's SHA256.
3. **Blob fetch** — calls the Rust `download_file_native(url, dest, token, chunk_size, verify_hash=False)`:
   - HEAD the blob URL to get `Content-Length`.
   - Pre-allocate the destination file at full size (`set_len`).
   - Spawn up to 16 concurrent `Range:`-request tasks, each streaming its slice **directly to the right offset** in the pre-allocated file (no temp files, no assembly phase).
   - Each chunk has up to 3 retries with exponential backoff.
4. **Cache placement** — Python moves the temp file to `{cache_dir}/models--test--bench/blobs/sha256:{digest}` and creates a symlink at `snapshots/5gb/model.bin` (HF's exact layout). Returns the symlink path.

The cache layout matching is intentional: `huggingface_hub.scan_cache_dir(cache_dir=hippius_cache)` parses it cleanly, and consumers like `transformers.AutoConfig.from_pretrained(...)` find files the same way they would for HF-downloaded ones.

### 4. Reporting

Three tables printed to stdout (with `print()`, no markdown for terminal viewing):

| Section | What it shows |
|---|---|
| **Per-run timings** | every individual download (cold + warm-1 + warm-2 …) with wall-clock seconds and MiB/s |
| **Cache warmth** | per client: cold MiB/s vs warm MiB/s, plus the speedup ratio. >1.1× means the server edge cache is hitting; ≈1.0× means either no caching or the first pull already hit a warm cache |
| **Warm-vs-warm** | hippius_hub vs huggingface_hub on the last run (fair apples-to-apples once both servers have warm caches) |

## CLI reference

```
--size SIZE                    Synthetic blob size in GiB (default: 5)
--seed                         Upload a fresh blob to Hippius before benchmarking
--runs RUNS                    Pulls per client (default: 2; set to 1 to disable warm observation)
--hf-repo HF_REPO              HF reference repo (default: microsoft/Phi-3-mini-4k-instruct)
--hf-file HF_FILE              HF reference file (default: model-00001-of-00002.safetensors, ~4.97 GB)
--hippius-repo HIPPIUS_REPO    Hippius repo_id (default: test/bench)
--hippius-revision HIPPIUS_REV Hippius revision (default: '{size}gb', e.g. '5gb')
--hippius-file HIPPIUS_FILE    Filename inside the Hippius revision (default: model.bin)
```

Env vars (auth — required):

```
HIPPIUS_TEST_USER + HIPPIUS_TEST_PASS    Harbor Basic auth (typical: a robot account)
HIPPIUS_TEST_TOKEN                       Alternative: a pre-minted Bearer token
```

## Functions called

| Phase | hippius_hub | huggingface_hub |
|---|---|---|
| Auth | `auth.login(username, password, token)` → writes `Bearer …`/`Basic …` to a temp token file | — |
| Seed | `upload_file(path_or_fileobj=..., path_in_repo=..., repo_id=..., revision=..., commit_message=...)` → `CommitInfo` | — |
| Download | `hf_hub_download(repo_id, filename, revision=, cache_dir=)` → str path | `hf_hub_download(repo_id, filename, cache_dir=)` → str path |
| Size check | `os.path.getsize(path)` | `os.path.getsize(path)` |

The `hippius_hub` calls are the exact same signatures `huggingface_hub` exposes — that's the drop-in claim. Pinned by `tests/test_drop_in_parity.py` in CI.

## Output interpretation guide

**Cold throughput** measures end-to-end pipeline performance on the first request: client CPU + local network out + ISP + transit + server CDN/proxy + origin storage.

**Warm throughput** measures the same thing minus origin storage — the request is served from the closest edge cache that holds the blob.

**Warm speedup** (`warm / cold`) tells you whether server-side caching is working:

- `> 1.1×` — the cache layer is doing its job.
- `≈ 1.0×` — either no cache is in path, or both pulls hit warm cache (e.g. a recent seed populated the cache before run 1).
- `< 1.0×` — first run was anomalously fast (cache had a recent hit from someone else), or the second pull hit a cold node in a multi-server pool.

**Warm-vs-warm cross-client ratio** is the fairest single number: both clients are running against their respective warm caches, so this isolates the difference between (a) the client's parallelism/protocol efficiency and (b) the registry's edge bandwidth. Note that this is *not* purely a client comparison — geo-routing differences between CloudFront edges and ATS POPs are folded in.

## Reproducibility

- The script writes nothing under `~/.cache/...` — token file and cache dirs are all under `tempfile.mkdtemp(...)`, cleaned in `finally:` blocks.
- The synthetic blob is non-deterministic per seed (`os.urandom`), but blob CONTENT doesn't affect download time since the registry serves it byte-for-byte. The size IS deterministic.
- Pinned dep: `hippius_hub==0.3.0` in `requirements.txt`. To re-pin to a different version, edit that line.

## Caveats

- **Single-machine measurement.** Network conditions, edge POP routing, and server load all vary minute-to-minute. For a defensible number, run 3-5 times and take the median.
- **Server bandwidth is in the measurement.** This is a *full pipeline* benchmark — not isolated client throughput. A faster client served by a slower CDN will look slow; a slower client served by a faster CDN will look fast.
- **First-pull-after-seed is already warm.** If you ran `--seed` right before benchmarking, the registry's ATS may have cached the blob during the PUT (write-through). Both pulls then hit warm cache and the warm-speedup will look ~1.0×. To see a true cold pull, wait long enough for the cache to evict (registry-dependent, often hours) or use a different revision.
- **Memory needs.** `hf_xet` (HF 1.x's Rust downloader) keeps working buffers in RAM. On a 1 GB-RAM VPS with no swap, a 5 GiB download will OOM-kill the process. Add ≥ 4 GiB swap, or run on a host with more RAM.
- **Disk needs.** Peak working set is one full file at a time (we wipe between runs). 5 GiB seed + 5 GiB download = 10 GiB. With `--seed` running concurrently with the first download phase: same, 10 GiB peak. Plan accordingly.
