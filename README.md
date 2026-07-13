# hippius_hub

Drop-in replacement for [`huggingface_hub`](https://github.com/huggingface/huggingface_hub) backed by an OCI registry (`registry.hippius.com` by default). Same Python API as the official client — `from hippius_hub import hf_hub_download` works where `from huggingface_hub import hf_hub_download` worked — with byte movement done by a Rust extension.

The CLI also wraps the Hippius console API: register a namespace, manage docker credentials, browse repositories, and search the AI model index without leaving the terminal.

> **AI agents / coding assistants**: a self-contained reference (install, auth, CLI surface, Python API, workflows, what raises `NotImplementedError`) lives at [`llms.txt`](./llms.txt) — point your agent at it instead of this README.

## Quickstart

```bash
# 1. Install
pip install hippius_hub

# 2. Get your API token at https://console.hippius.com/dashboard/settings
hippius-hub login --hippius-token <paste-token-here>

# 3. Provision a namespace + docker login (one shot)
hippius-hub registry provision my-models --docker-login

# 4. You're set — push, pull, search.
hippius-hub upload my-models/qwen-7b ./checkpoints/v1
hippius-hub download my-models/qwen-7b model.safetensors
hippius-hub models list --mine
```

## Install

```bash
pip install hippius_hub

hippius-hub --version   # confirm the install
hippius-hub --help      # discover commands
```

Or from source. `hippius_hub` ships a Rust extension (`hippius_core`) — published wheels include a pre-built binary for your platform, but `pip install git+…` or `maturin develop` will compile it locally and needs a working Rust toolchain.

**Prerequisite — install Rust via `rustup`** (not Homebrew). `rustup` ships the correct stdlib for your host triple; Homebrew's `rust` formula installs to the prefix it was built for, so on Apple Silicon an Intel-prefix (`/usr/local`) Homebrew rust will fail to build for `aarch64-apple-darwin` with `error[E0463]: can't find crate for 'core'`.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustc --version   # should print your host triple, e.g. aarch64-apple-darwin
```

Then either:

```bash
# A) Install the latest main directly
pip install "git+https://github.com/thenervelab/hippius-hub.git@main"

# B) Editable dev install
git clone https://github.com/thenervelab/hippius-hub
cd hippius-hub
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release
```

## Authenticate

There are **two kinds of credentials**, depending on what you're doing:

| For… | Use | Where |
|---|---|---|
| `registry` and `models` CLI commands (manage your namespace, list models, …) | **API token** from [console.hippius.com/dashboard/settings](https://console.hippius.com/dashboard/settings) | `hippius-hub login --hippius-token <token>` → `~/.cache/hippius/hub/api_token` |
| `download` / `upload` (raw OCI registry IO) | Docker registry credentials | `hippius-hub login --username <you> --password <secret>` → `~/.cache/hippius/hub/token` |

In Python:

```python
from hippius_hub import login
login(token="hf_xxx")                  # HF-shape: positional token (docker registry)
login(username="me", password="pwd")   # Basic auth (docker registry)
```

In practice the API token is all you save by hand. `hippius-hub registry provision <namespace>` mints the docker credentials *and* writes them into `~/.cache/hippius/hub/token` for you, so the next `upload`/`download` works without a second `hippius-hub login` step. Pass `--docker-login` to also run `docker login` so `docker push`/`pull` work; either way, hippius-hub's own auth is persisted.

The robot secret is printed **once** on first provision. If you lose it, rotate with `hippius-hub registry rotate-token` (also re-writes the local cache).

## Onboard from the terminal (no UI required)

```bash
# 1. Save your API token (grab it at https://console.hippius.com/dashboard/settings)
hippius-hub login --hippius-token <token>

# 2. See what plans exist and what your namespace name should look like
hippius-hub registry plans
hippius-hub registry check my-models

# 3. Create your namespace + optionally run `docker login` in one shot
hippius-hub registry provision my-models --docker-login

# 4. Push an image
docker tag my-image registry.hippius.com/my-models/qwen-7b:v1
docker push registry.hippius.com/my-models/qwen-7b:v1

# 5. Inspect what's in your namespace
hippius-hub registry repos
hippius-hub registry artifacts qwen-7b
hippius-hub registry usage
hippius-hub registry me
```

The full `registry` sub-tree:

| Command | Purpose |
|---|---|
| `registry plans` | List pricing tiers and quotas |
| `registry check <name>` | Is a namespace available? |
| `registry provision <ns> [--docker-login]` | Create your namespace and get docker credentials. New projects are public by default; toggle with `registry publicity`. |
| `registry status` | Poll while provisioning is in flight |
| `registry me` | Plan, quota, status, and robot login of your active project |
| `registry rotate-token [--docker-login]` | Issue a new docker secret (old one stops working immediately) |
| `registry repos [--page N --page-size M]` | List your repositories |
| `registry artifacts <repo> [--page N --page-size M]` | List artifacts in one repo |
| `registry usage` | Storage used + 7-day history |
| `registry publicity public\|private` | Toggle anonymous-pull access (also resizes quota to the plan's public/private tier) |
| `registry subscribe <plan> [--pay-upfront N]` | Subscribe to a plan on-chain. `<plan>` is the name (e.g. `Builder`) or numeric id. Debits your own credits — backend is just the whitelisted relayer. |
| `registry subscriptions` | List your current subscriptions (synced from chain every ~3 min) |
| `registry unsubscribe <sub-id>` | Cancel a subscription by its on-chain `SubscriptionId`. 30-day grace before the project is hard-deleted; re-subscribe within that window to keep everything. |

## Search the AI model index

Every artifact pushed to the registry is parsed server-side (GGUF, safetensors, ONNX, Diffusers) and indexed by format / architecture / parameter count / quantization. The `models` sub-tree exposes that index:

| Command | Purpose |
|---|---|
| `models list [filters] [--json]` | Search across all public models + your own. Filters: `--format`, `--arch`, `--quant`, `--min-params`, `--max-params`, `-q <text>`, `--mine`, `--page`, `--page-size` |
| `models show <project>/<repo>` | All indexed versions of a repo |
| `models show <project>/<repo> <tag-or-digest>` | One version with per-file breakdown + `docker pull` command |
| `models formats` | Available filter values (formats, architectures, quantizations) |

```bash
hippius-hub models list --format gguf --arch llama --max-params 8000000000
hippius-hub models show my-models/qwen-7b              # all versions of a repo
hippius-hub models show my-models/qwen-7b v1           # one version, with file breakdown
hippius-hub models list --mine                          # restrict to your own
hippius-hub models formats                              # available filter values
```

Add `--json` to `models list` / `models show` for machine-readable output.

## Push a model from the CLI

```bash
# Upload an entire model folder (every file under ./qwen-7b/) as `:v1`.
# Folder uploads merge into the existing manifest at that revision — re-running
# adds/replaces individual files without wiping the rest.
hippius-hub upload myorg/qwen-7b ./qwen-7b --revision v1

# Upload a single file (e.g. add a README to an existing revision)
hippius-hub upload myorg/qwen-7b ./README.md --revision v1

# Tag a folder as the default `:main` revision
hippius-hub upload myorg/qwen-7b ./qwen-7b
```

Once the push completes, the Harbor webhook fires the model index pipeline — the model shows up in `hippius-hub models list` within a few seconds with format/architecture/parameter-count/quantization parsed out of the bytes server-side.

### Mirroring a HuggingFace model to your namespace

```bash
# 1. Grab the model from HF (uses huggingface_hub's CLI)
pip install -U "huggingface_hub[cli]" hf_transfer
HF_HUB_ENABLE_HF_TRANSFER=1 hf download Qwen/Qwen2.5-7B-Instruct --local-dir ./qwen-7b

# 2. Push the whole folder under your namespace as :v1
hippius-hub upload myorg/qwen-7b ./qwen-7b --revision v1

# 3. Confirm it landed + got indexed
hippius-hub registry repos
hippius-hub models show myorg/qwen-7b v1
```

## Pull a model from the CLI

```bash
# One file — uses the parallel Rust downloader, picks up auth from
# ~/.cache/hippius/hub/token (set by `provision --docker-login` or `login`).
hippius-hub download myorg/qwen-7b model-00001-of-00003.safetensors

# Specific revision (= OCI tag)
hippius-hub download myorg/qwen-7b config.json --revision v1

# List a repo's revisions, newest first (the newest is marked "(latest)")
hippius-hub revisions myorg/qwen-7b

# Verify the SHA256 of the bytes after download
hippius-hub download myorg/qwen-7b model.safetensors --verify-hash
```

Omitting `--revision` downloads `main`, which always points to the most recently uploaded content (uploads default to `main` too) — so the default already gives you the latest, just like `huggingface_hub`. Use `hippius-hub revisions <repo>` to discover the available revisions and which one is newest.

For pulling **every file in a revision**, prefer the Python `snapshot_download` API below — it parallelizes across files and matches the HF cache layout that `transformers` / `diffusers` expect.

Optional flags: `--revision <tag>`, `--chunk-size <bytes>` (defaults to `HIPPIUS_CHUNK_SIZE`), `--verify-hash` (SHA256 after download), `--cache-dir <path>`.

## Quick start: download a model

```python
from hippius_hub import hf_hub_download

# Pull one file
path = hf_hub_download(
    repo_id="myorg/my-model",
    filename="config.json",
    revision="main",
)
print(path)  # ~/.cache/hippius/hub/models--myorg--my-model/snapshots/main/config.json
```

Cache layout mirrors `huggingface_hub` exactly, so `transformers` / `diffusers` / `datasets` reading from the same directory Just Works:

```python
from transformers import AutoConfig
import hippius_hub as huggingface_hub  # drop-in swap
config = AutoConfig.from_pretrained("myorg/my-model")
```

## Snapshot download — entire repo with pattern filters

```python
from hippius_hub import snapshot_download

local_dir = snapshot_download(
    repo_id="myorg/my-model",
    revision="v1.2",
    allow_patterns=["*.safetensors", "*.json"],
    ignore_patterns="optimizer*",
    max_workers=8,
)
```

## Upload a file or folder

```python
from hippius_hub import upload_file, upload_folder

# Single file. path_or_fileobj also accepts bytes or BinaryIO.
upload_file(
    path_or_fileobj="./model.safetensors",
    path_in_repo="model.safetensors",
    repo_id="myorg/my-model",
    revision="main",
    commit_message="Initial checkpoint",
)

# Folder, with pattern filters and delete semantics
upload_folder(
    folder_path="./outputs/checkpoint-1000",
    repo_id="myorg/my-model",
    revision="main",
    allow_patterns=["*.safetensors", "*.json"],
    delete_patterns="*.tmp",   # prune any *.tmp from the existing revision
)
```

`upload_file` and `upload_folder` **merge** into the existing manifest at `revision` — calling them repeatedly adds/replaces individual files without wiping the rest.

## Repo CRUD & inspection

```python
from hippius_hub import (
    create_repo, delete_repo,
    repo_info, model_info, list_repo_files, list_repo_refs,
    repo_exists, revision_exists, file_exists,
)

create_repo("myorg/my-model", exist_ok=True)
print(list_repo_files("myorg/my-model", revision="main"))
info = model_info("myorg/my-model", revision="main")
print(info.id, info.sha, [s.rfilename for s in info.siblings])

# Discover available revisions (HF-compatible GitRefs).
refs = list_repo_refs("myorg/my-model")
print([b.name for b in refs.branches])  # ['main']
print([t.name for t in refs.tags])      # ['v1', 'v1.2', ...]
```

## Object-oriented API: `HippiusApi`

```python
from hippius_hub import HippiusApi

api = HippiusApi(token="hf_xxx")
api.hf_hub_download("myorg/my-model", "config.json")
api.upload_file(
    path_or_fileobj="./README.md",
    path_in_repo="README.md",
    repo_id="myorg/my-model",
)
api.whoami()
```

`HippiusApi` subclasses `huggingface_hub.HfApi`, so `isinstance(api, HfApi)` is True. Methods we don't implement (Inference Endpoints, Spaces, Webhooks, Collections, Discussions) raise `NotImplementedError` with a clear "HF-specific" message — they never silently hit huggingface.co.

## Errors are HF's typed exceptions

```python
from hippius_hub.errors import RepositoryNotFoundError, RevisionNotFoundError, EntryNotFoundError
from huggingface_hub.errors import RepositoryNotFoundError as HFRepositoryNotFoundError

# They're the same class — re-exported verbatim
assert RepositoryNotFoundError is HFRepositoryNotFoundError
```

Existing code that catches HF's exceptions keeps working.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `HIPPIUS_CHUNK_SIZE` | `104857600` (100 MiB) | Per-chunk size for the parallel Rust downloader |
| `HIPPIUS_VERIFY_HASH` | on | Whole-file SHA256 verification of downloads before caching. Set `0`/`false` to skip on the plain/Range path (the chunked-v2 path always verifies) |
| `HIPPIUS_MAX_CONCURRENT` | `32` | Parallel connections per file download |
| `HIPPIUS_CONNECT_TIMEOUT` | `30` | TCP connect timeout (seconds) |
| `HIPPIUS_READ_TIMEOUT` | unset | Opt-in per-chunk total request timeout (seconds) |
| `HIPPIUS_SNAPSHOT_WORKERS` | `8` | Concurrent files in `snapshot_download` |
| `HIPPIUS_UPLOAD_WORKERS` | `8` | Concurrent files in a folder upload / concurrent chunk uploads per large file |
| `HIPPIUS_CHUNK_THRESHOLD` | `268435456` (256 MiB) | Files at or above this size upload as content-defined chunks; below it, one plain blob |
| `HIPPIUS_CDC_AVG_SIZE` | `4194304` (4 MiB) | FastCDC average chunk size — 4 MiB is fastcdc's max (larger is rejected); part of the layout wire contract |
| `HIPPIUS_PACK_SIZE` | `67108864` (64 MiB) | Target size of a content-addressed pack blob (many CDC chunks per pack) |
| `HIPPIUS_MAX_INFLIGHT_PACKS` | `8` | Process-wide cap on concurrent pack uploads (bounds resident memory during folder uploads) |
| `HIPPIUS_BLOB_REUPLOAD_RETRIES` | `2` | Extra whole-upload retries when the registry reports a just-committed blob as missing (`BLOB_UNKNOWN`) |
| `HIPPIUS_CHUNKED_WRITE` | on | Set `0`/`false` to store large files in the pre-chunking single-blob layout. Default on as of 0.6.0 — a reader must be ≥ 0.6.0 to read a chunked artifact |
| `HIPPIUS_DEBUG` / `RUST_LOG` | off | Verbose transport logging (per-chunk timings, retries) |
| `HIPPIUS_API_URL` | `https://api.hippius.com` | Console API base used by the `registry` + `models` CLI subtrees |
| `HIPPIUS_TEST_REPO` | `test/e2e-client` | Override the test repo used by the e2e suite |

Programmatic overrides via the `endpoint=` kwarg on any function let you point at an alternative Hippius registry.

### Large-file chunking

Files at or above `HIPPIUS_CHUNK_THRESHOLD` (256 MiB) are stored as **content-defined chunks** (FastCDC, ~4 MiB average) packed into ~64 MiB content-addressed **pack** blobs (`HIPPIUS_PACK_SIZE`) rather than one blob. The layout is a Git-LFS-style pointer: one titled `pointer.v2` layer per file — mapping each chunk to its pack, offset, and size — plus the untitled pack blobs it references, marked with `artifactType` and a `com.hippius.layout: chunked-v2` annotation. A re-uploaded, slightly-changed model references unchanged chunks by range into existing packs and uploads only the packs holding new chunks; downloads fetch each pack once (concurrently) and slice its chunks to their file offsets. Packing into ~64 MiB blobs cuts per-file upload round-trips versus one-blob-per-chunk, and a shared cap (`HIPPIUS_MAX_INFLIGHT_PACKS`) bounds concurrent pack uploads so folder uploads don't multiply resident memory. Small files and every pre-existing artifact are unchanged (one plain blob), so nothing already stored is rewritten.

Chunked **writes are on by default as of 0.6.0** (`HIPPIUS_CHUNKED_WRITE`). The reader-side guard ships from 0.6.0: a 0.6.0+ client refuses an unknown layout loudly (`UnsupportedLayoutError`, with an upgrade hint) instead of misreading it, and reads a chunked-v2 artifact correctly. An already-released client (≤ v0.5.1) has no guard, so it silently writes the pointer blob as the file — **every consumer of large files must be on ≥ 0.6.0** before you push them. Set `HIPPIUS_CHUNKED_WRITE=0` to fall back to the byte-identical single-blob layout while a consumer is still on an older client.

### Diagnosing slow transfers

If downloads/uploads are slow for some users but not others, run the built-in probe and share the report:

```bash
hippius-hub diagnose <repo_id> <filename>            # phased report + plain-English verdict
hippius-hub diagnose <repo_id> <filename> --verbose  # add per-chunk transport logs
hippius-hub diagnose <repo_id> <filename> --json     # machine-readable
```

It measures the endpoint handshake (DNS/TCP/TLS), the auth + metadata round-trips, and — the key signal — single-connection vs parallel-connection throughput, then tells you whether a slow link is being mitigated by parallelism or is bottlenecked elsewhere. See [`docs/diagnosing-speed.md`](docs/diagnosing-speed.md) for the full triage runbook.

## What's not supported

`hippius_hub` aims to be drop-in for the *download / upload / repo CRUD* surface of `huggingface_hub`. HF-specific features that have no equivalent in an OCI registry raise `NotImplementedError`:

- Inference Endpoints (`create_inference_endpoint`, etc.)
- Spaces (`request_space_hardware`, `enable_space_dev_mode`, etc.)
- Webhooks
- Collections
- Discussions / PRs
- HF-typed git refs like `refs/pr/3` — only OCI tags are supported as revisions. `list_repo_refs` reports the `main` revision under `branches` and every other revision under `tags`, with each `target_commit` set to the revision's manifest digest (resolved best-effort).

Also known semantic divergences:

- `model_info` fills `id`, `sha`, `lastModified`, `siblings`, `private`. Fields with no OCI-registry analog (`pipeline_tag`, `library_name`, `tags`, `downloads`, `likes`) are `None`.
- `hf_hub_url` returns the OCI manifest URL — usable for inspection but not a direct CDN download URL like HF's.
- Concurrent `upload_file` calls to the same `repo_id:revision` are protected by an `If-Match` header on the manifest PUT: the first writer wins, the second sees `ConcurrentManifestUpdateError` (subclass of `HfHubHTTPError`) and can retry on a fresh baseline. If the registry omits `Docker-Content-Digest` on the manifest GET (RECOMMENDED-but-not-REQUIRED per OCI Distribution Spec §4.4.1), the PUT proceeds without `If-Match` and a `UserWarning` is emitted so the unprotected write is grep-able in logs.

## Development

```bash
# Build the Rust extension into the venv
maturin develop --release

# Install test deps
pip install -e ".[test]"

# Fast tests (no creds, no network)
pytest

# Full e2e against registry.hippius.com (needs HIPPIUS_TEST_USER / HIPPIUS_TEST_PASS)
HIPPIUS_TEST_USER='...' HIPPIUS_TEST_PASS='...' pytest -m e2e -v

# Drop-in parity tests (also needs network to huggingface.co)
HIPPIUS_TEST_USER='...' HIPPIUS_TEST_PASS='...' pytest -m hf_parity -v
```

The CI workflow (`.github/workflows/e2e.yml`) runs fast tests on every PR, the Hippius e2e suite on every PR with credentials, and the full HF-parity nightly.

## CI secrets

The e2e workflow consumes three repository secrets. The `creds` fixture in `tests/conftest.py:34-45` accepts either the USER+PASS pair (Basic Auth) OR the TOKEN (Bearer) path; if both env vars are empty the `_have_creds()` check returns False and every `@pytest.mark.e2e` test skips cleanly. This is deliberate so PRs from **forks** — which never receive secrets under the `pull_request` trigger by GitHub's default — see the offline suite pass and the live suite skip, rather than a confusing fail.

| Secret name           | Status      | Purpose |
|-----------------------|-------------|---------|
| `HIPPIUS_TEST_USER`   | recommended | Username for Basic Auth against registry.hippius.com. Paired with HIPPIUS_TEST_PASS. |
| `HIPPIUS_TEST_PASS`   | recommended | Password / docker robot secret paired with HIPPIUS_TEST_USER. |
| `HIPPIUS_TEST_TOKEN`  | optional    | Bearer token alternative. Useful when rotating to a role-scoped robot via `hippius-hub registry keys create --role push`. |

Set them in repo settings under **Settings → Secrets and variables → Actions → Repository secrets**, or via the CLI:

```bash
gh secret set HIPPIUS_TEST_USER -b 'robot$your-project+ci'
gh secret set HIPPIUS_TEST_PASS    # interactive prompt; value won't appear in shell history
gh secret set HIPPIUS_TEST_TOKEN   # only if using the Bearer path
```

**Scope the test credentials to `test/e2e-client` only — never to a production namespace.** That keeps the blast radius of any leak limited to test data. For finer-grained scope, create a role-scoped robot:

```bash
# On the workstation where you're already logged in:
hippius-hub registry keys create ci-e2e --role push --expires-days 90
# Use the printed login/secret as HIPPIUS_TEST_USER / HIPPIUS_TEST_PASS.
```

The test namespace defaults to `test/e2e-client` (overridable via `HIPPIUS_TEST_REPO`).

GitHub Actions auto-masks secret values in job logs (they appear as `***`), but a malicious test that explicitly `print()`s a credential could still leak it via the streamed log. Review test changes accordingly.
