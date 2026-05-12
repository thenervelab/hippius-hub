# hippius_hub

Drop-in replacement for [`huggingface_hub`](https://github.com/huggingface/huggingface_hub) backed by an OCI registry (`registry.hippius.com` by default). Same Python API as the official client — `from hippius_hub import hf_hub_download` works where `from huggingface_hub import hf_hub_download` worked — with byte movement done by a Rust extension.

## Install

```bash
pip install hippius_hub
```

Or from source (requires Rust + maturin):

```bash
git clone https://github.com/thenervelab/hippius-hub
cd hippius-hub
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release
```

## Authenticate

```bash
hippius-hub login --username <you> --password <secret>
# or
hippius-hub login --token <bearer-token>
```

The token is saved to `~/.cache/hippius/hub/token`. In Python:

```python
from hippius_hub import login
login(token="hf_xxx")                  # HF-shape: positional token
login(username="me", password="pwd")   # Hippius/Harbor Basic auth
```

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
    repo_info, model_info, list_repo_files,
    repo_exists, revision_exists, file_exists,
)

create_repo("myorg/my-model", exist_ok=True)
print(list_repo_files("myorg/my-model", revision="main"))
info = model_info("myorg/my-model", revision="main")
print(info.id, info.sha, [s.rfilename for s in info.siblings])
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
| `HIPPIUS_VERIFY_HASH` | unset (off) | Set to `1`/`true` to SHA256-verify downloads locally |
| `HIPPIUS_TEST_REPO` | `test/e2e-client` | Override the test repo used by the e2e suite |

Programmatic overrides via the `endpoint=` kwarg on any function let you point at an alternative Hippius registry.

## What's not supported

`hippius_hub` aims to be drop-in for the *download / upload / repo CRUD* surface of `huggingface_hub`. HF-specific features that have no equivalent in an OCI registry raise `NotImplementedError`:

- Inference Endpoints (`create_inference_endpoint`, etc.)
- Spaces (`request_space_hardware`, `enable_space_dev_mode`, etc.)
- Webhooks
- Collections
- Discussions / PRs
- HF-typed git refs like `refs/pr/3` — only OCI tags are supported as revisions

Also known semantic divergences:

- `model_info` fills `id`, `sha`, `lastModified`, `siblings`, `private`. Fields with no OCI/Harbor analog (`pipeline_tag`, `library_name`, `tags`, `downloads`, `likes`) are `None`.
- `hf_hub_url` returns the OCI manifest URL — usable for inspection but not a direct CDN download URL like HF's.
- Concurrent `upload_file` calls to the same `repo_id:revision` race on the manifest with no If-Match check (last writer wins). Serialize same-revision uploads externally.

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
