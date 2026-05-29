"""Drop-in parity suite — same test code parameterized over hippius_hub
and huggingface_hub.

Each test exercises a public API call against a `client` fixture that yields
either our module or HF's, alongside a context dict with a repo and a
known-present file. A test that passes for both backends is the strongest
possible evidence the drop-in contract holds for that API.

Only read-only operations live here — we can't push to arbitrary HF repos.
Hippius-only behavior (upload_file/upload_folder/create_repo/etc.) stays in
test_phase_b.py.

Marker: every test is `e2e` (the suite hits both registry.hippius.com and
huggingface.co). Local fast loop: `pytest -m "not e2e"`.
"""
import os
import shutil
import tempfile
import uuid
from pathlib import Path

import huggingface_hub
import pytest

import hippius_hub


HF_REFERENCE_REPO = "hf-internal-testing/tiny-random-gpt2"
HF_REFERENCE_REVISION = "main"
HF_REFERENCE_FILE = "config.json"


pytestmark = pytest.mark.e2e


def _hippius_creds_available() -> bool:
    return bool(os.environ.get("HIPPIUS_TEST_TOKEN")) or (
        bool(os.environ.get("HIPPIUS_TEST_USER")) and bool(os.environ.get("HIPPIUS_TEST_PASS"))
    )


@pytest.fixture(scope="session")
def parity_seed():
    """Upload `config.json` + `tokenizer.json` to the Hippius test repo at a
    fresh session revision and return the context dict. Returns None if
    Hippius credentials are unavailable — the HF parametrization can still run.

    Session-scoped: all parametrizations share one upload. Manages its own
    auth state via a session-temp TOKEN_PATH so it doesn't depend on the
    per-test `logged_in` fixture or pollute the user's saved token.
    """
    if not _hippius_creds_available():
        # This is a generator fixture (it `yield`s below), so it must yield
        # its "no seed" sentinel rather than `return None` — a bare return
        # finishes the generator without yielding, which pytest reports as
        # "parity_seed did not yield a value" (an ERROR, not a clean skip).
        # The `client` fixture turns this None into a `pytest.skip` for the
        # hippius parametrization while the hf parametrization still runs.
        yield None
        return

    from hippius_hub import auth, upload_folder

    test_repo = os.environ.get("HIPPIUS_TEST_REPO", "test/e2e-client")

    orig_token_path = auth.TOKEN_PATH
    sess_tok = tempfile.NamedTemporaryFile(delete=False, suffix=".token")
    sess_tok.close()
    auth.TOKEN_PATH = sess_tok.name
    auth.login(
        username=os.environ.get("HIPPIUS_TEST_USER"),
        password=os.environ.get("HIPPIUS_TEST_PASS"),
        token=os.environ.get("HIPPIUS_TEST_TOKEN"),
    )

    seed_dir = tempfile.mkdtemp(prefix="hippius-parity-")
    sd = Path(seed_dir)
    (sd / "config.json").write_text('{"model_type": "test", "vocab_size": 32}\n')
    (sd / "tokenizer.json").write_text('{"version": "1.0", "model_type": "test"}\n')

    revision = f"parity-{uuid.uuid4().hex[:8]}"
    upload_folder(repo_id=test_repo, folder_path=str(sd), revision=revision)

    yield {
        "repo_id": test_repo,
        "revision": revision,
        "known_file": "config.json",
    }

    auth.TOKEN_PATH = orig_token_path
    os.unlink(sess_tok.name)
    shutil.rmtree(seed_dir, ignore_errors=True)


@pytest.fixture(params=["hippius", "hf"], ids=["hippius", "hf"])
def client(request, parity_seed):
    """Backend under test. Each parametrization yields a module + context
    dict so test bodies are backend-agnostic. The HF parametrization
    runs even without Hippius creds — it only needs public HF access."""
    if request.param == "hippius":
        if parity_seed is None:
            pytest.skip("HIPPIUS_TEST_USER/PASS or HIPPIUS_TEST_TOKEN not set")
        return {
            "mod": hippius_hub,
            "repo_id": parity_seed["repo_id"],
            "revision": parity_seed["revision"],
            "known_file": parity_seed["known_file"],
        }
    return {
        "mod": huggingface_hub,
        "repo_id": HF_REFERENCE_REPO,
        "revision": HF_REFERENCE_REVISION,
        "known_file": HF_REFERENCE_FILE,
    }


# ---------- hf_hub_download ----------

def test_hf_hub_download_returns_existing_file(client, tmp_path):
    p = client["mod"].hf_hub_download(
        repo_id=client["repo_id"],
        filename=client["known_file"],
        revision=client["revision"],
        cache_dir=str(tmp_path / "cache"),
    )
    assert os.path.exists(p)
    assert os.path.getsize(p) > 0


def test_hf_hub_download_into_local_dir(client, tmp_path):
    local = tmp_path / "local"
    local.mkdir()
    p = client["mod"].hf_hub_download(
        repo_id=client["repo_id"],
        filename=client["known_file"],
        revision=client["revision"],
        local_dir=str(local),
    )
    real = Path(os.path.realpath(p)).resolve()
    assert local.resolve() in [real, *real.parents]


def test_hf_hub_download_cache_hit_returns_same_path(client, tmp_path):
    cache_dir = str(tmp_path / "cache")
    p1 = client["mod"].hf_hub_download(
        repo_id=client["repo_id"], filename=client["known_file"],
        revision=client["revision"], cache_dir=cache_dir,
    )
    p2 = client["mod"].hf_hub_download(
        repo_id=client["repo_id"], filename=client["known_file"],
        revision=client["revision"], cache_dir=cache_dir,
    )
    assert p1 == p2


# ---------- try_to_load_from_cache ----------

def test_try_to_load_from_cache_miss_returns_none(client, tmp_path):
    result = client["mod"].try_to_load_from_cache(
        client["repo_id"], client["known_file"],
        cache_dir=str(tmp_path / "empty-cache"),
        revision=client["revision"],
    )
    assert result is None


def test_try_to_load_from_cache_hit_after_download(client, tmp_path):
    cache_dir = str(tmp_path / "cache")
    client["mod"].hf_hub_download(
        repo_id=client["repo_id"], filename=client["known_file"],
        revision=client["revision"], cache_dir=cache_dir,
    )
    result = client["mod"].try_to_load_from_cache(
        client["repo_id"], client["known_file"],
        cache_dir=cache_dir, revision=client["revision"],
    )
    assert result is not None
    assert os.path.exists(result)


# ---------- list_repo_files / file_exists ----------

def test_list_repo_files_returns_list_of_strings(client):
    files = client["mod"].list_repo_files(client["repo_id"], revision=client["revision"])
    assert isinstance(files, list)
    assert all(isinstance(f, str) for f in files)
    assert client["known_file"] in files


def test_file_exists_true_for_known(client):
    assert client["mod"].file_exists(
        client["repo_id"], client["known_file"], revision=client["revision"],
    ) is True


def test_file_exists_false_for_unknown(client):
    assert client["mod"].file_exists(
        client["repo_id"], "no-such-file-zzz.bin", revision=client["revision"],
    ) is False


# ---------- repo_exists / revision_exists ----------

def test_repo_exists_true_for_known_repo(client):
    assert client["mod"].repo_exists(client["repo_id"]) is True


def test_revision_exists_true_for_known(client):
    assert client["mod"].revision_exists(client["repo_id"], client["revision"]) is True


def test_revision_exists_false_for_fake(client):
    assert client["mod"].revision_exists(
        client["repo_id"], "definitely-not-a-real-revision-zzz",
    ) is False


# ---------- model_info ----------

def test_model_info_returns_ModelInfo_with_expected_id(client):
    info = client["mod"].model_info(client["repo_id"], revision=client["revision"])
    assert isinstance(info, huggingface_hub.ModelInfo)
    assert info.id == client["repo_id"]


def test_model_info_siblings_includes_known_file(client):
    info = client["mod"].model_info(client["repo_id"], revision=client["revision"])
    rfilenames = [s.rfilename for s in info.siblings]
    assert client["known_file"] in rfilenames


# ---------- snapshot_download ----------

def test_snapshot_download_with_allow_patterns_returns_directory(client, tmp_path):
    snap = client["mod"].snapshot_download(
        repo_id=client["repo_id"],
        revision=client["revision"],
        cache_dir=str(tmp_path / "cache"),
        allow_patterns=client["known_file"],
    )
    assert os.path.isdir(snap)
    assert os.path.exists(os.path.join(snap, client["known_file"]))


# ---------- hf_hub_url ----------

def test_hf_hub_url_returns_string(client):
    url = client["mod"].hf_hub_url(
        client["repo_id"], client["known_file"], revision=client["revision"],
    )
    assert isinstance(url, str)
    assert url.startswith("http")
