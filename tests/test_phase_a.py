"""Phase A drop-in compatibility tests.

Covers the HF-aligned surface: hf_hub_download new kwargs, snapshot_download
real implementation, try_to_load_from_cache, hf_hub_url, login/logout
HF-shape, whoami, and signature/exception parity against huggingface_hub.
"""
import inspect
import os
import time

import pytest

from hippius_hub import (
    hf_hub_download,
    hf_hub_url,
    hippius_hub_upload,
    login,
    logout,
    snapshot_download,
    try_to_load_from_cache,
    whoami,
)
from hippius_hub.errors import EntryNotFoundError, LocalEntryNotFoundError
from hippius_hub.file_download import _cache_dirname, _oci_repo_path

from tests._helpers import sha256_of_file, write_test_file


# ---------- repo_type mapping helpers ----------

@pytest.mark.parametrize("repo_type,expected", [
    (None,      "foo/bar"),
    ("model",   "foo/bar"),
    ("dataset", "datasets/foo/bar"),
    ("space",   "spaces/foo/bar"),
])
def test_oci_repo_path_mapping(repo_type, expected):
    assert _oci_repo_path("foo/bar", repo_type) == expected


@pytest.mark.parametrize("repo_type,repo_id", [
    ("dataset", "datasets/foo"),
    ("space",   "spaces/foo"),
])
def test_oci_repo_path_rejects_double_prefix(repo_type, repo_id):
    """Catch the foot-gun where a user passes the prefix in repo_id."""
    with pytest.raises(ValueError, match="already starts with"):
        _oci_repo_path(repo_id, repo_type)


@pytest.mark.parametrize("repo_type,expected", [
    (None,      "models--foo--bar"),
    ("model",   "models--foo--bar"),
    ("dataset", "datasets--foo--bar"),
    ("space",   "spaces--foo--bar"),
])
def test_cache_dirname_mapping(repo_type, expected):
    assert _cache_dirname("foo/bar", repo_type) == expected


# ---------- signature & exception parity ----------

def test_hf_hub_download_accepts_all_hf_kwargs():
    """Every kwarg HF accepts must be accepted by us (we can have extras)."""
    import huggingface_hub
    hf_params = set(inspect.signature(huggingface_hub.hf_hub_download).parameters.keys())
    our_params = set(inspect.signature(hf_hub_download).parameters.keys())
    missing = hf_params - our_params
    assert not missing, f"hippius_hub.hf_hub_download missing HF kwargs: {missing}"


def test_snapshot_download_accepts_all_hf_kwargs():
    import huggingface_hub
    hf_params = set(inspect.signature(huggingface_hub.snapshot_download).parameters.keys())
    our_params = set(inspect.signature(snapshot_download).parameters.keys())
    missing = hf_params - our_params
    assert not missing, f"hippius_hub.snapshot_download missing HF kwargs: {missing}"


def test_typed_exceptions_are_hf_classes():
    """Our error re-exports must be identity-equal to HF's so isinstance works."""
    import huggingface_hub.errors as hf_errors
    from hippius_hub import errors as our_errors
    for name in ("EntryNotFoundError", "RevisionNotFoundError", "RepositoryNotFoundError",
                 "LocalEntryNotFoundError", "LocalTokenNotFoundError", "GatedRepoError"):
        assert getattr(our_errors, name) is getattr(hf_errors, name), name


# ---------- try_to_load_from_cache ----------

def test_try_to_load_from_cache_miss(tmp_path):
    """No network, no cache → None."""
    result = try_to_load_from_cache(
        "any/repo", "any.bin", cache_dir=str(tmp_path), revision="main",
    )
    assert result is None


@pytest.mark.e2e
def test_try_to_load_from_cache_hit(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "cached.bin"
    write_test_file(src, 256, seed=b"cached")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    hf_hub_download(
        repo_id=test_repo, filename="cached.bin", revision=revision, cache_dir=cache_dir,
    )

    result = try_to_load_from_cache(
        test_repo, "cached.bin", cache_dir=cache_dir, revision=revision,
    )
    assert result is not None
    assert os.path.exists(result)


def test_try_to_load_from_cache_uses_dataset_cache_dirname(tmp_path):
    """Dataset cache lives under datasets--*--* per HF convention."""
    repo = "foo/bar"
    rev = "main"
    target = tmp_path / "datasets--foo--bar" / "snapshots" / rev / "config.json"
    target.parent.mkdir(parents=True)
    target.write_text("{}")
    result = try_to_load_from_cache(
        repo, "config.json", cache_dir=str(tmp_path), revision=rev, repo_type="dataset",
    )
    assert result == str(target)


def test_try_to_load_from_cache_rejects_unknown_repo_type(tmp_path):
    with pytest.raises(NotImplementedError, match="repo_type"):
        try_to_load_from_cache(
            "any/repo", "x.bin", cache_dir=str(tmp_path), repo_type="bogus",
        )


# ---------- hf_hub_url ----------

def test_hf_hub_url_default_revision():
    assert hf_hub_url("foo/bar", "model.bin") == (
        "https://registry.hippius.com/v2/foo/bar/manifests/main"
    )


def test_hf_hub_url_explicit_revision():
    assert hf_hub_url("foo/bar", "model.bin", revision="v1.0") == (
        "https://registry.hippius.com/v2/foo/bar/manifests/v1.0"
    )


def test_hf_hub_url_custom_endpoint():
    assert hf_hub_url("foo/bar", "model.bin", endpoint="https://my.registry") == (
        "https://my.registry/v2/foo/bar/manifests/main"
    )


def test_hf_hub_url_dataset_namespaces_under_datasets():
    """dataset repos resolve to /v2/datasets/{repo_id}/manifests/..."""
    assert hf_hub_url("foo/bar", "model.bin", repo_type="dataset") == (
        "https://registry.hippius.com/v2/datasets/foo/bar/manifests/main"
    )


def test_hf_hub_url_space_namespaces_under_spaces():
    assert hf_hub_url("foo/bar", "model.bin", repo_type="space") == (
        "https://registry.hippius.com/v2/spaces/foo/bar/manifests/main"
    )


def test_hf_hub_url_rejects_unknown_repo_type():
    with pytest.raises(NotImplementedError, match="repo_type"):
        hf_hub_url("foo/bar", "model.bin", repo_type="bogus")


# ---------- hf_hub_download new kwargs ----------

@pytest.mark.e2e
def test_hf_hub_download_local_dir(tmp_path, logged_in, test_repo, revision):
    src = tmp_path / "ld.bin"
    expected = write_test_file(src, 512, seed=b"ld")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    local_dir = tmp_path / "local"
    out = hf_hub_download(
        repo_id=test_repo, filename="ld.bin", revision=revision,
        local_dir=str(local_dir),
    )
    assert out == str(local_dir / "ld.bin")
    assert sha256_of_file(out) == expected


@pytest.mark.e2e
def test_hf_hub_download_force_download_rewrites_blob(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "fd.bin"
    write_test_file(src, 256, seed=b"fd")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    p1 = hf_hub_download(repo_id=test_repo, filename="fd.bin", revision=revision, cache_dir=cache_dir)
    mtime_before = os.path.getmtime(os.path.realpath(p1))
    time.sleep(1.1)
    p2 = hf_hub_download(
        repo_id=test_repo, filename="fd.bin", revision=revision,
        cache_dir=cache_dir, force_download=True,
    )
    assert p1 == p2
    assert os.path.getmtime(os.path.realpath(p2)) >= mtime_before


def test_hf_hub_download_local_files_only_miss_raises(tmp_path):
    """No network used because cache check short-circuits before auth."""
    with pytest.raises(LocalEntryNotFoundError):
        hf_hub_download(
            repo_id="any/repo", filename="not-here.bin", revision="rev",
            cache_dir=str(tmp_path), local_files_only=True,
        )


def test_hf_hub_download_force_download_and_local_files_only_raises_ValueError(tmp_path):
    """HF raises ValueError when both flags are set; pin that behavior."""
    with pytest.raises(ValueError, match="force_download.*local_files_only"):
        hf_hub_download(
            repo_id="any/repo", filename="x.bin", revision="rev",
            cache_dir=str(tmp_path),
            force_download=True, local_files_only=True,
        )


def test_hf_hub_download_dry_run_raises_not_implemented(tmp_path):
    """Audit M1: hf_hub_download does NOT honor dry_run (snapshot_download
    does). Pre-M1 the flag was silently ignored and the download would
    proceed anyway, defeating the intent. Now it raises NotImplementedError
    at the kwarg-validation step before any network call.

    The error message must point at snapshot_download so a caller who
    actually wanted dry_run sees the migration path.
    """
    with pytest.raises(NotImplementedError, match="snapshot_download"):
        hf_hub_download(
            repo_id="any/repo", filename="x.bin",
            cache_dir=str(tmp_path), dry_run=True,
        )


@pytest.mark.e2e
def test_hf_hub_download_local_files_only_hit(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "lfo.bin"
    expected = write_test_file(src, 128, seed=b"lfo")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    hf_hub_download(repo_id=test_repo, filename="lfo.bin", revision=revision, cache_dir=cache_dir)

    out = hf_hub_download(
        repo_id=test_repo, filename="lfo.bin", revision=revision,
        cache_dir=cache_dir, local_files_only=True,
    )
    assert sha256_of_file(out) == expected


def test_hf_hub_download_rejects_unknown_repo_type(tmp_path):
    with pytest.raises(NotImplementedError, match="repo_type"):
        hf_hub_download(
            repo_id="any/repo", filename="x.bin",
            cache_dir=str(tmp_path), repo_type="bogus",
        )


@pytest.mark.e2e
def test_hf_hub_download_subfolder(tmp_path, cache_dir, logged_in, test_repo, revision):
    src_dir = tmp_path / "tree"
    (src_dir / "sub").mkdir(parents=True)
    target = src_dir / "sub" / "file.bin"
    expected = write_test_file(target, 256, seed=b"subfolder")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo, filename="file.bin", revision=revision,
        cache_dir=cache_dir, subfolder="sub",
    )
    assert sha256_of_file(out) == expected


# ---------- snapshot_download ----------

@pytest.mark.e2e
def test_snapshot_download_full(tmp_path, cache_dir, logged_in, test_repo, revision):
    src_dir = tmp_path / "snap"
    src_dir.mkdir()
    expected = {}
    for name in ["a.bin", "b.bin", "sub/c.bin"]:
        p = src_dir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        expected[name] = write_test_file(p, 512, seed=name.encode())

    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)

    snap_path = snapshot_download(repo_id=test_repo, revision=revision, cache_dir=cache_dir)

    for name, want in expected.items():
        assert sha256_of_file(os.path.join(snap_path, name)) == want


@pytest.mark.e2e
def test_snapshot_download_allow_patterns(tmp_path, cache_dir, logged_in, test_repo, revision):
    src_dir = tmp_path / "snap"
    src_dir.mkdir()
    write_test_file(src_dir / "config.json", 64, seed=b"cfg")
    write_test_file(src_dir / "model.bin", 256, seed=b"bin")
    write_test_file(src_dir / "ignore.txt", 128, seed=b"ign")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)

    snap_path = snapshot_download(
        repo_id=test_repo, revision=revision, cache_dir=cache_dir,
        allow_patterns="*.json",
    )

    assert os.path.exists(os.path.join(snap_path, "config.json"))
    assert not os.path.exists(os.path.join(snap_path, "model.bin"))
    assert not os.path.exists(os.path.join(snap_path, "ignore.txt"))


@pytest.mark.e2e
def test_snapshot_download_ignore_patterns(tmp_path, cache_dir, logged_in, test_repo, revision):
    src_dir = tmp_path / "snap"
    src_dir.mkdir()
    write_test_file(src_dir / "a.bin", 64, seed=b"a")
    write_test_file(src_dir / "b.bin", 64, seed=b"b")
    write_test_file(src_dir / "secret.key", 64, seed=b"sk")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)

    snap_path = snapshot_download(
        repo_id=test_repo, revision=revision, cache_dir=cache_dir,
        ignore_patterns="*.key",
    )

    assert os.path.exists(os.path.join(snap_path, "a.bin"))
    assert os.path.exists(os.path.join(snap_path, "b.bin"))
    assert not os.path.exists(os.path.join(snap_path, "secret.key"))


def test_snapshot_download_local_files_only_miss(tmp_path):
    with pytest.raises(LocalEntryNotFoundError):
        snapshot_download(
            repo_id="nope/nope", revision="rev",
            cache_dir=str(tmp_path), local_files_only=True,
        )


# ---------- login / logout / whoami ----------

def test_login_hf_shape_token_positional(tmp_path, monkeypatch):
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "tok"))
    login("ghp_fake_token")
    assert (tmp_path / "tok").read_text() == "Bearer ghp_fake_token"


def test_login_username_password_path(tmp_path, monkeypatch):
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "tok"))
    login(username="u", password="p")
    assert (tmp_path / "tok").read_text().startswith("Basic ")


def test_login_no_creds_raises(tmp_path, monkeypatch):
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "tok"))
    with pytest.raises(ValueError):
        login()


def test_logout_removes_token(tmp_path, monkeypatch):
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "tok"))
    login("x")
    assert (tmp_path / "tok").exists()
    logout()
    assert not (tmp_path / "tok").exists()


def test_logout_when_no_token_is_a_noop(tmp_path, monkeypatch):
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "no-token"))
    logout()


@pytest.mark.e2e
def test_whoami_returns_hf_shape(logged_in):
    result = whoami()
    assert "name" in result
    assert "type" in result
    assert "orgs" in result
    assert isinstance(result["orgs"], list)
    assert result["name"].startswith("robot$")
    assert result["type"] == "robot"


# ---------- existing-Entry typed errors ----------

@pytest.mark.e2e
def test_missing_filename_raises_typed_error(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "present.bin"
    write_test_file(src, 64, seed=b"present")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    with pytest.raises(EntryNotFoundError, match="absent.bin"):
        hf_hub_download(
            repo_id=test_repo, filename="absent.bin", revision=revision,
            cache_dir=cache_dir,
        )
