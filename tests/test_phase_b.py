"""Phase B drop-in compatibility tests.

Signature + behavior parity for upload_file, upload_folder, create_repo,
delete_repo, repo_info/model_info, list_repo_files, repo_exists,
revision_exists, file_exists, plus the HippiusApi(HfApi) subclass and its
auto-stubbing of unsupported HF methods.
"""
import inspect
import io
import os

import pytest

import huggingface_hub
from huggingface_hub import CommitInfo, ModelInfo, RepoUrl
from huggingface_hub.hf_api import RepoSibling

import hippius_hub
from hippius_hub import (
    HippiusApi,
    create_repo,
    delete_repo,
    file_exists,
    list_repo_files,
    list_repo_refs,
    model_info,
    repo_exists,
    repo_info,
    revision_exists,
    upload_file,
    upload_folder,
)
from hippius_hub.errors import HfHubHTTPError, RepositoryNotFoundError, RevisionNotFoundError

from tests._helpers import write_test_file


# ---------- signature parity ----------

@pytest.mark.parametrize("name", [
    "upload_file",
    "upload_folder",
    "create_repo",
    "delete_repo",
    "repo_info",
    "model_info",
    "list_repo_files",
    "list_repo_refs",
    "repo_exists",
    "revision_exists",
    "file_exists",
])
def test_function_accepts_all_hf_kwargs(name):
    """Every kwarg HF accepts must be accepted by ours (modulo **kwargs catch-all)."""
    hf_fn = getattr(huggingface_hub, name)
    our_fn = getattr(hippius_hub, name)
    hf_params = inspect.signature(hf_fn).parameters
    our_params = inspect.signature(our_fn).parameters
    has_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in our_params.values())
    if has_kwargs:
        return
    # Ignore HF-internal kwargs (leading underscore — not part of the public contract)
    hf_public = {n for n in hf_params if not n.startswith("_")}
    missing = hf_public - set(our_params)
    assert not missing, f"hippius_hub.{name} missing HF kwargs: {missing}"


# ---------- HippiusApi delegation + stubs ----------

def test_hippius_api_is_hf_api_subclass():
    assert issubclass(HippiusApi, huggingface_hub.HfApi)


def test_hippius_api_init_accepts_hf_kwargs():
    api = HippiusApi(
        endpoint="https://x.example",
        token="fake",
        library_name="test",
        library_version="0.1",
        user_agent={"k": "v"},
        headers={"X-Test": "1"},
    )
    assert api.endpoint == "https://x.example"


def test_hippius_api_stubs_raise_clearly():
    api = HippiusApi()
    with pytest.raises(NotImplementedError, match="HF-specific"):
        api.create_inference_endpoint("any")
    with pytest.raises(NotImplementedError, match="HF-specific"):
        api.list_models()
    with pytest.raises(NotImplementedError, match="HF-specific"):
        api.create_discussion("foo/bar", title="t", description="d")


def test_hippius_api_implements_phase_a_b_methods():
    api = HippiusApi()
    for method in [
        "hf_hub_download", "snapshot_download", "whoami",
        "upload_file", "upload_folder",
        "create_repo", "delete_repo",
        "repo_info", "model_info", "list_repo_files", "list_repo_refs",
        "repo_exists", "revision_exists", "file_exists",
        "login", "logout",
    ]:
        attr = getattr(api, method)
        assert callable(attr)
        bound_qualname = getattr(attr, "__qualname__", "")
        assert "HippiusApi" in bound_qualname or "Hippius" in bound_qualname, (
            f"{method} resolves to {bound_qualname}, not HippiusApi's override"
        )


# ---------- existence checks ----------

@pytest.mark.e2e
def test_repo_exists_true_for_test_repo(logged_in, test_repo):
    assert repo_exists(test_repo) is True


@pytest.mark.e2e
def test_repo_exists_false_for_unknown(logged_in):
    assert repo_exists("test/definitely-not-here-zzz") is False


@pytest.mark.e2e
def test_revision_exists_true(tmp_path, logged_in, test_repo, revision):
    src = tmp_path / "rev.bin"
    write_test_file(src, 64, seed=b"rev")
    upload_file(path_or_fileobj=str(src), path_in_repo="rev.bin", repo_id=test_repo, revision=revision)
    assert revision_exists(test_repo, revision) is True


@pytest.mark.e2e
def test_revision_exists_false(logged_in, test_repo):
    assert revision_exists(test_repo, "does-not-exist-9999") is False


@pytest.mark.e2e
def test_file_exists_after_upload(tmp_path, logged_in, test_repo, revision):
    src = tmp_path / "fe.bin"
    write_test_file(src, 64, seed=b"fe")
    upload_file(path_or_fileobj=str(src), path_in_repo="fe.bin", repo_id=test_repo, revision=revision)
    assert file_exists(test_repo, "fe.bin", revision=revision) is True
    assert file_exists(test_repo, "nope.bin", revision=revision) is False


# ---------- list_repo_files ----------

@pytest.mark.e2e
def test_list_repo_files_returns_titles(tmp_path, logged_in, test_repo, revision):
    src_dir = tmp_path / "tree"
    src_dir.mkdir()
    for name in ["a.bin", "sub/b.bin"]:
        p = src_dir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        write_test_file(p, 64, seed=name.encode())
    upload_folder(repo_id=test_repo, folder_path=str(src_dir), revision=revision)

    files = list_repo_files(test_repo, revision=revision)
    assert "a.bin" in files
    assert "sub/b.bin" in files


@pytest.mark.e2e
def test_list_repo_files_revision_not_found(logged_in, test_repo):
    with pytest.raises(RevisionNotFoundError):
        list_repo_files(test_repo, revision="nope-9999")


# ---------- repo_info / model_info ----------

@pytest.mark.e2e
def test_model_info_shape(tmp_path, logged_in, test_repo, revision):
    src = tmp_path / "mi.bin"
    write_test_file(src, 128, seed=b"mi")
    upload_file(path_or_fileobj=str(src), path_in_repo="mi.bin", repo_id=test_repo, revision=revision)

    info = model_info(test_repo, revision=revision)
    assert isinstance(info, ModelInfo)
    assert info.id == test_repo
    assert info.sha
    sibling_names = [s.rfilename for s in info.siblings]
    assert "mi.bin" in sibling_names
    sibling = next(s for s in info.siblings if s.rfilename == "mi.bin")
    assert isinstance(sibling, RepoSibling)
    assert sibling.size == 128
    assert sibling.blob_id and sibling.blob_id.startswith("sha256:")


@pytest.mark.e2e
def test_repo_info_alias_works(tmp_path, logged_in, test_repo, revision):
    src = tmp_path / "ri.bin"
    write_test_file(src, 64, seed=b"ri")
    upload_file(path_or_fileobj=str(src), path_in_repo="ri.bin", repo_id=test_repo, revision=revision)
    info = repo_info(test_repo, revision=revision)
    assert isinstance(info, ModelInfo)


@pytest.mark.e2e
def test_repo_info_last_modified_from_manifest(tmp_path, logged_in, test_repo, revision):
    """lastModified/createdAt fall back to the manifest's upload timestamp."""
    src = tmp_path / "ts.bin"
    write_test_file(src, 64, seed=b"ts")
    upload_file(path_or_fileobj=str(src), path_in_repo="ts.bin", repo_id=test_repo, revision=revision)
    info = repo_info(test_repo, revision=revision)
    assert info.lastModified is not None


# ---------- list_repo_refs ----------

def test_list_repo_refs_maps_branches_and_tags(monkeypatch):
    """Unit-level: `main` goes under branches, everything else under tags. Mocks
    the network so the GitRefs/GitRefInfo mapping is covered without creds (the
    other refs tests are e2e-only). target_commit is intentionally None — the
    tag list doesn't carry digests and resolving them is O(N) round-trips."""
    from huggingface_hub import GitRefs
    from hippius_hub import _repo_ops

    monkeypatch.setattr(_repo_ops, "get_oci_bearer_token", lambda *a, **k: "tok")
    monkeypatch.setattr(_repo_ops, "_list_tags", lambda *a, **k: ["main", "v1", "v2"])

    refs = _repo_ops.list_repo_refs("org/model")
    assert isinstance(refs, GitRefs)
    assert [b.name for b in refs.branches] == ["main"]
    assert refs.branches[0].ref == "refs/heads/main"
    assert sorted(t.name for t in refs.tags) == ["v1", "v2"]
    assert all(t.ref.startswith("refs/tags/") for t in refs.tags)
    assert all(t.target_commit is None for t in refs.tags)


def test_list_repo_refs_unknown_repo_raises_unit(monkeypatch):
    from hippius_hub import _repo_ops

    monkeypatch.setattr(_repo_ops, "get_oci_bearer_token", lambda *a, **k: "tok")
    monkeypatch.setattr(_repo_ops, "_list_tags", lambda *a, **k: None)
    with pytest.raises(RepositoryNotFoundError):
        _repo_ops.list_repo_refs("org/missing")


def test_next_link_parsing():
    from hippius_hub._repo_ops import _next_link
    reg = "https://reg.example.com"
    assert _next_link(None, reg) is None
    # root-relative next → made absolute against the registry
    assert (
        _next_link('</v2/r/tags/list?last=x&n=2>; rel="next"', reg)
        == "https://reg.example.com/v2/r/tags/list?last=x&n=2"
    )
    # already-absolute next → returned as-is
    assert (
        _next_link('<https://cdn.example/v2/r/tags/list?last=x>; rel="next"', reg)
        == "https://cdn.example/v2/r/tags/list?last=x"
    )
    # only a prev link → no next page
    assert _next_link('</v2/r/tags/list?last=x>; rel="prev"', reg) is None


def test_list_tags_follows_pagination(monkeypatch):
    """_list_tags walks Link: rel=next so the full tag set comes back, not just
    the first page (the bug behind list_repo_refs missing fresh revisions)."""
    from hippius_hub import _repo_ops

    class _FakeResp:
        def __init__(self, tags, link=None):
            self.status_code = 200
            self._tags = tags
            self.headers = {"Link": link} if link else {}

        def raise_for_status(self):
            pass

        def json(self):
            return {"tags": self._tags}

    pages = {
        "https://reg/v2/r/tags/list": _FakeResp(["a", "b"], '</v2/r/tags/list?last=b>; rel="next"'),
        "https://reg/v2/r/tags/list?last=b": _FakeResp(["c"]),
    }
    monkeypatch.setattr(_repo_ops.httpx, "get", lambda url, **kwargs: pages[url])
    assert _repo_ops._list_tags("https://reg", "r", "tok") == ["a", "b", "c"]


def test_normalize_oci_timestamp_to_hf_form():
    from huggingface_hub.utils import parse_datetime
    from hippius_hub._repo_ops import _normalize_oci_timestamp

    assert _normalize_oci_timestamp(None) is None
    assert _normalize_oci_timestamp("") is None
    # The offset form datetime.isoformat() produces (what crashed ModelInfo).
    out = _normalize_oci_timestamp("2026-05-26T18:05:32.733878+00:00")
    assert out == "2026-05-26T18:05:32.733878Z"
    # And the result is something huggingface_hub can actually parse.
    parse_datetime(out)
    # Garbage degrades to None rather than raising.
    assert _normalize_oci_timestamp("not-a-date") is None


@pytest.mark.e2e
def test_list_repo_refs_includes_uploaded_revision(tmp_path, logged_in, test_repo, revision):
    import time
    from huggingface_hub import GitRefs

    src = tmp_path / "ref.bin"
    write_test_file(src, 64, seed=b"ref")
    upload_file(path_or_fileobj=str(src), path_in_repo="ref.bin", repo_id=test_repo, revision=revision)

    # The registry's tag listing is eventually consistent — a just-pushed tag can
    # take a moment to surface — so poll until the fresh revision appears under
    # tags. (Don't assume `main` exists: the shared test repo is populated almost
    # entirely from unique per-test revisions.)
    refs = None
    for _ in range(10):
        refs = list_repo_refs(test_repo)
        assert isinstance(refs, GitRefs)
        if revision in [t.name for t in refs.tags]:
            break
        time.sleep(3)
    else:
        pytest.fail(f"{revision!r} did not appear under tags within the timeout")

    # A non-main revision is classified as a tag, never a branch.
    assert revision not in [b.name for b in refs.branches]


@pytest.mark.e2e
def test_list_repo_refs_unknown_repo_raises(logged_in):
    with pytest.raises(RepositoryNotFoundError):
        list_repo_refs("test/definitely-not-here-zzz")


# ---------- upload_file ----------

@pytest.mark.e2e
def test_upload_file_returns_commit_info(tmp_path, logged_in, test_repo, revision):
    src = tmp_path / "u.bin"
    write_test_file(src, 64, seed=b"u")
    ci = upload_file(
        path_or_fileobj=str(src),
        path_in_repo="u.bin",
        repo_id=test_repo,
        revision=revision,
        commit_message="test commit",
        commit_description="desc here",
    )
    assert isinstance(ci, CommitInfo)
    assert ci.commit_message == "test commit"
    assert ci.commit_description == "desc here"
    assert ci.oid.startswith("sha256:")
    assert test_repo in ci.commit_url
    assert "/commit/" in ci.commit_url


@pytest.mark.e2e
def test_upload_file_accepts_bytes(tmp_path, logged_in, test_repo, revision, cache_dir):
    payload = b"hello hippius via bytes upload\n" * 16
    ci = upload_file(
        path_or_fileobj=payload,
        path_in_repo="bytes.bin",
        repo_id=test_repo,
        revision=revision,
    )
    assert isinstance(ci, CommitInfo)

    from hippius_hub import hf_hub_download
    out = hf_hub_download(test_repo, "bytes.bin", revision=revision, cache_dir=cache_dir)
    with open(out, "rb") as f:
        assert f.read() == payload


@pytest.mark.e2e
def test_upload_file_accepts_binary_io(tmp_path, logged_in, test_repo, revision, cache_dir):
    payload = b"binary-io stream content\n" * 8
    ci = upload_file(
        path_or_fileobj=io.BytesIO(payload),
        path_in_repo="bio.bin",
        repo_id=test_repo,
        revision=revision,
    )
    assert isinstance(ci, CommitInfo)
    from hippius_hub import hf_hub_download
    out = hf_hub_download(test_repo, "bio.bin", revision=revision, cache_dir=cache_dir)
    with open(out, "rb") as f:
        assert f.read() == payload


@pytest.mark.e2e
def test_upload_file_merges_into_existing_manifest(tmp_path, logged_in, test_repo, revision):
    """A second upload_file to the same revision should keep the first file."""
    a = tmp_path / "a.bin"
    write_test_file(a, 64, seed=b"a")
    upload_file(path_or_fileobj=str(a), path_in_repo="a.bin", repo_id=test_repo, revision=revision)

    b = tmp_path / "b.bin"
    write_test_file(b, 64, seed=b"b")
    upload_file(path_or_fileobj=str(b), path_in_repo="b.bin", repo_id=test_repo, revision=revision)

    files = list_repo_files(test_repo, revision=revision)
    assert "a.bin" in files
    assert "b.bin" in files


@pytest.mark.e2e
def test_upload_file_replaces_same_title(tmp_path, logged_in, test_repo, revision, cache_dir):
    """Uploading the same path_in_repo twice replaces, doesn't duplicate."""
    a = tmp_path / "first.bin"
    write_test_file(a, 64, seed=b"first")
    upload_file(path_or_fileobj=str(a), path_in_repo="replace.bin", repo_id=test_repo, revision=revision)

    b = tmp_path / "second.bin"
    write_test_file(b, 64, seed=b"second")
    upload_file(path_or_fileobj=str(b), path_in_repo="replace.bin", repo_id=test_repo, revision=revision)

    files = list_repo_files(test_repo, revision=revision)
    assert files.count("replace.bin") == 1


# ---------- upload_folder ----------

@pytest.mark.e2e
def test_upload_folder_returns_commit_info_and_filters(tmp_path, logged_in, test_repo, revision):
    folder = tmp_path / "f"
    folder.mkdir()
    write_test_file(folder / "config.json", 64, seed=b"cfg")
    write_test_file(folder / "model.bin", 256, seed=b"m")
    write_test_file(folder / "tmp.log", 16, seed=b"log")

    ci = upload_folder(
        repo_id=test_repo,
        folder_path=str(folder),
        revision=revision,
        allow_patterns=["*.json", "*.bin"],
    )
    assert isinstance(ci, CommitInfo)

    files = list_repo_files(test_repo, revision=revision)
    assert "config.json" in files
    assert "model.bin" in files
    assert "tmp.log" not in files


@pytest.mark.e2e
def test_upload_folder_delete_patterns(tmp_path, logged_in, test_repo, revision):
    """delete_patterns drops matching titles from the existing manifest."""
    folder = tmp_path / "g"
    folder.mkdir()
    write_test_file(folder / "keep.bin", 64, seed=b"k")
    write_test_file(folder / "remove.txt", 32, seed=b"r")
    upload_folder(repo_id=test_repo, folder_path=str(folder), revision=revision)

    # Second push: add nothing new, delete_patterns prunes the .txt
    folder2 = tmp_path / "g2"
    folder2.mkdir()
    write_test_file(folder2 / "added.bin", 16, seed=b"a")
    upload_folder(
        repo_id=test_repo,
        folder_path=str(folder2),
        revision=revision,
        delete_patterns=["*.txt"],
    )

    files = list_repo_files(test_repo, revision=revision)
    assert "keep.bin" in files
    assert "added.bin" in files
    assert "remove.txt" not in files


# ---------- create_repo / delete_repo ----------

@pytest.mark.e2e
def test_create_repo_exist_ok_returns_repo_url(logged_in, test_repo):
    url = create_repo(test_repo, exist_ok=True)
    assert isinstance(url, RepoUrl)
    assert str(url).endswith(test_repo)


@pytest.mark.e2e
def test_create_repo_exist_ok_false_raises_on_existing(logged_in, test_repo):
    """The test repo already has a populated 'main' manifest, so this should raise."""
    with pytest.raises(HfHubHTTPError):
        create_repo(test_repo, exist_ok=False)


def test_create_repo_rejects_unknown_repo_type():
    with pytest.raises(NotImplementedError, match="repo_type"):
        create_repo("any/repo", repo_type="bogus")


def test_delete_repo_rejects_unknown_repo_type():
    with pytest.raises(NotImplementedError, match="repo_type"):
        delete_repo("any/repo", repo_type="bogus")


# ---------- HF-only kwargs: warn vs raise ----------

def test_upload_file_create_pr_warns_and_proceeds_to_validation(tmp_path):
    """create_pr=True should emit a UserWarning (HF accept-and-ignore pattern),
    not raise NotImplementedError. We call without creds so it'll fail at the
    network/auth boundary later, but the warning must fire first."""
    src = tmp_path / "f.bin"
    src.write_bytes(b"x")
    with pytest.warns(UserWarning, match="create_pr=True is ignored"):
        # Will fail downstream (no creds), but we just care that it warns BEFORE failing.
        try:
            upload_file(
                path_or_fileobj=str(src),
                path_in_repo="f.bin",
                repo_id="test/e2e-client",
                create_pr=True,
                token=False,
            )
        except Exception:
            pass


def test_upload_file_parent_commit_warns(tmp_path):
    src = tmp_path / "f.bin"
    src.write_bytes(b"x")
    with pytest.warns(UserWarning, match="parent_commit is ignored"):
        try:
            upload_file(
                path_or_fileobj=str(src),
                path_in_repo="f.bin",
                repo_id="test/e2e-client",
                parent_commit="abc123",
                token=False,
            )
        except Exception:
            pass


def test_upload_file_run_as_future_still_raises(tmp_path):
    src = tmp_path / "f.bin"
    src.write_bytes(b"x")
    with pytest.raises(NotImplementedError, match="run_as_future"):
        upload_file(
            path_or_fileobj=str(src),
            path_in_repo="f.bin",
            repo_id="test/e2e-client",
            run_as_future=True,
            token=False,
        )


# ---------- backward compat: hippius_hub_upload ----------

@pytest.mark.e2e
def test_hippius_hub_upload_single_file_still_works(tmp_path, logged_in, test_repo, revision, cache_dir):
    src = tmp_path / "bc.bin"
    expected = write_test_file(src, 128, seed=b"bc")
    hippius_hub.hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)
    from hippius_hub import hf_hub_download
    from tests._helpers import sha256_of_file
    out = hf_hub_download(test_repo, "bc.bin", revision=revision, cache_dir=cache_dir)
    assert sha256_of_file(out) == expected


@pytest.mark.e2e
def test_hippius_hub_upload_folder_still_works(tmp_path, logged_in, test_repo, revision):
    folder = tmp_path / "bcf"
    folder.mkdir()
    write_test_file(folder / "x.bin", 64, seed=b"x")
    write_test_file(folder / "y.bin", 64, seed=b"y")
    hippius_hub.hippius_hub_upload(repo_id=test_repo, local_path=str(folder), revision=revision)
    files = list_repo_files(test_repo, revision=revision)
    assert "x.bin" in files
    assert "y.bin" in files


# ---------- HF scan_cache_dir on our layout (plan open question #4) ----------

@pytest.mark.e2e
def test_hf_scan_cache_dir_recognizes_our_cache(tmp_path, cache_dir, logged_in, test_repo, revision):
    """huggingface_hub.scan_cache_dir should parse our cache layout cleanly."""
    src = tmp_path / "scan.bin"
    write_test_file(src, 128, seed=b"scan")
    upload_file(path_or_fileobj=str(src), path_in_repo="scan.bin", repo_id=test_repo, revision=revision)

    from hippius_hub import hf_hub_download
    hf_hub_download(test_repo, "scan.bin", revision=revision, cache_dir=cache_dir)

    from huggingface_hub import scan_cache_dir
    info = scan_cache_dir(cache_dir=cache_dir)
    repo_ids = {r.repo_id for r in info.repos}
    assert test_repo in repo_ids
