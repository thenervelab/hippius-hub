"""The dataset/space repo_type gate (0.7.2) across every public entry point.

`_oci_repo_path` refuses `repo_type="dataset"` / `"space"` unless
`HIPPIUS_EXPERIMENTAL_REPO_TYPES` is truthy. The unit mapping is pinned in
test_phase_a.py; this file pins the surface that matters to a caller:

* every public function that maps a repo id refuses BEFORE any network call
  (a stray request here would hit an unmocked respx router and fail loudly);
* the flag's parser is the shared `_resolve_bool`: a typo raises instead of
  silently gating (or silently opening), blank/falsy keep the gate, and
  `model`/`None` never consult the flag at all;
* the cache-first paths (`hf_hub_download` cache hit, `local_files_only`,
  `snapshot_download` `local_files_only`/`dry_run`) keep working for cached
  dataset/space content, and a cache MISS under `local_files_only` is the
  cache error, not the gate;
* the CLI renders the refusal as one line + exit 1 for `revisions` and
  `registry repos delete`, for the gated types AND for a bogus type, and the
  repo-not-found exit 11 reaches the shell through `main()` from both the
  inline site and the typed dispatch.
"""
from __future__ import annotations

import argparse

import httpx
import pytest
import respx

from hippius_hub import _repo_ops, _snapshot_download, cli, diagnose, file_download, file_upload
from hippius_hub.errors import LocalEntryNotFoundError, RepositoryNotFoundError
from hippius_hub.file_download import _cache_dirname, _oci_repo_path

from tests.respx_fixtures import MOCK_REGISTRY, token_route  # noqa: F401 (fixture wiring)

REPO = "foo/bar"
GATED_TYPES = ["dataset", "space"]


@pytest.fixture(autouse=True)
def _gate_closed(monkeypatch):
    monkeypatch.delenv("HIPPIUS_EXPERIMENTAL_REPO_TYPES", raising=False)


def _entry_points(tmp_path):
    folder = tmp_path / "folder"
    folder.mkdir(exist_ok=True)
    (folder / "a.bin").write_bytes(b"a")
    return {
        "hf_hub_download": lambda rt: file_download.hf_hub_download(
            REPO, "x.bin", repo_type=rt, cache_dir=str(tmp_path / "cache")),
        "hf_hub_url": lambda rt: file_download.hf_hub_url(REPO, "x.bin", repo_type=rt),
        "snapshot_download": lambda rt: _snapshot_download.snapshot_download(
            REPO, repo_type=rt, cache_dir=str(tmp_path / "cache")),
        "upload_file": lambda rt: file_upload.upload_file(
            path_or_fileobj=b"x", path_in_repo="x.bin", repo_id=REPO, repo_type=rt, token="tok"),
        "upload_folder": lambda rt: file_upload.upload_folder(
            repo_id=REPO, folder_path=str(folder), repo_type=rt, token="tok"),
        "create_repo": lambda rt: _repo_ops.create_repo(REPO, repo_type=rt, token="tok"),
        "delete_repo": lambda rt: _repo_ops.delete_repo(REPO, repo_type=rt, token="tok"),
        "repo_info": lambda rt: _repo_ops.repo_info(REPO, repo_type=rt, token="tok"),
        "list_repo_files": lambda rt: _repo_ops.list_repo_files(REPO, repo_type=rt, token="tok"),
        "list_repo_refs": lambda rt: _repo_ops.list_repo_refs(REPO, repo_type=rt, token="tok"),
        "repo_exists": lambda rt: _repo_ops.repo_exists(REPO, repo_type=rt, token="tok"),
        "revision_exists": lambda rt: _repo_ops.revision_exists(
            REPO, "main", repo_type=rt, token="tok"),
        "file_exists": lambda rt: _repo_ops.file_exists(REPO, "x.bin", repo_type=rt, token="tok"),
        "run_diagnose": lambda rt: diagnose.run_diagnose(REPO, "x.bin", repo_type=rt, token="tok"),
    }


ENTRY_POINT_NAMES = [
    "hf_hub_download", "hf_hub_url", "snapshot_download", "upload_file", "upload_folder",
    "create_repo", "delete_repo", "repo_info", "list_repo_files", "list_repo_refs",
    "repo_exists", "revision_exists", "file_exists", "run_diagnose",
]


@pytest.mark.parametrize("repo_type", GATED_TYPES)
@pytest.mark.parametrize("name", ENTRY_POINT_NAMES)
@respx.mock(assert_all_called=False)
def test_every_entry_point_refuses_gated_types_before_any_request(
    monkeypatched_registry, tmp_path, name, repo_type
):
    """No route is registered, so any HTTP call would surface as a respx
    'unmocked request' error rather than the gate's NotImplementedError."""
    call = _entry_points(tmp_path)[name]
    with pytest.raises(NotImplementedError, match="Omit repo_type"):
        call(repo_type)
    assert respx.calls.call_count == 0, f"{name} contacted the registry before the gate"


@pytest.mark.parametrize("name", ENTRY_POINT_NAMES)
@respx.mock(assert_all_called=False)
def test_every_entry_point_refuses_a_bogus_type_before_any_request(
    monkeypatched_registry, tmp_path, name
):
    call = _entry_points(tmp_path)[name]
    with pytest.raises(NotImplementedError, match="Valid values"):
        call("bogus")
    assert respx.calls.call_count == 0


# ---------- the flag's parser ----------

@pytest.mark.parametrize("raw", ["yess", "enable", "2", "on!"])
def test_flag_typo_raises_instead_of_silently_gating(monkeypatch, raw):
    """A misspelt opt-in must not quietly fall back to 'refuse' (the user
    thinks they opted in) or to 'allow' (the gate is defeated by a typo)."""
    monkeypatch.setenv("HIPPIUS_EXPERIMENTAL_REPO_TYPES", raw)
    with pytest.raises(ValueError, match="HIPPIUS_EXPERIMENTAL_REPO_TYPES"):
        _oci_repo_path(REPO, "dataset")


@pytest.mark.parametrize("raw", ["0", "false", "no", "off", "", "  ", "FALSE"])
def test_flag_falsy_or_blank_keeps_the_gate(monkeypatch, raw):
    monkeypatch.setenv("HIPPIUS_EXPERIMENTAL_REPO_TYPES", raw)
    with pytest.raises(NotImplementedError, match="Omit repo_type"):
        _oci_repo_path(REPO, "space")


@pytest.mark.parametrize("raw", ["1", "true", "yes", "on", "TRUE", " yes "])
def test_flag_truthy_opens_the_gate(monkeypatch, raw):
    monkeypatch.setenv("HIPPIUS_EXPERIMENTAL_REPO_TYPES", raw)
    assert _oci_repo_path(REPO, "dataset") == "datasets/foo/bar"
    assert _oci_repo_path(REPO, "space") == "spaces/foo/bar"


@pytest.mark.parametrize("repo_type", [None, "model"])
def test_model_repos_never_consult_the_flag(monkeypatch, repo_type):
    """A typo in the experimental flag must not break the default repo type:
    the flag is read only on the dataset/space branch."""
    monkeypatch.setenv("HIPPIUS_EXPERIMENTAL_REPO_TYPES", "yess")
    assert _oci_repo_path(REPO, repo_type) == REPO


def test_double_prefix_check_runs_after_the_gate(monkeypatch):
    """With the gate closed the foot-gun check is unreachable: the user gets
    the actionable 'omit repo_type' refusal, not 'already starts with'."""
    with pytest.raises(NotImplementedError, match="Omit repo_type"):
        _oci_repo_path("datasets/foo", "dataset")


# ---------- cache-first paths keep working for gated types ----------

def _seed_cache(tmp_path, repo_type):
    cache_dir = tmp_path / "cache"
    cached = cache_dir / _cache_dirname(REPO, repo_type) / "snapshots" / "main" / "x.bin"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"cached")
    return cache_dir, cached


@pytest.mark.parametrize("repo_type", GATED_TYPES)
def test_hf_hub_download_cache_hit_and_local_files_only_bypass_the_gate(tmp_path, repo_type):
    cache_dir, cached = _seed_cache(tmp_path, repo_type)
    assert file_download.hf_hub_download(
        REPO, "x.bin", repo_type=repo_type, cache_dir=cache_dir) == str(cached)
    assert file_download.hf_hub_download(
        REPO, "x.bin", repo_type=repo_type, cache_dir=cache_dir, local_files_only=True,
    ) == str(cached)


@pytest.mark.parametrize("repo_type", GATED_TYPES)
def test_hf_hub_download_local_files_only_miss_is_the_cache_error_not_the_gate(
    tmp_path, repo_type
):
    """`local_files_only=True` promises no registry contact, so the gate (a
    registry-permissions concern) must never be the error a cache miss raises."""
    with pytest.raises(LocalEntryNotFoundError):
        file_download.hf_hub_download(
            REPO, "x.bin", repo_type=repo_type, cache_dir=str(tmp_path), local_files_only=True,
        )


@pytest.mark.parametrize("repo_type", GATED_TYPES)
def test_hf_hub_download_force_download_of_a_cached_file_hits_the_gate(tmp_path, repo_type):
    """`force_download` bypasses the cache, so the registry WOULD be contacted:
    the gate must fire even though the file is cached."""
    cache_dir, _ = _seed_cache(tmp_path, repo_type)
    with pytest.raises(NotImplementedError, match="Omit repo_type"):
        file_download.hf_hub_download(
            REPO, "x.bin", repo_type=repo_type, cache_dir=cache_dir, force_download=True,
        )


@pytest.mark.parametrize("repo_type", GATED_TYPES)
def test_snapshot_download_local_files_only_and_dry_run_bypass_the_gate(tmp_path, repo_type):
    cache_dir, cached = _seed_cache(tmp_path, repo_type)
    snapshot_dir = str(cached.parent)
    assert _snapshot_download.snapshot_download(
        REPO, repo_type=repo_type, cache_dir=cache_dir, local_files_only=True,
    ) == snapshot_dir
    assert _snapshot_download.snapshot_download(
        REPO, repo_type=repo_type, cache_dir=cache_dir, dry_run=True,
    ) == snapshot_dir


@pytest.mark.parametrize("repo_type", GATED_TYPES)
def test_snapshot_download_local_files_only_miss_is_the_cache_error_not_the_gate(
    tmp_path, repo_type
):
    with pytest.raises(LocalEntryNotFoundError):
        _snapshot_download.snapshot_download(
            REPO, repo_type=repo_type, cache_dir=str(tmp_path), local_files_only=True,
        )


@pytest.mark.parametrize("repo_type", GATED_TYPES)
def test_try_to_load_from_cache_never_consults_the_gate(tmp_path, repo_type):
    cache_dir, cached = _seed_cache(tmp_path, repo_type)
    assert file_download.try_to_load_from_cache(
        REPO, "x.bin", cache_dir=str(cache_dir), repo_type=repo_type) == str(cached)


# ---------- CLI: one line + exit 1, no traceback, no network ----------

@pytest.mark.parametrize("argv, match", [
    (["hippius-hub", "revisions", REPO, "--repo-type", "bogus"], "Valid values"),
    (["hippius-hub", "revisions", REPO, "--repo-type", "dataset"], "Omit repo_type"),
    (["hippius-hub", "revisions", REPO, "--repo-type", "space"], "Omit repo_type"),
    (["hippius-hub", "registry", "repos", "delete", REPO, "--repo-type", "bogus", "--yes"],
     "Valid values"),
    (["hippius-hub", "registry", "repos", "delete", REPO, "--repo-type", "space", "--yes"],
     "Omit repo_type"),
])
@respx.mock(assert_all_called=False)
def test_cli_unsupported_repo_type_exits_1_without_traceback(
    monkeypatched_registry, monkeypatch, capsys, argv, match
):
    monkeypatch.setattr("sys.argv", argv)
    with pytest.raises(SystemExit) as got:
        cli.main()
    assert got.value.code == 1
    captured = capsys.readouterr()
    assert "Traceback" not in captured.out and "Traceback" not in captured.err
    assert "❌" in captured.out
    assert match in captured.out
    assert respx.calls.call_count == 0, "the CLI contacted the registry before the gate"


def test_cli_revisions_inline_not_found_reaches_the_shell_as_11(monkeypatch, capsys):
    """The inline `sys.exit(EXIT_REPO_NOT_FOUND)` in `cmd_revisions` must reach
    the shell through `main()` untouched (SystemExit is not in the backstop)."""
    monkeypatch.setattr("sys.argv", ["hippius-hub", "revisions", REPO])
    monkeypatch.setattr(cli, "resolve_token_value", lambda *_a, **_k: "tok")
    monkeypatch.setattr(cli, "get_oci_bearer_token", lambda *_a, **_k: "oci")
    monkeypatch.setattr(cli, "_list_tags", lambda *_a, **_k: None)
    with pytest.raises(SystemExit) as got:
        cli.main()
    assert got.value.code == cli.EXIT_REPO_NOT_FOUND == 11
    assert "not found" in capsys.readouterr().out.lower()


def test_cli_repos_delete_404_reaches_the_shell_as_11(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["hippius-hub", "registry", "repos", "delete", REPO, "--yes"])
    resp = httpx.Response(404, request=httpx.Request("DELETE", "https://x/y"))

    def _gone(*_a, **_k):
        raise httpx.HTTPStatusError("no", request=resp.request, response=resp)

    monkeypatch.setattr(cli, "delete_repo", _gone)
    with pytest.raises(SystemExit) as got:
        cli.main()
    assert got.value.code == cli.EXIT_REPO_NOT_FOUND == 11
    assert "not found" in capsys.readouterr().out.lower()


def test_cli_typed_not_found_from_diagnose_reaches_the_shell_as_11(monkeypatch, capsys):
    """The typed dispatch route: a RepositoryNotFoundError out of any `handlers`
    command (here `diagnose`) is rendered by `_format_download_error` with the
    same constant the inline sites use."""
    monkeypatch.setattr("sys.argv", ["hippius-hub", "diagnose", REPO, "x.bin"])
    err = RepositoryNotFoundError(
        "gone", response=httpx.Response(404, request=httpx.Request("GET", "about:blank")))

    def _raise(_args):
        raise err

    monkeypatch.setattr(cli, "_cmd_diagnose", _raise)
    with pytest.raises(SystemExit) as got:
        cli.main()
    assert got.value.code == cli.EXIT_REPO_NOT_FOUND == 11
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert "Repository not found" in captured.out


def test_format_download_error_renders_the_gate_refusal_as_exit_1():
    exc = NotImplementedError(
        "repo_type='dataset' is not supported on Hippius yet — Omit repo_type")
    msg, code = cli._format_download_error(exc)
    assert code == 1
    assert msg.startswith("❌")
    assert "Omit repo_type" in msg


def test_cmd_revisions_gate_fires_before_the_token_call(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "get_oci_bearer_token", lambda *a, **k: calls.append("token"))
    args = argparse.Namespace(repo_id=REPO, repo_type="space", json=False)
    with pytest.raises(NotImplementedError, match="Omit repo_type"):
        cli.cmd_revisions(args)
    assert calls == []
