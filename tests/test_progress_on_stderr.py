"""Upload progress lines go to stderr; stdout stays the caller's (issue #137).

A Python program that calls `upload_folder` and prints its own results, or a shell
running `hippius-hub upload ... > out`, must not find transfer chatter on stdout.
The registry calls are stubbed at the module boundary; the progress lines printed
around them are what these tests pin. The download lines are pinned beside the
routing tests in `test_chunked_download_routing.py` and
`test_download_local_dir_atomic.py`.
"""
from __future__ import annotations

from types import SimpleNamespace

from hippius_hub import auth
from hippius_hub import file_upload as fu


def _stub_registry(monkeypatch):
    """Stub every registry call `upload_folder` makes; the blob is already stored."""
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "0")
    auth.clear_oci_token_cache()
    monkeypatch.setattr(auth, "get_oci_bearer_token", lambda *a, **k: "tok")
    monkeypatch.setattr(fu, "hash_file_native", lambda path: ("a" * 64, 5))
    monkeypatch.setattr(fu, "_ensure_blob_uploaded", lambda *a, **k: False)
    monkeypatch.setattr(fu, "fetch_manifest", lambda *a, **k: None)
    monkeypatch.setattr(fu, "_ensure_config_blob_uploaded", lambda *a, **k: ("sha256:" + "c" * 64, 2))
    monkeypatch.setattr(fu, "_put_manifest", lambda *a, **k: SimpleNamespace())
    monkeypatch.setattr(fu, "_build_commit_info", lambda *a, **k: "commit-ok")


def test_upload_folder_prints_progress_on_stderr_only(monkeypatch, capsys, tmp_path):
    _stub_registry(monkeypatch)
    folder = tmp_path / "repo"
    folder.mkdir()
    (folder / "a.bin").write_bytes(b"hello")

    result = fu.upload_folder(repo_id="acme/model", folder_path=str(folder), repo_type="model")

    assert result == "commit-ok"
    captured = capsys.readouterr()
    assert captured.out == ""
    for line in (
        "Preparing to upload 1 file(s)",
        "Uploading: a.bin",
        "Already published (skipped): a.bin",
        "Uploaded: a.bin",
        "Publishing OCI Manifest",
        "Successfully pushed 1 file(s)",
    ):
        assert line in captured.err, line
