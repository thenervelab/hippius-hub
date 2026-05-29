"""RES-1: a failed/interrupted ``local_dir`` download must not leave a file at
the user's path.

The Rust downloader pre-allocates the destination at full size
(``f.set_len(content_length)``) and streams chunks into it in place. If a chunk
fails after that, the old ``_download_to_local_dir`` left a full-size file full
of holes at the user's path — and because ``hf_hub_download`` short-circuits on
``os.path.exists(dest_file)``, the next call returned that corrupt file as a
cache hit. These are pure unit tests (no network): they fake
``download_file_native`` to reproduce the pre-allocate-then-fail shape.
"""
import os

import pytest

import hippius_hub.file_download as fd


def _prealloc_then_fail(*, url, dest_path, **kwargs):
    """Stand-in for the Rust native download: writes a full-size file (as
    ``set_len`` would) and then fails mid-stream like a dropped chunk."""
    with open(dest_path, "wb") as f:
        f.write(b"\x00" * 4096)
    raise RuntimeError("chunk 3 failed after retries")


def test_failed_local_dir_download_leaves_no_file(tmp_path, monkeypatch):
    monkeypatch.setattr(fd, "download_file_native", _prealloc_then_fail)
    dest = tmp_path / "model.bin"

    with pytest.raises(RuntimeError, match="chunk 3 failed"):
        fd._download_to_local_dir("https://registry.example/blob", str(dest), "tok")

    assert not dest.exists(), (
        "a failed local_dir download must not leave a partial file at the "
        "user's path — it would be served as a cache hit on the next call"
    )


def test_failed_local_dir_download_leaves_no_temp_siblings(tmp_path, monkeypatch):
    monkeypatch.setattr(fd, "download_file_native", _prealloc_then_fail)
    dest = tmp_path / "sub" / "model.bin"

    with pytest.raises(RuntimeError):
        fd._download_to_local_dir("https://registry.example/blob", str(dest), "tok")

    leftovers = sorted(p.name for p in (tmp_path / "sub").iterdir())
    assert leftovers == [], f"temp files leaked into the local dir: {leftovers}"


def test_successful_local_dir_download_writes_dest(tmp_path, monkeypatch):
    def _ok(*, url, dest_path, **kwargs):
        with open(dest_path, "wb") as f:
            f.write(b"payload")
        return None

    monkeypatch.setattr(fd, "download_file_native", _ok)
    dest = tmp_path / "model.bin"

    out = fd._download_to_local_dir("https://registry.example/blob", str(dest), "tok")

    assert out == str(dest)
    assert dest.read_bytes() == b"payload"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["model.bin"]
