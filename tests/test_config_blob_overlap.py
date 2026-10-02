"""Failure paths of the config-blob side thread in `upload_file` (0.7.2).

`_start_config_blob_upload` runs `_ensure_config_blob_uploaded` on a daemon
thread and hands back a join that re-raises the worker's exception unchanged
on the caller's thread. test_chunked_v2_upload.py pins the happy overlap, the
daemon flag, the join-before-manifest ordering and a generic RuntimeError.
This file pins what the join must carry for the wrapper stack ABOVE it to
keep working:

* a 401 raised on the side thread reaches `call_with_oci_token_refresh`, which
  refreshes the token exactly once and re-runs the whole operation;
* a second 401 (fresh token also rejected) propagates as the SAME object, and
  no manifest is PUT;
* the native-extension 401 shape (a RuntimeError whose message says
  `server returned 401`) is refreshed the same way;
* a KeyboardInterrupt raised inside the worker is not swallowed by the
  `BaseException` capture - it surfaces on the caller, with no manifest;
* when the pack wave fails first, THAT error surfaces and the config worker
  is abandoned, never joined.

Every stub records its thread and the test joins it before returning: an
abandoned daemon worker would otherwise outlive this test's respx routes and
land its HEAD/PUT retries in a later test.
"""
from __future__ import annotations

import threading

import httpx
import pytest
import respx

from hippius_hub import file_upload
from hippius_hub.file_upload import upload_file

from tests.test_chunked_v2_upload import REPO, _wire_registry


def _chunked_env(monkeypatch):
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1")
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "40")


def _unauthorized() -> httpx.HTTPStatusError:
    resp = httpx.Response(401, request=httpx.Request("HEAD", "https://x/v2/blobs/sha256:cfg"))
    return httpx.HTTPStatusError("401", request=resp.request, response=resp)


def _token_calls() -> int:
    return sum(1 for c in respx.calls if "/service/token" in str(c.request.url))


def _reap(workers):
    for t in workers:
        t.join(5)
        assert not t.is_alive(), "config worker must have finished"


@respx.mock
def test_config_blob_401_on_the_side_thread_refreshes_the_token_once(monkeypatch, tmp_path):
    _chunked_env(monkeypatch)
    captured = {}
    _wire_registry(monkeypatch, captured)

    real_config = file_upload._ensure_config_blob_uploaded
    seen = []
    workers = []

    def _config(*args, **kwargs):
        workers.append(threading.current_thread())
        seen.append(args)
        if len(seen) == 1:
            raise _unauthorized()
        return real_config(*args, **kwargs)

    monkeypatch.setattr(file_upload, "_ensure_config_blob_uploaded", _config)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")
    _reap(workers)

    assert len(seen) == 2, "the whole operation re-runs once after the refresh"
    assert _token_calls() == 2, "exactly one token refresh (initial mint + one re-mint)"
    assert "manifest" in captured, "the retried operation must commit"


@respx.mock
def test_config_blob_second_401_propagates_unchanged_without_a_manifest(monkeypatch, tmp_path):
    _chunked_env(monkeypatch)
    captured = {}
    _wire_registry(monkeypatch, captured)

    boom = _unauthorized()
    workers = []

    def _config(*args, **kwargs):
        workers.append(threading.current_thread())
        raise boom

    monkeypatch.setattr(file_upload, "_ensure_config_blob_uploaded", _config)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")
    _reap(workers)

    assert exc_info.value is boom, "the worker's exception must surface as the same object"
    assert len(workers) == 2, "one refresh, then the second 401 is final"
    assert _token_calls() == 2
    assert "manifest" not in captured, "no manifest may be PUT after the config blob failed"


@respx.mock
def test_config_blob_native_401_message_is_refreshed_too(monkeypatch, tmp_path):
    """`_is_oci_auth_error` duck-types the native extension's 401 (a
    RuntimeError carrying `server returned 401`); the join must not wrap or
    re-type it, or the refresh path stops recognising it."""
    _chunked_env(monkeypatch)
    captured = {}
    _wire_registry(monkeypatch, captured)

    real_config = file_upload._ensure_config_blob_uploaded
    seen = []
    workers = []

    def _config(*args, **kwargs):
        workers.append(threading.current_thread())
        seen.append(args)
        if len(seen) == 1:
            raise RuntimeError("server returned 401 (Unauthorized) for blob upload")
        return real_config(*args, **kwargs)

    monkeypatch.setattr(file_upload, "_ensure_config_blob_uploaded", _config)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")
    _reap(workers)

    assert len(seen) == 2
    assert _token_calls() == 2
    assert "manifest" in captured


@respx.mock
def test_keyboard_interrupt_inside_the_config_worker_surfaces_on_the_caller(monkeypatch, tmp_path):
    """The worker captures BaseException so nothing is lost to
    `threading.excepthook`; a KeyboardInterrupt must still be re-raised as a
    KeyboardInterrupt (not downgraded, not swallowed) and must not be
    mistaken for a 401 by the refresh wrapper."""
    _chunked_env(monkeypatch)
    captured = {}
    _wire_registry(monkeypatch, captured)

    workers = []

    def _config(*args, **kwargs):
        workers.append(threading.current_thread())
        raise KeyboardInterrupt

    monkeypatch.setattr(file_upload, "_ensure_config_blob_uploaded", _config)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    with pytest.raises(KeyboardInterrupt):
        upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")
    _reap(workers)

    assert len(workers) == 1, "an interrupt is not an auth error; no refresh re-run"
    assert _token_calls() == 1
    assert "manifest" not in captured


@respx.mock
def test_pack_wave_failure_surfaces_and_the_config_worker_is_abandoned(monkeypatch, tmp_path):
    """Both sides fail: the pack error is raised from `_upload_file_layers`
    BEFORE the join, so it - not the config error - is the upload's failure,
    and the join is never called on the error path."""
    _chunked_env(monkeypatch)
    captured = {}
    _wire_registry(monkeypatch, captured)

    config_started = threading.Event()
    config_release = threading.Event()
    workers = []

    def _config(*args, **kwargs):
        workers.append(threading.current_thread())
        config_started.set()
        config_release.wait(5)
        raise RuntimeError("config boom")

    def _pack(uploads_url, path, ranges, auth_token):
        assert config_started.wait(2), "config never started"
        raise RuntimeError("pack boom")

    monkeypatch.setattr(file_upload, "_ensure_config_blob_uploaded", _config)
    monkeypatch.setattr(file_upload, "pack_upload_native", _pack)

    src = tmp_path / "big.bin"
    src.write_bytes(b"x" * 100)
    with pytest.raises(RuntimeError, match="pack boom"):
        upload_file(path_or_fileobj=str(src), path_in_repo="big.bin", repo_id=REPO, token="tok")
    assert workers[0].is_alive(), "the config worker is abandoned, not joined, on the error path"
    config_release.set()
    _reap(workers)
    assert "manifest" not in captured


def test_start_config_blob_upload_join_reraises_the_same_object_and_is_repeatable(monkeypatch):
    """Unit pin of the seam itself: the join re-raises the identical exception
    object (identity matters for `_is_oci_auth_error` / `pytest.raises(...) is`)
    and a second join raises it again rather than returning a half-result."""
    boom = ValueError("cfg")

    def _fail(registry, repo_id, oci_token):
        raise boom

    monkeypatch.setattr(file_upload, "_ensure_config_blob_uploaded", _fail)
    join = file_upload._start_config_blob_upload("https://r", "ns/repo", "tok")
    with pytest.raises(ValueError) as first:
        join()
    with pytest.raises(ValueError) as second:
        join()
    assert first.value is boom and second.value is boom


def test_start_config_blob_upload_passes_arguments_through_unchanged(monkeypatch):
    seen = {}

    def _ok(registry, repo_id, oci_token):
        seen["args"] = (registry, repo_id, oci_token)
        seen["thread"] = threading.current_thread()
        return ("sha256:cfg", 2)

    monkeypatch.setattr(file_upload, "_ensure_config_blob_uploaded", _ok)
    join = file_upload._start_config_blob_upload("https://r", "ns/repo", "tok")
    assert join() == ("sha256:cfg", 2)
    assert seen["args"] == ("https://r", "ns/repo", "tok")
    assert seen["thread"].daemon and seen["thread"].name == "hippius-config-blob"
