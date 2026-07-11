"""Fan-out fail-fast (audit M3) + dedup-index gate (audit L10) regression tests.

M3 wraps the `snapshot_download` / `upload_folder` `ThreadPoolExecutor` loops in
``except BaseException: executor.shutdown(wait=False, cancel_futures=True); raise``
so the first worker failure (or Ctrl-C) cancels queued transfers instead of
draining the whole repo before the error surfaces. The remediation plan promised
these tests ("injected failure cancels queued futures — assert not all ran") and
the CHANGELOG advertises the behavior; nothing else pins it, so a refactor to
``except Exception`` or dropping ``cancel_futures=True`` would silently regress.

L10 gates the chunked-v2 dedup-index build (a manifest GET + N pointer-blob GETs)
behind "at least one file will actually chunk", so a folder of only-small files
skips it entirely. That gate is a new behavioral branch with no other coverage,
and a regression (``any`` → ``all``, ``>=`` → ``>``) is invisible because the
upload still succeeds either way.

The blocking workers use a BOUNDED wait: the executor's ``__exit__`` runs an
implicit ``shutdown(wait=True)`` that awaits already-running workers, so an
unbounded wait on the never-set release event would hang the test on the failure
path. The bound caps the worst case instead.
"""
from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from hippius_hub import _snapshot_download as sd
from hippius_hub import file_upload as fu

# Enough files that, with max_workers=2, the vast majority are queued (never
# started) and therefore cancellable — makes "not all ran" a strong signal.
FAN = 40
WORKERS = 2
# Cap on how long a blocked worker waits, so the executor teardown can't hang.
BLOCK_TIMEOUT = 2.0


def _write_files(folder, n, size=8):
    folder.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        (folder / f"f{i}.bin").write_bytes(b"x" * size)


def test_snapshot_download_fail_fast_cancels_queued(monkeypatch, tmp_path):
    names = [f"f{i}.bin" for i in range(FAN)]
    monkeypatch.setattr(sd, "get_oci_bearer_token", lambda *a, **k: "tok")
    monkeypatch.setattr(sd, "fetch_manifest", lambda *a, **k: SimpleNamespace(manifest={}))
    monkeypatch.setattr(sd, "group_files", lambda manifest: [SimpleNamespace(title=x) for x in names])

    started: list[str] = []
    lock = threading.Lock()
    release = threading.Event()

    def fake_dl(**kw):
        # The FIRST worker to enter raises (independent of scheduling/submit order);
        # the rest block on a bounded wait so a real cancellation shows up as fewer
        # than FAN starts without risking a hang.
        with lock:
            idx = len(started)
            started.append(kw["filename"])
        if idx == 0:
            raise RuntimeError("boom")
        release.wait(timeout=BLOCK_TIMEOUT)
        return kw["filename"]

    monkeypatch.setattr(sd, "hf_hub_download", fake_dl)

    with pytest.raises(RuntimeError, match="boom"):
        sd.snapshot_download(
            repo_id="acme/model", repo_type="model", cache_dir=str(tmp_path), max_workers=WORKERS
        )
    release.set()
    assert len(started) < FAN, f"queued downloads must be cancelled; {len(started)}/{FAN} started"


def test_upload_folder_fail_fast_cancels_queued(monkeypatch, tmp_path):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "0")  # plain path — no dedup-index fetch
    folder = tmp_path / "repo"
    _write_files(folder, FAN)

    monkeypatch.setattr(fu, "_oci_bearer", lambda *a, **k: "tok")

    def no_finalize(**_):
        raise AssertionError("manifest must NOT be finalized on a failed folder upload")

    monkeypatch.setattr(fu, "_finalize_upload_manifest", no_finalize)

    started: list[str] = []
    lock = threading.Lock()
    release = threading.Event()

    def fake_one(**kw):
        with lock:
            idx = len(started)
            started.append(kw["rel_path"])
        if idx == 0:
            raise RuntimeError("boom")
        release.wait(timeout=BLOCK_TIMEOUT)
        return []

    monkeypatch.setattr(fu, "_upload_one_file", fake_one)

    with pytest.raises(RuntimeError, match="boom"):
        fu.upload_folder(
            repo_id="acme/model", folder_path=str(folder), repo_type="model", max_workers=WORKERS
        )
    release.set()
    assert len(started) < FAN, f"queued uploads must be cancelled; {len(started)}/{FAN} started"


def test_l10_dedup_index_skipped_for_small_only_folder(monkeypatch, tmp_path):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1024")
    folder = tmp_path / "small"
    _write_files(folder, 3, size=8)  # all well under the 1024-byte threshold

    monkeypatch.setattr(fu, "_oci_bearer", lambda *a, **k: "tok")
    calls = {"manifest": 0, "dedup": 0}

    def spy_manifest(*a, **k):
        calls["manifest"] += 1
        return None

    def spy_dedup(*a, **k):
        calls["dedup"] += 1
        return ({}, {})

    monkeypatch.setattr(fu, "fetch_manifest", spy_manifest)
    monkeypatch.setattr(fu, "_build_dedup_index", spy_dedup)
    monkeypatch.setattr(fu, "_upload_one_file", lambda **kw: [])
    monkeypatch.setattr(fu, "_finalize_upload_manifest", lambda **k: SimpleNamespace())

    fu.upload_folder(repo_id="acme/model", folder_path=str(folder), repo_type="model", max_workers=WORKERS)
    assert calls["manifest"] == 0, "no file chunks → the dedup-index manifest GET must be skipped"
    assert calls["dedup"] == 0, "no file chunks → _build_dedup_index must be skipped"


def test_l10_dedup_index_built_when_a_file_chunks(monkeypatch, tmp_path):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "1")
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "1024")
    folder = tmp_path / "mixed"
    _write_files(folder, 2, size=8)
    (folder / "big.bin").write_bytes(b"x" * 2048)  # >= threshold → takes the chunked path

    monkeypatch.setattr(fu, "_oci_bearer", lambda *a, **k: "tok")
    calls = {"manifest": 0, "dedup": 0}

    def spy_manifest(*a, **k):
        calls["manifest"] += 1
        return None

    def spy_dedup(*a, **k):
        calls["dedup"] += 1
        return ({}, {})

    monkeypatch.setattr(fu, "fetch_manifest", spy_manifest)
    monkeypatch.setattr(fu, "_build_dedup_index", spy_dedup)
    monkeypatch.setattr(fu, "_upload_one_file", lambda **kw: [])
    monkeypatch.setattr(fu, "_finalize_upload_manifest", lambda **k: SimpleNamespace())

    fu.upload_folder(repo_id="acme/model", folder_path=str(folder), repo_type="model", max_workers=WORKERS)
    assert calls["manifest"] == 1, "a chunking file must trigger exactly one dedup-index manifest GET"
    assert calls["dedup"] == 1, "a chunking file must build the dedup index once"
