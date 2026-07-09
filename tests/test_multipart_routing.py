"""Routing between the parallel receiver path and the legacy single PUT.

The dedup HEAD is shared; the branch after it is what these tests pin. The
native upload functions are spied (not exercised) so the tests assert *which*
path was chosen and with what arguments, without needing a live receiver or
registry. The key safety property — receiver unset means the legacy path,
byte-for-byte as before — is covered explicitly.
"""
from unittest.mock import Mock

import httpx
import respx

from hippius_hub import file_upload
from hippius_hub.file_upload import _ensure_blob_uploaded, _should_use_multipart

REGISTRY = "https://registry.test"
REPO = "proj/model"
TOKEN = "tok"
DIGEST_URL = f"{REGISTRY}/v2/{REPO}/blobs/sha256:abc"
UPLOADS_URL = f"{REGISTRY}/v2/{REPO}/blobs/uploads/"
BIG = 300 * 1024 * 1024
SMALL = 10 * 1024 * 1024


def _install_native_spies(monkeypatch):
    multipart = Mock()
    single = Mock()
    monkeypatch.setattr(file_upload, "upload_blob_multipart_native", multipart)
    monkeypatch.setattr(file_upload, "upload_blob_native", single)
    return {"multipart": multipart, "single": single}


def test_should_use_multipart_requires_receiver_and_threshold(monkeypatch):
    monkeypatch.delenv("HIPPIUS_MULTIPART_THRESHOLD", raising=False)  # default 256 MB
    assert _should_use_multipart(BIG, "https://r") is True
    assert _should_use_multipart(BIG, None) is False, "no receiver -> never multipart"
    assert _should_use_multipart(SMALL, "https://r") is False, "below threshold -> single put"


@respx.mock
def test_routes_to_multipart_when_eligible(monkeypatch):
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "https://receiver.test")
    monkeypatch.delenv("HIPPIUS_MULTIPART_THRESHOLD", raising=False)
    monkeypatch.delenv("HIPPIUS_MULTIPART_PART_SIZE", raising=False)
    spies = _install_native_spies(monkeypatch)
    respx.head(DIGEST_URL).mock(return_value=httpx.Response(404))

    result = _ensure_blob_uploaded(REGISTRY, REPO, TOKEN, "/tmp/x", "abc", BIG)

    assert result is True
    assert spies["multipart"].called
    assert not spies["single"].called
    _, kwargs = spies["multipart"].call_args
    assert kwargs["base_url"] == "https://receiver.test"
    assert kwargs["repo"] == REPO
    assert kwargs["digest"] == "sha256:abc"
    assert kwargs["size"] == BIG
    assert kwargs["part_size"] == 64 * 1024 * 1024
    assert kwargs["auth_token"] == TOKEN


@respx.mock
def test_routes_to_single_put_when_receiver_unset(monkeypatch):
    # The load-bearing safety property: with no receiver, the legacy path runs
    # even for a huge blob — behavior is unchanged from before the feature.
    monkeypatch.delenv("HIPPIUS_RECEIVER_URL", raising=False)
    spies = _install_native_spies(monkeypatch)
    respx.head(DIGEST_URL).mock(return_value=httpx.Response(404))
    respx.post(UPLOADS_URL).mock(
        return_value=httpx.Response(202, headers={"Location": f"{UPLOADS_URL}uuid"})
    )

    result = _ensure_blob_uploaded(REGISTRY, REPO, TOKEN, "/tmp/x", "abc", BIG)

    assert result is True
    assert spies["single"].called
    assert not spies["multipart"].called


@respx.mock
def test_below_threshold_uses_single_put_even_with_receiver(monkeypatch):
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "https://receiver.test")
    monkeypatch.delenv("HIPPIUS_MULTIPART_THRESHOLD", raising=False)
    spies = _install_native_spies(monkeypatch)
    respx.head(DIGEST_URL).mock(return_value=httpx.Response(404))
    respx.post(UPLOADS_URL).mock(
        return_value=httpx.Response(202, headers={"Location": f"{UPLOADS_URL}uuid"})
    )

    result = _ensure_blob_uploaded(REGISTRY, REPO, TOKEN, "/tmp/x", "abc", SMALL)

    assert result is True
    assert spies["single"].called
    assert not spies["multipart"].called


@respx.mock
def test_dedup_hit_skips_both_paths(monkeypatch):
    monkeypatch.setenv("HIPPIUS_RECEIVER_URL", "https://receiver.test")
    spies = _install_native_spies(monkeypatch)
    respx.head(DIGEST_URL).mock(return_value=httpx.Response(200))

    result = _ensure_blob_uploaded(REGISTRY, REPO, TOKEN, "/tmp/x", "abc", BIG)

    assert result is False, "already-published blob is skipped"
    assert not spies["single"].called
    assert not spies["multipart"].called
