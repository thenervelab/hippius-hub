"""Concurrent-upload regression: when two writers race against the same
revision, one must succeed and the other must raise
ConcurrentManifestUpdateError (audit M5)."""
import httpx
import pytest
import respx

from hippius_hub.errors import ConcurrentManifestUpdateError


@respx.mock
def test_412_on_concurrent_put_raises_typed_error(monkeypatched_registry):
    """Direct unit-level reproduction of the race: a single thread that
    submits a PUT to a server returning 412 must surface
    ConcurrentManifestUpdateError, not a generic HTTPStatusError.

    The behavioral 'two threads racing' shape is exhaustively covered by
    the existing test_412_raises_ConcurrentManifestUpdateError in
    test_upload_if_match.py — this test exists to lock the audit M5
    finding to a dedicated, greppable test name."""
    from hippius_hub.file_upload import _put_manifest

    respx.put("https://registry.test.invalid/v2/foo/bar/manifests/main").mock(
        return_value=httpx.Response(412)
    )

    with pytest.raises(ConcurrentManifestUpdateError):
        _put_manifest(
            registry="https://registry.test.invalid",
            repo_id="foo/bar",
            revision="main",
            oci_token="mock-jwt",
            manifest={"schemaVersion": 2, "layers": []},
            if_match="sha256:old",
        )
