"""Forward-compatibility guard for `com.hippius.layout`.

The guard is the floor that lets us change the artifact layout without silently
corrupting downloads on older clients: a build that doesn't recognize the layout
value refuses the manifest loudly (`UnsupportedLayoutError`) instead of misreading
a chunked file's pointer blob as the file itself. These tests pin both the pure
decision (`_guard_layout`) and its enforcement on the real read path
(`fetch_manifest`), plus the invariant that every value THIS build advertises in
`KNOWN_LAYOUTS` is actually accepted.
"""
import httpx
import pytest
import respx

from hippius_hub._oci import _guard_layout, fetch_manifest
from hippius_hub.constants import KNOWN_LAYOUTS, LAYOUT_ANNOTATION_KEY
from hippius_hub.errors import UnsupportedLayoutError

from tests.respx_fixtures import MOCK_REGISTRY, manifest_get_route, token_route

# A value that must never be in KNOWN_LAYOUTS, so the "unknown → raise" assertions
# stay valid as the known set grows (Phase 1 adds "chunked-v1", etc.).
_BOGUS_LAYOUT = "chunked-v999-not-a-real-layout"


def _manifest(layout=None):
    """Minimal manifest, optionally carrying the layout annotation."""
    m = {"schemaVersion": 2, "layers": []}
    if layout is not None:
        m["annotations"] = {LAYOUT_ANNOTATION_KEY: layout}
    return m


def test_guard_passes_when_annotation_absent():
    # Every pre-chunking artifact has no layout annotation and must read cleanly.
    _guard_layout(_manifest(layout=None))


def test_guard_passes_when_annotations_empty():
    # An annotations block without our key is the same as absent.
    _guard_layout({"schemaVersion": 2, "annotations": {}, "layers": []})


def test_guard_rejects_unknown_layout():
    with pytest.raises(UnsupportedLayoutError, match=_BOGUS_LAYOUT):
        _guard_layout(_manifest(layout=_BOGUS_LAYOUT))


def test_guard_error_mentions_upgrade():
    # The message must tell the user what to do, not just that something failed.
    with pytest.raises(UnsupportedLayoutError, match="upgrade hippius-hub"):
        _guard_layout(_manifest(layout=_BOGUS_LAYOUT))


@pytest.mark.parametrize("layout", sorted(KNOWN_LAYOUTS))
def test_guard_accepts_every_known_layout(layout):
    # Whatever this build claims to support in KNOWN_LAYOUTS it must actually
    # accept — otherwise the client rejects artifacts it just wrote. (No cases
    # while the set is empty; grows automatically as layouts are added.)
    _guard_layout(_manifest(layout=layout))


@respx.mock
def test_fetch_manifest_enforces_guard(monkeypatched_registry):
    # The guard must fire on the real read chokepoint, not only when called
    # directly — an unknown layout reaching fetch_manifest raises before any
    # caller sees the (unreadable) manifest body.
    repo, revision = "acme/model", "main"
    token_route(respx.mock)
    url = f"{MOCK_REGISTRY}/v2/{repo}/manifests/{revision}"
    respx.get(url).mock(return_value=httpx.Response(200, json=_manifest(layout=_BOGUS_LAYOUT)))

    with pytest.raises(UnsupportedLayoutError):
        fetch_manifest(MOCK_REGISTRY, repo, revision, "tok")


@respx.mock
def test_fetch_manifest_passes_plain_layout(monkeypatched_registry):
    # A normal (annotation-free) manifest still round-trips through fetch_manifest.
    repo, revision = "acme/model", "main"
    url = f"{MOCK_REGISTRY}/v2/{repo}/manifests/{revision}"
    respx.get(url).mock(
        return_value=httpx.Response(
            200, json=_manifest(layout=None), headers={"Docker-Content-Digest": "sha256:" + "a" * 64}
        )
    )

    result = fetch_manifest(MOCK_REGISTRY, repo, revision, "tok")
    assert result.manifest["layers"] == []
    assert result.digest == "sha256:" + "a" * 64
