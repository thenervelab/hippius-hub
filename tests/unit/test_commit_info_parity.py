"""Parity tests for `file_upload._build_commit_info`.

HF's `CommitInfo.__post_init__` derives `repo_url` by splitting `commit_url` on
`/commit/` and feeding the prefix to `RepoUrl`. `_build_commit_info` must
therefore pass a prefix that (a) huggingface_hub 1.21's parser accepts and
(b) carries the correct repo_type. We build it from the type-prefixed
`oci_repo` (`datasets/…`/`spaces/…`), so `CommitInfo.repo_url` resolves the
right repo_id AND repo_type — a model commit_url would otherwise mis-type every
dataset/space commit as a model. No network or credentials.
"""
import pytest

from hippius_hub.file_upload import _build_commit_info
from hippius_hub.file_download import _oci_repo_path

EP = "https://registry.hippius.com"
REPO_ID = "test/e2e-client"

TYPE_CASES = [
    (None, "model"),
    ("dataset", "dataset"),
    ("space", "space"),
]


class _FakeResponse:
    """Minimal stand-in: _build_commit_info only reads response.headers.get()."""
    def __init__(self, digest="sha256:deadbeef"):
        self.headers = {"Docker-Content-Digest": digest}


@pytest.mark.parametrize("repo_type, expected_type", TYPE_CASES)
def test_commit_info_repo_url_parity(repo_type, expected_type):
    oci_repo = _oci_repo_path(REPO_ID, repo_type)
    ci = _build_commit_info(EP, oci_repo, "main", _FakeResponse(), "msg", "")

    assert "/v2/" not in ci.commit_url
    assert ci.oid == "sha256:deadbeef"
    # CommitInfo.repo_url is the HF-parsed RepoUrl built from commit_url.
    assert ci.repo_url.repo_id == REPO_ID
    assert ci.repo_url.repo_type == expected_type
    assert ci.repo_url.namespace == "test"
    assert ci.repo_url.repo_name == "e2e-client"


def test_commit_info_oid_falls_back_to_revision():
    """When the registry omits Docker-Content-Digest, oid is the revision."""
    ci = _build_commit_info(EP, REPO_ID, "main", _FakeResponse(digest=""), "m", "")
    assert ci.oid == "main"
    assert ci.repo_url.repo_id == REPO_ID
