"""Parity + regression tests for `_repo_ops._build_repo_url`.

Why this exists: `create_repo` (and anything returning a RepoUrl) used to build
`RepoUrl(f"{base}/v2/{repo_id}")` — the OCI distribution API path. Old
huggingface_hub (<=1.14) parsed that leniently (silently yielding the wrong
`repo_id="v2/..."`); huggingface_hub 1.21 tightened RepoUrl's parser
(`parse_hf_uri`) to reject the extra `/v2/` segment with `HfUriError`, which
broke `create_repo` repo-wide. That only surfaced in the network `e2e` suite,
so these no-network tests pin the contract in the *standard* suite: our RepoUrl
must match the canonical shape huggingface_hub's own `HfApi.create_repo`
returns — `{endpoint}/[{type-prefix}]{namespace}/{name}` — for every repo_type,
across HF versions.

These are pure construction tests: no registry, no credentials, no network.
"""
import pytest
from huggingface_hub import RepoUrl
from huggingface_hub.constants import REPO_TYPES_URL_PREFIXES

from hippius_hub._repo_ops import _build_repo_url
from hippius_hub.file_download import _oci_repo_path

EP = "https://registry.hippius.com"
REPO_ID = "test/e2e-client"

# (repo_type passed to the hub, expected RepoUrl.repo_type)
TYPE_CASES = [
    (None, "model"),
    ("model", "model"),
    ("dataset", "dataset"),
    ("space", "space"),
]


def _ours(repo_type: str | None, repo_id: str = REPO_ID) -> RepoUrl:
    """RepoUrl exactly as `create_repo` builds it: via the OCI repo path."""
    return _build_repo_url(_oci_repo_path(repo_id, repo_type), EP)


def _hf_reference(repo_type: str | None, repo_id: str = REPO_ID) -> RepoUrl:
    """RepoUrl the way huggingface_hub's own HfApi.create_repo constructs it:
    `{endpoint}/{repo_id}` for models, `{endpoint}/{prefix}{repo_id}` otherwise."""
    prefix = "" if repo_type in (None, "model") else REPO_TYPES_URL_PREFIXES[repo_type]
    return RepoUrl(f"{EP}/{prefix}{repo_id}", endpoint=EP)


@pytest.mark.parametrize("repo_type, expected_type", TYPE_CASES)
def test_build_repo_url_constructs_and_parses(repo_type, expected_type):
    """The fix: no `/v2/`, parses without raising, correct attributes."""
    url = _ours(repo_type)
    assert "/v2/" not in str(url), "regression: OCI /v2/ path leaked into the RepoUrl"
    assert str(url).endswith(REPO_ID)
    assert url.repo_id == REPO_ID
    assert url.repo_type == expected_type
    assert url.namespace == "test"
    assert url.repo_name == "e2e-client"
    assert url.endpoint == EP


@pytest.mark.parametrize("repo_type, _expected_type", TYPE_CASES)
def test_build_repo_url_matches_hf_reference(repo_type, _expected_type):
    """Drop-in parity: our RepoUrl resolves the same fields as the one
    huggingface_hub's HfApi.create_repo would return for the same repo."""
    ours = _ours(repo_type)
    ref = _hf_reference(repo_type)
    got = (ours.repo_id, ours.repo_type, ours.namespace, ours.repo_name)
    want = (ref.repo_id, ref.repo_type, ref.namespace, ref.repo_name)
    assert got == want


@pytest.mark.parametrize("repo_id", [REPO_ID, "veggies-test/fake-model", "justname"])
def test_build_repo_url_behavioral_parity_including_edges(repo_id):
    """Whatever huggingface_hub does with the canonical URL — succeed with given
    attributes, or raise — our builder must do the same. This covers the
    namespace/name happy path AND edge cases like a single-segment id (HF 1.21
    requires `namespace/name` and raises HfUriError), so we never diverge from
    the installed client regardless of its version."""
    def outcome(fn):
        try:
            u = fn()
            return ("ok", u.repo_id, u.repo_type)
        except Exception as e:  # noqa: BLE001 - parity compares exception *type*
            return ("err", type(e).__name__)

    assert outcome(lambda: _ours(None, repo_id)) == outcome(lambda: _hf_reference(None, repo_id))
