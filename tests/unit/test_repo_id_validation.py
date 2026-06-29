"""Unit tests for repo_id name validation (`_validate_repo_id` / `_oci_repo_path`).

Container registries reject repository names that aren't lowercase per the OCI
distribution spec, but do so with a misleading `401 Unauthorized` deep in the
upload — after auth has already succeeded. `_validate_repo_id` catches the bad
name up front and raises a clear ValueError instead. These cases come straight
from a real incident: pushing to `ranupthestairs/teutonic-5Gb7e4B4-v1` (note the
uppercase `G`/`B`) 401'd, while the lowercase `teutonic-5gb7e4b4-v1` worked.
"""
import pytest

from hippius_hub.file_download import _oci_repo_path, _validate_repo_id


# Names the OCI grammar accepts: lowercase alphanumerics joined by '.', '_',
# '__' or runs of '-', with '/'-separated path components.
_VALID = [
    "test",
    "test2",
    "ranupthestairs/test2",
    "ranupthestairs/teutonic-5gb7e4b4-v1",
    "happysmile/teutonic-5eavrgab-anyway",
    "newjana/albedo-qwen3-4b-r2c40s0609b-tao64",
    "qwen/qwen2.5-7b",
    "a__b",          # double underscore is a valid separator
    "foo--bar",      # runs of '-' are a valid separator
    "a/b/c",         # multiple path components
    "org.name/repo",
]

# Names the grammar rejects — the key class is anything containing uppercase.
_INVALID = [
    "ranupthestairs/teutonic-5Gb7e4B4-v1",   # the real incident name
    "ranupthestairs/teutonic-5Gb7e4B4",
    "Qwen/Qwen2.5-7B",                        # common HF model id casing
    "Foo",
    "trailing-",                              # separator may not be trailing
    "-leading",                               # ...or leading
    "a//b",                                   # empty path component
    "a_/b",                                   # separator with no following run
    "repo name",                              # whitespace is not allowed
    "",                                       # empty
]


@pytest.mark.parametrize("repo_id", _VALID)
def test_valid_repo_ids_pass(repo_id):
    _validate_repo_id(repo_id)  # must not raise


@pytest.mark.parametrize("repo_id", _INVALID)
def test_invalid_repo_ids_raise(repo_id):
    with pytest.raises(ValueError):
        _validate_repo_id(repo_id)


def test_error_message_suggests_lowercase_form():
    """The actionable bit: the message must name the offending id and point at
    the lowercase fix, so the user isn't left staring at a bare 401."""
    bad = "ranupthestairs/teutonic-5Gb7e4B4-v1"
    with pytest.raises(ValueError) as exc:
        _validate_repo_id(bad)
    msg = str(exc.value)
    assert bad in msg
    assert bad.lower() in msg
    assert "lowercase" in msg


def test_oci_repo_path_validates_before_mapping():
    """`_oci_repo_path` is the single chokepoint both upload and download funnel
    through; it must reject an invalid id rather than emit a bad `/v2/...` path."""
    with pytest.raises(ValueError):
        _oci_repo_path("ranupthestairs/teutonic-5Gb7e4B4-v1", "model")


@pytest.mark.parametrize("repo_type", [None, "model", "dataset", "space"])
def test_oci_repo_path_rejects_uppercase_for_every_type(repo_type):
    """Validation happens for models, datasets and spaces alike — the check sits
    ahead of the per-type prefixing, so no repo_type can smuggle an upper name in."""
    with pytest.raises(ValueError):
        _oci_repo_path("Bad/Name", repo_type)


def test_oci_repo_path_still_maps_valid_ids():
    """Sanity: valid ids continue to map as before for each repo_type."""
    assert _oci_repo_path("ranupthestairs/test2", "model") == "ranupthestairs/test2"
    assert _oci_repo_path("e2e/client", "dataset") == "datasets/e2e/client"
    assert _oci_repo_path("e2e/client", "space") == "spaces/e2e/client"
