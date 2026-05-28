"""Live coverage of the `HippiusApi` OO wrapper.

`test_phase_b.test_hippius_api_*` tests verify the class structure (subclass
of HfApi, stubs raise, methods are bound to HippiusApi) but never invoke an
instance method against the live service. A bug in the dispatch (e.g.
forgetting `self.token` or a wrong signature) would slip past those checks.
This file exercises each method against the real registry through an
instance, mirroring how downstream code uses the class.
"""
import io

import pytest

from huggingface_hub import CommitInfo, ModelInfo

from hippius_hub import HippiusApi

from tests._helpers import sha256_of_file, write_test_file


pytestmark = pytest.mark.e2e


def test_hippius_api_upload_then_download_via_instance(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """upload_file → hf_hub_download via a HippiusApi() instance. Same round
    trip as test_phase_b but routed through the OO entry point.

    No explicit endpoint kwarg — `HippiusApi()` defaults to DEFAULT_REGISTRY_URL
    (fixed in the same PR as this test). A regression that re-introduces the
    HF endpoint default would cause this test to 404 at huggingface.co.
    """
    api = HippiusApi()
    src = tmp_path / "api.bin"
    expected = write_test_file(src, 256, seed=b"hippius-api")

    ci = api.upload_file(
        path_or_fileobj=str(src),
        path_in_repo="api.bin",
        repo_id=test_repo,
        revision=revision,
    )
    assert isinstance(ci, CommitInfo)

    out = api.hf_hub_download(
        repo_id=test_repo, filename="api.bin",
        revision=revision, cache_dir=cache_dir,
    )
    assert sha256_of_file(out) == expected


def test_hippius_api_inspection_methods(tmp_path, logged_in, test_repo, revision):
    """list_repo_files / file_exists / model_info via the instance.
    Catches any binding bug where these resolve to the HF base implementation
    (which would fail by hitting huggingface.co)."""
    api = HippiusApi()
    src = tmp_path / "insp.bin"
    write_test_file(src, 64, seed=b"insp")
    api.upload_file(
        path_or_fileobj=str(src), path_in_repo="insp.bin",
        repo_id=test_repo, revision=revision,
    )

    files = api.list_repo_files(test_repo, revision=revision)
    assert "insp.bin" in files

    assert api.file_exists(test_repo, "insp.bin", revision=revision) is True
    assert api.file_exists(test_repo, "no-such.bin", revision=revision) is False

    info = api.model_info(test_repo, revision=revision)
    assert isinstance(info, ModelInfo)
    assert info.id == test_repo


def test_hippius_api_whoami_via_instance(logged_in):
    """The HF base class has its own whoami; we must override it so the
    call routes to harbor, not huggingface.co."""
    api = HippiusApi()
    result = api.whoami()
    assert result["name"].startswith("robot$")
    assert result["type"] == "robot"


def test_hippius_api_upload_file_accepts_binary_io(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """The bytes/BinaryIO path is tested at the module level — re-test through
    the instance to prove `_normalize_path_or_fileobj` is reached the same way."""
    api = HippiusApi()
    payload = b"hippius-api-binaryio\n" * 32
    api.upload_file(
        path_or_fileobj=io.BytesIO(payload),
        path_in_repo="api-bio.bin",
        repo_id=test_repo,
        revision=revision,
    )
    out = api.hf_hub_download(
        test_repo, "api-bio.bin", revision=revision, cache_dir=cache_dir,
    )
    assert open(out, "rb").read() == payload
