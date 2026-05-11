import pytest

from hippius_hub import hippius_hub_upload, hf_hub_download, snapshot_download

from tests._helpers import write_test_file


def test_snapshot_download_is_NotImplementedError():
    with pytest.raises(NotImplementedError):
        snapshot_download(repo_id="any/repo")


@pytest.mark.e2e
def test_missing_filename_raises_ValueError(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "present.bin"
    write_test_file(src, 64, seed=b"present")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    with pytest.raises(ValueError, match="absent.bin"):
        hf_hub_download(
            repo_id=test_repo,
            filename="absent.bin",
            revision=revision,
            cache_dir=cache_dir,
        )


@pytest.mark.e2e
def test_bad_revision_raises_ValueError(cache_dir, logged_in, test_repo):
    with pytest.raises(ValueError, match="does-not-exist"):
        hf_hub_download(
            repo_id=test_repo,
            filename="anything.bin",
            revision="does-not-exist",
            cache_dir=cache_dir,
        )


# Known divergence from huggingface_hub: HF raises typed exceptions
# (RepositoryNotFoundError, RevisionNotFoundError, EntryNotFoundError) from
# huggingface_hub.errors. We currently raise plain ValueError. Aligning these
# is a separate behavioral change tracked outside this test suite.
