import pytest

from hippius_hub import hippius_hub_upload, hf_hub_download
from hippius_hub.errors import EntryNotFoundError, RevisionNotFoundError

from tests._helpers import write_test_file


@pytest.mark.e2e
def test_missing_filename_raises_EntryNotFoundError(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "present.bin"
    write_test_file(src, 64, seed=b"present")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    with pytest.raises(EntryNotFoundError, match="absent.bin"):
        hf_hub_download(
            repo_id=test_repo,
            filename="absent.bin",
            revision=revision,
            cache_dir=cache_dir,
        )


@pytest.mark.e2e
def test_bad_revision_raises_RevisionNotFoundError(cache_dir, logged_in, test_repo):
    with pytest.raises(RevisionNotFoundError, match="does-not-exist"):
        hf_hub_download(
            repo_id=test_repo,
            filename="anything.bin",
            revision="does-not-exist",
            cache_dir=cache_dir,
        )
