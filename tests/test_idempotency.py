import pytest

from hippius_hub import hippius_hub_upload, hf_hub_download

from tests._helpers import sha256_of_file, write_test_file


pytestmark = pytest.mark.e2e


def test_upload_skips_existing_blob(tmp_path, logged_in, test_repo, revision, capsys):
    src_dir = tmp_path / "skip"
    src_dir.mkdir()
    write_test_file(src_dir / "a.bin", 1024, seed=b"skip-a")
    write_test_file(src_dir / "b.bin", 2048, seed=b"skip-b")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)
    capsys.readouterr()

    second_rev = revision + "-resend"
    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=second_rev)
    out = capsys.readouterr()

    combined = out.out + out.err
    assert combined.count("Already published (skipped)") >= 2


def test_download_cache_hit_skips_blob_download(
    tmp_path, cache_dir, logged_in, test_repo, revision, monkeypatch
):
    src = tmp_path / "cached.bin"
    expected = write_test_file(src, 1024, seed=b"cached")
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    first = hf_hub_download(
        repo_id=test_repo,
        filename="cached.bin",
        revision=revision,
        cache_dir=cache_dir,
    )

    from hippius_hub import file_download as fd

    def boom(*args, **kwargs):
        raise AssertionError("Rust downloader called despite cache hit")

    monkeypatch.setattr(fd, "download_file_native", boom)

    second = hf_hub_download(
        repo_id=test_repo,
        filename="cached.bin",
        revision=revision,
        cache_dir=cache_dir,
    )
    assert second == first
    assert sha256_of_file(second) == expected
