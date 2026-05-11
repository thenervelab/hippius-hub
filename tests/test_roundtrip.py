import os

import pytest

from hippius_hub import hippius_hub_upload, hf_hub_download

from tests._helpers import sha256_of_file, write_test_file


pytestmark = pytest.mark.e2e


def test_single_small_file(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "small.bin"
    expected = write_test_file(src, 1024, seed=b"small")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo,
        filename="small.bin",
        revision=revision,
        cache_dir=cache_dir,
    )
    assert sha256_of_file(out) == expected


def test_empty_file(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "empty.bin"
    src.write_bytes(b"")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo,
        filename="empty.bin",
        revision=revision,
        cache_dir=cache_dir,
    )
    assert os.path.getsize(out) == 0


def test_directory_upload(tmp_path, cache_dir, logged_in, test_repo, revision):
    src_dir = tmp_path / "tree"
    src_dir.mkdir()
    sizes = {"a.bin": 100, "b.bin": 4096, "sub/c.bin": 8192, "sub/d.bin": 16384, "sub/deep/e.bin": 200}
    expected = {}
    for name, size in sizes.items():
        p = src_dir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        expected[name] = write_test_file(p, size, seed=name.encode())

    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)

    for name, want in expected.items():
        out = hf_hub_download(
            repo_id=test_repo,
            filename=name,
            revision=revision,
            cache_dir=cache_dir,
        )
        assert sha256_of_file(out) == want, f"hash mismatch for {name}"


def test_unicode_filename(tmp_path, cache_dir, logged_in, test_repo, revision):
    src_dir = tmp_path / "uni"
    (src_dir / "café").mkdir(parents=True)
    target = src_dir / "café" / "résumé.bin"
    expected = write_test_file(target, 2048, seed=b"unicode")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src_dir), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo,
        filename="café/résumé.bin",
        revision=revision,
        cache_dir=cache_dir,
    )
    assert sha256_of_file(out) == expected


@pytest.mark.parametrize(
    "file_size,chunk_size",
    [
        (1024 * 1024, 512 * 1024),
        (1024 * 1024, 512 * 1024 + 1),
        (1024 * 1024, 1024 * 1024),
        (1024 * 1024 + 1, 1024 * 1024),
    ],
    ids=["clean-2-chunks", "off-by-one-chunk", "exactly-one-chunk", "one-byte-tail"],
)
def test_chunk_size_boundary(tmp_path, cache_dir, logged_in, test_repo, revision, file_size, chunk_size):
    src = tmp_path / "chunked.bin"
    expected = write_test_file(src, file_size, seed=f"chunk-{file_size}".encode())

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo,
        filename="chunked.bin",
        revision=revision,
        cache_dir=cache_dir,
        chunk_size=chunk_size,
    )
    assert sha256_of_file(out) == expected
    assert os.path.getsize(out) == file_size


def test_verify_hash_true(tmp_path, cache_dir, logged_in, test_repo, revision):
    src = tmp_path / "verify.bin"
    expected = write_test_file(src, 64 * 1024, seed=b"verify")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo,
        filename="verify.bin",
        revision=revision,
        cache_dir=cache_dir,
        verify_hash=True,
    )
    assert sha256_of_file(out) == expected


@pytest.mark.slow
def test_single_large_file(tmp_path, cache_dir, logged_in, test_repo, revision):
    size = 250 * 1024 * 1024
    src = tmp_path / "big.bin"
    expected = write_test_file(src, size, seed=b"big")

    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo,
        filename="big.bin",
        revision=revision,
        cache_dir=cache_dir,
    )
    assert sha256_of_file(out) == expected
    assert os.path.getsize(out) == size
