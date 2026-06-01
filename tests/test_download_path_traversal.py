"""DL-2: filenames from the OCI manifest are server-controlled (the
``org.opencontainers.image.title`` layer annotation). They must not be able to
escape the cache directory or the user's ``local_dir`` via ``..`` segments or
an absolute path — the path-traversal vector huggingface_hub guards in
``_get_pointer_path``. These are pure unit tests on the path-resolution choke
point ``_resolve_dest_paths``; no network.
"""
import os

import pytest

import hippius_hub.file_download as fd


MALICIOUS = [
    "../escape.bin",
    "../../etc/cron.d/evil",
    "/etc/passwd",
    "sub/../../escape.bin",
    "a/../../../b.bin",
    "..\\windows-escape.bin",
]


@pytest.mark.parametrize("bad", MALICIOUS)
def test_cache_path_rejects_traversal(bad, tmp_path):
    with pytest.raises(ValueError, match="(?i)unsafe|escape|outside"):
        fd._resolve_dest_paths(
            repo_id="org/model",
            filename=bad,
            repo_type=None,
            revision="main",
            cache_dir=str(tmp_path),
            local_dir=None,
        )


@pytest.mark.parametrize("bad", MALICIOUS)
def test_local_dir_path_rejects_traversal(bad, tmp_path):
    with pytest.raises(ValueError, match="(?i)unsafe|escape|outside"):
        fd._resolve_dest_paths(
            repo_id="org/model",
            filename=bad,
            repo_type=None,
            revision="main",
            cache_dir=str(tmp_path / "cache"),
            local_dir=str(tmp_path / "ld"),
        )


def test_legit_nested_filename_allowed_cache(tmp_path):
    """A normal nested path (subfolder/file) must still resolve under the
    snapshot dir — the guard rejects escapes, not legitimate subdirectories."""
    paths = fd._resolve_dest_paths(
        repo_id="org/model",
        filename="weights/model.safetensors",
        repo_type=None,
        revision="main",
        cache_dir=str(tmp_path),
        local_dir=None,
    )
    dest_abs = os.path.abspath(paths.dest_file)
    base_abs = os.path.abspath(str(tmp_path))
    assert os.path.commonpath([dest_abs, base_abs]) == base_abs
    assert paths.dest_file.endswith(os.path.join("weights", "model.safetensors"))


def test_legit_flat_filename_allowed_local_dir(tmp_path):
    ld = tmp_path / "ld"
    paths = fd._resolve_dest_paths(
        repo_id="org/model",
        filename="model.bin",
        repo_type=None,
        revision="main",
        cache_dir=str(tmp_path / "cache"),
        local_dir=str(ld),
    )
    assert os.path.abspath(paths.dest_file).startswith(os.path.abspath(str(ld)) + os.sep)
