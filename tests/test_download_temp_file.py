"""Regression tests for unique temp-file naming in _download_to_cache (audit N3)."""
import inspect

from hippius_hub import file_download


def test_download_to_cache_uses_mkstemp():
    """The temp path must be unique per call (mkstemp), not a fixed name."""
    src = inspect.getsource(file_download._download_to_cache)
    assert "tempfile.mkstemp" in src, (
        "_download_to_cache must use tempfile.mkstemp for unique temp paths; "
        "shared temp paths race when two processes download the same file."
    )
    # Defensive: ensure we didn't leave the old buggy pattern.
    assert 'f"tmp_{filename' not in src, (
        "fixed-pattern temp path still present in _download_to_cache"
    )


def test_download_to_cache_cleans_up_on_failure():
    """If download_file_native raises, the temp file must not leak."""
    src = inspect.getsource(file_download._download_to_cache)
    # Look for the cleanup pattern: a try block around download_file_native + os.remove
    assert "os.remove(temp_path)" in src, "Cleanup of temp_path on failure missing"
    assert "except OSError" in src, "Cleanup must use narrow OSError, not bare except"
