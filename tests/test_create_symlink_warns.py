"""Regression: _create_symlink fallbacks must warn (audit N5)."""
import os
import sys
import warnings
from unittest.mock import patch

import pytest

from hippius_hub.file_download import _create_symlink


@pytest.mark.skipif(sys.platform == "win32", reason="symlink semantics differ on Windows")
def test_symlink_success_does_not_warn(tmp_path):
    src = tmp_path / "blob"
    src.write_text("hello")
    dst = tmp_path / "link"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _create_symlink(str(src), str(dst))
        assert all("fallback" not in str(w.message) for w in caught)
        assert all("falling back" not in str(w.message) for w in caught)
    assert os.path.exists(dst)


def test_symlink_failure_warns_then_falls_back_to_hardlink(tmp_path):
    src = tmp_path / "blob"
    src.write_text("hello")
    dst = tmp_path / "link"
    # Force os.symlink to fail to exercise the warning + fallback. The
    # hardlink path should succeed on the same-tmpfs test sandbox.
    with patch("os.symlink", side_effect=OSError("simulated")):
        with pytest.warns(UserWarning, match="symlink.*failed.*hardlink"):
            _create_symlink(str(src), str(dst))
    # Hardlink (or copy if hardlink also fails) should have succeeded.
    assert os.path.exists(dst)
