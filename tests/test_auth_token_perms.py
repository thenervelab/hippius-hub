"""Regression: login() must chmod 0600 the saved token file."""
import os
import stat
from hippius_hub.auth import login, TOKEN_PATH


def test_login_chmods_token_file(tmp_path, monkeypatch):
    monkeypatch.setattr("hippius_hub.auth.DEFAULT_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "token"))
    login(token="abc")
    mode = os.stat(tmp_path / "token").st_mode
    perms = stat.S_IMODE(mode)
    assert perms == 0o600, f"expected 0600, got {oct(perms)}"
