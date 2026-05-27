"""Behavioral unit tests for get_docker_auth — covers the post-Task-2.5
host-equality fix and the post-Task-3.5 corruption-warning behavior."""
import json
import os
import stat
import pytest

from hippius_hub.auth import get_docker_auth


def _write_config(tmp_path, content: str, monkeypatch):
    """Write `content` to a tmp path and monkeypatch os.path.expanduser to point at it."""
    cfg = tmp_path / "config.json"
    cfg.write_text(content)
    monkeypatch.setattr(
        "os.path.expanduser",
        lambda p: str(cfg) if p.endswith("config.json") else p,
    )
    return cfg


def test_missing_config_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "os.path.expanduser",
        lambda p: str(tmp_path / "nonexistent.json") if p.endswith("config.json") else p,
    )
    assert get_docker_auth("https://registry.hippius.com") is None


def test_exact_host_match(tmp_path, monkeypatch):
    _write_config(tmp_path, json.dumps({
        "auths": {"https://registry.hippius.com": {"auth": "MATCH"}}
    }), monkeypatch)
    assert get_docker_auth("https://registry.hippius.com") == "MATCH"


def test_trailing_slash_normalized(tmp_path, monkeypatch):
    _write_config(tmp_path, json.dumps({
        "auths": {"https://registry.hippius.com/": {"auth": "MATCH"}}
    }), monkeypatch)
    assert get_docker_auth("https://registry.hippius.com") == "MATCH"


def test_http_vs_https_normalized(tmp_path, monkeypatch):
    """Scheme is stripped on both sides — http://x and https://x are equivalent."""
    _write_config(tmp_path, json.dumps({
        "auths": {"http://registry.hippius.com": {"auth": "MATCH"}}
    }), monkeypatch)
    assert get_docker_auth("https://registry.hippius.com") == "MATCH"


def test_confused_deputy_resists_substring(tmp_path, monkeypatch):
    """Audit N1: a superstring entry must NOT shadow the real registry."""
    _write_config(tmp_path, json.dumps({
        "auths": {
            "https://registry.hippius.com.evil.example": {"auth": "STOLEN"},
            "https://registry.hippius.com": {"auth": "REAL"},
        }
    }), monkeypatch)
    assert get_docker_auth("https://registry.hippius.com") == "REAL"


def test_no_matching_host_returns_none(tmp_path, monkeypatch):
    _write_config(tmp_path, json.dumps({
        "auths": {"https://other.example.com": {"auth": "X"}}
    }), monkeypatch)
    assert get_docker_auth("https://registry.hippius.com") is None


def test_corrupted_json_warns_and_returns_none(tmp_path, monkeypatch):
    """Audit N2: a malformed config must emit UserWarning instead of silently failing."""
    _write_config(tmp_path, "{not valid json", monkeypatch)
    with pytest.warns(UserWarning, match="unreadable"):
        assert get_docker_auth("https://registry.hippius.com") is None


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
def test_unreadable_file_warns_and_returns_none(tmp_path, monkeypatch):
    """Audit N2: a permission-denied config must warn."""
    cfg = _write_config(tmp_path, "{}", monkeypatch)
    os.chmod(cfg, 0)
    try:
        with pytest.warns(UserWarning, match="unreadable"):
            assert get_docker_auth("https://registry.hippius.com") is None
    finally:
        os.chmod(cfg, 0o644)  # restore for pytest cleanup


def test_empty_auths_section_returns_none(tmp_path, monkeypatch):
    _write_config(tmp_path, json.dumps({"auths": {}}), monkeypatch)
    assert get_docker_auth("https://registry.hippius.com") is None


def test_no_auths_key_returns_none(tmp_path, monkeypatch):
    """A config without an `auths` key is valid but yields no matches."""
    _write_config(tmp_path, json.dumps({"credsStore": "osxkeychain"}), monkeypatch)
    assert get_docker_auth("https://registry.hippius.com") is None
