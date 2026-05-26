"""Regression: get_docker_auth must not substring-match registry hosts.

Without this fix, ~/.docker/config.json containing both 'registry.hippius.com'
and 'registry.hippius.com.evil.example' would resolve the latter for any
query to the real host — classic confused-deputy when registry hostnames
share a suffix.
"""
import json
from hippius_hub.auth import get_docker_auth


def test_confused_deputy_resists_substring_match(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({
        "auths": {
            # Attacker entry — superstring of the real registry.
            "https://registry.hippius.com.evil.example": {"auth": "STOLEN"},
            "https://registry.hippius.com": {"auth": "REAL"},
        }
    }))
    monkeypatch.setattr("os.path.expanduser",
                        lambda p: str(cfg) if p.endswith("config.json") else p)
    result = get_docker_auth("https://registry.hippius.com")
    assert result == "REAL"


def test_exact_match_returns_auth(tmp_path, monkeypatch):
    """Sanity: the happy path still works."""
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({
        "auths": {"https://registry.hippius.com": {"auth": "REAL"}}
    }))
    monkeypatch.setattr("os.path.expanduser",
                        lambda p: str(cfg) if p.endswith("config.json") else p)
    assert get_docker_auth("https://registry.hippius.com") == "REAL"


def test_no_match_returns_none(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({
        "auths": {"https://other.example.com": {"auth": "X"}}
    }))
    monkeypatch.setattr("os.path.expanduser",
                        lambda p: str(cfg) if p.endswith("config.json") else p)
    assert get_docker_auth("https://registry.hippius.com") is None


def test_missing_config_file_returns_none(tmp_path, monkeypatch):
    """If ~/.docker/config.json doesn't exist, return None."""
    monkeypatch.setattr("os.path.expanduser",
                        lambda p: str(tmp_path / "nonexistent") if p.endswith("config.json") else p)
    assert get_docker_auth("https://registry.hippius.com") is None


def test_trailing_slash_in_input_is_normalized(tmp_path, monkeypatch):
    """A trailing slash on the input URL must not break the lookup."""
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({
        "auths": {"https://registry.hippius.com": {"auth": "REAL"}}
    }))
    monkeypatch.setattr("os.path.expanduser",
                        lambda p: str(cfg) if p.endswith("config.json") else p)
    assert get_docker_auth("https://registry.hippius.com/") == "REAL"
