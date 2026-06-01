"""Behavioral unit tests for hippius_hub.auth.resolve_token_value.

Covers the three-state HF semantics (None/True → saved, False → False
sentinel, str → str). Phase 5.3 routed dispatch through TokenInput;
these tests pin the resolved values returned to callers."""
import pytest

from hippius_hub import auth


def test_none_reads_saved_token(tmp_path, monkeypatch):
    token_path = tmp_path / "token"
    token_path.write_text("Bearer saved-value")
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(token_path))
    assert auth.resolve_token_value(None) == "Bearer saved-value"


def test_true_reads_saved_token(tmp_path, monkeypatch):
    token_path = tmp_path / "token"
    token_path.write_text("Bearer saved-true")
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(token_path))
    assert auth.resolve_token_value(True) == "Bearer saved-true"


def test_false_returns_false_sentinel(tmp_path, monkeypatch):
    """token=False is the explicit-anonymous sentinel (Task 1.2)."""
    token_path = tmp_path / "token"
    token_path.write_text("Bearer would-not-use")
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(token_path))
    assert auth.resolve_token_value(False) is False


def test_str_returns_literal_value():
    assert auth.resolve_token_value("literal-token") == "literal-token"


def test_no_saved_token_returns_none(tmp_path, monkeypatch):
    """If TOKEN_PATH doesn't exist, returns None (callers fall back to docker config)."""
    monkeypatch.setattr("hippius_hub.auth.TOKEN_PATH", str(tmp_path / "nonexistent"))
    assert auth.resolve_token_value(None) is None
    assert auth.resolve_token_value(True) is None


def test_empty_string_returns_empty_string():
    """Empty string is a string — return as-is (not coerced to None)."""
    assert auth.resolve_token_value("") == ""


def test_unsupported_type_raises():
    """from_hf rejects unsupported types; resolve_token_value surfaces that."""
    with pytest.raises(TypeError, match="Unsupported token type"):
        auth.resolve_token_value(123)
