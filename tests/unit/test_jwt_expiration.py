"""Behavioral unit tests for hippius_hub.auth._jwt_expiration.

Phase 4.2 added warnings for parse failures (audit M4); these tests
exhaustively cover the edge cases of JWT parsing without any HTTP traffic."""
import base64
import json
import warnings

import pytest

from hippius_hub.auth import _jwt_expiration


def _make_jwt(payload: dict) -> str:
    """Build a fake JWT — header.payload.signature, b64url-encoded.

    The header and signature are placeholder strings since _jwt_expiration
    only looks at the payload (middle segment).
    """
    def b64(x):
        return base64.urlsafe_b64encode(json.dumps(x).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64(payload)}.signature"


def test_exp_extracted_from_valid_jwt():
    jwt = _make_jwt({"exp": 1700000000, "sub": "u"})
    assert _jwt_expiration(jwt) == 1700000000


def test_no_exp_field_returns_none():
    jwt = _make_jwt({"sub": "u"})
    assert _jwt_expiration(jwt) is None


def test_two_part_string_returns_none():
    with pytest.warns(UserWarning, match="3 segments"):
        assert _jwt_expiration("only.two") is None


def test_one_part_string_returns_none():
    with pytest.warns(UserWarning, match="3 segments"):
        assert _jwt_expiration("only") is None


def test_garbage_payload_returns_none():
    with pytest.warns(UserWarning, match="parse failed"):
        assert _jwt_expiration("h.@@not-base64@@.s") is None


def test_payload_not_json_returns_none():
    bad = base64.urlsafe_b64encode(b"not-json").decode().rstrip("=")
    with pytest.warns(UserWarning, match="parse failed"):
        assert _jwt_expiration(f"h.{bad}.s") is None


def test_payload_missing_padding_handled():
    """JWT b64 segments may lack padding — _jwt_expiration must pad before decode."""
    # Construct a payload whose b64-encoded form has no trailing =
    payload = {"exp": 1234567890}
    raw = json.dumps(payload).encode()
    b64 = base64.urlsafe_b64encode(raw).decode().rstrip("=")  # strip padding
    jwt = f"h.{b64}.s"
    assert _jwt_expiration(jwt) == 1234567890


def test_valid_jwt_does_not_warn():
    """Sanity: a valid JWT with exp must NOT warn."""
    jwt = _make_jwt({"exp": 9999999999})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert _jwt_expiration(jwt) == 9999999999
        jwt_warnings = [w for w in caught if "JWT" in str(w.message)]
        assert jwt_warnings == []
