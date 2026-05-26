"""Regression: _jwt_expiration must warn on parse failure (M4)."""
import pytest
from hippius_hub.auth import _jwt_expiration


def test_warns_on_two_segment_jwt():
    with pytest.warns(UserWarning, match="3 segments"):
        assert _jwt_expiration("only.two") is None


def test_warns_on_unparseable_payload():
    with pytest.warns(UserWarning, match="parse failed"):
        assert _jwt_expiration("h.@@notbase64@@.s") is None


def test_no_warning_on_valid_jwt():
    """Sanity: a valid JWT must NOT warn."""
    import base64, json, warnings
    payload = base64.urlsafe_b64encode(json.dumps({"exp": 9999999999}).encode()).decode().rstrip("=")
    jwt = f"h.{payload}.s"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert _jwt_expiration(jwt) == 9999999999
        assert not any("JWT" in str(w.message) for w in caught)
