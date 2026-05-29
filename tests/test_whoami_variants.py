"""End-to-end coverage of `whoami`'s three-state `token` argument.

The existing `test_whoami_returns_hf_shape` only exercises the saved-token
path (`whoami()` with no args). The other branches are part of the public
contract (HF parity) and need their own live coverage:

  - token=False         → LocalTokenNotFoundError (never hits network)
  - token=True          → same as None (uses saved token)
  - token="<raw>"       → wraps as Bearer header internally
  - token="Bearer …"    → passes verbatim
"""
import pytest

from hippius_hub import logout, whoami
from hippius_hub.auth import get_token
from hippius_hub.errors import LocalTokenNotFoundError


pytestmark = pytest.mark.e2e


def test_whoami_token_false_raises_without_network():
    """token=False must short-circuit on the auth check before touching the
    registry. Unit-style: no `logged_in` fixture — we want to prove the
    error fires even when no token is saved."""
    with pytest.raises(LocalTokenNotFoundError):
        whoami(token=False)


def test_whoami_token_true_uses_saved_token(logged_in):
    """token=True is HF's "use whatever is saved" sentinel; must behave
    identically to the default (None)."""
    result = whoami(token=True)
    assert result["name"].startswith("robot$")


def test_whoami_with_raw_token_string(logged_in):
    """A raw token string (no `Bearer ` prefix) must be wrapped as `Bearer`
    internally and reach harbor successfully.

    Only runs when the saved token is already Bearer-shaped: re-wrapping a
    base64 Basic-auth payload as `Bearer <b64>` would produce a malformed
    header. The CI robot's creds are Basic (`<robot$user>:<secret>` b64),
    so this test typically skips in CI and only fires locally against an
    HF-style bearer token. The Basic-input path is exercised by
    test_whoami_with_prewrapped_header below.
    """
    saved = get_token()
    assert saved, "logged_in fixture should have written a token"
    if saved.startswith("Basic "):
        pytest.skip("Saved creds are Basic auth; bare-token rewrap only applies to Bearer")
    bare = saved.split(" ", 1)[1] if saved.startswith("Bearer ") else saved
    result = whoami(token=bare)
    assert result["name"].startswith("robot$")


def test_whoami_with_prewrapped_header(logged_in):
    """A token already in `Bearer <…>` / `Basic <…>` form must be passed
    through without re-wrapping (otherwise we'd get `Bearer Basic …`)."""
    saved = get_token()
    assert saved, "logged_in fixture should have written a token"
    assert saved.startswith(("Bearer ", "Basic "))
    result = whoami(token=saved)
    assert result["name"].startswith("robot$")


def test_whoami_missing_token_raises(tmp_path, monkeypatch):
    """No saved token AND token=None → LocalTokenNotFoundError. Distinct from
    `token=False`: this is the "I forgot to log in" path."""
    from hippius_hub import auth

    monkeypatch.setattr(auth, "TOKEN_PATH", str(tmp_path / "no-such-token"))
    with pytest.raises(LocalTokenNotFoundError):
        whoami()
