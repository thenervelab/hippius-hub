"""Regression: auth.py:192 must pass timeout= to httpx.get."""
import inspect
from hippius_hub import auth


def test_get_oci_bearer_token_passes_timeout():
    src = inspect.getsource(auth.get_oci_bearer_token)
    assert "timeout=" in src, (
        "get_oci_bearer_token must pass timeout= to its httpx call; "
        "without it a stalled token endpoint hangs the whole client."
    )
