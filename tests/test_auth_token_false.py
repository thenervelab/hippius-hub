"""Regression: token=False must skip docker-config fallback.

HF semantics: 'If False, no token will be used.'
Current code: falls through to get_docker_auth(), violating that contract.
"""
from unittest.mock import patch
import pytest
from hippius_hub.auth import get_oci_bearer_token


@patch("hippius_hub.auth.get_docker_auth")
@patch("hippius_hub.auth.httpx.get")
def test_explicit_no_auth_skips_docker_fallback(mock_http, mock_docker):
    mock_docker.return_value = "stolen-base64-auth"
    mock_http.return_value.json.return_value = {"token": "anon-token"}
    mock_http.return_value.raise_for_status.return_value = None

    # Caller explicitly says: no auth. Sentinel value at the boundary.
    get_oci_bearer_token("foo/bar", token=False, use_cache=False)

    # The docker fallback must not be consulted.
    mock_docker.assert_not_called()
    # The request must go out with NO Authorization header.
    call_headers = mock_http.call_args.kwargs["headers"]
    assert "Authorization" not in call_headers
