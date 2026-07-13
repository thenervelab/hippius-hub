"""Regression: token=False must skip docker-config fallback.

HF semantics: 'If False, no token will be used.'
Current code: falls through to get_docker_auth(), violating that contract.
"""
from unittest.mock import patch
from hippius_hub.auth import get_oci_bearer_token


@patch("hippius_hub.auth.get_docker_auth")
@patch("hippius_hub.auth._http.client")
def test_explicit_no_auth_skips_docker_fallback(mock_client, mock_docker):
    mock_docker.return_value = "stolen-base64-auth"
    # The token GET now goes through the shared pooled client; intercept its .get.
    mock_get = mock_client.return_value.get
    # status_code is inspected by the L3 retry wrapper (200 = success, not retried).
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"token": "anon-token"}
    mock_get.return_value.raise_for_status.return_value = None

    # Caller explicitly says: no auth. Sentinel value at the boundary.
    get_oci_bearer_token("foo/bar", token=False, use_cache=False)

    # The docker fallback must not be consulted.
    mock_docker.assert_not_called()
    # The request must go out with NO Authorization header.
    call_headers = mock_get.call_args.kwargs["headers"]
    assert "Authorization" not in call_headers
