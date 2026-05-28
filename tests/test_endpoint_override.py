"""Verify the `endpoint=` kwarg is actually plumbed.

Every Python API function accepts `endpoint=` for pointing at an alternate
Hippius registry (the README documents it as the official extension point).
Until now nothing actually proved the kwarg takes effect — it could be
silently ignored and every test would still pass against the default
registry. These tests assert that:

  1. URL-shape functions (`hf_hub_url`) embed the override verbatim.
  2. Network functions (`hf_hub_download`, `repo_exists`) actually attempt
     the override (a bogus DNS surface produces a network error, proving
     the request never went to the default).
"""
import pytest

from hippius_hub import hf_hub_download, hf_hub_url, repo_exists


# `hf_hub_url` is a pure string operation — no network — so it doesn't need
# the e2e marker. Kept here alongside the network tests because the contract
# under test ("endpoint= takes effect") is the same.


def test_hf_hub_url_uses_endpoint_override():
    """The override must replace the default base entirely; trailing slash
    must be stripped (matches `resolve_registry`)."""
    url = hf_hub_url("foo/bar", "x.bin", endpoint="https://alt.example.com/")
    assert url == "https://alt.example.com/v2/foo/bar/manifests/main"


def test_hf_hub_url_endpoint_combines_with_repo_type():
    url = hf_hub_url(
        "foo/bar", "x.bin",
        endpoint="https://alt.example.com",
        repo_type="dataset",
    )
    assert url == "https://alt.example.com/v2/datasets/foo/bar/manifests/main"


@pytest.mark.e2e
def test_hf_hub_download_endpoint_override_routes_to_override(tmp_path, logged_in, test_repo):
    """Passing `endpoint=` to an unresolvable host must produce a network
    error, NOT a successful download from the default registry. The error
    type matters less than that the call failed before reaching the cache —
    if endpoint= were ignored, this would silently succeed against prod.
    """
    cache = tmp_path / "cache"
    cache.mkdir()
    with pytest.raises(Exception) as excinfo:
        hf_hub_download(
            repo_id=test_repo,
            filename="anything.bin",
            cache_dir=str(cache),
            endpoint="https://nonexistent.hippius.invalid",
        )
    # Sanity-check the failure was network-shaped, not the wrong-args path.
    msg = str(excinfo.value).lower()
    assert any(s in msg for s in ("nonexistent.hippius.invalid", "name or service",
                                  "nodename", "connection", "resolve", "dns",
                                  "could not", "failed")), (
        f"expected a network/DNS failure mentioning the override; got: {excinfo.value!r}"
    )


@pytest.mark.e2e
def test_repo_exists_endpoint_override_routes_to_override(logged_in, test_repo):
    """Same proof for `repo_exists`: an unresolvable endpoint must surface
    a network failure rather than falling back to the default registry."""
    with pytest.raises(Exception):
        repo_exists(test_repo, endpoint="https://nonexistent.hippius.invalid")


@pytest.mark.e2e
def test_hf_hub_download_explicit_default_endpoint_still_works(
    tmp_path, cache_dir, logged_in, test_repo, revision,
):
    """Passing `endpoint=` with the default URL explicitly must be a no-op.
    Catches a regression where override logic could accidentally rewrite the
    default URL."""
    from hippius_hub import hippius_hub_upload
    src = tmp_path / "ep.bin"
    write = src.write_bytes
    write(b"endpoint-default-test\n" * 8)
    hippius_hub_upload(repo_id=test_repo, local_path=str(src), revision=revision)

    out = hf_hub_download(
        repo_id=test_repo,
        filename="ep.bin",
        revision=revision,
        cache_dir=cache_dir,
        endpoint="https://registry.hippius.com",
    )
    assert out
