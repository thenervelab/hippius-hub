"""The empty-config-blob HEAD is cached per (registry, repo) (change #3).

The autouse `_clear_process_caches` fixture (conftest) empties the cache before each
test, so each starts from a cold cache.
"""
import httpx
import respx

from hippius_hub.file_upload import _ensure_config_blob_uploaded, clear_config_blob_cache
from tests.respx_fixtures import EMPTY_CONFIG_DIGEST, MOCK_REGISTRY


@respx.mock
def test_config_head_skipped_after_first_confirm():
    # 1st call HEADs (200) and caches (registry, repo); the 2nd to the same pair
    # skips the HEAD entirely -- the whole point of the cache.
    head = respx.head(
        f"{MOCK_REGISTRY}/v2/proj/repo/blobs/{EMPTY_CONFIG_DIGEST}"
    ).mock(return_value=httpx.Response(200))
    _ensure_config_blob_uploaded(MOCK_REGISTRY, "proj/repo", "tok")
    _ensure_config_blob_uploaded(MOCK_REGISTRY, "proj/repo", "tok")
    assert head.call_count == 1


@respx.mock
def test_config_head_not_shared_across_registries():
    # The cache key includes the registry, so a different endpoint re-HEADs -- a blob
    # in registry A implies nothing about registry B (the comment on the cache warns
    # exactly this).
    other = "https://other.test.invalid"
    h_default = respx.head(
        f"{MOCK_REGISTRY}/v2/proj/repo/blobs/{EMPTY_CONFIG_DIGEST}"
    ).mock(return_value=httpx.Response(200))
    h_other = respx.head(
        f"{other}/v2/proj/repo/blobs/{EMPTY_CONFIG_DIGEST}"
    ).mock(return_value=httpx.Response(200))
    _ensure_config_blob_uploaded(MOCK_REGISTRY, "proj/repo", "tok")
    _ensure_config_blob_uploaded(other, "proj/repo", "tok")
    assert h_default.call_count == 1
    assert h_other.call_count == 1


@respx.mock
def test_clear_config_blob_cache_rearms_the_head():
    head = respx.head(
        f"{MOCK_REGISTRY}/v2/proj/repo/blobs/{EMPTY_CONFIG_DIGEST}"
    ).mock(return_value=httpx.Response(200))
    _ensure_config_blob_uploaded(MOCK_REGISTRY, "proj/repo", "tok")
    clear_config_blob_cache()
    _ensure_config_blob_uploaded(MOCK_REGISTRY, "proj/repo", "tok")
    assert head.call_count == 2
