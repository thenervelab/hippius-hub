import os
import uuid

import pytest


# Load the Phase 2.4 respx fixture module as a pytest plugin so fixtures it
# defines (notably `monkeypatched_registry`) are discoverable by tests that
# request them by name. Helper functions (`token_route`, `manifest_get_route`,
# etc.) remain plain imports — the `pytest_plugins` mechanism is specifically
# about exposing `@pytest.fixture`-decorated callables to test collection.
# respx itself is auto-registered via its `pytest11` entry point (verified
# via `importlib.metadata.entry_points(group='pytest11')`), so it is NOT
# listed here.
pytest_plugins = ["tests.respx_fixtures"]


@pytest.fixture(autouse=True)
def _clear_oci_token_cache():
    """Prevent cross-test contamination of the global OCI bearer-token cache.

    `auth._OCI_TOKEN_CACHE` is module-level and survives across tests. Some
    tests monkeypatch `auth.TOKEN_PATH` per-test — the cache key includes the
    auth-input string so divergent saved tokens get separate entries, but
    clearing between tests is defense in depth against future regressions.
    """
    from hippius_hub.auth import clear_oci_token_cache

    clear_oci_token_cache()
    yield
    clear_oci_token_cache()


def _have_creds():
    return bool(os.environ.get("HIPPIUS_TEST_TOKEN")) or (
            bool(os.environ.get("HIPPIUS_TEST_USER")) and bool(os.environ.get("HIPPIUS_TEST_PASS")))


@pytest.fixture(scope="session")
def creds():
    if not _have_creds():
        pytest.skip("HIPPIUS_TEST_USER/PASS or HIPPIUS_TEST_TOKEN not set")
    return {"user": os.environ.get("HIPPIUS_TEST_USER"),
        "password": os.environ.get("HIPPIUS_TEST_PASS"),
        "token": os.environ.get("HIPPIUS_TEST_TOKEN"), }


@pytest.fixture(scope="session")
def test_repo():
    return os.environ.get("HIPPIUS_TEST_REPO", "test/e2e-client")


@pytest.fixture
def revision(request):
    safe_name = (
        request.node.name.replace("[", "-").replace("]", "").replace("/", "-").replace(" ", "_"))
    return f"ci-{uuid.uuid4().hex[:8]}-{safe_name}"[:128]


@pytest.fixture
def cache_dir(tmp_path):
    d = tmp_path / "cache"
    d.mkdir()
    return str(d)


@pytest.fixture
def logged_in(creds, tmp_path, monkeypatch):
    from hippius_hub import auth

    token_file = tmp_path / "token"
    monkeypatch.setattr(auth, "TOKEN_PATH", str(token_file))
    auth.login(username=creds["user"], password=creds["password"], token=creds["token"], )
    return str(token_file)
