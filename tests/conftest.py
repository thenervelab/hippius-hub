import os
import uuid

import pytest


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
