import os
import sys
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


def _have_console_token():
    return bool(os.environ.get("HIPPIUS_TEST_CONSOLE_TOKEN"))


@pytest.fixture(scope="session")
def creds():
    if not _have_creds():
        pytest.skip("HIPPIUS_TEST_USER/PASS or HIPPIUS_TEST_TOKEN not set")
    return {"user": os.environ.get("HIPPIUS_TEST_USER"),
        "password": os.environ.get("HIPPIUS_TEST_PASS"),
        "token": os.environ.get("HIPPIUS_TEST_TOKEN"), }


@pytest.fixture(scope="session")
def console_token():
    """The console.hippius.com API token. Separate from `creds` (docker registry)
    because the console API authenticates via `Authorization: Token <…>` whereas
    OCI uses Basic/Bearer. CI sets this from secrets.HIPPIUS_TEST_CONSOLE_TOKEN."""
    if not _have_console_token():
        pytest.skip("HIPPIUS_TEST_CONSOLE_TOKEN not set")
    return os.environ["HIPPIUS_TEST_CONSOLE_TOKEN"]


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


@pytest.fixture
def console_logged_in(console_token, tmp_path, monkeypatch):
    """Persist the console API token to a tmp path so `console.*` calls and the
    CLI `registry`/`models` subtrees authenticate against api.hippius.com without
    touching the user's real ~/.cache/hippius/hub/api_token."""
    from hippius_hub import console as console_mod

    api_token_file = tmp_path / "api_token"
    monkeypatch.setattr(console_mod, "API_TOKEN_PATH", str(api_token_file))
    console_mod.save_api_token(console_token)
    return str(api_token_file)


@pytest.fixture
def console_test_project(console_logged_in):
    """Active project name from the live console. Tests that need a project
    name (e.g. to build a `<project>/<repo>` arg for `registry artifacts`)
    pull it from here instead of hardcoding."""
    from hippius_hub import console as console_mod

    me = console_mod.me()
    project = me.get("project_name")
    if not project:
        pytest.skip("Console account has no provisioned project; can't run project-scoped tests")
    return project


@pytest.fixture
def cli_env(tmp_path, console_token, creds):
    """Isolated environment for CLI subprocess tests. Points HOME at tmp_path
    so the CLI's ~/.cache/hippius/hub/* writes can't clobber the developer's
    saved tokens, and pre-seeds both the docker-registry token and the console
    API token via the CLI's own `login` subcommand — same flow a real user
    runs, no monkeypatching."""
    import subprocess

    env = {**os.environ, "HOME": str(tmp_path)}
    cache = tmp_path / ".cache" / "hippius" / "hub"
    cache.mkdir(parents=True)

    cli = [sys.executable, "-m", "hippius_hub.cli"]

    # 1. Docker-registry creds for `download`/`upload`.
    if creds.get("user") and creds.get("password"):
        login = cli + ["login", "--username", creds["user"], "--password", creds["password"]]
    else:
        login = cli + ["login", "--token", creds["token"]]
    subprocess.run(login, env=env, check=True, capture_output=True)

    # 2. Console API token for `registry *` / `models *`.
    subprocess.run(
        cli + ["login", "--hippius-token", console_token],
        env=env, check=True, capture_output=True,
    )

    return env
