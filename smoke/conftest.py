"""Fixtures for the hourly production smoke suite.

This suite is deliberately NOT under `tests/`. `tests/` is a Python package
(`tests/__init__.py`) with a `conftest.py` that pytest imports from the repo
root — which prepends the repo root to `sys.path` and makes `import hippius_hub`
resolve to the in-tree source instead of the wheel installed from PyPI. That
would defeat the entire purpose here: we want to prove the *published* client
works against production, not the working tree.

The `_assert_published_wheel()` guard below is the belt to that suspenders: if
anything ever re-introduces the shadowing, the suite fails loudly with an
explanation instead of silently testing the wrong code (or, more likely,
blowing up on a missing `hippius_core` .so with no hint as to why).

The guard only ASSERTS — it must never mutate `sys.path`. An earlier version
scrubbed the repo root from `sys.path` here, which broke `pytest` at the repo
root for the whole project: the editable maturin install (`python-source = "."`)
resolves `hippius_hub` *through* the repo root, so dropping that entry made all
76 `tests/` modules fail to import. `testpaths = ["tests"]` in pyproject.toml is
what actually keeps the two suites apart; this file just refuses to run against
the wrong client.
"""
import os
from pathlib import Path

import pytest

import hippius_hub

REPO_ROOT = Path(__file__).resolve().parent.parent
IN_TREE_PACKAGE = REPO_ROOT / "hippius_hub"


def _assert_published_wheel():
    """Refuse to run against the working tree instead of the published wheel.

    The test is deliberately narrow: does the import resolve to the *source
    package* at `<repo>/hippius_hub/`? An earlier version asked the broader
    question "is it anywhere under the repo root?", which is wrong — a venv
    created inside the checkout puts a perfectly good wheel at
    `<repo>/.venv/lib/pythonX.Y/site-packages/hippius_hub/`. That is what CI
    does (`python -m venv .venv` in the workspace) and what the README tells
    developers to do, so the guard rejected every legitimate run. It only
    passed pre-merge because the validation venv happened to sit outside the
    repo.
    """
    installed = Path(hippius_hub.__file__).resolve()
    if installed.is_relative_to(IN_TREE_PACKAGE):
        raise RuntimeError(
            f"hippius_hub resolved to the in-tree source at {installed}, not an "
            f"installed wheel — this environment has an editable/in-tree install, "
            f"so the smoke suite would be testing the working tree instead of what "
            f"users actually get from PyPI. Run it from a clean venv:\n"
            f"  python -m venv /tmp/smoke && /tmp/smoke/bin/pip install -r smoke/requirements.txt\n"
            f"  cd smoke && /tmp/smoke/bin/pytest -v"
        )
    return installed


@pytest.fixture(scope="session")
def client_path():
    """Absolute path of the hippius_hub package actually under test."""
    return _assert_published_wheel()


@pytest.fixture(scope="session")
def smoke_key():
    """The `hip_` key the private read uses, passed as `token=` the way a user would.

    A key and not a login: `hippius-hub login` writes `~/.docker/config.json`, and the
    suite must not change the runner's (or a developer's) stored credentials.

    Missing is a hard failure, not a skip: a smoke suite that silently skips is
    indistinguishable from a green one, which is the failure this job exists to catch.
    """
    key = os.environ.get("HIPPIUS_SMOKE_KEY")
    if not key:
        raise RuntimeError(
            "Set HIPPIUS_SMOKE_KEY to a hip_ key that may read the private repo in "
            "HIPPIUS_SMOKE_PRIVATE_REPO (veggies-test/e2e-client by default), and nothing "
            "more. README, 'Production smoke tests', says how to make one."
        )
    return key


@pytest.fixture(scope="session")
def private_repo():
    """A private repo of ours on the hub, which the key may read and anonymous may not."""
    return os.environ.get("HIPPIUS_SMOKE_PRIVATE_REPO", "veggies-test/e2e-client")


@pytest.fixture
def download_cache(tmp_path):
    """A cold cache per test. A warm one would let a cached blob satisfy the download
    without a network fetch, turning the read check into a local file check."""
    return str(tmp_path / "cache")
