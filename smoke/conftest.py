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
"""
import hashlib
import os
import sys
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Scrub the repo root before the first `import hippius_hub` anywhere in the
# suite. Ordering matters: conftest is imported before any test module.
sys.path[:] = [p for p in sys.path if p and Path(p).resolve() != REPO_ROOT]

import _revisions  # noqa: E402
import hippius_hub  # noqa: E402
from hippius_hub import auth, console  # noqa: E402


def _assert_published_wheel():
    installed = Path(hippius_hub.__file__).resolve()
    if installed.is_relative_to(REPO_ROOT):
        raise RuntimeError(
            f"hippius_hub resolved to the repo checkout ({installed}), not an "
            f"installed wheel. The smoke suite must exercise the published "
            f"client — run it from outside the repo tree, or `pip install "
            f"hippius_hub` into the active environment."
        )
    return installed


# Revisions older than this are swept by test_01. Hourly runs each push a fresh
# ~100 MiB blob (content must be unique per run — see `model_dir`), so without
# a sweep the test namespace grows ~2.4 GB/day. Six hours keeps the last few
# runs around for post-mortem inspection.
REVISION_RETENTION_HOURS = int(os.environ.get("HIPPIUS_SMOKE_RETENTION_HOURS", "6"))


@pytest.fixture(scope="session")
def client_path():
    """Absolute path of the hippius_hub package actually under test."""
    return _assert_published_wheel()


@pytest.fixture(scope="session")
def smoke_repo():
    """Namespace the smoke run pushes to.

    Defaults to the same `test/e2e-client` repo the e2e suite uses, because the
    CI robot credentials are scoped to it (README §CI secrets). Smoke revisions
    are prefixed `smoke-` so the sweep never touches an e2e revision.
    """
    return os.environ.get(
        "HIPPIUS_SMOKE_REPO", os.environ.get("HIPPIUS_TEST_REPO", "test/e2e-client")
    )


@pytest.fixture(scope="session")
def qualified_repo(smoke_repo):
    """`<project>/<repo>` as the console API wants it. Identical to `smoke_repo`
    for a two-segment repo id; kept as its own fixture so a deeper repo path
    doesn't silently produce a malformed console URL."""
    project, _, tail = smoke_repo.partition("/")
    if not tail:
        pytest.fail(f"HIPPIUS_SMOKE_REPO must be '<project>/<repo>', got {smoke_repo!r}")
    return f"{project}/{tail}"


@pytest.fixture(scope="session")
def session_revision():
    return _revisions.build(uuid.uuid4().hex[:6])


@pytest.fixture(scope="session")
def logged_in(client_path, tmp_path_factory):
    """Authenticate against the production registry.

    Writes the credential to an ephemeral token file rather than the real
    `~/.cache/hippius/hub/token`, so running this suite on a workstation
    doesn't clobber a developer's existing login (same approach as
    `scripts/bench.py`).

    Missing credentials are a hard failure, not a skip: a smoke suite that
    silently skips is indistinguishable from a green one, which is exactly the
    failure mode this job exists to prevent.
    """
    user = os.environ.get("HIPPIUS_TEST_USER")
    password = os.environ.get("HIPPIUS_TEST_PASS")
    token = os.environ.get("HIPPIUS_TEST_TOKEN")
    if not token and not (user and password):
        raise RuntimeError(
            "Set HIPPIUS_TEST_USER + HIPPIUS_TEST_PASS, or HIPPIUS_TEST_TOKEN. "
            "The production smoke suite must never run unauthenticated."
        )

    token_file = tmp_path_factory.mktemp("hippius-smoke") / "token"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(auth, "TOKEN_PATH", str(token_file))
        auth.login(username=user, password=password, token=token)
        yield str(token_file)


@pytest.fixture(scope="session")
def console_logged_in(tmp_path_factory):
    """Persist the console API token to an ephemeral path. Only test_01
    (revision sweep) needs it — the console API is how a single tag gets
    deleted without nuking the whole repository."""
    api_token = os.environ.get("HIPPIUS_TEST_CONSOLE_TOKEN")
    if not api_token:
        pytest.skip("HIPPIUS_TEST_CONSOLE_TOKEN not set — cannot sweep old revisions")

    token_file = tmp_path_factory.mktemp("hippius-smoke-console") / "api_token"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(console, "API_TOKEN_PATH", str(token_file))
        console.save_api_token(api_token)
        yield str(token_file)


@pytest.fixture(scope="session")
def retention_cutoff():
    return datetime.now(timezone.utc) - timedelta(hours=REVISION_RETENTION_HOURS)


@pytest.fixture(scope="session")
def model_size_bytes():
    return int(os.environ.get("HIPPIUS_SMOKE_MODEL_MB", "100")) * 1024 * 1024


@pytest.fixture(scope="session")
def model_dir(tmp_path_factory, model_size_bytes, session_revision):
    """A model-shaped folder: a tiny `config.json` plus a ~100 MiB weights file.

    The weights are freshly random every run, never seeded deterministically.
    Two reasons: (1) OCI blobs are content-addressed, so identical bytes would
    dedup server-side and the "upload" would become a no-op HEAD — the suite
    would go green without ever pushing a byte; (2) a fixed seed produces
    colliding digests across runs, which can wedge permanently against a
    half-GC'd blob of the same digest.

    Returns (path, {relative_name: sha256}).
    """
    root = tmp_path_factory.mktemp("model")
    (root / "config.json").write_text(
        '{"model_type": "hippius-smoke", "hidden_size": 4096}\n'
    )

    weights = root / "model.safetensors"
    digest = hashlib.sha256()
    block = 8 * 1024 * 1024
    remaining = model_size_bytes
    with open(weights, "wb") as f:
        while remaining > 0:
            chunk = os.urandom(min(block, remaining))
            f.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)

    expected = {
        "config.json": hashlib.sha256((root / "config.json").read_bytes()).hexdigest(),
        "model.safetensors": digest.hexdigest(),
    }
    print(
        f"Generated smoke model for {session_revision}: "
        f"{model_size_bytes / (1024 * 1024):.0f} MiB weights, "
        f"sha256={expected['model.safetensors'][:12]}..."
    )
    return root, expected


@pytest.fixture(scope="session")
def download_cache(tmp_path_factory):
    """A cold cache per run. Reusing the runner's default cache would let a
    previously-downloaded blob satisfy the download without a network fetch,
    turning the read half of the smoke test into a local file check."""
    return str(tmp_path_factory.mktemp("cache"))
