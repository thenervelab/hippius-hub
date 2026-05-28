"""Subprocess-driven coverage of every read-only `registry`/`models` CLI command.

These complement `test_console_readonly.py` — that file exercises the Python
client; these prove the CLI wires the client's output into the user-facing
stdout text the README documents. A regression in `_fmt_bytes`, the
print-loop, or argument plumbing fails here, not in user sessions.
"""
import json

import pytest

from tests._helpers import run_cli


pytestmark = pytest.mark.e2e


# ---------- registry: pure reads, no project-scoped state required ----------

def test_cli_registry_plans(cli_env):
    """`registry plans` lists at least one plan with a name and credits line."""
    r = run_cli(["registry", "plans"], env=cli_env)
    assert r.returncode == 0
    assert "credits/mo" in r.stdout


def test_cli_registry_check_random_namespace_available(cli_env):
    """check exits 0 on availability (random uuid) and prints the green tick."""
    import uuid
    r = run_cli(
        ["registry", "check", f"avail-{uuid.uuid4().hex[:12]}"],
        env=cli_env,
    )
    assert r.returncode == 0
    assert "available" in r.stdout.lower()


def test_cli_registry_me(cli_env):
    """`registry me` prints Project/Plan/Status/Registry fields the README documents."""
    r = run_cli(["registry", "me"], env=cli_env)
    assert r.returncode == 0
    for field in ("Project:", "Status:", "Registry:"):
        assert field in r.stdout, f"missing {field!r} in stdout: {r.stdout!r}"


def test_cli_registry_status(cli_env):
    """`registry status` either prints projects or the empty-state line."""
    r = run_cli(["registry", "status"], env=cli_env)
    assert r.returncode == 0
    # Either we get a row (`status=...`) or the "no projects" hint.
    assert "status=" in r.stdout or "No projects" in r.stdout


def test_cli_registry_repos(cli_env):
    """`registry repos` either lists rows or prints the empty-state line."""
    r = run_cli(["registry", "repos", "--page-size", "5"], env=cli_env)
    assert r.returncode == 0
    assert ("artifacts=" in r.stdout) or ("No repositories" in r.stdout)


def test_cli_registry_usage(cli_env):
    """`registry usage` always prints the storage-used/quota header even on
    a fresh account."""
    r = run_cli(["registry", "usage"], env=cli_env)
    assert r.returncode == 0
    assert "Storage used:" in r.stdout
    assert "Storage quota:" in r.stdout


def test_cli_registry_subscriptions(cli_env):
    """`registry subscriptions` lists subs or the empty-state hint."""
    r = run_cli(["registry", "subscriptions"], env=cli_env)
    assert r.returncode == 0
    assert ("paid/mo=" in r.stdout) or ("No subscriptions" in r.stdout)


def test_cli_registry_keys_list(cli_env):
    """`registry keys list` lists keys or the empty-state hint."""
    r = run_cli(["registry", "keys", "list"], env=cli_env)
    assert r.returncode == 0
    assert ("role=" in r.stdout) or ("No keys yet" in r.stdout)


# ---------- registry: requires a project ----------

def test_cli_registry_artifacts(cli_env, console_test_project, test_repo):
    """`registry artifacts <project>/<repo>` returns rows or the empty hint."""
    repo_tail = test_repo.split("/", 1)[1] if "/" in test_repo else test_repo
    r = run_cli(
        ["registry", "artifacts", f"{console_test_project}/{repo_tail}",
         "--page-size", "5"],
        env=cli_env, check=False,
    )
    # 0 = success (rows or empty); 18 = EXIT_INVALID_REPO_FORMAT. We passed
    # a valid form, so 18 here would be a real failure.
    assert r.returncode == 0, f"stderr: {r.stderr!r}"


def test_cli_registry_artifacts_rejects_single_segment(cli_env):
    """Passing a one-segment repo must exit with EXIT_INVALID_REPO_FORMAT (18)
    and the format hint, not blow up server-side. The typed exit code was
    bumped from argparse's generic 2 to a dedicated value so shell wrappers
    can distinguish "bad arg" from "argparse usage error"."""
    from hippius_hub.cli import EXIT_INVALID_REPO_FORMAT

    r = run_cli(["registry", "artifacts", "no-slash"], env=cli_env, check=False)
    assert r.returncode == EXIT_INVALID_REPO_FORMAT
    assert "<project>/<repo>" in r.stdout


# ---------- models ----------

def test_cli_models_formats(cli_env):
    """`models formats` always prints the three categories."""
    r = run_cli(["models", "formats"], env=cli_env)
    assert r.returncode == 0
    for line in ("formats:", "architectures:", "quantizations:"):
        assert line in r.stdout


def test_cli_models_list(cli_env):
    """`models list` prints the `Found N model(s):` header."""
    r = run_cli(["models", "list", "--page-size", "5"], env=cli_env)
    assert r.returncode == 0
    assert "Found" in r.stdout and "model" in r.stdout


def test_cli_models_list_json_is_parseable(cli_env):
    """--json must produce a parseable object with `results` and `total`."""
    r = run_cli(["models", "list", "--page-size", "3", "--json"], env=cli_env)
    assert r.returncode == 0
    parsed = json.loads(r.stdout)
    assert "results" in parsed
    assert "total" in parsed


def test_cli_models_show_no_versions_known_repo(
    cli_env, console_test_project, test_repo,
):
    """`models show <project>/<repo>` prints the version list (or skips if the
    indexer hasn't seen it yet — same skip path as the Python-level test)."""
    repo_tail = test_repo.split("/", 1)[1] if "/" in test_repo else test_repo
    r = run_cli(
        ["models", "show", f"{console_test_project}/{repo_tail}", "--json"],
        env=cli_env, check=False,
    )
    if r.returncode != 0:
        pytest.skip(f"`models show` not available for {console_test_project}/{repo_tail}")
    parsed = json.loads(r.stdout)
    assert "artifacts" in parsed
