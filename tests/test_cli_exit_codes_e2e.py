"""End-to-end CLI exit-code test (audit H3, audit IMP-5 extension).

The existing test_cli_error_routing.py covers `_format_download_error`
in-process — fine for the mapping, but it never exercises the integration
seam between argparse dispatch and `sys.exit`. A regression that loses
the call-site `sys.exit(code)` after `_format_download_error` returns
would silently degrade every exit code to 0 (or 1, depending on where
the bug lands) and `test_cli_error_routing.py` wouldn't catch it.

This file spawns `python -c "..."` subprocesses that import the CLI,
monkeypatch the relevant exception source, then run `cli.main()`. The
subprocess return code is what bash wrappers see in production — the
test pins exactly that.

Coverage matrix:
    EntryNotFoundError          → 10
    RepositoryNotFoundError     → 11
    RevisionNotFoundError       → 12
    LocalEntryNotFoundError     → 13
    GatedRepoError              → 14
    ConcurrentManifestUpdateError → 15
    HfHubHTTPError              → 16
    EXIT_NAMESPACE_TAKEN (`registry check`)  → 17
    EXIT_INVALID_REPO_FORMAT (`registry artifacts foo`) → 18
"""
from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest


def _run_cli_with_injected_error(
    error_class_import: str,
    error_construct: str,
    argv: list[str],
) -> subprocess.CompletedProcess:
    """Spawn a subprocess that monkeypatches hippius_hub_download to raise.

    `error_class_import` is a Python import statement (executed verbatim
    in the subprocess). `error_construct` is the expression used to build
    the exception instance. `argv` is what's passed after `python -c
    "<script>"` to `sys.argv` so argparse sees it as the CLI invocation.
    """
    script = textwrap.dedent(f"""
        import sys
        {error_class_import}
        from hippius_hub import cli

        def fake_download(**kwargs):
            raise {error_construct}

        # Replace the production function with a raiser, then dispatch
        # through the real cli.main() so argparse, _format_download_error,
        # and sys.exit are all exercised end-to-end.
        cli.hippius_hub_download = fake_download
        sys.argv = ["hippius-hub"] + {argv!r}
        cli.main()
    """)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )


# HF error signatures differ by class:
#   - EntryNotFoundError / LocalEntryNotFoundError inherit from plain
#     Exception (NOT from HfHubHTTPError) and accept only positional args
#   - RepositoryNotFoundError / RevisionNotFoundError / GatedRepoError /
#     HfHubHTTPError require `response=httpx.Response(...)` kwarg
#   - hippius_hub's ConcurrentManifestUpdateError synthesizes a response
#     when not given one (see errors.py:52-54), so a bare str works.
_HTTPX_PREAMBLE = "import httpx; _r = httpx.Response(404, request=httpx.Request('GET', 'about:blank'))"


@pytest.mark.parametrize("exc_import,exc_expr,expected_code,error_label", [
    (
        "from hippius_hub.errors import EntryNotFoundError",
        "EntryNotFoundError('foo.bin not found')",
        10,
        "File not found in repo",
    ),
    (
        f"{_HTTPX_PREAMBLE}; from hippius_hub.errors import RepositoryNotFoundError",
        "RepositoryNotFoundError('no such repo', response=_r)",
        11,
        "Repository not found",
    ),
    (
        f"{_HTTPX_PREAMBLE}; from hippius_hub.errors import RevisionNotFoundError",
        "RevisionNotFoundError('no such revision', response=_r)",
        12,
        "Revision not found",
    ),
    (
        "from hippius_hub.errors import LocalEntryNotFoundError",
        "LocalEntryNotFoundError('cache miss')",
        13,
        "Local cache miss",
    ),
    (
        f"{_HTTPX_PREAMBLE}; from hippius_hub.errors import GatedRepoError",
        "GatedRepoError('access denied', response=_r)",
        14,
        "Access denied",
    ),
    (
        "from hippius_hub.errors import ConcurrentManifestUpdateError",
        "ConcurrentManifestUpdateError('foo at main')",
        15,
        "Concurrent write detected",
    ),
    (
        f"{_HTTPX_PREAMBLE}; from hippius_hub.errors import HfHubHTTPError",
        "HfHubHTTPError('500 server error', response=_r)",
        16,
        "Registry HTTP error",
    ),
])
def test_download_typed_error_routes_to_exit_code(
    exc_import, exc_expr, expected_code, error_label,
):
    """Each typed error reaches `_format_download_error` and exits with
    its mapped code. Verifies the integration seam that unit tests mock.
    """
    proc = _run_cli_with_injected_error(
        exc_import,
        exc_expr,
        ["download", "owner/repo", "model.bin"],
    )
    assert proc.returncode == expected_code, (
        f"expected exit {expected_code} for {exc_expr!r}, got "
        f"{proc.returncode}.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert error_label in proc.stdout, (
        f"expected '{error_label}' in stdout, got:\n{proc.stdout}"
    )


def test_registry_check_taken_exits_17():
    """`registry check <name>` for a taken namespace must exit 17.

    Uses a console stub injected via the same monkeypatch-and-dispatch
    pattern: we replace `console.check_namespace` with a stub that says
    "taken", then run the CLI through to completion.
    """
    script = textwrap.dedent("""
        import sys
        from hippius_hub import cli

        cli.console.check_namespace = lambda name: {
            "available": False, "message": f"'{name}' already taken"
        }
        sys.argv = ["hippius-hub", "registry", "check", "myorg"]
        cli.main()
    """)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 17, (
        f"expected exit 17, got {proc.returncode}.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "already taken" in proc.stdout


def test_registry_artifacts_malformed_repo_exits_18():
    """`registry artifacts foo` (no slash) must exit 18 — was 2 before.

    This is the bash-codespace-collision regression: a shell wrapper
    inspecting $? could not distinguish a malformed --repo from an
    argparse usage error if both used 2.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "hippius_hub.cli", "registry", "artifacts", "no-slash"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 18, (
        f"expected exit 18, got {proc.returncode}.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "<project>/<repo>" in proc.stdout


def test_models_show_malformed_repo_exits_18():
    """`models show foo` (no slash) must exit 18 — was 1 before."""
    proc = subprocess.run(
        [sys.executable, "-m", "hippius_hub.cli", "models", "show", "no-slash"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 18, (
        f"expected exit 18, got {proc.returncode}.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


def test_argparse_usage_error_still_exits_2():
    """Sanity: argparse's own usage error path must remain at 2. The
    typed codes deliberately avoid this slot so a shell wrapper can
    distinguish 'bad flag' (2) from 'typed semantic failure' (10+)."""
    proc = subprocess.run(
        [sys.executable, "-m", "hippius_hub.cli", "download"],  # missing positional args
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 2, (
        f"argparse usage error must exit 2, got {proc.returncode}"
    )


def test_unknown_exception_falls_through_to_1():
    """An exception type that `_format_download_error` doesn't recognize
    must still produce a sane exit — the generic-failure fallback (1)."""
    proc = _run_cli_with_injected_error(
        "",  # built-in exception, no import needed
        "ValueError('unmapped failure')",
        ["download", "owner/repo", "model.bin"],
    )
    assert proc.returncode == 1, (
        f"unmapped exception should exit 1, got {proc.returncode}.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "Operation failed" in proc.stdout
