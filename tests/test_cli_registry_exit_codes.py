"""Typed exit codes for non-exception CLI failure modes.

`_format_download_error` covers typed exceptions (10-16). The registry +
models subcommands have two non-exception paths the dispatcher can't see:

  - `cmd_registry_check` exits when a namespace is TAKEN (17)
  - `cmd_registry_artifacts` and `cmd_models_show` exit when the
    `<project>/<repo>` argument is malformed (18)

Both used to share `sys.exit(2)` with argparse's usage-error code, making
them indistinguishable from a bad-flag failure to a shell wrapper. These
tests pin the new codes by calling the handlers directly with stubbed
console responses.
"""
from __future__ import annotations

import argparse

import pytest

from hippius_hub import cli


def test_namespace_taken_exits_17(monkeypatch, capsys):
    """`registry check` on a taken name must exit 17, not 2."""
    monkeypatch.setattr(
        cli.console,
        "check_namespace",
        lambda name: {"available": False, "message": f"'{name}' is already taken"},
    )
    args = argparse.Namespace(name="myorg")
    with pytest.raises(SystemExit) as exc:
        cli.cmd_registry_check(args)
    assert exc.value.code == 17
    assert exc.value.code == cli.EXIT_NAMESPACE_TAKEN
    out = capsys.readouterr().out
    assert "already taken" in out


def test_namespace_available_returns_normally(monkeypatch, capsys):
    """Sanity: the success path must NOT exit."""
    monkeypatch.setattr(
        cli.console,
        "check_namespace",
        lambda name: {"available": True},
    )
    args = argparse.Namespace(name="myorg")
    cli.cmd_registry_check(args)  # must not raise
    assert "is available" in capsys.readouterr().out


def test_invalid_repo_format_in_artifacts_exits_18(capsys):
    """`registry artifacts foo` (no slash) must exit 18, not 2."""
    args = argparse.Namespace(repo="no-slash", page=1, page_size=50)
    with pytest.raises(SystemExit) as exc:
        cli.cmd_registry_artifacts(args)
    assert exc.value.code == 18
    assert exc.value.code == cli.EXIT_INVALID_REPO_FORMAT
    out = capsys.readouterr().out
    assert "must be '<project>/<repo>'" in out


def test_invalid_repo_format_in_models_show_exits_18(capsys):
    """`models show foo` (no slash) must exit 18 — was 1 before this fix.

    Was inconsistent with cmd_registry_artifacts (which exited 2) for the
    same logical error class. Both now resolve to 18 so wrappers can
    branch on "malformed CLI arg" deterministically.
    """
    args = argparse.Namespace(repo_id="no-slash", reference=None, json=False)
    with pytest.raises(SystemExit) as exc:
        cli.cmd_models_show(args)
    assert exc.value.code == 18
    out = capsys.readouterr().out
    assert "<project>/<repo>" in out


def test_exit_code_constants_are_outside_bash_reserved_range():
    """Bash reserves 0-2; argparse uses 2. Typed codes must be >= 10."""
    assert cli.EXIT_NAMESPACE_TAKEN >= 10
    assert cli.EXIT_INVALID_REPO_FORMAT >= 10
    # And distinct from the _format_download_error codes (10-16).
    assert cli.EXIT_NAMESPACE_TAKEN not in range(1, 17)
    assert cli.EXIT_INVALID_REPO_FORMAT not in range(1, 17)
