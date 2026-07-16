"""CLI surface contract test (audit follow-up: gap #4 from coverage analysis).

argparse's `--help` output is the public contract between the CLI and
its callers. A renamed flag (`--username` → `--user`), a dropped
subcommand, or a flipped default would silently break user
invocations, with no test today exercising the wired-up paths.

Strategy: drive `python -m hippius_hub.cli --help` and each subcommand
`--help` via subprocess, then assert that documented flags/subcommands
appear in the output. NOT a literal-snapshot test — argparse's
formatting differs across Python minor versions (`options:` vs
`optional arguments:`) and we don't want to gate merges on cosmetic
help text. The contract is "the flag exists and is mentioned in
--help", not "the help text matches a checked-in string verbatim".

Each subcommand is tested with `--help` so the regression surface
covers the full dispatcher, not just `parser.parse_args()` happy
paths the live e2e tests happen to hit.
"""
from __future__ import annotations

import subprocess
import sys

import pytest


HIPPIUS_CLI = [sys.executable, "-m", "hippius_hub.cli"]


def _help(*args: str) -> str:
    """Spawn `hippius-hub <args> --help` and return stdout. argparse exits
    with code 0 on `--help`."""
    result = subprocess.run(
        HIPPIUS_CLI + list(args) + ["--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"`hippius-hub {' '.join(args)} --help` exited "
        f"{result.returncode}.\nstdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    return result.stdout


# ---- Top-level ----


def test_top_level_help_lists_all_subcommands():
    """Every documented subcommand must show up in `hippius-hub --help`.

    A regression that drops a subparser would fail here; argparse's
    own listing IS the surface this test pins.
    """
    out = _help()
    for sub in ("download", "upload", "login", "registry", "models"):
        assert sub in out, f"subcommand {sub!r} missing from `--help`"


def test_top_level_help_mentions_version_flag():
    out = _help()
    assert "--version" in out or "-V" in out


# ---- download ----


def test_download_help_pins_documented_flags():
    out = _help("download")
    for flag in ("repo_id", "filename", "--revision", "--chunk-size",
                 "--cache-dir", "--verify-hash"):
        assert flag in out, f"download flag/arg {flag!r} missing from --help"


# ---- upload ----


def test_upload_help_pins_documented_flags():
    out = _help("upload")
    for flag in ("repo_id", "local_path", "--revision"):
        assert flag in out, f"upload flag/arg {flag!r} missing from --help"


# ---- login ----


def test_login_help_pins_three_credential_flags():
    """Audit H2 / IMP-7 sibling: the three auth-input flags MUST stay
    distinct. Collapsing `--token` and `--hippius-token` would silently
    re-route docker-registry creds to the console API endpoint.
    """
    out = _help("login")
    for flag in ("--username", "--password", "--token", "--hippius-token"):
        assert flag in out, f"login flag {flag!r} missing from --help"


# ---- registry ----


@pytest.mark.parametrize("subcmd", [
    "plans", "check", "provision", "status", "me", "rotate-token",
    "repos", "artifacts", "usage", "publicity",
    "subscribe", "subscriptions", "unsubscribe",
    "keys",
])
def test_registry_subcommands_are_reachable(subcmd):
    """Each documented `registry <subcommand>` must have its own --help.

    A regression that drops one of these subparsers from `_build_parser`
    surfaces here as a non-zero exit code (argparse rejects the unknown
    subcommand).
    """
    _help("registry", subcmd)


def test_registry_repos_delete_is_reachable_and_pins_flags():
    """`registry repos delete` mirrors `hf repos delete`; its flag set is the
    public contract. A dropped subparser fails the reachability call; a renamed
    flag fails the membership asserts.
    """
    out = _help("registry", "repos", "delete")
    for flag in ("<project>/<repo>", "--repo-type", "--token",
                 "--missing-ok", "--yes"):
        assert flag in out, f"`repos delete` flag {flag!r} missing from --help"


@pytest.mark.parametrize("keys_subcmd", [
    "list", "create", "show", "rotate", "revoke",
])
def test_registry_keys_subcommands_are_reachable(keys_subcmd):
    _help("registry", "keys", keys_subcmd)


def test_registry_keys_create_pins_role_choices():
    """The `--role` choice set is part of the public CLI contract —
    callers script around the exact role names. A rename ("read" →
    "pull") would break those scripts.
    """
    out = _help("registry", "keys", "create")
    for role in ("read", "push", "push-delete", "admin"):
        assert role in out, f"role choice {role!r} missing from `keys create --help`"
    assert "--expires-days" in out


def test_registry_subscribe_pins_pay_upfront_flag():
    out = _help("registry", "subscribe")
    assert "--pay-upfront" in out


# ---- models ----


@pytest.mark.parametrize("subcmd", ["list", "show", "formats"])
def test_models_subcommands_are_reachable(subcmd):
    _help("models", subcmd)


def test_models_list_pins_filter_flags():
    """The `--format` / `--arch` / `--quant` / `--min-params` / `--max-params`
    flag set is the public search contract for the model index.
    """
    out = _help("models", "list")
    for flag in ("--format", "--arch", "--quant",
                 "--min-params", "--max-params", "--mine", "--json"):
        assert flag in out, f"models list flag {flag!r} missing from --help"
