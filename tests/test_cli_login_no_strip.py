"""Regression: .strip() must not be applied to secrets — it silently
mutates passwords that happen to end in whitespace, producing
misleading 401s downstream."""
import inspect
from hippius_hub import cli


def test_cli_does_not_strip_secrets():
    src = inspect.getsource(cli.main)
    # We allow .strip() on the username and the visible prompts,
    # but the lines that handle getpass output must not.
    for line in src.splitlines():
        if "getpass.getpass" in line:
            assert ".strip()" not in line, (
                f"strip() on a getpass result silently mutates secrets: {line}"
            )
