"""Audit L11: `models list`/`show` must not crash on a null `format` field.

The model index treats `format` as nullable, but the CLI rendered it with a
width spec (`{...:12}`); `format(None, '12')` raises TypeError, and since
`main()` catches only `ConsoleError`, one unclassifiable row aborted the whole
listing with an uncaught traceback (no rows printed). These tests pin that a
null `format` renders as the `—` placeholder instead.
"""
from __future__ import annotations

from types import SimpleNamespace

from hippius_hub import cli


def test_models_list_survives_null_format(monkeypatch, capsys):
    monkeypatch.setattr(
        cli.console,
        "models_list",
        lambda **_: {
            "total": 1,
            "results": [
                {
                    "project": "p",
                    "repo": "r",
                    "format": None,  # the L11 trigger
                    "architecture": None,
                    "parameter_count": 1_000_000,
                    "quantization": None,
                    "total_size_bytes": 1024,
                    "is_mine": False,
                    "is_public": True,
                }
            ],
        },
    )
    args = SimpleNamespace(
        format=None, arch=None, quant=None, min_params=None, max_params=None,
        q=None, mine=False, page=1, page_size=100, json=False,
    )
    cli.cmd_models_list(args)  # must not raise
    out = capsys.readouterr().out
    assert "p/r" in out
    assert "—" in out  # null format -> placeholder, not a TypeError


def test_models_show_files_survive_null_format(monkeypatch, capsys):
    monkeypatch.setattr(
        cli.console,
        "model_detail",
        lambda *a, **k: {
            "project": "p",
            "repo": "r",
            "primary_tag": "main",
            "format": None,
            "architecture": None,
            "parameter_count": None,
            "quantization": None,
            "total_size_bytes": None,
            "digest": "sha256:x",
            "files": [{"filename": "model.safetensors", "format": None, "size_bytes": 1024}],
            "pull_command": "hippius-hub models pull p/r",
        },
    )
    args = SimpleNamespace(repo_id="p/r", reference="main", json=False)
    cli.cmd_models_show(args)  # must not raise on the null file `format`
    out = capsys.readouterr().out
    assert "model.safetensors" in out
    assert "—" in out
