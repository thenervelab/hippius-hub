"""Env-gate config for the chunked-artifact upload path.

Pins the chunk threshold / CDC-average resolvers and the rollout write-gate:
`HIPPIUS_CHUNKED_WRITE` is ON by default as of 0.6.0 (the chunk-aware reader
guard ships from 0.6.0). `=0`/`false`/... opts back out to the single-blob
layout; an unrecognized token raises rather than silently picking a default, so
a typo on a layout-changing gate can't pass unnoticed.
"""
import pytest

from hippius_hub.constants import (
    DEFAULT_CDC_AVG_SIZE,
    DEFAULT_CHUNK_THRESHOLD,
    resolve_cdc_avg_size,
    resolve_chunk_threshold,
    resolve_chunked_write_enabled,
    resolve_verify_hash,
)


def test_threshold_default(monkeypatch):
    monkeypatch.delenv("HIPPIUS_CHUNK_THRESHOLD", raising=False)
    assert resolve_chunk_threshold() == DEFAULT_CHUNK_THRESHOLD == 256 * 1024 * 1024


def test_cdc_avg_default_is_256kib(monkeypatch):
    # 256 KiB, chosen by the C5 measurement. Inside fastcdc's [256 B, 4 MiB] average
    # range (derived min=64 KiB, max=1 MiB), so no crate swap. See constants.py.
    monkeypatch.delenv("HIPPIUS_CDC_AVG_SIZE", raising=False)
    assert resolve_cdc_avg_size() == DEFAULT_CDC_AVG_SIZE == 256 * 1024


def test_threshold_and_avg_override(monkeypatch):
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", str(8 * 1024 * 1024))
    monkeypatch.setenv("HIPPIUS_CDC_AVG_SIZE", str(2 * 1024 * 1024))
    assert resolve_chunk_threshold() == 8 * 1024 * 1024
    assert resolve_cdc_avg_size() == 2 * 1024 * 1024


def test_threshold_rejects_non_positive(monkeypatch):
    monkeypatch.setenv("HIPPIUS_CHUNK_THRESHOLD", "0")
    with pytest.raises(ValueError):
        resolve_chunk_threshold()


def test_chunked_write_enabled_by_default(monkeypatch):
    # Default ON as of 0.6.0: unset emits the chunked layout. A reader must be
    # >= 0.6.0 to carry the guard that reads it.
    monkeypatch.delenv("HIPPIUS_CHUNKED_WRITE", raising=False)
    assert resolve_chunked_write_enabled() is True


@pytest.mark.parametrize("value", ["0", "false", "False", "no", "off", "OFF"])
def test_chunked_write_disabled_by_falsy_values(monkeypatch, value):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    assert resolve_chunked_write_enabled() is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes", "on", "ON"])
def test_chunked_write_enabled_by_truthy_values(monkeypatch, value):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    assert resolve_chunked_write_enabled() is True


@pytest.mark.parametrize("value", ["anything", "enabled", "2", "  yep  "])
def test_chunked_write_unrecognized_value_raises(monkeypatch, value):
    # Fail fast: a layout-changing gate must not silently pick a default on a
    # typo. An unrecognized token surfaces the misconfiguration immediately.
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    with pytest.raises(ValueError):
        resolve_chunked_write_enabled()


def test_empty_write_gate_defaults_enabled(monkeypatch):
    # An empty/whitespace `HIPPIUS_CHUNKED_WRITE=` in a profile falls back to the
    # default, which is ON as of 0.6.0.
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "  ")
    assert resolve_chunked_write_enabled() is True


def test_verify_hash_enabled_by_default(monkeypatch):
    # Default ON as of 0.6.0: the plain/Range download path verifies the
    # whole-file digest before caching. `=0` opts back out.
    monkeypatch.delenv("HIPPIUS_VERIFY_HASH", raising=False)
    assert resolve_verify_hash() is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off"])
def test_verify_hash_disabled_by_falsy_values(monkeypatch, value):
    monkeypatch.setenv("HIPPIUS_VERIFY_HASH", value)
    assert resolve_verify_hash() is False
