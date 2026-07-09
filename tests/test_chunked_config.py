"""Env-gate config for the chunked-artifact upload path.

Pins the chunk threshold / CDC-average resolvers and the rollout write-gate:
`HIPPIUS_CHUNKED_WRITE` is the switch that lets an operator keep emitting the
pre-chunking single-blob layout until every consumer has the chunk-aware reader.
"""
import pytest

from hippius_hub.constants import (
    DEFAULT_CDC_AVG_SIZE,
    DEFAULT_CHUNK_THRESHOLD,
    resolve_cdc_avg_size,
    resolve_chunk_threshold,
    resolve_chunked_write_enabled,
)


def test_threshold_default(monkeypatch):
    monkeypatch.delenv("HIPPIUS_CHUNK_THRESHOLD", raising=False)
    assert resolve_chunk_threshold() == DEFAULT_CHUNK_THRESHOLD == 256 * 1024 * 1024


def test_cdc_avg_default_is_64mib(monkeypatch):
    monkeypatch.delenv("HIPPIUS_CDC_AVG_SIZE", raising=False)
    assert resolve_cdc_avg_size() == DEFAULT_CDC_AVG_SIZE == 64 * 1024 * 1024


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
    monkeypatch.delenv("HIPPIUS_CHUNKED_WRITE", raising=False)
    assert resolve_chunked_write_enabled() is True


@pytest.mark.parametrize("value", ["0", "false", "False", "no", "off", "OFF"])
def test_chunked_write_disabled_by_falsy_values(monkeypatch, value):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    assert resolve_chunked_write_enabled() is False


@pytest.mark.parametrize("value", ["1", "true", "yes", "on", "anything"])
def test_chunked_write_enabled_by_truthy_values(monkeypatch, value):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    assert resolve_chunked_write_enabled() is True


def test_empty_write_gate_defaults_enabled(monkeypatch):
    # An empty `HIPPIUS_CHUNKED_WRITE=` in a profile must not silently disable.
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "  ")
    assert resolve_chunked_write_enabled() is True
