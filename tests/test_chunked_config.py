"""Env-gate config for the chunked-artifact upload path.

Pins the chunk threshold / CDC-average resolvers and the rollout write-gate:
`HIPPIUS_CHUNKED_WRITE` is opt-in for this release — off unless explicitly set to
a truthy value — because the reader-side layout guard ships in this same release,
so no already-deployed consumer would refuse (rather than mis-write) a chunked
artifact. A producer opts in with `=1` once the chunk-aware reader is deployed.
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


def test_chunked_write_disabled_by_default(monkeypatch):
    # Opt-in this release: unset must NOT emit the chunked layout, because no
    # deployed reader (<= v0.5.1) carries the guard that refuses it.
    monkeypatch.delenv("HIPPIUS_CHUNKED_WRITE", raising=False)
    assert resolve_chunked_write_enabled() is False


@pytest.mark.parametrize("value", ["0", "false", "False", "no", "off", "OFF"])
def test_chunked_write_disabled_by_falsy_values(monkeypatch, value):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    assert resolve_chunked_write_enabled() is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes", "on", "ON"])
def test_chunked_write_enabled_by_truthy_values(monkeypatch, value):
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    assert resolve_chunked_write_enabled() is True


@pytest.mark.parametrize("value", ["anything", "enabled", "2", "  yep  "])
def test_chunked_write_unrecognized_value_stays_off(monkeypatch, value):
    # Fail safe: only an explicit truthy token opts in. An unrecognized value
    # must not accidentally enable chunked writes during the opt-in period.
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", value)
    assert resolve_chunked_write_enabled() is False


def test_empty_write_gate_defaults_disabled(monkeypatch):
    # An empty/whitespace `HIPPIUS_CHUNKED_WRITE=` in a profile falls back to the
    # default, which is off (opt-in) for this release.
    monkeypatch.setenv("HIPPIUS_CHUNKED_WRITE", "  ")
    assert resolve_chunked_write_enabled() is False
