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
    DEFAULT_PACK_SIZE,
    FASTCDC_MAXIMUM_MAX,
    MAX_PACK_BYTES,
    resolve_cdc_avg_size,
    resolve_chunk_threshold,
    resolve_chunked_write_enabled,
    resolve_pack_size,
    resolve_verify_hash,
)


def test_threshold_default(monkeypatch):
    monkeypatch.delenv("HIPPIUS_CHUNK_THRESHOLD", raising=False)
    assert resolve_chunk_threshold() == DEFAULT_CHUNK_THRESHOLD == 256 * 1024 * 1024


def test_cdc_avg_default_is_fastcdc_ceiling(monkeypatch):
    # 4 MiB is fastcdc's AVERAGE_MAX — the largest average the splitter accepts.
    # A larger default (the original 64 MiB) panics the Rust chunker.
    monkeypatch.delenv("HIPPIUS_CDC_AVG_SIZE", raising=False)
    assert resolve_cdc_avg_size() == DEFAULT_CDC_AVG_SIZE == 4 * 1024 * 1024


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


def test_pack_size_default_fits_reader_cap_with_cdc_overshoot(monkeypatch):
    monkeypatch.delenv("HIPPIUS_PACK_SIZE", raising=False)
    value = resolve_pack_size()
    assert value == DEFAULT_PACK_SIZE
    assert value + FASTCDC_MAXIMUM_MAX <= MAX_PACK_BYTES


def test_pack_size_rejects_target_that_overshoots_reader_cap(monkeypatch):
    # Exactly MAX_PACK_BYTES would produce packs of MAX + 16 MiB - 1 that
    # the reader refuses. The guard is pack_size + 16 MiB <= MAX, not
    # pack_size <= MAX.
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", str(MAX_PACK_BYTES))
    with pytest.raises(ValueError, match="overshoot"):
        resolve_pack_size()
    monkeypatch.setenv(
        "HIPPIUS_PACK_SIZE", str(MAX_PACK_BYTES - FASTCDC_MAXIMUM_MAX)
    )
    assert resolve_pack_size() == MAX_PACK_BYTES - FASTCDC_MAXIMUM_MAX


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


_PACK_CAP = MAX_PACK_BYTES - FASTCDC_MAXIMUM_MAX


@pytest.mark.parametrize("raw", [str(_PACK_CAP - 1), str(_PACK_CAP), "1", str(DEFAULT_PACK_SIZE)])
def test_pack_size_accepts_values_up_to_and_including_the_cap(monkeypatch, raw):
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", raw)
    assert resolve_pack_size() == int(raw)


@pytest.mark.parametrize("raw", [str(_PACK_CAP + 1), str(MAX_PACK_BYTES + 1), str(2**40)])
def test_pack_size_rejects_every_value_over_the_cap(monkeypatch, raw):
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", raw)
    with pytest.raises(ValueError, match="overshoot"):
        resolve_pack_size()


@pytest.mark.parametrize("raw", ["0", "-1", "-67108864"])
def test_pack_size_rejects_non_positive_values(monkeypatch, raw):
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", raw)
    with pytest.raises(ValueError, match="positive"):
        resolve_pack_size()


@pytest.mark.parametrize("raw", ["64M", "abc", "1.5", "0x40", " "])
def test_pack_size_rejects_non_numeric_values(monkeypatch, raw):
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", raw)
    with pytest.raises(ValueError):
        resolve_pack_size()


def test_pack_size_blank_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", "")
    assert resolve_pack_size() == DEFAULT_PACK_SIZE
