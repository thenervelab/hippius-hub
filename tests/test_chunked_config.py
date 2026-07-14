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
    DEFAULT_PACK_SIZE,
    MAX_PACK_SIZE,
    resolve_cdc_avg_size,
    resolve_chunk_threshold,
    resolve_chunked_write_enabled,
    resolve_pack_size,
)


def test_threshold_default(monkeypatch):
    monkeypatch.delenv("HIPPIUS_CHUNK_THRESHOLD", raising=False)
    assert resolve_chunk_threshold() == DEFAULT_CHUNK_THRESHOLD == 256 * 1024 * 1024


def test_cdc_avg_default_is_fastcdc_ceiling(monkeypatch):
    # 4 MiB is fastcdc's AVERAGE_MAX — the largest average the splitter accepts.
    # A larger default (the original 64 MiB) panics the Rust chunker.
    monkeypatch.delenv("HIPPIUS_CDC_AVG_SIZE", raising=False)
    assert resolve_cdc_avg_size() == DEFAULT_CDC_AVG_SIZE == 4 * 1024 * 1024


def test_pack_size_default(monkeypatch):
    monkeypatch.delenv("HIPPIUS_PACK_SIZE", raising=False)
    assert resolve_pack_size() == DEFAULT_PACK_SIZE == 64 * 1024 * 1024


def test_pack_size_accepts_value_at_the_cap(monkeypatch):
    # The bound is inclusive — an off-by-one here would reject a legal configuration.
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", str(MAX_PACK_SIZE))
    assert resolve_pack_size() == MAX_PACK_SIZE


def test_pack_size_above_cap_is_rejected_at_upload(monkeypatch):
    # The reader buffers a pack whole and refuses one declaring more than MAX_PACK_SIZE
    # (src/chunk_fetcher.rs). Writing packs above that bound would produce an artifact
    # nothing can read back, so it must fail here — loudly, at configuration time —
    # rather than at every future download.
    monkeypatch.setenv("HIPPIUS_PACK_SIZE", str(MAX_PACK_SIZE + 1))
    with pytest.raises(ValueError, match="per-pack maximum"):
        resolve_pack_size()


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
