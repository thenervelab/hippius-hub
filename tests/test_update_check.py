"""Tests for `hippius_hub._update_check`.

Covers: version comparison, the disable knobs (env var + CI convention),
the disk cache (fresh vs stale, write-then-reuse), and that any failure
anywhere in the path (network, bad JSON, offline) is swallowed rather
than raised — this module backs a nag banner, not a required feature,
so it must never be able to break an otherwise-working CLI invocation.
"""
import json
import time

import httpx
import pytest
import respx

from hippius_hub import _update_check as uc


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    """Point the module's cache file at a scratch path so tests never read
    or write the real `~/.cache/hippius/hub/update_check.json`."""
    cache_path = tmp_path / "update_check.json"
    monkeypatch.setattr(uc, "CACHE_PATH", str(cache_path))
    yield cache_path


@pytest.fixture(autouse=True)
def _no_env_disable(monkeypatch):
    """Tests opt into the disable knobs explicitly; don't inherit the host
    shell's CI env var (real CI runs would otherwise silently skip these)."""
    monkeypatch.delenv("HIPPIUS_HUB_NO_UPDATE_CHECK", raising=False)
    monkeypatch.delenv("CI", raising=False)


# ---- version comparison ----


@pytest.mark.parametrize(
    "a, b, a_gt_b",
    [
        ("0.5.1", "0.5.0", True),
        ("0.6.0", "0.5.9", True),
        ("1.0.0", "0.9.9", True),
        ("0.5.1", "0.5.1", False),
        ("0.5.1", "0.5.2", False),
        ("0.6.0rc1", "0.5.1", True),  # numeric prefix still orders correctly
        ("0.0.0+unknown", "0.0.0", False),
    ],
)
def test_parse_version_ordering(a, b, a_gt_b):
    assert (uc._parse_version(a) > uc._parse_version(b)) is a_gt_b


# ---- disable knobs ----


def test_disabled_via_explicit_env_var(monkeypatch):
    monkeypatch.setenv("HIPPIUS_HUB_NO_UPDATE_CHECK", "1")
    assert uc._disabled() is True


def test_disabled_in_ci(monkeypatch):
    monkeypatch.setenv("CI", "true")
    assert uc._disabled() is True


def test_not_disabled_by_default():
    assert uc._disabled() is False


def test_check_for_update_noop_when_disabled(monkeypatch, capsys):
    monkeypatch.setenv("HIPPIUS_HUB_NO_UPDATE_CHECK", "1")
    result = uc.check_for_update()
    assert result is None
    assert capsys.readouterr().err == ""


def test_check_for_update_noop_for_unknown_source_version(monkeypatch, capsys):
    monkeypatch.setattr(uc, "__version__", "0.0.0+unknown")
    result = uc.check_for_update()
    assert result is None
    assert capsys.readouterr().err == ""


# ---- happy path: newer version on PyPI ----


@respx.mock
def test_check_for_update_warns_on_newer_release(monkeypatch, capsys):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    respx.get(uc.PYPI_URL).mock(
        return_value=httpx.Response(200, json={"info": {"version": "0.6.0"}})
    )

    latest = uc.check_for_update()

    assert latest == "0.6.0"
    err = capsys.readouterr().err
    assert "0.6.0" in err
    assert "0.5.1" in err
    assert "pip install -U hippius_hub" in err


@respx.mock
def test_check_for_update_silent_when_up_to_date(monkeypatch, capsys):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    respx.get(uc.PYPI_URL).mock(
        return_value=httpx.Response(200, json={"info": {"version": "0.5.1"}})
    )

    latest = uc.check_for_update()

    assert latest is None
    assert capsys.readouterr().err == ""


# ---- failure modes must never raise ----


@respx.mock
def test_check_for_update_swallows_network_error(monkeypatch, capsys):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    respx.get(uc.PYPI_URL).mock(side_effect=httpx.ConnectError("offline"))

    latest = uc.check_for_update()  # must not raise

    assert latest is None
    assert capsys.readouterr().err == ""


@respx.mock
def test_check_for_update_swallows_malformed_json(monkeypatch, capsys):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    respx.get(uc.PYPI_URL).mock(return_value=httpx.Response(200, text="not json"))

    latest = uc.check_for_update()  # must not raise

    assert latest is None
    assert capsys.readouterr().err == ""


@respx.mock
def test_check_for_update_swallows_http_error(monkeypatch, capsys):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    respx.get(uc.PYPI_URL).mock(return_value=httpx.Response(500))

    latest = uc.check_for_update()  # must not raise

    assert latest is None
    assert capsys.readouterr().err == ""


# ---- caching behaviour ----


@respx.mock
def test_latest_version_hits_network_once_then_uses_cache(monkeypatch, _isolated_cache):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    route = respx.get(uc.PYPI_URL).mock(
        return_value=httpx.Response(200, json={"info": {"version": "0.6.0"}})
    )

    first = uc._latest_version()
    second = uc._latest_version()

    assert first == "0.6.0"
    assert second == "0.6.0"
    assert route.call_count == 1, "second call within the cache window should not hit PyPI"


@respx.mock
def test_latest_version_refetches_after_cache_expires(monkeypatch, _isolated_cache):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    route = respx.get(uc.PYPI_URL).mock(
        return_value=httpx.Response(200, json={"info": {"version": "0.6.0"}})
    )

    uc._latest_version()
    # Simulate the cache having gone stale.
    stale = json.loads(_isolated_cache.read_text())
    stale["checked_at"] = time.time() - uc.CHECK_INTERVAL_SECONDS - 1
    _isolated_cache.write_text(json.dumps(stale))

    uc._latest_version()

    assert route.call_count == 2


@respx.mock
def test_latest_version_falls_back_to_stale_cache_on_network_failure(
    monkeypatch, _isolated_cache
):
    monkeypatch.setattr(uc, "__version__", "0.5.1")
    _isolated_cache.write_text(
        json.dumps({"checked_at": 0.0, "latest_version": "0.5.9"})
    )
    respx.get(uc.PYPI_URL).mock(side_effect=httpx.ConnectError("offline"))

    latest = uc._latest_version()

    assert latest == "0.5.9", "a failed refresh should fall back to the last known-good value"