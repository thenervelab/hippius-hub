"""The shared control-plane httpx.Client is a lazily-built process singleton (change #2).

Pins the two behaviors the pooling win rests on: one instance is reused across
calls (so connections stay warm), and `_close()` (the atexit handler) drops it so a
later call rebuilds rather than reusing a closed pool.
"""
import httpx

from hippius_hub import _http


def test_client_is_singleton():
    a = _http.client()
    b = _http.client()
    assert a is b
    assert isinstance(a, httpx.Client)


def test_close_rebuilds_on_next_use():
    first = _http.client()
    _http._close()
    second = _http.client()
    assert first is not second
    assert isinstance(second, httpx.Client)
