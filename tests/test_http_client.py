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


def test_shared_client_persists_no_cookies():
    # Regression guard for the CSRF bug: Harbor exempts token-auth requests from
    # CSRF only while no session cookie is present, so the shared client must store
    # NO cookies -- a replayed Harbor sid/_gorilla_csrf cookie flips Harbor into
    # session mode and 403s the manifest PUT ("CSRF token not found in request").
    # The offline respx suite can't exercise this (it emits no Set-Cookie), so pin
    # the reject-all policy directly.
    c = _http.client()
    c.cookies.clear()
    resp = httpx.Response(
        200,
        headers=[("Set-Cookie", "sid=abc; Path=/"), ("Set-Cookie", "_gorilla_csrf=xyz; Path=/")],
        request=httpx.Request("GET", "https://registry.test.invalid/service/token"),
    )
    c.cookies.extract_cookies(resp)
    assert dict(c.cookies) == {}
