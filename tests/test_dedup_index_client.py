"""C3 — the frozen contract ① client (query/announce), master plan §6.3.

Pure client tests: no upload path, no registry. Everything is fail-open by
construction (§6.6) — the index is a cache, so any error must cost dedup, never the
upload. These pin that promise.
"""
import json

import httpx
import pytest
import respx

from hippius_hub._dedup_index import (
    ChunkRef,
    IndexHit,
    announce_chunks,
    query_chunks,
)
from hippius_hub.constants import resolve_dedup_index_url

INDEX = "https://index.test"
QUERY = f"{INDEX}/v1/chunks/query"
ANNOUNCE = f"{INDEX}/v1/chunks/announce"


# ── config gate ────────────────────────────────────────────────────────────────
def test_resolve_index_url_unset_is_none(monkeypatch):
    monkeypatch.delenv("HIPPIUS_DEDUP_INDEX_URL", raising=False)
    assert resolve_dedup_index_url() is None


def test_resolve_index_url_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("HIPPIUS_DEDUP_INDEX_URL", "https://index.test/")
    assert resolve_dedup_index_url() == "https://index.test"


# ── query: the happy path and the neighbourhood ────────────────────────────────
@respx.mock
def test_query_returns_hit_with_neighbourhood():
    route = respx.post(QUERY).mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": {
                    "a1b2": {
                        "pack": "7e91",
                        "repo": "ns/name",
                        "chunks": [
                            {"digest": "a1b2", "offset": 0, "size": 262144},
                            {"digest": "9f0e", "offset": 262144, "size": 262144},
                        ],
                    }
                }
            },
        )
    )
    hits = query_chunks(INDEX, "tok", ["a1b2", "c3d4"])

    assert route.called
    assert set(hits) == {"a1b2"}
    hit = hits["a1b2"]
    assert isinstance(hit, IndexHit)
    assert hit.pack == "7e91" and hit.repo == "ns/name"
    assert hit.chunks == (
        ChunkRef("a1b2", 0, 262144),
        ChunkRef("9f0e", 262144, 262144),
    )
    # bearer header threaded through; both queried digests sent
    assert route.calls.last.request.headers["Authorization"] == "Bearer tok"
    assert json.loads(route.calls.last.request.content)["chunks"] == ["a1b2", "c3d4"]


@respx.mock
def test_query_miss_returns_empty():
    respx.post(QUERY).mock(return_value=httpx.Response(200, json={"hits": {}}))
    assert query_chunks(INDEX, "tok", ["a1b2"]) == {}


@respx.mock
def test_query_batches_at_256():
    route = respx.post(QUERY).mock(return_value=httpx.Response(200, json={"hits": {}}))
    digests = [f"{i:064x}" for i in range(600)]

    query_chunks(INDEX, "tok", digests)

    assert route.call_count == 3  # 256 + 256 + 88
    for call in route.calls:
        assert len(json.loads(call.request.content)["chunks"]) <= 256


@respx.mock
def test_query_skips_a_malformed_hit_keeps_the_good_one():
    respx.post(QUERY).mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": {
                    "bad": {"repo": "ns/name"},  # no "pack"
                    "good": {"pack": "7e91", "repo": "ns/name", "chunks": []},
                }
            },
        )
    )
    hits = query_chunks(INDEX, "tok", ["bad", "good"])
    assert set(hits) == {"good"}


# ── query: fail-open (§6.6) — every failure mode is a miss, never a raise ───────
@respx.mock
def test_query_failopen_on_5xx():
    respx.post(QUERY).mock(return_value=httpx.Response(503))
    assert query_chunks(INDEX, "tok", ["a1b2"]) == {}


@respx.mock
def test_query_failopen_on_timeout():
    respx.post(QUERY).mock(side_effect=httpx.TimeoutException("slow"))
    assert query_chunks(INDEX, "tok", ["a1b2"]) == {}


@respx.mock
def test_query_failopen_on_malformed_body():
    respx.post(QUERY).mock(return_value=httpx.Response(200, text="not json"))
    assert query_chunks(INDEX, "tok", ["a1b2"]) == {}


def test_query_no_url_is_a_noop():
    assert query_chunks(None, "tok", ["a1b2"]) == {}


@respx.mock
def test_query_partial_availability_returns_what_it_can():
    # First batch hits, second batch times out -> we still return the first's hits.
    calls = {"n": 0}

    def _side_effect(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                200, json={"hits": {"a": {"pack": "p", "repo": "r", "chunks": []}}}
            )
        raise httpx.TimeoutException("slow")

    respx.post(QUERY).mock(side_effect=_side_effect)
    digests = [f"{i:064x}" for i in range(300)]  # 2 batches
    hits = query_chunks(INDEX, "tok", digests)
    assert set(hits) == {"a"}


# ── announce: fire-and-forget, no visibility field ─────────────────────────────
@respx.mock
def test_announce_posts_all_chunks_and_no_visibility_field():
    route = respx.post(ANNOUNCE).mock(return_value=httpx.Response(202))
    announce_chunks(
        INDEX,
        "tok",
        "ns/name",
        "7e91",
        [ChunkRef("a1b2", 0, 262144), ChunkRef("9f0e", 262144, 100)],
    )
    assert route.called
    body = json.loads(route.calls.last.request.content)
    assert body["repo"] == "ns/name" and body["pack"] == "7e91"
    assert len(body["chunks"]) == 2
    # §6.5 — the client must NEVER declare visibility; the service resolves it.
    assert "visibility" not in body
    assert route.calls.last.request.headers["Authorization"] == "Bearer tok"


@respx.mock
def test_announce_never_raises_on_error():
    respx.post(ANNOUNCE).mock(side_effect=httpx.ConnectError("down"))
    # Must not raise — a failed announce costs future dedup, never this push.
    announce_chunks(INDEX, "tok", "ns/name", "7e91", [ChunkRef("a1b2", 0, 1)])


def test_announce_no_url_is_a_noop():
    announce_chunks(None, "tok", "ns/name", "7e91", [ChunkRef("a1b2", 0, 1)])
