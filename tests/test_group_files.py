"""`group_files` — the read-side collapse of manifest layers into logical files.

Property: whatever layout a file was stored in (plain K=1 blob, or a pointer +
K untitled chunk layers), `group_files` recovers exactly one entry per logical
file with the whole-file size/digest and the ordered chunk list. The property
test round-trips a reference Option-B manifest builder; the explicit cases pin
the edges the plan calls out (0-byte, size divisible by chunk size, mixed
plain/chunked, and every malformed-manifest rejection).
"""
import hashlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hippius_hub._oci import group_files
from hippius_hub.constants import (
    CHUNK_COUNT_KEY,
    CHUNK_MEDIA_TYPE,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    LAYER_TITLE_KEY,
    POINTER_MEDIA_TYPE,
)
from hippius_hub.errors import MalformedManifestError


# ---- reference manifest builder (mirrors the Option-B layout Phase 3 writes) ----

def _digest(seed: bytes) -> str:
    return "sha256:" + hashlib.sha256(seed).hexdigest()


def _plain_layer(title: str, digest: str, size: int) -> dict:
    return {
        "mediaType": "application/octet-stream",
        "size": size,
        "digest": digest,
        "annotations": {LAYER_TITLE_KEY: title},
    }


def _pointer_layer(title: str, file_size: int, file_digest: str, chunk_count: int) -> dict:
    return {
        "mediaType": POINTER_MEDIA_TYPE,
        "size": 200,
        "digest": _digest(f"pointer:{title}".encode()),
        "annotations": {
            LAYER_TITLE_KEY: title,
            FILE_SIZE_KEY: str(file_size),
            FILE_DIGEST_KEY: file_digest,
            CHUNK_COUNT_KEY: str(chunk_count),
        },
    }


def _chunk_layer(digest: str, size: int) -> dict:
    return {"mediaType": CHUNK_MEDIA_TYPE, "size": size, "digest": digest}


def _build_manifest(shapes: list) -> dict:
    """Turn a list of file `shapes` into a valid Option-B manifest.

    Titles are assigned positionally (`file0.bin`, ...) so the test doesn't have
    to fight hypothesis for uniqueness; the invariant under test is layout
    round-trip, not title generation.
    """
    layers = []
    for i, shape in enumerate(shapes):
        title = f"file{i}.bin"
        if shape["kind"] == "plain":
            layers.append(_plain_layer(title, shape["digest"], shape["size"]))
        else:
            size = sum(sz for _, sz in shape["chunks"])
            layers.append(_pointer_layer(title, size, shape["digest"], len(shape["chunks"])))
            layers.extend(_chunk_layer(d, sz) for d, sz in shape["chunks"])
    return {"schemaVersion": 2, "layers": layers}


# ---- property test ----

_digests = st.builds(_digest, st.binary(min_size=1, max_size=24))
_plain = st.builds(
    lambda d, s: {"kind": "plain", "digest": d, "size": s},
    _digests,
    st.integers(min_value=0, max_value=10**12),
)
_chunked = st.builds(
    lambda cs, d: {"kind": "chunked", "digest": d, "chunks": cs},
    st.lists(st.tuples(_digests, st.integers(min_value=1, max_value=10**9)), min_size=1, max_size=6),
    _digests,
)


@given(st.lists(st.one_of(_plain, _chunked), max_size=8))
def test_group_files_roundtrips_the_layout(shapes):
    groups = group_files(_build_manifest(shapes))

    assert len(groups) == len(shapes)
    for i, (shape, g) in enumerate(zip(shapes, groups)):
        assert g.title == f"file{i}.bin"
        assert g.digest == shape["digest"]
        if shape["kind"] == "plain":
            assert not g.is_chunked
            assert g.size == shape["size"]
        else:
            assert g.is_chunked
            assert g.size == sum(sz for _, sz in shape["chunks"])
            assert [(c.digest, c.size) for c in g.chunks] == shape["chunks"]


# ---- explicit edge cases ----

def test_plain_single_file():
    m = _build_manifest([{"kind": "plain", "digest": _digest(b"a"), "size": 123}])
    (g,) = group_files(m)
    assert not g.is_chunked and g.size == 123 and g.chunks == ()


def test_zero_byte_file_is_k1():
    # A 0-byte file is stored as one plain blob (empty-sha256); it must NOT be
    # mistaken for a chunked file with no chunks.
    empty = "sha256:" + hashlib.sha256(b"").hexdigest()
    (g,) = group_files(_build_manifest([{"kind": "plain", "digest": empty, "size": 0}]))
    assert not g.is_chunked and g.size == 0


def test_size_exactly_divisible_no_empty_tail_chunk():
    # 3 equal 64-MiB chunks, whole size an exact multiple — no phantom 0-byte
    # trailing chunk should appear.
    chunks = [(_digest(f"c{i}".encode()), 64 * 1024 * 1024) for i in range(3)]
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"whole"), "chunks": chunks}])
    (g,) = group_files(m)
    assert g.is_chunked and len(g.chunks) == 3
    assert g.size == 3 * 64 * 1024 * 1024
    assert all(c.size == 64 * 1024 * 1024 for c in g.chunks)


def test_mixed_plain_and_chunked_preserves_order_and_boundaries():
    # chunked big.bin (3 chunks) followed by a plain config.json: the plain file
    # must not absorb big.bin's chunks, and big.bin must claim exactly its three.
    chunks = [(_digest(f"b{i}".encode()), 10) for i in range(3)]
    m = _build_manifest([
        {"kind": "chunked", "digest": _digest(b"big"), "chunks": chunks},
        {"kind": "plain", "digest": _digest(b"cfg"), "size": 7},
    ])
    big, cfg = group_files(m)
    assert big.title == "file0.bin" and big.is_chunked and len(big.chunks) == 3
    assert cfg.title == "file1.bin" and not cfg.is_chunked and cfg.size == 7


# ---- malformed manifests must be rejected, not silently truncated ----

def test_reject_count_greater_than_chunks_present():
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"x"), "chunks": [(_digest(b"c0"), 5)]}])
    m["layers"][0]["annotations"][CHUNK_COUNT_KEY] = "3"  # promise 3, supply 1
    with pytest.raises(MalformedManifestError, match="promises 3"):
        group_files(m)


def test_reject_zero_count_pointer():
    # A count=0 pointer would otherwise collapse to a plain file whose whole-file
    # blob was never uploaded (404 on download); it must be rejected.
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"x"), "chunks": [(_digest(b"c0"), 5)]}])
    m["layers"] = [m["layers"][0]]  # drop the chunk layer
    m["layers"][0]["annotations"][CHUNK_COUNT_KEY] = "0"
    with pytest.raises(MalformedManifestError, match=">= 1"):
        group_files(m)


def test_reject_non_integer_count():
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"x"), "chunks": [(_digest(b"c0"), 5)]}])
    m["layers"][0]["annotations"][CHUNK_COUNT_KEY] = "not-a-number"
    with pytest.raises(MalformedManifestError):
        group_files(m)


def test_reject_missing_file_size_annotation():
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"x"), "chunks": [(_digest(b"c0"), 5)]}])
    del m["layers"][0]["annotations"][FILE_SIZE_KEY]
    with pytest.raises(MalformedManifestError):
        group_files(m)


def test_reject_orphan_chunk_without_pointer():
    # An untitled *chunk* layer with no preceding pointer is our-layout corruption
    # (its whole-file context is gone) — must raise, not skip.
    m = {"schemaVersion": 2, "layers": [_chunk_layer(_digest(b"orphan"), 5)]}
    with pytest.raises(MalformedManifestError, match="preceding pointer"):
        group_files(m)


def test_skip_foreign_untitled_layers():
    # A repo shared with other tooling can hold untitled layers of some OTHER
    # media type (a `docker`/`oras` push). group_files must SKIP them (degrade to
    # the titled subset, as the pre-chunking reader did) instead of hard-failing
    # every read API on a co-located foreign manifest. Foreign layers appear both
    # between our files and trailing, to exercise the mid-loop and end-of-loop skip.
    foreign = {
        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
        "size": 999,
        "digest": _digest(b"foreign"),
    }  # untitled, non-chunk
    m = {
        "schemaVersion": 2,
        "layers": [
            _plain_layer("model.bin", _digest(b"m"), 42),
            foreign,
            _plain_layer("config.json", _digest(b"c"), 7),
            foreign,
        ],
    }
    groups = group_files(m)
    assert [g.title for g in groups] == ["model.bin", "config.json"]
    assert all(not g.is_chunked for g in groups)


def test_reject_chunk_run_interrupted_by_titled_layer():
    # pointer promises 3 chunks but a new titled file starts after only 1.
    chunks = [(_digest(b"c0"), 5)]
    m = _build_manifest([
        {"kind": "chunked", "digest": _digest(b"x"), "chunks": chunks},
        {"kind": "plain", "digest": _digest(b"y"), "size": 9},
    ])
    m["layers"][0]["annotations"][CHUNK_COUNT_KEY] = "3"
    with pytest.raises(MalformedManifestError):
        group_files(m)
