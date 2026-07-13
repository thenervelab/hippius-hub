"""`group_files` — the read-side collapse of manifest layers into logical files.

Property: whatever layout a file was stored in (plain K=1 blob, or a chunked-v2
`pointer.v2` layer plus its untitled pack blobs), `group_files` recovers exactly
one entry per logical file with the whole-file size/digest. The property test
round-trips a reference manifest builder; the explicit cases pin the edges
(0-byte, mixed plain/chunked, packs skipped, and every malformed-pointer
rejection). v2 carries the chunk→pack mapping in the pointer BLOB, so `group_files`
never parses positional chunk layers — it only reads the pointer's annotations.
"""
import hashlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hippius_hub._oci import group_files
from hippius_hub.constants import (
    CHUNK_COUNT_KEY,
    CHUNKED_LAYOUT_V2,
    FILE_DIGEST_KEY,
    FILE_SIZE_KEY,
    LAYER_TITLE_KEY,
    PACK_MEDIA_TYPE,
    POINTER_MEDIA_TYPE_V2,
)
from hippius_hub.errors import MalformedManifestError


# ---- reference manifest builder (mirrors the chunked-v2 layout the uploader writes) ----

def _digest(seed: bytes) -> str:
    return "sha256:" + hashlib.sha256(seed).hexdigest()


def _plain_layer(title: str, digest: str, size: int) -> dict:
    return {
        "mediaType": "application/octet-stream",
        "size": size,
        "digest": digest,
        "annotations": {LAYER_TITLE_KEY: title},
    }


def _pointer_v2_layer(title: str, file_size: int, file_digest: str, chunk_count: int) -> dict:
    return {
        "mediaType": POINTER_MEDIA_TYPE_V2,
        "size": 200,
        "digest": _digest(f"pointer:{title}".encode()),
        "annotations": {
            LAYER_TITLE_KEY: title,
            FILE_SIZE_KEY: str(file_size),
            FILE_DIGEST_KEY: file_digest,
            CHUNK_COUNT_KEY: str(chunk_count),
        },
    }


def _pack_layer(digest: str, size: int) -> dict:
    # Untitled: group_files skips it (resolved via the pointer blob, not by position).
    return {"mediaType": PACK_MEDIA_TYPE, "size": size, "digest": digest}


def _build_manifest(shapes: list) -> dict:
    """Turn a list of file `shapes` into a valid chunked-v2 manifest.

    Titles are assigned positionally (`file0.bin`, ...) so the test doesn't have
    to fight hypothesis for uniqueness; the invariant under test is layout
    round-trip, not title generation. Each chunked file emits a pointer layer plus
    its (untitled) pack layers, which group_files must skip.
    """
    layers = []
    for i, shape in enumerate(shapes):
        title = f"file{i}.bin"
        if shape["kind"] == "plain":
            layers.append(_plain_layer(title, shape["digest"], shape["size"]))
        else:
            layers.append(_pointer_v2_layer(title, shape["size"], shape["digest"], shape["chunk_count"]))
            layers.extend(_pack_layer(d, sz) for d, sz in shape["packs"])
    return {"schemaVersion": 2, "layers": layers}


# ---- property test ----

_digests = st.builds(_digest, st.binary(min_size=1, max_size=24))
_plain = st.builds(
    lambda d, s: {"kind": "plain", "digest": d, "size": s},
    _digests,
    st.integers(min_value=0, max_value=10**12),
)
_chunked = st.builds(
    lambda packs, d, sz, cc: {"kind": "chunked", "digest": d, "size": sz, "chunk_count": cc, "packs": packs},
    st.lists(st.tuples(_digests, st.integers(min_value=1, max_value=10**9)), min_size=1, max_size=4),
    _digests,
    st.integers(min_value=1, max_value=10**12),
    st.integers(min_value=1, max_value=64),
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
            assert g.layout == CHUNKED_LAYOUT_V2
            assert g.size == shape["size"]
            assert g.pointer_digest is not None


# ---- explicit edge cases ----

def test_plain_single_file():
    m = _build_manifest([{"kind": "plain", "digest": _digest(b"a"), "size": 123}])
    (g,) = group_files(m)
    assert not g.is_chunked and g.size == 123 and g.layout is None


def test_zero_byte_file_is_k1():
    # A 0-byte file is stored as one plain blob (empty-sha256); it must NOT be
    # mistaken for a chunked file.
    empty = "sha256:" + hashlib.sha256(b"").hexdigest()
    (g,) = group_files(_build_manifest([{"kind": "plain", "digest": empty, "size": 0}]))
    assert not g.is_chunked and g.size == 0


def test_chunked_v2_reads_whole_file_metadata_and_skips_packs():
    packs = [(_digest(f"p{i}".encode()), 64 * 1024 * 1024) for i in range(3)]
    m = _build_manifest([{
        "kind": "chunked", "digest": _digest(b"whole"),
        "size": 200 * 1024 * 1024, "chunk_count": 50, "packs": packs,
    }])
    # Three pack layers plus the pointer, but only ONE logical file surfaces.
    (g,) = group_files(m)
    assert g.is_chunked and g.layout == CHUNKED_LAYOUT_V2
    assert g.size == 200 * 1024 * 1024
    assert g.pointer_digest == _digest(b"pointer:file0.bin")


def test_mixed_plain_and_chunked_preserves_order_and_boundaries():
    # A chunked big.bin (with pack layers) followed by a plain config.json: the
    # plain file must not be shadowed by big.bin's untitled packs, and both surface
    # once, in order.
    packs = [(_digest(f"b{i}".encode()), 10) for i in range(3)]
    m = _build_manifest([
        {"kind": "chunked", "digest": _digest(b"big"), "size": 30, "chunk_count": 3, "packs": packs},
        {"kind": "plain", "digest": _digest(b"cfg"), "size": 7},
    ])
    big, cfg = group_files(m)
    assert big.title == "file0.bin" and big.is_chunked and big.layout == CHUNKED_LAYOUT_V2
    assert cfg.title == "file1.bin" and not cfg.is_chunked and cfg.size == 7


# ---- malformed pointers must be rejected, not silently degraded ----

def test_reject_zero_count_pointer():
    # A count=0 pointer would reference no chunks — a malformed artifact.
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"x"), "size": 5, "chunk_count": 0, "packs": []}])
    with pytest.raises(MalformedManifestError, match=">= 1"):
        group_files(m)


def test_reject_non_integer_count():
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"x"), "size": 5, "chunk_count": 1, "packs": []}])
    m["layers"][0]["annotations"][CHUNK_COUNT_KEY] = "not-a-number"
    with pytest.raises(MalformedManifestError):
        group_files(m)


def test_reject_missing_file_size_annotation():
    m = _build_manifest([{"kind": "chunked", "digest": _digest(b"x"), "size": 5, "chunk_count": 1, "packs": []}])
    del m["layers"][0]["annotations"][FILE_SIZE_KEY]
    with pytest.raises(MalformedManifestError):
        group_files(m)


def test_skip_foreign_untitled_layers():
    # A repo shared with other tooling can hold untitled layers of some OTHER media
    # type (a `docker`/`oras` push). group_files must SKIP them (degrade to the
    # titled subset, as the pre-chunking reader did) instead of hard-failing every
    # read API on a co-located foreign manifest. Foreign layers appear both between
    # our files and trailing, to exercise the mid-loop and end-of-loop skip.
    foreign = {
        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
        "size": 999,
        "digest": _digest(b"foreign"),
    }  # untitled, non-pointer
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
