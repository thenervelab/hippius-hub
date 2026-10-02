"""ChunkStream FFI smoke tests — the pull-based streaming counterpart of
`chunk_and_hash_native` (B3.2).

Pins the wire-level equivalence (streamed batches concatenate to exactly the
buffered chunk list, `finish()` returns the same whole-file hex), the explicit
finish contract (draining to None first — never a silent drain), the error
surfacing for a file that cannot be opened (the open happens on the producer
thread, so it raises on the first `next_batch`), and that abandoning a stream
mid-way does not hang (drop-side pipeline unwind; the Rust-side termination
proof lives in `uploader::chunk_stream_tests`).
"""
from __future__ import annotations

import random

import pytest

try:
    from hippius_hub.hippius_core import chunk_and_hash_native, chunk_stream_native
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )

# Small average over a ~5 MiB file -> ~130 chunks, several 64-chunk batches,
# so multi-batch emission and ordering are actually exercised.
AVG_SIZE = 32 * 1024
FILE_SIZE = 5 * 1024 * 1024


@pytest.fixture()
def sample_file(tmp_path):
    # Deterministic pseudo-random bytes: CDC needs non-degenerate content for
    # realistic boundaries, and a fixed seed keeps failures reproducible.
    path = tmp_path / "sample.bin"
    path.write_bytes(random.Random(7).randbytes(FILE_SIZE))
    return str(path)


def test_stream_matches_chunk_and_hash(sample_file):
    expected_whole, expected_chunks = chunk_and_hash_native(sample_file, AVG_SIZE)

    stream = chunk_stream_native(sample_file, AVG_SIZE)
    batches = []
    while (batch := stream.next_batch()) is not None:
        assert batch, "an empty batch must never be emitted"
        assert len(batch) <= 64, "batches are capped at CHUNK_BATCH"
        batches.append(batch)

    assert len(batches) >= 2, "fixture must span multiple batches"
    flat = [triple for batch in batches for triple in batch]
    assert flat == expected_chunks
    assert stream.finish() == expected_whole
    # EOF and finish are idempotent.
    assert stream.next_batch() is None
    assert stream.finish() == expected_whole


def test_finish_before_drain_raises(sample_file):
    stream = chunk_stream_native(sample_file, AVG_SIZE)
    with pytest.raises(RuntimeError, match="not finished"):
        stream.finish()
    # The refusal must not have consumed anything: draining still works.
    assert stream.next_batch() is not None


def test_unreadable_path_raises_on_first_next_batch(tmp_path):
    stream = chunk_stream_native(str(tmp_path / "does-not-exist.bin"), AVG_SIZE)
    with pytest.raises(RuntimeError):
        stream.next_batch()
    # Poisoned, not hung: subsequent calls keep raising clearly.
    with pytest.raises(RuntimeError, match="chunk stream failed"):
        stream.next_batch()


def test_abandoned_stream_does_not_hang(sample_file):
    # Pull one batch, then walk away. Dropping the object drops the receiver,
    # which unwinds the producer thread through the pipeline's acyclic
    # shutdown — if that broke, interpreter exit (or this test) would hang.
    stream = chunk_stream_native(sample_file, AVG_SIZE)
    assert stream.next_batch() is not None
    del stream
