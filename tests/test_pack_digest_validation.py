"""A corrupted pointer's malformed chunk digest must fail fast with a clear error.

The native pack assembler parses every chunk digest at the FFI boundary, before
any network fetch: a non-hex or wrong-length digest could never verify anyway,
so it raises a RuntimeError naming the pack URL, chunk index, and offending
string instead of surfacing later as an opaque per-chunk integrity mismatch (or
worse, a traceback crash). The pack URL below points at port 9 (discard), which
refuses connections instantly - so if a fetch were ever attempted the test would
fail fast with a connection error, proving the parse happens first.
"""
from __future__ import annotations

import pytest

try:
    from hippius_hub.hippius_core import download_packs_native
except ImportError:
    pytest.skip(
        "hippius_core extension not built; run `maturin develop`",
        allow_module_level=True,
    )


@pytest.mark.parametrize("bad_digest", ["not-a-digest", "abc123", "g" * 64])
def test_malformed_chunk_digest_raises_clear_error(tmp_path, bad_digest):
    pack_url = "http://127.0.0.1:9/v2/acme/model/blobs/sha256:dead"
    with pytest.raises(RuntimeError, match="bad sha256 hex") as excinfo:
        download_packs_native(
            pack_urls=[pack_url],
            pack_sizes=[4],
            pack_chunks=[[(0, 4, 0, bad_digest)]],
            dest_path=str(tmp_path / "out.bin"),
            total_size=4,
            file_digest=None,
        )
    # The message names the pack, chunk index, and offending input so a corrupted
    # pointer is locatable, not just detectable.
    message = str(excinfo.value)
    assert f"pack {pack_url} chunk 0" in message
    assert bad_digest in message
    # Fail-fast means no destination file is ever created.
    assert not (tmp_path / "out.bin").exists()
