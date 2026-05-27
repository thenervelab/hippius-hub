"""Regression: Python caller of download_file_native handles None correctly.

Audit L6 (Phase 3.12): the Rust side now returns `Optional[str]` (was
`str` with `""` as an in-band "skipped" sentinel). The Python caller
must dispatch on `calculated_hash is None`, not on the verify_hash flag
or string emptiness, otherwise the empty-string sentinel resurrects
silently the next time someone refactors the Rust signature.

The assertions below are structural source-grep — a behavioral test
would need a working OCI manifest fixture, a mock blob server, and a
maturin-built native extension, which is a Phase 4+ concern. The
contract we pin here is single-paragraph: "the Python caller routes on
the typed Option, not on the verify_hash flag".
"""

import inspect

from hippius_hub import file_download


def test_download_to_cache_dispatches_on_calculated_hash_not_verify_hash():
    """`_download_to_cache` must branch on `calculated_hash is not None`.

    Pinned because the old code branched on `verify_hash` and treated
    `""` as the absent value — both bad. The new contract is: the
    Rust-returned `Optional[str]` is the single source of truth for
    "was the hash computed?". The verify_hash flag is the *input* that
    controls the Rust side; the *output* is then consumed by `is None`.
    """
    src = inspect.getsource(file_download._download_to_cache)

    # Positive assertion: the new dispatch shape must be present. The
    # substring is specific to the variable name so an unrelated `is not
    # None` (e.g. on `target_digest`) cannot accidentally satisfy this.
    assert "calculated_hash is not None" in src, (
        "_download_to_cache must dispatch on `calculated_hash is not "
        "None` — post Phase 3.12, the Rust side returns Optional[str] "
        "and the Python caller must consume it as such."
    )

    # Negative assertion: the old `verify_hash`-flag dispatch must be
    # gone. Catches a partial revert that re-introduces the flag-based
    # fork while leaving the new check accidentally present.
    assert "calculated_hash if verify_hash else" not in src, (
        "_download_to_cache still uses the old `verify_hash`-based "
        "fork; the Phase 3.12 contract dispatches on the returned "
        "Optional value, not on the input flag."
    )
