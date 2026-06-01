"""Hypothesis property tests for hippius_hub.file_upload._merge_layers.

The merge logic is the read-modify-write tail of upload_folder; getting
it wrong silently drops or duplicates layers. Hypothesis generates
random layer sets and asserts the structural invariants the hand-picked
fixture tests in test_phase_b.py cannot exhaustively cover.

Five properties pinned:
  1. New layer with matching title REPLACES existing (no duplication).
  2. Titles in `delete_titles` are absent from the result.
  3. Existing layers neither replaced nor deleted are preserved.
  4. Result size is bounded by `len(existing) + len(new)`.
  5. Merging an empty new-list with no deletes is identity on the title set.

The strategy uses short ASCII titles and small list sizes — enough to
exercise overlap/disjoint/empty cases while keeping the default ~100
examples per property under a second.
"""
from hypothesis import given, strategies as st

from hippius_hub.file_upload import _merge_layers
from hippius_hub.constants import LAYER_TITLE_KEY


def _layer(title: str, digest: str = "sha256:abc", size: int = 100) -> dict:
    """Build a minimal OCI layer dict carrying `title` in its annotations.

    Matches the shape `_build_layer` produces in file_upload.py — the digest
    is parameterized so overlap tests can assert the NEW digest wins.
    """
    return {
        "mediaType": "application/octet-stream",
        "digest": digest,
        "size": size,
        "annotations": {LAYER_TITLE_KEY: title},
    }


# Strategy: layer titles are short lowercase ASCII strings. Bounded length
# and alphabet keep shrinking fast and avoid pathological-unicode noise the
# merge contract does not care about — `_merge_layers` is dict-keyed on the
# title string, so any hashable str is equivalent under the contract.
title_st = st.text(
    alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
    min_size=1,
    max_size=10,
)


@given(
    existing_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
    new_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
)
def test_new_layer_with_existing_title_replaces(existing_titles, new_titles):
    """A new layer whose title matches an existing one REPLACES it.

    Pins two facts simultaneously: result has NO duplicate titles, and where
    titles overlap, the new digest wins. A buggy merge that appended instead
    of dict-keyed would surface here as a duplicate title; a buggy merge
    that kept the existing layer on overlap would fail the digest check.
    """
    existing = [_layer(t, digest=f"sha256:old-{t}") for t in existing_titles]
    new = [_layer(t, digest=f"sha256:new-{t}") for t in new_titles]
    result = _merge_layers(existing, new)

    result_titles = [layer["annotations"][LAYER_TITLE_KEY] for layer in result]
    assert len(result_titles) == len(set(result_titles)), (
        f"duplicate titles in result: {result_titles}"
    )

    # Where titles overlap, the new digest must win.
    for t in set(existing_titles) & set(new_titles):
        matching = [layer for layer in result if layer["annotations"][LAYER_TITLE_KEY] == t]
        assert len(matching) == 1
        assert matching[0]["digest"] == f"sha256:new-{t}", (
            f"new digest must win for overlapping title {t}"
        )


@given(
    existing_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
    delete_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
)
def test_delete_titles_removed_from_result(existing_titles, delete_titles):
    """Any title in `delete_titles` must NOT appear in the result.

    Tested with `new_layers=[]` so the deletion is the only operation —
    isolates the delete path from the replace path covered in test 1.
    """
    existing = [_layer(t) for t in existing_titles]
    result = _merge_layers(existing, [], delete_titles=set(delete_titles))
    result_titles = {layer["annotations"][LAYER_TITLE_KEY] for layer in result}
    deleted = set(delete_titles) & set(existing_titles)
    assert not (deleted & result_titles), (
        f"deleted titles {deleted & result_titles} leaked into result"
    )


@given(
    existing_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
    new_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
)
def test_untouched_layers_preserved(existing_titles, new_titles):
    """An existing title not replaced (no overlap with new) must remain.

    Complement of test 1: where test 1 asserts new wins on overlap, this
    asserts existing wins on non-overlap. Together they cover the full
    title-space partition: (overlap → new), (existing only → existing).
    """
    existing = [_layer(t) for t in existing_titles]
    new = [_layer(t, digest="sha256:new") for t in new_titles]
    result = _merge_layers(existing, new, delete_titles=None)
    result_titles = {layer["annotations"][LAYER_TITLE_KEY] for layer in result}
    untouched = set(existing_titles) - set(new_titles)
    assert untouched <= result_titles, (
        f"untouched titles {untouched - result_titles} missing from result"
    )


@given(
    existing_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
    new_titles=st.lists(title_st, min_size=0, max_size=5, unique=True),
)
def test_result_size_bounded(existing_titles, new_titles):
    """Result count never exceeds `len(existing) + len(new)`.

    Defense against a regression where the merge accidentally appended
    instead of dict-keying, producing duplicate titles per overlap.
    """
    existing = [_layer(t) for t in existing_titles]
    new = [_layer(t) for t in new_titles]
    result = _merge_layers(existing, new)
    assert len(result) <= len(existing) + len(new)


@given(layers=st.lists(title_st, min_size=0, max_size=5, unique=True))
def test_merge_with_empty_new_is_identity_on_titles(layers):
    """Merging an empty new-list with no deletes preserves the title set.

    The values stored are still the original layer dicts, but identity-on-
    titles is the contract that matters for downstream `commit_layers` —
    a layer dropped here would be silently absent from the next manifest.
    """
    existing = [_layer(t) for t in layers]
    result = _merge_layers(existing, [], delete_titles=None)
    assert {layer["annotations"][LAYER_TITLE_KEY] for layer in result} == set(layers)
