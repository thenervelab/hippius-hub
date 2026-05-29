"""Unit regression tests for the silent-HF-routing bug class.

Two production bugs in this class were fixed in the same PR as these tests:

  1. `HippiusApi()` with no `endpoint` kwarg inherited HfApi's default
     (`huggingface.co`) and forwarded it via `_inject`, silently routing
     every method call at HF. Surfaced as 404s in CI run 26559541574.

  2. `ModelCard` / `DatasetCard` / `RepoCard` were re-exported verbatim
     from huggingface_hub. Their `.load()` and `.push_to_hub()` action
     methods are hardcoded to huggingface.co and would silently fetch /
     upload README files at HF when called under the hippius_hub
     namespace.

These tests are PURE UNIT (no e2e marker) so they run on every PR —
defending against the regression even on dev environments without
registry creds. If either bug ever reappears these would fail in milliseconds.
"""
import pytest

from hippius_hub import DatasetCard, HippiusApi, ModelCard, RepoCard
from hippius_hub.constants import DEFAULT_REGISTRY_URL


# ---- HippiusApi endpoint default ----

def test_hippius_api_defaults_to_hippius_endpoint():
    """`HippiusApi()` with no args must point at the Hippius registry, NOT
    huggingface.co. Without this guard, `HippiusApi().upload_file(...)`
    silently routes at HF (was CI run 26559541574 failure)."""
    api = HippiusApi()
    assert api.endpoint == DEFAULT_REGISTRY_URL
    assert "huggingface" not in api.endpoint.lower()


def test_hippius_api_respects_explicit_endpoint():
    """An explicit endpoint kwarg must override the Hippius default — that's
    the documented extension point for self-hosted Hippius registries."""
    api = HippiusApi(endpoint="https://my.hippius.example")
    assert api.endpoint == "https://my.hippius.example"


def test_hippius_api_explicit_none_endpoint_falls_back_to_hippius():
    """`HippiusApi(endpoint=None)` is the same as `HippiusApi()` — both
    must resolve to the Hippius default. Catches a regression where the
    falsy check accidentally accepts None as 'use HF default'."""
    api = HippiusApi(endpoint=None)
    assert api.endpoint == DEFAULT_REGISTRY_URL


# ---- Card action methods blocked ----

@pytest.mark.parametrize("Card", [RepoCard, ModelCard, DatasetCard])
def test_card_push_to_hub_raises_not_implemented(Card):
    """`Card.push_to_hub()` in huggingface_hub hits huggingface.co. The
    Hippius subclass must raise NotImplementedError so a user porting
    code from huggingface_hub gets a clear error instead of silently
    uploading their README to the wrong place."""
    # Build a minimal instance — RepoCard accepts a raw README string.
    card = Card("---\n---\n# README\n")
    with pytest.raises(NotImplementedError, match="huggingface.co"):
        card.push_to_hub("any/repo")


@pytest.mark.parametrize("Card", [RepoCard, ModelCard, DatasetCard])
def test_card_load_classmethod_raises_not_implemented(Card):
    """Same defense for the classmethod load path."""
    with pytest.raises(NotImplementedError, match="huggingface.co"):
        Card.load("any/repo")


@pytest.mark.parametrize("Card", [RepoCard, ModelCard, DatasetCard])
def test_card_validate_raises_not_implemented(Card):
    """`Card.validate()` in huggingface_hub POSTs the card YAML to
    huggingface.co/api/validate-yaml (repocard.py). Like `push_to_hub`/`load`
    it routes at HF, so a drop-in user calling `card.validate()` would silently
    ship metadata to huggingface.co. It must raise NotImplementedError — same
    HF-routing leak class as the other two action methods."""
    card = Card("---\nlicense: mit\n---\n# README\n")
    with pytest.raises(NotImplementedError, match="huggingface.co"):
        card.validate()


@pytest.mark.parametrize("Card", [RepoCard, ModelCard, DatasetCard])
def test_card_local_methods_still_work(Card, tmp_path):
    """The block is specifically on the two network methods. Local
    operations (constructor, `save`, dict access) must keep working —
    that's the whole reason we subclass instead of dropping the export."""
    card = Card("---\nlicense: mit\n---\n# Hi\n")
    out = tmp_path / "README.md"
    card.save(str(out))
    assert out.read_text().startswith("---")


@pytest.mark.parametrize("Card", [RepoCard, ModelCard, DatasetCard])
def test_card_is_hf_subclass_for_compatibility(Card):
    """Downstream code may do `isinstance(card, huggingface_hub.RepoCard)`.
    Subclassing preserves that contract while still blocking the network
    methods."""
    import huggingface_hub
    assert issubclass(Card, huggingface_hub.RepoCard)


def test_card_error_message_points_at_hippius_alternative():
    """The NotImplementedError must tell the user how to do the same thing
    with hippius_hub primitives — bare 'not supported' wastes their time."""
    card = ModelCard("---\n---\n# x\n")
    with pytest.raises(NotImplementedError, match="upload_file"):
        card.push_to_hub("any/repo")
    with pytest.raises(NotImplementedError, match="hf_hub_download"):
        ModelCard.load("any/repo")
