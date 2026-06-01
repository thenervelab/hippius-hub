"""Regression: TokenInput typed dispatch correctly parses HF's three states."""
import pytest
from hippius_hub._token import Anonymous, UseStored, Literal, from_hf


def test_false_parses_to_anonymous():
    assert from_hf(False) == Anonymous()


def test_none_parses_to_use_stored():
    assert from_hf(None) == UseStored()


def test_true_parses_to_use_stored():
    assert from_hf(True) == UseStored()


def test_string_parses_to_literal():
    assert from_hf("abc-token") == Literal(value="abc-token")


def test_empty_string_parses_to_literal():
    """An empty string is still a string — parse as Literal, don't reinterpret."""
    assert from_hf("") == Literal(value="")


def test_unsupported_type_raises():
    with pytest.raises(TypeError, match="Unsupported token type: int"):
        from_hf(123)


def test_dataclasses_are_frozen():
    """TokenInput dataclasses are immutable — protects against accidental mutation."""
    anon = Anonymous()
    with pytest.raises(AttributeError):
        anon.value = "x"  # type: ignore[attr-defined]
    lit = Literal(value="x")
    with pytest.raises(AttributeError):
        lit.value = "y"
