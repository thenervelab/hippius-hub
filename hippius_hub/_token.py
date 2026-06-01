"""Typed three-state representation of HF's `token` argument.

HF's `token` parameter accepts:
- `None` or `True` → use the saved token (via `get_token()`)
- `False` → no authentication (anonymous)
- `str` → use this literal string

This module turns those three implicit cases into typed dataclasses so
the auth-resolution code dispatches on a tagged union instead of running
`isinstance(token, str)` / `token is False` checks at every call site.

External callers still pass HF's untyped value; `TokenInput.from_hf(...)`
parses it once at the boundary.
"""
from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class Anonymous:
    """token=False — explicit no-auth. The docker-config fallback is skipped."""


@dataclass(frozen=True)
class UseStored:
    """token=None or token=True — use whatever is in ~/.cache/hippius/hub/token."""


@dataclass(frozen=True)
class Literal:
    """token='...' — use this literal string verbatim."""
    value: str


TokenInput = Union[Anonymous, UseStored, Literal]


def from_hf(token) -> TokenInput:
    """Parse HF's three-state `token` argument into a typed TokenInput.

    >>> from_hf(False)
    Anonymous()
    >>> from_hf(None)
    UseStored()
    >>> from_hf(True)
    UseStored()
    >>> from_hf("abc")
    Literal(value='abc')
    """
    if token is False:
        return Anonymous()
    if token is True or token is None:
        return UseStored()
    if isinstance(token, str):
        return Literal(value=token)
    raise TypeError(f"Unsupported token type: {type(token).__name__}")


__all__ = ["Anonymous", "UseStored", "Literal", "TokenInput", "from_hf"]
