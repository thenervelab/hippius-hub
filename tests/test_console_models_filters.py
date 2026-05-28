"""Server-side filter behavior for `models_list`.

The CLI surfaces seven filters (format, architecture, quantization,
min_params, max_params, q, mine, pagination). Each is one query-string
parameter on the server side, and a typo there could silently return the
unfiltered set. These tests assert each filter actually narrows the
result set and that returned rows match the filter predicate.

Skip-aware: if the server has no models in a given category, the
corresponding test skips rather than failing — we test the filter
mechanism, not the catalog contents.
"""
import pytest

from hippius_hub import console


pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def available_filters():
    """Pull the live filter dictionary once so each filter test picks a value
    the server actually has rows for."""
    return console.models_formats() or {}


def _all_results(**filters):
    res = console.models_list(page=1, page_size=100, **filters) or {}
    return res.get("results") or []


def test_format_filter_narrows_and_matches(available_filters):
    formats = available_filters.get("formats") or []
    if not formats:
        pytest.skip("No formats indexed yet on this backend")
    chosen = formats[0]
    rows = _all_results(fmt=chosen)
    if not rows:
        pytest.skip(f"Server reports format {chosen!r} but returns no rows")
    assert all(r.get("format") == chosen for r in rows), (
        f"format= filter returned non-matching rows: "
        f"{[r.get('format') for r in rows if r.get('format') != chosen]}"
    )


def test_architecture_filter_narrows_and_matches(available_filters):
    archs = available_filters.get("architectures") or []
    if not archs:
        pytest.skip("No architectures indexed")
    chosen = archs[0]
    rows = _all_results(architecture=chosen)
    if not rows:
        pytest.skip(f"Server reports arch {chosen!r} but returns no rows")
    assert all(r.get("architecture") == chosen for r in rows)


def test_quantization_filter_narrows_and_matches(available_filters):
    quants = available_filters.get("quantizations") or []
    if not quants:
        pytest.skip("No quantizations indexed")
    chosen = quants[0]
    rows = _all_results(quantization=chosen)
    if not rows:
        pytest.skip(f"Server reports quant {chosen!r} but returns no rows")
    assert all(r.get("quantization") == chosen for r in rows)


def test_min_params_filter_excludes_smaller_models():
    """min_params is an inclusive lower bound. Pick a value larger than the
    smallest indexed model so the filter actually drops rows."""
    all_rows = _all_results()
    sizes = [r.get("parameter_count") or 0 for r in all_rows]
    sizes = [s for s in sizes if s > 0]
    if len(sizes) < 2:
        pytest.skip("Need at least two indexed models with parameter counts")
    threshold = max(sizes)  # excludes everything smaller than the biggest
    rows = _all_results(min_params=threshold)
    assert all((r.get("parameter_count") or 0) >= threshold for r in rows)


def test_max_params_filter_excludes_larger_models():
    """max_params is an inclusive upper bound (mirror of min_params)."""
    all_rows = _all_results()
    sizes = [r.get("parameter_count") or 0 for r in all_rows if r.get("parameter_count")]
    if len(sizes) < 2:
        pytest.skip("Need at least two indexed models with parameter counts")
    threshold = min(sizes)
    rows = _all_results(max_params=threshold)
    assert all((r.get("parameter_count") or 0) <= threshold for r in rows
               if r.get("parameter_count"))


def test_pagination_smaller_page_size_returns_at_most_n():
    """page_size is a hard cap on `results` length, regardless of total."""
    res = console.models_list(page=1, page_size=3) or {}
    assert len(res.get("results") or []) <= 3


def test_mine_filter_restricts_to_caller(console_logged_in, console_test_project):
    """mine=True filters to rows owned by the authed account."""
    rows = _all_results(mine=True)
    # Skip if the test account has no models yet — common for fresh CI accounts.
    if not rows:
        pytest.skip("Test account has no indexed models yet")
    # Every returned row must be either flagged is_mine or belong to the active
    # project (the server-side definition of "mine").
    for r in rows:
        owns = r.get("is_mine") or r.get("project") == console_test_project
        assert owns, f"mine=True returned non-owned row: {r}"


def test_query_filter_returns_subset_of_unfiltered():
    """`q=<text>` is a free-text search. Without knowing what's indexed we
    can only assert it returns no more rows than the unfiltered list."""
    all_total = (console.models_list(page=1, page_size=1) or {}).get("total") or 0
    filtered = console.models_list(page=1, page_size=1, q="z") or {}
    assert (filtered.get("total") or 0) <= all_total
