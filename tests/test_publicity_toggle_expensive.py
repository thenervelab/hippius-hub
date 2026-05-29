"""Project publicity toggle. Marked `expensive` because flipping public/private
resizes the project quota (different size tiers per plan), and concurrent
tests assuming a stable state would see drift.

Test always restores the original state in teardown so the project ends up
where it started even if the test raises mid-flight.
"""
import os

import pytest

from hippius_hub import console


pytestmark = [pytest.mark.e2e, pytest.mark.expensive]


@pytest.fixture(autouse=True)
def _require_prod_mutation_opt_in():
    if os.environ.get("HIPPIUS_TEST_ALLOW_PROD_MUTATION") != "1":
        pytest.skip("Set HIPPIUS_TEST_ALLOW_PROD_MUTATION=1 to enable publicity toggle")


def test_publicity_round_trip(console_logged_in):
    """Snapshot original state → flip → flip back → confirm both flips took
    effect server-side. End state matches start."""
    me_before = console.me()
    original = bool(me_before.get("public"))

    try:
        flipped = console.toggle_publicity(public=not original)
        assert flipped.get("public") is (not original)

        me_mid = console.me()
        assert bool(me_mid.get("public")) is (not original)

        # Quota changes between public and private tiers; just assert it's
        # a positive int after the flip so we know the server recomputed it.
        assert (me_mid.get("storage_quota_bytes") or 0) > 0
    finally:
        # Restore original state regardless of what happened above.
        console.toggle_publicity(public=original)
        me_after = console.me()
        assert bool(me_after.get("public")) is original, (
            "teardown failed to restore original publicity — manual fix needed"
        )
