"""On-chain subscription lifecycle. Marked `expensive` because:

  - `subscribe` posts a Substrate extrinsic and debits credits.
  - `cancel_subscription` opens a 30-day grace window — the project enters
    "cancelling" state and the robot secret stops working on the next sync.

Only safe to run against a CI-only sandbox account with throwaway credits.
Gated additionally by `HIPPIUS_TEST_ALLOW_CHAIN=1` to prevent accidental
runs even when `-m expensive` is selected.
"""
import os
import time

import pytest

from hippius_hub import console


pytestmark = [pytest.mark.e2e, pytest.mark.expensive]


@pytest.fixture(autouse=True)
def _require_chain_opt_in():
    if os.environ.get("HIPPIUS_TEST_ALLOW_CHAIN") != "1":
        pytest.skip("Set HIPPIUS_TEST_ALLOW_CHAIN=1 to enable chain-touching tests")


def _resolve_free_plan_id():
    """Pick the cheapest available plan so the credit debit is minimal."""
    plans = console.list_plans() or []
    if not plans:
        pytest.skip("No plans available on this backend")
    # Sort by price; first entry is the cheapest.
    cheap = sorted(plans, key=lambda p: p.get("price_credits") or 0)[0]
    return cheap["id"], cheap["name"]


def test_subscribe_then_cancel_lifecycle(console_logged_in):
    """subscribe → wait for chain sync → list_subscriptions contains it →
    cancel_subscription → list_subscriptions shows active=False with
    cancelled_at populated. End-state is the project in a 30-day grace
    window — operationally fine for a sandbox account."""
    plan_id, plan_name = _resolve_free_plan_id()

    sub = console.subscribe(plan_id)
    assert sub.get("extrinsic_hash"), "subscribe should return extrinsic_hash"
    assert sub.get("block_hash"), "subscribe should return block_hash"

    # Chain sync runs every ~3 min; poll up to 5 min.
    deadline = time.time() + 300
    found = None
    while time.time() < deadline:
        rows = console.list_subscriptions() or []
        for r in rows:
            if r.get("plan_name") == plan_name and r.get("active"):
                found = r
                break
        if found:
            break
        time.sleep(20)
    assert found, f"new subscription to {plan_name!r} never appeared in list_subscriptions"

    # Cancel.
    cancel = console.cancel_subscription(found["subscription_id"])
    assert cancel.get("extrinsic_hash")

    # Poll for the cancel to reflect.
    deadline = time.time() + 300
    cancelled = None
    while time.time() < deadline:
        rows = console.list_subscriptions() or []
        for r in rows:
            if r.get("subscription_id") == found["subscription_id"]:
                if not r.get("active") and r.get("cancelled_at"):
                    cancelled = r
                    break
        if cancelled:
            break
        time.sleep(20)
    assert cancelled, "cancellation never reflected in list_subscriptions"
