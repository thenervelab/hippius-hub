"""The hourly sweep must not treat a timed-out delete as a reported success.

api.hippius.com often answers DELETE after the client's read timeout, and
Harbor removes the tag a few seconds later. An immediate re-list still shows
the tag. That tag is not a hard failure; a delete that returned and left the
tag listed is.
"""
import importlib.util
from pathlib import Path


def _load_sweep():
    path = Path(__file__).resolve().parents[2] / "smoke" / "_sweep.py"
    spec = importlib.util.spec_from_file_location("hippius_smoke_sweep", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sweep = _load_sweep()


def test_timeout_still_listed_is_not_a_false_success():
    false_success, still_pending = sweep.judge_immediate(
        accepted=[],
        pending=["smoke-old"],
        listed={"smoke-old"},
    )
    assert false_success == []
    assert still_pending == ["smoke-old"]


def test_returned_delete_still_listed_is_a_false_success():
    false_success, still_pending = sweep.judge_immediate(
        accepted=["smoke-old"],
        pending=[],
        listed={"smoke-old"},
    )
    assert false_success == ["smoke-old"]
    assert still_pending == []


def test_timeout_already_gone_needs_no_settle():
    false_success, still_pending = sweep.judge_immediate(
        accepted=["smoke-ok"],
        pending=["smoke-slow"],
        listed=set(),
    )
    assert false_success == []
    assert still_pending == []


def test_mixed_batch_keeps_each_revisions_own_bucket():
    false_success, still_pending = sweep.judge_immediate(
        accepted=["smoke-ok", "smoke-lied"],
        pending=["smoke-slow", "smoke-denied"],
        listed={"smoke-lied", "smoke-denied", "unrelated"},
    )
    assert false_success == ["smoke-lied"]
    assert still_pending == ["smoke-denied"]


def test_empty_names_does_not_list():
    def list_names():
        raise AssertionError("list_names should not be called")

    still = sweep.wait_until_unlisted(
        list_names,
        [],
        settle_seconds=45,
        poll_seconds=5,
        monotonic=lambda: 0.0,
        sleep=lambda _seconds: None,
    )
    assert still == []


def test_already_absent_does_not_sleep():
    slept = []

    still = sweep.wait_until_unlisted(
        lambda: set(),
        ["smoke-old"],
        settle_seconds=45,
        poll_seconds=5,
        monotonic=lambda: 0.0,
        sleep=slept.append,
    )
    assert still == []
    assert slept == []


def test_tag_that_disappears_during_the_window_is_gone():
    clock = {"t": 0.0}
    listings = iter([{"smoke-old"}, {"smoke-old"}, set()])

    def monotonic():
        return clock["t"]

    def sleep(seconds):
        clock["t"] += seconds

    still = sweep.wait_until_unlisted(
        lambda: next(listings),
        ["smoke-old"],
        settle_seconds=45,
        poll_seconds=5,
        monotonic=monotonic,
        sleep=sleep,
    )
    assert still == []
    assert clock["t"] == 10


def test_tag_still_listed_at_the_deadline_is_returned():
    clock = {"t": 0.0}
    calls = {"n": 0}

    def list_names():
        calls["n"] += 1
        return {"smoke-old"}

    def monotonic():
        return clock["t"]

    def sleep(seconds):
        clock["t"] += seconds

    still = sweep.wait_until_unlisted(
        list_names,
        ["smoke-old"],
        settle_seconds=12,
        poll_seconds=5,
        monotonic=monotonic,
        sleep=sleep,
    )
    assert still == ["smoke-old"]
    assert clock["t"] == 12
    assert calls["n"] == 4


def test_a_cleared_tag_drops_out_while_the_other_is_waited_out():
    clock = {"t": 0.0}

    def list_names():
        if clock["t"] < 5:
            return {"smoke-a", "smoke-b"}
        return {"smoke-b"}

    def sleep(seconds):
        clock["t"] += seconds

    still = sweep.wait_until_unlisted(
        list_names,
        ["smoke-a", "smoke-b"],
        settle_seconds=12,
        poll_seconds=5,
        monotonic=lambda: clock["t"],
        sleep=sleep,
    )
    assert still == ["smoke-b"]
    assert clock["t"] == 12
