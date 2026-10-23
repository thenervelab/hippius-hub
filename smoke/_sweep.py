"""Classify smoke-revision deletes after the console API has answered.

A DELETE that returns is judged on the spot: the tag must already be gone.
A DELETE that raises (the client's read timeout is the common case) gets one
shared settle window first. Harbor often removes the tag a few seconds after
api.hippius.com has blown the client's timeout, and an immediate re-list
still shows it. The window is shared across the batch so a backlog of
timeouts does not stack another wait onto every tag.
"""


# Long enough for a delete that lands shortly after the 30s client timeout.
# Observed lag from the timeout to Harbor's 200 is well under this.
SETTLE_SECONDS = 45.0
SETTLE_POLL_SECONDS = 5.0


def split_by_listing(names, listed):
    """Return `(gone, still)`, each in the order of `names`."""
    listed = set(listed)
    gone = [name for name in names if name not in listed]
    still = [name for name in names if name in listed]
    return gone, still


def judge_immediate(accepted, pending, listed):
    """Split a listing taken right after the delete calls.

    `accepted` revisions are ones `delete_artifact` returned for. Any of
    those still listed are a hard failure: the API reported success and the
    tag is still there (a 204 that hid a Harbor 404, or the wrong project).

    `pending` revisions are ones that raised something other than 404. The
    ones still listed need the settle window before they count as a backlog.
    The ones already absent were removed after the client gave up.
    """
    _, false_success = split_by_listing(accepted, listed)
    _, still_pending = split_by_listing(pending, listed)
    return false_success, still_pending


def wait_until_unlisted(
    list_names, names, *, settle_seconds, poll_seconds, monotonic, sleep
):
    """Re-check `names` until each is absent or `settle_seconds` elapses.

    `list_names` is called once immediately, then again after each sleep
    while any name remains and the deadline is still ahead. Returns the
    names still listed when the wait ends, in the original order. An empty
    `names` does not call `list_names`.
    """
    if not names:
        return []
    deadline = monotonic() + settle_seconds
    _, still = split_by_listing(names, list_names())
    while still and monotonic() < deadline:
        remaining = deadline - monotonic()
        step = min(poll_seconds, remaining)
        if step <= 0:
            break
        sleep(step)
        _, still = split_by_listing(names, list_names())
    return still
