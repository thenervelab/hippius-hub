"""Revision naming shared by the smoke conftest and the sweep test.

Lives in its own module rather than in `conftest.py` so the test file can
import it by name — pytest prepends this directory to `sys.path`, but importing
`conftest` directly is an implementation detail of pytest's collection.
"""
from datetime import datetime
from datetime import timezone

PREFIX = "smoke-"
TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"


def build(short_id: str) -> str:
    stamp = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
    return f"{PREFIX}{stamp}-{short_id}"


def parse_timestamp(revision: str):
    """Return the UTC datetime encoded in a `smoke-<YYYYmmdd-HHMMSS>-<short>`
    revision, or None if this isn't one of ours.

    Returning None rather than raising is what keeps the sweep from touching
    revisions it doesn't own — e2e revisions live in the same namespace.
    """
    if not revision.startswith(PREFIX):
        return None
    parts = revision[len(PREFIX):].split("-")
    if len(parts) < 2:
        return None
    try:
        stamp = datetime.strptime("-".join(parts[:2]), TIMESTAMP_FORMAT)
    except ValueError:
        return None
    return stamp.replace(tzinfo=timezone.utc)
