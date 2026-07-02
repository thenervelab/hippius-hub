"""Best-effort check for a newer `hippius_hub` release on PyPI.

Called once from `cli.main()` after arg parsing so `--help`/`--version`
(which argparse exits out of before we ever get here) stay silent and
fast. Two things this module promises no matter what:

  1. It NEVER raises. Offline, PyPI down, malformed JSON, a `+unknown`
     source-checkout version — all of it is swallowed. A broken update
     check must never be the reason a real command fails.
  2. It NEVER adds a network round trip to every invocation. The result
     is cached to disk (`~/.cache/hippius/hub/update_check.json`) and
     only refreshed once every `CHECK_INTERVAL_SECONDS`; in between, the
     cached verdict is reused with zero I/O beyond a local file read.
"""
import json
import os
import sys
import time
from typing import Optional

import httpx

from . import __version__
from .constants import DEFAULT_CACHE_DIR

PYPI_PACKAGE = "hippius_hub"
PYPI_URL = f"https://pypi.org/pypi/{PYPI_PACKAGE}/json"
CACHE_PATH = os.path.join(DEFAULT_CACHE_DIR, "update_check.json")
CHECK_INTERVAL_SECONDS = 24 * 60 * 60  # re-hit PyPI at most once a day
REQUEST_TIMEOUT = 2.0  # seconds; a stalled update check must never stall the CLI


def _disabled() -> bool:
    """Opt-out knobs. `CI` is the de-facto convention npm/GH CLI/etc. already
    honor for this exact kind of notifier — pipelines that reinstall on every
    run don't need to be told to update."""
    if os.environ.get("HIPPIUS_HUB_NO_UPDATE_CHECK", "").lower() in ("1", "true", "yes"):
        return True
    if os.environ.get("CI", "").lower() in ("1", "true", "yes"):
        return True
    return False


def _parse_version(v: str) -> tuple:
    """Best-effort dotted-int tuple, e.g. '0.5.1' -> (0, 5, 1).

    Stops at the first segment that isn't purely numeric, so pre-release/
    build suffixes like '0.6.0rc1' or '0.0.0+unknown' compare on their
    numeric prefix instead of raising. Good enough for ordering straight
    `X.Y.Z` PyPI releases, which is all this needs.
    """
    parts = []
    for chunk in v.split("."):
        digits = ""
        for ch in chunk:
            if ch.isdigit():
                digits += ch
            else:
                break
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def _read_cache() -> Optional[dict]:
    try:
        with open(CACHE_PATH, "r") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _write_cache(data: dict) -> None:
    try:
        os.makedirs(DEFAULT_CACHE_DIR, exist_ok=True)
        tmp = CACHE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, CACHE_PATH)
    except OSError:
        pass  # caching is purely an optimization; never fatal


def _fetch_latest_version() -> Optional[str]:
    try:
        resp = httpx.get(PYPI_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()["info"]["version"]
    except Exception:
        return None


def _latest_version() -> Optional[str]:
    """Cached latest-version lookup; hits PyPI at most once per
    CHECK_INTERVAL_SECONDS, otherwise returns the cached value."""
    cache = _read_cache()
    now = time.time()
    if cache and (now - cache.get("checked_at", 0)) < CHECK_INTERVAL_SECONDS:
        return cache.get("latest_version")

    latest = _fetch_latest_version()
    stale_fallback = cache.get("latest_version") if cache else None
    _write_cache({"checked_at": now, "latest_version": latest or stale_fallback})
    return latest or stale_fallback


def check_for_update() -> Optional[str]:
    """Print a one-line update recommendation to stderr if PyPI has a newer
    release than the running `__version__`. Stderr (not stdout) so `--json`
    output and other machine-readable/piped output stay clean.

    Best-effort: any failure anywhere in this path is swallowed. Returns the
    latest version string when a warning was printed, else None.
    """
    if _disabled():
        return None
    if "+unknown" in __version__:
        return None  # source checkout, not an installed release — nothing to compare

    try:
        latest = _latest_version()
        if not latest:
            return None
        if _parse_version(latest) > _parse_version(__version__):
            print(
                f"⚠️  A new version of hippius-hub is available: {latest} "
                f"(you have {__version__}). Run `pip install -U hippius_hub` to update.",
                file=sys.stderr,
            )
            return latest
    except Exception:
        pass
    return None