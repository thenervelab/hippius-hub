"""Race-condition test for audit C3 / RACE-1: token file is never world-readable.

The previous `auth.login` used write-then-chmod, which opens the file
under the process umask (typically 0o022 → file is 0o644) and chmod's
to 0o600 in a separate syscall. Between the two syscalls — even on a
fast SSD, ~10 µs — a reader could observe the bearer token at world-
readable mode. On shared CI runners or multi-tenant boxes that is a
real exfiltration window.

The fix (`auth._atomic_write_secret`) writes to a sibling tempfile
(mkstemp creates with 0o600 on POSIX), enforces the mode with fchmod,
then atomically rename's over the destination. Readers see either the
old file or the new file — never an intermediate wrong-mode state.

This test pins that invariant. A polling thread watches the token
path while `login()` is called repeatedly; if any observation lands
on a mode other than {file-absent, 0o600}, the test fails.
"""
from __future__ import annotations

import os
import stat
import sys
import threading
import time

import pytest

from hippius_hub import auth


pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="POSIX permission bits are not enforced on Windows; chmod is a no-op there.",
)


def test_token_file_never_world_readable_under_concurrent_login(tmp_path, monkeypatch):
    """Spawn a polling thread that records token-file modes for ~200 ms while
    login() is invoked repeatedly. The polling thread runs faster than the
    syscall pair would allow if write-then-chmod were back — at ~50 µs per
    observation we get ~4000 samples, enough to land on the wrong-mode
    window if it exists.

    Acceptable observed states:
      - file does not exist (between writes)
      - file exists at exactly 0o600 (post-rename)
    Any other mode is the regression.
    """
    token_file = tmp_path / "token"
    monkeypatch.setattr(auth, "TOKEN_PATH", str(token_file))

    bad_modes: list[int] = []
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            try:
                mode = os.stat(token_file).st_mode & 0o777
                if mode != 0o600:
                    bad_modes.append(mode)
            except FileNotFoundError:
                pass
            # Yield aggressively without sleeping — we want to catch a
            # write-then-chmod window of a few microseconds.

    poller = threading.Thread(target=poll, daemon=True)
    poller.start()

    try:
        # Many login() calls so the polling thread has many windows to land in.
        for i in range(50):
            auth.login(token=f"bearer-value-{i}")
            # Tiny sleep so the OS scheduler gives the poller a turn between
            # logins; without this the test thread could starve the poller
            # on a busy box.
            time.sleep(0.001)
    finally:
        stop.set()
        poller.join(timeout=5)

    assert not bad_modes, (
        f"token file observed at non-0o600 modes during login: "
        f"{sorted({oct(m) for m in bad_modes})!r} "
        f"({len(bad_modes)} observations out of N). "
        f"A regression to write-then-chmod re-opens the world-readable window."
    )
    # And the final state must still be 0o600 — the rename target.
    final_mode = os.stat(token_file).st_mode & 0o777
    assert final_mode == 0o600, f"final mode is 0o{final_mode:o}, expected 0o600"


def test_atomic_write_secret_cleans_up_tempfile_on_error(tmp_path, monkeypatch):
    """If `os.replace` fails (e.g. cross-device error or destination is a
    locked file on Windows), the tempfile must be unlinked so we don't
    leak `.token-XXX.tmp` files in the cache directory.

    Stub `os.replace` to raise; assert the only files left in the
    target directory after the failure are the ones already present
    (i.e. our tempfile got cleaned up).
    """
    target = tmp_path / "token"

    def boom(src, dst):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(auth.os, "replace", boom)

    with pytest.raises(OSError, match="simulated rename failure"):
        auth._atomic_write_secret(str(target), "secret-content")

    leftover = [p.name for p in tmp_path.iterdir()]
    assert leftover == [], (
        f"tempfile leaked after os.replace failure: {leftover!r}. "
        f"Without the finally-block unlink, every failing login() call "
        f"would litter ~/.cache/hippius/hub/ with .token-*.tmp."
    )


def test_atomic_write_secret_overwrites_existing_file(tmp_path):
    """Same path, two writes — second must replace, not append, and mode
    must remain 0o600 across both.
    """
    target = tmp_path / "token"

    auth._atomic_write_secret(str(target), "first")
    assert target.read_text() == "first"
    assert os.stat(target).st_mode & 0o777 == 0o600

    auth._atomic_write_secret(str(target), "second")
    assert target.read_text() == "second"
    assert os.stat(target).st_mode & 0o777 == 0o600
