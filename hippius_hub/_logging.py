"""Progress lines and opt-in verbose logging, both on stderr.

`progress()` prints the per-transfer status lines (`Downloading ...`,
`Uploaded: ...`) that are on by default. `configure_logging()` attaches a stderr
handler only when verbose logging is requested (HIPPIUS_DEBUG / RUST_LOG set; the
CLI flips HIPPIUS_DEBUG on for `--verbose`).

Neither writes to stdout: stdout belongs to the caller. A script running
`hippius-hub download ... > out` or a Python program printing its own results
must not get transfer chatter mixed in (issue #137). The diagnose report is the
one CLI output on stdout.
"""
import logging
import sys

from tqdm import tqdm

from .constants import debug_enabled

_LOGGER_NAME = "hippius_hub"


def configure_logging() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if debug_enabled():
        logger.setLevel(logging.DEBUG)
        if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
            )
            logger.addHandler(handler)
    return logger


def progress(msg: str) -> None:
    """Print one progress line to stderr.

    `tqdm.write` rather than `print` so the line lands above a live `tqdm` bar
    (the folder upload's) instead of through it.
    """
    tqdm.write(msg, file=sys.stderr)
