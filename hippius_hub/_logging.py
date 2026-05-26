"""Opt-in verbose logging for the transport layer.

Quiet by default. `configure_logging()` attaches a stderr handler only when
verbose logging is requested (HIPPIUS_DEBUG / RUST_LOG set; the CLI flips
HIPPIUS_DEBUG on for `--verbose`). Output stays on stderr so it never pollutes
stdout, where the diagnose report and progress bars live.
"""
import logging
import sys

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
