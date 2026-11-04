"""Shutdown notices: `huggingface_hub` replaces this package.

Every byte this package moves goes through the Hippius registry (Harbor). Harbor shuts down
on `HARBOR_SHUTDOWN_DATE`, and its repositories move to the Hippius Hub, which serves the
Hugging Face Hub API. The stock `huggingface_hub` client talks to it directly, so this
package has no successor release.

Importing the package warns once. The Harbor-only surfaces (`login`, `logout`, `whoami`,
the `_harbor` helpers and the `_oci` manifest requests) also warn, but only when called
from outside this package: code such as subnet validators imports `_harbor` and `_oci`
directly, while a plain `hf_hub_download` reaches them internally and is already covered
by the import warning.

All of these are `FutureWarning`, which Python shows by default. `DeprecationWarning` was
rejected: the default filters hide it unless it is raised from `__main__`, so a library
importing `hippius_hub` from a training script or a validator would never see it, and those
are the users who must migrate before the shutdown date.

Every warning starts with "hippius_hub", so one standard filter set before the import
silences them all: `warnings.filterwarnings("ignore", "hippius_hub", FutureWarning)`, or
`PYTHONWARNINGS=ignore:hippius_hub:FutureWarning`. There is deliberately no
package-specific switch: one more environment variable would be one more thing to document
and remove.
"""

import sys
import warnings

# TODO(GeorgiosDelkos, #159): set the real ISO date before tagging the release. The
# release workflow refuses to publish while this placeholder is anywhere in the package.
HARBOR_SHUTDOWN_DATE = "YYYY-MM-DD"

# TODO(GeorgiosDelkos, #159): set the public Hub URL before tagging the release. It is
# HUB_PUBLIC_URL in hippius-hub-rust deploy/production/hub.env, still CHANGE-ME there.
HUB_URL = "https://HUB-URL-TBD"

# The README section on the default branch, not a commit permalink: users who see this
# warning after the release need the current guide, and this repository is public while
# the hippius-hub-rust migration plan behind it is not. Point it at the user-facing guide
# once that exists.
MIGRATION_GUIDE_URL = "https://github.com/thenervelab/hippius-hub#migrating-to-huggingface_hub"

IMPORT_MESSAGE = (
    f"hippius_hub is replaced by huggingface_hub and stops working on {HARBOR_SHUTDOWN_DATE}, "
    "when the Hippius registry (Harbor) shuts down. To migrate: install huggingface_hub; "
    f"export HF_ENDPOINT={HUB_URL} before Python starts (huggingface_hub reads it once, "
    "when it is imported); use a hip_ API key as the token (HF_TOKEN=hip_... or "
    "huggingface_hub.login(token='hip_...')); and replace `hippius_hub` imports with "
    f"`huggingface_hub`. Migration guide: {MIGRATION_GUIDE_URL}"
)


def shutdown_notice(surface: str, instead: str) -> str:
    """Return the notice for one Harbor-only `surface`, naming what to use `instead`."""
    return (
        f"{surface} depends on the Hippius registry (Harbor) and stops working on "
        f"{HARBOR_SHUTDOWN_DATE}. {instead} Migration guide: {MIGRATION_GUIDE_URL}"
    )


def warn_shutdown(surface: str, instead: str) -> None:
    """Warn that `surface` stops working, when the code calling it is outside this package.

    Called as the first statement of the surface. Frame 2 is then the code that called the
    surface, and `stacklevel=3` names that line, so the default filter shows the warning
    once per calling line.
    """
    assert surface.startswith("hippius_hub"), "the documented filter matches this prefix"
    caller = sys._getframe(2).f_globals.get("__name__", "")
    if caller == "hippius_hub" or caller.startswith("hippius_hub."):
        return
    warnings.warn(shutdown_notice(surface, instead), FutureWarning, stacklevel=3)
