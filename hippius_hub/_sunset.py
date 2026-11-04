"""Shutdown notices: `huggingface_hub` replaces this package.

Every byte this package moves goes through the Hippius registry (Harbor). Harbor shuts down
on `HARBOR_SHUTDOWN_DATE`, and its repositories move to the Hippius Hub, which serves the
Hugging Face Hub API. The stock `huggingface_hub` client talks to it directly, so this
package has no successor release.

All notices are `FutureWarning`, which Python shows by default. `DeprecationWarning` was
rejected: the default filters hide it unless it is raised from `__main__`, so a library
importing `hippius_hub` from a training script or a validator would never see it, and those
are the users who must migrate before the shutdown date.

Every notice starts with "hippius_hub", so one standard filter set before the import
silences them all: `warnings.filterwarnings("ignore", "hippius_hub", FutureWarning)`, or
`PYTHONWARNINGS=ignore:hippius_hub:FutureWarning`. There is deliberately no
package-specific switch: one more environment variable would be one more thing to document
and remove.
"""

import warnings

# TODO(GeorgiosDelkos, #159): set the real date, as YYYY-MM-DD, before tagging the release.
HARBOR_SHUTDOWN_DATE = "YYYY-MM-DD"

# TODO(GeorgiosDelkos, #159): set the public Hub URL before tagging the release. It is
# HUB_PUBLIC_URL in hippius-hub-rust deploy/production/hub.env, still CHANGE-ME there.
HUB_URL = "https://HUB-URL-TBD"

# Permalink to the parity and migration plan in hippius-hub-rust. Replace it with the
# user-facing migration guide once that exists.
MIGRATION_GUIDE_URL = (
    "https://github.com/thenervelab/hippius-hub-rust/blob/"
    "fc31435e90712b971d9714a3ef1f11bee20e87f6/"
    "docs/plans/2026-10-30-python-hub-parity-and-migration.md"
)

IMPORT_MESSAGE = (
    f"hippius_hub is replaced by huggingface_hub and stops working on {HARBOR_SHUTDOWN_DATE}, "
    "when the Hippius registry (Harbor) shuts down. To migrate: install huggingface_hub, "
    f"set HF_ENDPOINT={HUB_URL}, use a hip_ API key as the token (HF_TOKEN=hip_... or "
    "huggingface_hub.login(token='hip_...')), and replace `hippius_hub` imports with "
    f"`huggingface_hub`. Migration guide: {MIGRATION_GUIDE_URL}"
)


def shutdown_notice(surface: str, instead: str) -> str:
    """Return the notice for one Harbor-only `surface`, naming what to use `instead`.

    A `surface` passed to `warn_shutdown` starts with "hippius_hub", so the filter in
    the module docs matches its warning.
    """
    return (
        f"{surface} depends on the Hippius registry (Harbor) and stops working on "
        f"{HARBOR_SHUTDOWN_DATE}. {instead} Migration guide: {MIGRATION_GUIDE_URL}"
    )


def warn_shutdown(surface: str, instead: str) -> None:
    """Emit `shutdown_notice` as a `FutureWarning` attributed to the caller's caller.

    `stacklevel=3` skips this function and the surface that calls it, so the warning names
    the line that used the surface. The default filter then shows it once per such line.
    """
    warnings.warn(shutdown_notice(surface, instead), FutureWarning, stacklevel=3)
