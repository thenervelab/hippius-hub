"""What the hub serves this client, for the live tests.

The live tests run against `registry.hippius.com`, whose projects, `test` included, moved
from Harbor to the Hippius Hub in November 2026 (hippius-hub-rust
`docs/plans/2026-11-18-production-cutover-plan.md`). The hub serves this client's reads. It
refuses its model pushes, which `test_hub_refuses_push.py` asserts. The Harbor API
(`/api/v2.0`) is not the hub's: the edge answers a write to a moved project there with 410,
and reads still go to Harbor, which does not know a `hip_` key, so `whoami` and the deletes of
`delete_repo` fail. Until W12 of that plan gives this client a way to do those against the
hub, the tests that need them carry `MOVES_TO_W12`.
"""

import pytest

# Strict: a test that starts to pass is reported as a failure, so the mark is removed when
# W12 lands rather than hiding a test that works again.
MOVES_TO_W12 = pytest.mark.xfail(
    strict=True,
    reason=(
        "Moves to W12: needs a model push, which the hub refuses from this client "
        "(test_hub_refuses_push.py), or the Harbor API, which does not take a hip_ key"
    ),
)
