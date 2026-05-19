"""Cachet health monitor for `registry.hippius.com`.

Polls `HIPPIUS_HEALTH_URL` on an interval and PUTs the result to a Cachet
component. Designed to run as a long-lived k8s Deployment in the
`hippius-hub` namespace; Cachet itself sits in the `monitoring` namespace
and is normally reached via cluster-internal DNS.

Cachet component status enum (PUT /api/v1/components/{id}):
    1 = Operational
    2 = Performance Issues
    3 = Partial Outage
    4 = Major Outage

We only emit 1 or 4 — anything that isn't a clean "200 + ok=true" is a
major outage. Promoting to a more nuanced mapping later is purely a
matter of widening `_probe_health`'s return type and the push body.
"""
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple

import httpx


# Cachet's component-status enum. Kept as named constants so the call site
# reads as "Operational"/"Major Outage" rather than magic numbers.
STATUS_OPERATIONAL = 1
STATUS_MAJOR_OUTAGE = 4


@dataclass(frozen=True)
class Config:
    health_url: str
    cachet_url: str
    cachet_token: str
    component_id: str
    poll_interval_s: float
    request_timeout_s: float


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"FATAL: required env var {name} is not set", file=sys.stderr, flush=True)
        sys.exit(2)
    return val


def _load_config() -> Config:
    return Config(
        health_url=_require_env("HIPPIUS_HEALTH_URL"),
        cachet_url=_require_env("CACHET_URL").rstrip("/"),
        cachet_token=_require_env("CACHET_TOKEN"),
        component_id=_require_env("CACHET_COMPONENT_ID"),
        poll_interval_s=float(os.environ.get("POLL_INTERVAL_SECONDS", "60")),
        request_timeout_s=float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "10")),
    )


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{ts} {msg}", flush=True)


def _probe_health(client: httpx.Client, url: str, timeout: float) -> Tuple[bool, Optional[int], str]:
    """Returns (healthy, latency_ms_reported_by_server, detail).

    `healthy` is True iff HTTP 200 and the JSON body has `ok == true`.
    `detail` is a short, log-friendly description of the outcome.
    """
    r = client.get(url, timeout=timeout)
    if r.status_code != 200:
        return False, None, f"http {r.status_code}"
    body = r.json()
    ok = bool(body.get("ok"))
    latency = body.get("latency_ms")
    return ok, latency if isinstance(latency, int) else None, "ok" if ok else "ok=false"


def _push_status(client: httpx.Client, cfg: Config, healthy: bool) -> str:
    # Cachet 3.x: PUT /api/components/{id} guarded by Sanctum bearer auth
    # (verified against Cachet 3.x-dev in registry.starkleytech.com/hippius/cachet).
    # The legacy `/v1` prefix and `X-Cachet-Token` header from Cachet 2.x are gone.
    status = STATUS_OPERATIONAL if healthy else STATUS_MAJOR_OUTAGE
    r = client.put(
        f"{cfg.cachet_url}/api/components/{cfg.component_id}",
        headers={
            "Authorization": f"Bearer {cfg.cachet_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        content=json.dumps({"status": status}),
        timeout=cfg.request_timeout_s,
    )
    if r.status_code >= 400:
        return f"push_failed http={r.status_code} body={r.text[:200]!r}"
    return f"push_ok status={status}"


def main() -> None:
    cfg = _load_config()
    _log(
        f"starting: health_url={cfg.health_url} cachet={cfg.cachet_url} "
        f"component={cfg.component_id} interval={cfg.poll_interval_s}s"
    )

    with httpx.Client() as client:
        while True:
            # Per-iteration try/except is the one place this is intentional:
            # the entire point of the worker is that a failing probe (network
            # error, JSON parse error, Cachet 5xx) MUST become a Cachet event,
            # not a crashed pod. Startup-time config errors crash on purpose
            # (see _require_env above).
            try:
                healthy, latency_ms, detail = _probe_health(
                    client, cfg.health_url, cfg.request_timeout_s
                )
            except Exception as e:
                healthy, latency_ms, detail = False, None, f"probe_error {type(e).__name__}: {e}"

            try:
                push_result = _push_status(client, cfg, healthy)
            except Exception as e:
                push_result = f"push_error {type(e).__name__}: {e}"

            _log(
                f"healthy={healthy} probe={detail} "
                f"latency_ms={latency_ms if latency_ms is not None else '-'} {push_result}"
            )
            time.sleep(cfg.poll_interval_s)


if __name__ == "__main__":
    main()
