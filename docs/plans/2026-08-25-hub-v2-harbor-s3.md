# Hub v2 (speed) — Harbor S3 driver, same registry

**Goal:** Prod hub uploads at S3-class wall-clock (≥ 80 MiB/s on 1 GiB), without replacing Harbor or the client.

**Architecture:** Keep Harbor 2.15, keep `harbor-postgres`, keep robots. Copy existing blob files to an S3 bucket using Harbor’s own keys (`docker/registry/v2/blobs/sha256/<aa>/<digest>/data`). Switch only `persistence.imageChartStorage` from filesystem/JuiceFS to s3. Client unchanged.

**Never** `helm upgrade` / `helm uninstall` in namespace `harbor` except for that storage-driver switch, and only after the blob copy is done.

---

## Verdict (measured 2026-08-25)

POSIX-over-JuiceFS is the cap. Harbor writing whole S3 objects is not. Client is already fast. A new registry / CAS is **not** required for speed.

| Arm | Path | 1 GiB upload |
|---|---|---|
| N | client → discard-bytes registry | **769 MiB/s** (local); 187 with pipe capped at 200 |
| B | prod Harbor → JuiceFS | **47 MiB/s** v2 packs / **6.9 MiB/s** plain ([CI](https://github.com/thenervelab/hippius-hub/actions/runs/32834370054)) |
| C | staging Harbor → S3 driver → MinIO (in-cluster) | **93 MiB/s** median (89 / 103 / 93) |

C used MinIO. First hippius-s3 gate the same day (bucket `hub-test`, ns `harbor-staging`, prod untouched) was **21.3 MiB/s** — CopyObject was a streaming GET+PUT.

Later the same day, after hippius-s3 `#445` (CopyObject alias), `#448` (ingest-node hint), `#451` (cipher sizes on that hint), staging `UVICORN_WORKERS=8` / `API_DB_POOL_MAX_SIZE=8`, and Harbor `multipartcopythresholdsize=134217728`:

| Check | Result |
|---|---|
| Unique 1 GiB Harbor → hippius-s3 ×3, stock `0.7.0` | 10.28s 99.6 / 11.91s 86.0 / 8.81s 116.2 → median **99.6 MiB/s** |
| Bar ≥ 80 MiB/s | **PASS** (shipping `hippius_hub==0.7.0`, no overlay) |
| Same Harbor knobs + unreleased config-blob overlap | median **134.1 MiB/s** (optional; not required for the bar) |
| Unique 1 GiB Harbor → MinIO (ceiling) | median **159.3 MiB/s** |

Caveats: Harbor knobs are a live kubectl patch (helm rev still 4; do not helm `harbor-staging` — jobservice RWO Multi-Attach). Prod Harbor is still helm rev 4 filesystem. Do not helm-upgrade `-n harbor` until the prod overlay carries `multipartcopythresholdsize` (it now does, in `deploy/harbor-s3-prod/overlay-s3.yaml`). No hub client release is required for the 80 bar.

Harbor probe on prod: POST-init 1.59 s; more workers do not help (1.3× from 1→16). `0.7.0` hashing was real; prod wall-clock barely moved because JuiceFS ate it.

`harbor-staging` is still up for inspection. Manifests remain to recreate the experiment.

---

## Remaining work (prod cut)

Concrete runbook (team eval, not executed): `docs/plans/2026-08-25-harbor-s3-prod-runbook.md` + `deploy/harbor-s3-prod/`.

The unique-1-GiB gate is **PASS** on stock `0.7.0` (median 99.6) — but that run was
against `gateway.hippius-s3-**staging**`, not `-prod` (read-only review 2026-08-27).
hippius-s3-prod runs `api:539eec1` and is missing #451 and the reader-TTFB work, and the
`UVICORN_WORKERS=8` / `API_DB_POOL_MAX_SIZE=8` tuning is staging-only. The prod number is
**unmeasured**. Do not helm `-n harbor` until §0 of the runbook is signed.

1. Bucket (not `hippius-juicefs-data`). Fund the Harbor S3 account, and decide whether it
   shares the JuiceFS account — `can_upload` 402 is keyed on the main account.
2. Online copy of `/storage/docker/registry/v2/blobs/sha256/<aa>/<digest>/data` → that bucket, same keys.
3. **Rebuild the ~152k per-repository link objects** from Harbor's Postgres
   (`deploy/harbor-s3-prod/harbor_link_objects.py`). The blob copy alone leaves every
   artifact 404: distribution resolves `GET /v2/<repo>/blobs/<digest>` through
   `_layers/…/link`, which Postgres does not hold. Tags *are* in Postgres;
   `_manifests/tags/` is genuinely dead here.
4. Pause the daily 04:00 GC for the copy-and-flip window.
5. Freeze **pushes** a few minutes. Catch-up (blobs and links). Scale jobservice to 0
   (RWO Multi-Attach wedges the release). Helm: `imageChartStorage.type=s3` using
   `deploy/harbor-s3-prod/overlay-s3.yaml` (must keep `multipartcopythresholdsize=5368709120` — 5 GiB per PR #92, not the 128 MiB the 08-25 run used
   and the `core`/`nginx` replica counts helm would otherwise revert to 1).
6. **Sweep every artifact for reachability before unfreezing**
   (`harbor_reachability_sweep.py`) — the upload prove cannot run under `read_only`, so
   pull-prove first, then unfreeze, then measure writes.
7. Same Postgres — no `pgdump`. JuiceFS PVC stays until soak for rollback.

Not in this plan: JuiceFS block size, presigned client PUT, new registry/CAS, client
release. Replication endpoints are not the migration — Harbor has no object-storage
target, and the Harbor-to-Harbor shape would drop 351 robots and 1,310 untagged
artifacts; see D6 in the runbook.
