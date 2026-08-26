# harbor-staging

Isolated Harbor 2.15, namespace `harbor-staging` only. **Never** install into
namespace `harbor`.

Two backends:

| Script | Backend | Purpose |
|---|---|---|
| `install.sh` | in-namespace MinIO | Recreate arm C (2026-08-25 median **93.4 MiB/s**) |
| `install-hippius-s3.sh` | `gateway.hippius-s3-prod` | Required gate before the prod cut. Same Harbor S3 driver, real hippius-s3. |

`install-hippius-s3.sh` runs, in order:

1. Harbor-shaped S3 contract (PUT/HEAD/GET/Range/MPU/CopyObject/List/Delete)
2. Helm Harbor 1.19.0 (`disableredirect`, `chunksize` 64 MiB, `multipartcopythresholdsize` 128 MiB, `REGISTRY_STORAGE_S3_FORCEPATHSTYLE`, `REGISTRY_STORAGE_S3_SECURE=false`). Do not helm-upgrade this release for later knobs if jobservice is RWO Multi-Attach — kubectl-patch `harbor-staging-registry` `config.yml` + env instead. Never helm `-n harbor`.
3. Unique 1 GiB `hippius-hub` 0.7.0 upload × 3 (`arm-c-job.yaml`). Stock wheel, no overlay. 2026-08-25 median **99.6 MiB/s** on hippius-s3. Prod overlay uses `multipartcopythresholdsize=5368709120` (S3 CopyObject ceiling).

Success bar: contract all PASS **including CopyObject 64 MiB ≥ 50 MiB/s** (alias, not streaming GET+PUT), bench median **≥ 80 MiB/s**.

Prod flip: `docs/plans/2026-08-25-harbor-s3-prod-runbook.md`. Do not helm `-n harbor` from this directory.

## Hard rules

- Namespace is `harbor-staging`. Helm release is `harbor-staging`.
- **Never** `helm upgrade` / `helm uninstall` in namespace `harbor` (production).
- **Never** bucket `hippius-juicefs-data` (or any `*juicefs*` name).
- **Never** NodePort 30002/30003 (prod Harbor). Staging is ClusterIP only.

## hippius-s3 gate

```bash
export KUBECONFIG=~/Hippius-Storage/Configs/k8s/hippius.yaml
export HARBOR_S3_BUCKET=harbor-registry-cas   # or a probe bucket
export HARBOR_S3_ACCESS_KEY=hip_...
export HARBOR_S3_SECRET_KEY=...
./deploy/harbor-staging/install-hippius-s3.sh
```

Tear down: `./deploy/harbor-staging/uninstall.sh`

## MinIO (historical arm C)

```bash
./deploy/harbor-staging/install.sh
```
