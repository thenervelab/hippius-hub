# Prod Harbor S3 flip artifacts

Do not apply until `docs/plans/2026-08-25-harbor-s3-prod-runbook.md` §0 is filled
and hippius-s3 prod is on the staging image (`#445` alias CopyObject, `#448`/`#451`
fresh-part hint, workers 8 with pool sized for 5 api-local).

| File | Use |
|---|---|
| `overlay-s3.yaml` | The only prod helm overlay. `--reuse-values --version 1.19.0`. |
| `overlay-filesystem-rollback.yaml` | Rollback to JuiceFS. |
| `secret-harbor-s3.yaml` | Bucket-scoped `hip_` key. Fill, do not commit secrets. |
| `copy-blobs-job.yaml` | Online copy of blob `data` **and** `repositories/**/link` (not `--size-only` on links). |
| `hippius_s3_contract.py` | Harbor-shaped S3 ops. CopyObject 64 MiB < 50 MiB/s fails. |

Never `helm upgrade` / `helm uninstall` in ns `harbor` except the flip in that runbook.
Never bucket `hippius-juicefs-data`.
