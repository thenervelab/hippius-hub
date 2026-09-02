# Prod Harbor S3 flip artifacts

Do not apply until `docs/plans/2026-08-25-harbor-s3-prod-runbook.md` §0 is filled and
the §1 gate against `gateway.hippius-s3-prod` has passed.

| File | Runbook step | Use |
|---|---|---|
| `hippius_s3_contract.py`, `hippius-s3-contract-job.yaml` | §1 | Harbor-shaped S3 ops against the prod gateway. CopyObject 64 MiB below 50 MiB/s is FAIL (streaming, not alias). |
| `blob-census-job.yaml` | §0.8 | Read-only count and size of the blob tree on the JuiceFS PVC. |
| `secret-harbor-s3.yaml` | §2 | Bucket-scoped `hip_` key. Fill, never commit values. |
| `overlay-s3.yaml` | §3, §6 | The only prod helm overlay. `--reuse-values --version 1.19.0`. Carries the 5 GiB copy threshold, path-style and insecure env, node1 affinity, core/nginx replica counts. |
| `copy-blobs-job.yaml` | §4 | Online rclone copy of `blobs/**/data`, `--size-only`, re-runnable for catch-up. |
| `harbor_link_objects.py`, `link-objects-job.yaml` | §4a | Generate the `_layers` and `_manifests/revisions` link objects from Harbor's Postgres. `plan`, then `apply`, then `verify`. |
| `harbor_reachability_sweep.py`, `reachability-sweep-job.yaml` | §0.10, §7a | Ranged GET of every artifact's manifest and one layer through Harbor. Baseline before anything changes; again under the freeze. |
| `overlay-filesystem-rollback.yaml` | §8 | Back to JuiceFS. Restores the same replica counts and affinity as the flip. |

Never `helm upgrade` / `helm uninstall` in ns `harbor` except the flip in that runbook.
Never bucket `hippius-juicefs-data`. Never a copy threshold above 5 GiB.
