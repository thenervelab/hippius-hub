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
| `redis-ha.yaml` | Redis HA cutover | Operator-managed Redis for Harbor: 1 master + 2 replicas and 3 Sentinels, each pinned to a different node. |
| `overlay-redis-ha.yaml` | Redis HA cutover | Points Harbor at the Sentinels. Removes the chart's single-pod Redis. |

Never `helm upgrade` / `helm uninstall` in ns `harbor` except the flip in that runbook
and the Redis HA cutover below.
Never bucket `hippius-juicefs-data`. Never a copy threshold above 5 GiB.

Both overlays use `--reuse-values`, so they compose: a later S3 flip or rollback keeps
Redis external, and the Redis cutover keeps the S3 storage settings.

## Redis HA cutover

Why: on 2026-10-22 node4 went NotReady with `harbor-redis-0` on it. The pod sat
`Terminating`, every `harbor-core` crash-looped on `failed to initialize cache`, and
the registry served 502 until the pod was force-deleted and its VolumeAttachment
removed by hand. `redis-ha.yaml` has the details.

Keys written to the old Redis between step 2 and the end of step 3 are lost: cache
entries and job stats. Cut over when the Job Service dashboard shows no running jobs.

```sh
# 1. Create the new Redis. Wait for a master and three pods on three nodes.
kubectl apply -n harbor -f redis-ha.yaml
kubectl -n harbor get redisreplication harbor-redis-ha -o jsonpath='{.status.masterNode}'
kubectl -n harbor get pods -l 'app in (harbor-redis-ha,harbor-redis-ha-sentinel)' -o wide
kubectl -n harbor exec harbor-redis-ha-sentinel-0 -- \
  redis-cli -p 26379 SENTINEL get-master-addr-by-name myMaster

# 2. Copy every key from the old Redis (db0 core, db1 jobservice, db2 registry,
#    db3 harbor-warmer, db5 trivy). COPY leaves the source intact for rollback.
kubectl -n harbor exec harbor-redis-0 -- sh -c 'for db in 0 1 2 3 5; do
  redis-cli -n $db --scan | xargs -r -n 200 \
    redis-cli -n $db MIGRATE harbor-redis-ha-master 6379 "" $db 5000 COPY REPLACE KEYS
  echo "db$db old=$(redis-cli -n $db dbsize) new=$(redis-cli -h harbor-redis-ha-master -n $db dbsize)"
done'

# 3. Switch Harbor. Read the dry run's redis URLs before the real one.
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor --reuse-values \
  -f overlay-redis-ha.yaml --dry-run | grep -E '_REDIS_URL|redis_url|sentinelMasterSet'
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor --reuse-values \
  -f overlay-redis-ha.yaml

# 4. Point harbor-warmer at the master Service. Its REDIS_URL is in
#    thenervelab/harbor-warmer k8s/deployment.yaml:
#    redis://harbor-redis-ha-master:6379/3
```

5. Verify: `curl -s -o /dev/null -w '%{http_code}' https://registry.hippius.com/v2/`
   is 401; `harbor-core`, `harbor-jobservice` and `harbor-trivy` logs show no Redis
   errors; a `docker pull` through the registry works.
6. Failover drill: delete the pod named by `.status.masterNode`. Within about 15s
   Sentinel names a new master, `/v2/` stays 401, and `harbor-core` does not restart.
   `kubectl -n harbor get endpoints harbor-redis-ha-master` must now list the new
   master's IP: harbor-warmer reaches Redis only through that Service. If it still
   lists the old pod, the warmer is writing to a replica; stop and fix before step 7.
7. Once stable: delete PVC `data-harbor-redis-0`.

Rollback before step 7: `helm upgrade ... --reuse-values --set redis.type=internal`
brings the chart's Redis back on its retained PVC, which still has the keys as of
step 2. Point harbor-warmer back at `redis://harbor-redis:6379/3`.
