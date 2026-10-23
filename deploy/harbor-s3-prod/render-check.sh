#!/bin/bash
# Render the Harbor chart locally and check the HA overlay.
# Uses helm template only. Does not contact the cluster.
set -euo pipefail
cd "$(dirname "$0")"

if grep -E -n 'htpasswd|BEGIN |PRIVATE KEY|^[[:space:]]*password:' overlay-ha.yaml overlay-trivy-emptydir.yaml; then
  echo "refusing: secret-looking line in an overlay" >&2
  exit 1
fi

ruby render_check.rb trivy-overlay overlay-trivy-emptydir.yaml

before=$(mktemp)
after=$(mktemp)
trap 'rm -f "$before" "$after"' EXIT
chmod 600 "$before" "$after"

# database.type and expose.type match prod. They are release values, not keys in
# these overlays. Chart defaults are internal Postgres and ingress, which hide
# the nginx Deployment this overlay budgets.
common=(
  -f overlay-s3.yaml -f overlay-redis-ha.yaml -f overlay-ha.yaml
  --set database.type=external
  --set expose.type=nodePort
  --set expose.tls.auto.commonName=registry.hippius.com
)
helm template harbor harbor/harbor --version 1.19.0 "${common[@]}" >"$before"
helm template harbor harbor/harbor --version 1.19.0 "${common[@]}" -f overlay-trivy-emptydir.yaml >"$after"

ruby render_check.rb upgrade1 "$before"
ruby render_check.rb trivy-diff "$before" "$after"
echo "render-check: overlay renders the spread, and the Trivy file changes only Trivy's volume"
