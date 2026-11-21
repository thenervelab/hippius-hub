#!/bin/bash
# Dry-run guard for the Trivy emptyDir overlay. Does not apply or delete anything.
#
# Run this only after upgrade.sh's overlay is deployed. It first dry-runs the
# release with no overlay file and refuses unless that render already has the
# HA overlay. It then renders with and without overlay-trivy-emptydir.yaml and
# refuses unless the only spec change outside of regenerated secrets is Trivy's
# volume.
set -euo pipefail
cd "$(dirname "$0")"

context=$(kubectl config current-context)
if [ "$context" != "hippius" ]; then
  echo "refusing: current context is ${context}, want hippius" >&2
  exit 1
fi

ruby render_check.rb trivy-overlay overlay-trivy-emptydir.yaml

history=$(helm history harbor -n harbor -o json)
status=$(ruby -rjson -e 'h=JSON.parse(STDIN.read); abort "empty history" if h.empty?; puts h.last["status"]' <<<"$history")
revision=$(ruby -rjson -e 'h=JSON.parse(STDIN.read); puts h.last["revision"]' <<<"$history")
echo "harbor revision ${revision} is ${status}"
if [ "$status" != "deployed" ]; then
  echo "refusing: last revision is ${status}. Do not start the Trivy upgrade." >&2
  exit 1
fi

current=$(mktemp)
before=$(mktemp)
after=$(mktemp)
trap 'rm -f "$current" "$before" "$after"' EXIT
chmod 600 "$current" "$before" "$after"

# No overlay file. This is the release as it is deployed. The HA overlay has
# to already be in it. Passing overlay-ha.yaml on both dry-runs below would
# hide a release that does not have it yet, and one upgrade would then carry
# the replica changes and the Trivy volume change together.
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values \
  --dry-run=server --hide-secret -o yaml \
  >"$current"
if ! ruby render_check.rb upgrade1 "$current"; then
  echo "refusing: the deployed release does not have the HA overlay yet. Deploy that first." >&2
  exit 1
fi

helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values -f overlay-ha.yaml \
  --dry-run=server --hide-secret -o yaml \
  >"$before"

helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values -f overlay-ha.yaml -f overlay-trivy-emptydir.yaml \
  --dry-run=server --hide-secret -o yaml \
  >"$after"

ruby render_check.rb trivy-diff "$before" "$after"
echo "dry-run diff is only Trivy's volume. This script did not delete the StatefulSet or apply. Follow README.md, Trivy emptyDir."
