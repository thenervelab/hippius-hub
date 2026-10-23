#!/bin/bash
# Dry-run guard for the HA overlay. Does not apply, delete, cordon, or scale.
#
# Contacts the cluster only for: current context, helm history, a server dry-run,
# and a read of StatefulSet harbor-trivy. The dry-run is written to a temp file
# and deleted. Values are not printed.
#
# Refuses unless the last Harbor revision is deployed. A failed revision makes
# the next --reuse-values drop that revision's values and put the chart Redis back.
# Refuses if this overlay would change Trivy's volumeClaimTemplates.
set -euo pipefail
cd "$(dirname "$0")"

context=$(kubectl config current-context)
if [ "$context" != "hippius" ]; then
  echo "refusing: current context is ${context}, want hippius" >&2
  exit 1
fi

history=$(helm history harbor -n harbor -o json)
status=$(ruby -rjson -e 'h=JSON.parse(STDIN.read); abort "empty history" if h.empty?; puts h.last["status"]' <<<"$history")
revision=$(ruby -rjson -e 'h=JSON.parse(STDIN.read); puts h.last["revision"]' <<<"$history")
echo "harbor revision ${revision} is ${status}"
if [ "$status" != "deployed" ]; then
  echo "refusing: last revision is ${status}. Do not --reuse-values. See README, the reuse-values trap." >&2
  exit 1
fi

rendered=$(mktemp)
live=$(mktemp)
trap 'rm -f "$rendered" "$live"' EXIT
chmod 600 "$rendered" "$live"

# stdout is the manifest stream. stderr is Helm's own errors. --hide-secret
# keeps Secret bodies out of the stream. This script never cats either file.
helm upgrade harbor harbor/harbor --version 1.19.0 -n harbor \
  --reuse-values -f overlay-ha.yaml \
  --dry-run=server --hide-secret -o yaml \
  >"$rendered"

kubectl get sts harbor-trivy -n harbor -o yaml >"$live"

ruby render_check.rb upgrade1-live "$rendered" "$live"
echo "dry-run is clean. This script did not apply it. The upgrade command is in README.md, HA overlay."
