#!/bin/bash
# Check redis-ha.yaml. Does not apply.
# With --live, also diffs the file against the cluster and refuses if the
# RedisReplication object would change. The Sentinel diff is expected.
set -euo pipefail
cd "$(dirname "$0")"

ruby -ryaml -e '
path = ARGV.fetch(0)
docs = []
YAML.load_stream(File.read(path)) { |d| docs << d if d.is_a?(Hash) }
rep = docs.find { |d| d["kind"] == "RedisReplication" }
sen = docs.find { |d| d["kind"] == "RedisSentinel" }
abort "missing RedisReplication" unless rep
abort "missing RedisSentinel" unless sen
abort "RedisReplication clusterSize #{rep.dig("spec", "clusterSize").inspect}" unless rep.dig("spec", "clusterSize") == 3
abort "RedisReplication must not set nodeAffinity" if rep.dig("spec", "affinity", "nodeAffinity")
pdb = rep.dig("spec", "pdb") || {}
abort "RedisReplication pdb #{pdb.inspect}" unless pdb["enabled"] == true && pdb["maxUnavailable"] == 1 && !pdb.key?("minAvailable")
abort "Sentinel clusterSize #{sen.dig("spec", "clusterSize").inspect}" unless sen.dig("spec", "clusterSize") == 5
q = sen.dig("spec", "redisSentinelConfig", "quorum")
abort "Sentinel quorum #{q.inspect}, want the string \"3\"" unless q == "3"
spdb = sen.dig("spec", "pdb") || {}
abort "Sentinel pdb #{spdb.inspect}" unless spdb["enabled"] == true && spdb["maxUnavailable"] == 1 && !spdb.key?("minAvailable")
terms = sen.dig("spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
abort "Sentinel nodeAffinity missing" unless terms.is_a?(Array) && terms.length == 1
exprs = terms[0]["matchExpressions"]
abort "hostname expression missing" unless exprs.is_a?(Array) && exprs.length == 1
expr = exprs[0]
abort "hostname key #{expr.inspect}" unless expr["key"] == "kubernetes.io/hostname" && expr["operator"] == "In"
want = %w[k8s-v3-node1 k8s-v3-node2 k8s-v3-node3 k8s-v3-node5 k8s-v3-node6-cache]
got = expr["values"]
abort "hostname values #{got.inspect}" unless got == want
abort "node4 must stay out of the Sentinel pin" if got.any? { |n| n.include?("node4") }
anti = sen.dig("spec", "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution")
abort "Sentinel pod anti-affinity missing" unless anti.is_a?(Array) && !anti.empty?
warn "ok redis-ha.yaml"
' redis-ha.yaml

if [ "${1:-}" != "--live" ]; then
  echo "local checks passed. Re-run with --live to diff against the cluster. This script does not apply."
  exit 0
fi

context=$(kubectl config current-context)
if [ "$context" != "hippius" ]; then
  echo "refusing: current context is ${context}, want hippius" >&2
  exit 1
fi

live=$(mktemp)
diff_out=$(mktemp)
trap 'rm -f "$live" "$diff_out"' EXIT
kubectl get redisreplication harbor-redis-ha -n harbor -o json >"$live"
ruby -rjson -ryaml -e '
def walk(file, live, prefix, out)
  return unless file.is_a?(Hash)
  file.each do |k, v|
    path = "#{prefix}.#{k}"
    if !live.is_a?(Hash) || !live.key?(k)
      out << "#{path} missing on the live object"
    elsif v != live[k]
      if v.is_a?(Hash) && live[k].is_a?(Hash)
        walk(v, live[k], path, out)
      else
        out << path
      end
    end
  end
end
docs = []
YAML.load_stream(File.read(ARGV[0])) { |d| docs << d if d.is_a?(Hash) }
rep = docs.find { |d| d["kind"] == "RedisReplication" }
live = JSON.parse(File.read(ARGV[1]))
out = []
walk(rep["spec"], live["spec"], "spec", out)
unless out.empty?
  warn "refusing: RedisReplication spec differs from the file"
  out.each { |line| warn "  #{line}" }
  exit 1
end
warn "RedisReplication spec matches the file"
' redis-ha.yaml "$live"

# A custom KUBECTL_EXTERNAL_DIFF can print a diff with no kind in the header,
# which would hide a RedisReplication change from the grep below.
unset KUBECTL_EXTERNAL_DIFF
set +e
kubectl diff -n harbor -f redis-ha.yaml >"$diff_out"
code=$?
set -e
if [ "$code" -gt 1 ]; then
  echo "kubectl diff failed (${code}). Not applying." >&2
  exit "$code"
fi
if [ "$code" -eq 0 ]; then
  echo "cluster already matches redis-ha.yaml. Not applying."
  exit 0
fi
# A changed RedisReplication is in the diff body even when the diff header
# does not name the kind. Sentinel diffs do not contain this line.
if grep -n 'kind: RedisReplication' "$diff_out"; then
  echo "refusing: kubectl diff would change RedisReplication. Not applying." >&2
  exit 1
fi
echo "RedisReplication is unchanged. Sentinel diff:"
cat "$diff_out"
echo "Not applying. kubectl apply -n harbor -f redis-ha.yaml is the apply, after this diff looks right."
