#!/usr/bin/env ruby
# frozen_string_literal: false
# Checks rendered Harbor manifests. Ruby 2.6, stdlib only.
# Does not talk to a cluster. Prints failures, never manifest bodies.

require "yaml"

def die(msg)
  warn msg
  exit 1
end

def load_docs(path)
  raw = File.read(path)
  docs = []
  YAML.load_stream(raw) do |doc|
    docs << doc if doc.is_a?(Hash) && doc["kind"]
    if doc.is_a?(Hash) && doc["manifest"].is_a?(String)
      YAML.load_stream(doc["manifest"]) do |inner|
        docs << inner if inner.is_a?(Hash) && inner["kind"]
      end
    end
  end
  docs
end

def index_docs(docs)
  found = {}
  docs.each do |doc|
    next if doc["kind"] == "Secret"
    name = doc.dig("metadata", "name")
    found["#{doc["kind"]}/#{name}"] = doc
  end
  found
end

def pdb_fields(doc)
  spec = doc["spec"] || {}
  spec.keys - ["selector"]
end

def affinity_of(deploy)
  deploy.dig("spec", "template", "spec", "affinity")
end

def anti_of(deploy)
  affinity_of(deploy) && affinity_of(deploy)["podAntiAffinity"]
end

def required_terms(deploy)
  anti = anti_of(deploy) || {}
  anti["requiredDuringSchedulingIgnoredDuringExecution"]
end

def preferred_terms(deploy)
  anti = anti_of(deploy) || {}
  anti["preferredDuringSchedulingIgnoredDuringExecution"]
end

def list_cleared?(value)
  value.nil? || (value.is_a?(Array) && value.empty?)
end

def node_affinity_cleared?(aff)
  node = aff && aff["nodeAffinity"]
  return true if node.nil?
  return false unless node.is_a?(Hash)
  allowed = ["preferredDuringSchedulingIgnoredDuringExecution"]
  return false unless (node.keys - allowed).empty?
  list_cleared?(node["preferredDuringSchedulingIgnoredDuringExecution"])
end

def assert_required(deploy, component, label)
  terms = required_terms(deploy)
  die "#{label}: required anti-affinity missing" unless terms.is_a?(Array) && !terms.empty?
  term = terms[0]
  topo = term["topologyKey"]
  die "#{label}: topologyKey #{topo.inspect}" unless topo == "kubernetes.io/hostname"
  labels = term.dig("labelSelector", "matchLabels") || {}
  die "#{label}: app label #{labels["app"].inspect}" unless labels["app"] == "harbor"
  die "#{label}: component label #{labels["component"].inspect}" unless labels["component"] == component
end

def check_upgrade1(docs)
  idx = index_docs(docs)
  %w[Deployment/harbor-jobservice Deployment/harbor-registry Deployment/harbor-core Deployment/harbor-nginx
     PodDisruptionBudget/harbor-core PodDisruptionBudget/harbor-nginx
     PodDisruptionBudget/harbor-registry PodDisruptionBudget/harbor-jobservice
     StatefulSet/harbor-trivy].each do |key|
    die "missing #{key}" unless idx[key]
  end

  die "jobservice PVC was rendered" if idx.keys.any? { |k| k =~ %r{PersistentVolumeClaim/harbor-jobservice} }

  job = idx["Deployment/harbor-jobservice"]
  die "jobservice replicas #{job.dig("spec", "replicas").inspect}" unless job.dig("spec", "replicas") == 2
  die "jobservice strategy #{job.dig("spec", "strategy").inspect}" unless job.dig("spec", "strategy", "type") == "RollingUpdate"
  vols = job.dig("spec", "template", "spec", "volumes") || []
  logs = vols.find { |v| v["name"] == "job-logs" }
  die "job-logs volume is not an emptyDir: #{logs.inspect}" unless logs.is_a?(Hash) && logs.key?("emptyDir") && !logs.key?("persistentVolumeClaim")
  assert_required(job, "jobservice", "jobservice")
  die "jobservice still has a preferred anti-affinity" unless list_cleared?(preferred_terms(job))

  reg = idx["Deployment/harbor-registry"]
  die "registry replicas #{reg.dig("spec", "replicas").inspect}" unless reg.dig("spec", "replicas") == 2
  die "registry strategy #{reg.dig("spec", "strategy").inspect}" unless reg.dig("spec", "strategy", "type") == "RollingUpdate"
  assert_required(reg, "registry", "registry")
  die "registry preferred anti-affinity survived" unless list_cleared?(preferred_terms(reg))
  die "registry nodeAffinity survived: #{affinity_of(reg) && affinity_of(reg)["nodeAffinity"].inspect}" unless node_affinity_cleared?(affinity_of(reg))

  core = idx["Deployment/harbor-core"]
  nginx = idx["Deployment/harbor-nginx"]
  [core, nginx].each do |deploy|
    name = deploy.dig("metadata", "name")
    pref = preferred_terms(deploy)
    die "#{name}: preferred anti-affinity missing" unless pref.is_a?(Array) && !pref.empty?
    die "#{name}: required anti-affinity must stay unset" unless list_cleared?(required_terms(deploy))
    die "#{name}: replicas dropped to #{deploy.dig("spec", "replicas")}" unless deploy.dig("spec", "replicas").to_i >= 5
  end

  {"harbor-core" => "max", "harbor-nginx" => "max", "harbor-registry" => "min", "harbor-jobservice" => "min"}.each do |name, mode|
    pdb = idx["PodDisruptionBudget/#{name}"]
    fields = pdb_fields(pdb)
    if mode == "max"
      die "#{name} PDB fields #{fields.inspect}" unless fields.sort == ["maxUnavailable"]
      die "#{name} maxUnavailable #{pdb.dig("spec", "maxUnavailable").inspect}" unless pdb.dig("spec", "maxUnavailable") == 1
    else
      die "#{name} PDB fields #{fields.inspect}" unless fields.sort == ["minAvailable"]
      die "#{name} minAvailable #{pdb.dig("spec", "minAvailable").inspect}" unless pdb.dig("spec", "minAvailable") == 1
    end
  end

  trivy = idx["StatefulSet/harbor-trivy"]
  claims = trivy.dig("spec", "volumeClaimTemplates")
  die "upgrade 1 removed Trivy volumeClaimTemplates" unless claims.is_a?(Array) && !claims.empty?
  vols = trivy.dig("spec", "template", "spec", "volumes") || []
  die "upgrade 1 already mounts an emptyDir on Trivy" if vols.any? { |v| v["name"] == "data" && v.key?("emptyDir") }
end

def claim_identity(claims)
  return nil unless claims.is_a?(Array)
  claims.map do |c|
    spec = c["spec"] || {}
    {
      "name" => c.dig("metadata", "name"),
      "accessModes" => spec["accessModes"],
      "storage" => spec.dig("resources", "requests", "storage"),
      "storageClassName" => spec["storageClassName"],
    }
  end
end

def check_upgrade1_live(rendered_docs, live_docs)
  check_upgrade1(rendered_docs)
  rendered = index_docs(rendered_docs)["StatefulSet/harbor-trivy"]
  live = index_docs(live_docs)["StatefulSet/harbor-trivy"]
  die "live StatefulSet harbor-trivy not in the second file" unless live
  a = claim_identity(rendered.dig("spec", "volumeClaimTemplates"))
  b = claim_identity(live.dig("spec", "volumeClaimTemplates"))
  die "rendered Trivy volumeClaimTemplates differ from live:\n  rendered #{a.inspect}\n  live #{b.inspect}" unless a == b
end

def strip_noise(doc)
  copy = Marshal.load(Marshal.dump(doc))
  ann = copy.dig("spec", "template", "metadata", "annotations")
  if ann.is_a?(Hash)
    ann.keys.each { |k| ann.delete(k) if k.to_s.start_with?("checksum/") }
    copy["spec"]["template"]["metadata"].delete("annotations") if ann.empty?
  end
  copy
end

def diff_paths(a, b, prefix, out)
  if a.class != b.class
    out << prefix
    return
  end
  if a.is_a?(Hash)
    (a.keys | b.keys).each do |k|
      path = "#{prefix}.#{k}"
      if !a.key?(k) || !b.key?(k) || a[k] != b[k]
        if a[k].is_a?(Hash) && b[k].is_a?(Hash)
          diff_paths(a[k], b[k], path, out)
        else
          out << path
        end
      end
    end
  elsif a != b
    out << prefix
  end
end

def kind_names(docs, kind)
  docs.select { |d| d["kind"] == kind }.map { |d| d.dig("metadata", "name") }.compact.sort
end

def check_trivy_diff(before_docs, after_docs)
  %w[Secret ConfigMap].each do |kind|
    a = kind_names(before_docs, kind)
    b = kind_names(after_docs, kind)
    die "#{kind} set changed:\n  before #{a.inspect}\n  after #{b.inspect}" unless a == b
  end
  before = index_docs(before_docs)
  after = index_docs(after_docs)
  keys = (before.keys | after.keys).sort
  allowed = {
    "StatefulSet/harbor-trivy" => [
      "StatefulSet/harbor-trivy.spec.template.spec.volumes",
      "StatefulSet/harbor-trivy.spec.volumeClaimTemplates",
    ],
  }
  keys.each do |key|
    if !before[key]
      die "trivy overlay added #{key}"
    elsif !after[key]
      die "trivy overlay removed #{key}"
    else
      paths = []
      diff_paths(strip_noise(before[key]), strip_noise(after[key]), key, paths)
      next if paths.empty?
      extra = paths - (allowed[key] || [])
      die "trivy overlay changed more than the Trivy volume:\n  #{paths.join("\n  ")}" unless extra.empty?
    end
  end
  trivy = after["StatefulSet/harbor-trivy"]
  die "missing Trivy StatefulSet in the emptyDir render" unless trivy
  die "emptyDir render still has volumeClaimTemplates" if trivy.dig("spec", "volumeClaimTemplates")
  vols = trivy.dig("spec", "template", "spec", "volumes") || []
  data = vols.find { |v| v["name"] == "data" }
  die "emptyDir render has no data emptyDir: #{vols.inspect}" unless data.is_a?(Hash) && data["emptyDir"].is_a?(Hash)
end

def check_trivy_overlay(path)
  doc = YAML.load_file(path)
  die "trivy overlay is not a map" unless doc.is_a?(Hash)
  die "trivy overlay keys #{doc.keys.inspect}" unless doc.keys == ["persistence"]
  inner = doc["persistence"]
  die "persistence keys #{inner.inspect}" unless inner.is_a?(Hash) && inner.keys == ["enabled"]
  die "persistence.enabled is #{inner["enabled"].inspect}" unless inner["enabled"] == false
end

# Helm 4.3.0 reuseValues (bec5b06, pkg/action/upgrade.go) plus
# coalesceTablesFullKey (pkg/chart/common/util/coalesce.go).
# --reuse-values coalesces the new file with the previous user values, deletes a
# null whose previous value was non-nil, replaces the chart values with the old
# coalesced values, then coalesces again. A missing key is copied back. An empty
# list is not. helm template does not do this.

def deep_copy(obj)
  Marshal.load(Marshal.dump(obj))
end

def clean_nil_values(map)
  return unless map.is_a?(Hash)
  map.keys.each do |key|
    val = map[key]
    if val.nil?
      map.delete(key)
    elsif val.is_a?(Hash)
      clean_nil_values(val)
    end
  end
end

def coalesce_tables(dst, src)
  return dst if src.nil?
  return src if dst.nil?
  src_original_non_nil = {}
  src.each { |key, val| src_original_non_nil[key] = true unless val.nil? }
  dst.each { |key, val| src[key] = nil if val.nil? }
  src.each do |key, val|
    if dst.key?(key) && dst[key].nil? && src_original_non_nil[key]
      dst.delete(key)
    elsif !dst.key?(key)
      dst[key] = val
    elsif val.is_a?(Hash) && dst[key].is_a?(Hash)
      coalesce_tables(dst[key], val)
    end
  end
  dst
end

def coalesce_values_into(chart_map, user_map)
  return unless chart_map.is_a?(Hash) && user_map.is_a?(Hash)
  chart_map.each do |key, val|
    if user_map.key?(key)
      value = user_map[key]
      if value.nil?
        user_map.delete(key)
      elsif value.is_a?(Hash) && val.is_a?(Hash)
        clean_nil_values(val)
        coalesce_tables(value, val)
      end
    else
      next if val.nil?
      clean_nil_values(val) if val.is_a?(Hash)
      user_map[key] = val
    end
  end
end

def coalesce_values(chart_values, user_values)
  user = deep_copy(user_values)
  chart = deep_copy(chart_values)
  coalesce_values_into(chart, user)
  user
end

def reuse_values(chart_values, current_config, overlay)
  old_vals = coalesce_values(chart_values, current_config)
  new_vals = coalesce_tables(deep_copy(overlay), deep_copy(current_config))
  coalesce_values(old_vals, new_vals)
end

def value_at(obj, path)
  path.each do |key|
    return nil unless obj.is_a?(Hash)
    obj = obj[key]
  end
  obj
end

def harbor_chart_defaults
  {
    "persistence" => {"enabled" => true},
    "registry" => {"affinity" => {}, "podDisruptionBudget" => {"enabled" => false, "minAvailable" => 1}},
    "jobservice" => {"affinity" => {}, "podDisruptionBudget" => {"enabled" => false, "minAvailable" => 1}},
    "core" => {"affinity" => {}, "podDisruptionBudget" => {"enabled" => false, "minAvailable" => 1}},
    "nginx" => {"affinity" => {}, "podDisruptionBudget" => {"enabled" => false, "minAvailable" => 1}},
  }
end

def deployed_registry_preferred
  [{
    "weight" => 100,
    "podAffinityTerm" => {
      "topologyKey" => "kubernetes.io/hostname",
      "labelSelector" => {"matchLabels" => {"app" => "harbor", "component" => "registry"}},
    },
  }]
end

def deployed_node_pin
  {
    "preferredDuringSchedulingIgnoredDuringExecution" => [{
      "weight" => 100,
      "preference" => {
        "matchExpressions" => [{
          "key" => "kubernetes.io/hostname",
          "operator" => "In",
          "values" => ["k8s-v3-node1"],
        }],
      },
    }],
  }
end

def deployed_user_config(with_pin)
  affinity = {
    "podAntiAffinity" => {
      "preferredDuringSchedulingIgnoredDuringExecution" => deployed_registry_preferred,
    },
  }
  affinity["nodeAffinity"] = deployed_node_pin if with_pin
  {
    "registry" => {"replicas" => 2, "affinity" => affinity},
    "jobservice" => {"jobLoggers" => ["database"]},
    "persistence" => {"enabled" => true},
    "core" => {"secretName" => "harbor-token-service", "replicas" => 5},
    "nginx" => {"replicas" => 5},
  }
end

def assert_reuse_cleared(label, rendered)
  preferred = value_at(rendered, ["registry", "affinity", "podAntiAffinity", "preferredDuringSchedulingIgnoredDuringExecution"])
  die "#{label}: registry preferred came back #{preferred.inspect}" unless list_cleared?(preferred)
  required = value_at(rendered, ["registry", "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution"])
  die "#{label}: registry required rule missing" unless required.is_a?(Array) && !required.empty?
  node = value_at(rendered, ["registry", "affinity", "nodeAffinity"])
  die "#{label}: node1 pin came back #{node.inspect}" unless node_affinity_cleared?({"nodeAffinity" => node})
  job_preferred = value_at(rendered, ["jobservice", "affinity", "podAntiAffinity", "preferredDuringSchedulingIgnoredDuringExecution"])
  die "#{label}: jobservice preferred came back #{job_preferred.inspect}" unless list_cleared?(job_preferred)
  core_min = value_at(rendered, ["core", "podDisruptionBudget", "minAvailable"])
  core_max = value_at(rendered, ["core", "podDisruptionBudget", "maxUnavailable"])
  die "#{label}: core PDB minAvailable #{core_min.inspect} maxUnavailable #{core_max.inspect}" unless core_min.nil? && core_max == 1
  reg_min = value_at(rendered, ["registry", "podDisruptionBudget", "minAvailable"])
  reg_max = value_at(rendered, ["registry", "podDisruptionBudget", "maxUnavailable"])
  die "#{label}: registry PDB minAvailable #{reg_min.inspect} maxUnavailable #{reg_max.inspect}" unless reg_min == 1 && reg_max.nil?
end

def check_reuse_merge(overlay_path, rollback_path)
  # The port has to keep restoring a null, or this check can no longer see the bug.
  probe_chart = {"registry" => {"affinity" => {}}}
  probe_config = {"registry" => {"affinity" => {"podAntiAffinity" => {"preferredDuringSchedulingIgnoredDuringExecution" => [{"weight" => 100}]}}}}
  probe_overlay = {"registry" => {"affinity" => {"podAntiAffinity" => {"preferredDuringSchedulingIgnoredDuringExecution" => nil}}}}
  probe = reuse_values(probe_chart, probe_config, probe_overlay)
  probe_preferred = value_at(probe, ["registry", "affinity", "podAntiAffinity", "preferredDuringSchedulingIgnoredDuringExecution"])
  die "reuse-merge port no longer restores a nulled list" unless probe_preferred.is_a?(Array) && probe_preferred.length == 1

  overlay = YAML.load_file(overlay_path)
  die "overlay-ha is not a map" unless overlay.is_a?(Hash)
  chart = harbor_chart_defaults
  assert_reuse_cleared("pin still in user values", reuse_values(chart, deployed_user_config(true), overlay))
  assert_reuse_cleared("pin already dropped from user values", reuse_values(chart, deployed_user_config(false), overlay))
  stored = harbor_chart_defaults
  stored["registry"]["affinity"] = {
    "nodeAffinity" => deployed_node_pin,
    "podAntiAffinity" => {"preferredDuringSchedulingIgnoredDuringExecution" => deployed_registry_preferred},
  }
  assert_reuse_cleared("pin only in stored chart values", reuse_values(stored, deployed_user_config(false), overlay))

  rollback = YAML.load_file(rollback_path)
  die "filesystem rollback is not a map" unless rollback.is_a?(Hash)
  after_trivy = {"persistence" => {"enabled" => false, "imageChartStorage" => {"type" => "s3"}}}
  merged = reuse_values(harbor_chart_defaults, after_trivy, rollback)
  enabled = value_at(merged, ["persistence", "enabled"])
  storage_type = value_at(merged, ["persistence", "imageChartStorage", "type"])
  die "filesystem rollback leaves persistence.enabled #{enabled.inspect} with type #{storage_type.inspect}" unless enabled == true && storage_type == "filesystem"
end

cmd = ARGV.shift
case cmd
when "upgrade1"
  die "usage: render_check.rb upgrade1 MANIFEST" unless ARGV.length == 1
  check_upgrade1(load_docs(ARGV[0]))
when "upgrade1-live"
  die "usage: render_check.rb upgrade1-live RENDERED LIVE_STS" unless ARGV.length == 2
  check_upgrade1_live(load_docs(ARGV[0]), load_docs(ARGV[1]))
when "trivy-diff"
  die "usage: render_check.rb trivy-diff BEFORE AFTER" unless ARGV.length == 2
  check_trivy_diff(load_docs(ARGV[0]), load_docs(ARGV[1]))
when "trivy-overlay"
  die "usage: render_check.rb trivy-overlay FILE" unless ARGV.length == 1
  check_trivy_overlay(ARGV[0])
when "reuse-merge"
  die "usage: render_check.rb reuse-merge OVERLAY-HA ROLLBACK" unless ARGV.length == 2
  check_reuse_merge(ARGV[0], ARGV[1])
else
  die "unknown command #{cmd.inspect}"
end
warn "ok #{cmd}"
