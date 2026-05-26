import argparse
import getpass
import json
import os
import shutil
import subprocess
import sys
from typing import Any

from . import __version__
from .auth import login
from .file_download import hippius_hub_download
from . import console
from .console import ConsoleError


def _fmt_bytes(n) -> str:
    if n is None:
        return "—"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} EB"


def _fmt_params(n) -> str:
    if not n:
        return "—"
    n = int(n)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    return f"{n:,}"


def _print_json(obj: Any):
    print(json.dumps(obj, indent=2, sort_keys=True, default=str))


def _format_download_error(e: Exception) -> tuple[str, int]:
    """Map a download/upload exception to (message, exit_code) for the CLI.

    Distinct exit codes let CI scripts and wrappers branch on the failure
    mode (retry on concurrent-write, prompt for auth on gated/disabled,
    abort on bad-revision typo) instead of swallowing every error as
    generic exit 1. Imports are kept local so the CLI startup path doesn't
    pull in huggingface_hub.errors when no failure has occurred.

    Exit codes:
        1  generic failure (unknown exception)
        2  reserved — argparse usage error (set elsewhere in cli.py)
        10 file not found in repo (EntryNotFoundError)
        11 repository not found (RepositoryNotFoundError)
        12 revision not found (RevisionNotFoundError)
        13 local cache miss (LocalEntryNotFoundError)
        14 access denied (GatedRepoError, DisabledRepoError)
        15 concurrent manifest write (ConcurrentManifestUpdateError)
        16 registry HTTP error (HfHubHTTPError)

    Codes start at 10 (not 2) to avoid colliding with bash's reserved
    exit code 2 ("misuse of shell builtin") and argparse's default for
    usage errors — both of which the CLI already produces at the parser
    layer. A typed routing code that overlapped with those would be
    indistinguishable from a bad-argument failure to a shell wrapper.

    Ordering invariant: HF's typed exception hierarchy has three subclass
    relationships that matter here — LocalEntryNotFoundError <: Entry-
    NotFoundError; GatedRepoError <: RepositoryNotFoundError <:
    HfHubHTTPError, while DisabledRepoError <: HfHubHTTPError directly
    (NOT via RepositoryNotFoundError — that asymmetry is pinned by
    test_disabled_repo_is_not_subclass_of_repository_not_found); and
    ConcurrentManifestUpdateError <: HfHubHTTPError. The isinstance
    checks MUST run subclass-before-parent or a cache miss would be
    reported as a missing-in-repo file (10), a gated repo as 'not found'
    (11) instead of 'access denied' (14), and a 412 manifest collision
    as a generic HTTP error (16) instead of the actionable concurrent-
    write code (15).
    """
    from .errors import (
        ConcurrentManifestUpdateError,
        DisabledRepoError,
        EntryNotFoundError,
        GatedRepoError,
        HfHubHTTPError,
        LocalEntryNotFoundError,
        RepositoryNotFoundError,
        RevisionNotFoundError,
    )
    # Subclass-first: LocalEntryNotFoundError inherits from Entry-
    # NotFoundError. Checking the parent first would route every cache
    # miss to code 10 (file-not-found-in-repo) — wrong actionable hint.
    if isinstance(e, LocalEntryNotFoundError):
        return (f"❌ Local cache miss: {e}", 13)
    if isinstance(e, EntryNotFoundError):
        return (f"❌ File not found in repo: {e}", 10)
    # Subclass-first: GatedRepoError subclasses RepositoryNotFoundError
    # (auth-gated repos return 403, which HF models as a kind of "you
    # can't see this repo"). DisabledRepoError, despite being grouped
    # with Gated here for the same user-facing message, does NOT inherit
    # from RepositoryNotFoundError — but routing it first is still
    # required so it doesn't fall through to the generic HfHubHTTPError
    # arm below.
    if isinstance(e, (GatedRepoError, DisabledRepoError)):
        return (f"❌ Access denied: {e}", 14)
    if isinstance(e, RepositoryNotFoundError):
        return (f"❌ Repository not found: {e}", 11)
    if isinstance(e, RevisionNotFoundError):
        return (f"❌ Revision not found: {e}", 12)
    # ConcurrentManifestUpdateError subclasses HfHubHTTPError; it must be
    # tested first so the actionable retry/serialize guidance survives.
    if isinstance(e, ConcurrentManifestUpdateError):
        return (
            f"❌ Concurrent write detected: {e}. Another writer pushed "
            f"to the same revision. Retry or serialize uploads externally.",
            15,
        )
    if isinstance(e, HfHubHTTPError):
        return (f"❌ Registry HTTP error: {e}", 16)
    return (f"❌ Operation failed: {e}", 1)


# ----- registry sub-commands -----

def cmd_registry_plans(_args):
    plans = console.list_plans()
    for p in plans:
        print(f"\n{p['name']} — {p['price_credits']:g} credits/mo")
        print(f"  private: {p['private_storage_gb']} GB")
        print(f"  public:  {p['public_storage_gb']} GB")
        print(f"  max projects: {p['max_projects']}")
        for f in p.get("features", []):
            print(f"    • {f}")


def cmd_registry_check(args):
    res = console.check_namespace(args.name)
    if res.get("available"):
        print(f"✅ {args.name} is available")
    else:
        print(f"❌ {res.get('message') or 'taken'}")
        sys.exit(2)


def _maybe_docker_login(host: str, user: str, secret: str, *, auto: bool):
    # Always persist creds to hippius-hub's own cache (~/.cache/hippius/hub/token).
    # The robot secret is shown only once by the API; if we don't save it here the
    # user has to rerun `hippius-hub login --username --password` by hand before
    # the next upload/download, which is a footgun.
    login(username=user, password=secret)

    if not auto:
        print("\nTo also enable `docker push`/`pull`, run:")
        print(f"  docker login {host} -u '{user}' -p '<secret-printed-above>'")
        return
    if not shutil.which("docker"):
        print("\nℹ️  docker CLI not found in PATH — skipping `docker login`.")
        print("    hippius-hub's own auth is set up; only `docker push`/`pull` will be unavailable.")
        print(f"    Run manually if you install docker later: docker login {host} -u '{user}' -p '...'")
        return
    p = subprocess.run(
        ["docker", "login", host, "-u", user, "--password-stdin"],
        input=secret.encode(), capture_output=True,
    )
    if p.returncode == 0:
        print(f"✅ docker login {host} OK")
    else:
        print(f"⚠️  docker login failed: {p.stderr.decode().strip()}")


def cmd_registry_provision(args):
    try:
        res = console.provision(args.namespace)
    except ConsoleError as e:
        if e.status_code == 402:
            print(f"❌ Not enough credits: {e.body}")
        elif e.status_code == 409:
            print(f"❌ {e.body}")
        elif e.status_code == 202:
            print(f"⏳ Project is still being created. Poll `hippius-hub registry status`.")
        else:
            print(f"❌ {e}")
        sys.exit(1)

    if res.get("idempotent"):
        print(f"ℹ️  Project '{res['project_name']}' already exists ({res.get('plan_name')} plan).")
        print("    Run `hippius-hub registry rotate-token` to get fresh docker credentials.")
        return

    if res.get("pending"):
        print(f"⏳ {res.get('message') or 'Provisioning started in the background.'}")
        if res.get("details"):
            print(f"   {res['details']}")
        print("    Poll `hippius-hub registry status`.")
        return

    # A stuck row that the server self-recovered on this POST. The robot
    # secret stays encrypted at rest, so it's not in the response — we
    # tell the user to rotate to get a fresh one.
    if res.get("recovered"):
        print(f"✅ Recovered '{res['project_name']}' — now {res.get('status')}.")
        print(f"   {res.get('message') or 'Run `hippius-hub registry rotate-token` to get docker credentials.'}")
        return

    # Fresh provision: creds are returned exactly once.
    print(f"✅ Created '{res['project_name']}' on the {res['plan_name']} plan.")
    print(f"   Quota: {_fmt_bytes(res.get('storage_quota_bytes'))}")
    print()
    print("Docker credentials (save these — the secret is only shown ONCE):")
    print(f"  Login:    {res['robot_login']}")
    print(f"  Secret:   {res['robot_secret']}")
    print(f"  Registry: {res['registry_url']}")
    host = res['registry_url'].replace("https://", "").replace("http://", "")
    _maybe_docker_login(host, res['robot_login'], res['robot_secret'], auto=args.docker_login)


def cmd_registry_status(_args):
    res = console.provision_status()
    projects = res.get("projects") or []
    if not projects:
        print("No projects. Run `hippius-hub registry provision <namespace>`.")
        return
    for p in projects:
        print(f"  {p['project_name']:30} status={p['status']:12} plan={p.get('plan_name')}")


def cmd_registry_me(_args):
    res = console.me()
    print(f"Project:   {res['project_name']}")
    print(f"Plan:      {res.get('plan_name')}")
    print(f"Status:    {res['status']}")
    print(f"Public:    {res['public']}")
    print(f"Quota:     {_fmt_bytes(res.get('storage_quota_bytes'))}")
    print(f"Registry:  {res['registry_url']}")
    print(f"Login:     {res.get('robot_login') or '—'}")


def cmd_registry_rotate(args):
    res = console.rotate_robot()
    print(f"✅ New docker secret issued.")
    print(f"  Login:  {res['robot_login']}")
    print(f"  Secret: {res['robot_secret']}")
    me = console.me()
    host = me['registry_url'].replace("https://", "").replace("http://", "")
    _maybe_docker_login(host, res['robot_login'], res['robot_secret'], auto=args.docker_login)


def cmd_registry_repos(args):
    res = console.list_repositories(page=args.page, page_size=args.page_size)
    if not res:
        print("No repositories.")
        return
    for r in res:
        full = r.get("name", "")
        repo = full.split("/", 1)[1] if "/" in full else full
        print(f"  {repo:40} artifacts={r.get('artifact_count', 0):4} "
              f"pulls={r.get('pull_count', 0):6} updated={r.get('update_time', '—')}")


def cmd_registry_artifacts(args):
    if "/" not in args.repo:
        print(f"❌ Repo must be '<project>/<repo>', got '{args.repo}'.")
        print(f"   Example: hippius-hub registry artifacts myorg/my-models")
        print(f"   (run `hippius-hub registry me` to see your project name)")
        sys.exit(2)
    res = console.list_artifacts(args.repo, page=args.page, page_size=args.page_size)
    if not res:
        print("No artifacts.")
        return
    for a in res:
        tags = ", ".join(t for t in (a.get("all_tags") or []) if t) \
            or a.get("primary_tag") or "-"
        size = a.get("total_size_bytes")
        digest = a.get("digest", "")
        print(f"  {digest[:24]:24} tags={tags:20} size={_fmt_bytes(size):>10}  "
              f"indexed={a.get('indexed_at', '—')}")


def cmd_registry_usage(_args):
    res = console.usage()
    live = res.get("live", {}) or {}
    print(f"Storage used:  {_fmt_bytes(live.get('storage_used_bytes'))}")
    print(f"Storage quota: {_fmt_bytes(live.get('storage_quota_bytes'))}")
    print(f"Artifacts:     {live.get('artifact_count') if live.get('artifact_count') is not None else '—'}")
    hist = res.get("history") or []
    if hist:
        print("\nLast 7 snapshots:")
        for s in hist[:7]:
            print(f"  {s['snapshot_at']}  storage={_fmt_bytes(s['storage_used_bytes'])}  "
                  f"repos={s['repo_count']}  artifacts={s['artifact_count']}")


def cmd_registry_publicity(args):
    new = args.value.lower() == "public"
    res = console.toggle_publicity(public=new)
    print(f"✅ Project is now {'public' if res['public'] else 'private'}")
    print(f"   Quota: {_fmt_bytes(res.get('storage_quota_bytes'))}")


def _resolve_plan_id(plan_arg: str) -> int:
    """Accept either an integer plan_id or a plan name (case-insensitive)."""
    if plan_arg.isdigit():
        return int(plan_arg)
    plans = console.list_plans() or []
    for p in plans:
        if p.get("name", "").lower() == plan_arg.lower():
            return p["id"]
    avail = ", ".join(p.get("name", "?") for p in plans) or "(no plans)"
    raise SystemExit(f"❌ Unknown plan: {plan_arg!r}. Available: {avail}")


def cmd_registry_subscribe(args):
    plan_id = _resolve_plan_id(args.plan)
    res = console.subscribe(plan_id, pay_upfront=args.pay_upfront)
    print(f"✅ Subscription submitted for plan '{res.get('plan_name', plan_id)}'")
    print(f"   owner:          {res.get('owner') or '—'}")
    print(f"   extrinsic_hash: {res.get('extrinsic_hash')}")
    print(f"   block_hash:     {res.get('block_hash')}")
    print()
    print("Chain state will reflect in the next sync (~3 min).")
    print("Watch with: hippius-hub registry subscriptions")


def cmd_registry_subscriptions(_args):
    rows = console.list_subscriptions() or []
    if not rows:
        print("No subscriptions yet. Run `hippius-hub registry subscribe <plan>`.")
        return
    for r in rows:
        mark = "✅" if r.get("active") else "❌"
        nxt = r.get("next_charge_unix_day")
        print(f"  {mark} #{r['subscription_id']:<6} {r['plan_name']:20} "
              f"paid/mo={r.get('paid_per_month', 0)}  "
              f"next_unix_day={nxt or '—'}  synced={r.get('synced_at')}")
        if not r.get("active") and r.get("cancelled_at"):
            print(f"     (cancelled at {r['cancelled_at']} — 30-day grace running)")


def cmd_registry_unsubscribe(args):
    res = console.cancel_subscription(args.subscription_id)
    print(f"✅ Cancel submitted for subscription #{res.get('subscription_id', args.subscription_id)}")
    print(f"   extrinsic_hash: {res.get('extrinsic_hash')}")
    print(f"   block_hash:     {res.get('block_hash')}")
    print()
    print("Grace period: 30 days. Robot/docker login stops working on the")
    print("next sync (~3 min). Artifacts + project survive until grace expires.")
    print("Re-subscribe within 30 days to keep everything.")


# ----- registry keys sub-commands -----

_ROLE_CHOICES = ["read", "push", "push-delete", "admin"]


def _print_key_row(k: dict) -> None:
    exp = k.get("expires_at") or "never"
    last_used = k.get("last_used_at") or "—"
    print(f"  #{k['id']:<5} {k['name']:20} role={k['role']:12} "
          f"expires={exp:<25} last_used={last_used}")
    print(f"        login={k['login']}")


def cmd_registry_keys_list(_args):
    rows = console.list_keys() or []
    if not rows:
        print("No keys yet. Create one with: hippius-hub registry keys create <name> --role read")
        return
    print(f"{len(rows)} key(s):")
    for r in rows:
        _print_key_row(r)


def cmd_registry_keys_create(args):
    res = console.create_key(args.name, args.role, expires_days=args.expires_days)
    print(f"✅ Key '{res['name']}' created — role={res['role']}")
    print(f"  Login:  {res['login']}")
    print(f"  Secret: {res['secret']}")
    print("  ⚠ Save the secret now — it won't be shown again. Rotate to recover.")
    print()
    print(f"  docker login: {res['docker_login_cmd']}")
    if args.docker_login:
        host = res["docker_login_cmd"].split(" ")[2]
        _maybe_docker_login(host, res["login"], res["secret"], auto=True)


def cmd_registry_keys_show(args):
    res = console.show_key(args.key_id)
    _print_key_row(res)


def cmd_registry_keys_rotate(args):
    res = console.rotate_key(args.key_id)
    print(f"✅ Key '{res['name']}' rotated")
    print(f"  Login:  {res['login']}")
    print(f"  Secret: {res['secret']}")
    print("  ⚠ Save the secret now — it won't be shown again.")


def cmd_registry_keys_revoke(args):
    console.revoke_key(args.key_id)
    print(f"✅ Key #{args.key_id} revoked. Its docker login will stop working immediately.")


# ----- models sub-commands -----

def cmd_models_list(args):
    res = console.models_list(
        fmt=args.format, architecture=args.arch, quantization=args.quant,
        min_params=args.min_params or None, max_params=args.max_params or None,
        q=args.q, mine=args.mine, page=args.page, page_size=args.page_size,
    )
    if args.json:
        _print_json(res)
        return
    total = res.get("total", 0)
    print(f"Found {total} model(s):")
    for m in res.get("results", []):
        own = " [mine]" if m.get("is_mine") else (" [public]" if m.get("is_public") else "")
        print(f"  {m['project']}/{m['repo']:30} {m.get('format'):12} "
              f"arch={m.get('architecture') or '—':10} params={_fmt_params(m.get('parameter_count')):>7}  "
              f"quant={m.get('quantization') or '—':6}  size={_fmt_bytes(m.get('total_size_bytes')):>9}{own}")


def cmd_models_show(args):
    parts = args.repo_id.split("/", 1)
    if len(parts) != 2:
        print("❌ repo_id must be <project>/<repo>")
        sys.exit(1)
    project, repo = parts
    if args.reference:
        res = console.model_detail(project, repo, args.reference)
        if args.json:
            _print_json(res)
            return
        print(f"\n{res['project']}/{res['repo']}  tag={res.get('primary_tag') or '—'}")
        print(f"  Format:   {res.get('format')}")
        print(f"  Arch:     {res.get('architecture') or '—'}")
        print(f"  Params:   {_fmt_params(res.get('parameter_count'))}")
        print(f"  Quant:    {res.get('quantization') or '—'}")
        print(f"  Size:     {_fmt_bytes(res.get('total_size_bytes'))}")
        print(f"  Digest:   {res.get('digest')}")
        print(f"  Files:")
        for f in res.get("files", []):
            print(f"    {f['filename']:40} {f['format']:12} {_fmt_bytes(f['size_bytes'])}")
        print(f"\n  pull: {res.get('pull_command')}")
    else:
        res = console.model_repo(project, repo)
        if args.json:
            _print_json(res)
            return
        print(f"\n{project}/{repo} — {res.get('total', 0)} version(s):")
        for a in res.get("artifacts", []):
            print(f"  tag={a.get('primary_tag') or '—':10} "
                  f"params={_fmt_params(a.get('parameter_count')):>7}  "
                  f"size={_fmt_bytes(a.get('total_size_bytes')):>9}  "
                  f"indexed={a.get('indexed_at')}")


def cmd_models_formats(_args):
    res = console.models_formats()
    print("Available filters:")
    print(f"  formats:        {', '.join(res.get('formats') or [])}")
    print(f"  architectures:  {', '.join(res.get('architectures') or [])}")
    print(f"  quantizations:  {', '.join(res.get('quantizations') or [])}")


# ----- top-level -----

def main():
    parser = argparse.ArgumentParser(
        prog="hippius-hub",
        description=f"Hippius Hub CLI v{__version__} — registry namespaces, AI model index, "
                    f"and fast model downloads.",
    )
    parser.add_argument("-V", "--version", action="version",
                        version=f"hippius-hub {__version__}")
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # Download (existing)
    d = sub.add_parser("download", help="Download a file from a repository")
    d.add_argument("repo_id"); d.add_argument("filename")
    d.add_argument("--revision", default="main")
    d.add_argument("--chunk-size", type=int, default=None,
                   help="Chunk size in bytes (defaults to the library default)")
    d.add_argument("--cache-dir", default=None)
    d.add_argument("--verify-hash", action="store_true")

    # Upload (existing)
    u = sub.add_parser("upload", help="Upload a file or folder to a repository")
    u.add_argument("repo_id"); u.add_argument("local_path")
    u.add_argument("--revision", default="main")

    # Login: accepts EITHER docker registry creds OR a Hippius API token.
    l = sub.add_parser("login", help="Save credentials. Use --hippius-token for the console API, --username/--password for the docker registry, or --token for either.")
    l.add_argument("--username")
    l.add_argument("--password")
    l.add_argument("--token", help="Legacy docker-registry token")
    l.add_argument("--hippius-token", help="API token from console.hippius.com")

    # Registry sub-tree (wraps api.hippius.com).
    reg = sub.add_parser("registry", help="Manage your registry namespace, repos, and quota")
    regsub = reg.add_subparsers(dest="registry_cmd")

    regsub.add_parser("plans", help="List available pricing plans").set_defaults(func=cmd_registry_plans)

    rc = regsub.add_parser("check", help="Check if a namespace is available")
    rc.add_argument("name"); rc.set_defaults(func=cmd_registry_check)

    rp = regsub.add_parser("provision", help="Provision your registry namespace")
    rp.add_argument("namespace")
    rp.add_argument("--docker-login", action="store_true",
                    help="Also run `docker login` so `docker push`/`pull` works "
                         "(hippius-hub's own auth is always persisted regardless)")
    rp.set_defaults(func=cmd_registry_provision)

    regsub.add_parser("status", help="Polling status for in-flight provisioning"
                     ).set_defaults(func=cmd_registry_status)
    regsub.add_parser("me", help="Show my active project").set_defaults(func=cmd_registry_me)

    rr = regsub.add_parser("rotate-token", help="Issue a new docker secret")
    rr.add_argument("--docker-login", action="store_true",
                    help="Also run `docker login` so `docker push`/`pull` works "
                         "(hippius-hub's own auth is always re-persisted regardless)")
    rr.set_defaults(func=cmd_registry_rotate)

    rrepos = regsub.add_parser("repos", help="List my repositories")
    rrepos.add_argument("--page", type=int, default=1)
    rrepos.add_argument("--page-size", type=int, default=50)
    rrepos.set_defaults(func=cmd_registry_repos)

    rart = regsub.add_parser("artifacts", help="List artifacts in one repo")
    rart.add_argument("repo", metavar="<project>/<repo>",
                      help="Two-segment repo path, e.g. myorg/my-models")
    rart.add_argument("--page", type=int, default=1)
    rart.add_argument("--page-size", type=int, default=50)
    rart.set_defaults(func=cmd_registry_artifacts)

    regsub.add_parser("usage", help="Show storage usage + 7d history"
                     ).set_defaults(func=cmd_registry_usage)

    rpub = regsub.add_parser("publicity", help="Toggle public/private (quota changes)")
    rpub.add_argument("value", choices=["public", "private"])
    rpub.set_defaults(func=cmd_registry_publicity)

    rs = regsub.add_parser("subscribe",
                           help="Subscribe to a plan on-chain (debits your own credits)")
    rs.add_argument("plan", help="Plan name (e.g. 'Free', 'Builder', 'Pro') or numeric plan_id")
    rs.add_argument("--pay-upfront", type=int, default=None,
                    help="Pay upfront for N months (1-24). Default = monthly.")
    rs.set_defaults(func=cmd_registry_subscribe)

    regsub.add_parser("subscriptions",
                      help="List my current subscriptions (synced from chain every ~3 min)"
                      ).set_defaults(func=cmd_registry_subscriptions)

    ru = regsub.add_parser("unsubscribe",
                           help="Cancel a subscription by its on-chain ID. 30-day grace.")
    ru.add_argument("subscription_id", type=int, help="On-chain SubscriptionId (u32)")
    ru.set_defaults(func=cmd_registry_unsubscribe)

    # Per-project API keys (role-scoped Harbor robots). The bootstrap admin
    # robot from `registry provision` is separate; `rotate-token` still
    # rotates that one. These commands manage the EXTRA keys.
    rkeys = regsub.add_parser("keys", help="Per-project API keys (CI, read-only, etc.)")
    rkeyssub = rkeys.add_subparsers(dest="keys_cmd")

    rkeyssub.add_parser("list", help="List keys for the active project"
                       ).set_defaults(func=cmd_registry_keys_list)

    rkc = rkeyssub.add_parser("create", help="Create a new key (returns secret ONCE)")
    rkc.add_argument("name", help="Short slug; becomes `robot$<project>+<name>` on the registry")
    rkc.add_argument("--role", required=True, choices=_ROLE_CHOICES,
                     help="ACL preset: read = pull/list; push = read + push/create; "
                          "push-delete = push + delete; admin = full project")
    rkc.add_argument("--expires-days", type=int, default=None,
                     help="Days until the key expires. Omit for no expiry.")
    rkc.add_argument("--docker-login", action="store_true",
                     help="Also run `docker login` so `docker push`/`pull` works")
    rkc.set_defaults(func=cmd_registry_keys_create)

    rks = rkeyssub.add_parser("show", help="Show one key (no secret)")
    rks.add_argument("key_id", type=int)
    rks.set_defaults(func=cmd_registry_keys_show)

    rkr = rkeyssub.add_parser("rotate", help="Rotate the secret for one key")
    rkr.add_argument("key_id", type=int)
    rkr.set_defaults(func=cmd_registry_keys_rotate)

    rkrv = rkeyssub.add_parser("revoke", help="Delete a key (irreversible)")
    rkrv.add_argument("key_id", type=int)
    rkrv.set_defaults(func=cmd_registry_keys_revoke)

    # Models sub-tree
    mod = sub.add_parser("models", help="Search and inspect the AI model index")
    modsub = mod.add_subparsers(dest="models_cmd")

    ml = modsub.add_parser("list", help="Search the model index")
    ml.add_argument("--format")
    ml.add_argument("--arch", "--architecture", dest="arch")
    ml.add_argument("--quant", "--quantization", dest="quant")
    ml.add_argument("--min-params", type=int, default=0)
    ml.add_argument("--max-params", type=int, default=0)
    ml.add_argument("-q", "--query", dest="q")
    ml.add_argument("--mine", action="store_true", help="Restrict to your own models")
    ml.add_argument("--page", type=int, default=1)
    ml.add_argument("--page-size", type=int, default=25)
    ml.add_argument("--json", action="store_true")
    ml.set_defaults(func=cmd_models_list)

    ms = modsub.add_parser("show", help="Show a model (all versions, or a specific ref)")
    ms.add_argument("repo_id", help="<project>/<repo>")
    ms.add_argument("reference", nargs="?", default=None,
                    help="tag or sha256 digest; omit to list all versions")
    ms.add_argument("--json", action="store_true")
    ms.set_defaults(func=cmd_models_show)

    modsub.add_parser("formats", help="Show available filter values"
                     ).set_defaults(func=cmd_models_formats)

    args = parser.parse_args()

    if args.command == "download":
        print(f"Downloading {args.filename} from {args.repo_id} (revision: {args.revision})...")
        if args.chunk_size is not None:
            os.environ["HIPPIUS_CHUNK_SIZE"] = str(args.chunk_size)
        if args.verify_hash:
            os.environ["HIPPIUS_VERIFY_HASH"] = "1"
        try:
            path = hippius_hub_download(
                repo_id=args.repo_id, filename=args.filename, revision=args.revision,
                cache_dir=args.cache_dir,
            )
            print(f"✅ File downloaded to: {path}")
        except Exception as e:
            msg, code = _format_download_error(e)
            print(msg)
            sys.exit(code)
        return

    if args.command == "upload":
        from .file_upload import hippius_hub_upload
        try:
            hippius_hub_upload(repo_id=args.repo_id, local_path=args.local_path, revision=args.revision)
        except Exception as e:
            msg, code = _format_download_error(e)
            print(msg)
            sys.exit(code)
        return

    if args.command == "login":
        if args.hippius_token:
            console.save_api_token(args.hippius_token)
            print(f"✅ Hippius API token saved.")
            return
        username = args.username
        password = args.password
        token = args.token
        if not token and not (username and password):
            print("Get your API token from https://console.hippius.com, then run:")
            print("  hippius-hub login --hippius-token <token>")
            print()
            print("Alternatively, log in with docker registry credentials:")
            username = input("Username: ").strip()
            if username:
                # Do NOT strip(): a password that legitimately ends in
                # whitespace would silently lose those bytes and produce
                # a misleading 401 with no diagnostic clue.
                password = getpass.getpass("Password or CLI secret: ")
            else:
                token = getpass.getpass("Token: ")
        try:
            login(username=username, password=password, token=token)
        except ValueError as e:
            print(f"❌ Login failed: {e}")
            sys.exit(1)
        return

    if args.command in ("registry", "models"):
        if not hasattr(args, "func"):
            parser.print_help()
            sys.exit(1)
        try:
            args.func(args)
            return
        except ConsoleError as e:
            if e.status_code == 401:
                print("❌ Not logged in. Run `hippius-hub login --hippius-token <token>` "
                      "(get one from https://console.hippius.com).")
            elif e.status_code == 404:
                print(f"❌ Not found: {e.body}")
            else:
                print(f"❌ {e}")
            sys.exit(1)

    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
