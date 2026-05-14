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
        print(f"   Example: hippius-hub registry artifacts {args.repo}/some-repo")
        sys.exit(2)
    project, repo = args.repo.split("/", 1)
    res = console.list_artifacts(project, repo, page=args.page, page_size=args.page_size)
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
            print(f"❌ Download failed: {e}")
            sys.exit(1)
        return

    if args.command == "upload":
        from .file_upload import hippius_hub_upload
        try:
            hippius_hub_upload(repo_id=args.repo_id, local_path=args.local_path, revision=args.revision)
        except Exception as e:
            print(f"❌ Upload failed: {e}")
            sys.exit(1)
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
                password = getpass.getpass("Password or CLI secret: ").strip()
            else:
                token = getpass.getpass("Token: ").strip()
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
