import argparse
import sys
from .file_download import hippius_hub_download
from .auth import login

def main():
    parser = argparse.ArgumentParser(description="Hippius Hub CLI - Drop-in replacement for huggingface-cli with ultra-fast downloads")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download a file from a repository")
    download_parser.add_argument("repo_id", type=str, help="Repository ID (e.g., org/model)")
    download_parser.add_argument("filename", type=str, help="Filename to download")
    download_parser.add_argument("--revision", type=str, default="main", help="Revision/branch")
    download_parser.add_argument("--chunk-size", type=int, default=50 * 1024 * 1024, help="Chunk size in bytes")
    download_parser.add_argument("--cache-dir", type=str, default=None, help="Path to the cache directory")

    # Upload command
    upload_parser = subparsers.add_parser("upload", help="Upload a file or folder to a repository")
    upload_parser.add_argument("repo_id", type=str, help="Repository ID (e.g., org/model)")
    upload_parser.add_argument("local_path", type=str, help="Path to the local file or folder")
    upload_parser.add_argument("--revision", type=str, default="main", help="Revision/branch")

    # Login command
    login_parser = subparsers.add_parser("login", help="Log in to Hippius Hub")
    login_parser.add_argument("--token", type=str, required=True, help="Access token")

    args = parser.parse_args()

    if args.command == "download":
        print(f"Downloading {args.filename} from {args.repo_id} (revision: {args.revision})...")
        try:
            path = hippius_hub_download(
                repo_id=args.repo_id,
                filename=args.filename,
                revision=args.revision,
                chunk_size=args.chunk_size,
                cache_dir=args.cache_dir
            )
            print(f"✅ File downloaded to: {path}")
        except Exception as e:
            print(f"❌ Download failed: {e}")
            sys.exit(1)

    elif args.command == "upload":
        from .file_upload import hippius_hub_upload
        try:
            hippius_hub_upload(
                repo_id=args.repo_id,
                local_path=args.local_path,
                revision=args.revision
            )
        except Exception as e:
            print(f"❌ Upload failed: {e}")
            sys.exit(1)

    elif args.command == "login":
        login(args.token)
        print("✅ Login successful")

    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
