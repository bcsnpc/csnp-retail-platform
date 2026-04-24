"""Upload local file(s) to a Microsoft Fabric OneLake lakehouse.

Uses the ADLS Gen2 DataLake SDK and the CI service-principal credential
from deploy.auth.  No project-specific names are hardcoded.

Single file:
    python scripts/upload_to_bronze.py \\
        --workspace CSNP_Dev --lakehouse CSNP_Bronze \\
        --local ./data/dim_date.parquet \\
        --remote Files/bronze/dim_date/dim_date.parquet

Batch (glob + path template):
    python scripts/upload_to_bronze.py \\
        --workspace CSNP_Dev --lakehouse CSNP_Bronze \\
        --local "./data/*.parquet" \\
        --remote "Files/bronze/{stem}/{name}"

Template placeholders: {stem} = filename without extension, {name} = full filename.
"""
from __future__ import annotations

import argparse
import glob as _glob
import sys
from pathlib import Path

from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

from deploy.auth import get_ci_credentials

ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"


def _upload(
    service: DataLakeServiceClient,
    workspace: str,
    lakehouse: str,
    local: Path,
    remote: str,
) -> None:
    fabric_path = f"{lakehouse}.Lakehouse/{remote.lstrip('/')}"
    size = local.stat().st_size

    if size > 1_048_576:
        print(f"  [{local.name}]  {size / 1_048_576:.1f} MB — uploading...", end="", flush=True)

    file_client = service.get_file_client(file_system=workspace, file_path=fabric_path)
    with local.open("rb") as fh:
        file_client.upload_data(fh.read(), overwrite=True)

    if size > 1_048_576:
        print(" done")
    else:
        print(f"  [{local.name}]  {size:,} bytes — done")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--workspace", required=True, help="OneLake workspace display name")
    ap.add_argument("--lakehouse", required=True, help="Lakehouse display name (without .Lakehouse suffix)")
    ap.add_argument("--local", required=True, dest="local_pat", help="Local file path or glob pattern")
    ap.add_argument("--remote", required=True, help="Destination path within the lakehouse, or template with {stem}/{name}")
    args = ap.parse_args()

    load_dotenv()
    service = DataLakeServiceClient(account_url=ONELAKE_URL, credential=get_ci_credentials())

    is_glob = any(c in args.local_pat for c in ("*", "?", "["))
    if is_glob:
        paths = sorted(Path(p) for p in _glob.glob(args.local_pat))
        if not paths:
            print(f"ERROR: no files matched {args.local_pat!r}", file=sys.stderr)
            return 1
        print(f"Batch: {len(paths)} file(s) → {args.workspace}/{args.lakehouse}.Lakehouse/")
        for p in paths:
            _upload(service, args.workspace, args.lakehouse, p, args.remote.format(stem=p.stem, name=p.name))
    else:
        local = Path(args.local_pat)
        if not local.exists():
            print(f"ERROR: {local} not found", file=sys.stderr)
            return 1
        print(f"Uploading: {local}  →  {args.workspace}/{args.lakehouse}.Lakehouse/{args.remote}")
        _upload(service, args.workspace, args.lakehouse, local, args.remote)

    print("All done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
