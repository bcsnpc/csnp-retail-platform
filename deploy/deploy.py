"""Deploy fabric/ artifacts to a target Fabric workspace via fabric-cicd.

Usage
-----
    python -m deploy.deploy --environment dev      # module invocation
    csnp-deploy --environment dev                  # console-script entry (after uv sync)
    uv run csnp-deploy --environment dev           # without venv activation

The --environment value (dev/test/prod) selects which workspace ID env var
to read and is uppercased before being passed to FabricWorkspace, so it
matches the DEV / TEST / PROD keys in parameter.yml.

Item types in scope are deliberately narrow for the initial deploy
(Lakehouse only).  Expand ITEM_TYPES_IN_SCOPE as more artifacts land.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items,
)

from deploy.auth import get_ci_credentials, get_workspace_id

REPO_ROOT = Path(__file__).resolve().parent.parent
FABRIC_DIR = REPO_ROOT / "fabric"

ENV_TO_WORKSPACE_VAR = {
    "dev":  "FABRIC_WORKSPACE_DEV_ID",
    "test": "FABRIC_WORKSPACE_TEST_ID",
    "prod": "FABRIC_WORKSPACE_PROD_ID",
}

ITEM_TYPES_IN_SCOPE = ["Lakehouse"]

log = logging.getLogger("deploy")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deploy fabric/ to a target Fabric workspace.")
    p.add_argument(
        "--environment",
        choices=list(ENV_TO_WORKSPACE_VAR),
        required=True,
        help="Target environment (selects workspace ID env var and parameter.yml key).",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = _parse_args()

    load_dotenv(REPO_ROOT / ".env")

    workspace_id = get_workspace_id(ENV_TO_WORKSPACE_VAR[args.environment])
    # One credential reused across publish + unpublish; ClientSecretCredential
    # caches the bearer token internally for ~1h, so this avoids redundant
    # auth round-trips during a single deploy run.
    credential = get_ci_credentials()

    log.info(
        "Deploying %s → workspace %s (env=%s, scope=%s)",
        FABRIC_DIR, workspace_id, args.environment.upper(), ITEM_TYPES_IN_SCOPE,
    )

    target = FabricWorkspace(
        workspace_id=workspace_id,
        environment=args.environment.upper(),
        repository_directory=str(FABRIC_DIR),
        item_type_in_scope=ITEM_TYPES_IN_SCOPE,
        token_credential=credential,
    )

    publish_all_items(target)
    unpublish_all_orphan_items(target)

    log.info("Deploy complete.")


if __name__ == "__main__":
    main()
