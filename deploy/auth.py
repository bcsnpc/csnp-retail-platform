"""Fabric authentication and API-token utilities.

Generic infrastructure — any Fabric project can use this by populating
the expected environment variables.  No project-specific names.

Required environment variables
------------------------------
    FABRIC_TENANT_ID                 — Azure AD tenant ID
    FABRIC_CI_SP_CLIENT_ID           — CI / deployment service principal
    FABRIC_CI_SP_CLIENT_SECRET
    FABRIC_RUNTIME_SP_CLIENT_ID      — runtime / data-plane service principal
    FABRIC_RUNTIME_SP_CLIENT_SECRET

Workspace IDs are looked up by env var name (whatever the caller chose).
Recommended naming pattern: FABRIC_WORKSPACE_{ENV}_ID
    dev_id = get_workspace_id("FABRIC_WORKSPACE_DEV_ID")

Usage
-----
    from deploy.auth import get_ci_credentials, get_fabric_api_token

    cred  = get_ci_credentials()
    token = get_fabric_api_token(cred)
    # → use token as Bearer in REST calls to https://api.fabric.microsoft.com
"""

from __future__ import annotations

import os

from azure.identity import ClientSecretCredential

# Scope for Microsoft Fabric REST API tokens.
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"


class MissingEnvVarError(RuntimeError):
    """Raised when a required Fabric environment variable is unset or empty."""


# ── Internal helpers ──────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise MissingEnvVarError(
            f"Required environment variable {name!r} is not set or empty. "
            f"Populate it in your .env file or shell environment."
        )
    return val


def _build_credential(client_id_var: str, client_secret_var: str) -> ClientSecretCredential:
    return ClientSecretCredential(
        tenant_id=_require_env("FABRIC_TENANT_ID"),
        client_id=_require_env(client_id_var),
        client_secret=_require_env(client_secret_var),
    )


# ── Public API ────────────────────────────────────────────────────────────────

def get_ci_credentials() -> ClientSecretCredential:
    """ClientSecretCredential for the CI / deployment service principal.

    Reads FABRIC_TENANT_ID, FABRIC_CI_SP_CLIENT_ID, FABRIC_CI_SP_CLIENT_SECRET.
    """
    return _build_credential("FABRIC_CI_SP_CLIENT_ID", "FABRIC_CI_SP_CLIENT_SECRET")


def get_runtime_credentials() -> ClientSecretCredential:
    """ClientSecretCredential for the runtime / data-plane service principal.

    Reads FABRIC_TENANT_ID, FABRIC_RUNTIME_SP_CLIENT_ID, FABRIC_RUNTIME_SP_CLIENT_SECRET.
    """
    return _build_credential("FABRIC_RUNTIME_SP_CLIENT_ID", "FABRIC_RUNTIME_SP_CLIENT_SECRET")


def get_fabric_api_token(credential: ClientSecretCredential) -> str:
    """Acquire a bearer token for the Fabric REST API from any credential.

    The credential argument is intentionally typed as ClientSecretCredential
    (not the broader TokenCredential protocol) to make the supported auth
    paths explicit; widen the type later if managed-identity or interactive
    flows are added.
    """
    return credential.get_token(FABRIC_API_SCOPE).token


def get_workspace_id(env_var: str) -> str:
    """Look up a workspace ID stored under the given env-var name.

    Examples
    --------
        dev_id  = get_workspace_id("FABRIC_WORKSPACE_DEV_ID")
        prod_id = get_workspace_id("FABRIC_WORKSPACE_PROD_ID")
    """
    return _require_env(env_var)
