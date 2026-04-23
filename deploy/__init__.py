"""Generic Fabric deployment utilities.

Top-level package for any Fabric project's deployment infrastructure.
No project-specific names — drop this directory into another repo,
populate the expected env vars, and it works.
"""

from deploy.auth import (
    FABRIC_API_SCOPE,
    MissingEnvVarError,
    get_ci_credentials,
    get_fabric_api_token,
    get_runtime_credentials,
    get_workspace_id,
)

__all__ = [
    "FABRIC_API_SCOPE",
    "MissingEnvVarError",
    "get_ci_credentials",
    "get_fabric_api_token",
    "get_runtime_credentials",
    "get_workspace_id",
]
