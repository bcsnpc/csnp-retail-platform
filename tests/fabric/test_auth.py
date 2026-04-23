"""Live integration tests for deploy.auth against the Fabric REST API.

All tests in this file are marked fabric_integration — excluded from default
runs.  Execute explicitly with:

    pytest tests/fabric -m fabric_integration -v
"""

from __future__ import annotations

import pytest
import requests

from deploy.auth import get_fabric_api_token, get_workspace_id

pytestmark = pytest.mark.fabric_integration

_FABRIC_API_BASE = "https://api.fabric.microsoft.com"
_EXPECTED_WORKSPACES = {"CSNP_Dev", "CSNP_Test", "CSNP_Prod"}


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_ci_sp_authenticates(ci_credential):
    token = get_fabric_api_token(ci_credential)
    assert isinstance(token, str)
    assert len(token) > 100, "Bearer token suspiciously short"


def test_runtime_sp_authenticates(runtime_credential):
    token = get_fabric_api_token(runtime_credential)
    assert isinstance(token, str)
    assert len(token) > 100, "Bearer token suspiciously short"


def test_ci_sp_lists_workspaces(ci_token):
    r = requests.get(f"{_FABRIC_API_BASE}/v1/workspaces", headers=_bearer(ci_token), timeout=30)
    assert r.status_code == 200, f"List workspaces failed: HTTP {r.status_code} — {r.text[:300]}"

    visible = {w.get("displayName") for w in r.json().get("value", [])}
    missing = _EXPECTED_WORKSPACES - visible
    assert not missing, (
        f"Expected workspaces missing for CI SP: {sorted(missing)}. "
        f"Visible: {sorted(visible)}"
    )


def test_ci_sp_reads_dev_workspace(ci_token):
    dev_id = get_workspace_id("FABRIC_WORKSPACE_DEV_ID")
    r = requests.get(
        f"{_FABRIC_API_BASE}/v1/workspaces/{dev_id}",
        headers=_bearer(ci_token),
        timeout=30,
    )
    assert r.status_code == 200, f"Read workspace failed: HTTP {r.status_code} — {r.text[:300]}"
    assert r.json().get("displayName") == "CSNP_Dev"
