"""Fixtures for Fabric integration tests.

pytest-dotenv loads .env automatically (configured via env_files in
pyproject.toml), so by the time these fixtures run the FABRIC_* vars
are already in os.environ.
"""

from __future__ import annotations

import pytest

from deploy.auth import (
    get_ci_credentials,
    get_fabric_api_token,
    get_runtime_credentials,
)


@pytest.fixture(scope="module")
def ci_credential():
    return get_ci_credentials()


@pytest.fixture(scope="module")
def runtime_credential():
    return get_runtime_credentials()


@pytest.fixture(scope="module")
def ci_token(ci_credential) -> str:
    return get_fabric_api_token(ci_credential)
