"""Offline settings for the end-to-end suite.

Booting `_Settings` today needs 20 environment variables. That is not a test
smell — it is the bug Phase 3 fixes: `CTEBaseSettings` declares `tenant_id`,
`source_system`, `ds_env` and `name` as required with no defaults, and every
nested settings model inherits from it while carrying its own `env_prefix`, so
those four must be re-supplied under each of PROCESSED_LAKE_, INTERNAL_LAKE_
and KEYVAULT_. When Phase 3 lands "settings boot from a documented
minimal .env", this block should shrink to a handful of lines.

Every value is a placeholder. `CTE_TEST_MODE=true` with `KEYVAULT_URL` unset
makes the secret provider `MockSecrets`, so nothing touches Azure — verified
to make no network calls at all (D6).
"""

import pytest

OFFLINE_ENV = {
    "CTE_TEST_MODE": "true",
    # Base identity, consumed by the unprefixed settings models.
    "TENANT_ID": "00000000-0000-0000-0000-000000000000",
    "SOURCE_SYSTEM": "sap",
    "DS_ENV": "dev",
    "NAME": "fin",
    "LAKE_DATABASE_NAME": "lakedb",
    # Re-declared per nested model because each carries its own env_prefix.
    "PROCESSED_LAKE_TENANT_ID": "00000000-0000-0000-0000-000000000000",
    "PROCESSED_LAKE_SOURCE_SYSTEM": "sap",
    "PROCESSED_LAKE_DS_ENV": "dev",
    "PROCESSED_LAKE_NAME": "fin",
    "PROCESSED_LAKE_ACCOUNT_NAME": "devlake",
    "INTERNAL_LAKE_TENANT_ID": "00000000-0000-0000-0000-000000000000",
    "INTERNAL_LAKE_SOURCE_SYSTEM": "sap",
    "INTERNAL_LAKE_DS_ENV": "dev",
    "INTERNAL_LAKE_NAME": "fin",
    "INTERNAL_LAKE_ACCOUNT_NAME": "devlake",
    "KEYVAULT_TENANT_ID": "00000000-0000-0000-0000-000000000000",
    "KEYVAULT_SOURCE_SYSTEM": "sap",
    "KEYVAULT_DS_ENV": "dev",
    "KEYVAULT_NAME": "fin",
}


@pytest.fixture
def offline_settings(monkeypatch):
    """Boot real settings from placeholder env vars, then reset the singleton."""
    from core.settings import main as settings_main

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)

    settings = settings_main.get_settings(force_reload=True)
    try:
        yield settings
    finally:
        # The singleton outlives monkeypatch's env cleanup, so clear it or the
        # next caller inherits settings built from this fixture's environment.
        settings_main._settings = None
