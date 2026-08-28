"""Offline settings shared by the whole suite.

Four variables construct settings; the two lake accounts below are placeholders
so the offline suite exercises the same configured-lake path a real deployment
takes. Every value is fake.

`MEDALFLOW_TEST_MODE=true` with `MEDALFLOW_KEYVAULT__URL` unset makes the secret
provider `MockSecrets`, so nothing touches Azure -- verified to make no network
calls at all (D6).
"""

import pytest

OFFLINE_ENV = {
    "MEDALFLOW_TEST_MODE": "true",
    "MEDALFLOW_SOURCE_SYSTEM": "sap",
    "MEDALFLOW_DS_ENV": "dev",
    "MEDALFLOW_NAME": "fin",
    "MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME": "lakedb",
    "MEDALFLOW_DATALAKE__PROCESSED__ACCOUNT_NAME": "devlake",
    "MEDALFLOW_DATALAKE__INTERNAL__ACCOUNT_NAME": "devlake",
}


@pytest.fixture
def offline_settings(monkeypatch):
    """Boot real settings from placeholder env vars, then reset the singleton."""
    from medalflow.settings import main as settings_main

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)

    settings = settings_main.get_settings(force_reload=True)
    try:
        yield settings
    finally:
        # The singleton outlives monkeypatch's env cleanup, so clear it or the
        # next caller inherits settings built from this fixture's environment.
        settings_main._settings = None
