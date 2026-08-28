"""The zero-dependency secret provider (Phase 3, task 4e).

Without it, a deployment with no Key Vault and no test mode has no provider at
all, so every secret silently resolves to None. `EnvSecretProvider` makes the
package usable with no cloud SDK installed.
"""

import pytest
from medalflow.protocols import SecretProvider
from medalflow.secret_vault.env import EnvSecretProvider


@pytest.fixture
def provider():
    return EnvSecretProvider()


def test_reads_the_prefixed_environment_variable(provider, monkeypatch):
    monkeypatch.setenv("MEDALFLOW_SECRET_PROCESSED_ADLS_ACCOUNT_KEY", "lake-key")

    secret = provider.get_secret("PROCESSED-ADLS-ACCOUNT-KEY")

    assert secret.get_secret_value() == "lake-key"


def test_secret_names_are_normalised(provider, monkeypatch):
    monkeypatch.setenv("MEDALFLOW_SECRET_DB_PASSWORD", "hunter2")

    assert provider.get_secret("db.password").get_secret_value() == "hunter2"


def test_returns_none_when_unset(provider, monkeypatch):
    monkeypatch.delenv("MEDALFLOW_SECRET_MISSING", raising=False)

    assert provider.get_secret("MISSING") is None


def test_returns_the_default_when_unset(provider, monkeypatch):
    monkeypatch.delenv("MEDALFLOW_SECRET_MISSING", raising=False)

    assert provider.get_secret("MISSING", default="fallback").get_secret_value() == "fallback"


def test_satisfies_the_secret_provider_protocol(provider):
    assert isinstance(provider, SecretProvider)


def test_settings_use_it_when_key_vault_is_not_configured(monkeypatch):
    from medalflow.settings import main as settings_main

    from tests.conftest import OFFLINE_ENV

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("MEDALFLOW_TEST_MODE", "false")
    monkeypatch.delenv("MEDALFLOW_KEYVAULT__URL", raising=False)

    try:
        settings = settings_main.get_settings(force_reload=True)
        assert isinstance(settings.secrets, EnvSecretProvider)
    finally:
        settings_main._settings = None
