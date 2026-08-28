"""Correctness regressions for secret loading (Phase 3, task 3).

Every defect covered here was silent: a failed Key Vault lookup became `None`,
that `None` was cached for the lifetime of the process, and the resulting
connection string carried a literal ``AccountKey=None``. None of it was tested.
"""

import gc
import re
from pathlib import Path

import pytest
from medalflow.common.exceptions import CTEError
from medalflow.protocols.providers import SecretProvider
from medalflow.secret_vault.keyvault import KeyVaultSecrets
from medalflow.secret_vault.mock import MockSecrets
from medalflow.settings.datalake import ProcessedDataLakeConfig
from medalflow.settings.keyvault import KeyVaultSettings
from pydantic import SecretStr

PYPROJECT = Path(__file__).parents[2] / "pyproject.toml"


class _CountingProvider:
    """Records every call so caching behaviour is observable."""

    def __init__(self, values=None, failures=0):
        self.values = values or {}
        self.failures = failures
        self.calls = 0

    def get_secret(self, secret_name, default=None):
        self.calls += 1
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("key vault unreachable")
        value = self.values.get(secret_name, default)
        return SecretStr(value) if value is not None else None


def _lake(secret_name="LAKE-KEY"):
    return ProcessedDataLakeConfig(account_name="devlake", access_key_secret_name=secret_name)


# --- 3a: a provider failure must be loud, and must not be cached -------------


def test_provider_failure_raises_instead_of_yielding_none():
    config = _lake()
    config.attach_secrets(_CountingProvider(failures=1))

    with pytest.raises(CTEError):
        _ = config.access_key


def test_provider_failure_names_the_secret_and_keeps_the_cause():
    config = _lake("LAKE-KEY")
    config.attach_secrets(_CountingProvider(failures=1))

    with pytest.raises(CTEError) as excinfo:
        _ = config.access_key

    assert "LAKE-KEY" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_transient_failure_is_not_cached_forever():
    provider = _CountingProvider(values={"LAKE-KEY": "real-key"}, failures=1)
    config = _lake()
    config.attach_secrets(provider)

    with pytest.raises(CTEError):
        _ = config.access_key

    # The retry must reach the provider again, not a cached None.
    assert config.access_key == "real-key"
    assert provider.calls == 2


def test_absent_provider_still_yields_none():
    # Managed identity: no provider attached, no secret, no error.
    assert _lake().access_key is None


def test_successful_lookup_is_still_cached():
    provider = _CountingProvider(values={"LAKE-KEY": "real-key"})
    config = _lake()
    config.attach_secrets(provider)

    assert config.access_key == "real-key"
    assert config.access_key == "real-key"
    assert provider.calls == 1


def test_connection_string_never_interpolates_a_failed_lookup():
    config = _lake()
    config.attach_secrets(_CountingProvider(failures=1))

    with pytest.raises(CTEError):
        _ = config.connection_string


# --- 3b: the cache belongs to the instance, not to the descriptor ------------


def test_cached_value_lives_on_the_instance():
    config = _lake()
    config.attach_secrets(_CountingProvider(values={"LAKE-KEY": "real-key"}))

    assert config.access_key == "real-key"
    assert any("real-key" in str(value) for value in config.__dict__.values())


def test_cache_does_not_outlive_the_instance():
    descriptor = ProcessedDataLakeConfig.access_key
    config = _lake()
    config.attach_secrets(_CountingProvider(values={"LAKE-KEY": "real-key"}))
    assert config.access_key == "real-key"

    del config
    gc.collect()

    # An id()-keyed cache on the descriptor keeps the dead instance's value and
    # can hand it to whatever CPython allocates at that address next.
    assert getattr(descriptor, "_cache", {}) == {}


# --- 3c: test mode must refuse to activate in production ---------------------


def _prod_env(monkeypatch, app_env="prod"):
    from tests.conftest import OFFLINE_ENV

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("MEDALFLOW_APP_ENV", app_env)


def test_test_mode_is_refused_in_production(monkeypatch):
    from medalflow.settings import main as settings_main

    _prod_env(monkeypatch, "prod")
    try:
        with pytest.raises(Exception, match="(?i)test.?mode"):
            settings_main.get_settings(force_reload=True)
    finally:
        settings_main._settings = None


def test_test_mode_is_allowed_outside_production(monkeypatch):
    from medalflow.settings import main as settings_main

    _prod_env(monkeypatch, "dev")
    try:
        assert settings_main.get_settings(force_reload=True).test_mode is True
    finally:
        settings_main._settings = None


# --- 3d: the protocol is get_secret, and nothing else ------------------------


def test_keyvault_provider_satisfies_its_own_protocol():
    assert isinstance(KeyVaultSecrets(settings=KeyVaultSettings()), SecretProvider)


def test_mock_provider_satisfies_the_protocol():
    assert isinstance(MockSecrets(), SecretProvider)


def test_protocol_does_not_declare_clear_cache():
    declared = {name for name in vars(SecretProvider) if not name.startswith("_")}

    assert declared == {"get_secret"}


# --- 3f: every imported third-party package is declared ----------------------


@pytest.mark.parametrize("package", ["azure-keyvault-secrets", "adlfs", "pyarrow"])
def test_imported_package_is_declared_as_a_dependency(package):
    text = PYPROJECT.read_text(encoding="utf-8")
    section = text.split("[tool.poetry.dependencies]", 1)[1].split("\n[", 1)[0]

    assert re.search(
        rf"^{re.escape(package)}\s*=", section, re.MULTILINE
    ), f"{package} is imported at runtime but not declared in pyproject.toml"
