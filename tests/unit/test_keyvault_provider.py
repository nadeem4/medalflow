"""Regressions for the Key Vault retry loop (Phase 3, task 3e).

Every distinct Azure failure -- 404, auth, throttling, a missing SDK -- used to
collapse into one generic ``ValueError`` distinguished only by an interpolated
string, non-retryable errors were retried with a fixed delay, and two paths
(``max_retries=0`` and a falsy client) returned ``None`` with no log at all.
"""

import pytest
from medalflow.common.exceptions import CTEError
from medalflow.secret_vault import keyvault as keyvault_module
from medalflow.secret_vault.keyvault import KeyVaultSecrets
from medalflow.settings.keyvault import KeyVaultSettings


class ResourceNotFoundError(Exception):
    status_code = 404


class ClientAuthenticationError(Exception):
    status_code = 401


class ServiceRequestError(Exception):
    """Transient transport failure."""


class _FakeSecret:
    def __init__(self, value):
        self.value = value


class _FakeClient:
    def __init__(self, error=None, value="vault-value"):
        self.error = error
        self.value = value
        self.calls = 0

    def get_secret(self, secret_name):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return _FakeSecret(self.value)


@pytest.fixture
def sleeps(monkeypatch):
    recorded = []
    monkeypatch.setattr(keyvault_module.time, "sleep", recorded.append)
    return recorded


def _provider(client, **overrides):
    settings = KeyVaultSettings(url="https://vault.example.net/", **overrides)
    provider = KeyVaultSecrets(settings=settings)
    provider._secret_client = client
    return provider


def test_secret_is_returned(sleeps):
    provider = _provider(_FakeClient())

    assert provider.get_secret("LAKE-KEY").get_secret_value() == "vault-value"
    assert sleeps == []


def test_zero_max_retries_still_makes_one_attempt(sleeps):
    client = _FakeClient(error=ServiceRequestError("boom"))
    provider = _provider(client, max_retries=0)

    with pytest.raises(CTEError):
        provider.get_secret("LAKE-KEY")

    assert client.calls == 1


def test_missing_client_raises_instead_of_returning_none(monkeypatch, sleeps):
    monkeypatch.setattr(KeyVaultSecrets, "secret_client", property(lambda self: None))
    provider = KeyVaultSecrets(settings=KeyVaultSettings(url="https://vault.example.net/"))

    with pytest.raises(CTEError):
        provider.get_secret("LAKE-KEY")


def test_authentication_failure_is_not_retried(sleeps):
    client = _FakeClient(error=ClientAuthenticationError("bad credentials"))
    provider = _provider(client)

    with pytest.raises(CTEError):
        provider.get_secret("LAKE-KEY")

    assert client.calls == 1
    assert sleeps == []


def test_missing_secret_is_not_retried(sleeps):
    client = _FakeClient(error=ResourceNotFoundError("no such secret"))
    provider = _provider(client)

    with pytest.raises(CTEError):
        provider.get_secret("LAKE-KEY")

    assert client.calls == 1
    assert sleeps == []


def test_missing_sdk_is_not_retried(sleeps):
    client = _FakeClient(error=ImportError("No module named 'azure.keyvault.secrets'"))
    provider = _provider(client)

    with pytest.raises(CTEError):
        provider.get_secret("LAKE-KEY")

    assert client.calls == 1
    assert sleeps == []


def test_transient_failure_is_retried_with_growing_backoff(sleeps):
    client = _FakeClient(error=ServiceRequestError("connection reset"))
    provider = _provider(client, max_retries=3, retry_delay_seconds=1.0)

    with pytest.raises(CTEError):
        provider.get_secret("LAKE-KEY")

    assert client.calls == 3
    assert len(sleeps) == 2
    assert sleeps[1] > sleeps[0]


def test_error_identifies_the_underlying_azure_failure(sleeps):
    cause = ClientAuthenticationError("bad credentials")
    provider = _provider(_FakeClient(error=cause))

    with pytest.raises(CTEError) as excinfo:
        provider.get_secret("LAKE-KEY")

    assert excinfo.value.details["error_type"] == "ClientAuthenticationError"
    assert excinfo.value.__cause__ is cause
    assert excinfo.value.is_retryable is False


def test_throttling_is_reported_as_retryable(sleeps):
    class TooManyRequests(Exception):
        status_code = 429

    provider = _provider(_FakeClient(error=TooManyRequests("slow down")), max_retries=1)

    with pytest.raises(CTEError) as excinfo:
        provider.get_secret("LAKE-KEY")

    assert excinfo.value.is_retryable is True


def test_unconfigured_vault_returns_the_default():
    provider = KeyVaultSecrets(settings=KeyVaultSettings())

    assert provider.get_secret("LAKE-KEY", default="fallback").get_secret_value() == "fallback"
    assert provider.get_secret("LAKE-KEY") is None
