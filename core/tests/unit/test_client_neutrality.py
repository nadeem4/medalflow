"""Regression tests for client-specific names left in the framework (Phase 3, task 7).

MedalFlow was extracted from one client's warehouse and shipped with that
client's name baked into three places: the default Key Vault secret name for
the internal lake, an entry in the shipped mock secret provider, and the text
of every "feature is disabled" error, which told the reader to contact a team
that exists at exactly one company.

A framework names no deployment. The source scan below is the guard: it fails
on the string reappearing anywhere in the package, not just at the three sites
that were known when it was written.
"""

from pathlib import Path

from medalflow.common.exceptions import feature_not_enabled_error
from medalflow.secret_vault.mock import MockSecrets
from medalflow.settings.datalake import InternalDataLakeConfig

SRC = Path(__file__).resolve().parents[2] / "src" / "medalflow"
CLIENT_NAME = "cmaa"


def test_no_client_name_anywhere_in_the_package():
    offenders = sorted(
        path.relative_to(SRC).as_posix()
        for path in SRC.rglob("*.py")
        if CLIENT_NAME in path.read_text(encoding="utf-8").lower()
    )

    assert offenders == []


def test_internal_lake_secret_default_is_neutral():
    """The default must be neutral *and* resolvable by the shipped mock.

    Renaming it to something the mock provider does not know would leave
    `test_mode` reading an empty access key for a lake it reports as
    configured -- a quieter failure than the client name it replaced.
    """
    default = InternalDataLakeConfig().access_key_secret_name

    assert CLIENT_NAME not in default.lower()
    assert MockSecrets().get_secret(default) is not None


def test_feature_disabled_error_does_not_route_to_a_client_team():
    error = feature_not_enabled_error("snapshots", "Set MEDALFLOW_FEATURES__X=true.")

    assert CLIENT_NAME not in str(error).lower()
