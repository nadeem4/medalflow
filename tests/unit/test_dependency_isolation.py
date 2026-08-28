"""The cloud SDKs stay out of the core import graph (Phase 3, tasks 5 and 6).

`import medalflow` used to pull azure-identity, azure-storage-file-datalake,
pyodbc and pandas in transitively, through
``api -> compute -> datalake.client`` and ``compute.engines.base``. That made
the package uninstallable without the whole Azure stack and contradicted the
three-seam boundary documented in :mod:`medalflow.protocols`.

These tests are the in-repo half of the guard; the `bare-install` CI job is the
other half.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from medalflow.common.exceptions import CTEError

CORE_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = CORE_ROOT / "pyproject.toml"

# Everything the `azure` extra installs. None of it may be reachable from a
# bare `import medalflow`.
OPTIONAL_DISTRIBUTIONS = (
    "azure-identity",
    "azure-keyvault-secrets",
    "azure-storage-file-datalake",
    "adlfs",
    "pyarrow",
    "pyodbc",
    "pandas",
)
OPTIONAL_TOP_LEVEL_MODULES = ("azure", "adlfs", "pyarrow", "pyodbc", "pandas")


def _extras_section() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    return text.split("[tool.poetry.extras]", 1)[1].split("\n[", 1)[0]


def _dependencies_section() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    return text.split("[tool.poetry.dependencies]", 1)[1].split("\n[", 1)[0]


# --- 5a: nothing heavy is imported at module scope ---------------------------


def test_importing_the_package_does_not_import_any_cloud_sdk():
    """A fresh interpreter importing the public surface pulls in no SDK.

    Run out-of-process: this suite has already imported everything, so
    inspecting the in-process ``sys.modules`` would prove nothing.
    """
    script = (
        "import sys\n"
        "import medalflow, medalflow.medallion, medalflow.api\n"
        f"watched = set({OPTIONAL_TOP_LEVEL_MODULES!r})\n"
        "roots = {name.split('.')[0] for name in sys.modules}\n"
        "print(','.join(sorted(roots & watched)))\n"
    )

    env = dict(os.environ)
    # Mirror `pythonpath = ["src"]`; the console script does not add it.
    env["PYTHONPATH"] = os.pathsep.join([str(CORE_ROOT / "src"), env.get("PYTHONPATH", "")]).rstrip(
        os.pathsep
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=str(CORE_ROOT),
        env=env,
        check=True,
    )

    assert (
        result.stdout.strip() == ""
    ), f"importing medalflow leaked optional dependencies: {result.stdout.strip()}"


@pytest.mark.parametrize(
    "module_name",
    [
        "medalflow.protocols.features",
        "medalflow.compute.types",
        "medalflow.compute.engines.base",
        "medalflow.datalake.client",
        "medalflow.core.features.managers.stats",
    ],
)
def test_module_binds_no_cloud_sdk_at_module_scope(module_name):
    module = __import__(module_name, fromlist=["__name__"])
    bound = {
        name
        for name, value in vars(module).items()
        if getattr(value, "__name__", "").split(".")[0] in OPTIONAL_TOP_LEVEL_MODULES
    }

    assert bound == set(), f"{module_name} binds {sorted(bound)} at module scope"


# --- 5b: a missing extra says what to install --------------------------------


def test_missing_optional_module_names_the_extra():
    from medalflow.common.optional_deps import require_module

    with pytest.raises(CTEError) as excinfo:
        require_module("medalflow_no_such_module")

    message = str(excinfo.value)
    assert "medalflow_no_such_module" in message
    assert "medalflow[azure]" in message


def test_datalake_client_reports_the_missing_extra(offline_settings, monkeypatch):
    from medalflow.constants.datalake import LakeType
    from medalflow.datalake.client import DatalakeClient

    monkeypatch.setitem(sys.modules, "azure.identity", None)
    monkeypatch.setitem(sys.modules, "azure.storage.filedatalake", None)
    client = DatalakeClient(LakeType.PROCESSED)

    with pytest.raises(CTEError) as excinfo:
        client._get_fs_client()

    assert "medalflow[azure]" in str(excinfo.value)


def test_sql_engine_reports_the_missing_extra(offline_settings, monkeypatch):
    from medalflow.compute.engines.base import BaseSQLEngine

    monkeypatch.setitem(sys.modules, "pyodbc", None)
    engine = BaseSQLEngine(offline_settings.compute.synapse)

    with pytest.raises(CTEError) as excinfo:
        engine._create_engine()

    assert "medalflow[azure]" in str(excinfo.value)


# --- 5c: the extra exists, and every member is optional ----------------------


@pytest.mark.parametrize("distribution", OPTIONAL_DISTRIBUTIONS)
def test_optional_distribution_is_declared_optional(distribution):
    section = _dependencies_section()

    assert (
        f"{distribution} = " in section or f"{distribution} = " in section
    ), f"{distribution} is not declared in [tool.poetry.dependencies]"
    line = next(
        line for line in section.splitlines() if line.strip().startswith(f"{distribution} =")
    )
    assert "optional = true" in line, f"{distribution} is not marked optional: {line!r}"


@pytest.mark.parametrize("distribution", OPTIONAL_DISTRIBUTIONS)
def test_azure_extra_lists_every_optional_distribution(distribution):
    assert f'"{distribution}"' in _extras_section()


# --- 5d: a Key Vault that cannot be built still yields a provider ------------


def test_settings_fall_back_to_env_secrets_when_keyvault_cannot_be_built(monkeypatch):
    from medalflow.secret_vault.env import EnvSecretProvider
    from medalflow.settings import main as settings_main

    def _explode(*args, **kwargs):
        raise RuntimeError("vault unreachable")

    monkeypatch.setattr(settings_main, "KeyVaultSecrets", _explode)
    for key, value in {
        "MEDALFLOW_SOURCE_SYSTEM": "sap",
        "MEDALFLOW_DS_ENV": "dev",
        "MEDALFLOW_NAME": "fin",
        "MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME": "lakedb",
        "MEDALFLOW_KEYVAULT__URL": "https://example.vault.azure.net/",
    }.items():
        monkeypatch.setenv(key, value)

    try:
        settings = settings_main.get_settings(force_reload=True)
        assert isinstance(settings.secrets, EnvSecretProvider)
    finally:
        settings_main._settings = None
