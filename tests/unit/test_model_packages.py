"""Where MedalFlow looks for your models (ADR 002, Decision 6).

The package paths used to be derived from one client's naming convention
(`custom_{name}.silver`), so a project could only be discovered if it adopted
that convention. Four of the five derived properties had zero readers. They are
replaced by configuration: one `models_package` for the common case, per-layer
overrides for the rest, and an error naming the variable to set when neither is
given.
"""

from pathlib import Path

import pytest
from medalflow.settings.main import MedalflowSettings

BOOT_ENV = {
    "source_system": "sap",
    "ds_env": "dev",
    "name": "fin",
    "compute": {"lake_database_name": "lakedb"},
}


def _settings(**overrides) -> MedalflowSettings:
    return MedalflowSettings(**BOOT_ENV, **overrides)


# --- resolution order ------------------------------------------------------


def test_models_package_derives_every_layer():
    settings = _settings(models_package="acme_warehouse")

    assert settings.package_for_layer("bronze") == "acme_warehouse.bronze"
    assert settings.package_for_layer("silver") == "acme_warehouse.silver"
    assert settings.package_for_layer("gold") == "acme_warehouse.gold"


def test_a_layer_override_beats_models_package():
    settings = _settings(models_package="acme_warehouse", gold_package="acme_reporting.marts")

    assert settings.package_for_layer("gold") == "acme_reporting.marts"
    assert settings.package_for_layer("silver") == "acme_warehouse.silver"


def test_a_layer_override_works_without_models_package():
    settings = _settings(silver_package="acme.silver")

    assert settings.package_for_layer("silver") == "acme.silver"


# --- the boot contract stays at four variables -----------------------------


def test_settings_construct_without_any_package_configured():
    """`models_package` is optional: the failure belongs at discovery time."""
    assert _settings().models_package is None


def test_an_unconfigured_layer_names_the_variables_to_set():
    settings = _settings()

    with pytest.raises(ValueError) as error:
        settings.package_for_layer("gold")

    message = str(error.value)
    assert "MEDALFLOW_MODELS_PACKAGE" in message
    assert "MEDALFLOW_GOLD_PACKAGE" in message


def test_an_unknown_layer_is_rejected():
    with pytest.raises(ValueError):
        _settings(models_package="acme").package_for_layer("platinum")


# --- the convention-derived properties are gone ----------------------------


@pytest.mark.parametrize(
    "removed",
    [
        "silver_package_name",
        "gold_package_name",
        "dimension_package_name",
        "silver_proc_mapping_package_name",
        "silver_proc_crud_mapping_package_name",
    ],
)
def test_the_convention_derived_package_properties_are_deleted(removed):
    assert not hasattr(MedalflowSettings, removed)


# --- .env.example matches the live schema ----------------------------------

ENV_EXAMPLE = Path(__file__).resolve().parents[2] / ".env.example"


def _documented_variables() -> set[str]:
    """Every MEDALFLOW_ variable named in .env.example, commented out or not."""
    variables = set()
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = line.lstrip("#").strip()
        if line.startswith("MEDALFLOW_") and "=" in line:
            variables.add(line.split("=", 1)[0].strip())
    return variables


def test_env_example_documents_the_model_packages():
    documented = _documented_variables()

    assert "MEDALFLOW_MODELS_PACKAGE" in documented
    assert {"MEDALFLOW_BRONZE_PACKAGE", "MEDALFLOW_SILVER_PACKAGE", "MEDALFLOW_GOLD_PACKAGE"} <= (
        documented
    )


def test_every_documented_variable_exists_in_the_settings_schema():
    """A variable in .env.example that no field reads is a lie in the docs."""
    unknown = []
    for variable in sorted(_documented_variables()):
        path = variable.removeprefix("MEDALFLOW_").lower().split("__")
        model = MedalflowSettings
        for index, part in enumerate(path):
            fields = getattr(model, "model_fields", {})
            if part not in fields:
                unknown.append(variable)
                break
            annotation = fields[part].annotation
            if index < len(path) - 1:
                model = next(
                    (
                        arg
                        for arg in getattr(annotation, "__args__", [annotation])
                        if arg is not None
                    ),
                    annotation,
                )

    assert unknown == []
