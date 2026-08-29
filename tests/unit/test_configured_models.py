"""`configured_models` is a filter, and an unconfigured filter filters nothing.

`get_configured_model_list()` returns `[]` when the setting is unset, and
`is_model_configured` asked whether a name was *in* that list -- so every
silver model was skipped and the whole layer compiled to nothing, silently. A
filter nobody configured must not delete the thing it filters. That reading is
also the one `selection=None` already has everywhere else in the codebase:
absent means everything, empty means nothing.

A non-empty list keeps filtering exactly as it did.
"""

import sys
from pathlib import Path

import pytest
from medalflow.medallion.silver.metadata_discovery import SilverMetadataDiscovery
from medalflow.settings.main import MedalflowSettings

EXAMPLE = Path(__file__).resolve().parents[2] / "examples"

BOOT_ENV = {
    "source_system": "sap",
    "ds_env": "dev",
    "name": "fin",
    "compute": {"lake_database_name": "lakedb"},
}


def _settings(**overrides) -> MedalflowSettings:
    return MedalflowSettings(**BOOT_ENV, **overrides)


# --- the predicate ---------------------------------------------------------


def test_an_unset_model_list_configures_every_model():
    """The quickstart's own settings: nothing listed, nothing filtered out."""
    settings = _settings()

    assert settings.is_model_configured("sales") is True
    assert settings.is_model_configured("anything_at_all") is True


def test_a_configured_list_still_filters():
    settings = _settings(configured_models="sales,purchase")

    assert settings.is_model_configured("sales") is True
    assert settings.is_model_configured("inventory") is False


def test_the_list_itself_stays_empty_when_unset():
    """Only the predicate changed. `[]` still means 'nothing was listed'."""
    assert _settings().get_configured_model_list() == []


# --- what it does to silver discovery --------------------------------------


@pytest.fixture(autouse=True)
def example_project_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    for name in [m for m in sys.modules if m.split(".")[0] == "models"]:
        del sys.modules[name]


def _discover(settings):
    discovery = SilverMetadataDiscovery("models.silver", settings=settings)
    # A throwaway walk must not read, or leave behind, the layer-namespaced
    # entries the real cache manager keeps.
    discovery._cache_manager = None

    return sorted(model.name for model in discovery.discover_all())


def test_silver_discovery_finds_every_model_when_none_are_configured():
    """This is the bug: a new project's silver layer used to compile to zero."""
    assert _discover(_settings()) == ["DimCustomer", "FactOrders"]


def test_silver_discovery_still_honours_a_configured_list():
    assert _discover(_settings(configured_models="sales")) == [
        "DimCustomer",
        "FactOrders",
    ]


def test_a_configured_list_naming_another_model_still_excludes_silver():
    assert _discover(_settings(configured_models="purchase")) == []
