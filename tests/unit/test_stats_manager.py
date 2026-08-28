"""Regression tests for the stats manager and its protocol (Phase 3, task 9).

Two defects, both silent:

1. `_process_stats` stripped the configured table prefix with
   `Series.replace(prefix, "")`, which is whole-value equality, not prefix
   removal -- so "fin_customer" stayed "fin_customer", every later lookup by
   bare table name missed, and the stats feature was a no-op for any prefixed
   deployment. The prefix was also compared un-lowercased against a column the
   line above had lowercased.
2. `StatsProtocol` declared six methods and `StatsManager` implemented four.
   Because the protocol is used as a *base class*, the two it did not
   implement were inherited as `...` bodies returning None -- so
   `should_create_stats` answered None instead of a bool and
   `get_configured_tables` answered None instead of a list, with `isinstance`
   still passing and `runtime_checkable` catching nothing.
"""

import pytest
from medalflow.core.features.managers.stats import StatsManager
from medalflow.protocols.features import StatsProtocol
from medalflow.settings.main import MedalflowSettings

CSV_ROWS = [
    ("bronze", "FIN_Customer", "CustomerId"),
    ("bronze", "FIN_Customer", "CreatedOn"),
    ("bronze", "FIN_Order", "OrderId"),
]


@pytest.fixture
def manager(monkeypatch):
    """A StatsManager fed a fake CSV, with settings resolved offline (D6)."""
    pd = pytest.importorskip("pandas")
    import medalflow.core.features.managers.stats as stats_module

    settings = MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb"},
    )
    # `stats` imports both names at module scope, so they are patched there.
    monkeypatch.setattr(stats_module, "get_settings", lambda: settings)
    # No cache manager, so get_stats_config goes straight to _process_stats.
    monkeypatch.setattr(stats_module, "get_feature_manager", lambda name: None)

    instance = StatsManager()
    instance.set_csv_loader(
        lambda path: pd.DataFrame(
            CSV_ROWS, columns=["schema_name", "table_name", "stats_column_name"]
        )
    )
    return instance


def test_configured_tables_are_keyed_without_the_prefix(manager):
    """`Series.replace` is whole-value equality, so the prefix survived."""
    config = manager.get_stats_config("bronze")

    assert sorted(config.get_tables()) == ["customer", "order"]


def test_stats_columns_resolve_for_a_prefixed_table(manager):
    assert manager.get_stats_columns("Customer", "bronze") == ["customerid", "createdon"]


def test_should_create_stats_answers_a_bool(manager):
    """Inherited from the protocol as `...`, it used to answer None."""
    assert manager.should_create_stats("Customer", "bronze") is True
    assert manager.should_create_stats("Unknown", "bronze") is False


def test_get_configured_tables_answers_a_list(manager):
    assert sorted(manager.get_configured_tables("bronze")) == ["customer", "order"]


def test_manager_implements_every_method_the_protocol_declares():
    """The protocol is a base class, so a missing method is not an error --
    it is an inherited `...` that returns None. Only an explicit check finds
    it."""
    declared = {
        name
        for name in vars(StatsProtocol)
        if not name.startswith("_") and callable(getattr(StatsProtocol, name))
    }

    unimplemented = {
        name for name in declared if getattr(StatsManager, name) is getattr(StatsProtocol, name)
    }

    assert unimplemented == set()
