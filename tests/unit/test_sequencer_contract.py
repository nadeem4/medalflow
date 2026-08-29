"""ADR 002 D5 — the three sequencers take one constructor.

Bronze, silver and gold each had a different constructor: bronze took injected
settings, a source schema and a comma-separated *string* of table names; silver
took a SQL dialect and resolved its own settings; gold took `selected_tables` as
a list and resolved its own settings too. Callers could not treat them
interchangeably, and two of the three reached for global state at construction.

They now share `(settings, selection)` as their first two parameters, in that
order and under those names. Anything a layer genuinely needs beyond that is
keyword-only and comes after.
"""

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.bronze.sequencer import BronzeSequencer
from medalflow.medallion.gold.sequencer import GoldSequencer
from medalflow.medallion.silver.sequencer import SilverTransformationSequencer
from medalflow.settings.main import MedalflowSettings

SEQUENCERS = [BronzeSequencer, SilverTransformationSequencer, GoldSequencer]


def _settings(**compute):
    return MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb", **compute},
    )


@pytest.fixture(autouse=True)
def offline_env(monkeypatch):
    """`_BaseSequencer._init_feature_managers` resolves *global* settings.

    Construction therefore needs a resolvable environment even though every
    sequencer is handed its settings explicitly. That the base still reaches
    for the singleton is a separate D6 leftover, not this decision's business.
    """
    from medalflow.settings import main as settings_main

    from tests.conftest import OFFLINE_ENV

    for key, value in OFFLINE_ENV.items():
        monkeypatch.setenv(key, value)
    settings_main._settings = None
    try:
        yield
    finally:
        settings_main._settings = None


@pytest.fixture(autouse=True)
def no_lake_database(monkeypatch):
    """Nothing here may open a warehouse connection (D6).

    Bronze's `lake_db` is lazy, so merely constructing one must not reach this;
    the test that pins that laziness asserts on the counter.
    """
    from medalflow.medallion.bronze import sequencer as bronze_sequencer

    built = []

    def _explode(settings, schema):
        built.append(schema)
        raise AssertionError("LakeDatabase was constructed")

    monkeypatch.setattr(bronze_sequencer, "LakeDatabase", _explode)
    return built


# --- the shared shape ------------------------------------------------------


@pytest.mark.parametrize("sequencer_class", SEQUENCERS, ids=lambda c: c.__name__)
def test_the_first_two_parameters_are_settings_then_selection(sequencer_class):
    import inspect

    parameters = list(inspect.signature(sequencer_class.__init__).parameters)

    assert parameters[:3] == ["self", "settings", "selection"]


@pytest.mark.parametrize("sequencer_class", SEQUENCERS, ids=lambda c: c.__name__)
def test_settings_and_selection_are_accepted_positionally(sequencer_class):
    settings = _settings()

    sequencer = sequencer_class(settings, ["Customer", "Order"])

    assert sequencer.settings is settings
    assert sequencer.selection == ["Customer", "Order"]


@pytest.mark.parametrize("sequencer_class", SEQUENCERS, ids=lambda c: c.__name__)
def test_selection_defaults_to_none(sequencer_class):
    assert sequencer_class(_settings()).selection is None


# --- bronze ----------------------------------------------------------------


def test_bronze_does_not_build_its_lake_database_at_construction(no_lake_database):
    """D6: constructing a sequencer must not require a warehouse."""
    BronzeSequencer(_settings(), ["Customer"])

    assert no_lake_database == []


def test_bronze_builds_its_lake_database_on_first_use_and_caches_it(monkeypatch):
    from medalflow.medallion.bronze import sequencer as bronze_sequencer

    built = []

    class _LakeDatabase:
        def __init__(self, settings, schema):
            built.append(schema)

    monkeypatch.setattr(bronze_sequencer, "LakeDatabase", _LakeDatabase)

    sequencer = BronzeSequencer(_settings(), source_schema="staging")
    first, second = sequencer.lake_db, sequencer.lake_db

    assert built == ["staging"]
    assert first is second


def test_bronze_rejects_the_csv_table_names_parameter():
    with pytest.raises(TypeError, match="table_names"):
        BronzeSequencer(_settings(), table_names="Customer,Order")


def test_bronze_no_longer_parses_csv():
    assert not hasattr(BronzeSequencer, "_parse_table_names")


def test_bronze_source_schema_is_keyword_only():
    with pytest.raises(TypeError):
        BronzeSequencer(_settings(), ["Customer"], "staging")


def test_bronze_source_schema_defaults_to_dbo():
    assert BronzeSequencer(_settings()).source_schema == "dbo"


# --- silver ----------------------------------------------------------------


def test_silver_honours_the_configured_dialect():
    """The `sql_dialect="tsql"` default overwrote the configured dialect.

    `_BaseSequencer.__init__` already sets `self.sql_dialect` from
    `settings.compute.active_config.dialect`; silver then did
    `if sql_dialect: self.sql_dialect = sql_dialect`, which with a defaulted
    argument is always true. A deployment configured for any other dialect
    silently got T-SQL.
    """
    sequencer = SilverTransformationSequencer(_settings(dialect="spark"))

    assert sequencer.sql_dialect == "spark"


def test_silver_rejects_the_sql_dialect_parameter():
    import inspect

    # Asserted on the signature, not only on the TypeError: with `sql_dialect`
    # still the first parameter, the call below raises TypeError anyway --
    # "multiple values for argument" -- and the test would pass for the wrong
    # reason.
    assert "sql_dialect" not in inspect.signature(SilverTransformationSequencer.__init__).parameters

    with pytest.raises(TypeError, match="sql_dialect"):
        SilverTransformationSequencer(_settings(), sql_dialect="spark")


# --- gold ------------------------------------------------------------------


def test_gold_rejects_selected_tables():
    import inspect

    assert "selected_tables" not in inspect.signature(GoldSequencer.__init__).parameters

    with pytest.raises(TypeError, match="selected_tables"):
        GoldSequencer(_settings(), selected_tables=["vw_Revenue"])


def test_gold_selection_of_none_means_every_table():
    """`None` = all, `[]` = nothing. The rename must not collapse the two."""
    assert GoldSequencer(_settings(), None).selection is None
    assert GoldSequencer(_settings(), []).selection == []


# --- selection is one behaviour, not gold's private one --------------------


class _TwoTableSilver(SilverTransformationSequencer):
    """Two decorated methods, so a selection has something to choose between."""

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="DimCustomer", schema_name="silver")
    def build_dim_customer(self) -> str:
        return "SELECT CustomerId FROM bronze.Customers"

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="FactOrders", schema_name="silver")
    def build_fact_orders(self) -> str:
        return "SELECT OrderId FROM bronze.Orders"


class _TwoViewGold(GoldSequencer):
    @query_metadata(
        type=QueryType.CREATE_OR_ALTER_VIEW, table_name="vw_Revenue", schema_name="gold"
    )
    def build_revenue(self) -> str:
        return "SELECT 1 AS Revenue"

    @query_metadata(type=QueryType.CREATE_OR_ALTER_VIEW, table_name="vw_Churn", schema_name="gold")
    def build_churn(self) -> str:
        return "SELECT 1 AS Churn"


SELECTABLE = [
    (_TwoSilver := _TwoTableSilver, ["DimCustomer", "FactOrders"], "FactOrders"),
    (_TwoGold := _TwoViewGold, ["vw_Churn", "vw_Revenue"], "vw_Revenue"),
]


@pytest.mark.parametrize("sequencer_class,all_tables,one_table", SELECTABLE, ids=["silver", "gold"])
def test_selection_of_none_yields_every_operation(sequencer_class, all_tables, one_table):
    operations = sequencer_class(_settings()).get_queries()

    assert sorted(operation.object_name for operation in operations) == all_tables


@pytest.mark.parametrize("sequencer_class,all_tables,one_table", SELECTABLE, ids=["silver", "gold"])
def test_selection_filters_down_to_the_named_tables(sequencer_class, all_tables, one_table):
    """`selection` had to mean the same thing in every layer, or it means nothing.

    Gold owned this filter privately; silver accepted `selection` and discarded
    it, which is the exact "parameter that lied" shape D3 deleted. The filter
    lives on the base now.
    """
    operations = sequencer_class(_settings(), [one_table]).get_queries()

    assert [operation.object_name for operation in operations] == [one_table]


@pytest.mark.parametrize("sequencer_class,all_tables,one_table", SELECTABLE, ids=["silver", "gold"])
def test_an_empty_selection_yields_nothing(sequencer_class, all_tables, one_table):
    """`[]` is a selection of no tables, distinct from `None` meaning all."""
    assert sequencer_class(_settings(), []).get_queries() == []


def test_an_unmatched_selection_warns(caplog):
    with caplog.at_level("WARNING"):
        operations = _TwoTableSilver(_settings(), ["NoSuchTable"]).get_queries()

    assert operations == []
    assert "No methods found for selected tables" in caplog.text


def test_gold_no_longer_owns_a_private_selection_filter():
    """The override is gone; the behaviour it held is the base's now."""
    assert "_get_queries" not in GoldSequencer.__dict__
