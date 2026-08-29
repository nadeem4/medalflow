"""Bronze builds its tables from declared models (ADR 002, Decision 6).

Bronze derived its table list from a live `INFORMATION_SCHEMA` query, so
compiling a plan required a warehouse -- contradicting offline compile (D6) and
making the example project unrunnable without cloud credentials.

Introspection's only contribution was ever a `list[TableInfo]`; everything after
it -- the CTAS, the soft-delete filter, the statistics -- is generated. So that
list is the one seam, and it moved to where the question belongs:
`IntrospectedBronzeDiscovery` asks the warehouse which models exist and derives
a `@bronze_metadata` declaration for each table it finds. Both modes therefore
produce the same records, and everything downstream -- selectors, `compile()`,
`run()` -- cannot tell them apart.
"""

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.bronze import bronze_metadata
from medalflow.medallion.bronze.metadata_discovery import IntrospectedBronzeDiscovery
from medalflow.medallion.bronze.sequencer import BronzeSequencer
from medalflow.medallion.types import TableInfo
from medalflow.settings.main import MedalflowSettings


def _settings(**conventions):
    return MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb"},
        conventions=conventions,
    )


@pytest.fixture(autouse=True)
def offline_env(monkeypatch):
    """`_BaseSequencer._init_feature_managers` resolves the settings singleton."""
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
    """A declared model reaches no warehouse at all (D6).

    Bronze's `lake_db` was already lazy, so construction stayed offline; this
    explodes on *use*, which is what `get_queries` used to do.
    """

    def _explode(settings, schema):
        raise AssertionError("LakeDatabase was constructed")

    monkeypatch.setattr("medalflow.medallion.bronze.sequencer.LakeDatabase", _explode)


@bronze_metadata(name="Customers", schema="bronze", source_system="d365")
class Customers(BronzeSequencer):
    """The whole of a declared bronze model."""


# --- a declared model compiles offline -------------------------------------


def test_a_declared_model_builds_its_table_without_a_warehouse():
    operations = Customers(_settings()).get_queries()

    assert len(operations) == 1
    assert operations[0].operation_type == QueryType.CREATE_TABLE
    assert operations[0].object_name == "Customers"


def test_the_declared_schema_is_the_write_target():
    """`schema_name="bronze"` was a literal at sequencer.py:100."""

    @bronze_metadata(name="Customers", schema="raw", source_system="d365")
    class RawCustomers(BronzeSequencer):
        pass

    assert RawCustomers(_settings()).get_queries()[0].schema_name == "raw"


def test_the_ctas_reads_the_declared_source():
    """The whole generated SELECT, not a substring of it.

    `_create_select_operation` passed `columns=["*"]`, and the builder
    validates every named column against an identifier whitelist -- so bronze
    raised `Invalid identifier name: *` before rendering anything. No test
    caught it because rendering was only reachable behind a live warehouse.
    """
    operation = Customers(_settings()).get_queries()[0]

    # `dbo` is a source schema, so `skip_prefix_on_schema` leaves it unprefixed
    # -- the source system named the table, not this deployment (ADR 000).
    assert operation.select_query == "SELECT * FROM [dbo].[Customers]"


def test_source_table_may_differ_from_the_model_name():
    @bronze_metadata(
        name="Customers", schema="bronze", source_system="d365", source_table="CUSTTABLE"
    )
    class RenamedSource(BronzeSequencer):
        pass

    operation = RenamedSource(_settings()).get_queries()[0]

    assert operation.object_name == "Customers"
    assert "CUSTTABLE" in operation.select_query


def test_source_schema_falls_back_to_the_sequencers_own():
    operation = Customers(_settings(), source_schema="staging").get_queries()[0]

    assert "[staging].[fin_Customers]" in operation.select_query


def test_a_declared_source_schema_wins():
    @bronze_metadata(
        name="Customers", schema="bronze", source_system="d365", source_schema="landing"
    )
    class Landed(BronzeSequencer):
        pass

    operation = Landed(_settings(), source_schema="staging").get_queries()[0]

    assert "[landing].[fin_Customers]" in operation.select_query


def test_the_conventions_still_apply_to_a_declared_source():
    settings = _settings(soft_delete={"predicate": "IsDelete IS NULL"})

    assert "IsDelete IS NULL" in Customers(settings).get_queries()[0].select_query


# --- selection filters a declared model too --------------------------------


def test_selection_of_none_yields_the_models_table():
    assert len(Customers(_settings(), None).get_queries()) == 1


def test_selection_naming_the_model_yields_its_table():
    assert len(Customers(_settings(), ["Customers"]).get_queries()) == 1


def test_selection_naming_another_model_yields_nothing():
    assert Customers(_settings(), ["Orders"]).get_queries() == []


def test_an_empty_selection_yields_nothing():
    """`None` = all, `[]` = nothing, the same as every other layer."""
    assert Customers(_settings(), []).get_queries() == []


# --- an undecorated sequencer says so --------------------------------------


def test_an_undecorated_bronze_sequencer_names_the_decorator():
    with pytest.raises(ValueError, match="bronze_metadata"):
        BronzeSequencer(_settings()).get_queries()


# --- the metadata carried into the plan ------------------------------------


def test_the_plan_reports_the_declared_name_not_the_class_name():
    @bronze_metadata(name="Customers", schema="bronze", source_system="d365")
    class AnyClassName(BronzeSequencer):
        pass

    assert AnyClassName(_settings()).get_obj_name() == "Customers"


def test_query_metadata_schema_name_is_a_string():
    """It was `self.layer`, a `Layer` member, in a `str` field -- which only
    validated because `Layer` subclasses `str`. It is the write target now,
    which is the declared schema rather than the layer's name."""
    from medalflow.constants.medallion import Layer

    metadata = Customers(_settings()).get_queries()[0].metadata

    assert not isinstance(metadata.schema_name, Layer)
    assert metadata.schema_name == "bronze"


# --- introspection, the opt-in alternative ---------------------------------


class _LakeDatabase:
    """Stands in for the warehouse; records how it was asked."""

    def __init__(self, tables):
        self.tables = tables
        self.calls = []

    def get_tables(self, table_names=None):
        self.calls.append(table_names)
        return self.tables


def _introspected(settings, tables=None, **kwargs):
    """An introspecting discovery whose warehouse is faked."""
    lake_db = _LakeDatabase(
        tables
        if tables is not None
        else [
            TableInfo(table_name="Customers", schema_name="dbo", full_table_name="dbo.Customers"),
            TableInfo(table_name="Orders", schema_name="dbo", full_table_name="dbo.Orders"),
        ]
    )
    return IntrospectedBronzeDiscovery(settings=settings, **kwargs), lake_db


@pytest.fixture(autouse=True)
def _fake_lake(monkeypatch):
    """Route the discovery's `LakeDatabase` at whatever `_introspected` seeded."""
    monkeypatch.setattr(
        "medalflow.medallion.bronze.metadata_discovery.LakeDatabase",
        lambda settings, schema="dbo": _CURRENT["lake_db"],
    )


_CURRENT: dict = {}


def test_an_introspected_table_becomes_a_declared_model():
    """The design decision: introspection answers 'which models exist', and
    each answer is an ordinary bronze model. A selector can then match it."""
    discovery, _CURRENT["lake_db"] = _introspected(_settings())

    discovered = discovery.discover_all()

    assert [model.name for model in discovered] == ["Customers", "Orders"]
    assert all(issubclass(model.sequencer_class, BronzeSequencer) for model in discovered)


def test_introspection_builds_one_table_per_discovered_source():
    discovery, _CURRENT["lake_db"] = _introspected(_settings())

    operations = [
        operation
        for model in discovery.discover_all()
        for operation in model.sequencer_class(_settings()).get_queries()
    ]

    assert [operation.object_name for operation in operations] == ["Customers", "Orders"]
    assert {operation.schema_name for operation in operations} == {"bronze"}


def test_the_source_schema_is_queried_once_and_whole():
    """Narrowing moved to the selector, which is the point: the warehouse is
    asked what exists, not what was wanted. Pushing a selection down to
    `get_tables` would also have meant one query per selected table."""
    discovery, lake_db = _introspected(_settings())
    _CURRENT["lake_db"] = lake_db

    discovery.discover_all()

    assert lake_db.calls == [None]


def test_an_introspected_model_needs_no_further_warehouse_access():
    """The derived declaration carries its own source table, so building the
    operations is offline even in this mode."""
    discovery, _CURRENT["lake_db"] = _introspected(_settings())

    model = discovery.discover_all()[0]

    assert model.bronze_metadata.source_table == "Customers"
    assert model.bronze_metadata.source_schema == "dbo"


def test_introspection_writes_to_its_configured_target_schema():
    discovery, _CURRENT["lake_db"] = _introspected(_settings(), target_schema="raw")

    operations = [
        operation
        for model in discovery.discover_all()
        for operation in model.sequencer_class(_settings()).get_queries()
    ]

    assert {operation.schema_name for operation in operations} == {"raw"}


def test_the_introspecting_sequencer_is_gone():
    """Its whole job -- the INFORMATION_SCHEMA query, the empty selection, the
    target-schema override, source-name-as-target-name -- is discovery's now.
    Leaving the class in place would be a second way to reach the same mode,
    and no way at all to reach it through the public API."""
    import medalflow.medallion
    import medalflow.medallion.bronze

    assert not hasattr(medalflow.medallion, "IntrospectedBronzeSequencer")
    assert not hasattr(medalflow.medallion.bronze, "IntrospectedBronzeSequencer")
