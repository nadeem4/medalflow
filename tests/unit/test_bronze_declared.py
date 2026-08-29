"""Bronze builds its tables from declared models (ADR 002, Decision 6).

Bronze derived its table list from a live `INFORMATION_SCHEMA` query, so
compiling a plan required a warehouse -- contradicting offline compile (D6) and
making the example project unrunnable without cloud credentials.

Introspection's only contribution was ever a `list[TableInfo]`; everything after
it -- the CTAS, the soft-delete filter, the statistics -- is generated. So that
list is the one seam, `_source_tables()`. `BronzeSequencer` reads it off its own
`@bronze_metadata`, and `IntrospectedBronzeSequencer` overrides it to query the
warehouse, which is the documented alternative rather than the only mode.
"""

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.bronze import IntrospectedBronzeSequencer, bronze_metadata
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
        if not table_names:
            return self.tables
        return [table for table in self.tables if table.table_name in table_names]


def _introspected(settings, selection=None, tables=None, **kwargs):
    sequencer = IntrospectedBronzeSequencer(settings, selection, **kwargs)
    lake_db = _LakeDatabase(
        tables
        if tables is not None
        else [
            TableInfo(table_name="Customers", schema_name="dbo", full_table_name="dbo.Customers"),
            TableInfo(table_name="Orders", schema_name="dbo", full_table_name="dbo.Orders"),
        ]
    )
    # `lake_db` is a cached_property, so seeding the instance dict is the seam.
    sequencer.__dict__["lake_db"] = lake_db
    return sequencer, lake_db


def test_introspection_is_a_bronze_sequencer():
    assert issubclass(IntrospectedBronzeSequencer, BronzeSequencer)


def test_introspection_builds_one_table_per_discovered_source():
    sequencer, _ = _introspected(_settings())

    operations = sequencer.get_queries()

    assert [operation.object_name for operation in operations] == ["Customers", "Orders"]
    assert {operation.schema_name for operation in operations} == {"bronze"}


def test_introspection_passes_the_selection_to_the_warehouse():
    sequencer, lake_db = _introspected(_settings(), ["Orders"])

    operations = sequencer.get_queries()

    assert lake_db.calls == [["Orders"]]
    assert [operation.object_name for operation in operations] == ["Orders"]


def test_an_empty_selection_introspects_nothing():
    """`lake_db.get_tables([])` returns *every* table -- `[]` must mean none."""
    sequencer, lake_db = _introspected(_settings(), [])

    assert sequencer.get_queries() == []
    assert lake_db.calls == []


def test_introspection_writes_to_its_configured_target_schema():
    sequencer, _ = _introspected(_settings(), target_schema="raw")

    assert {operation.schema_name for operation in sequencer.get_queries()} == {"raw"}
