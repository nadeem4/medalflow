"""Naming conventions must be opt-in, and off until they are opted into.

Four of this client's naming conventions were compiled into the sequencers:
a soft-delete predicate on every bronze SELECT, an enum lookup table, a
temp-Detail-to-silver rewrite that changed both the schema and the table name
under the caller, and eleven name-pattern rules that decided how NULLs were
filled. None of them was configurable and none of them announced itself.

A framework does not silently rewrite a user's SQL. Every convention below is
a `ConventionsSettings` entry defaulting to off; these tests pin both halves --
the convention firing when configured, and staying out of the way when not.
"""

import logging

import pytest
import sqlglot
from medalflow.constants.sql import QueryType
from medalflow.medallion.bronze.sequencer import BronzeSequencer
from medalflow.medallion.silver.sequencer import SilverTransformationSequencer
from medalflow.medallion.types import TableInfo
from medalflow.settings.main import MedalflowSettings
from medalflow.types.metadata import QueryMetadata


def _settings(**conventions):
    return MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb"},
        conventions=conventions,
    )


def _bronze(settings):
    """A BronzeSequencer without its LakeDatabase -- offline per D6."""
    sequencer = BronzeSequencer.__new__(BronzeSequencer)
    sequencer.settings = settings
    sequencer.source_schema = "dbo"
    return sequencer


def _silver(settings):
    sequencer = SilverTransformationSequencer.__new__(SilverTransformationSequencer)
    sequencer.settings = settings
    sequencer.sql_dialect = "tsql"
    sequencer.logger = logging.getLogger("test-conventions")
    return sequencer


def _table(name):
    return TableInfo(table_name=name, schema_name="dbo", full_table_name=f"dbo.{name}")


@pytest.fixture(autouse=True)
def offline_query_builder(monkeypatch):
    """`create_query_builder` resolves live settings; keep that offline (D6)."""
    import medalflow.settings

    monkeypatch.setattr(medalflow.settings, "get_settings", lambda: _settings())


# --- defaults --------------------------------------------------------------


def test_every_convention_is_off_by_default():
    conventions = _settings().conventions

    assert conventions.soft_delete is None
    assert conventions.enum_table is None
    assert conventions.detail_tables is None
    assert conventions.null_handling == []


# --- soft delete -----------------------------------------------------------


def test_bronze_select_carries_no_filter_when_soft_delete_is_unconfigured():
    operation = _bronze(_settings())._create_select_operation(_table("Customer"))

    assert operation.where_clause is None


def test_bronze_select_applies_the_configured_soft_delete_predicate():
    settings = _settings(soft_delete={"predicate": "IsDelete IS NULL"})

    operation = _bronze(settings)._create_select_operation(_table("Customer"))

    assert operation.where_clause == "IsDelete IS NULL"


def test_bronze_select_exempts_the_configured_table_suffixes():
    settings = _settings(
        soft_delete={"predicate": "IsDelete IS NULL", "exempt_table_suffixes": ["Metadata"]}
    )
    sequencer = _bronze(settings)

    assert sequencer._create_select_operation(_table("CustomerMetadata")).where_clause is None
    assert sequencer._create_select_operation(_table("Customer")).where_clause is not None


# --- enum table ------------------------------------------------------------


ENUM_CONVENTION = {
    "schema_name": "bronze",
    "table_name": "Enumeration",
    "name_column": "Enum",
    "value_column": "EnumValue",
    "value_id_column": "EnumValueID",
}


def _enum_metadata(filter_value="StatusEnum"):
    return QueryMetadata(type=QueryType.SELECT, table_name="Status", filter=filter_value)


def test_enum_query_without_the_convention_fails_loudly():
    """Absent config the feature is unavailable, not silently broken SQL."""
    sequencer = _silver(_settings())

    with pytest.raises(ValueError, match="ENUM_TABLE"):
        sequencer._generate_enum_query(_enum_metadata())


def test_enum_query_reads_the_configured_table_and_columns():
    settings = _settings(
        enum_table={
            "schema_name": "reference",
            "table_name": "CodeList",
            "name_column": "CodeSet",
            "value_column": "CodeText",
            "value_id_column": "CodeId",
        }
    )

    sql = _silver(settings)._generate_enum_query(_enum_metadata())

    assert "[reference].[fin_CodeList]" in sql
    assert "CodeText AS Status" in sql
    assert "CodeId AS StatusID" in sql
    assert "WHERE CodeSet = 'StatusEnum'" in sql


def test_enum_name_is_escaped_not_interpolated():
    """`filter` reaches the WHERE clause as a literal; a quote in it must not
    close the literal early."""
    sql = _silver(_settings(enum_table=ENUM_CONVENTION))._generate_enum_query(
        _enum_metadata("O'Brien' OR 1=1 --")
    )

    assert "WHERE Enum = 'O''Brien'' OR 1=1 --'" in sql
    assert sqlglot.parse_one(sql, dialect="tsql") is not None


# --- detail table rewriting ------------------------------------------------


def _detail_metadata(table_name="OrderDetail", schema_name="temp"):
    return QueryMetadata(
        type=QueryType.CREATE_TABLE, table_name=table_name, schema_name=schema_name
    )


def test_detail_rewrite_does_not_fire_when_unconfigured():
    sequencer = _silver(_settings())

    sql, metadata = sequencer._transform_query_result("SELECT 1", _detail_metadata())

    assert sql == "SELECT 1"
    assert metadata.schema_name == "temp"
    assert metadata.table_name == "OrderDetail"


def test_detail_rewrite_strips_the_suffix_rather_than_every_occurrence():
    """`replace("Detail", "")` turned DetailOrderDetail into Order. Only the
    trailing suffix is the convention."""
    settings = _settings(detail_tables={"table_suffix": "Detail"})

    _, metadata = _silver(settings)._transform_query_result(
        "SELECT 1 AS Id", _detail_metadata("DetailOrderDetail")
    )

    assert metadata.schema_name == "silver"
    assert metadata.table_name == "DetailOrder"


def test_detail_rewrite_respects_the_configured_schemas():
    settings = _settings(
        detail_tables={
            "table_suffix": "_stg",
            "source_schema": "staging",
            "target_schema": "curated",
        }
    )
    sequencer = _silver(settings)

    _, matched = sequencer._transform_query_result(
        "SELECT 1 AS Id", _detail_metadata("Order_stg", "staging")
    )
    _, unmatched = sequencer._transform_query_result(
        "SELECT 1 AS Id", _detail_metadata("Order_stg", "temp")
    )

    assert (matched.schema_name, matched.table_name) == ("curated", "Order")
    assert (unmatched.schema_name, unmatched.table_name) == ("temp", "Order_stg")


# --- null handling ---------------------------------------------------------


def test_no_null_handling_rules_means_no_null_handling():
    sequencer = _silver(_settings())

    sql = sequencer.transform_detail_to_silver("SELECT CustomerKey FROM temp.Stage")

    assert "ISNULL" not in sql
    assert "NULL AS [CustomerKey]" in sql


def test_null_handling_rules_apply_in_declared_order():
    """The first matching rule wins, so a narrow suffix declared ahead of a
    broad one is how KeyID gets -1 and a plain ID gets ''."""
    settings = _settings(
        null_handling=[
            {"match": "suffix", "columns": ["Key", "KeyID"], "default": "-1"},
            {"match": "suffix", "columns": ["ID"], "default": "''"},
        ]
    )
    sequencer = _silver(settings)

    assert sequencer._get_null_handling("CustomerKeyID", None) == (
        "ISNULL([CustomerKeyID], -1)",
        "-1",
    )
    assert sequencer._get_null_handling("CustomerID", None) == ("ISNULL([CustomerID], '')", "''")


def test_unwrapped_rule_leaves_the_column_alone():
    settings = _settings(
        null_handling=[
            {
                "match": "exact",
                "columns": ["_SourceDate"],
                "default": "CURRENT_TIMESTAMP",
                "wrap": False,
            }
        ]
    )

    assert _silver(settings)._get_null_handling("_SourceDate", None) == (
        "[_SourceDate]",
        "CURRENT_TIMESTAMP",
    )


def test_existing_coalesce_is_never_double_wrapped():
    """Structural, not a convention: a column the author already defaulted
    keeps that default whether or not any rule is configured."""
    select = sqlglot.parse_one("SELECT COALESCE(Amount, 0) AS Amount FROM t", dialect="tsql")
    expression = select.expressions[0].this

    null_expr, default = _silver(_settings())._get_null_handling("Amount", expression)

    assert null_expr == "[Amount]"
    assert default == "0"
