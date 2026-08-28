"""SQL injection safety for generated statements (Phase 3, task 1).

Every string literal the Synapse builder emits used to be interpolated raw:
`quote_string` existed but was never reached on the live path, and
`format_set_clause` routed anything containing `+ - * / (` through an
"is this an expression?" heuristic that inlined it verbatim -- so a surname
like `O'Brien-Smith` both skipped escaping and broke the quoting.

The rules these tests pin down:

* Values are escaped (`'` doubled).
* Identifiers are bracket-quoted -- `order_by` included, not just `columns`.
* `location` is a path, and is validated as one.
* `data_type` is a type name, and is validated as one.
* Fields that are inherently raw SQL take a bare `str` only when it is
  lexically closed; anything else must say `RawSQL` explicitly, which is
  inlined verbatim.

Entirely offline (Decision D6): the builder is booted from the placeholder
settings in tests/conftest.py, so nothing touches Azure.
"""

import pytest
from medalflow.operations import CreateTable, Select
from medalflow.operations.columns import ColumnDefinition
from medalflow.types import RawSQL
from pydantic import ValidationError


@pytest.fixture
def query_builder(offline_settings):
    from medalflow.query_builder.factory import create_query_builder

    return create_query_builder()


# --- 1. string literals are escaped ---------------------------------------


def test_literal_with_quote_and_hyphen_is_escaped(query_builder):
    """`_is_expression` saw the hyphen, called it an expression and inlined it."""
    assert query_builder.format_set_clause({"name": "O'Brien-Smith"}) == (
        "[name] = 'O''Brien-Smith'"
    )


def test_literal_that_looks_like_a_function_call_is_still_escaped(query_builder):
    """A parenthesis is not consent to inline. Only RawSQL is."""
    assert query_builder.format_set_clause({"note": "GETDATE()"}) == "[note] = 'GETDATE()'"


def test_raw_sql_in_a_set_clause_is_inlined_verbatim(query_builder):
    assert query_builder.format_set_clause({"loaded_at": RawSQL("GETDATE()")}) == (
        "[loaded_at] = GETDATE()"
    )


# --- 2. CreateTable.location is a path ------------------------------------


def test_location_rejects_quote_breakout():
    with pytest.raises(ValidationError):
        CreateTable(
            schema_name="silver",
            object_name="DimCustomer",
            location="x'; DROP EXTERNAL TABLE y --",
            columns=[ColumnDefinition(name="c", data_type="INT")],
        )


def test_location_rejects_parent_traversal():
    with pytest.raises(ValidationError):
        CreateTable(
            schema_name="silver",
            object_name="DimCustomer",
            location="silver/../../etc",
            columns=[ColumnDefinition(name="c", data_type="INT")],
        )


def test_location_accepts_an_ordinary_path():
    operation = CreateTable(
        schema_name="silver",
        object_name="DimCustomer",
        location="silver/Dim-Customer_v2.1/",
        columns=[ColumnDefinition(name="c", data_type="INT")],
    )
    assert operation.location == "silver/Dim-Customer_v2.1/"


def test_location_is_escaped_at_the_emission_site(query_builder):
    """Defence in depth: even a location that skipped validation cannot break out."""
    operation = CreateTable.model_construct(
        schema_name="silver",
        object_name="DimCustomer",
        location="x'; DROP EXTERNAL TABLE y --",
        columns=[ColumnDefinition(name="c", data_type="INT")],
        file_format="parquet",
        select_query=None,
        recreate=False,
    )
    sql = query_builder.build_query(operation)
    assert "LOCATION = 'x''; DROP EXTERNAL TABLE y --'" in sql


# --- 3. free-text clauses -------------------------------------------------


@pytest.mark.parametrize("field", ["where_clause", "join_clause", "having_clause"])
def test_free_text_clause_rejects_quote_breakout(field):
    with pytest.raises(ValidationError):
        Select(
            schema_name="bronze",
            object_name="Customers",
            **{field: "x'; DROP EXTERNAL TABLE y --"},
        )


def test_free_text_clause_still_accepts_a_balanced_string(query_builder):
    operation = Select(
        schema_name="bronze",
        object_name="Customers",
        where_clause="Country = 'IE'",
    )
    assert query_builder.build_query(operation) == (
        "SELECT * FROM [bronze].[fin_Customers] WHERE Country = 'IE'"
    )


# --- 4. order_by is an identifier list, like columns and group_by ---------


def test_order_by_is_quoted(query_builder):
    operation = Select(
        schema_name="bronze",
        object_name="Customers",
        order_by=["Name", "CustomerId DESC"],
    )
    assert query_builder.build_query(operation) == (
        "SELECT * FROM [bronze].[fin_Customers] ORDER BY [Name], [CustomerId] DESC"
    )


def test_order_by_is_quoted_on_the_top_path_too(query_builder):
    """`_build_select` rebuilds the whole query when limit is set without offset."""
    operation = Select(
        schema_name="bronze",
        object_name="Customers",
        order_by=["CustomerId DESC"],
        limit=10,
    )
    assert query_builder.build_query(operation) == (
        "SELECT TOP 10 * FROM [bronze].[fin_Customers] ORDER BY [CustomerId] DESC"
    )


def test_order_by_rejects_an_injected_statement(query_builder):
    operation = Select(
        schema_name="bronze",
        object_name="Customers",
        order_by=["1); DROP EXTERNAL TABLE y --"],
    )
    with pytest.raises(ValueError):
        query_builder.build_query(operation)


# --- 5. data_type is a type name ------------------------------------------


def test_data_type_rejects_a_statement_break():
    with pytest.raises(ValidationError):
        ColumnDefinition(name="c", data_type="INT) WITH (X) --")


@pytest.mark.parametrize(
    "data_type", ["INT", "NVARCHAR(MAX)", "DECIMAL(18,2)", "TIMESTAMP_NTZ", "STRING"]
)
def test_data_type_accepts_ordinary_types(data_type):
    assert ColumnDefinition(name="c", data_type=data_type).data_type == data_type


def test_exotic_data_type_can_be_declared_raw(query_builder):
    column = ColumnDefinition(name="tags", data_type=RawSQL("ARRAY<STRING>"))
    assert query_builder.format_column_definitions([column]) == "[tags] ARRAY<STRING>"


# --- 6. RawSQL passthrough ------------------------------------------------


def test_raw_sql_where_clause_is_inlined_verbatim(query_builder):
    operation = Select(
        schema_name="bronze",
        object_name="Customers",
        where_clause=RawSQL("Name = 'O''Brien' AND Amount > 0"),
    )
    assert query_builder.build_query(operation) == (
        "SELECT * FROM [bronze].[fin_Customers] WHERE Name = 'O''Brien' AND Amount > 0"
    )


def test_raw_sql_survives_a_model_dump_round_trip():
    operation = Select(
        schema_name="bronze",
        object_name="Customers",
        where_clause=RawSQL("1 = 1"),
    )
    rebuilt = Select(**operation.to_dict())
    assert isinstance(rebuilt.where_clause, RawSQL)
    assert str(rebuilt.where_clause) == "1 = 1"
