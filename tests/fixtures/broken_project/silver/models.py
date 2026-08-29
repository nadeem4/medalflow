"""One healthy model, and three ways an author breaks one.

Each break fails in a different place, and all three must reach the caller
from a single `compile()`:

* `Raises` -- a `@query_metadata` method whose own code raises.
* `NoTable` -- a `@query_metadata` that names no table, so no operation can
  be built for it.
* `NotSql` -- a `@query_metadata` method returning something that is not SQL.
"""

from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.silver import SilverTransformationSequencer, silver_metadata


@silver_metadata(name="Good", schema="silver", model="sales", tags=["healthy"])
class Good(SilverTransformationSequencer):
    """The one model in this project that compiles."""

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="Good")
    def build_good(self) -> str:
        return "SELECT Id FROM bronze.Source"


@silver_metadata(name="Raises", schema="silver", model="sales")
class Raises(SilverTransformationSequencer):
    """A model whose own code raises while its SQL is being read."""

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="Raises")
    def build_raises(self) -> str:
        raise RuntimeError("this model's own code failed")


@silver_metadata(name="NoTable", schema="silver", model="sales")
class NoTable(SilverTransformationSequencer):
    """A `@query_metadata` that names no table to write."""

    @query_metadata(type=QueryType.CREATE_TABLE)
    def build_no_table(self) -> str:
        return "SELECT Id FROM bronze.Source"


@silver_metadata(name="NotSql", schema="silver", model="sales")
class NotSql(SilverTransformationSequencer):
    """A `@query_metadata` method that does not return SQL."""

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="NotSql")
    def build_not_sql(self):
        return 42
