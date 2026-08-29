"""Gold model reading a silver model — the silver -> gold edge."""

from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.gold import GoldSequencer, gold_metadata


@gold_metadata(schema="gold", description="Revenue reporting view")
class Revenue(GoldSequencer):
    """silver.FactOrders -> gold.vw_Revenue."""

    @query_metadata(
        type=QueryType.CREATE_OR_ALTER_VIEW,
        table_name="vw_Revenue",
        schema_name="gold",
    )
    def build_revenue_view(self) -> str:
        return "SELECT CustomerId, COUNT(*) AS Orders FROM silver.FactOrders GROUP BY CustomerId"
