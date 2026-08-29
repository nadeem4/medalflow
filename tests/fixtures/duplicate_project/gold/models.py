"""Two gold models, one name."""

from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.gold import GoldSequencer, gold_metadata


@gold_metadata(name="Revenue", schema="gold")
class RevenueByCustomer(GoldSequencer):
    """The model that declared the name first."""

    @query_metadata(type=QueryType.CREATE_OR_ALTER_VIEW, table_name="vw_RevenueByCustomer")
    def build_view(self) -> str:
        return "SELECT CustomerId FROM silver.FactOrders"


@gold_metadata(name="Revenue", schema="gold")
class RevenueByRegion(GoldSequencer):
    """The model that declared it again."""

    @query_metadata(type=QueryType.CREATE_OR_ALTER_VIEW, table_name="vw_RevenueByRegion")
    def build_view(self) -> str:
        return "SELECT RegionId FROM silver.FactOrders"
