"""Silver model reading another silver model — the silver -> silver edge."""

from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.silver import SilverTransformationSequencer, silver_metadata


@silver_metadata(
    name="FactOrders",
    schema="silver",
    model="sales",
    description="Order facts joined to the customer dimension",
)
class FactOrders(SilverTransformationSequencer):
    """silver.DimCustomer -> silver.FactOrders."""

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="FactOrders")
    def build_fact_orders(self) -> str:
        return (
            "SELECT o.OrderId, c.CustomerId "
            "FROM bronze.Orders o "
            "JOIN silver.DimCustomer c ON c.CustomerId = o.CustomerId"
        )
