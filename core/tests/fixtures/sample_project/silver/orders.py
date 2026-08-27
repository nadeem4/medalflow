"""Silver model reading another silver model — the silver -> silver edge."""

from core.constants.sql import QueryType
from core.medallion.base.decorators import query_metadata
from core.medallion.silver import SilverTransformationSequencer, silver_metadata


@silver_metadata(
    sp_name="usp_load_fact_orders",
    group_file_name="group_sales/orders.json",
    model_name="sales",
    description="Order facts joined to the customer dimension",
)
class FactOrders(SilverTransformationSequencer):
    """silver.DimCustomer -> silver.FactOrders."""

    @query_metadata(
        type=QueryType.CREATE_TABLE,
        table_name="FactOrders",
        schema_name="silver",
    )
    def build_fact_orders(self) -> str:
        return (
            "SELECT o.OrderId, c.CustomerId "
            "FROM bronze.Orders o "
            "JOIN silver.DimCustomer c ON c.CustomerId = o.CustomerId"
        )
