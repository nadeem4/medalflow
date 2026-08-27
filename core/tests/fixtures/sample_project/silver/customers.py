"""Silver model reading straight from the bronze layer."""

from core.constants.sql import QueryType
from core.medallion.base.decorators import query_metadata
from core.medallion.silver import SilverTransformationSequencer, silver_metadata


@silver_metadata(
    sp_name="usp_load_dim_customer",
    group_file_name="group_sales/customers.json",
    model_name="sales",
    description="Cleansed customer dimension",
)
class DimCustomer(SilverTransformationSequencer):
    """bronze.Customers -> silver.DimCustomer."""

    @query_metadata(
        type=QueryType.CREATE_TABLE,
        table_name="DimCustomer",
        schema_name="silver",
    )
    def build_dim_customer(self) -> str:
        return "SELECT CustomerId, Name FROM bronze.Customers"
