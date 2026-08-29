"""Silver model reading straight from the bronze layer."""

from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.silver import SilverTransformationSequencer, silver_metadata


@silver_metadata(
    name="usp_load_dim_customer",
    schema="silver",
    model="sales",
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
