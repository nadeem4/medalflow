"""Bronze model whose source table is named differently from the model."""

from medalflow.medallion.bronze import BronzeSequencer, bronze_metadata


@bronze_metadata(
    name="Orders",
    schema="bronze",
    source_system="d365",
    source_table="SalesOrders",
    description="Raw order feed",
)
class Orders(BronzeSequencer):
    """dbo.SalesOrders -> bronze.Orders."""
