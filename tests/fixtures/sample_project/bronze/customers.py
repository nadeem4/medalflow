"""Bronze model landing a source table, declared rather than introspected."""

from medalflow.medallion.bronze import BronzeSequencer, bronze_metadata


@bronze_metadata(
    name="Customers",
    schema="bronze",
    source_system="d365",
    description="Raw customer feed",
)
class Customers(BronzeSequencer):
    """dbo.Customers -> bronze.Customers."""
