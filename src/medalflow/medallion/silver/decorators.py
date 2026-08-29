"""Silver layer decorators for ETL metadata configuration.

This module provides decorators specific to the Silver layer of the medallion
architecture, including both general Silver transformations and dimension table
processing with SCD support.
"""

from collections.abc import Callable

from medalflow.types.metadata import SilverMetadata


def silver_metadata(
    name: str,
    schema: str,
    model: str,
    description: str | None = None,
    tags: list[str] | None = None,
    disable_key_reshuffling: bool = False,
    disabled: bool = False,
) -> Callable[[type], type]:
    """Decorator for Silver layer ETL classes.

    This decorator configures classes that implement Silver layer transformations.
    The Silver layer is responsible for data cleansing, validation, standardization,
    and enrichment. It transforms raw Bronze data into consistent, reliable datasets
    ready for analytics.

    Args:
        name: Identity of this transformation. It is the name the plan reports,
            the key discovery indexes on, and the name of the stored procedure
            generated for it.
        schema: Target schema this transformation writes into. It is also the
            default `schema_name` for the class's own `@query_metadata`
            methods, so a method that omits one lands here. Silver's target
            schema used to live only on those methods, which made it the one
            layer whose declaration did not say where the model writes.
        model: The model this transformation belongs to. Discovery filters on it
            against the configured model list, so a transformation whose model is
            not configured is skipped. It was previously back-derived from a
            filename; it is now declared.
        description: Human-readable description of the ETL process purpose and
            business value. Used in documentation and monitoring dashboards.
        tags: List of tags for categorizing and filtering ETL processes.
            Use consistent taxonomy: ["layer:silver", "type:dimension", "domain:sales"].
        disable_key_reshuffling: Carried on the metadata for downstream key
            handling; the framework itself does not act on it.
        disabled: If True, this transformation won't be executed. Used for client-specific
            features or gradual feature rollout. Default is False (enabled).

    Returns:
        Decorated class with SilverMetadata attached as _silver_metadata attribute.

    Example:
        Basic dimension ETL:
        >>> @silver_metadata(
        ...     name="Load_Customer_Dim",
        ...     schema="silver",
        ...     model="sales",
        ...     description="Customer dimension with CDC and data quality checks"
        ... )
        ... class CustomerDimensionETL(SilverTransformationSequencer):
        ...     def transform_customers(self):
        ...         # Implement transformation logic
        ...         pass

        Fact table ETL:
        >>> @silver_metadata(
        ...     name="Load_Sales_Fact",
        ...     schema="silver",
        ...     model="sales",
        ...     description="Daily sales fact with product and customer dimensions",
        ...     tags=["fact", "sales", "daily", "high-priority"]
        ... )
        ... class SalesFactETL(SilverTransformationSequencer):
        ...     @query_metadata(type=QueryType.INSERT, table_name="SalesFact")
        ...     def aggregate_sales(self):
        ...         return "SELECT ... GROUP BY ..."

    Notes:
        - The decorated class should inherit from SilverTransformationSequencer
        - Stored procedures are auto-generated from class methods
        - Use consistent naming for easier maintenance
        - Tag consistently for automated orchestration
    """

    def decorator(cls: type) -> type:
        metadata = SilverMetadata(
            name=name,
            schema=schema,
            model=model,
            description=description,
            tags=tags or [],
            disable_key_reshuffling=disable_key_reshuffling,
            disabled=disabled,
        )

        cls._silver_metadata = metadata
        return cls

    return decorator
