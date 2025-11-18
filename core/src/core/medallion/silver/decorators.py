"""Silver layer decorators for ETL metadata configuration.

This module provides decorators specific to the Silver layer of the medallion
architecture, including both general Silver transformations and dimension table
processing with SCD support.
"""

from typing import Callable, List, Optional, Type, Union

from core.constants.compute import EngineType
from core.types.metadata import SilverMetadata


def silver_metadata(
    sp_name: str,
    group_file_name: str,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    preferred_engine: Union[str, EngineType] = EngineType.SQL,
    disable_key_reshuffling: bool = False,
    disabled: bool = False,
    take_snapshot: bool = False,
    model_name: Optional[str] = None
) -> Callable[[Type], Type]:
    """Decorator for Silver layer ETL classes.
    
    This decorator configures classes that implement Silver layer transformations.
    The Silver layer is responsible for data cleansing, validation, standardization,
    and enrichment. It transforms raw Bronze data into consistent, reliable datasets
    ready for analytics.
    
    Args:
        sp_name: Name of the stored procedure that will be generated. Follow
            naming conventions: "Load_{Entity}_{Type}" where Entity is the
            business object and Type is Dim/Fact/Staging.
        group_file_name: Path to JSON configuration file defining transformation
            rules, groupings, and aggregations. Path is relative to config root.
        description: Human-readable description of the ETL process purpose and
            business value. Used in documentation and monitoring dashboards.
        tags: List of tags for categorizing and filtering ETL processes.
            Use consistent taxonomy: ["layer:silver", "type:dimension", "domain:sales"].
        preferred_engine: Engine preference for all queries in this sequencer. Can be string
            or EngineType enum. Options: SQL (default), SPARK, AUTO. SQL maintains backward
            compatibility. AUTO lets platform analyze query complexity. Default is SQL.
        disabled: If True, this transformation won't be executed. Used for client-specific
            features or gradual feature rollout. Default is False (enabled).
        
    Returns:
        Decorated class with SilverMetadata attached as _silver_metadata attribute.
        
    Example:
        Basic dimension ETL:
        >>> @silver_metadata(
        ...     sp_name="Load_Customer_Dim",
        ...     group_file_name="dimensions/customer.json",
        ...     description="Customer dimension with CDC and data quality checks"
        ... )
        ... class CustomerDimensionETL(SilverTransformationSequencer):
        ...     def transform_customers(self):
        ...         # Implement transformation logic
        ...         pass
        
        Fact table ETL with complex grouping:
        >>> @silver_metadata(
        ...     sp_name="Load_Sales_Fact",
        ...     group_file_name="facts/sales_aggregations.json",
        ...     description="Daily sales fact with product and customer dimensions",
        ...     tags=["fact", "sales", "daily", "high-priority"]
        ... )
        ... class SalesFactETL(SilverTransformationSequencer):
        ...     @query_metadata(type=QueryType.INSERT, table_name="SalesFact")
        ...     def aggregate_sales(self):
        ...         return "SELECT ... GROUP BY ..."
        
        Real-time streaming ETL:
        >>> @silver_metadata(
        ...     sp_name="Load_Streaming_Events",
        ...     group_file_name="streaming/event_processor.json",
        ...     description="Near real-time event processing for IoT sensors",
        ...     tags=["streaming", "iot", "real-time"]
        ... )
        ... class StreamingEventETL(SilverTransformationSequencer):
        ...     def process_event_batch(self):
        ...         # Handle micro-batches from streaming source
        ...         pass
    
    Configuration File Format (group_file_name):
        The JSON configuration file should define:
        - Source tables and join conditions
        - Transformation rules and business logic
        - Data quality validations
        - Grouping and aggregation specifications
        - Target table schema and data types
    
    Notes:
        - The decorated class should inherit from SilverTransformationSequencer
        - Stored procedures are auto-generated from class methods
        - Use consistent SP naming for easier maintenance
        - Configuration files enable no-code transformation changes
        - Tag consistently for automated orchestration
    """
    def decorator(cls: Type) -> Type:
        engine_type = EngineType(preferred_engine) if isinstance(preferred_engine, str) else preferred_engine
        final_model_name = model_name if model_name else group_file_name.split('/')[0].replace('group_', '')
 
        metadata = SilverMetadata(
            sp_name=sp_name,
            group_file_name=group_file_name,
            description=description,
            tags=tags or [],
            preferred_engine=engine_type,
            model_name=final_model_name,
            disable_key_reshuffling=disable_key_reshuffling,
            disabled=disabled
        )
        
        cls._silver_metadata = metadata
        return cls
    
    return decorator