"""Gold layer decorators for analytical view configuration.

This module provides decorators specific to the Gold layer of the medallion
architecture, which focuses on business-ready analytical datasets and views.
"""

from typing import Callable, List, Optional, Type

from core.types.metadata import GoldMetadata


def gold_metadata(
    schema_name: str,
    layer: str = "gold",
    description: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> Callable[[Type], Type]:
    """Decorator for Gold layer sequencer classes.
    
    This decorator configures classes that create and manage Gold layer views.
    Gold layer views provide business-ready data for analytics, reporting, and
    data science. The decorator enables automatic view generation, dependency
    tracking, and refresh orchestration.
    
    Args:
        schema_name: Target schema for creating analytical views. This should
            be a dedicated schema for Gold layer objects to maintain clear
            separation between layers.
        layer: Medallion layer identifier. Defaults to "gold" but can be
            customized for specialized analytical layers like "gold_ml" or
            "gold_executive".
        description: Human-readable description of the view collection's purpose
            and content. This appears in data catalogs and documentation.
        tags: List of tags for categorizing and discovering views. Use consistent
            tagging strategies like ["domain:sales", "refresh:daily", "priority:high"].
        
    Returns:
        Decorated class with GoldMetadata attached as _gold_metadata attribute.
        
    Example:
        Basic Gold layer configuration:
        >>> @gold_metadata(
        ...     schema_name="gold",
        ...     description="Core business metrics and KPIs"
        ... )
        ... class BusinessMetricsViews(GoldSequencer):
        ...     def create_views(self):
        ...         return [self.revenue_view(), self.customer_view()]
        
        Domain-specific Gold layer:
        >>> @gold_metadata(
        ...     schema_name="gold_sales",
        ...     description="Sales analytics views for performance tracking",
        ...     tags=["sales", "performance", "daily-refresh"]
        ... )
        ... class SalesAnalyticsViews(GoldSequencer):
        ...     @query_metadata(type=QueryType.CREATE_VIEW, table_name="v_sales_summary")
        ...     def sales_summary_view(self):
        ...         return "CREATE VIEW v_sales_summary AS ..."
        
        ML-ready feature views:
        >>> @gold_metadata(
        ...     schema_name="gold_ml",
        ...     layer="gold_ml",
        ...     description="Feature engineering views for ML models",
        ...     tags=["ml-features", "customer-360", "high-compute"]
        ... )
        ... class MLFeatureViews(GoldSequencer):
        ...     pass
    
    Notes:
        - The decorated class should inherit from GoldSequencer
        - Views created in Gold layer should be optimized for query performance
        - Consider materialized views for frequently accessed aggregations
        - Use consistent naming conventions for views (e.g., v_ prefix)
        - Document business logic and calculations within view definitions
    """
    def decorator(cls: Type) -> Type:
        metadata = GoldMetadata(
            schema_name=schema_name,
            layer=layer,
            description=description,
            tags=tags or []
        )
        
        cls._gold_metadata = metadata
        return cls
    
    return decorator


# Create alias for backward compatibility
view_metadata = gold_metadata