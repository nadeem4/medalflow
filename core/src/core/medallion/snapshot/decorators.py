"""Snapshot layer decorators for point-in-time data capture configuration.

This module provides decorators specific to the Snapshot layer of the medallion
architecture, which focuses on preserving historical states of data for
compliance, auditing, and temporal analysis.
"""

from typing import Callable, List, Optional, Type

from core.constants.medallion import SnapshotFrequency
from core.types.metadata import SnapshotMetadata


def snapshot_metadata(
    schema_name: str,
    retention_days: int = 90,
    compression: bool = True,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    frequency: SnapshotFrequency = SnapshotFrequency.DAILY
) -> Callable[[Type], Type]:
    """Decorator for Snapshot layer sequencer classes.
    
    This decorator configures classes that manage point-in-time data captures.
    Snapshots preserve historical states for compliance, auditing, and time-series
    analysis. The decorator defines retention policies, storage optimization, and
    capture schedules.
    
    Args:
        schema_name: Target schema for snapshot tables. Should be separate from
            operational schemas to manage retention and permissions independently.
        retention_days: How long to retain snapshots before automatic deletion.
            Set to -1 for indefinite retention. Consider compliance requirements
            and storage costs. Default is 90 days.
        compression: Whether to compress snapshot data. Highly recommended for
            data older than 30 days to reduce storage costs. Uses Parquet
            compression. Default is True.
        description: Description of what data is being snapshotted and why.
            Include business justification and usage patterns.
        tags: Categorization tags for snapshot management and discovery.
            Examples: ["compliance:sox", "frequency:daily", "domain:finance"].
        frequency: How often snapshots are captured - DAILY, WEEKLY, or MONTHLY.
            Affects storage planning and query patterns. Default is DAILY.
        
    Returns:
        Decorated class with SnapshotMetadata attached as _snapshot_metadata attribute.
        
    Example:
        Daily operational snapshots:
        >>> @snapshot_metadata(
        ...     schema_name="snapshot_ops",
        ...     retention_days=90,
        ...     frequency=SnapshotFrequency.DAILY,
        ...     description="Daily operational state for trend analysis"
        ... )
        ... class OperationalSnapshots(SnapshotSequencer):
        ...     def snapshot_tables(self):
        ...         return ["inventory", "orders", "shipments"]
        
        Compliance snapshots with long retention:
        >>> @snapshot_metadata(
        ...     schema_name="snapshot_compliance",
        ...     retention_days=365 * 7,  # 7 years
        ...     compression=True,
        ...     frequency=SnapshotFrequency.MONTHLY,
        ...     description="Month-end financial positions for SOX compliance",
        ...     tags=["compliance", "sox", "financial-close"]
        ... )
        ... class FinancialSnapshots(SnapshotSequencer):
        ...     @query_metadata(type=QueryType.CREATE_TABLE, table_name="balance_sheet_snapshot")
        ...     def snapshot_balance_sheet(self):
        ...         return "CREATE TABLE ... AS SELECT * FROM gold.balance_sheet"
        
        High-frequency trading snapshots:
        >>> @snapshot_metadata(
        ...     schema_name="snapshot_trading",
        ...     retention_days=7,  # Short retention for high volume
        ...     compression=False,  # Keep uncompressed for fast access
        ...     frequency=SnapshotFrequency.DAILY,
        ...     description="Intraday position snapshots",
        ...     tags=["trading", "positions", "high-frequency"]
        ... )
        ... class TradingSnapshots(SnapshotSequencer):
        ...     pass
    
    Notes:
        - Snapshot tables are typically partitioned by snapshot date
        - Consider incremental snapshots for large tables to save storage
        - Implement cleanup jobs to enforce retention policies
        - Use consistent naming: {table_name}_snapshot_{YYYYMMDD}
        - Monitor storage growth and adjust retention/compression as needed
    """
    def decorator(cls: Type) -> Type:
        metadata = SnapshotMetadata(
            schema_name=schema_name,
            retention_days=retention_days,
            compression=compression,
            description=description,
            tags=tags or [],
            frequency=frequency
        )
        
        cls._snapshot_metadata = metadata
        return cls
    
    return decorator