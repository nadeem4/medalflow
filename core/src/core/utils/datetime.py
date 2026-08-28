"""DateTime utilities for snapshot and partition management.

This module provides utilities for working with dates, times, and
partition paths in the data lake.
"""

from datetime import datetime
from typing import Dict, Optional

from core.constants.medallion import SnapshotFrequency


def get_current_timestamp() -> datetime:
    """Get current UTC timestamp.
    
    Returns:
        Current UTC datetime
    """
    return datetime.utcnow()


def get_snapshot_datetime() -> str:
    """Get formatted datetime string for snapshots.
    
    Returns:
        Datetime string in format 'YYYY-MM-DD HH:MM:SS'
    """
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def get_partition_path(
    base_path: str,
    frequency: SnapshotFrequency,
    timestamp: Optional[datetime] = None
) -> str:
    """Generate partition path based on frequency and timestamp.
    
    Args:
        base_path: Base storage path
        frequency: Snapshot frequency
        timestamp: Timestamp to use (defaults to current time)
        
    Returns:
        Formatted partition path
        
    Example:
        >>> path = get_partition_path(
        ...     "silver/inventory",
        ...     SnapshotFrequency.DAILY,
        ...     datetime(2024, 1, 15)
        ... )
        >>> print(path)
        silver/inventory/daily/2024/01/15
    """
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    # Format path based on frequency
    if frequency == SnapshotFrequency.EVERY_RUN:
        path_suffix = timestamp.strftime("every_run/%Y/%m/%d/%H_%M")
    elif frequency == SnapshotFrequency.HOURLY:
        path_suffix = timestamp.strftime("hourly/%Y/%m/%d/%H")
    elif frequency == SnapshotFrequency.DAILY:
        path_suffix = timestamp.strftime("daily/%Y/%m/%d")
    elif frequency == SnapshotFrequency.WEEKLY:
        # Get ISO week number
        year, week, _ = timestamp.isocalendar()
        path_suffix = f"weekly/{year}/week_{week:02d}"
    elif frequency == SnapshotFrequency.MONTHLY:
        path_suffix = timestamp.strftime("monthly/%Y/%m")
    elif frequency == SnapshotFrequency.QUARTERLY:
        quarter = (timestamp.month - 1) // 3 + 1
        path_suffix = f"quarterly/{timestamp.year}/q{quarter}"
    elif frequency == SnapshotFrequency.YEARLY:
        path_suffix = f"yearly/{timestamp.year}"
    else:
        # Default to daily
        path_suffix = timestamp.strftime("daily/%Y/%m/%d")
    
    return f"{base_path}/{path_suffix}"


def parse_snapshot_path(path: str) -> Dict[str, str]:
    """Parse snapshot path to extract date components.
    
    Args:
        path: Snapshot path to parse
        
    Returns:
        Dictionary with extracted components (year, month, day, etc.)
        
    Example:
        >>> components = parse_snapshot_path("data/daily/2024/01/15")
        >>> print(components)
        {'frequency': 'daily', 'year': '2024', 'month': '01', 'day': '15'}
    """
    parts = path.split('/')
    components = {}
    
    # Find frequency indicator
    for i, part in enumerate(parts):
        if part in ['every_run', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly']:
            components['frequency'] = part
            
            # Extract date components based on frequency
            if part in ['daily', 'hourly', 'every_run'] and i + 3 < len(parts):
                components['year'] = parts[i + 1]
                components['month'] = parts[i + 2]
                components['day'] = parts[i + 3]
                
                if part == 'hourly' and i + 4 < len(parts):
                    components['hour'] = parts[i + 4]
                elif part == 'every_run' and i + 4 < len(parts):
                    time_parts = parts[i + 4].split('_')
                    if len(time_parts) == 2:
                        components['hour'] = time_parts[0]
                        components['minute'] = time_parts[1]
            
            elif part == 'weekly' and i + 2 < len(parts):
                components['year'] = parts[i + 1]
                week_part = parts[i + 2]
                if week_part.startswith('week_'):
                    components['week'] = week_part.replace('week_', '')
            
            elif part == 'monthly' and i + 2 < len(parts):
                components['year'] = parts[i + 1]
                components['month'] = parts[i + 2]
            
            elif part == 'quarterly' and i + 2 < len(parts):
                components['year'] = parts[i + 1]
                quarter_part = parts[i + 2]
                if quarter_part.startswith('q'):
                    components['quarter'] = quarter_part[1:]
            
            elif part == 'yearly' and i + 1 < len(parts):
                components['year'] = parts[i + 1]
            
            break
    
    return components
