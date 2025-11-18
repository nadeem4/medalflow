"""DateTime utilities for snapshot and partition management.

This module provides utilities for working with dates, times, and
partition paths in the data lake.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

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


def get_ds_prefix() -> str:
    """Get data source prefix from settings.
    
    This function maintains backward compatibility with the original
    utility function.
    
    Returns:
        Data source prefix from settings
    """
    # Lazy import to avoid circular dependency
    from core.settings import get_settings
    settings = get_settings()
    if settings.datasource and settings.datasource.environment:
        return settings.datasource.environment.lower()
    return "default"


def get_env(key: str, default: Optional[str] = None) -> str:
    """Get configuration value from settings.
    
    This function is deprecated. Use get_settings() directly instead.
    
    Args:
        key: Configuration key name
        default: Default value if not set
        
    Returns:
        Configuration value
        
    Raises:
        ValueError: If variable not set and no default provided
    """
    # This function is deprecated - direct environment access should be through settings
    # For backward compatibility, we still support it but log a warning
    import warnings
    warnings.warn(
        "get_env() is deprecated. Use get_settings() to access configuration values.",
        DeprecationWarning,
        stacklevel=2
    )
    
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Environment variable {key} not set")
    return value


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


def get_date_range_for_frequency(
    frequency: SnapshotFrequency,
    reference_date: Optional[datetime] = None
) -> Tuple[datetime, datetime]:
    """Get date range for a given frequency.
    
    Args:
        frequency: Snapshot frequency
        reference_date: Reference date (defaults to current date)
        
    Returns:
        Tuple of (start_date, end_date) for the period
        
    Example:
        >>> start, end = get_date_range_for_frequency(
        ...     SnapshotFrequency.WEEKLY,
        ...     datetime(2024, 1, 15)
        ... )
    """
    if reference_date is None:
        reference_date = datetime.utcnow()
    
    if frequency == SnapshotFrequency.HOURLY:
        start = reference_date.replace(minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=1)
    
    elif frequency == SnapshotFrequency.DAILY:
        start = reference_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    
    elif frequency == SnapshotFrequency.WEEKLY:
        # Start from Monday
        days_since_monday = reference_date.weekday()
        start = reference_date - timedelta(days=days_since_monday)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(weeks=1)
    
    elif frequency == SnapshotFrequency.MONTHLY:
        start = reference_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Get first day of next month
        if reference_date.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
    
    elif frequency == SnapshotFrequency.QUARTERLY:
        quarter = (reference_date.month - 1) // 3
        start_month = quarter * 3 + 1
        start = reference_date.replace(
            month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        # Add 3 months
        end_month = start_month + 3
        if end_month > 12:
            end = start.replace(year=start.year + 1, month=end_month - 12)
        else:
            end = start.replace(month=end_month)
    
    elif frequency == SnapshotFrequency.YEARLY:
        start = reference_date.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        end = start.replace(year=start.year + 1)
    
    else:  # EVERY_RUN or default
        start = reference_date
        end = reference_date
    
    return start, end


def format_datetime_for_sql(dt: datetime) -> str:
    """Format datetime for SQL queries.
    
    Args:
        dt: Datetime to format
        
    Returns:
        SQL-compatible datetime string
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_sql_datetime(dt_str: str) -> datetime:
    """Parse SQL datetime string.
    
    Args:
        dt_str: Datetime string from SQL
        
    Returns:
        Parsed datetime object
    """
    # Try common SQL datetime formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Could not parse datetime string: {dt_str}")