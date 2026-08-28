"""Statistics configuration settings.

This module provides configuration settings for database statistics
management across compute platforms. The actual statistics logic is
handled by the StatsManager feature plugin.
"""

from pydantic import BaseModel, Field


class StatsSettings(BaseModel):
    """Statistics configuration settings.
    
    This class contains only configuration data for statistics management.
    The actual logic for determining when and how to create statistics
    is handled by the StatsManager feature plugin in medalflow.core.features.
    
    Configuration includes:
    - Path to CSV file containing detailed stats configuration
    """

    stats_csv_path: str = Field(
        default="client_configuration/external_table_stats.csv",
        description="Path to stats configuration CSV in Internal ADLS"
    )
