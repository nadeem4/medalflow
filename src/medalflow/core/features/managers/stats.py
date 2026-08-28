"""Statistics feature manager.

This module provides the StatsManager plugin for managing database
statistics configuration and operations across all application layers.

The DataFrame this manager reshapes arrives from an injected loader
(``set_csv_loader``), so pandas is only ever a type name here and is imported
lazily.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from medalflow.core.features import get_feature_manager
from medalflow.core.features.base import FeatureManager
from medalflow.core.features.registry import register_feature
from medalflow.protocols.features import CacheProtocol, StatsProtocol
from medalflow.settings import get_settings

from ...types import StatsConfiguration

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class StatsManager(StatsProtocol, FeatureManager):
    """Statistics configuration manager with business logic.

    Manages database statistics configuration across all layers
    (compute, medallion, datalake, etc.). This manager provides:
    - Table-level statistics configuration
    - Column-specific statistics metadata
    - Cross-layer statistics coordination
    - CSV data loading with dependency injection

    Uses CacheManager for caching and supports dependency injection
    for data loading from Layer 2 components.

    Example:
        >>> stats_mgr = get_feature_manager('stats')
        >>> if stats_mgr:
        >>>     columns = stats_mgr.get_stats_columns('InventTrans', 'bronze')
        >>>     if columns:
        >>>         # Create statistics on columns
        >>>         pass
    """

    def __init__(self):
        """Initialize the stats manager."""
        super().__init__()
        self._csv_loader: Callable[[str], pd.DataFrame] | None = None
        self._initialized = False

    def get_feature_name(self) -> str:
        """Return the feature flag name.

        Returns:
            'stats' - the feature flag that controls this manager
        """
        return "stats"

    def is_available(self) -> bool:
        """Check if stats feature is enabled.

        Returns:
            True if cte_stats_enabled is True in settings
        """
        return self.feature_settings.cte_stats_enabled

    def set_csv_loader(self, loader: Callable[[str], pd.DataFrame]) -> None:
        """Inject CSV loader function (dependency injection).

        This allows Layer 2 components to provide data loading capability
        without creating circular dependencies.

        Args:
            loader: Function that takes a path and returns a DataFrame
        """
        self._csv_loader = loader
        logger.debug("CSV loader injected into StatsManager")

    def initialize(self, config: dict[str, Any] | None = None) -> None:
        """Initialize stats manager.

        Sets up the manager for operation.

        Args:
            config: Optional configuration for initialization
        """
        if self._initialized:
            return

        self._initialized = True

        logger.info("StatsManager initialized successfully")

        if config:
            logger.debug(f"StatsManager initialized with config: {config}")

    @property
    def csv_path(self) -> str:
        """Get the CSV path from settings.

        Returns:
            Path to the statistics configuration CSV file
        """
        settings = get_settings()
        return settings.stats.stats_csv_path

    def get_stats_config(self, schema: str) -> StatsConfiguration | None:
        """Get processed stats configuration for a schema.

        Args:
            schema: Schema name ('bronze', 'silver', 'gold')

        Returns:
            StatsConfiguration object or None
        """
        cache: CacheProtocol | None = get_feature_manager("cache")

        if cache:
            return cache.get(f"stats:{schema}", loader=lambda: self._process_stats(schema))
        return self._process_stats(schema)

    def get_stats_columns(self, table_name: str, layer: str = "bronze") -> list[str] | None:
        """Get statistics columns for a specific table.

        Args:
            table_name: Name of the table
            layer: Data layer ('bronze', 'silver', etc.)

        Returns:
            List of column names if defined, None otherwise
        """
        config = self.get_stats_config(layer)
        if config:
            return config.get_table_columns(table_name.lower())
        return None

    def should_create_stats(self, table_name: str, layer: str = "bronze") -> bool:
        """Check whether statistics are configured for a table.

        Declared by `StatsProtocol`, which `StatsManager` uses as a base
        class -- so until this existed it was inherited as the protocol's `...`
        body and answered None to a question typed `bool`.

        Args:
            table_name: Name of the table
            layer: Data layer ('bronze', 'silver', etc.)

        Returns:
            True if the table has at least one column configured
        """
        return bool(self.get_stats_columns(table_name, layer))

    def get_configured_tables(self, layer: str = "bronze") -> list[str]:
        """List the tables configured for statistics in a layer.

        Args:
            layer: Data layer ('bronze', 'silver', etc.)

        Returns:
            Table names, empty if the layer has no configuration
        """
        config = self.get_stats_config(layer)
        return config.get_tables() if config else []

    def _process_stats(self, schema: str) -> StatsConfiguration | None:
        """Process raw CSV into StatsConfiguration.

        Args:
            schema: Schema name to filter for

        Returns:
            StatsConfiguration object or None
        """
        if not self._csv_loader:
            logger.warning("No CSV loader injected into StatsManager")
            return None

        try:
            settings = get_settings()
            df = self._csv_loader(self.csv_path)
            if df is None or df.empty:
                logger.debug(f"No stats configuration found for schema: {schema}")
                return None

            df["schema_name"] = df["schema_name"].str.lower()
            # removeprefix, not replace: `Series.replace` compares whole values,
            # so the prefix was never removed and every later lookup by bare
            # table name missed -- the stats feature was a no-op for any
            # prefixed deployment. The prefix is lowered to match the column.
            df["table_name"] = (
                df["table_name"].str.lower().str.removeprefix(settings.table_prefix.lower())
            )
            df["stats_column_name"] = df["stats_column_name"].str.lower()

            # Filter for the specified schema
            schema_df = df[df["schema_name"] == schema]

            if schema_df.empty:
                logger.debug(f"No stats entries for schema: {schema}")
                return StatsConfiguration(schema_name=schema, table_stats={})

            # Build stats dictionary
            stats_dict = {}
            for table_name, group in schema_df.groupby("table_name"):
                stats_dict[table_name] = group["stats_column_name"].tolist()

            config = StatsConfiguration(schema_name=schema, table_stats=stats_dict)

            logger.info(
                f"Loaded stats configuration for {schema}: "
                f"{len(stats_dict)} tables, "
                f"{config.get_total_columns()} columns"
            )

            return config

        except Exception as e:
            logger.error(f"Failed to process stats for {schema}: {e}")
            return None

    def clear_metadata(self, layer: str | None = None) -> None:
        """Clear cached statistics metadata.

        Args:
            layer: Optional layer to clear. If None, clears all metadata
        """
        cache = get_feature_manager("cache")
        if not cache:
            return

        if layer:
            # Clear specific layer
            cleared = cache.clear(f"stats:{layer}*")
            logger.info(f"Cleared {cleared} stats cache entries for layer: {layer}")
        else:
            # Clear all stats
            cleared = cache.clear("stats:*")
            logger.info(f"Cleared {cleared} stats cache entries")

    def cleanup(self) -> None:
        """Cleanup resources when shutting down."""
        self._initialized = False
        self._csv_loader = None
        logger.debug("StatsManager cleaned up")

    @property
    def is_initialized(self) -> bool:
        """Check if the manager is initialized.

        Returns:
            True if initialized, False otherwise
        """
        return self._initialized


# Auto-register when module is imported
register_feature("stats", StatsManager)
logger.debug("StatsManager registered with feature registry")
