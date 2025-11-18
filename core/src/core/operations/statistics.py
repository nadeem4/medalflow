"""Statistics-related operations.

This module contains operation classes for managing database statistics.
"""

import logging
from typing import List, Literal, Optional

from pydantic import Field, model_validator

from core.constants.sql import QueryType
from core.operations.base import BaseOperation
from core.protocols import StatsProtocol


logger = logging.getLogger(__name__)


class CreateStatistics(BaseOperation):
    """Create statistics operation with auto-discovery support.
    
    This operation can automatically discover statistics columns using
    the StatsManager when columns are not explicitly provided.
    
    Attributes:
        columns: List of column names for statistics. Can be auto-discovered.
        sample_percent: Optional sampling percentage (0-100).
        with_fullscan: Whether to use full scan (default True).
        stats_name: Statistics name (auto-generated if not provided).
        auto_discover: Enable automatic column discovery via StatsManager.
        table_name: Optional table name for column discovery (uses object_name if not set).
    """
    operation_type: Literal[QueryType.CREATE_STATISTICS] = Field(
        default=QueryType.CREATE_STATISTICS,
        frozen=True
    )
    
    columns: Optional[List[str]] = Field(default=None)
    sample_percent: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    with_fullscan: bool = Field(default=True)
    stats_name: Optional[str] = Field(default=None)  # Auto-generate if not provided
    auto_discover: bool = Field(
        default=True,
        description="Enable automatic column discovery via StatsManager"
    )
    
    @model_validator(mode='after')
    def validate_and_resolve(self):
        """Validate sampling options and resolve columns if needed."""
        if self.sample_percent is not None and self.with_fullscan:
            raise ValueError("Cannot specify both sample_percent and with_fullscan")
        
        if self.columns:
            return self
        
        if self.auto_discover:
            self._discover_columns()
        
        if not self.columns:
            raise ValueError(
                f"No columns specified for statistics on {self.full_object_name}. "
                "Either provide columns explicitly or enable auto_discover with StatsManager configured."
            )
        
        return self
    
    def _discover_columns(self) -> None:
        """Discover statistics columns using StatsManager.
        
        This method attempts to use the StatsManager feature to discover
        appropriate columns for statistics based on configuration.
        """
        try:
            if hasattr(self.metadata, 'stats_columns') and self.metadata.stats_columns:
                self.columns = self.metadata.stats_columns
                logger.info(
                    f"Using metadata-defined statistics columns for {self.full_object_name}: {self.columns}"
                )
                return
            
            
            from core.core.features import get_feature_manager
            
            stats_mgr: StatsProtocol = get_feature_manager('stats')
            if not stats_mgr:
                logger.debug(
                    f"StatsManager not available for column discovery "
                    f"on {self.full_object_name}"
                )
                return
            
            discovered_columns = stats_mgr.get_stats_columns(
                table_name=self.object_name,
                layer=self.schema_name
            )
            
            if discovered_columns:
                self.columns = discovered_columns
                logger.info(
                    f"Auto-discovered {len(discovered_columns)} statistics columns "
                    f"for {self.full_object_name}: {discovered_columns}"
                )
            else:
                logger.debug(
                    f"No statistics columns found in configuration "
                    f"for {self.full_object_name}"
                )
                
        except Exception as e:
            logger.warning(
                f"Failed to auto-discover statistics columns "
                f"for {self.full_object_name}: {e}"
            )