"""Azure Synapse SQL engine implementation.

This module provides a minimal Synapse-specific SQL engine that inherits
all functionality from BaseSQLEngine. It only adds Synapse-specific
connection settings.
"""

import logging
from typing import Any, Dict, TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Connection

from core.compute.engines.base import BaseSQLEngine
from core.constants.compute import ComputeEnvironment

if TYPE_CHECKING:
    from core.settings import SynapseSettings

logger = logging.getLogger(__name__)


class SynapseSQLEngine(BaseSQLEngine):
    """SQL engine implementation for Azure Synapse Analytics.
    
    This engine inherits all SQLAlchemy functionality from BaseSQLEngine
    and only provides Synapse-specific connection settings.
    """
    
    settings: 'SynapseSettings'  # Type narrowing: specify exact settings type
    
    def __init__(self, settings: 'SynapseSettings', environment: ComputeEnvironment = ComputeEnvironment.ETL):
        """Initialize Synapse SQL engine.
        
        Args:
            settings: Synapse settings from configuration
            environment: Compute environment (ETL or CONSUMPTION)
        """
        super().__init__(settings, environment)
        self._connection_info.update({
            "platform": "synapse",
            "environment": environment.value,
            "lake_database": settings.lake_database_name,
        })
    
    def _apply_connection_settings(self, conn: Connection) -> None:
        """Apply Synapse-specific connection settings.
        
        Synapse requires specific SET options for proper operation.
        
        Args:
            conn: SQLAlchemy Connection object
        """
        # Set required options for Synapse
        conn.execute(text("SET ARITHABORT ON"))
        conn.execute(text("SET ANSI_NULLS ON"))
        conn.execute(text("SET ANSI_PADDING ON"))
        conn.execute(text("SET ANSI_WARNINGS ON"))
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get Synapse-specific connection information.
        
        Returns:
            Dictionary with Synapse connection details
        """
        info = super().get_connection_info()
        info.update({
            "platform": "synapse",
            "lake_database": self.settings.lake_database_name,
            "external_data_sources": {
                "raw": self.settings.raw_external_data_source_name,
                "processed": self.settings.processed_external_data_source_name
            }
        })
        return info