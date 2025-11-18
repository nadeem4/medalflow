"""Microsoft Fabric SQL engine implementation.

This module provides a minimal Fabric-specific SQL engine that inherits
all functionality from BaseSQLEngine. Since Fabric uses ODBC connections
just like Synapse, minimal customization is needed.
"""

import logging
from typing import Any, Dict, TYPE_CHECKING

from sqlalchemy.engine import Connection

from core.compute.engines.base import BaseSQLEngine
from core.constants.compute import ComputeEnvironment

if TYPE_CHECKING:
    from core.settings import FabricSettings

logger = logging.getLogger(__name__)


class FabricSQLEngine(BaseSQLEngine):
    """SQL engine implementation for Microsoft Fabric.
    
    This engine inherits all SQLAlchemy functionality from BaseSQLEngine.
    Fabric works seamlessly with ODBC connections, requiring minimal
    platform-specific customization.
    
    Features:
        - Supports both SQL Warehouse and Lakehouse endpoints via ODBC
        - Automatic endpoint selection based on environment
        - Direct OneLake integration
        - Full T-SQL compatibility
    """
    
    settings: 'FabricSettings'  # Type narrowing: specify exact settings type
    
    def __init__(self, settings: 'FabricSettings', environment: ComputeEnvironment = ComputeEnvironment.ETL):
        """Initialize Fabric SQL engine.
        
        Args:
            settings: Fabric settings from configuration
            environment: Compute environment (ETL or CONSUMPTION)
        """
        super().__init__(settings, environment)
        self._connection_info.update({
            "platform": "fabric",
            "environment": environment.value,
            "lake_database": settings.lake_database_name,
        })
        
        # Log which endpoint is being used based on ODBC configuration
        logger.info(f"Fabric SQL engine initialized for {environment.value} environment")
    
    def _apply_connection_settings(self, conn: Connection) -> None:
        """Apply Fabric-specific connection settings.
        
        Fabric generally works well with default settings, but this
        method is available for any future platform-specific requirements.
        
        Args:
            conn: SQLAlchemy Connection object
        """
        # Fabric typically doesn't need special SET commands like Synapse
        # The ODBC driver handles most settings appropriately
        pass
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get Fabric-specific connection information.
        
        Returns:
            Dictionary with Fabric connection details
        """
        info = super().get_connection_info()
        info.update({
            "platform": "fabric",
            "lake_database": self.settings.lake_database_name,
        })
            
        return info