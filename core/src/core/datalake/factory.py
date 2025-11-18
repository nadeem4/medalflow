"""Factory for creating datalake clients."""
from core.constants.datalake import LakeType
from core.logging import get_logger
from .client import DatalakeClient

logger = get_logger(__name__)


class DatalakeFactory:
    """Simple factory for creating datalake clients."""
    
    @staticmethod
    def get_client(lake_type: LakeType = LakeType.PROCESSED) -> DatalakeClient:
        """Get a datalake client for the specified lake type.
        
        Args:
            lake_type: Type of lake to connect to
            
        Returns:
            DatalakeClient instance
        """
        logger.debug(f"Creating client for {lake_type.value} lake")
        return DatalakeClient(lake_type)
    
    @staticmethod
    def get_processed_client() -> DatalakeClient:
        """Get a client for the Processed lake.
        
        Returns:
            DatalakeClient configured for Processed lake
        """
        return DatalakeFactory.get_client(LakeType.PROCESSED)
    
    @staticmethod
    def get_internal_client() -> DatalakeClient:
        """Get a client for the Internal lake.
        
        Returns:
            DatalakeClient configured for Internal lake
        """
        return DatalakeFactory.get_client(LakeType.INTERNAL)


def get_processed_datalake_client() -> DatalakeClient:
    """Get a client for the Processed lake.
    
    Returns:
        DatalakeClient configured for Processed lake
    """
    return DatalakeFactory.get_client(LakeType.PROCESSED)


def get_internal_datalake_client() -> DatalakeClient:
    """Get a client for the Internal lake.
    
    Returns:
        DatalakeClient configured for Internal lake
    """
    return DatalakeFactory.get_client(LakeType.INTERNAL)