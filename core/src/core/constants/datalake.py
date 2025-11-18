"""Data lake constants and enumerations.

This module contains all enum types used for data lake operations
and configurations.
"""

from enum import Enum


class LakeType(str, Enum):
    """Enum for data lake types.
    
    Defines the types of data lakes used in the medalflow system.
    Each lake serves different purposes in the data architecture.
    
    Values:
        PROCESSED: Primary data lake for processed data (bronze/silver/gold)
        INTERNAL: Internal data lake for configuration and temporary data
    """
    
    PROCESSED = "processed"
    INTERNAL = "internal"
    
    @classmethod
    def all(cls) -> list['LakeType']:
        """Get all lake types as a list."""
        return list(cls)


class DataLakeAuthMethod(str, Enum):
    """Authentication method for data lake access.
    
    Defines the supported authentication methods for accessing
    Azure Data Lake Storage.
    
    Values:
        ACCESS_KEY: Storage account access key authentication
            - Full account access
            - Connection string generated automatically
            - Best for service-to-service communication
            
        MANAGED_IDENTITY: Azure managed identity authentication
            - No secrets to manage
            - Azure AD based authentication
            - Requires proper RBAC configuration
            - Recommended for production
    """
    
    ACCESS_KEY = "access_key"
    MANAGED_IDENTITY = "managed_identity"