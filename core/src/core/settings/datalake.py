from typing import Optional, Union, Any, ClassVar
from functools import cached_property

from pydantic import Field, SecretStr, model_validator, PrivateAttr
from pydantic_settings import SettingsConfigDict

from .base import CTEBaseSettings
from core.core.descriptors import SecretField
from core.constants.datalake import LakeType, DataLakeAuthMethod
from core.protocols import SecretProvider



class BaseDataLakeConfig(CTEBaseSettings):
    
    account_name: str = Field(..., description="DataLake account name")
    file_system_name: Optional[str] = Field(None, description="File system name (can be overridden by data source)")
    auth_method: DataLakeAuthMethod = Field(
        DataLakeAuthMethod.ACCESS_KEY,
        description="Authentication method to use"
    )
    
    access_key: ClassVar[SecretField] = SecretField()
    
    @model_validator(mode='after')
    def validate_auth_credentials(self):
        """Validate that appropriate credentials are provided for the auth method.
        
        Note: Only MANAGED_IDENTITY and ACCESS_KEY authentication methods are supported.
        """
        if self.auth_method not in [DataLakeAuthMethod.MANAGED_IDENTITY, DataLakeAuthMethod.ACCESS_KEY]:
            raise ValueError(
                f"Unsupported auth method: {self.auth_method}. "
                "Only 'managed_identity' and 'access_key' are supported."
            )
        
        return self
    
    def get_file_system_name(self) -> str:
        """Get file system name, using data source if provided.
        
        Args:
            data_source_config: Optional data source configuration
            
        Returns:
            File system name (from data source or config)
            
        Raises:
            ValueError: If no file system name can be determined
        """
        
        return self.source_system
    
    @property
    def connection_string(self) -> Optional[str]:
        """Calculate and return the connection string for this DataLake.
        
        Connection strings are calculated based on the account name and auth method.
        Subclasses provide their own access_key through descriptors.
        
        Returns:
            Connection string for Azure DataLake or None
        """
        if self.auth_method == DataLakeAuthMethod.ACCESS_KEY:
            return (
                        f"DefaultEndpointsProtocol=https;"
                        f"AccountName={self.account_name};"
                        f"AccountKey={self.access_key};"
                        f"EndpointSuffix=core.windows.net"
                    )
                    
        return None
    
    
    
    @property
    def is_configured(self) -> bool:
        """Check if this DataLake is properly configured.
        
        A DataLake is considered configured if:
        - account_name is provided
        - auth_method is valid (MANAGED_IDENTITY or ACCESS_KEY)
        
        Returns:
            True if properly configured, False otherwise
        """
        try:
            if not self.account_name:
                return False
            
            if self.auth_method in [DataLakeAuthMethod.ACCESS_KEY, DataLakeAuthMethod.MANAGED_IDENTITY]:
                return True
            else:
                return False
        except Exception:
            return False


class ProcessedDataLakeConfig(BaseDataLakeConfig):
    model_config = SettingsConfigDict(env_prefix="PROCESSED_LAKE_")
    
    # Pydantic field for secret name - can be overridden via env var
    access_key_secret_name: str = Field(
        default="PROCESSED-ADLS-ACCOUNT-KEY",
        description="KeyVault secret name for DataLake access key"
    )


class InternalDataLakeConfig(BaseDataLakeConfig):
    model_config = SettingsConfigDict(env_prefix="INTERNAL_LAKE_")
    
    # Pydantic field for secret name - can be overridden via env var
    access_key_secret_name: str = Field(
        default="CMAA-CONTENT-ADLS-ACCESS-KEY",
        description="KeyVault secret name for DataLake access key"
    )


class MultiDataLakeSettings(CTEBaseSettings):
    
    model_config = SettingsConfigDict()
    
    processed: ProcessedDataLakeConfig = Field(
        default_factory=ProcessedDataLakeConfig,
        description="Processed DataLake configuration"
    )
    internal: InternalDataLakeConfig = Field(
        default_factory=InternalDataLakeConfig,
        description="Internal DataLake configuration"
    )

    def get_lake_config(self, lake_type: LakeType) -> Union[ProcessedDataLakeConfig, InternalDataLakeConfig]:
        """Get the DataLake configuration for the specified lake type.
        
        Args:
            lake_type: Type of DataLake (PROCESSED or INTERNAL
            
        Returns:
            Corresponding DataLake configuration instance
        
        Raises:
            ValueError: If an unsupported lake type is provided
        """
        if lake_type == LakeType.PROCESSED:
            return self.processed
        elif lake_type == LakeType.INTERNAL:
            return self.internal
        else:
            raise ValueError(f"Unsupported lake type: {lake_type}")

    @property
    def processed_storage(self) -> ProcessedDataLakeConfig:
        """Get the processed DataLake configuration.
        
        Returns:
            ProcessedDataLakeConfig instance
        """
        return self.processed
    
    @property
    def internal_storage(self) -> InternalDataLakeConfig:
        """Get the internal DataLake configuration.
        
        Returns:
            InternalDataLakeConfig instance
        """
        return self.internal
    
    @property
    def is_processed_configured(self) -> bool:
        """Check if processed DataLake is properly configured.
        
        Returns:
            True if processed DataLake is configured
        """
        return self.processed.is_configured
    
    @property
    def is_internal_configured(self) -> bool:
        """Check if internal DataLake is properly configured.
        
        Returns:
            True if internal DataLake is configured
        """
        return self.internal.is_configured
    
    @property
    def is_configured(self) -> bool:
        """Check if DataLake settings are properly configured.
        
        Args:
            require_both: If True, both processed and internal must be configured.
                         If False (default), only processed is required.
        
        Returns:
            True if configuration meets requirements
        """
        return self.is_processed_configured and self.is_internal_configured
    
    def attach_secrets(self, provider: SecretProvider) -> 'MultiDataLakeSettings':
        """Attach secret provider to all DataLake configurations.
        
        This method propagates the secret provider to all nested DataLake
        configurations, enabling lazy secret loading.
        
        Args:
            provider: Secret provider instance (KeyVault or mock)
            
        Returns:
            Self for method chaining
        """
        super().attach_secrets(provider)
        
        self.processed.attach_secrets(provider)
        self.internal.attach_secrets(provider)
        
        return self