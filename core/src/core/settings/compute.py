from typing import List, Optional, TYPE_CHECKING, Any, ClassVar
from functools import cached_property
import logging
from pydantic import Field, SecretStr, field_validator, PrivateAttr
from pydantic_settings import SettingsConfigDict

from core.constants.compute import ComputeType, EngineType, ComputeEnvironment
from core.core.descriptors import SecretField
from .base import CTEBaseSettings


class BaseComputeSettings(CTEBaseSettings):
    lake_database_name: str = Field(
        ...,
        description="Name of the lake database for metadata storage"
    )

    etl_odbc_secret_name: str = Field(
        default="ETL-SERVER",
        description="KeyVault secret name for ETL ODBC connection string"
    )
    consumption_odbc_secret_name: str = Field(
        default="CONSUMPTION-SERVER",
        description="KeyVault secret name for Consumption ODBC connection string"
    )

    etl_odbc: ClassVar[SecretField] = SecretField()
    consumption_odbc: ClassVar[SecretField] = SecretField()
    
    schemas: List[str] = Field(
        default=["silver", "bronze", "gold", "temp", "snapshot"],
        description="Database schemas to create/manage"
    )

    skip_prefix_on_schema: List[str] = Field(
        default=["dbo", "gold", "snapshot"],
        description="Schemas that do not get the prefix applied"
    )

    dialect: str = Field(
        default="tsql",
        description="SQL dialect for query generation and analysis"
    )

    sql_pool_size: int = Field(default=20, ge=1, le=100)
    sql_pool_timeout: int = Field(default=30, ge=1)
    sql_max_overflow: int = Field(default=10, ge=0)

    spark_workspace_name: Optional[str] = Field(None, description="Synapse workspace for Spark")
    spark_pool_name: Optional[str] = Field(None, description="Spark pool name")
    spark_executor_instances: int = Field(default=2, ge=1)
    spark_executor_cores: int = Field(default=4, ge=1)
    spark_executor_memory: str = Field(default="4g")
    spark_driver_memory: str = Field(default="4g")
    spark_max_concurrent_jobs: int = Field(default=10, ge=1)


    spark_enabled: bool = Field(default=False, description="Whether Spark is enabled")


    
    @property
    def spark_configured(self) -> bool:
        """Check if Spark is configured."""
        return bool(self.spark_workspace_name and self.spark_pool_name)
    
    @property
    def is_configured(self) -> bool:
        """Check if compute settings are properly configured.
        
        ETL ODBC is mandatory for the settings to be considered configured.
        Consumption ODBC is optional but will log a warning if missing.
        
        Returns:
            True if properly configured (ETL ODBC is present)
        """
        if not self.etl_odbc:
            return False
        
        if not self.consumption_odbc:
            logging.warning("Consumption ODBC connection string is not set - some features may be limited")
        
        return True
    
    def get_odbc_string(self, environment: ComputeEnvironment) -> Optional[str]:
        """Get ODBC connection string for specified environment.
        
        Secrets are now loaded lazily through descriptors.
        
        Args:
            environment: The compute environment (ETL or CONSUMPTION)
            
        Returns:
            The ODBC connection string or None
        """
        if environment == ComputeEnvironment.ETL:
            return self.etl_odbc
        else:
            return self.consumption_odbc
    
    


class SynapseSettings(BaseComputeSettings):   
    model_config = SettingsConfigDict(case_sensitive=False)
    
    database_scoped_cred_name: str = Field(
        default="cte_adls_creds", 
        description="Database Scoped Credential"
    )  
    raw_external_data_source_name_override: Optional[str] = Field(
        default=None,
        description="Override for raw external data source name. If not set, auto-constructed from DataSourceConfig.",
    )
    processed_external_data_source_name_override: Optional[str] = Field(
        default=None,
        description="Override for processed external data source name. If not set, auto-constructed from DataSourceConfig.",
    )
    csv_file_format: str = Field(default="csv_file_format")
    parquet_file_format: str = Field(default="parquet_file_format")
    

    @property
    def raw_external_data_source_name(self) -> str:
        """Get raw external data source name.
        
        Returns override if set, otherwise constructs from DataSourceConfig.
        Computed once and cached for performance.
        """
        if self.raw_external_data_source_name_override:
            return self.raw_external_data_source_name_override
            
        return f"ds_{self.name}_raw"
    
    @property
    def processed_external_data_source_name(self) -> str:
        """Get processed external data source name.
        
        Returns override if set, otherwise constructs from DataSourceConfig.
        Computed once and cached for performance.
        """
        if self.processed_external_data_source_name_override:
            return self.processed_external_data_source_name_override
            
        return f"ds_{self.name}_proc"



class FabricSettings(BaseComputeSettings):
    
    model_config = SettingsConfigDict(case_sensitive=False)
    
    
    

class ComputeSettings(CTEBaseSettings):
    
    model_config = SettingsConfigDict(case_sensitive=False)
    
    compute_type: ComputeType = Field(
        default=ComputeType.SYNAPSE,
        description="Active compute platform"
    )
    
    _synapse: Optional[SynapseSettings] = PrivateAttr(default=None)
    _fabric: Optional[FabricSettings] = PrivateAttr(default=None)
    
    @property
    def synapse(self) -> SynapseSettings:
        """Get or create Synapse settings.
        
        Lazily creates the settings and propagates secret provider and datasource config.
        """
        if self._synapse is None:
            self._synapse = SynapseSettings()
            if self._secret_provider:
                self._synapse.attach_secrets(self._secret_provider)
            else:
                raise ValueError("Secret provider must be attached before accessing Synapse settings")
        return self._synapse
    
    @property
    def fabric(self) -> FabricSettings:
        """Get or create Fabric settings.
        
        Lazily creates the settings and propagates secret provider and datasource config.
        """
        if self._fabric is None:
            self._fabric = FabricSettings()
            if self._secret_provider:
                self._fabric.attach_secrets(self._secret_provider)
            else:
                raise ValueError("Secret provider must be attached before accessing Fabric settings")
        return self._fabric
    
    
    @property
    def active_config(self) -> BaseComputeSettings:
        """Get configuration for the active compute type.
        
        Returns the appropriate platform settings based on compute_type.
        """
        if self.compute_type == ComputeType.SYNAPSE:
            return self.synapse
        elif self.compute_type == ComputeType.FABRIC:
            return self.fabric
        else:
            raise ValueError(f"Unknown compute type: {self.compute_type}")
    
    def get_active_config(self) -> BaseComputeSettings:
        """Get configuration for the active compute type.
        
        Backward compatibility method - use active_config property instead.
        """
        return self.active_config
    
    
    
    
    @property
    def is_configured(self) -> bool:
        """Check if the active compute platform is properly configured.
        
        Returns:
            True if the active platform is configured
        """
        return self.active_config.is_configured
    
   