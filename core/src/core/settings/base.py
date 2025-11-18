from typing import Any, Dict, Optional, Tuple, List, TYPE_CHECKING, ClassVar
from pydantic import Field, field_validator, SecretStr, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict

from core.core.descriptors import SecretField
from core.core.mixins import SecretProviderMixin
from core.constants import LayerType



class CTEBaseSettings(SecretProviderMixin, BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__"
    )

    tenant_id: str = Field(
        ...,
        description="Azure AD tenant ID for the application (GUID format)"
    )
    source_system: str = Field(
        ..., 
        description="Source system name (e.g., sap, oracle, dynamics365, salesforce, etc.)"
    )
    ds_env: str = Field(
        ..., 
        description="Data source environment (dev, qa, uat, prod). This setting ensures environment isolation in the data lake."
    )
    name: str = Field(
        ..., 
        description="Used for table prefixing and package naming. Use short, descriptive names (e.g., sap, oracle, d365, sf, etc.)"
    )


    app_env: str = Field(
        default="dev",
        description="Application deployment environment (e.g., dev, qa, uat, prod, local, dev-west, prod-east, etc.)"
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum number of retry attempts for operations"
    )
    retry_delay_seconds: float = Field(
        default=1.0,
        ge=0.1,
        le=60.0,
        description="Delay between retry attempts in seconds"
    )

    max_workers: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of worker threads for concurrent operations"
    )

    sp_client_id_secret_name: str = Field(
        default="SP-CLIENT-ID",
        description="KeyVault secret name for Service Principal Client ID. This will be use to retrieve the actual client ID from Key Vault. This wil be used to authenticate to Azure services (Power BI)."
    )
    sp_client_secret_secret_name: str = Field(
        default="SP-CLIENT-SECRET",
        description="KeyVault secret name for Service Principal Client Secret. This will be use to retrieve the actual client secret from Key Vault. This wil be used to authenticate to Azure services (Power BI)."
    )

    sp_client_id: ClassVar[SecretField] = SecretField()
    sp_client_secret: ClassVar[SecretField] = SecretField(return_secret_str=True)
    
    
    layer_type: LayerType = Field(
        default=LayerType.BASE,
        description="Type of layer structure: 'base' (default) or 'custom'. This setting deetermines the silver, gold, and snapshots package names."
    )
    
    configured_models: str = Field(
        default="",
        description=(
            "Comma-separated list of configured model names. "
            "These correspond to subdirectories in silver_grouping "
            "(e.g., 'sales,purchase,inventory,finance'). "
            "Each model represents a logical domain of related tables."
        )
    )
    
    @field_validator("configured_models")
    @classmethod
    def validate_configured_models(cls, v: str) -> str:
        """Validate the configured models string.
        
        Ensures the configured_models value is properly formatted
        and contains valid model names.
        
        Args:
            v: The configured_models string value
            
        Returns:
            Validated and normalized model string
        """
        if not v:
            return v
        
        models = [m.strip() for m in v.split(",") if m.strip()]
        
        for model in models:
            if not model.replace("_", "").replace("-", "").isalnum():
                raise ValueError(
                    f"Invalid model name '{model}'. "
                    f"Model names must be alphanumeric with optional underscores or hyphens."
                )
        
        return ",".join(models)

    @classmethod
    def get_env_prefix(cls) -> str:
        """Get the environment variable prefix for this settings class.

        Override this method in subclasses to provide custom prefixes for
        environment variable namespacing. This enables different settings
        modules to use different prefixes (e.g., COMPUTE_, DATALAKE_, etc.)
        
        Returns:
            str: Environment variable prefix (empty string for base class)
            
        Example:
            ```python
            class MySettings(CTEBaseSettings):
                @classmethod
                def get_env_prefix(cls) -> str:
                    return "MYAPP_"
            ```
        """
        return ""

    def model_post_init(self, __context: Any) -> None:
        """Post initialization hook for additional setup.
        
        This method is called after Pydantic has initialized and validated
        all fields. Subclasses should override this method and call super()
        to add custom initialization logic.
        
        Args:
            __context: Pydantic context object (internal use)
            
        Example:
            ```python
            def model_post_init(self, __context):
                super().model_post_init(__context)
                # Load secrets from Key Vault
                self._load_secrets()
                # Set up cross-references
                self._setup_references()
            ```
        """
        super().model_post_init(__context)


    def get_effective_client_id(self) -> Optional[str]:
        """Get the effective client ID for authentication.

        Returns the client ID from the secret descriptor. This method
        provides backward compatibility for code using the old pattern.

        Returns:
            Optional[str]: The client ID string, or None if not loaded
            
        Example:
            ```python
            client_id = settings.get_effective_client_id()
            if client_id:
                # Use for authentication
                auth_config = {"client_id": client_id}
            ```
        """
        return self.sp_client_id

    def get_effective_client_secret(self) -> Optional[SecretStr]:
        """Get the effective client secret for this settings instance.

        Returns the client secret from the secret descriptor.

        Returns:
            The client secret to use for authentication, or None if not loaded
        """
        return self.sp_client_secret

    def get_effective_credentials(self) -> Tuple[Optional[str], Optional[SecretStr]]:
        """Get the complete client credentials for authentication.

        Returns both client ID and client secret as a tuple. This is useful
        when both values are needed together for authentication scenarios.
        
        Returns:
            Tuple[Optional[str], Optional[SecretStr]]: 
                - client_id: The client ID string or None
                - client_secret: The client secret (still wrapped in SecretStr) or None
                
        Example:
            ```python
            client_id, client_secret = settings.get_effective_credentials()
            if client_id and client_secret:
                credential = ClientSecretCredential(
                    tenant_id=settings.tenant_id,
                    client_id=client_id,
                    client_secret=client_secret.get_secret_value()
                )
            ```
        """
        return self.get_effective_client_id(), self.get_effective_client_secret()
    
    @property
    def base_path(self) -> str:
        """Get the base path for this data source.
        
        Returns just the data source environment as the directory path,
        since source_system is used as the file system (container).
        """
        return self.ds_env 
    
    @property
    def datasource_file_system(self) -> str:
        """Get the file system (container) name for this data source."""
        return self.source_system.lower() 
    
    @property
    def full_path(self) -> str:
        """Get the full prefix including file system for display purposes."""
        return f"{self.datasource_file_system}/{self.base_path}"
    
    
    @property
    def table_prefix(self) -> str:
        return f"{self.name}_" 
    
    @property
    def ds_name(self) -> str:
        """Get the data source name for package naming.
        
        Returns the name if specified, otherwise uses source_system.
        """
        return self.name
    
    def get_configured_model_list(self) -> List[str]:
        """Get the list of configured models.
        
        Returns:
            List of model names, or empty list if none configured
        """
        if not self.configured_models:
            return []
        return [m.strip() for m in self.configured_models.split(",")]
    
    def is_model_configured(self, model_name: str) -> bool:
        """Check if a specific model is configured.
        
        Args:
            model_name: Name of the model to check
            
        Returns:
            True if the model is configured, False otherwise
        """
        return model_name in self.get_configured_model_list() 

