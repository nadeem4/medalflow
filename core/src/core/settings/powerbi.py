from typing import Optional, List, Any, TYPE_CHECKING
from pydantic import Field, SecretStr, field_validator, BaseModel
from pydantic_settings import SettingsConfigDict

from .base import CTEBaseSettings




class PowerBIRefreshConfig(BaseModel):
    """Configuration for a single Power BI dataset refresh.
    
    This model represents one row in the CSV configuration file.
    """
    workspace_id: str = Field(..., description="Power BI workspace GUID")
    dataset_id: str = Field(..., description="Power BI dataset GUID")
    datamodel_name: str = Field(..., description="Name of the data model")
    
    @field_validator('workspace_id', 'dataset_id')
    @classmethod
    def validate_guid(cls, v: str) -> str:
        """Validate GUID format."""
        import re
        guid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        if not re.match(guid_pattern, v.strip()):
            raise ValueError(f"Invalid GUID format: {v}")
        return v.strip()


class PowerBISettings(CTEBaseSettings):
    """Power BI integration configuration.
    
    Manages settings for Power BI workspace access, dataset refresh,
    and authentication using service principal. This enables automated
    dataset refreshes after ETL pipeline completion.
    
    **Authentication**:
        Power BI integration uses service principal authentication:
        1. Service principal must be registered in Azure AD
        2. Principal needs Power BI API permissions
        3. Principal must have workspace access
        4. Credentials stored in Key Vault
        
    **Refresh Workflow**:
        1. ETL pipeline completes data processing
        2. PowerBISettings provides API configuration
        3. Service triggers dataset refresh via REST API
        4. Monitors refresh status until completion
        
    **Configuration Options**:
        - Single dataset refresh (workspace_id + dataset_id)
        - Multiple dataset support via CSV configuration
        - Configurable timeouts and retry logic
        - Optional refresh based on enable_refresh flag
    """
    
    model_config = SettingsConfigDict(
        env_prefix="POWERBI_",
        case_sensitive=False
    )
    
    url: str = Field(
        default="https://api.powerbi.com/v1.0/myorg",
        description="Power BI REST API base URL. Change only for sovereign clouds "
                   "or special environments (e.g., China, Germany, US Gov)."
    )
    
        
        
