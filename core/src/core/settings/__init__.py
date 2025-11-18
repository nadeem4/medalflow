"""Settings module providing organized configuration management for MedalFlow.

This module provides a comprehensive configuration management system built on Pydantic Settings.
It organizes settings into domain-specific files for better maintainability and clarity,
with each settings file handling a specific aspect of the application configuration.

Key Features:
    - Type-safe configuration with automatic validation
    - Environment variable support with consistent naming conventions
    - Azure Key Vault integration for secure secret management
    - Modular organization by domain (compute, datalake, features, etc.)
    - Flexible authentication supporting multiple methods
    - Production-grade validation with detailed error reporting
    - Test mode support for development environments

Architecture:
    The settings system follows a hierarchical structure:
    
    1. Base Layer (base.py):
       - CTEBaseSettings: Base class with common functionality
       
    2. Domain Settings:
       - compute.py: Compute platform configuration (Synapse/Fabric)
       - datalake.py: Azure Data Lake Storage configuration
       - datasource.py: Data source identity and organization
       - keyvault.py: Azure Key Vault integration
       - features.py: Feature flags and toggles
       - processing.py: Data processing behavior
       - powerbi.py: Power BI integration
       - stats.py: Statistics management
       - models.py: Model group configuration for medallion orchestration
       
    3. Main Aggregator (main.py):
       - Settings: Main class that aggregates all domain settings
       - get_settings(): Singleton factory function
       - reload_settings(): Force reload from environment

Configuration Sources (precedence order):
    1. Environment Variables (highest priority)
    2. Azure Key Vault Secrets (loaded after initialization)
    3. Default Values in code (lowest priority)

Environment Variable Naming:
    - Format: [PREFIX_]SETTING_NAME
    - Case: UPPER_SNAKE_CASE
    - Prefixes: Module-specific (e.g., KEYVAULT_, FEATURE_)
    - Nested: Use double underscore __ (e.g., COMPUTE__SYNAPSE__DATABASE)

Quick Start:
    >>> from core.settings import get_settings
    >>> 
    >>> # Get singleton instance
    >>> settings = get_settings()
    >>> 
    >>> # Access configuration
    >>> account = settings.datalake.processed.account_name
    >>> compute_type = settings.compute.compute_type
    >>> 
    >>> # Settings are validated automatically by Pydantic
    >>> # Any configuration errors will raise ValidationError on initialization

Minimal Required Configuration:
    - TENANT_ID: Azure AD tenant ID
    - CLIENT_NAME: Client/tenant identifier
    - SOURCE_SYSTEM: Source ERP system
    - DS_ENV: Data source environment
    - NAME: Table prefix and package name
    - PROCESSED_LAKE_ACCOUNT_NAME: Primary data lake account
    - COMPUTE_TYPE: Platform type (synapse/fabric)
    - Platform-specific settings (e.g., SYNAPSE_LAKE_DATABASE_NAME)

For detailed documentation, see the README.md file in this directory.
"""

# Main settings and functions
from .main import _Settings, get_settings, _reload_settings

# Base classes
from .base import CTEBaseSettings

# Domain-specific settings
from .compute import (
    ComputeSettings,
    ComputeType,
    EngineType,
    ComputeEnvironment,
    SynapseSettings,
    FabricSettings,
    BaseComputeSettings,
)
from .datalake import (
    DataLakeAuthMethod,
    BaseDataLakeConfig,
    ProcessedDataLakeConfig,
    InternalDataLakeConfig,
    MultiDataLakeSettings
)
# DataSourceConfig fields are now in CTEBaseSettings
# LayerType is imported from constants
from .keyvault import KeyVaultSettings
from .features import FeatureSettings
from .processing import ProcessingSettings
from .powerbi import PowerBISettings
from .stats import StatsSettings

__all__ = [
    # Public API - only expose the settings accessor
    "get_settings",
]


