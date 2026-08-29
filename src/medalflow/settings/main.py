"""The MedalFlow settings object.

One :class:`MedalflowSettings` reads every configuration value MedalFlow needs.
Identity (``source_system``, ``ds_env``, ``name``) lives once, at the top level;
domain groups are plain pydantic models reached through the nested delimiter.

All environment variables share the ``MEDALFLOW_`` prefix and use ``__`` to
descend into a group::

    MEDALFLOW_NAME=fin
    MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME=lakedb
    MEDALFLOW_DATALAKE__PROCESSED__ACCOUNT_NAME=mylake

See ``.env.example`` at the repository root for the full minimal set.
"""

import logging
from typing import TYPE_CHECKING, Optional

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from medalflow.constants import LayerType
from medalflow.core.mixins import NestedSecretsMixin
from medalflow.secret_vault.env import ENV_PREFIX, EnvSecretProvider
from medalflow.secret_vault.keyvault import KeyVaultSecrets
from medalflow.secret_vault.mock import MockSecrets

from .compute import ComputeSettings
from .conventions import ConventionsSettings
from .datalake import MultiDataLakeSettings
from .features import FeatureSettings
from .keyvault import KeyVaultSettings
from .stats import StatsSettings

if TYPE_CHECKING:
    from medalflow.protocols.providers import SecretProvider

# app_env values that mean "real data, real secrets".
PRODUCTION_ENVIRONMENTS = frozenset({"prod", "production"})


class MedalflowSettings(NestedSecretsMixin, BaseSettings):
    """Every configuration value MedalFlow reads, in one object."""

    model_config = SettingsConfigDict(
        env_prefix="MEDALFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__",
    )

    # --- Identity: declared once, read by every layer -----------------------
    source_system: str = Field(
        ..., description="Source system name (e.g., sap, oracle, dynamics365, salesforce)"
    )
    ds_env: str = Field(
        ...,
        description="Data source environment (dev, qa, uat, prod). Ensures environment isolation in the data lake.",
    )
    name: str = Field(
        ...,
        description="Short data source name. The single source of both the table prefix "
        "('fin_') and the external data source names ('ds_fin_proc'). "
        "Use short, descriptive names (e.g., sap, oracle, d365, sf).",
    )

    app_env: str = Field(
        default="dev",
        description="Application deployment environment (e.g., dev, qa, uat, prod, local)",
    )

    test_mode: bool = Field(
        default=False,
        validation_alias=AliasChoices("MEDALFLOW_TEST_MODE", "CTE_TEST_MODE"),
        description="Run with mock secrets instead of Azure Key Vault. Refused when "
        "app_env names a production environment. "
        "CTE_TEST_MODE is a deprecated alias for MEDALFLOW_TEST_MODE.",
    )

    max_retries: int = Field(
        default=3, ge=0, le=10, description="Maximum number of retry attempts for operations"
    )
    retry_delay_seconds: float = Field(
        default=1.0, ge=0.1, le=60.0, description="Delay between retry attempts in seconds"
    )
    max_workers: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of worker threads for concurrent operations",
    )

    layer_type: LayerType = Field(
        default=LayerType.BASE,
        description="Layer structure: 'base' (default) or 'custom'. Determines the silver "
        "and gold package names.",
    )

    configured_models: str = Field(
        default="",
        description=(
            "Comma-separated list of configured model names. "
            "These correspond to subdirectories in silver_grouping "
            "(e.g., 'sales,purchase,inventory,finance'). "
            "Each model represents a logical domain of related tables."
        ),
    )

    # --- Domain groups ------------------------------------------------------
    datalake: MultiDataLakeSettings = Field(
        default_factory=MultiDataLakeSettings, description="ADLS configuration"
    )
    compute: ComputeSettings = Field(
        ..., description="Compute platform configuration (SQL engines)"
    )
    keyvault: KeyVaultSettings = Field(
        default_factory=KeyVaultSettings, description="Key Vault configuration"
    )
    features: FeatureSettings = Field(
        default_factory=FeatureSettings, description="Feature flags configuration"
    )
    stats: StatsSettings = Field(
        default_factory=StatsSettings, description="Statistics management configuration"
    )
    conventions: ConventionsSettings = Field(
        default_factory=ConventionsSettings,
        description="Opt-in naming conventions; every one of them defaults to off",
    )

    @model_validator(mode="after")
    def refuse_test_mode_in_production(self) -> "MedalflowSettings":
        """Stop mock secrets from ever standing in for real ones in production.

        Returns:
            Self, unchanged, when the combination is allowed

        Raises:
            ValueError: If test mode is requested while app_env names production
        """
        if self.test_mode and self.app_env.strip().lower() in PRODUCTION_ENVIRONMENTS:
            raise ValueError(
                f"test_mode substitutes mock secrets for real ones and cannot run with "
                f"app_env={self.app_env!r}. Unset MEDALFLOW_TEST_MODE (or the deprecated "
                f"CTE_TEST_MODE alias), or point MEDALFLOW_APP_ENV at a non-production "
                f"environment."
            )

        return self

    @field_validator("configured_models")
    @classmethod
    def validate_configured_models(cls, v: str) -> str:
        """Validate and normalize the comma-separated model list.

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

    def model_post_init(self, __context) -> None:
        """Wire the object graph once pydantic has validated every field.

        1. Hands the top-level ``name`` to the compute group, so the external
           data source names derive from the same field as ``table_prefix``.
        2. Creates the secret provider (Key Vault, or mock in test mode) and
           propagates it to the groups that carry ``SecretField`` descriptors.

        Args:
            __context: Pydantic context (internal use)
        """
        super().model_post_init(__context)

        self.compute.bind_data_source_name(self.name)

        secret_provider = self._create_secret_provider()
        if secret_provider:
            self.attach_secrets(secret_provider)
            self.propagate_secrets(self.compute, self.datalake)

    def _create_secret_provider(self) -> "SecretProvider":
        """Create the secret provider named by the configuration.

        Never returns None: a settings object without a provider is a settings
        object whose ``SecretField`` descriptors all read empty.

        Returns:
            A KeyVault provider if Key Vault is configured, a mock provider in
            test mode, otherwise the zero-dependency environment provider.
        """
        logger = logging.getLogger(__name__)

        if self.keyvault.is_configured:
            try:
                logger.debug("Creating KeyVault secret provider")
                return KeyVaultSecrets(settings=self.keyvault)
            except Exception as e:
                # Was `return None`, the last route to a provider-less settings
                # object. The environment provider needs nothing installed, so
                # falling back to it beats having no provider at all.
                logger.warning(
                    "Failed to create KeyVault provider (%s); falling back to %s* variables",
                    e,
                    ENV_PREFIX,
                )
                return EnvSecretProvider()

        elif self.test_mode:
            logger.info("Using mock secret provider for test mode")
            return MockSecrets()

        logger.info("Key Vault is not configured; reading secrets from %s* variables", ENV_PREFIX)
        return EnvSecretProvider()

    @property
    def secrets(self) -> Optional["SecretProvider"]:
        """The attached secret provider, created on first access if needed."""
        if self._secret_provider is None:
            self._secret_provider = self._create_secret_provider()
        return self._secret_provider

    # --- Derived paths and names -------------------------------------------
    @property
    def base_path(self) -> str:
        """Base path for this data source inside the file system."""
        return self.ds_env

    @property
    def datasource_file_system(self) -> str:
        """File system (container) name for this data source."""
        return self.source_system.lower()

    @property
    def full_path(self) -> str:
        """Full prefix including file system, for display purposes."""
        return f"{self.datasource_file_system}/{self.base_path}"

    @property
    def table_prefix(self) -> str:
        """Prefix applied to table names, derived from ``name``."""
        return f"{self.name}_"

    @property
    def ds_name(self) -> str:
        """Data source name used for package naming."""
        return self.name

    def get_configured_model_list(self) -> list[str]:
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

    @property
    def silver_package_name(self) -> str:
        """Python package path for the silver layer.

        Examples:
            - layer_type=CUSTOM, name="fin" -> "custom_fin.silver"
            - layer_type=BASE, name="fin" -> "fin.layers.custom.silver"
        """
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.silver"
        else:
            return f"{self.ds_name}.layers.custom.silver"

    @property
    def gold_package_name(self) -> str:
        """Python package path for the gold layer."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.gold.gold"
        else:
            return f"{self.ds_name}.layers.custom.gold.gold_query"

    @property
    def dimension_package_name(self) -> str:
        """Python package path for dimension tables."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.silver.dimension_table.dimension_tables"
        else:
            return f"{self.ds_name}.layers.custom.silver.dimension_table.dimension_tables"

    @property
    def silver_proc_mapping_package_name(self) -> str:
        """Python package path for silver stored procedure mappings."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.config.python_proc"
        else:
            return f"{self.ds_name}.config.python_proc"

    @property
    def silver_proc_crud_mapping_package_name(self) -> str:
        """Python package path for silver CRUD mappings."""
        if self.layer_type == LayerType.CUSTOM:
            return f"custom_{self.ds_name}.config.crud_mapping"
        else:
            return f"{self.ds_name}.config.crud_mapping"


# Singleton instance
_settings: MedalflowSettings | None = None


def get_settings(force_reload: bool = False) -> MedalflowSettings:
    """Get the singleton settings instance for the application.

    Settings are loaded from environment variables (and ``.env``) on first
    access and reused, so configuration stays consistent across components and
    Key Vault is contacted at most once per process.

    Args:
        force_reload: If True, build a new instance even if one already exists.
                      Useful for testing or when the environment has changed.

    Returns:
        The singleton MedalflowSettings instance

    Example:
        >>> settings = get_settings()
        >>> get_settings() is settings
        True
        >>> get_settings(force_reload=True) is settings
        False

    Note:
        Reading is thread-safe; the initial creation is not. Settings are
        normally loaded once at startup, before threading begins.
    """
    global _settings

    if _settings is None or force_reload:
        _settings = MedalflowSettings()

    return _settings
