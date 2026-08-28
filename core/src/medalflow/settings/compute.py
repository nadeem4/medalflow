"""Compute platform configuration.

A single group holding everything the SQL compute platform needs. ``ComputeType``
currently has exactly one member (``SYNAPSE``), so there is no separate
per-platform model: :attr:`ComputeSettings.active_config` and
:attr:`ComputeSettings.synapse` both return the group itself. When a second
platform is added, those two properties are the seam to split on.
"""

import logging
from typing import ClassVar, Optional

from pydantic import Field, PrivateAttr

from medalflow.constants.compute import ComputeEnvironment, ComputeType
from medalflow.core.descriptors import SecretField
from medalflow.core.mixins import SecretProviderMixin


class ComputeSettings(SecretProviderMixin):
    """Configuration for the SQL compute platform.

    Environment variables are namespaced by the parent settings object, e.g.
    ``MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME``.
    """

    compute_type: ComputeType = Field(
        default=ComputeType.SYNAPSE, description="Active compute platform"
    )

    lake_database_name: str = Field(
        ..., description="Name of the lake database for metadata storage"
    )

    etl_odbc_secret_name: str = Field(
        default="ETL-SERVER", description="KeyVault secret name for ETL ODBC connection string"
    )
    consumption_odbc_secret_name: str = Field(
        default="CONSUMPTION-SERVER",
        description="KeyVault secret name for Consumption ODBC connection string",
    )

    etl_odbc: ClassVar[SecretField] = SecretField()
    consumption_odbc: ClassVar[SecretField] = SecretField()

    schemas: list[str] = Field(
        default=["silver", "bronze", "gold", "temp", "snapshot"],
        description="Database schemas to create/manage",
    )

    skip_prefix_on_schema: list[str] = Field(
        default=["dbo", "gold", "snapshot"],
        description="Schemas that do not get the prefix applied",
    )

    dialect: str = Field(
        default="tsql", description="SQL dialect for query generation and analysis"
    )

    sql_pool_size: int = Field(default=20, ge=1, le=100)
    sql_pool_timeout: int = Field(default=30, ge=1)
    sql_max_overflow: int = Field(default=10, ge=0)

    database_scoped_cred_name: str = Field(
        default="cte_adls_creds", description="Database Scoped Credential"
    )
    raw_external_data_source_name_override: Optional[str] = Field(
        default=None,
        description="Override for raw external data source name. If unset, derived from the top-level `name`.",
    )
    processed_external_data_source_name_override: Optional[str] = Field(
        default=None,
        description="Override for processed external data source name. If unset, derived from the top-level `name`.",
    )
    csv_file_format: str = Field(default="csv_file_format")
    parquet_file_format: str = Field(default="parquet_file_format")

    # Set by MedalflowSettings.model_post_init from the single top-level `name`.
    # Deliberately private: the data source identity is configured once, at the
    # top level, and is not separately settable under MEDALFLOW_COMPUTE__.
    _data_source_name: str = PrivateAttr(default="")

    def bind_data_source_name(self, name: str) -> "ComputeSettings":
        """Bind the top-level data source ``name`` used to derive external names.

        Args:
            name: The single top-level ``name`` from :class:`MedalflowSettings`

        Returns:
            Self for method chaining
        """
        self._data_source_name = name
        return self

    @property
    def synapse(self) -> "ComputeSettings":
        """The active Synapse configuration.

        ``ComputeType`` has one member, so the compute group *is* the Synapse
        configuration. Kept as a named alias for call sites that read
        ``settings.compute.synapse``.
        """
        return self

    @property
    def active_config(self) -> "ComputeSettings":
        """Configuration for the active compute type."""
        if self.compute_type != ComputeType.SYNAPSE:
            raise ValueError(f"Unknown compute type: {self.compute_type}")
        return self

    @property
    def raw_external_data_source_name(self) -> str:
        """Raw external data source name, derived from the top-level ``name``."""
        if self.raw_external_data_source_name_override:
            return self.raw_external_data_source_name_override

        return f"ds_{self._data_source_name}_raw"

    @property
    def processed_external_data_source_name(self) -> str:
        """Processed external data source name, derived from the top-level ``name``."""
        if self.processed_external_data_source_name_override:
            return self.processed_external_data_source_name_override

        return f"ds_{self._data_source_name}_proc"

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
            logging.warning(
                "Consumption ODBC connection string is not set - some features may be limited"
            )

        return True

    def get_odbc_string(self, environment: ComputeEnvironment) -> Optional[str]:
        """Get ODBC connection string for the specified environment.

        Args:
            environment: The compute environment (ETL or CONSUMPTION)

        Returns:
            The ODBC connection string, or None if no secret provider is attached
        """
        if environment == ComputeEnvironment.ETL:
            return self.etl_odbc
        else:
            return self.consumption_odbc
