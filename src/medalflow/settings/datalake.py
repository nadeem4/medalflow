"""Azure Data Lake Storage configuration.

Two lakes are configured: ``processed`` (the medallion data) and ``internal``
(client configuration files). Environment variables are namespaced by the
parent settings object, e.g. ``MEDALFLOW_DATALAKE__PROCESSED__ACCOUNT_NAME``.
"""

from typing import ClassVar

from pydantic import Field, model_validator

from medalflow.constants.datalake import DataLakeAuthMethod, LakeType
from medalflow.core.descriptors import SecretField
from medalflow.core.mixins import NestedSecretsMixin, SecretProviderMixin
from medalflow.protocols import SecretProvider


class BaseDataLakeConfig(SecretProviderMixin):
    """Configuration for a single Azure Data Lake Storage account."""

    account_name: str | None = Field(
        None,
        description="DataLake account name. Required to use this lake; see `is_configured`.",
    )
    file_system_name: str | None = Field(
        None, description="File system name (can be overridden by data source)"
    )
    auth_method: DataLakeAuthMethod = Field(
        DataLakeAuthMethod.ACCESS_KEY, description="Authentication method to use"
    )

    access_key: ClassVar[SecretField] = SecretField()

    @model_validator(mode="after")
    def validate_auth_credentials(self):
        """Validate that appropriate credentials are provided for the auth method.

        Note: Only MANAGED_IDENTITY and ACCESS_KEY authentication methods are supported.
        """
        if self.auth_method not in [
            DataLakeAuthMethod.MANAGED_IDENTITY,
            DataLakeAuthMethod.ACCESS_KEY,
        ]:
            raise ValueError(
                f"Unsupported auth method: {self.auth_method}. "
                "Only 'managed_identity' and 'access_key' are supported."
            )

        return self

    @property
    def connection_string(self) -> str | None:
        """Connection string for this DataLake, or None if not key-authenticated."""
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
        """Check if this DataLake is usable.

        A DataLake is considered configured when an account name is supplied.
        Lakes a project does not use may be left unconfigured.
        """
        return bool(self.account_name)


class ProcessedDataLakeConfig(BaseDataLakeConfig):
    """The lake holding medallion (bronze/silver/gold) data."""

    access_key_secret_name: str = Field(
        default="PROCESSED-ADLS-ACCOUNT-KEY",
        description="KeyVault secret name for DataLake access key",
    )


class InternalDataLakeConfig(BaseDataLakeConfig):
    """The lake holding client configuration files."""

    access_key_secret_name: str = Field(
        default="INTERNAL-ADLS-ACCESS-KEY",
        description="KeyVault secret name for DataLake access key",
    )


class MultiDataLakeSettings(NestedSecretsMixin):
    """Both configured data lakes."""

    processed: ProcessedDataLakeConfig = Field(
        default_factory=ProcessedDataLakeConfig, description="Processed DataLake configuration"
    )
    internal: InternalDataLakeConfig = Field(
        default_factory=InternalDataLakeConfig, description="Internal DataLake configuration"
    )

    def get_lake_config(
        self, lake_type: LakeType
    ) -> ProcessedDataLakeConfig | InternalDataLakeConfig:
        """Get the DataLake configuration for the specified lake type.

        Args:
            lake_type: Type of DataLake (PROCESSED or INTERNAL)

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

    def attach_secrets(self, provider: SecretProvider) -> "MultiDataLakeSettings":
        """Attach a secret provider to both DataLake configurations.

        Args:
            provider: Secret provider instance (KeyVault or mock)

        Returns:
            Self for method chaining
        """
        super().attach_secrets(provider)
        self.propagate_secrets(self.processed, self.internal)

        return self
