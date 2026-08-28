"""Secret field descriptors for lazy-loaded secrets.

This module provides Python descriptors that enable lazy loading of secrets
from any secret provider without creating circular dependencies.

The descriptors cache values per instance and only load secrets when first
accessed, improving performance and allowing objects to be created even
when secret providers are not immediately available.
"""

from typing import TYPE_CHECKING, Optional, Union

from pydantic import SecretStr

from medalflow.common.exceptions import CTEError, ErrorCode
from medalflow.logging import get_logger

if TYPE_CHECKING:
    from medalflow.core.mixins.injection import SecretProviderMixin

logger = get_logger(__name__)


class SecretField:
    """Descriptor for lazy-loaded secrets using instance-level secret names.

    This descriptor looks for a corresponding field on the instance
    (e.g., etl_odbc_secret_name) to determine which secret to load from
    the secret provider. This allows secret names to be configured via
    environment variables or code.

    A successful lookup is cached on the *instance* (in ``obj.__dict__``), so
    the cache dies with the object it belongs to. A failed lookup is never
    cached: the provider is asked again on the next access.

    By default, returns the plain string value from SecretStr for convenience.
    Set return_secret_str=True to return the SecretStr object itself.

    Attributes:
        return_secret_str: Whether to return SecretStr or plain string
        attr_name: The attribute name this descriptor is assigned to
        secret_name_attr: The corresponding secret name field (e.g., "etl_odbc_secret_name")

    Example:
        >>> class MySettings(SecretProviderMixin, BaseSettings):
        >>>     # Pydantic field with default secret name
        >>>     database_password_secret_name: str = Field(default="DB-PASSWORD")
        >>>     api_key_secret_name: str = Field(default="API-KEY")
        >>>
        >>>     # Descriptors that use the above fields
        >>>     database_password: ClassVar[SecretField] = SecretField()
        >>>     api_key: ClassVar[SecretField] = SecretField(return_secret_str=True)
        >>>
        >>> settings = MySettings()
        >>> settings.attach_secrets(keyvault_provider)
        >>>
        >>> # Secret is loaded using the name from database_password_secret_name field
        >>> password = settings.database_password  # Returns plain string
        >>> api_key = settings.api_key  # Returns SecretStr object
    """

    def __init__(self, return_secret_str: bool = False):
        """Initialize the secret field descriptor.

        Args:
            return_secret_str: If True, return SecretStr object; if False, return plain string
        """
        self.return_secret_str = return_secret_str

    def __set_name__(self, owner: type, name: str) -> None:
        """Store the attribute name when descriptor is attached to a class.

        Args:
            owner: The class that owns this descriptor
            name: The attribute name in the class
        """
        self.attr_name = name
        self.secret_name_attr = f"{name}_secret_name"

    def _cache_attr(self) -> str:
        """Name of the per-instance slot holding this field's cached value."""
        return f"_secret_cache_{self.attr_name}"

    def __get__(
        self, obj: Optional["SecretProviderMixin"], objtype: Optional[type] = None
    ) -> Optional[Union[str, SecretStr]]:
        """Get the secret value, loading from provider if necessary.

        Args:
            obj: The instance with SecretProviderMixin, or None if accessed on class
            objtype: The type of the instance (not used)

        Returns:
            The secret value as a string (default) or SecretStr (if
            return_secret_str=True). ``None`` means "no provider is attached" --
            the managed-identity path -- never "the lookup failed".

        Raises:
            ValueError: If secret name field exists but is empty or None
            CTEError: If the attached provider failed to return the secret
        """
        if obj is None:
            return self  # Accessing via class, return descriptor itself

        # Check if the secret name field exists
        if not hasattr(obj, self.secret_name_attr):
            return None

        secret_name = getattr(obj, self.secret_name_attr)
        if not secret_name:
            raise ValueError(
                f"Secret name not configured for field '{self.attr_name}'. "
                f"Please set '{self.secret_name_attr}'."
            )

        cache_attr = self._cache_attr()
        cached = obj.__dict__.get(cache_attr)
        if cached is not None and cached[0] == secret_name:
            return cached[1]

        provider = getattr(obj, "_secret_provider", None)
        if provider is None:
            # No provider: the managed-identity path. Distinct from a failure,
            # which raises, and deliberately not cached so attaching a provider
            # later still works.
            logger.debug(
                "No secret provider attached; '%s' (%s) resolves to None",
                self.attr_name,
                secret_name,
            )
            return None

        try:
            secret_value = provider.get_secret(secret_name)
        except Exception as error:
            # Loud, uncached, and re-raised: a transient Key Vault blip must not
            # become a permanent None that ends up in a connection string.
            raise CTEError(
                f"Failed to load secret '{secret_name}' for field '{self.attr_name}'",
                error_code=ErrorCode.CONNECTION_ERROR,
                details={"secret_name": secret_name, "field": self.attr_name},
                cause=error,
            ) from error

        if isinstance(secret_value, SecretStr) and not self.return_secret_str:
            value: Optional[Union[str, SecretStr]] = secret_value.get_secret_value()
        else:
            value = secret_value

        obj.__dict__[cache_attr] = (secret_name, value)
        return value
