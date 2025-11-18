"""Secret field descriptors for lazy-loaded secrets.

This module provides Python descriptors that enable lazy loading of secrets
from any secret provider without creating circular dependencies.

The descriptors cache values per instance and only load secrets when first
accessed, improving performance and allowing objects to be created even
when secret providers are not immediately available.
"""

from typing import Optional, Any, Dict, TYPE_CHECKING, Union
from pydantic import SecretStr

if TYPE_CHECKING:
    from core.core.mixins.injection import SecretProviderMixin


class SecretField:
    """Descriptor for lazy-loaded secrets using instance-level secret names.
    
    This descriptor looks for a corresponding field on the instance
    (e.g., etl_odbc_secret_name) to determine which secret to load from
    the secret provider. This allows secret names to be configured via
    environment variables or code.
    
    The descriptor caches values per instance and secret name combination,
    only loading secrets when first accessed.
    
    By default, returns the plain string value from SecretStr for convenience.
    Set return_secret_str=True to return the SecretStr object itself.
    
    Attributes:
        return_secret_str: Whether to return SecretStr or plain string
        attr_name: The attribute name this descriptor is assigned to
        secret_name_attr: The corresponding secret name field (e.g., "etl_odbc_secret_name")
        _cache: Dictionary mapping (instance_id, secret_name) to cached values
        
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
        self._cache: Dict[tuple[int, str], Optional[Any]] = {}
    
    def __set_name__(self, owner: type, name: str) -> None:
        """Store the attribute name when descriptor is attached to a class.
        
        Args:
            owner: The class that owns this descriptor
            name: The attribute name in the class
        """
        self.attr_name = name
        self.secret_name_attr = f"{name}_secret_name"
        
    def __get__(self, obj: Optional['SecretProviderMixin'], objtype: Optional[type] = None) -> Optional[Union[str, SecretStr]]:
        """Get the secret value, loading from provider if necessary.
        
        Args:
            obj: The instance with SecretProviderMixin, or None if accessed on class
            objtype: The type of the instance (not used)
            
        Returns:
            The secret value as a string (default) or SecretStr (if return_secret_str=True),
            or None if the secret is not available or field doesn't exist
            
        Raises:
            ValueError: If secret name field exists but is empty or None
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
        
        # Check cache
        cache_key = (id(obj), secret_name)
        
        if cache_key not in self._cache:
            # Try to load from secret provider
            if hasattr(obj, '_secret_provider') and obj._secret_provider:
                try:
                    secret_value = obj._secret_provider.get_secret(secret_name)
                    
                    # Convert to appropriate format
                    if secret_value and not self.return_secret_str:
                        if isinstance(secret_value, SecretStr):
                            self._cache[cache_key] = secret_value.get_secret_value()
                        else:
                            self._cache[cache_key] = secret_value
                    else:
                        self._cache[cache_key] = secret_value
                        
                except Exception:
                    self._cache[cache_key] = None
            else:
                self._cache[cache_key] = None
                
        return self._cache[cache_key]
    
    def clear_cache(self) -> None:
        """Clear the cached values for all instances.
        
        This can be useful for testing or when secrets need to be reloaded.
        """
        self._cache.clear()