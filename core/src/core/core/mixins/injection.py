"""Dependency injection mixins.

This module provides mixins that enable dependency injection patterns,
allowing classes to receive dependencies at runtime rather than construction time.
"""

from typing import Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from core.protocols.providers import SecretProvider


class SecretProviderMixin:
    """Mixin for attaching secret providers to instances.
    
    This mixin adds the ability to attach a secret provider (like KeyVault)
    to an instance, enabling lazy loading of secrets through descriptors.
    
    The mixin follows the composition pattern, allowing classes to work
    with any secret provider that implements the SecretProvider protocol.
    
    Attributes:
        _secret_provider: The attached secret provider instance
        
    Methods:
        attach_secrets(): Attach a secret provider for lazy loading
        has_secret_provider(): Check if a secret provider is attached
        detach_secrets(): Remove the current secret provider
        
    Example:
        >>> class MySettings(SecretProviderMixin, BaseSettings):
        >>>     api_key = SecretField()
        >>>     
        >>> settings = MySettings()
        >>> settings.attach_secrets(keyvault_provider)
        >>> 
        >>> # Now secrets can be accessed lazily
        >>> key = settings.api_key  # Loads from provider
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize the mixin with no secret provider attached."""
        super().__init__(*args, **kwargs)
        self._secret_provider: Optional['SecretProvider'] = None
    
    def attach_secrets(self, provider: 'SecretProvider') -> 'SecretProviderMixin':
        """Attach a secret provider for lazy loading.
        
        This method attaches a secret provider that will be used by
        SecretField descriptors to load secrets on demand.
        
        Args:
            provider: A secret provider instance implementing SecretProvider protocol
            
        Returns:
            Self for method chaining (fluent interface)
            
        Example:
            >>> settings = MySettings().attach_secrets(keyvault_provider)
        """
        self._secret_provider = provider
        return self
    
    def has_secret_provider(self) -> bool:
        """Check if a secret provider is attached.
        
        Returns:
            True if a secret provider has been attached, False otherwise
        """
        return self._secret_provider is not None
    
    def detach_secrets(self) -> 'SecretProviderMixin':
        """Detach the current secret provider.
        
        This can be useful for testing or when switching providers.
        
        Returns:
            Self for method chaining
        """
        self._secret_provider = None
        return self


class NestedSecretsMixin(SecretProviderMixin):
    """Extended mixin for settings with nested settings objects.
    
    This mixin extends SecretProviderMixin to automatically propagate
    the secret provider to nested settings objects that also use
    SecretProviderMixin.
    
    This is useful for top-level settings classes that contain
    multiple nested settings objects, ensuring all of them have
    access to the same secret provider.
    
    Example:
        >>> class MainSettings(NestedSecretsMixin, BaseSettings):
        >>>     compute: ComputeSettings
        >>>     storage: StorageSettings
        >>>     
        >>>     def attach_secrets(self, provider):
        >>>         super().attach_secrets(provider)
        >>>         # Propagate to nested settings
        >>>         self.propagate_secrets(self.compute, self.storage)
        >>>         return self
    """
    
    def propagate_secrets(self, *nested_settings: SecretProviderMixin) -> None:
        """Propagate secret provider to nested settings objects.
        
        Args:
            *nested_settings: Settings objects that should receive the provider
            
        Example:
            >>> main_settings.propagate_secrets(
            >>>     main_settings.compute,
            >>>     main_settings.storage,
            >>>     main_settings.auth
            >>> )
        """
        if self._secret_provider:
            for settings in nested_settings:
                if isinstance(settings, SecretProviderMixin):
                    settings.attach_secrets(self._secret_provider)