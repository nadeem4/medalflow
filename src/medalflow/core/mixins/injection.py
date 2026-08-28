"""Dependency injection mixins.

This module provides mixins that enable dependency injection patterns,
allowing classes to receive dependencies at runtime rather than construction time.
"""

from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, PrivateAttr

if TYPE_CHECKING:
    from medalflow.protocols.providers import SecretProvider


class SecretProviderMixin(BaseModel):
    """Mixin for attaching secret providers to pydantic models.

    This mixin adds the ability to attach a secret provider (like KeyVault)
    to a model, enabling lazy loading of secrets through
    :class:`~medalflow.core.descriptors.SecretField` descriptors.

    ``_secret_provider`` is declared as a pydantic ``PrivateAttr`` rather than
    being assigned in ``__init__``. An ``__init__`` override cannot be used
    here: ``model_post_init`` runs *inside* ``BaseModel.__init__``, so any
    assignment made after ``super().__init__(...)`` returns would silently undo
    a provider attached during post-init.

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

    _secret_provider: Optional["SecretProvider"] = PrivateAttr(default=None)

    def attach_secrets(self, provider: "SecretProvider") -> "SecretProviderMixin":
        """Attach a secret provider for lazy loading.

        Args:
            provider: A secret provider instance implementing SecretProvider protocol

        Returns:
            Self for method chaining (fluent interface)
        """
        self._secret_provider = provider
        return self

    def has_secret_provider(self) -> bool:
        """Check if a secret provider is attached."""
        return self._secret_provider is not None

    def detach_secrets(self) -> "SecretProviderMixin":
        """Detach the current secret provider."""
        self._secret_provider = None
        return self


class NestedSecretsMixin(SecretProviderMixin):
    """Extended mixin for settings with nested settings objects.

    Adds :meth:`propagate_secrets`, which hands the attached provider to
    nested models that also use :class:`SecretProviderMixin`.

    Example:
        >>> class MainSettings(NestedSecretsMixin, BaseSettings):
        >>>     compute: ComputeSettings
        >>>     storage: StorageSettings
        >>>
        >>>     def model_post_init(self, __context) -> None:
        >>>         self.attach_secrets(provider)
        >>>         self.propagate_secrets(self.compute, self.storage)
    """

    def propagate_secrets(self, *nested_settings: SecretProviderMixin) -> None:
        """Propagate the attached secret provider to nested settings objects.

        Args:
            *nested_settings: Settings objects that should receive the provider
        """
        if self._secret_provider:
            for settings in nested_settings:
                if isinstance(settings, SecretProviderMixin):
                    settings.attach_secrets(self._secret_provider)
