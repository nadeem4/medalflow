"""Mixin classes for MedalFlow.

This module provides reusable mixin classes that add specific
functionality to other classes through multiple inheritance.
"""

from .injection import SecretProviderMixin, NestedSecretsMixin

__all__ = [
    "SecretProviderMixin",
    "NestedSecretsMixin",
]