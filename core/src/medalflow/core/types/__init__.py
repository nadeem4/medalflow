"""Core type definitions for cross-cutting concerns.

This package contains type definitions that are used across multiple
layers of the application. These types are part of the core infrastructure
(Layer 1) and can be used by any higher layer (compute, medallion, datalake, etc.).

Types included:
- StatsConfiguration: Statistics metadata configuration
"""

from .stats import StatsConfiguration

__all__ = [
    'StatsConfiguration',
]