"""Decorators for Bronze layer metadata configuration.

This module provides decorators for configuring Bronze layer ETL processes
with metadata that controls execution behavior and data flow.
"""

from typing import Callable, Optional

from medalflow.types.metadata import BronzeMetadata


def bronze_metadata(
    source_system: str,
    ingestion_mode: str = "incremental",
    description: Optional[str] = None,
    tags: Optional[list[str]] = None
) -> Callable[[type], type]:
    """Decorator for Bronze layer sequencer classes.

    Attaches a :class:`~medalflow.types.metadata.BronzeMetadata` instance to the
    decorated class as ``_bronze_metadata``, mirroring ``gold_metadata`` and
    ``silver_metadata``.

    Args:
        source_system: Name of the system the raw data is ingested from.
        ingestion_mode: How the source is read — "incremental", "full" or
            "append". Defaults to "incremental".
        description: Optional human-readable description of the source.
        tags: Optional list of tags for cataloguing and filtering.

    Returns:
        Class decorator that attaches the metadata and returns the class.

    Example:
        >>> @bronze_metadata(source_system="d365", ingestion_mode="full")
        ... class Customers:
        ...     pass
        >>> Customers._bronze_metadata.source_system
        'd365'
    """
    def decorator(cls: type) -> type:
        metadata = BronzeMetadata(
            source_system=source_system,
            ingestion_mode=ingestion_mode,
            description=description,
            tags=tags or []
        )

        cls._bronze_metadata = metadata
        return cls

    return decorator
