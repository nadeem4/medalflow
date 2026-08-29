"""Decorators for Bronze layer metadata configuration.

This module provides decorators for configuring Bronze layer ETL processes
with metadata that controls execution behavior and data flow.
"""

from collections.abc import Callable

from medalflow.types.metadata import BronzeMetadata


def bronze_metadata(
    name: str,
    schema: str,
    source_system: str,
    source_schema: str | None = None,
    source_table: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    disabled: bool = False,
) -> Callable[[type], type]:
    """Decorator declaring one Bronze model, which is one Bronze table.

    Attaches a :class:`~medalflow.types.metadata.BronzeMetadata` instance to the
    decorated class as ``_bronze_metadata``, mirroring ``gold_metadata`` and
    ``silver_metadata``. Bronze discovery reads it, so a declared model needs no
    warehouse to compile (ADR 002, Decision 6).

    Args:
        name: The model's identity, and the name of the Bronze table it builds.
        schema: Target schema the Bronze table is created in.
        source_system: Name of the system the raw data is ingested from.
        source_schema: Source schema to read from. Defaults to the sequencer's
            own ``source_schema``.
        source_table: Source table to read from. Defaults to ``name``.
        description: Optional human-readable description of the source.
        tags: Optional list of tags for cataloguing and filtering.
        disabled: If True, discovery leaves this model out of the plan.

    Returns:
        Class decorator that attaches the metadata and returns the class.

    Example:
        >>> @bronze_metadata(name="Customers", schema="bronze", source_system="d365")
        ... class Customers:
        ...     pass
        >>> Customers._bronze_metadata.source_table
        'Customers'
    """

    def decorator(cls: type) -> type:
        metadata = BronzeMetadata(
            name=name,
            schema=schema,
            source_system=source_system,
            source_schema=source_schema,
            source_table=source_table or name,
            description=description,
            tags=tags or [],
            disabled=disabled,
        )

        cls._bronze_metadata = metadata
        return cls

    return decorator
