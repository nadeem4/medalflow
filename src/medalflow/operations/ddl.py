"""Data Definition Language (DDL) operations.

This module contains operation classes for DDL commands like
CREATE TABLE, DROP TABLE, CREATE SCHEMA, etc.
"""

import re
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from medalflow.constants.sql import QueryType
from medalflow.operations.base import BaseOperation
from medalflow.operations.columns import ColumnDefinition
from medalflow.types import SQLFragment

#: A `location` is a data lake path, not SQL. Each `/`-separated segment may
#: hold letters, digits, underscore, dot and hyphen -- which excludes the
#: quote that would end the LOCATION literal it is emitted into.
_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9_.\-]*$")


class CreateTable(BaseOperation):
    """Create table operation.

    Supports multiple table creation patterns:
    - CREATE TABLE AS SELECT (CTAS) via select_query
    - External tables via location
    - Empty tables via columns definition
    - Copy schema from existing table via source_table
    """

    operation_type: Literal[QueryType.CREATE_TABLE] = Field(
        default=QueryType.CREATE_TABLE, frozen=True
    )

    # Table definition options
    columns: list[ColumnDefinition] | None = Field(default=None)
    select_query: SQLFragment | None = Field(default=None)
    source_table: str | None = Field(default=None)

    # External table options
    location: str | None = Field(default=None)
    file_format: str = Field(default="parquet")

    # Table properties
    partitions: list[str] | None = Field(default=None)
    distribution: str | None = Field(default=None)  # HASH, ROUND_ROBIN, REPLICATE
    properties: dict[str, Any] = Field(default_factory=dict)
    recreate: bool = Field(
        default=True,
        description="If True, drop and recreate table if it exists. If False, only create if not exists.",
    )

    @field_validator("location")
    @classmethod
    def validate_location_is_a_path(cls, v: str | None) -> str | None:
        """Reject anything that is not a plain data lake path."""
        if v is None:
            return v
        for segment in v.split("/"):
            if segment == "..":
                raise ValueError(f"location may not traverse upwards: {v!r}")
            if not _PATH_SEGMENT.match(segment):
                raise ValueError(
                    f"Invalid location segment {segment!r} in {v!r}. "
                    "Locations are paths: letters, digits, underscore, dot, "
                    "hyphen and / only."
                )
        return v

    @model_validator(mode="after")
    def validate_table_definition(self):
        """Ensure at least one table definition method is provided."""
        definition_methods = [
            self.columns is not None,
            self.select_query is not None,
            self.source_table is not None,
            self.location is not None,
        ]
        if not any(definition_methods):
            raise ValueError(
                "CreateTable requires at least one definition method: "
                "columns, select_query, source_table, or location"
            )
        return self

    @model_validator(mode="after")
    def set_default_location(self):
        """Set default location if not provided.

        For external tables created with CTAS, if location is not explicitly set,
        default to schema_name/object_name pattern. This ensures consistent
        data lake organization following the medallion architecture.
        """
        if self.location is None and self.select_query is not None:
            self.location = f"{self.schema_name}/{self.object_name}"

        return self


class DropTable(BaseOperation):
    """Drop table operation."""

    operation_type: Literal[QueryType.DROP_TABLE] = Field(default=QueryType.DROP_TABLE, frozen=True)
    if_exists: bool = Field(default=True)
    cascade: bool = Field(default=False)


class CreateSchema(BaseOperation):
    """Create schema operation."""

    operation_type: Literal[QueryType.CREATE_SCHEMA] = Field(
        default=QueryType.CREATE_SCHEMA, frozen=True
    )
    if_not_exists: bool = Field(default=True)
    authorization: str | None = Field(default=None)


class DropSchema(BaseOperation):
    """Drop schema operation."""

    operation_type: Literal[QueryType.DROP_SCHEMA] = Field(
        default=QueryType.DROP_SCHEMA, frozen=True
    )
    if_exists: bool = Field(default=True)
    cascade: bool = Field(default=False)
    restrict: bool = Field(default=False)
