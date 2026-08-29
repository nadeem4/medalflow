"""Operation protocol definitions and base types.

This module defines protocols and base types for database operations
that are used across the MedalFlow framework. These definitions belong
in Layer 0 as they have no dependencies and provide the foundation
for SQL operations throughout the system.
"""

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from medalflow.types import RawSQL, SQLFragment

#: A bare `data_type` is emitted straight into the column list, so it is held
#: to the shape of a type name: an identifier, optionally followed by a size
#: or precision -- `INT`, `NVARCHAR(MAX)`, `DECIMAL(18,2)`. Anything richer
#: (`ARRAY<STRING>`, `STRUCT<...>`) must say `RawSQL` and own the risk.
_SQL_TYPE_NAME = re.compile(
    r"^[A-Za-z][A-Za-z0-9_]*\s*(\(\s*(MAX|\d+(\s*,\s*\d+)?)\s*\))?$", re.IGNORECASE
)


class ColumnDefinition(BaseModel):
    """Column definition for table creation.

    ``data_type`` is emitted into the generated SQL verbatim, so it has to be
    a type the target warehouse accepts. Nothing here translates between
    dialects: the validator checks only that the string is *shaped* like a
    type name, which is a safety rule, not a compatibility one. A type your
    warehouse does not know reaches it unchanged and fails there.

    Synapse (T-SQL) is the warehouse MedalFlow generates for today:
        - Strings: NVARCHAR(60), NVARCHAR(MAX), VARCHAR(100), CHAR(10)
        - Numbers: INT, BIGINT, SMALLINT, TINYINT, DECIMAL(18,2), NUMERIC(10,5), FLOAT, REAL
        - Dates: DATETIME2, DATETIME, DATE, TIME, DATETIMEOFFSET
        - Boolean: BIT
        - Binary: VARBINARY(MAX), BINARY(100)
        - Others: UNIQUEIDENTIFIER, XML, JSON

    Include the full specification, size and precision included. A type too
    rich for the shape rule -- ``ARRAY<STRING>``, ``STRUCT<...>`` -- has to be
    declared :class:`~medalflow.types.RawSQL`, which skips validation and
    makes the risk the caller's.

    Examples:
        >>> ColumnDefinition(name="customer_name", data_type="NVARCHAR(100)")
        >>> ColumnDefinition(name="price", data_type="DECIMAL(10,2)")
        >>> ColumnDefinition(name="id", data_type="BIGINT", nullable=False, primary_key=True)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., min_length=1, max_length=128)
    data_type: str | RawSQL = Field(
        ...,
        description="Platform-specific SQL data type (e.g., 'NVARCHAR(60)', 'INT', 'DECIMAL(10,2)')",
    )
    nullable: bool = Field(default=True)
    default_value: Any | None = Field(default=None)
    primary_key: bool = Field(default=False)
    unique: bool = Field(default=False)
    check_constraint: SQLFragment | None = Field(default=None)
    collation: str | None = Field(default=None)
    computed_expression: str | None = Field(default=None)

    @field_validator("name")
    @classmethod
    def validate_column_name(cls, v: str) -> str:
        """Validate column name follows SQL naming rules."""
        if not v:
            raise ValueError("Column name cannot be empty")

        # Check for valid SQL identifier
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                f"Invalid column name: '{v}'. "
                f"Must start with letter or underscore, and contain only alphanumeric or underscore."
            )

        return v

    @field_validator("data_type")
    @classmethod
    def validate_data_type(cls, v: str | RawSQL) -> str | RawSQL:
        """Hold a bare string to the shape of a SQL type name."""
        if isinstance(v, RawSQL):
            return v
        if not _SQL_TYPE_NAME.match(v.strip()):
            raise ValueError(
                f"Invalid data type: {v!r}. Expected a type name such as "
                "'INT' or 'DECIMAL(18,2)'; wrap anything richer in RawSQL(...)."
            )
        return v

    @model_validator(mode="after")
    def validate_constraints(self):
        """Validate constraint combinations."""
        if self.computed_expression and self.default_value is not None:
            raise ValueError("Computed columns cannot have default values")
        return self
