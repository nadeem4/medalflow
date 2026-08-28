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

    Users must provide platform-specific SQL data types in the data_type field.
    The data_type should include the complete type specification including
    size/precision where applicable.

    Common Platform-Specific Types:

    **Azure Synapse (T-SQL):**
        - Strings: NVARCHAR(60), NVARCHAR(MAX), VARCHAR(100), CHAR(10)
        - Numbers: INT, BIGINT, SMALLINT, TINYINT, DECIMAL(18,2), NUMERIC(10,5), FLOAT, REAL
        - Dates: DATETIME2, DATETIME, DATE, TIME, DATETIMEOFFSET
        - Boolean: BIT
        - Binary: VARBINARY(MAX), BINARY(100)
        - Others: UNIQUEIDENTIFIER, XML, JSON

    **Databricks / Spark SQL:**
        - Strings: STRING, VARCHAR(100), CHAR(10)
        - Numbers: INT, BIGINT, SMALLINT, TINYINT, DECIMAL(18,2), DOUBLE, FLOAT
        - Dates: TIMESTAMP, DATE, INTERVAL
        - Boolean: BOOLEAN
        - Binary: BINARY
        - Complex: ARRAY<type>, MAP<key_type, value_type>, STRUCT<fields>

    **Snowflake:**
        - Strings: VARCHAR(16777216), STRING, TEXT, CHAR(10)
        - Numbers: NUMBER(38,0), INT, BIGINT, DECIMAL(18,2), FLOAT, DOUBLE
        - Dates: TIMESTAMP_NTZ, TIMESTAMP_TZ, DATE, TIME
        - Boolean: BOOLEAN
        - Binary: BINARY, VARBINARY
        - Semi-structured: VARIANT, OBJECT, ARRAY

    Examples:
        >>> # For a string column that stores names (T-SQL)
        >>> ColumnDefinition(name="customer_name", data_type="NVARCHAR(100)")

        >>> # For a decimal column (works across platforms)
        >>> ColumnDefinition(name="price", data_type="DECIMAL(10,2)")

        >>> # For Databricks/Spark
        >>> ColumnDefinition(name="description", data_type="STRING")

        >>> # For a non-nullable ID column
        >>> ColumnDefinition(name="id", data_type="BIGINT", nullable=False, primary_key=True)

    Note: When migrating between platforms, ensure data types are compatible
    or adjust them accordingly for the target platform.
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
