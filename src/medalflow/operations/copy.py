"""Copy and data movement operations.

This module contains operation classes for bulk data operations
like COPY and executing arbitrary SQL.
"""

from typing import Any, Literal

from pydantic import Field, model_validator

from medalflow.constants.compute import ResultFormat
from medalflow.constants.sql import QueryType
from medalflow.operations.base import BaseOperation
from medalflow.types import SQLFragment


class Copy(BaseOperation):
    """Copy data operation.

    High-performance bulk copy from external sources.
    """

    operation_type: Literal[QueryType.COPY] = Field(default=QueryType.COPY, frozen=True)

    source_path: str = Field(..., min_length=1)  # External data path
    file_format: str = Field(default="parquet")
    copy_options: dict[str, Any] = Field(default_factory=dict)
    credential: str | None = Field(default=None)


class ExecuteSQL(BaseOperation):
    """Execute arbitrary SQL operation.

    For operations not covered by specific operation types.
    Supports both DDL/DML operations and SELECT queries.
    Use with caution as it bypasses type safety.
    """

    operation_type: Literal[QueryType.EXECUTE_SQL] = Field(
        default=QueryType.EXECUTE_SQL, frozen=True
    )
    sql: SQLFragment = Field(...)
    returns_results: bool = Field(
        default=False, description="Whether the SQL query returns results (e.g., SELECT queries)"
    )
    result_format: ResultFormat = Field(
        default=ResultFormat.DATAFRAME,
        description="Format for returned results (only applies when returns_results=True)",
    )
    limit: int | None = Field(
        default=None,
        gt=0,
        description="Limit number of rows returned (only applies when returns_results=True)",
    )

    @model_validator(mode="after")
    def validate_result_format(self):
        """Validate that result_format is only used when returns_results is True."""
        if not self.returns_results and self.result_format != ResultFormat.DATAFRAME:
            raise ValueError("result_format can only be set when returns_results=True")
        return self
