"""Data Manipulation Language (DML) operations.

This module contains operation classes for DML commands like
INSERT, UPDATE, DELETE, MERGE.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, field_validator, model_validator

from core.constants.sql import QueryType
from core.operations.base import BaseOperation


class Select(BaseOperation):
    """Select data operation.
    
    Supports various SELECT query patterns including:
    - Simple SELECT with columns
    - WHERE clause filtering
    - JOIN operations
    - GROUP BY and ORDER BY
    - LIMIT/TOP clauses
    """
    operation_type: Literal[QueryType.SELECT] = Field(
        default=QueryType.SELECT,
        frozen=True
    )
    
    # Column selection
    columns: Optional[List[str]] = Field(default=None)  # None = SELECT *
    distinct: bool = Field(default=False)
    
    # Filtering and joins
    where_clause: Optional[str] = Field(default=None)
    join_clause: Optional[str] = Field(default=None)
    
    # Grouping and ordering
    group_by: Optional[List[str]] = Field(default=None)
    having_clause: Optional[str] = Field(default=None)
    order_by: Optional[List[str]] = Field(default=None)
    
    # Limiting results
    limit: Optional[int] = Field(default=None, gt=0)
    offset: Optional[int] = Field(default=None, ge=0)


class Insert(BaseOperation):
    """Insert data operation.
    
    Supports:
    - INSERT INTO ... SELECT via source_query
    - INSERT VALUES via values
    - Append or overwrite modes
    """
    operation_type: Literal[QueryType.INSERT] = Field(
        default=QueryType.INSERT,
        frozen=True
    )
    
    # Data source (use one)
    source_query: Optional[str] = Field(default=None)  # INSERT INTO ... SELECT
    values: Optional[List[Dict[str, Any]]] = Field(default=None)  # Direct values
    
    # Insert options
    mode: str = Field(default="append", pattern="^(append|overwrite)$")  # append, overwrite
    columns: Optional[List[str]] = Field(default=None)  # Specific columns for insert
    
    @model_validator(mode='after')
    def validate_data_source(self):
        """Ensure exactly one data source is provided."""
        if (self.source_query is None) == (self.values is None):
            raise ValueError("Insert requires exactly one data source: source_query or values")
        return self


class Update(BaseOperation):
    """Update data operation."""
    operation_type: Literal[QueryType.UPDATE] = Field(
        default=QueryType.UPDATE,
        frozen=True
    )
    set_columns: Dict[str, Any] = Field(...)  # Column -> value or expression
    where_clause: Optional[str] = Field(default=None)
    from_clause: Optional[str] = Field(default=None)  # For UPDATE with JOIN
    
    @field_validator('set_columns')
    @classmethod
    def validate_set_columns(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure set_columns is not empty."""
        if not v:
            raise ValueError("set_columns cannot be empty")
        return v


class Delete(BaseOperation):
    """Delete data operation."""
    operation_type: Literal[QueryType.DELETE] = Field(
        default=QueryType.DELETE,
        frozen=True
    )
    where_clause: Optional[str] = Field(default=None)  # None = delete all


class Merge(BaseOperation):
    """Merge (UPSERT) operation.
    
    Performs INSERT, UPDATE, and DELETE in a single atomic operation.
    """
    operation_type: Literal[QueryType.MERGE] = Field(
        default=QueryType.MERGE,
        frozen=True
    )
    
    source_query: str = Field(..., min_length=1)  # Source data query
    merge_condition: str = Field(..., min_length=1)  # Join condition
    
    # Merge actions
    when_matched_update: Optional[Dict[str, Any]] = Field(default=None)
    when_matched_delete: Optional[str] = Field(default=None)  # Condition for delete
    when_not_matched_insert: Optional[Dict[str, Any]] = Field(default=None)
    when_not_matched_by_source_update: Optional[Dict[str, Any]] = Field(default=None)
    when_not_matched_by_source_delete: bool = Field(default=False)
    
    @model_validator(mode='after')
    def validate_merge_actions(self):
        """Ensure at least one merge action is specified."""
        actions = [
            self.when_matched_update is not None,
            self.when_matched_delete is not None,
            self.when_not_matched_insert is not None,
            self.when_not_matched_by_source_update is not None,
            self.when_not_matched_by_source_delete
        ]
        if not any(actions):
            raise ValueError("Merge requires at least one action to be specified")
        return self