"""Data Definition Language (DDL) operations.

This module contains operation classes for DDL commands like
CREATE TABLE, DROP TABLE, CREATE SCHEMA, etc.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, model_validator

from core.constants.sql import QueryType
from core.protocols.operations import ColumnDefinition
from core.operations.base import BaseOperation


class CreateTable(BaseOperation):
    """Create table operation.
    
    Supports multiple table creation patterns:
    - CREATE TABLE AS SELECT (CTAS) via select_query
    - External tables via location
    - Empty tables via columns definition
    - Copy schema from existing table via source_table
    """
    operation_type: Literal[QueryType.CREATE_TABLE] = Field(
        default=QueryType.CREATE_TABLE,
        frozen=True
    )
    
    # Table definition options
    columns: Optional[List[ColumnDefinition]] = Field(default=None)
    select_query: Optional[str] = Field(default=None)  
    source_table: Optional[str] = Field(default=None)  
    
    # External table options
    location: Optional[str] = Field(default=None)  
    file_format: str = Field(default="parquet")
    
    # Table properties
    partitions: Optional[List[str]] = Field(default=None)
    distribution: Optional[str] = Field(default=None)  # HASH, ROUND_ROBIN, REPLICATE
    properties: Dict[str, Any] = Field(default_factory=dict)
    recreate: bool = Field(
        default=True,
        description="If True, drop and recreate table if it exists. If False, only create if not exists."
    ) 
    
    @model_validator(mode='after')
    def validate_table_definition(self):
        """Ensure at least one table definition method is provided."""
        definition_methods = [
            self.columns is not None,
            self.select_query is not None,
            self.source_table is not None,
            self.location is not None
        ]
        if not any(definition_methods):
            raise ValueError(
                "CreateTable requires at least one definition method: "
                "columns, select_query, source_table, or location"
            )
        return self
    
    @model_validator(mode='after')
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
    operation_type: Literal[QueryType.DROP_TABLE] = Field(
        default=QueryType.DROP_TABLE,
        frozen=True
    )
    if_exists: bool = Field(default=True)
    cascade: bool = Field(default=False)


class CreateSchema(BaseOperation):
    """Create schema operation."""
    operation_type: Literal[QueryType.CREATE_SCHEMA] = Field(
        default=QueryType.CREATE_SCHEMA,
        frozen=True
    )
    if_not_exists: bool = Field(default=True)
    authorization: Optional[str] = Field(default=None)


class DropSchema(BaseOperation):
    """Drop schema operation."""
    operation_type: Literal[QueryType.DROP_SCHEMA] = Field(
        default=QueryType.DROP_SCHEMA,
        frozen=True
    )
    if_exists: bool = Field(default=True)
    cascade: bool = Field(default=False)
    restrict: bool = Field(default=False)