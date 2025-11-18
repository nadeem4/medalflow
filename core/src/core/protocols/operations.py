"""Operation protocol definitions and base types.

This module defines protocols and base types for database operations
that are used across the MedalFlow framework. These definitions belong
in Layer 0 as they have no dependencies and provide the foundation
for SQL operations throughout the system.
"""

from typing import Protocol, Optional, Any, Dict, List
import re

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator



class ColumnDefinition(BaseModel):
    """Column definition for table creation.
    
    Users must provide platform-specific SQL data types in the data_type field.
    The data_type should include the complete type specification including
    size/precision where applicable.
    
    Common Platform-Specific Types:
    
    **Azure Synapse / Microsoft Fabric Warehouse (T-SQL):**
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
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    name: str = Field(..., min_length=1, max_length=128)
    data_type: str = Field(
        ..., 
        min_length=1,
        description="Platform-specific SQL data type (e.g., 'NVARCHAR(60)', 'INT', 'DECIMAL(10,2)')"
    )
    nullable: bool = Field(default=True)
    default_value: Optional[Any] = Field(default=None)
    primary_key: bool = Field(default=False)
    unique: bool = Field(default=False)
    check_constraint: Optional[str] = Field(default=None)
    collation: Optional[str] = Field(default=None)
    computed_expression: Optional[str] = Field(default=None)
    
    @field_validator('name')
    @classmethod
    def validate_column_name(cls, v: str) -> str:
        """Validate column name follows SQL naming rules."""
        if not v:
            raise ValueError("Column name cannot be empty")
        
        # Check for valid SQL identifier
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', v):
            raise ValueError(
                f"Invalid column name: '{v}'. "
                f"Must start with letter or underscore, and contain only alphanumeric or underscore."
            )
        
        return v
    
    @model_validator(mode='after')
    def validate_constraints(self):
        """Validate constraint combinations."""
        if self.computed_expression and self.default_value is not None:
            raise ValueError("Computed columns cannot have default values")
        return self