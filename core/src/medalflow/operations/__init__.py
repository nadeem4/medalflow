"""Database operations module.

This module provides data structures that describe database operations
independent of how they are executed. Operations are pure data that can be:
- Transformed into SQL by query builders
- Executed by compute engines
- Serialized for remote execution

The operations module is in Layer 1, making it available to all Layer 2
business logic modules without creating circular dependencies.
"""

# Base operation
from medalflow.operations.base import BaseOperation

# DDL operations
from medalflow.operations.ddl import (
    CreateTable,
    DropTable,
    CreateSchema,
    DropSchema
)

# DML operations  
from medalflow.operations.dml import (
    Select,
    Insert,
    Update,
    Delete,
    Merge
)

# View operations
from medalflow.operations.views import (
    CreateOrAlterView,
    DropView
)

# Statistics operations
from medalflow.operations.statistics import CreateStatistics

# Copy and misc operations
from medalflow.operations.copy import (
    Copy,
    ExecuteSQL
)

# Query context
from medalflow.operations.context import QueryContext

# Builder
from medalflow.operations.builder import OperationBuilder

__all__ = [
    # Base
    "BaseOperation",
    # DDL
    "CreateTable",
    "DropTable", 
    "CreateSchema",
    "DropSchema",
    # DML
    "Select",
    "Insert",
    "Update",
    "Delete",
    "Merge",
    # Views
    "CreateOrAlterView",
    "DropView",
    # Statistics
    "CreateStatistics",
    # Copy
    "Copy",
    "ExecuteSQL",
    # Context
    "QueryContext",
    # Builder
    "OperationBuilder"
]