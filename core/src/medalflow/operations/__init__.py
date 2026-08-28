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

# Builder
from medalflow.operations.builder import OperationBuilder

# Query context
from medalflow.operations.context import QueryContext

# Copy and misc operations
from medalflow.operations.copy import Copy, ExecuteSQL

# DDL operations
from medalflow.operations.ddl import CreateSchema, CreateTable, DropSchema, DropTable

# DML operations
from medalflow.operations.dml import Delete, Insert, Merge, Select, Update

# Statistics operations
from medalflow.operations.statistics import CreateStatistics

# View operations
from medalflow.operations.views import CreateOrAlterView, DropView

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
    "OperationBuilder",
]
