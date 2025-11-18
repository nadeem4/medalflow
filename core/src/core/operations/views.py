"""View-related operations.

This module contains operation classes for managing database views.
"""

from typing import List, Literal, Optional

from pydantic import Field

from core.constants.sql import QueryType
from core.operations.base import BaseOperation


class CreateOrAlterView(BaseOperation):
    """Create or alter view operation."""
    operation_type: Literal[QueryType.CREATE_OR_ALTER_VIEW] = Field(
        default=QueryType.CREATE_OR_ALTER_VIEW,
        frozen=True
    )
    
    select_query: str = Field(..., min_length=1)
    columns: Optional[List[str]] = Field(default=None)  # Column aliases
    with_schemabinding: bool = Field(default=False)
    materialized: bool = Field(default=False)
    or_replace: bool = Field(default=True)  # Whether to use CREATE OR REPLACE


class DropView(BaseOperation):
    """Drop view operation."""
    operation_type: Literal[QueryType.DROP_VIEW] = Field(
        default=QueryType.DROP_VIEW,
        frozen=True
    )
    if_exists: bool = Field(default=True)