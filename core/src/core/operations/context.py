"""Query execution context.

This module contains classes that provide context and hints
for query execution and engine selection.
"""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from core.constants.compute import EngineType


class QueryContext(BaseModel):
    """Context for query execution and engine selection.
    
    Provides hints and metadata to help platforms make intelligent
    decisions about how to execute operations.
    """
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        use_enum_values=False
    )
    
    preferred_engine: EngineType = Field(default=EngineType.AUTO)
    query_complexity: int = Field(default=1, ge=1, le=10)  # 1-10 scale
    estimated_rows: int = Field(default=0, ge=0)
    has_complex_transformations: bool = Field(default=False)
    timeout_seconds: Optional[int] = Field(default=None, gt=0)