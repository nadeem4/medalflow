"""Compute-specific types and results.

This module contains types that are specific to compute execution,
such as operation results, job configurations, and execution metadata.
Operations themselves have been moved to the operations module.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pydantic import ConfigDict, Field, model_validator

from core.types.base import CTEBaseModel
from core.constants.compute import EngineType
from core.constants.sql import QueryType


class OperationResult(CTEBaseModel):
    """Result of an operation execution.
    
    Provides comprehensive information about the operation outcome,
    including success status, timing, and any error details.
    This is compute-specific as it contains execution metadata.
    
    Attributes:
        success: Whether the operation completed successfully
        operation_type: Type of operation that was executed
        schema_name: Schema where operation was performed
        object_name: Name of the affected object
        duration_seconds: Time taken to execute
        rows_affected: Number of rows affected (for DML operations)
        data: Query result data (for SELECT operations with returns_results=True)
        error_message: Error details if operation failed
        error_type: Type of error (for categorization)
        engine_used: Which engine executed the operation
        query_executed: Actual SQL query that was run
        statistics: Additional metrics (platform-specific)
    """
    success: bool
    operation_type: QueryType
    schema_name: str = Field(..., min_length=1, max_length=128)
    object_name: str = Field(..., min_length=1, max_length=128)
    duration_seconds: float = Field(..., ge=0.0)
    
    # Optional details
    rows_affected: Optional[int] = Field(default=None, ge=0)
    data: Optional[Union[pd.DataFrame, List[Dict[str, Any]], Any]] = Field(
        default=None,
        description="Query result data - DataFrame, list of dicts, or scalar value"
    )
    error_message: Optional[str] = Field(default=None)
    error_type: Optional[str] = Field(default=None)
    engine_used: Optional[EngineType] = Field(default=None)
    query_executed: Optional[str] = Field(default=None)
    statistics: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def full_object_name(self) -> str:
        """Get fully qualified object name."""
        return f"{self.schema_name}.{self.object_name}"


class BatchOperationResult(CTEBaseModel):
    """Result of batch operation execution."""
    total_operations: int = Field(..., ge=0)
    successful_operations: int = Field(..., ge=0)
    failed_operations: int = Field(..., ge=0)
    results: List[OperationResult] = Field(default_factory=list)
    total_duration_seconds: float = Field(..., ge=0.0)
    used_transaction: bool = Field(default=False)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_operations == 0:
            return 0.0
        return (self.successful_operations / self.total_operations) * 100
    
    @model_validator(mode='after')
    def validate_operation_counts(self):
        """Ensure operation counts are consistent."""
        if self.successful_operations + self.failed_operations != self.total_operations:
            raise ValueError(
                f"Operation counts don't match: "
                f"{self.successful_operations} + {self.failed_operations} != {self.total_operations}"
            )
        if len(self.results) != self.total_operations:
            raise ValueError(
                f"Results count ({len(self.results)}) doesn't match total operations ({self.total_operations})"
            )
        return self
