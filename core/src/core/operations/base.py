"""Base operation definitions.

This module defines the base operation class that all database operations
inherit from. Operations are data structures that describe what database
action should be performed, independent of how it's executed.
"""

import re
from typing import Any, Dict, Optional

from pydantic import Field, field_validator

from core.constants.compute import EngineType
from core.constants.sql import QueryType
from core.observability.context import ExecutionRequestContext
from core.types import QueryMetadata
from core.types.base import CTEBaseModel


def _stringify(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return str(value)


class BaseOperation(CTEBaseModel):
    """Base class for all database operations.
    
    This is the fundamental abstraction that allows platform-agnostic
    database operations. Each specific operation type extends this base.
    
    Operations are pure data structures that describe WHAT to do,
    not HOW to do it. They are transformed into SQL by query builders
    and executed by compute engines.
    
    Attributes:
        operation_type: The type of operation to perform
        schema_name: Database schema name
        object_name: Name of the database object (table/view/etc)
        engine_hint: Optional hint for engine selection (SQL/SPARK/AUTO)
    """
    operation_type: QueryType
    schema_name: str = Field(..., min_length=1, max_length=128)
    object_name: str = Field(..., min_length=1, max_length=128)
    engine_hint: Optional[EngineType] = Field(default=None)
    logging_context: Optional[dict] = Field(default_factory=dict, description="Optional operation name for logging/tracking")
    metadata: Optional[QueryMetadata] = Field(default=None, description="Optional metadata for the operation")
    context: Optional[ExecutionRequestContext] = Field(
        default=None,
        description="Observability context for this operation",
    )
    
    @field_validator('schema_name', 'object_name')
    @classmethod
    def validate_sql_identifier(cls, v: str, info) -> str:
        """Validate SQL identifiers to prevent injection."""
        if not v:
            raise ValueError(f"{info.field_name} cannot be empty")
        
        # Allow alphanumeric, underscore, and limited special chars
        # This pattern prevents SQL injection while allowing valid identifiers
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_$#@]*$', v):
            raise ValueError(
                f"Invalid {info.field_name}: '{v}'. "
                f"Must start with letter or underscore, and contain only alphanumeric, underscore, $, #, or @ characters."
            )
        
        # Check length
        if len(v) > 128:
            raise ValueError(f"{info.field_name} too long: maximum 128 characters")
            
        return v
    
    def get_table_prefix(self) -> str:
        """Get the full table prefix including schema."""

        from core.settings import get_settings
        settings = get_settings()
        return settings.table_prefix

    @property
    def full_object_name(self) -> str:
        """Get the full object name with prefix (no schema)."""
        prefix = self.get_table_prefix()
        full_name = f"{self.schema_name}.{prefix}_{self.object_name}"
        return full_name
    
    @property
    def full_object_name_no_schema(self) -> str:
        """Get the full object name with prefix (no schema)."""
        prefix = self.get_table_prefix()
        return f"{prefix}_{self.object_name}"

    def attach_context(
        self,
        ctx: ExecutionRequestContext,
    ) -> None:
        """Attach observability context to the operation."""
        self.context = ctx
        if self.logging_context:
            self.context.attributes.update(self.logging_context)
        if self.engine_hint is not None:
            self.context.attributes['engine_hint'] = self.engine_hint.value
        self.context.telemetry_base = self.context.to_telemetry_dict()

    def telemetry_fields(self) -> Dict[str, str]:
        """Return flattened telemetry fields describing this operation."""
        payload: Dict[str, str] = {
            "operation.type": str(self.operation_type),
            "operation.schema": self.schema_name,
            "operation.object": self.object_name,
        }
        if self.engine_hint:
            payload["operation.engine_hint"] = self.engine_hint.value
        if self.metadata and getattr(self.metadata, "operation_id", None):
            payload["operation.id"] = str(self.metadata.operation_id)
        for key, value in (self.logging_context or {}).items():
            sanitized = _stringify(value)
            if sanitized is not None:
                payload[f"operation.ctx.{key}"] = sanitized
        return payload

    def observability_attributes(self) -> Dict[str, str]:
        """Return key attributes useful for logging/metrics."""
        attrs: Dict[str, str] = {
            "schema": self.schema_name,
            "object": self.object_name,
            "operation_type": str(self.operation_type),
        }
        for key, value in (self.logging_context or {}).items():
            sanitized = _stringify(value)
            if sanitized is not None:
                attrs[f"context_{key}"] = sanitized
        return attrs
