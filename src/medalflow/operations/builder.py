"""Operation builder for creating database operations.

This module provides a builder class for creating operation instances
based on QueryType. It implements a registry-based builder pattern
to eliminate if-else chains throughout the codebase.

This module consolidates all operation creation logic in the operations
package (Layer 1), making it available to all higher layers.
"""

from typing import Any

from medalflow.constants.compute import EngineType
from medalflow.constants.sql import QueryType
from medalflow.logging import get_logger
from medalflow.observability.context import ExecutionRequestContext
from medalflow.operations.base import BaseOperation
from medalflow.operations.copy import Copy, ExecuteSQL
from medalflow.operations.ddl import (
    CreateSchema,
    CreateTable,
    DropSchema,
    DropTable,
)
from medalflow.operations.dml import (
    Delete,
    Insert,
    Merge,
    Select,
    Update,
)
from medalflow.operations.statistics import CreateStatistics
from medalflow.operations.views import CreateOrAlterView, DropView
from medalflow.types import QueryMetadata

logger = get_logger(__name__)


class OperationBuilder:
    """Builder for creating operation instances based on QueryType.

    This builder provides a centralized way to create operations from QueryType
    values without if-else chains. It maintains an internal registry of
    QueryType to Operation class mappings.

    Example:
        >>> # Direct creation with parameters
        >>> operation = OperationBuilder.create_operation(
        ...     QueryType.INSERT,
        ...     schema="dbo",
        ...     object_name="users",
        ...     source_query="SELECT * FROM temp_users",
        ...     mode="append"
        ... )
    """

    # Registry mapping QueryType to Operation class
    _registry: dict[QueryType, type[BaseOperation]] = {
        QueryType.SELECT: Select,
        QueryType.INSERT: Insert,
        QueryType.UPDATE: Update,
        QueryType.DELETE: Delete,
        QueryType.MERGE: Merge,
        QueryType.CREATE_TABLE: CreateTable,
        QueryType.DROP_TABLE: DropTable,
        QueryType.CREATE_SCHEMA: CreateSchema,
        QueryType.DROP_SCHEMA: DropSchema,
        QueryType.CREATE_OR_ALTER_VIEW: CreateOrAlterView,
        QueryType.DROP_VIEW: DropView,
        QueryType.CREATE_STATISTICS: CreateStatistics,
        QueryType.COPY: Copy,
        QueryType.EXECUTE_SQL: ExecuteSQL,
    }

    @classmethod
    def create_operation(
        cls,
        query_type: QueryType,
        schema_name: str,
        object_name: str,
        engine_hint: EngineType = EngineType.SQL,
        logging_context: dict | None = None,
        metadata: QueryMetadata | None = None,
        **kwargs: Any,
    ) -> BaseOperation:
        """Create an operation instance from QueryType and parameters.

        Args:
            query_type: The type of query operation to create
            schema_name: Database schema name
            object_name: Name of the database object
            **kwargs: Additional operation-specific parameters

        Returns:
            Configured operation instance

        Raises:
            ValueError: If parameters are invalid for the operation type

        Example:
            >>> operation = OperationBuilder.create_operation(
            ...     QueryType.INSERT,
            ...     schema_name="dbo",
            ...     object_name="users",
            ...     source_query="SELECT * FROM temp_users"
            ... )
        """
        # `QueryMetadata.type` is stored as a plain `str` (`use_enum_values=True`),
        # so every call from a sequencer arrives with one. Both messages below
        # used to read `query_type.value` and raise `AttributeError` on it --
        # inside the handler, which meant the *real* failure never surfaced.
        type_label = getattr(query_type, "value", query_type)

        # Get operation class from registry
        operation_class = cls._registry.get(query_type)

        if operation_class is None:
            raise ValueError(
                f"No operation class registered for query type "
                f"'{type_label}'. Register one in OperationBuilder._registry."
            )

        # Create operation instance
        try:
            return operation_class(
                schema_name=schema_name,
                object_name=object_name,
                engine_hint=engine_hint,
                logging_context=logging_context,
                metadata=metadata,
                **kwargs,
            )
        except Exception as e:
            logger.error(
                f"Failed to create {operation_class.__name__} for "
                f"{schema_name}.{object_name}: {e}"
            )
            raise ValueError(f"Cannot create operation {type_label}: {e}") from e

    @classmethod
    def create_operation_from_dict(cls, operation_dict: dict) -> BaseOperation:
        """Create operation instance from dictionary.

        Deserializes operations that were serialized using CTEBaseModel.to_dict() method.

        Args:
            operation_dict: Serialized operation from CTEBaseModel.to_dict()

        Returns:
            BaseOperation instance

        Raises:
            ValueError: If operation type is unknown or data is invalid

        Example:
            >>> op = Insert(schema="silver", object_name="customers", ...)
            >>> op_dict = op.to_dict()  # Serialize using inherited method
            >>> restored_op = OperationBuilder.create_operation_from_dict(op_dict)
        """
        stage = operation_dict.pop("_cte_stage", None)
        position = operation_dict.pop("_cte_position", None)
        ctx_dict = operation_dict.pop("_cte_request_context", None)

        # Get operation type
        operation_type_value = operation_dict.get("operation_type")
        if not operation_type_value:
            raise ValueError("operation_type is required in operation dictionary")

        # Convert string to QueryType enum if needed
        if isinstance(operation_type_value, str):
            try:
                query_type = QueryType(operation_type_value)
            except ValueError as e:
                raise ValueError(f"Invalid operation_type: {operation_type_value}") from e
        else:
            query_type = operation_type_value

        # Get operation class from registry
        operation_class = cls._registry.get(query_type)
        if not operation_class:
            raise ValueError(
                f"No operation class registered for query type "
                f"'{query_type.value}'. Register one in OperationBuilder._registry."
            )

        # Handle nested metadata if present
        if "metadata" in operation_dict and operation_dict["metadata"]:
            from medalflow.types.metadata import QueryMetadata

            if isinstance(operation_dict["metadata"], dict):
                # Reconstruct QueryMetadata from dict
                operation_dict["metadata"] = QueryMetadata.model_validate(
                    operation_dict["metadata"]
                )

        # Create operation using Pydantic's validation
        try:
            operation = operation_class.model_validate(operation_dict)
        except Exception as e:
            logger.error(f"Failed to create {operation_class.__name__} from dict: {e}")
            raise ValueError(f"Invalid operation data for {query_type.value}: {e}") from e

        if ctx_dict:
            ctx = ExecutionRequestContext.model_validate(ctx_dict)
            operation.attach_context(ctx)
        if stage is not None:
            operation.logging_context.setdefault("stage", stage)
        if position is not None:
            operation.logging_context.setdefault("position", position)

        return operation
