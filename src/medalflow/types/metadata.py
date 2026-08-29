"""Metadata types for medallion architecture and query operations.

This module contains all metadata classes including layer-specific metadata
(Bronze, Silver, Gold) and query-related metadata types.
"""

import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, NamedTuple

from pydantic import Field

from medalflow.constants.compute import EngineType
from medalflow.constants.sql import QueryType
from medalflow.types.base import CTEBaseModel

# ============================================================================
# Layer Metadata Classes
# ============================================================================


@contextmanager
def _shadowing_schema_is_intended() -> Iterator[None]:
    """Declare a model whose `schema` field shadows the deprecated v1 shim.

    Pydantic warns that a field named `schema` shadows ``BaseModel.schema()``,
    its deprecated v1 compatibility shim. Shadowing it is the point: ADR 002 D2
    makes `schema` the layers' vocabulary for their write target, and nothing
    calls the shim.

    The warning names the class, so the suppression was copy-pasted once per
    model. All three layers declare a `schema` now, and three copies of a
    filter differing only in a class name is one filter. The pattern stays
    narrow deliberately -- it matches the `schema` shadow warning and nothing
    else, so an unrelated `UserWarning` raised while the class body runs still
    reaches the author.

    Yields:
        None, for the duration of the class definition
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r'Field name "schema" in ".*" shadows an attribute',
            category=UserWarning,
        )
        yield


with _shadowing_schema_is_intended():

    class BronzeMetadata(CTEBaseModel):
        """One declared Bronze model, which is one Bronze table.

        The Bronze layer is the landing zone for raw data from source systems.
        A model declares which source table it reads and which Bronze table it
        writes; everything else about the ingest -- the CTAS, the soft-delete
        filter, the statistics -- is generated from that.

        Attributes:
            name: The model's identity: what discovery keys on, what the plan
                reports, and the name of the Bronze table it creates.
            schema: Target schema the Bronze table is created in. Was the
                hardcoded literal ``"bronze"`` until Decision 6 part 2.
            source_system: Name of the source system providing the data.
                Examples: "salesforce", "sap", "postgres_orders".
            source_table: Source table the rows are read from. Defaults to
                ``name``, which is the usual case.
            source_schema: Source schema the rows are read from. None means the
                sequencer's own ``source_schema`` stands in.
            description: Description of the data being ingested and its purpose.
            tags: Tags for categorizing ingestion processes. Include source type
                and data domain: ["source:api", "domain:sales", "frequency:hourly"].
            disabled: If True, discovery leaves this model out of the plan.
                Defaults to False.
        """

        name: str
        schema: str
        source_system: str
        source_table: str
        source_schema: str | None = None
        description: str | None = None
        tags: list[str] = Field(default_factory=list)
        disabled: bool = False


with _shadowing_schema_is_intended():

    class SilverMetadata(CTEBaseModel):
        """Metadata for Silver layer ETL processes.

        The Silver layer is responsible for data transformation and enrichment.
        This metadata class defines how Silver layer ETL processes are configured
        and executed, including grouping strategies and stored procedure generation.

        Attributes:
            name: Identity of the transformation — what the plan reports, what
                discovery indexes on, and the name of the generated stored
                procedure.
            schema: Target schema this transformation writes into, and the
                default `schema_name` for the class's own `@query_metadata`
                methods. It lived only on those methods until Decision 2, which
                made silver the one layer whose declaration did not say where
                the model writes.
            model: The model this transformation belongs to. Discovery filters on it
                against the configured model list.
            description: Human-readable description of what this ETL process does.
                Used for documentation and monitoring dashboards.
            tags: List of tags for categorizing and filtering ETL processes.
                Examples: ["dimension", "daily", "customer-data"].
            disable_key_reshuffling: Carried for downstream key handling; the
                framework itself does not act on it.
            disabled: If True, this transformation won't be executed. Used for
                client-specific features or gradual rollout. Defaults to False.
        """

        name: str
        schema: str
        model: str
        description: str | None = None
        tags: list[str] = Field(default_factory=list)
        disable_key_reshuffling: bool = False
        # If True, transformation won't be executed (for client-specific features)
        disabled: bool = False


with _shadowing_schema_is_intended():

    class GoldMetadata(CTEBaseModel):
        """Metadata for Gold layer analytical processes.

        The Gold layer contains business-ready data products optimized for
        analytics, reporting, and machine learning. This metadata configures
        how Gold layer views and aggregations are created and managed.

        Attributes:
            name: The model's identity: what discovery keys on, what the plan
                reports, and what a selector matches. It was the class name
                until Decision 2 landed here, which made renaming a class
                rename the model.
            schema: Target schema for Gold layer objects. This should be
                a dedicated schema for analytical views and aggregations. It is
                also the default `schema_name` for the class's own
                `@query_metadata` methods.
            layer: Medallion layer identifier. Defaults to "gold" but can be
                customized for specialized layers like "gold_ml" or "gold_executive".
            description: Human-readable description of the analytical dataset's
                purpose and content. Used in data catalogs and documentation.
            tags: List of tags for categorizing and discovering views. Use
                consistent tagging: ["domain:sales", "refresh:daily", "priority:high"].
            disabled: If True, discovery leaves this model out of the plan.
                Defaults to False.
        """

        name: str
        schema: str
        layer: str = "gold"
        description: str | None = None
        tags: list[str] = Field(default_factory=list)
        disabled: bool = False


class TransformationMetadata(CTEBaseModel):
    """Metadata for transformation processes in model groups.

    Defines configuration for transformations that are part of
    model groups, particularly in Silver layer processing.

    Attributes:
        name: Name of the transformation
        silver_table_name: Target table in Silver layer
        intermediate_synapse_object: Intermediate object in Synapse
        function_name: Azure Function name if applicable
        add_default_row: Whether to add a default row
        is_surrogate_key_calculated: Whether surrogate keys are calculated
        surrogate_key: Name of the surrogate key column
        unique_idx: List of columns forming the unique index
        disable_key_reshuffling: Whether to disable key reshuffling
    """

    name: str
    silver_table_name: str
    intermediate_synapse_object: str
    add_default_row: bool = False
    is_surrogate_key_calculated: bool = False
    surrogate_key: str | None = None
    unique_idx: list[str] | None = None
    disable_key_reshuffling: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransformationMetadata":
        """Create TransformationMetadata from dictionary.

        Handles string to boolean conversions and optional fields.
        """
        # Convert string booleans to actual booleans
        if "add_default_row" in data and isinstance(data["add_default_row"], str):
            data["add_default_row"] = data["add_default_row"].lower() == "true"
        if "is_surrogate_key_calculated" in data and isinstance(
            data["is_surrogate_key_calculated"], str
        ):
            data["is_surrogate_key_calculated"] = (
                data["is_surrogate_key_calculated"].lower() == "true"
            )
        if "disable_key_reshuffling" in data and isinstance(data["disable_key_reshuffling"], str):
            data["disable_key_reshuffling"] = data["disable_key_reshuffling"].lower() == "true"

        # Handle empty strings as None
        for field in ["function_name", "surrogate_key"]:
            if field in data and data[field] == "":
                data[field] = None

        return cls(**data)


# Union type for all class metadata
ClassMetadata = BronzeMetadata | SilverMetadata | GoldMetadata


# ============================================================================
# Query Metadata Classes
# ============================================================================


class QueryMetadata(CTEBaseModel):
    """Simplified metadata for query methods within ETL sequencers.

    This class defines how individual SQL queries should be executed within
    the ETL framework. It captures query characteristics, execution strategy,
    and optimization hints like automatic statistics creation.

    Attributes:
        type: Type of SQL query operation (SELECT, INSERT, UPDATE, etc.).
            Determines how the framework processes the query result.
        table_name: Target table name for the query operation. For SELECT,
            this is where results are stored. For INSERT/UPDATE, this is
            the table being modified.
        schema_name: Database schema containing the target table. If empty,
            uses the default schema for the connection.
        preferred_engine: Internal engine hint, forwarded to the operation's
            ``engine_hint``. Not settable by an author: engine selection is
            inert, so every operation executes on SQL (ADR 002, D4).
        unique_idx: List of column names forming the natural/business key for dimensions.
            When specified, indicates this is a dimension table with unique constraints.
        filter: Enum name for filter-based dimensions. When specified and method returns None,
            an enum query is auto-generated from bronze.Enumeration table.
        create_stats: Whether to automatically create statistics after the operation.
            Useful for optimizing query performance on newly created/populated tables.
        stats_columns: Specific columns to create statistics on. If None and create_stats
            is True, statistics will be created on all columns.
    """

    type: QueryType
    table_name: str = ""
    schema_name: str = ""
    preferred_engine: EngineType = EngineType.SQL  # internal plumbing; see ADR 002 D4
    unique_idx: list[str] | None = None  # Dimension natural key columns
    filter: str | None = None  # Enum name for auto-generation
    create_stats: bool = False  # Auto-create statistics after operation
    stats_columns: list[str] | None = None  # Specific columns for statistics


class DiscoveredMethod(NamedTuple):
    """Represents a discovered method with its metadata and SQL query.

    This NamedTuple encapsulates the output of method discovery during sequencer
    initialization. It preserves tuple unpacking compatibility while providing
    type safety and named attribute access.

    Attributes:
        method_name: Name of the discovered method
        method: The actual method object
        metadata: Query metadata from the decorator
        sql: The executed SQL query string
    """

    method_name: str
    method: Any
    metadata: QueryMetadata
    sql: str


class SQLDependencies(CTEBaseModel):
    """Extracted SQL dependencies from a query.

    This type encapsulates the essential dependency information extracted from
    SQL queries - which tables are read from and which table is written to.
    Used by the SQL dependency analyzer to understand data flow.

    Both fields use one shape: fully-qualified, lowercase ``"schema.table"``
    strings (bare ``"table"`` when the SQL leaves it unqualified). Matching is
    global, so a silver model reading ``bronze.customers`` resolves against the
    bronze operation that writes it.

    Attributes:
        reads_from: Set of qualified source tables the query reads from
        writes_to: Qualified target table for DML operations (None for SELECT)
    """

    reads_from: set[str] = Field(default_factory=set)
    writes_to: str | None = None


class QueryAnalysis(CTEBaseModel):
    """Analysis results for a discovered query method.

    This type encapsulates the analysis results for a query method including SQL,
    dependencies, and metadata. Used to avoid redundant analysis during
    execution plan generation.

    Attributes:
        sql: The SQL query string (None if analysis failed)
        dependencies: Structured SQL dependencies extracted from the query
        metadata: Original QueryMetadata from the method decorator
        method: Reference to the actual method object
        error: Error message if analysis failed (None if successful)
    """

    sql: str | None
    dependencies: "SQLDependencies"
    metadata: QueryMetadata
    method: Any
    error: str | None = None


# Export all metadata types
__all__ = [
    # Layer metadata
    "BronzeMetadata",
    "SilverMetadata",
    "GoldMetadata",
    "TransformationMetadata",
    "ClassMetadata",
    # Query metadata
    "QueryMetadata",
    "DiscoveredMethod",
    "SQLDependencies",
    "QueryAnalysis",
]
