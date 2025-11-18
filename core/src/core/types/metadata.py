"""Metadata types for medallion architecture and query operations.

This module contains all metadata classes including layer-specific metadata
(Bronze, Silver, Gold, Snapshot) and query-related metadata types.
"""

from typing import Any, Dict, List, NamedTuple, Optional, Set, Union

from pydantic import ConfigDict, Field, field_serializer, model_validator

from core.types.base import CTEBaseModel
from core.constants.medallion import ExecutionMode, SnapshotFrequency
from core.constants.sql import QueryType
from core.constants.compute import EngineType


# ============================================================================
# Layer Metadata Classes
# ============================================================================

class BronzeMetadata(CTEBaseModel):
    """Metadata for Bronze layer ingestion processes.
    
    The Bronze layer is the landing zone for raw data from source systems.
    This metadata defines how data is ingested, stored, and prepared for
    downstream processing.
    
    Attributes:
        source_system: Name of the source system providing the data.
            Examples: "salesforce", "sap", "postgres_orders".
        ingestion_mode: How data is ingested - "incremental" for changes only,
            "full" for complete refresh, "append" for adding new records only.
        description: Description of the data being ingested and its purpose.
        tags: Tags for categorizing ingestion processes. Include source type
            and data domain: ["source:api", "domain:sales", "frequency:hourly"].
    """
    source_system: str
    ingestion_mode: str = "incremental"  # "incremental", "full", "append"
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class SilverMetadata(CTEBaseModel):
    """Metadata for Silver layer ETL processes.
    
    The Silver layer is responsible for data transformation and enrichment.
    This metadata class defines how Silver layer ETL processes are configured
    and executed, including grouping strategies and stored procedure generation.
    
    Attributes:
        sp_name: Name of the stored procedure that will be generated for this
            ETL process. Should follow naming conventions like "Load_[Entity]_[Type]".
        group_file_name: Path to the JSON configuration file that defines
            grouping and transformation rules for the Silver layer process.
        description: Human-readable description of what this ETL process does.
            Used for documentation and monitoring dashboards.
        tags: List of tags for categorizing and filtering ETL processes.
            Examples: ["dimension", "daily", "customer-data"].
        preferred_engine: Engine preference for all queries in this sequencer.
            Valid values: "sql", "spark", "auto". Defaults to "sql".
        disabled: If True, this transformation won't be executed. Used for
            client-specific features or gradual rollout. Defaults to False.
    """
    sp_name: str
    group_file_name: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    preferred_engine: EngineType = EngineType.SQL  # Valid values: "sql", "spark", "auto"
    model_name: Optional[str] = None 
    disable_key_reshuffling: bool = False
    disabled: bool = False  # If True, transformation won't be executed (for client-specific features)
        
    @field_serializer('preferred_engine')
    def serialize_engine(self, value: str) -> str:
        """Serialize engine preference to string value."""
        return value
    
    @model_validator(mode='after')
    def set_model_name(self):
        """Set the model name for this SilverMetadata instance."""
        if not self.model_name:
            self.model_name = self.group_file_name.split('/')[0].replace('group_', '')

        return self


class GoldMetadata(CTEBaseModel):
    """Metadata for Gold layer analytical processes.
    
    The Gold layer contains business-ready data products optimized for
    analytics, reporting, and machine learning. This metadata configures
    how Gold layer views and aggregations are created and managed.
    
    Attributes:
        schema_name: Target schema for Gold layer objects. This should be
            a dedicated schema for analytical views and aggregations.
        layer: Medallion layer identifier. Defaults to "gold" but can be
            customized for specialized layers like "gold_ml" or "gold_executive".
        description: Human-readable description of the analytical dataset's
            purpose and content. Used in data catalogs and documentation.
        tags: List of tags for categorizing and discovering views. Use
            consistent tagging: ["domain:sales", "refresh:daily", "priority:high"].
    """
    schema_name: str
    layer: str = "gold"
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class SnapshotMetadata(CTEBaseModel):
    """Metadata for Snapshot layer processes.
    
    The Snapshot layer captures point-in-time states of data for historical
    tracking, compliance, and temporal analysis. This metadata defines retention
    policies, capture frequencies, and storage optimization strategies.
    
    Attributes:
        schema_name: Target schema for snapshot tables. Should be separate from
            operational schemas to manage retention and permissions independently.
        retention_days: How long to retain snapshots before automatic deletion.
            Set to -1 for indefinite retention. Consider compliance requirements
            and storage costs.
        compression: Whether to compress snapshot data. Highly recommended for
            data older than 30 days to reduce storage costs.
        frequency: How often to capture snapshots. EVERY_RUN captures on each
            ETL execution, while scheduled frequencies reduce storage overhead.
        description: Description of what data is being snapshotted and why.
            Important for compliance and audit purposes.
        tags: Tags for categorizing snapshots. Include data classification
            levels: ["pii:true", "compliance:gdpr", "retention:7years"].
    """
    schema_name: str
    retention_days: int = 90
    compression: bool = True
    frequency: SnapshotFrequency = SnapshotFrequency.DAILY
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class TransformationMetadata(CTEBaseModel):
    """Metadata for transformation processes in model groups.
    
    Defines configuration for transformations that are part of
    model groups, particularly in Silver layer processing.
    
    Attributes:
        sp_name: Stored procedure name for the transformation
        silver_table_name: Target table in Silver layer
        intermediate_synapse_object: Intermediate object in Synapse
        function_name: Azure Function name if applicable
        add_default_row: Whether to add a default row
        is_surrogate_key_calculated: Whether surrogate keys are calculated
        surrogate_key: Name of the surrogate key column
        unique_idx: List of columns forming the unique index
        disable_key_reshuffling: Whether to disable key reshuffling
    """
    sp_name: str
    silver_table_name: str
    intermediate_synapse_object: str
    add_default_row: bool = False
    is_surrogate_key_calculated: bool = False
    surrogate_key: Optional[str] = None
    unique_idx: Optional[List[str]] = None
    disable_key_reshuffling: bool = False
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TransformationMetadata':
        """Create TransformationMetadata from dictionary.
        
        Handles string to boolean conversions and optional fields.
        """
        # Convert string booleans to actual booleans
        if 'add_default_row' in data and isinstance(data['add_default_row'], str):
            data['add_default_row'] = data['add_default_row'].lower() == 'true'
        if 'is_surrogate_key_calculated' in data and isinstance(data['is_surrogate_key_calculated'], str):
            data['is_surrogate_key_calculated'] = data['is_surrogate_key_calculated'].lower() == 'true'
        if 'disable_key_reshuffling' in data and isinstance(data['disable_key_reshuffling'], str):
            data['disable_key_reshuffling'] = data['disable_key_reshuffling'].lower() == 'true'
        
        # Handle empty strings as None
        for field in ['function_name', 'surrogate_key']:
            if field in data and data[field] == '':
                data[field] = None
        
        return cls(**data)


# Union type for all class metadata
ClassMetadata = Union[
    BronzeMetadata,
    SilverMetadata,
    GoldMetadata,
    SnapshotMetadata
]


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
        execution_type: [DEPRECATED - Ignored] Previously controlled execution strategy.
            Now all dependencies are automatically determined from SQL analysis.
        order: [DEPRECATED - Ignored] Previously controlled execution priority.
            Now execution order is determined by actual data dependencies.
        preferred_engine: Engine preference for query execution.
            Valid values: "sql", "spark", "auto". Defaults to "sql".
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
    execution_type: ExecutionMode = ExecutionMode.SEQUENTIAL  # Deprecated - ignored
    order: float = 0.0  # Deprecated - ignored
    preferred_engine: EngineType = EngineType.SQL  # Valid values: "sql", "spark", "auto"
    unique_idx: Optional[List[str]] = None  # Dimension natural key columns
    filter: Optional[str] = None  # Enum name for auto-generation
    create_stats: bool = False  # Auto-create statistics after operation
    stats_columns: Optional[List[str]] = None  # Specific columns for statistics


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
    
    Attributes:
        reads_from: Set of source table names that the query reads from
        writes_to: Target table name for DML operations (None for SELECT queries)
    """
    reads_from: Dict[str, Set] = Field(default_factory=dict)
    writes_to: Optional[str] = None


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
    sql: Optional[str]
    dependencies: 'SQLDependencies'
    metadata: QueryMetadata
    method: Any
    error: Optional[str] = None


# Export all metadata types
__all__ = [
    # Layer metadata
    'BronzeMetadata',
    'SilverMetadata',
    'GoldMetadata',
    'SnapshotMetadata',
    'TransformationMetadata',
    'ClassMetadata',
    # Query metadata
    'QueryMetadata',
    'DiscoveredMethod',
    'SQLDependencies',
    'QueryAnalysis',
]