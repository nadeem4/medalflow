"""Silver layer implementation for data cleansing, standardization, and enrichment.

The Silver layer is the second stage of the medallion architecture, responsible for
transforming Bronze layer data into validated, conformed, and enriched datasets.
This layer implements business rules, data quality checks, and master data management
patterns including slowly changing dimensions (SCD).

Architecture:
    The Silver layer follows a metadata-driven approach where transformations are
    defined declaratively using Python classes and decorators. Each transformation
    is encapsulated in a sequencer that can contain multiple queries executed in
    parallel or sequential order.

Key Responsibilities:
    - **Data Validation**: Schema validation, data type casting, constraint checking
    - **Data Cleansing**: Null handling, duplicate removal, data standardization
    - **Business Rules**: Apply domain-specific logic and calculations
    - **Data Enrichment**: Lookup operations, reference data joins
    - **Dimension Processing**: SCD Type 1, 2, and 3 implementations
    - **Quality Metrics**: Data quality scoring and monitoring

Components:
    - **SilverTransformationSequencer**: Base class for Silver layer ETL transformations
        Handles standard fact table processing, incremental loads, and data validation
        
    - **DimensionSequencer**: Specialized for dimension table processing
        Implements slowly changing dimension patterns with automatic key management
        
    - **silver_metadata**: Class decorator for Silver sequencer configuration
        Defines stored procedure names, grouping, and processing metadata
        
    - **dimension_metadata**: Class decorator for dimension configuration
        Specifies SCD type, key columns, and change tracking behavior

Execution Patterns:
    Silver layer processing follows these common patterns:
    
    1. **Standard Fact Processing**:
       - Load from Bronze with validation
       - Apply business rules and calculations
       - Insert/update Silver tables with audit columns
    
    2. **Dimension Processing**:
       - Extract unique dimension records
       - Detect changes using business keys
       - Apply SCD logic (Type 1: update, Type 2: versioning)
       - Manage surrogate keys automatically
    
    3. **Incremental Loading**:
       - Process only changed/new records
       - Maintain watermarks and processing timestamps
       - Handle late-arriving data scenarios

Data Quality Framework:
    The Silver layer implements comprehensive data quality controls:
    
    - **Schema Validation**: Ensure data types match expectations
    - **Constraint Checks**: Validate foreign keys, unique constraints
    - **Business Rule Validation**: Apply domain-specific validation logic
    - **Data Profiling**: Collect statistics on data distribution
    - **Anomaly Detection**: Flag unusual patterns in data

Example Usage:
    Basic Silver layer transformation::
    
        from core.medallion.silver import SilverTransformationSequencer, silver_metadata, query_metadata
        from core.constants.sql import QueryType, ExecutionMode
        
        @silver_metadata(
            sp_name="Load_Customer_Silver",
            group_file_name="customer_processing.json",
            description="Customer data cleansing and enrichment"
        )
        class CustomerSilver(SilverTransformationSequencer):
            
            @query_metadata(
                type=QueryType.INSERT,
                table_name="customer",
                schema_name="silver",
                description="Load cleansed customer data",
                execution_type=ExecutionMode.SEQUENTIAL,
                order=1
            )
            def load_customers(self) -> str:
                return '''
                    INSERT INTO silver.customer
                    SELECT 
                        customer_id,
                        UPPER(TRIM(customer_name)) as customer_name,
                        COALESCE(email, 'unknown@domain.com') as email,
                        CASE 
                            WHEN LENGTH(phone) = 10 THEN phone
                            ELSE NULL 
                        END as phone,
                        CURRENT_TIMESTAMP as processed_date
                    FROM bronze.raw_customers
                    WHERE is_active = 1
                    AND customer_id IS NOT NULL
                '''
    
    Dimension processing with SCD Type 2::
    
        from core.medallion.silver import DimensionSequencer, dimension_metadata
        
        @dimension_metadata(
            sp_name="Load_Product_Dimension",
            scd_type=2,
            business_key="product_code",
            dimension_name="product"
        )
        class ProductDimension(DimensionSequencer):
            
            def get_source_query(self) -> str:
                return '''
                    SELECT 
                        product_code,
                        product_name,
                        category,
                        price,
                        effective_date
                    FROM bronze.product_master
                    WHERE is_current = 1
                '''

Performance Considerations:
    - **Batch Processing**: Optimal batch sizes for different table types
    - **Partitioning**: Leverage data partitioning for large fact tables
    - **Indexing**: Create appropriate indexes for lookup operations
    - **Statistics**: Maintain table statistics for query optimization
    - **Parallel Execution**: Use parallel queries where data dependencies allow

See Also:
    - medallion.bronze: Raw data ingestion layer
    - medallion.gold: Analytics-ready aggregated views
    - medallion.snapshot: Point-in-time data captures
    - compute: Platform abstraction for query execution
    - datalake: Data lake operations for file management
"""

from .sequencer import SilverTransformationSequencer
from .decorators import silver_metadata
from core.types.metadata import SilverMetadata
from .processor import _SilverProcessor as SilverProcessor
from .validator import _SilverValidator as SilverValidator

__all__ = [
    "SilverTransformationSequencer",
    "SilverProcessor",
    "SilverValidator",
    "silver_metadata",
    "SilverMetadata",
]