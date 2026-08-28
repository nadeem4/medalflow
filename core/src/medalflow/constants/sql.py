"""SQL and query-related constants.

This module contains fundamental SQL operation enums and constants
that are used across multiple layers of the architecture.

These constants are in Layer 0 as they represent core SQL concepts
that can be used by any layer without creating circular dependencies.
"""

from enum import Enum


class QueryType(str, Enum):
    """SQL query type enumeration.
    
    Defines the types of SQL queries that can be executed.
    Used for query analysis and optimization decisions across
    operations, query builders, compute, and medallion layers.
    
    Categories:
    - DML: SELECT, INSERT, UPDATE, DELETE, MERGE, COPY, TRUNCATE
    - DDL: CREATE_TABLE, DROP_TABLE, CREATE_VIEW, CREATE_OR_ALTER_VIEW, 
           DROP_VIEW, CREATE_SCHEMA, DROP_SCHEMA, ALTER
    - Statistics: CREATE_STATISTICS, DROP_STATISTICS
    - Execution: EXECUTE_SQL, UNKNOWN
    """
    
    # Data Query
    SELECT = "SELECT"
    
    # Data Manipulation (DML)
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    MERGE = "MERGE"
    COPY = "COPY"
    TRUNCATE = "TRUNCATE"
    
    # Data Definition (DDL) - Tables
    CREATE_TABLE = "CREATE_TABLE"
    DROP_TABLE = "DROP_TABLE"
    
    # Data Definition (DDL) - Views
    CREATE_OR_ALTER_VIEW = "CREATE_OR_ALTER_VIEW"
    DROP_VIEW = "DROP_VIEW"
    
    # Data Definition (DDL) - Schema
    CREATE_SCHEMA = "CREATE_SCHEMA"
    DROP_SCHEMA = "DROP_SCHEMA"
    
    # Data Definition (DDL) - Other
    ALTER = "ALTER"
    
    # Statistics Operations
    CREATE_STATISTICS = "CREATE_STATISTICS"
    DROP_STATISTICS = "DROP_STATISTICS"
    
    # Generic Execution
    EXECUTE_SQL = "EXECUTE_SQL"
    UNKNOWN = "UNKNOWN"