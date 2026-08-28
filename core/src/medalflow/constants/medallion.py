"""Medallion architecture constants and enumerations.

This module contains medallion-specific enum types used throughout 
the medallion module for layer management and execution control.

Note: QueryType has been moved to constants/sql.py as it's a fundamental
SQL concept used by multiple layers, not medallion-specific.
"""

from enum import Enum


class ExecutionMode(str, Enum):
    """Query execution mode enumeration.
    
    Defines how queries should be executed across the platform.
    Affects parallelization and resource allocation strategies.
    """
    
    SEQUENTIAL = "SEQUENTIAL"
    PARALLEL = "PARALLEL"


class Layer(str, Enum):
    """Data layer type enumeration.
    
    Defines the medallion architecture layers and additional
    utility layers for data processing.
    
    Medallion Architecture:
        BRONZE: Raw ingested data with minimal processing
        SILVER: Cleaned and validated data with business rules applied  
        GOLD: Aggregated business-ready data for analytics
        SNAPSHOT: Point-in-time data capture for historical analysis
        
    Utility Layers:
        TEMP: Temporary tables for intermediate processing
        STAGING: Staging area for data preparation
        ARCHIVE: Long-term storage for historical data
        DBO: Database objects and system tables
    """
    
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    TEMP = "temp"
    STAGING = "staging"
    SNAPSHOT = "snapshot"
    ARCHIVE = "archive"
    DBO = "dbo"


class SnapshotFrequency(str, Enum):
    """Snapshot frequency enumeration.
    
    Defines how often snapshots should be taken for
    historical data tracking and point-in-time analysis.
    """
    
    EVERY_RUN = "EVERY_RUN"
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    YEARLY = "YEARLY"


class CalendarType(str, Enum):
    """Calendar type enumeration.
    
    Defines the type of calendar system used for date calculations.
    """
    
    STANDARD = "standard"
    FOUR_FOUR_FIVE = "4-4-5"