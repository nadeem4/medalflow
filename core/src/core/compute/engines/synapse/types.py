"""Synapse-specific type definitions for Spark engine implementation.

This module contains types that are specific to the Azure Synapse Spark
implementation and are not part of the core compute interface.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from core.constants.compute import JobStatus


class SessionStatus(str, Enum):
    """Status of a Synapse Spark session."""
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"


class SparkSessionConfig(BaseModel):
    """Configuration for Synapse Spark session.
    
    Used to configure Spark session parameters for job execution.
    """
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    app_name: str = Field(default="CTE_Spark_Session")
    master: str = Field(default="yarn")
    deploy_mode: str = Field(default="client", pattern="^(client|cluster)$")
    spark_conf: Dict[str, Any] = Field(default_factory=dict)
    executor_instances: Optional[int] = Field(default=None, gt=0)
    executor_cores: Optional[int] = Field(default=None, gt=0)
    executor_memory: Optional[str] = Field(default=None, pattern=r'^\d+[kmg]$')
    driver_memory: Optional[str] = Field(default=None, pattern=r'^\d+[kmg]$')
    max_result_size: Optional[str] = Field(default=None, pattern=r'^\d+[kmg]$')


class JobDefinition(BaseModel):
    """Definition of a Synapse Spark job."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    job_id: str = Field(..., min_length=1)
    job_type: str = Field(default="batch")
    main_class: Optional[str] = Field(default=None)
    main_file: Optional[str] = Field(default=None)
    arguments: List[str] = Field(default_factory=list)
    configuration: Dict[str, Any] = Field(default_factory=dict)


class SessionInfo(BaseModel):
    """Information about a Synapse Spark session."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    session_id: str = Field(..., min_length=1)
    app_id: Optional[str] = Field(default=None)
    state: str = Field(default="starting")
    log: Optional[str] = Field(default=None)


class StatementResult(BaseModel):
    """Result of a Synapse Spark statement execution."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    statement_id: int = Field(..., ge=0)
    state: str
    output: Optional[Dict[str, Any]] = Field(default=None)
    progress: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class JobDefinitionInfo(BaseModel):
    """Information about a Synapse job definition."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    name: str = Field(..., min_length=1)
    definition: JobDefinition
    created_at: datetime
    updated_at: Optional[datetime] = Field(default=None)


class FileUploadResult(BaseModel):
    """Result of file upload for Synapse Spark job."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    file_path: str = Field(..., min_length=1)
    size_bytes: int = Field(..., ge=0)
    upload_time: datetime
    checksum: Optional[str] = Field(default=None)


class JobLogs(BaseModel):
    """Logs from a Synapse Spark job execution."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    job_id: str = Field(..., min_length=1)
    stdout: Optional[str] = Field(default=None)
    stderr: Optional[str] = Field(default=None)
    driver_logs: Optional[str] = Field(default=None)
    executor_logs: Optional[Dict[str, str]] = Field(default=None)


class NotebookConfig(BaseModel):
    """Configuration for Synapse notebook execution.
    
    Used to configure Spark notebook execution parameters.
    """
    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )
    
    notebook_path: str = Field(..., min_length=1)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: Optional[int] = Field(default=None, gt=0)
    retry_on_failure: bool = Field(default=False)
    max_retries: int = Field(default=3, ge=0, le=10)


class NotebookResult(BaseModel):
    """Result of Synapse notebook execution."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        use_enum_values=False
    )
    
    notebook_path: str = Field(..., min_length=1)
    run_id: str = Field(..., min_length=1)
    status: JobStatus
    start_time: datetime
    end_time: Optional[datetime] = Field(default=None)
    duration_seconds: Optional[float] = Field(default=None, ge=0.0)
    output: Optional[Dict[str, Any]] = Field(default=None)
    error_message: Optional[str] = Field(default=None)
    parameters_used: Dict[str, Any] = Field(default_factory=dict)


# Export all Synapse-specific types
__all__ = [
    'SessionStatus',
    'SparkSessionConfig',
    'JobDefinition',
    'SessionInfo',
    'StatementResult',
    'JobDefinitionInfo',
    'FileUploadResult',
    'JobLogs',
    'NotebookConfig',
    'NotebookResult',
]